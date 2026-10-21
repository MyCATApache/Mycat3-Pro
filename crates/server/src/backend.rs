//! 后端连接抽象：MySQL（原生协议）与 Oracle（官方 rust-oracledb thin 驱动）双后端。
//!
//! 设计要点：
//! - `BackendOutcome` 统一为 `Ok{affected,last_insert_id}` 或
//!   `Rows(mycat3_client::ResultSet)`（Oracle 结果**合成** MySQL 文本行载荷，
//!   使归并/透传/三前端回写零改动复用）；
//! - 事务语义：MySQL 显式 `BEGIN`；Oracle 事务隐式开启（连接固定即事务），
//!   COMMIT/ROLLBACK 走驱动原生接口，SAVEPOINT 语句直传；
//! - Oracle 连接在事务会话中仍由池持有（Arc 池 + permit），事务结束自然归还。

use std::sync::Arc;
use std::time::Duration;

use mycat3_client::{CheckedOutConnection, ClientError, QueryOutcome, ResultSet};
use mycat3_mssql::{CheckedOutMssqlConn, MssqlError, MssqlOutcome, MssqlPool};
use mycat3_oracle::{config_from_jdbc_url, OraclePool, PooledOracleConn};
use mycat3_pgclient::{CheckedOutPgConn, PgError};
use mycat3_protocol::packets::{build_text_row, ColumnDef};
use thiserror::Error;

use crate::state::ServerState;

#[derive(Debug, Error)]
pub enum BackendError {
    #[error("backend error {errno} ({sqlstate}): {message}")]
    Server {
        errno: u16,
        sqlstate: String,
        message: String,
    },
    #[error("connection error: {0}")]
    Conn(String),
}

impl BackendError {
    pub fn to_parts(&self) -> (u16, String, String) {
        match self {
            BackendError::Server {
                errno,
                sqlstate,
                message,
            } => (*errno, sqlstate.clone(), message.clone()),
            BackendError::Conn(m) => (1105, "HY000".to_string(), m.clone()),
        }
    }
}

impl From<ClientError> for BackendError {
    fn from(e: ClientError) -> Self {
        match e {
            ClientError::Server {
                errno,
                sqlstate,
                message,
            } => BackendError::Server {
                errno,
                sqlstate,
                message,
            },
            other => BackendError::Conn(other.to_string()),
        }
    }
}

/// 后端连接池（按数据源名）。对应 `JdbcConnectionManager` 的多方言分派。
pub enum BackendPool {
    MySql(Arc<mycat3_client::DataSourcePool>),
    Oracle(Arc<OraclePool>),
    Pg(Arc<mycat3_pgclient::PgDataSourcePool>),
    Mssql(Arc<MssqlPool>),
}

impl BackendPool {
    /// 按数据源配置分派构造。
    pub fn get(state: &Arc<ServerState>, datasource: &str) -> Result<BackendPool, BackendError> {
        let cfg = state
            .replica()
            .datasource_config(datasource)
            .ok_or_else(|| BackendError::Conn(format!("datasource {} not found", datasource)))?;
        if cfg.db_type.eq_ignore_ascii_case("oracle") {
            let pool = {
                let existing = state.oracle_pools.read().get(datasource).cloned();
                match existing {
                    Some(p) => p,
                    None => {
                        let oracle_cfg = config_from_jdbc_url(&cfg.url, &cfg.user, &cfg.password)
                            .map_err(|e| {
                            BackendError::Conn(format!("datasource {}: {}", datasource, e))
                        })?;
                        let p = OraclePool::new(
                            datasource,
                            oracle_cfg,
                            cfg.max_con as usize,
                            cfg.init_sqls.clone(),
                        );
                        state
                            .oracle_pools
                            .write()
                            .insert(datasource.to_string(), p.clone());
                        p
                    }
                }
            };
            Ok(BackendPool::Oracle(pool))
        } else if cfg.db_type.eq_ignore_ascii_case("mssql")
            || cfg.db_type.eq_ignore_ascii_case("sqlserver")
            || cfg.db_type.eq_ignore_ascii_case("mssqlserver")
        {
            let pool = {
                let existing = state.mssql_pools.read().get(datasource).cloned();
                match existing {
                    Some(p) => p,
                    None => {
                        let mut ep =
                            mycat3_mssql::parse_mssql_jdbc_url(&cfg.url, &cfg.user, &cfg.password)
                                .map_err(|e| {
                                    BackendError::Conn(format!("datasource {}: {}", datasource, e))
                                })?;
                        ep.connect_timeout =
                            Duration::from_millis(cfg.max_connect_timeout.max(1000));
                        let p = MssqlPool::new(
                            datasource,
                            ep,
                            cfg.max_con as usize,
                            cfg.min_con as usize,
                            Duration::from_millis(cfg.idle_timeout.max(1000)),
                            cfg.init_sqls.clone(),
                        );
                        state
                            .mssql_pools
                            .write()
                            .insert(datasource.to_string(), p.clone());
                        if let Ok(h) = tokio::runtime::Handle::try_current() {
                            let warm = p.clone();
                            h.spawn(async move { warm.prewarm().await });
                        }
                        p
                    }
                }
            };
            Ok(BackendPool::Mssql(pool))
        } else if cfg.db_type.eq_ignore_ascii_case("postgresql")
            || cfg.db_type.eq_ignore_ascii_case("postgres")
            || cfg.db_type.eq_ignore_ascii_case("pgsql")
        {
            let pool = {
                let existing = state.pg_pools.read().get(datasource).cloned();
                match existing {
                    Some(p) => p,
                    None => {
                        let ep =
                            mycat3_pgclient::parse_pg_jdbc_url(&cfg.url, &cfg.user, &cfg.password)
                                .map_err(|e| {
                                    BackendError::Conn(format!("datasource {}: {}", datasource, e))
                                })?;
                        let p = mycat3_pgclient::PgDataSourcePool::new(
                            datasource,
                            ep,
                            cfg.max_con as usize,
                        );
                        state
                            .pg_pools
                            .write()
                            .insert(datasource.to_string(), p.clone());
                        p
                    }
                }
            };
            Ok(BackendPool::Pg(pool))
        } else {
            let pool = state
                .conn_mgr
                .get_pool(&cfg)
                .map_err(|e| BackendError::Conn(e.to_string()))?;
            Ok(BackendPool::MySql(pool))
        }
    }

    pub async fn acquire(&self) -> Result<BackendSession, BackendError> {
        match self {
            BackendPool::MySql(p) => {
                let conn = p.acquire().await.map_err(BackendError::from)?;
                Ok(BackendSession {
                    kind: BackendKind::MySql(conn),
                    in_tx: false,
                })
            }
            BackendPool::Oracle(p) => {
                let conn = p
                    .acquire()
                    .await
                    .map_err(|e| BackendError::Conn(e.to_string()))?;
                Ok(BackendSession {
                    kind: BackendKind::Oracle(conn),
                    in_tx: false,
                })
            }
            BackendPool::Pg(p) => {
                let conn = p
                    .acquire()
                    .await
                    .map_err(|e| BackendError::Conn(e.to_string()))?;
                Ok(BackendSession {
                    kind: BackendKind::Pg(conn),
                    in_tx: false,
                })
            }
            BackendPool::Mssql(p) => {
                let conn = p
                    .acquire()
                    .await
                    .map_err(|e| BackendError::Conn(e.to_string()))?;
                Ok(BackendSession {
                    kind: BackendKind::Mssql(conn),
                    in_tx: false,
                })
            }
        }
    }
}

/// 后端会话（语句级或事务固定级）。
pub struct BackendSession {
    kind: BackendKind,
    in_tx: bool,
}

enum BackendKind {
    MySql(CheckedOutConnection),
    Oracle(PooledOracleConn),
    Pg(CheckedOutPgConn),
    Mssql(CheckedOutMssqlConn),
}

/// 统一的执行结果。
pub enum BackendOutcome {
    Ok {
        affected_rows: u64,
        last_insert_id: u64,
    },
    Rows(ResultSet),
}

impl BackendSession {
    pub fn in_transaction(&self) -> bool {
        self.in_tx
    }

    pub fn set_in_transaction(&mut self, v: bool) {
        self.in_tx = v;
    }

    pub fn mark_broken(&mut self) {
        match &mut self.kind {
            BackendKind::MySql(c) => c.mark_broken(),
            BackendKind::Oracle(c) => c.mark_broken(),
            BackendKind::Pg(c) => c.mark_broken(),
            BackendKind::Mssql(c) => c.mark_broken(),
        }
    }

    /// 执行一条 SQL。`translate` 为 true 时先做 MySQL→Oracle 方言翻译。
    pub async fn query(
        &mut self,
        sql: &str,
        timeout: Option<Duration>,
    ) -> Result<BackendOutcome, BackendError> {
        match &mut self.kind {
            BackendKind::MySql(conn) => {
                let outcome = match timeout {
                    Some(t) => conn.conn().query_timeout(sql, t).await,
                    None => conn.conn().query(sql).await,
                };
                match outcome {
                    Ok(QueryOutcome::Ok(ok)) => Ok(BackendOutcome::Ok {
                        affected_rows: ok.affected_rows,
                        last_insert_id: ok.last_insert_id,
                    }),
                    Ok(QueryOutcome::Rows(rs)) => Ok(BackendOutcome::Rows(rs)),
                    Err(e) => match e {
                        // SQL 级错误（ERR 包：唯一键冲突/语法/权限等）：连接可复用、
                        // 事务继续（MySQL 语句错误不终止事务），绝不销毁连接
                        ClientError::Server {
                            errno,
                            sqlstate,
                            message,
                        } => Err(BackendError::Server {
                            errno,
                            sqlstate,
                            message,
                        }),
                        other => {
                            conn.mark_broken();
                            Err(BackendError::Conn(other.to_string()))
                        }
                    },
                }
            }
            BackendKind::Oracle(conn) => {
                // 语句级超时（Oracle 驱动无原生超时：外层 tokio 超时，语句在
                // spawn_blocking 内完成后结果被丢弃，连接状态仍一致可复用）
                let r = match timeout {
                    Some(t) => match tokio::time::timeout(t, conn.query(sql)).await {
                        Ok(r) => r,
                        Err(_) => Err(mycat3_oracle::OracleError::Timeout(sql.to_string())),
                    },
                    None => conn.query(sql).await,
                };
                match r {
                    Ok(outcome) => {
                        let is_dml = matches!(outcome, mycat3_oracle::OracleOutcome::Ok { .. });
                        let backend = oracle_to_backend(outcome);
                        // 驱动无 autocommit 开关（默认 false）：非事务语句即时提交，
                        // 对应 JDBC autocommit=true 语义；事务内语句由 tx_command 的
                        // COMMIT/ROLLBACK 收口，此处绝不提交
                        if !self.in_tx && is_dml {
                            if let Err(e) = conn.commit().await {
                                if e.is_conn_level() {
                                    conn.mark_broken();
                                }
                                return Err(oracle_err(e));
                            }
                        }
                        Ok(backend)
                    }
                    Err(e) => {
                        // SQL 级错误（ORA-xxxx）连接可复用；仅连接级错误销毁
                        if e.is_conn_level() {
                            conn.mark_broken();
                        }
                        Err(oracle_err(e))
                    }
                }
            }
            BackendKind::Pg(conn) => {
                let r = conn.conn().query(sql).await;
                match r {
                    Ok(outcome) => Ok(pg_to_backend(outcome)),
                    Err(e) => {
                        // 仅连接级错误销毁连接；SQL 级（数据库错误）连接可复用
                        if e.is_conn_level() {
                            conn.mark_broken();
                        }
                        Err(pg_err(e))
                    }
                }
            }
            BackendKind::Mssql(conn) => match conn.conn().query(sql, timeout).await {
                Ok(MssqlOutcome::Ok {
                    affected_rows,
                    last_insert_id,
                }) => Ok(BackendOutcome::Ok {
                    affected_rows,
                    last_insert_id,
                }),
                Ok(MssqlOutcome::Rows { columns, rows }) => Ok(text_rows_to_backend(
                    columns
                        .iter()
                        .map(|c| (c.name.as_str(), c.mysql_type))
                        .collect(),
                    rows,
                )),
                Err(e) => {
                    if e.is_conn_level() {
                        conn.mark_broken();
                    }
                    Err(mssql_err(e))
                }
            },
        }
    }

    /// XA 语句下推（仅 MySQL；Oracle 后端显式报错——rust-oracledb thin 驱动无 XA）。
    pub async fn xa_command(&mut self, sql: &str) -> Result<(), BackendError> {
        match &mut self.kind {
            BackendKind::MySql(conn) => conn
                .conn()
                .query(sql)
                .await
                .map(|_| ())
                .map_err(BackendError::from),
            BackendKind::Oracle(_) => Err(BackendError::Conn(
                "XA 事务不支持 Oracle 后端（rust-oracledb thin 驱动无 XA 接口）".into(),
            )),
            BackendKind::Mssql(_) => Err(BackendError::Conn(
                "XA 事务不支持 SQL Server 后端（无 XA；MSDTC 未实现）".into(),
            )),
            BackendKind::Pg(conn) => conn.conn().query(sql).await.map(|_| ()).map_err(pg_err),
        }
    }

    /// SET 类会话设置转发：仅 MySQL 后端支持（Oracle 无 SET 会话语法，跳过）。
    pub async fn set_forward(&mut self, sql: &str) -> Result<(), BackendError> {
        match &mut self.kind {
            BackendKind::MySql(conn) => conn
                .conn()
                .query(sql)
                .await
                .map(|_| ())
                .map_err(BackendError::from),
            BackendKind::Oracle(_) => Ok(()),
            BackendKind::Pg(conn) => conn.conn().query(sql).await.map(|_| ()).map_err(pg_err),
            BackendKind::Mssql(conn) => conn
                .conn()
                .set_forward(sql, Some(Duration::from_secs(30)))
                .await
                .map_err(mssql_err),
        }
    }

    /// 事务控制：BEGIN / COMMIT / ROLLBACK / SAVEPOINT 类语句。
    /// MySQL：SQL 下推；Oracle：BEGIN 为无操作（隐式事务），COMMIT/ROLLBACK 走驱动接口，
    /// 其余语句（SAVEPOINT 等）直传执行。
    pub async fn tx_command(&mut self, sql: &str) -> Result<(), BackendError> {
        match &mut self.kind {
            BackendKind::MySql(conn) => conn
                .conn()
                .query(sql)
                .await
                .map(|_| ())
                .map_err(BackendError::from),
            BackendKind::Oracle(conn) => {
                let upper = sql.trim().to_uppercase();
                if upper == "BEGIN" || upper.starts_with("BEGIN ") || upper == "START TRANSACTION" {
                    self.in_tx = true;
                    // 事务开启：连接可能残留未提交事务，归还池后由 acquire 回滚净化
                    conn.set_dirty(true);
                    return Ok(());
                }
                if upper == "COMMIT" {
                    self.in_tx = false;
                    return match conn.commit().await {
                        Ok(()) => {
                            conn.set_dirty(false);
                            Ok(())
                        }
                        Err(e) => {
                            if e.is_conn_level() {
                                conn.mark_broken();
                            }
                            // 失败时 dirty 保持 true：归还后回滚净化，杜绝脏连接复用
                            Err(oracle_err(e))
                        }
                    };
                }
                if upper == "ROLLBACK" {
                    self.in_tx = false;
                    return match conn.rollback().await {
                        Ok(()) => {
                            conn.set_dirty(false);
                            Ok(())
                        }
                        Err(e) => {
                            if e.is_conn_level() {
                                conn.mark_broken();
                            }
                            Err(oracle_err(e))
                        }
                    };
                }
                // SAVEPOINT / RELEASE SAVEPOINT / ROLLBACK TO SAVEPOINT 等
                conn.exec_sql_blocking(sql).await.map_err(oracle_err)
            }
            BackendKind::Pg(conn) => conn.conn().query(sql).await.map(|_| ()).map_err(pg_err),
            BackendKind::Mssql(conn) => {
                conn.conn()
                    .tx_command(sql, Some(Duration::from_secs(30)))
                    .await
                    .map_err(mssql_err)?;
                let upper = sql.trim_start().to_ascii_uppercase();
                let compact = upper.split_whitespace().collect::<Vec<_>>();
                if let Some(first) = compact.first() {
                    let first = first.trim_end_matches(';');
                    if first == "BEGIN" || first == "START" {
                        self.in_tx = true;
                    } else if first == "COMMIT" {
                        self.in_tx = false;
                    } else if first == "ROLLBACK"
                        && !compact.iter().any(|x| {
                            x.eq_ignore_ascii_case("SAVEPOINT")
                                || x.eq_ignore_ascii_case("TRANSACTION")
                        })
                    {
                        self.in_tx = false;
                    }
                }
                Ok(())
            }
        }
    }

    // ---- XA 分支原语（MySQL-only XA 的按后端分派；PG 走原生 2PC）----

    /// 分支开始（XA 模式首条语句前）：MySQL → XA START；PG → BEGIN；Oracle → 错误。
    pub async fn xa_start_branch(&mut self, gtrid: &str, bqual: &str) -> Result<(), BackendError> {
        match &mut self.kind {
            BackendKind::MySql(conn) => conn
                .conn()
                .query(&crate::xa::xa_start(gtrid, bqual))
                .await
                .map(|_| ())
                .map_err(BackendError::from),
            BackendKind::Pg(conn) => conn.conn().query("BEGIN").await.map(|_| ()).map_err(pg_err),
            BackendKind::Oracle(_) => Err(BackendError::Conn(
                "XA 事务不支持 Oracle 后端（rust-oracledb thin 驱动无 XA 接口）".into(),
            )),
            BackendKind::Mssql(_) => Err(BackendError::Conn(
                "XA 事务不支持 SQL Server 后端（无 XA；MSDTC 未实现）".into(),
            )),
        }
    }

    /// 分支结束+准备：MySQL → XA END + XA PREPARE；PG → PREPARE TRANSACTION 'gid'。
    pub async fn xa_prepare_branch(
        &mut self,
        gtrid: &str,
        bqual: &str,
    ) -> Result<(), BackendError> {
        match &mut self.kind {
            BackendKind::MySql(conn) => {
                conn.conn()
                    .query(&crate::xa::xa_end(gtrid, bqual))
                    .await
                    .map(|_| ())
                    .map_err(BackendError::from)?;
                conn.conn()
                    .query(&crate::xa::xa_prepare(gtrid, bqual))
                    .await
                    .map(|_| ())
                    .map_err(BackendError::from)
            }
            BackendKind::Pg(conn) => conn
                .conn()
                .query(&format!("PREPARE TRANSACTION '{}_{}'", gtrid, bqual))
                .await
                .map(|_| ())
                .map_err(pg_err),
            BackendKind::Oracle(_) => Err(BackendError::Conn("XA 不支持 Oracle 后端".into())),
            BackendKind::Mssql(_) => Err(BackendError::Conn(
                "XA 不支持 SQL Server 后端（无 XA；MSDTC 未实现）".into(),
            )),
        }
    }

    /// 分支提交：MySQL → XA COMMIT [ONE PHASE]；PG → COMMIT PREPARED（单参与者直接 COMMIT）。
    pub async fn xa_commit_branch(
        &mut self,
        gtrid: &str,
        bqual: &str,
        one_phase: bool,
    ) -> Result<(), BackendError> {
        match &mut self.kind {
            BackendKind::MySql(conn) => {
                let sql = if one_phase {
                    crate::xa::xa_commit_one_phase(gtrid, bqual)
                } else {
                    crate::xa::xa_commit(gtrid, bqual)
                };
                conn.conn()
                    .query(&sql)
                    .await
                    .map(|_| ())
                    .map_err(BackendError::from)
            }
            BackendKind::Pg(conn) => {
                let sql = if one_phase {
                    "COMMIT".to_string()
                } else {
                    format!("COMMIT PREPARED '{}_{}'", gtrid, bqual)
                };
                conn.conn().query(&sql).await.map(|_| ()).map_err(pg_err)
            }
            BackendKind::Oracle(_) => Err(BackendError::Conn("XA 不支持 Oracle 后端".into())),
            BackendKind::Mssql(_) => Err(BackendError::Conn(
                "XA 不支持 SQL Server 后端（无 XA；MSDTC 未实现）".into(),
            )),
        }
    }

    /// 分支回滚：MySQL → XA END + XA ROLLBACK；PG → ROLLBACK PREPARED（失败回退 ROLLBACK）。
    pub async fn xa_rollback_branch(
        &mut self,
        gtrid: &str,
        bqual: &str,
    ) -> Result<(), BackendError> {
        match &mut self.kind {
            BackendKind::MySql(conn) => {
                let _ = conn.conn().query(&crate::xa::xa_end(gtrid, bqual)).await;
                conn.conn()
                    .query(&crate::xa::xa_rollback(gtrid, bqual))
                    .await
                    .map(|_| ())
                    .map_err(BackendError::from)
            }
            BackendKind::Pg(conn) => {
                // 未 prepare（prepared 不存在）→ 普通 ROLLBACK 兜底
                if let Err(e) = conn
                    .conn()
                    .query(&format!("ROLLBACK PREPARED '{}_{}'", gtrid, bqual))
                    .await
                {
                    let _ = e;
                    conn.conn()
                        .query("ROLLBACK")
                        .await
                        .map(|_| ())
                        .map_err(pg_err)?;
                }
                Ok(())
            }
            BackendKind::Oracle(_) => Err(BackendError::Conn("XA 不支持 Oracle 后端".into())),
            BackendKind::Mssql(_) => Err(BackendError::Conn(
                "XA 不支持 SQL Server 后端（无 XA；MSDTC 未实现）".into(),
            )),
        }
    }
}

/// SQL Server 错误 → BackendError：SQL 级 → Server（可复用）；连接级 → Conn。
pub(crate) fn mssql_err(e: MssqlError) -> BackendError {
    if e.is_conn_level() {
        BackendError::Conn(e.to_string())
    } else {
        let (errno, sqlstate, message) = e.to_mysql_parts();
        BackendError::Server {
            errno,
            sqlstate,
            message,
        }
    }
}

/// PG 错误 → BackendError：SQL 级（Db）→ Server（可复用）；连接级 → Conn（销毁连接）。
fn pg_err(e: PgError) -> BackendError {
    if e.is_conn_level() {
        BackendError::Conn(e.to_string())
    } else {
        BackendError::Server {
            errno: 1105,
            sqlstate: "HY000".into(),
            message: e.to_string(),
        }
    }
}

/// Oracle 错误 → BackendError：连接级 → Conn（销毁连接）；SQL 级 → Server（可复用）。
fn oracle_err(e: mycat3_oracle::OracleError) -> BackendError {
    if e.is_conn_level() {
        BackendError::Conn(e.to_string())
    } else {
        BackendError::Server {
            errno: 1105,
            sqlstate: "HY000".into(),
            message: e.to_string(),
        }
    }
}

/// Oracle 结果 → 统一 BackendOutcome（合成 MySQL 文本行载荷）。
/// PG 结果 → 统一 BackendOutcome（合成 MySQL 文本行载荷）。
fn pg_to_backend(o: mycat3_pgclient::PgOutcome) -> BackendOutcome {
    match o {
        mycat3_pgclient::PgOutcome::Ok { affected_rows } => BackendOutcome::Ok {
            affected_rows,
            last_insert_id: 0,
        },
        mycat3_pgclient::PgOutcome::Rows { columns, rows } => text_rows_to_backend(
            columns
                .iter()
                .map(|c| (c.name.as_str(), c.mysql_type))
                .collect(),
            rows,
        ),
    }
}

/// 文本行模型 → BackendOutcome（Oracle/PG 共用）。
fn text_rows_to_backend(
    columns: Vec<(&str, u8)>,
    rows: Vec<Vec<Option<String>>>,
) -> BackendOutcome {
    let cols: Vec<ColumnDef> = columns
        .iter()
        .map(|(name, t)| {
            let mut def = ColumnDef::new(name, *t);
            if *t == mycat3_protocol::consts::field_type::BLOB {
                def.charset = mycat3_protocol::consts::charset::BINARY as u16;
            }
            def
        })
        .collect();
    let column_def_payloads: Vec<Vec<u8>> = cols.iter().map(|c| c.encode()).collect();
    let row_payloads: Vec<Vec<u8>> = rows
        .iter()
        .map(|row| {
            let values: Vec<Option<&[u8]>> = row
                .iter()
                .map(|v| v.as_ref().map(|s| s.as_bytes()))
                .collect();
            build_text_row(&values)
        })
        .collect();
    BackendOutcome::Rows(ResultSet {
        column_count: cols.len(),
        column_def_payloads,
        columns: cols,
        row_payloads,
        final_status: 0,
        warnings: 0,
    })
}

fn oracle_to_backend(o: mycat3_oracle::OracleOutcome) -> BackendOutcome {
    match o {
        mycat3_oracle::OracleOutcome::Ok { affected_rows } => BackendOutcome::Ok {
            affected_rows,
            last_insert_id: 0,
        },
        mycat3_oracle::OracleOutcome::Rows { columns, rows } => {
            let cols: Vec<ColumnDef> = columns
                .iter()
                .map(|c| {
                    let mut def = ColumnDef::new(&c.name, c.mysql_type);
                    if c.mysql_type == mycat3_protocol::consts::field_type::BLOB {
                        def.charset = mycat3_protocol::consts::charset::BINARY as u16;
                    }
                    def
                })
                .collect();
            let column_def_payloads: Vec<Vec<u8>> = cols.iter().map(|c| c.encode()).collect();
            let row_payloads: Vec<Vec<u8>> = rows
                .iter()
                .map(|row| {
                    let values: Vec<Option<&[u8]>> = row
                        .iter()
                        .map(|v| v.as_ref().map(|s| s.as_bytes()))
                        .collect();
                    build_text_row(&values)
                })
                .collect();
            BackendOutcome::Rows(ResultSet {
                column_count: cols.len(),
                column_def_payloads,
                columns: cols,
                row_payloads,
                final_status: 0,
                warnings: 0,
            })
        }
    }
}

/// 按数据源 dbType 做方言翻译（MySQL 直传；Oracle 走翻译器）。
pub fn translate_for_backend(
    state: &Arc<ServerState>,
    datasource: &str,
    sql: &str,
) -> Result<String, BackendError> {
    let cfg = state.replica().datasource_config(datasource);
    let db_type = cfg.map(|c| c.db_type).unwrap_or_default();
    if db_type.eq_ignore_ascii_case("oracle") {
        mycat3_sql::translate_oracle_sql(sql)
            .map_err(|e| BackendError::Conn(format!("SQL 翻译为 Oracle 方言失败: {}", e)))
    } else if db_type.eq_ignore_ascii_case("mssql")
        || db_type.eq_ignore_ascii_case("sqlserver")
        || db_type.eq_ignore_ascii_case("mssqlserver")
    {
        mycat3_sql::translate_mssql_sql(sql)
            .map_err(|e| BackendError::Conn(format!("SQL 翻译为 SQL Server 方言失败: {}", e)))
    } else if db_type.eq_ignore_ascii_case("postgresql")
        || db_type.eq_ignore_ascii_case("postgres")
        || db_type.eq_ignore_ascii_case("pgsql")
    {
        mycat3_sql::translate_pg_sql(sql)
            .map_err(|e| BackendError::Conn(format!("SQL 翻译为 PostgreSQL 方言失败: {}", e)))
    } else {
        Ok(sql.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mycat3_config::*;

    fn state_with(ds: DatasourceConfig) -> Arc<ServerState> {
        let mut cfg = MycatRouterConfig::new();
        cfg.datasources.insert(ds.name.clone(), ds);
        ServerState::new(cfg).unwrap()
    }

    fn ds(name: &str, db_type: &str, url: &str) -> DatasourceConfig {
        DatasourceConfig {
            name: name.into(),
            url: url.into(),
            user: "u".into(),
            password: "p".into(),
            db_type: db_type.into(),
            weight: 0,
            max_con: 4,
            min_con: 1,
            max_retry_count: 3,
            max_connect_timeout: 3000,
            init_sqls: vec![],
            instance_type: InstanceType::ReadWrite,
            idle_timeout: 60_000,
            r#type: "NATIVE".into(),
        }
    }

    #[test]
    fn test_translate_mysql_identity() {
        let st = state_with(ds("m", "mysql", "jdbc:mysql://127.0.0.1:3306/db"));
        let out = translate_for_backend(&st, "m", "SELECT * FROM `t` LIMIT 1").unwrap();
        assert_eq!(out, "SELECT * FROM `t` LIMIT 1");
    }

    #[test]
    fn test_translate_oracle() {
        let st = state_with(ds("o", "oracle", "jdbc:oracle:thin:@//127.0.0.1:1521/XE"));
        let out = translate_for_backend(&st, "o", "SELECT * FROM t LIMIT 1").unwrap();
        assert!(out.contains("ROWNUM"), "{}", out);
        // 不支持结构 → 明确报错
        assert!(translate_for_backend(&st, "o", "DELETE FROM t LIMIT 1").is_err());
    }

    #[test]
    fn test_translate_mssql() {
        let st = state_with(ds(
            "s",
            "mssql",
            "jdbc:sqlserver://127.0.0.1:1433;databaseName=db1;encrypt=true;trustServerCertificate=true",
        ));
        let out = translate_for_backend(&st, "s", "SELECT * FROM t LIMIT 10").unwrap();
        assert!(out.contains("TOP"), "{}", out);
        let out = translate_for_backend(&st, "s", "SELECT * FROM t ORDER BY id LIMIT 10 OFFSET 5")
            .unwrap();
        assert!(out.contains("OFFSET 5 ROWS"), "{}", out);
        assert!(out.contains("FETCH NEXT 10 ROWS ONLY"), "{}", out);
    }

    #[test]
    fn test_backend_pool_dispatch() {
        // 仅构造池（不连接网络）
        let st = state_with(ds("o", "oracle", "jdbc:oracle:thin:@//127.0.0.1:1521/XE"));
        match BackendPool::get(&st, "o").unwrap() {
            BackendPool::Oracle(_) => {}
            _ => panic!("expected oracle pool"),
        }
        let st = state_with(ds("m", "mysql", "jdbc:mysql://127.0.0.1:3306/db"));
        match BackendPool::get(&st, "m").unwrap() {
            BackendPool::MySql(_) => {}
            _ => panic!("expected mysql pool"),
        }
        assert!(BackendPool::get(&st, "missing").is_err());
    }

    #[test]
    fn test_oracle_outcome_synthesis() {
        use mycat3_oracle::{OracleColumn, OracleOutcome};
        use mycat3_protocol::consts::field_type;
        let out = oracle_to_backend(OracleOutcome::Rows {
            columns: vec![
                OracleColumn {
                    name: "id".into(),
                    mysql_type: field_type::NEWDECIMAL,
                },
                OracleColumn {
                    name: "name".into(),
                    mysql_type: field_type::VAR_STRING,
                },
            ],
            rows: vec![vec![Some("42".into()), None]],
        });
        match out {
            BackendOutcome::Rows(rs) => {
                assert_eq!(rs.column_count, 2);
                assert_eq!(rs.columns[0].field_type, field_type::NEWDECIMAL);
                let rows = rs.decode_rows();
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0][0].as_deref(), Some(&b"42"[..]));
                assert_eq!(rows[0][1], None); // NULL 保真
            }
            _ => panic!(),
        }
        // DML → Ok
        match oracle_to_backend(OracleOutcome::Ok { affected_rows: 7 }) {
            BackendOutcome::Ok { affected_rows, .. } => assert_eq!(affected_rows, 7),
            _ => panic!(),
        }
    }
}
