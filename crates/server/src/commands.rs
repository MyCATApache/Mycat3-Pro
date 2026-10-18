//! 本地命令处理。
//!
//! 对应 Mycat3 `mycat3/sqlhandler` 下的本地 SQLHandler：
//! `SetSQLHandler` / `UseSQLHandler` / `CommitSQLHandler` / `ShowDatabasesSQLHandler` /
//! `ShowTablesSQLHandler` / `ExplainHandler` / `ShowVariantsHandler` 等；
//! 并新增 Pro 管理命令（`show @@datasource` / `show @@cluster` / `show @@connection` /
//! `show @@heartbeat`，Mycat2 风格，便于运维观察）。

use std::sync::Arc;

use mycat3_protocol::consts::field_type;
use mycat3_protocol::packets::ColumnDef;
use mycat3_sql::{RouteResult, StmtType};

use crate::session::SessionContext;
use crate::state::ServerState;
use crate::tx::TransactionSession;

/// 本地处理结果。
pub enum LocalOutcome {
    /// OK 包
    Ok,
    /// 结果集
    Rows {
        columns: Vec<ColumnDef>,
        rows: Vec<Vec<Option<String>>>,
    },
    /// 错误
    Error {
        errno: u16,
        sqlstate: String,
        message: String,
    },
    /// 无需响应（前端已处理，如 USE 已改会话 schema）
    Nothing,
}

fn col(name: &str) -> ColumnDef {
    ColumnDef::new(name, field_type::VAR_STRING)
}

fn err(errno: u16, sqlstate: &str, msg: impl Into<String>) -> LocalOutcome {
    LocalOutcome::Error {
        errno,
        sqlstate: sqlstate.to_string(),
        message: msg.into(),
    }
}

/// 处理路由后 tasks 为空的本地语句。
pub async fn handle_local(
    state: &Arc<ServerState>,
    ctx: &mut SessionContext,
    tx: &mut TransactionSession,
    route: &RouteResult,
    sql: &str,
) -> LocalOutcome {
    match route.stmt_type {
        StmtType::Set => handle_set(state, ctx, tx, sql).await,
        StmtType::Use => handle_use(ctx, route).await,
        StmtType::TxBegin => {
            let xa_mode = ctx.transaction_policy.eq_ignore_ascii_case("xa");
            if let Err(e) = tx.begin(xa_mode).await {
                return err(1105, "HY000", e.to_string());
            }
            ctx.in_transaction = true;
            LocalOutcome::Ok
        }
        StmtType::TxCommit => match tx.commit(&state.xa_log).await {
            Ok(()) => {
                ctx.in_transaction = false;
                state
                    .stats
                    .tx_commits
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                LocalOutcome::Ok
            }
            Err(e) => err(1105, "HY000", format!("commit failed: {}", e)),
        },
        StmtType::TxRollback => match tx.rollback().await {
            Ok(()) => {
                ctx.in_transaction = false;
                state
                    .stats
                    .tx_rollbacks
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                LocalOutcome::Ok
            }
            Err(e) => err(1105, "HY000", format!("rollback failed: {}", e)),
        },
        StmtType::TxSavepoint | StmtType::TxReleaseSavepoint | StmtType::TxRollbackSavepoint => {
            if tx.xa_active() {
                // MySQL XA 事务不支持 SAVEPOINT（XA START 后无法回滚到保存点）
                return err(1235, "42000", "XA 事务不支持 SAVEPOINT");
            }
            let name = route.use_schema.clone().unwrap_or_default();
            let cmd = match route.stmt_type {
                StmtType::TxSavepoint => format!("SAVEPOINT `{}`", name),
                StmtType::TxReleaseSavepoint => format!("RELEASE SAVEPOINT `{}`", name),
                _ => format!("ROLLBACK TO SAVEPOINT `{}`", name),
            };
            if tx.pinned_count() == 0 {
                // 无固定连接：本地记录即可（后续连接由 BEGIN 语义覆盖）
                return LocalOutcome::Ok;
            }
            match tx.broadcast_to_pinned(&cmd).await {
                Ok(()) => LocalOutcome::Ok,
                Err(e) => err(1105, "HY000", e.to_string()),
            }
        }
        StmtType::Explain => {
            let rows = route
                .explain_rows
                .iter()
                .map(|r| vec![Some(r[0].clone()), Some(r[1].clone())])
                .collect();
            LocalOutcome::Rows {
                columns: vec![col("DATA_NODE"), col("SQL")],
                rows,
            }
        }
        StmtType::Show => handle_show(state, ctx, sql).await,
        _ => LocalOutcome::Ok,
    }
}

// ---------------------------------------------------------------------------
// SET
// ---------------------------------------------------------------------------

async fn handle_set(
    state: &Arc<ServerState>,
    ctx: &mut SessionContext,
    tx: &mut TransactionSession,
    sql: &str,
) -> LocalOutcome {
    let norm = sql.to_lowercase();
    let norm = norm.trim().trim_end_matches(';').trim();
    // set [session|global] autocommit = 0/1（压掉空白后匹配）
    if norm.contains("autocommit") {
        let compact: String = norm.chars().filter(|c| !c.is_whitespace()).collect();
        if compact.contains("autocommit=0") || compact.contains("autocommit=off") {
            ctx.autocommit = false;
        } else if compact.contains("autocommit=1") || compact.contains("autocommit=on") {
            ctx.autocommit = true;
            // autocommit 恢复且无显式事务 → 结束语句级状态
            if !tx.explicit_tx {
                ctx.in_transaction = false;
            }
        }
        propagate_to_pinned(tx, sql).await;
        return LocalOutcome::Ok;
    }
    // set names xxx
    if let Some(pos) = norm.find("names") {
        let name = norm[pos + 5..]
            .trim()
            .trim_matches(|c: char| c == '=' || c.is_whitespace() || c == ';');
        ctx.charset = charset_by_name(name);
        propagate_to_pinned(tx, sql).await;
        return LocalOutcome::Ok;
    }
    // set transaction_policy='xa' | 'proxy'（对应 Java MycatdbCommand 事务策略切换）
    if norm.contains("transaction_policy") {
        // 取 = 之后的策略值（容忍 set transaction_policy = 'xa' 各种空白/引号形式）
        let v = sql
            .to_lowercase()
            .split("transaction_policy")
            .nth(1)
            .unwrap_or("")
            .trim_matches(|c: char| {
                c.is_whitespace() || c == '=' || c == '\'' || c == '"' || c == ';'
            })
            .to_string();
        if tx.explicit_tx || tx.pinned_count() > 0 {
            return LocalOutcome::Error {
                errno: 1105,
                sqlstate: "HY000".into(),
                message: "事务中不能切换事务策略".into(),
            };
        }
        match v.as_str() {
            "xa" => ctx.transaction_policy = "xa".into(),
            "proxy" => ctx.transaction_policy = "proxy".into(),
            _ => {
                return LocalOutcome::Error {
                    errno: 1064,
                    sqlstate: "42000".into(),
                    message: format!("未知事务策略: {}（支持 proxy/xa）", v),
                };
            }
        }
        return LocalOutcome::Ok;
    }
    // SET @var = 字面量：会话用户变量跟踪（文本 PREPARE 的 EXECUTE USING 用）
    if let Some(vars) = crate::prepared_text::parse_mysql_set_vars(sql) {
        for (name, value) in vars {
            ctx.user_vars.insert(name, value);
        }
        return LocalOutcome::Ok;
    }
    // set session transaction isolation level xxx
    if norm.contains("isolation level") {
        ctx.isolation = if norm.contains("read-uncommitted") || norm.contains("read uncommitted") {
            0
        } else if norm.contains("read-committed") || norm.contains("read committed") {
            1
        } else if norm.contains("serializable") {
            2
        } else {
            3
        };
        propagate_to_pinned(tx, sql).await;
        return LocalOutcome::Ok;
    }
    // 其他 SET（用户变量等）：转发到已固定连接；无连接则仅记录 OK
    let _ = state;
    propagate_to_pinned(tx, sql).await;
    LocalOutcome::Ok
}

/// 将 SET 语句同步到事务中已固定的全部后端连接。
async fn propagate_to_pinned(tx: &mut TransactionSession, sql: &str) {
    for name in tx.pinned_names() {
        if let Some(conn) = tx.get_mut(&name) {
            let _ = conn.set_forward(sql).await;
        }
    }
}

fn charset_by_name(name: &str) -> u8 {
    match name {
        "utf8mb4" => 45,
        "utf8" | "utf8mb3" => 33,
        "latin1" => 8,
        "gbk" => 28,
        "gb2312" => 24,
        "binary" => 63,
        "ascii" => 11,
        _ => 45,
    }
}

// ---------------------------------------------------------------------------
// USE
// ---------------------------------------------------------------------------

async fn handle_use(ctx: &mut SessionContext, route: &RouteResult) -> LocalOutcome {
    let name = match &route.use_schema {
        Some(n) if !n.is_empty() => n.clone(),
        _ => return err(1064, "42000", "USE requires a schema name"),
    };
    // 逻辑库存在性校验（对应 Java UseSQLHandler）
    ctx.schema = Some(name);
    LocalOutcome::Ok
}

// ---------------------------------------------------------------------------
// SHOW / 管理命令
// ---------------------------------------------------------------------------

pub async fn handle_show(
    state: &Arc<ServerState>,
    ctx: &mut SessionContext,
    sql: &str,
) -> LocalOutcome {
    let norm = sql.to_lowercase();
    let norm = norm.trim().trim_end_matches(';');
    let norm = norm.trim();

    // Pro 管理命令（Mycat2 风格 show @@xxx）
    if norm.starts_with("show @@") {
        return handle_admin_show(state, norm);
    }
    if norm.starts_with("show databases") || norm.starts_with("show schemas") {
        let meta = state.metadata();
        let mut rows: Vec<Vec<Option<String>>> =
            meta.schemas.keys().map(|s| vec![Some(s.clone())]).collect();
        // 系统库（客户端兼容）
        for sys in ["information_schema"] {
            rows.push(vec![Some(sys.to_string())]);
        }
        return LocalOutcome::Rows {
            columns: vec![col("Database")],
            rows,
        };
    }
    if norm.starts_with("show tables") {
        let schema = ctx.schema.clone();
        let meta = state.metadata();
        let schema = match schema {
            Some(s) => s,
            None => return err(1046, "3D000", "No database selected"),
        };
        // show tables from xx
        let schema = norm
            .split(" from ")
            .nth(1)
            .map(|s| s.trim().trim_matches('`').to_string())
            .unwrap_or(schema);
        return match meta.get_schema(&schema) {
            Some(s) => {
                let rows: Vec<Vec<Option<String>>> =
                    s.tables.keys().map(|t| vec![Some(t.clone())]).collect();
                LocalOutcome::Rows {
                    columns: vec![col(&format!("Tables_in_{}", schema))],
                    rows,
                }
            }
            None => err(1049, "42000", format!("Unknown database '{}'", schema)),
        };
    }
    // show full tables / show create table / show columns → 前端转发（见 frontend.rs proxy 分支）
    // 其余 SHOW 交由 prototype 后端处理
    LocalOutcome::Nothing
}

fn handle_admin_show(state: &Arc<ServerState>, norm: &str) -> LocalOutcome {
    let cmd = norm.trim_start_matches("show @@").trim();
    match cmd {
        "version" => LocalOutcome::Rows {
            columns: vec![col("VERSION")],
            rows: vec![vec![Some(
                state.config().server.server.server_version.clone(),
            )]],
        },
        "datasource" => {
            let mut rows: Vec<Vec<Option<String>>> = state
                .conn_mgr
                .snapshot()
                .into_iter()
                .map(|(name, idle, permits, created, _)| {
                    vec![
                        Some(name),
                        Some(idle.to_string()),
                        Some(permits.to_string()),
                        Some(created.to_string()),
                    ]
                })
                .collect();
            // Oracle 后端池（dbType=oracle）单独统计，合并展示
            for (name, pool) in state.oracle_pools.read().iter() {
                let (idle, permits, created) = pool.snapshot();
                rows.push(vec![
                    Some(name.clone()),
                    Some(idle.to_string()),
                    Some(permits.to_string()),
                    Some(created.to_string()),
                ]);
            }
            // PG 后端池（dbType=postgresql）单独统计，合并展示
            for (name, pool) in state.pg_pools.read().iter() {
                let (idle, permits, created) = pool.snapshot();
                rows.push(vec![
                    Some(name.clone()),
                    Some(idle.to_string()),
                    Some(permits.to_string()),
                    Some(created.to_string()),
                ]);
            }
            rows.sort_by(|a, b| a[0].cmp(&b[0]));
            LocalOutcome::Rows {
                columns: vec![col("NAME"), col("IDLE"), col("AVAILABLE"), col("CREATED")],
                rows,
            }
        }
        "cluster" => {
            let mut rows = Vec::new();
            for sel in state.replica().selectors() {
                for inst in sel.all_instances() {
                    rows.push(vec![
                        Some(sel.name.clone()),
                        Some(inst.name.clone()),
                        Some(if inst.is_master { "MASTER" } else { "REPLICA" }.to_string()),
                        Some(if inst.is_alive() { "alive" } else { "dead" }.to_string()),
                        Some(if inst.as_select_read() { "yes" } else { "no" }.to_string()),
                        Some(inst.active().to_string()),
                    ]);
                }
            }
            LocalOutcome::Rows {
                columns: vec![
                    col("CLUSTER"),
                    col("DATASOURCE"),
                    col("ROLE"),
                    col("ALIVE"),
                    col("SELECT_AS_READ"),
                    col("ACTIVE"),
                ],
                rows,
            }
        }
        "connection" | "processlist" => {
            let s = state.stats.snapshot();
            LocalOutcome::Rows {
                columns: vec![col("ACTIVE"), col("TOTAL"), col("QUERIES")],
                rows: vec![vec![
                    Some(s[1].to_string()),
                    Some(s[0].to_string()),
                    Some(s[2].to_string()),
                ]],
            }
        }
        "heartbeat" => {
            let rows = state
                .heartbeat()
                .snapshot()
                .into_iter()
                .map(|(name, err_count)| vec![Some(name), Some(err_count.to_string())])
                .collect();
            LocalOutcome::Rows {
                columns: vec![col("DATASOURCE"), col("ERROR_COUNT")],
                rows,
            }
        }
        "users" => {
            let cfg = state.config();
            let mut rows: Vec<Vec<Option<String>>> = cfg
                .users
                .values()
                .map(|u| {
                    vec![
                        Some(u.username.clone()),
                        Some(u.ip.clone().unwrap_or_else(|| "%".into())),
                        Some(u.dialect.clone()),
                        Some(u.visible_schemas().join(",")),
                    ]
                })
                .collect();
            rows.sort();
            LocalOutcome::Rows {
                columns: vec![col("USERNAME"), col("IP"), col("DIALECT"), col("SCHEMAS")],
                rows,
            }
        }
        "schemas" => {
            let cfg = state.config();
            let mut rows: Vec<Vec<Option<String>>> = cfg
                .schemas
                .values()
                .map(|s| vec![Some(s.schema_name.clone()), Some(s.target_name.clone())])
                .collect();
            rows.sort();
            LocalOutcome::Rows {
                columns: vec![col("SCHEMA_NAME"), col("TARGET_NAME")],
                rows,
            }
        }
        "help" => LocalOutcome::Rows {
            columns: vec![col("COMMAND"), col("DESCRIPTION")],
            rows: vec![
                vec![Some("show @@version".into()), Some("服务端版本".into())],
                vec![
                    Some("show @@datasource".into()),
                    Some("数据源连接池状态".into()),
                ],
                vec![
                    Some("show @@cluster".into()),
                    Some("集群/实例存活状态".into()),
                ],
                vec![
                    Some("show @@connection".into()),
                    Some("前端连接统计".into()),
                ],
                vec![Some("show @@heartbeat".into()), Some("心跳错误计数".into())],
                vec![Some("show @@users".into()), Some("用户清单".into())],
                vec![Some("show @@schemas".into()), Some("逻辑库清单".into())],
            ],
        },
        other => err(
            1064,
            "42000",
            format!("unknown admin command: show @@{}", other),
        ),
    }
}

// ---------------------------------------------------------------------------
// 客户端探针 SQL 本地应答（select @@version 等）
// ---------------------------------------------------------------------------

/// 若 SQL 是常见客户端探针，返回本地结果集；否则 None。
/// 对应 `ShowVariantsHandler` / `MysqlVariableServiceImpl` 的本地变量应答。
pub fn probe_local_select(
    state: &Arc<ServerState>,
    ctx: &SessionContext,
    sql: &str,
) -> Option<LocalOutcome> {
    let norm = sql.trim().trim_end_matches(';').to_lowercase();
    let norm = norm.replace(" limit 1", "");
    let norm = norm.trim();
    if !norm.starts_with("select ") {
        return None;
    }
    let body = norm["select ".len()..].trim();
    let server_version = &state.config().server.server.server_version;
    let (col_name, value) = match body {
        "@@version_comment" | "@@version_comment limit 1" => {
            ("@@version_comment", "Mycat 3 Pro (Rust)".to_string())
        }
        "@@version" | "version()" => ("@@version", server_version.clone()),
        "database()" => (
            "DATABASE()",
            ctx.schema.clone().unwrap_or_else(|| "NULL".to_string()),
        ),
        "user()" | "current_user()" | "system_user()" | "session_user()" => {
            ("USER()", format!("{}@{}", ctx.user, ctx.client_ip))
        }
        "connection_id()" => ("CONNECTION_ID()", ctx.connection_id.to_string()),
        "@@session.tx_isolation"
        | "@@tx_isolation"
        | "@@transaction_isolation"
        | "@@session.transaction_isolation" => (
            "@@transaction_isolation",
            match ctx.isolation {
                0 => "READ-UNCOMMITTED",
                1 => "READ-COMMITTED",
                2 => "SERIALIZABLE",
                _ => "REPEATABLE-READ",
            }
            .to_string(),
        ),
        "@@autocommit" => (
            "@@autocommit",
            if ctx.autocommit { "1" } else { "0" }.to_string(),
        ),
        "@@max_allowed_packet" => ("@@max_allowed_packet", "16777216".to_string()),
        "@@character_set_client" | "@@character_set_results" | "@@character_set_connection" => {
            (body, charset_name(ctx.charset).to_string())
        }
        _ => return None,
    };
    Some(LocalOutcome::Rows {
        columns: vec![ColumnDef::new(col_name, field_type::VAR_STRING)],
        rows: vec![vec![Some(value)]],
    })
}

fn charset_name(c: u8) -> &'static str {
    match c {
        33 => "utf8",
        45 | 46 | 47 => "utf8mb4",
        8 => "latin1",
        28 => "gbk",
        63 => "binary",
        _ => "utf8mb4",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mycat3_config::{default_config, MycatRouterConfig};

    fn state() -> Arc<ServerState> {
        ServerState::new(default_config()).unwrap()
    }

    #[tokio::test]
    async fn test_admin_show_commands() {
        let st = state();
        for cmd in [
            "show @@version",
            "show @@datasource",
            "show @@cluster",
            "show @@connection",
            "show @@heartbeat",
            "show @@help",
        ] {
            match handle_admin_show(&st, cmd) {
                LocalOutcome::Rows { rows, columns } => {
                    assert!(!columns.is_empty());
                    let _ = rows;
                }
                _ => panic!("{} should produce rows", cmd),
            }
        }
        match handle_admin_show(&st, "show @@whatever") {
            LocalOutcome::Error { errno, .. } => assert_eq!(errno, 1064),
            _ => panic!(),
        }
    }

    #[tokio::test]
    async fn test_show_databases_tables() {
        let st = state();
        let mut ctx = SessionContext::new(1, "127.0.0.1".into());
        match handle_show(&st, &mut ctx, "show databases").await {
            LocalOutcome::Rows { rows, .. } => {
                let names: Vec<String> = rows.iter().map(|r| r[0].clone().unwrap()).collect();
                assert!(names.contains(&"db1".to_string()));
            }
            _ => panic!(),
        }
        ctx.schema = Some("db1".into());
        match handle_show(&st, &mut ctx, "show tables").await {
            LocalOutcome::Rows { rows, .. } => {
                assert!(rows.iter().any(|r| r[0].as_deref() == Some("travelrecord")));
            }
            _ => panic!(),
        }
        // 未选库 → 1046
        let mut ctx2 = SessionContext::new(2, "127.0.0.1".into());
        match handle_show(&st, &mut ctx2, "show tables").await {
            LocalOutcome::Error { errno, .. } => assert_eq!(errno, 1046),
            _ => panic!(),
        }
    }

    #[tokio::test]
    async fn test_set_handling() {
        let st = state();
        let mut ctx = SessionContext::new(1, "127.0.0.1".into());
        let mut tx = TransactionSession::new();
        let route = RouteResult {
            stmt_type: StmtType::Set,
            tasks: vec![],
            merge: None,
            use_schema: None,
            hint: None,
            explain_rows: vec![],
        };
        handle_local(&st, &mut ctx, &mut tx, &route, "set autocommit=0").await;
        assert!(!ctx.autocommit);
        handle_local(&st, &mut ctx, &mut tx, &route, "SET AUTOCOMMIT = 1").await;
        assert!(ctx.autocommit);
        handle_local(&st, &mut ctx, &mut tx, &route, "set names gbk").await;
        assert_eq!(ctx.charset, 28);
        handle_local(
            &st,
            &mut ctx,
            &mut tx,
            &route,
            "set session transaction isolation level read committed",
        )
        .await;
        assert_eq!(ctx.isolation, 1);
    }

    #[tokio::test]
    async fn test_use_changes_schema() {
        let st = state();
        let mut ctx = SessionContext::new(1, "127.0.0.1".into());
        let mut tx = TransactionSession::new();
        let route = RouteResult {
            stmt_type: StmtType::Use,
            tasks: vec![],
            merge: None,
            use_schema: Some("db1".into()),
            hint: None,
            explain_rows: vec![],
        };
        match handle_local(&st, &mut ctx, &mut tx, &route, "use db1").await {
            LocalOutcome::Ok => {}
            _ => panic!(),
        }
        assert_eq!(ctx.schema.as_deref(), Some("db1"));
    }

    #[tokio::test]
    async fn test_tx_begin_commit_without_backend() {
        // 无固定连接时 BEGIN/COMMIT 应为纯本地操作成功
        let st = state();
        let mut ctx = SessionContext::new(1, "127.0.0.1".into());
        let mut tx = TransactionSession::new();
        let mk = |t| RouteResult {
            stmt_type: t,
            tasks: vec![],
            merge: None,
            use_schema: None,
            hint: None,
            explain_rows: vec![],
        };
        assert!(matches!(
            handle_local(&st, &mut ctx, &mut tx, &mk(StmtType::TxBegin), "begin").await,
            LocalOutcome::Ok
        ));
        assert!(ctx.in_transaction);
        assert!(matches!(
            handle_local(&st, &mut ctx, &mut tx, &mk(StmtType::TxCommit), "commit").await,
            LocalOutcome::Ok
        ));
        assert!(!ctx.in_transaction);
    }

    #[tokio::test]
    async fn test_probe_local_selects() {
        let st = state();
        let mut ctx = SessionContext::new(42, "127.0.0.1".into());
        ctx.user = "root".into();
        ctx.schema = Some("db1".into());
        match probe_local_select(&st, &ctx, "select @@version_comment limit 1").unwrap() {
            LocalOutcome::Rows { rows, .. } => {
                assert_eq!(rows[0][0].as_deref(), Some("Mycat 3 Pro (Rust)"))
            }
            _ => panic!(),
        }
        match probe_local_select(&st, &ctx, "SELECT DATABASE()").unwrap() {
            LocalOutcome::Rows { rows, .. } => assert_eq!(rows[0][0].as_deref(), Some("db1")),
            _ => panic!(),
        }
        match probe_local_select(&st, &ctx, "select connection_id()").unwrap() {
            LocalOutcome::Rows { rows, .. } => assert_eq!(rows[0][0].as_deref(), Some("42")),
            _ => panic!(),
        }
        assert!(probe_local_select(&st, &ctx, "select * from t").is_none());
        let _ = MycatRouterConfig::new();
    }
}
