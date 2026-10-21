//! Mycat3 Pro Oracle 后端：官方 rust-oracledb（thin 驱动）的封装。
//!
//! 关键设计：
//! - 驱动为**同步阻塞 API**（`Connection::query/execute` 等均为 `fn`），因此
//!   全部数据库操作在 `tokio::task::spawn_blocking` 中执行，不阻塞异步运行时；
//! - 连接池自研（`Mutex<Vec<Connection>>` + 信号量）：驱动自带 `Pool` 使用引用借用
//!   语义，不便于事务会话**长期固定**连接（pinned），自研池语义更可控；
//! - 结果统一为 `Vec<Vec<Option<String>>>` 字符串行模型（与 PG/TDS 前端、engine 层
//!   同构），按 `DB_TYPE_*` 分派渲染（NUMBER→OracleNumber Display、
//!   TIMESTAMP→OracleTimestamp Display、BLOB→\x十六进制等）；
//! - ANO（Oracle 原生网络加密）驱动不支持（TLS 可用）；`ENCRYPTION_SERVER=REQUIRED`
//!   的加固实例无法连接，由 connect 错误显式暴露。

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use mycat3_protocol::consts::field_type;
use oracledb::{Config, Connection, Cursor, DbType, Metadata, Row};
use parking_lot::Mutex;
use thiserror::Error;
use tokio::sync::Semaphore;
use tracing::warn;

pub use oracledb;

#[derive(Debug, Error)]
pub enum OracleError {
    #[error("oracle driver error: {0}")]
    Driver(oracledb::Error),
    #[error("task join error: {0}")]
    Join(#[from] tokio::task::JoinError),
    #[error("connection pool exhausted")]
    PoolExhausted,
    #[error("connect string parse error: {0}")]
    BadConnectString(String),
    #[error("statement timeout: {0}")]
    Timeout(String),
}

pub type OracleResult<T> = Result<T, OracleError>;

/// 复用空闲连接时免探活的空闲窗口：刚归还的连接不再 ping（省一次往返），
/// 超窗后才 ping + （脏连接）rollback 校验。
pub const PING_AFTER_IDLE: Duration = Duration::from_secs(1);

/// oracledb 错误分类：`DbError` 且非会话致命 ORA 码 → SQL 级（连接可复用）；
/// 其余（网络/连接/协议）→ 连接级（连接必须重建）。
fn is_conn_error(e: &oracledb::Error) -> bool {
    use oracledb::ErrorKind;
    match e.kind() {
        // ORA-00028 会话被终止 / ORA-03113/03114/03135 通信通道断：连接已死
        ErrorKind::DbError(d) => matches!(d.code(), 28 | 3113 | 3114 | 3135),
        _ => true,
    }
}

impl OracleError {
    /// Map Oracle errors to the MySQL error surface used by all frontends.
    pub fn to_mysql_parts(&self) -> (u16, String, String) {
        match self {
            OracleError::Driver(e) => {
                if let oracledb::ErrorKind::DbError(d) = e.kind() {
                    let (errno, state) = match d.code() {
                        1 => (1062, "23000"),    // unique constraint
                        2291 => (1452, "23000"), // parent key not found
                        2292 => (1451, "23000"), // child record found
                        1400 => (1048, "23000"), // null into NOT NULL
                        942 => (1146, "42S02"),  // table or view does not exist
                        904 => (1054, "42S22"),  // invalid identifier
                        1017 => (1045, "28000"), // invalid credentials
                        955 => (1050, "42S01"),  // name already used
                        1418 => (1091, "42000"), // index does not exist
                        12514 | 12541 | 12170 => (2003, "HY000"),
                        _ => (1105, "HY000"),
                    };
                    (errno, state.to_string(), d.message().to_string())
                } else {
                    (2003, "HY000".into(), self.to_string())
                }
            }
            OracleError::Timeout(m) => (1105, "HY000".into(), m.clone()),
            OracleError::PoolExhausted => (1040, "08004".into(), self.to_string()),
            OracleError::BadConnectString(m) => (2003, "HY000".into(), m.clone()),
            OracleError::Join(m) => (1105, "HY000".into(), m.to_string()),
        }
    }

    /// 是否为连接级错误（连接不可复用，需重建）。
    pub fn is_conn_level(&self) -> bool {
        match self {
            OracleError::Driver(e) => is_conn_error(e),
            OracleError::Join(_) => true,
            OracleError::PoolExhausted | OracleError::BadConnectString(_) => false,
            OracleError::Timeout(_) => true,
        }
    }
}

/// 从 JDBC url 构造连接配置。
/// 支持 `jdbc:oracle:thin:@//host:port/service`、`jdbc:oracle:thin:@host:port:sid`、
/// 以及裸 `//host:port/service` / `host:port/service`。
pub fn config_from_jdbc_url(url: &str, user: &str, password: &str) -> OracleResult<Config> {
    let mut rest = url;
    if let Some(idx) = rest.find("://") {
        rest = &rest[idx + 3..];
    }
    for prefix in [
        "jdbc:oracle:thin:@",
        "jdbc:oracle:oci:@",
        "jdbc:oracle:thin:",
        "oracle:thin:@",
    ] {
        if rest.starts_with(prefix) {
            rest = &rest[prefix.len()..];
            break;
        }
    }
    let rest = rest.trim_start_matches('/');
    if rest.is_empty() {
        return Err(OracleError::BadConnectString(url.to_string()));
    }
    let cfg = Config::default()
        .set_credentials(user, password)
        .set_connect_string(rest)
        .map_err(|e| OracleError::BadConnectString(format!("{}: {}", url, e)))?;
    Ok(cfg)
}

/// 结果列（携带 MySQL field_type 映射，便于 executor 合成 ResultSet）。
#[derive(Debug, Clone)]
pub struct OracleColumn {
    pub name: String,
    pub mysql_type: u8,
}

/// 驱动 DbType → MySQL field_type 映射。
pub fn map_db_type(t: &'static DbType) -> u8 {
    use oracledb::*;
    match t {
        DB_TYPE_NUMBER => field_type::NEWDECIMAL,
        DB_TYPE_BINARY_FLOAT => field_type::FLOAT,
        DB_TYPE_BINARY_DOUBLE => field_type::DOUBLE,
        DB_TYPE_DATE | DB_TYPE_TIMESTAMP | DB_TYPE_TIMESTAMP_TZ | DB_TYPE_TIMESTAMP_LTZ => {
            field_type::DATETIME
        }
        DB_TYPE_BLOB | DB_TYPE_RAW | DB_TYPE_LONG_RAW | DB_TYPE_BFILE => field_type::BLOB,
        DB_TYPE_BOOLEAN => field_type::TINY,
        // VARCHAR/VARCHAR2/CHAR/NCHAR/NVARCHAR/CLOB/NCLOB/XMLTYPE/JSON/LONG/ROWID/INTERVAL/VECTOR
        _ => field_type::VAR_STRING,
    }
}

/// 单条语句执行结果。
pub enum OracleOutcome {
    /// DML/DDL
    Ok { affected_rows: u64 },
    /// 结果集（字符串行模型）
    Rows {
        columns: Vec<OracleColumn>,
        rows: Vec<Vec<Option<String>>>,
    },
}

// ---------------------------------------------------------------------------
// 阻塞式连接池
// ---------------------------------------------------------------------------

/// 空闲连接（含归还时间与"可能残留未提交事务"标记，供复用时按需校验）。
struct IdleOracleConn {
    conn: Connection,
    returned_at: std::time::Instant,
    dirty: bool,
}

pub struct OraclePool {
    name: String,
    cfg: Config,
    max_con: usize,
    min_con: usize,
    idle_timeout: Duration,
    connect_timeout: Duration,
    /// 连接建立后执行的会话初始化语句（对应 MySQL 池 initSqls；如
    /// `ALTER SESSION SET NLS_DATE_FORMAT=...` 解决日期字面量格式兼容）
    init_sqls: Vec<String>,
    idle: Mutex<Vec<IdleOracleConn>>,
    permits: Arc<Semaphore>,
    created: AtomicUsize,
}

impl OraclePool {
    pub fn new(
        name: &str,
        cfg: Config,
        max_con: usize,
        min_con: usize,
        idle_timeout: Duration,
        connect_timeout: Duration,
        init_sqls: Vec<String>,
    ) -> Arc<Self> {
        let max_con = max_con.max(1);
        Arc::new(OraclePool {
            name: name.to_string(),
            cfg,
            max_con,
            min_con: min_con.min(max_con),
            idle_timeout,
            connect_timeout,
            init_sqls,
            idle: Mutex::new(Vec::new()),
            permits: Arc::new(Semaphore::new(max_con)),
            created: AtomicUsize::new(0),
        })
    }

    /// 预热 minCon 条空闲连接。
    pub async fn prewarm(self: &Arc<Self>) {
        for _ in 0..self.min_con {
            match self.acquire().await {
                Ok(c) => drop(c),
                Err(e) => {
                    warn!(pool = %self.name, error = %e, "oracle pool prewarm failed");
                    break;
                }
            }
        }
    }

    /// 异步取出一条连接（空闲复用 + ping；上限 max_con）。
    pub async fn acquire(self: &Arc<Self>) -> OracleResult<PooledOracleConn> {
        let permit = self
            .permits
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| OracleError::PoolExhausted)?;
        // 同步快路径：取空闲（守卫先释放，避免跨 await 持有）。
        // 免 ping 复用：刚归还且干净的连接直接交付；超窗才 ping，脏连接才 rollback
        //（事务内错误/中断残留的未提交事务），避免每次取用两趟固定往返。
        loop {
            let candidate = {
                let mut idle = self.idle.lock();
                let now = std::time::Instant::now();
                let mut candidate = None;
                while let Some(c) = idle.pop() {
                    if now.duration_since(c.returned_at) > self.idle_timeout {
                        continue;
                    }
                    candidate = Some(c);
                    break;
                }
                candidate
            };
            let Some(idle_conn) = candidate else { break };
            let r = tokio::task::spawn_blocking(move || {
                let fresh = idle_conn.returned_at.elapsed() < PING_AFTER_IDLE && !idle_conn.dirty;
                let alive = fresh || idle_conn.conn.ping().is_ok();
                if alive && idle_conn.dirty {
                    let _ = idle_conn.conn.rollback();
                }
                (idle_conn.conn, alive)
            })
            .await
            .map_err(OracleError::Join)?;
            let (conn, alive) = r;
            if alive {
                return Ok(PooledOracleConn {
                    conn: Some(conn),
                    pool: self.clone(),
                    _permit: permit,
                    broken: false,
                    dirty: false,
                });
            }
        }
        // 新建（阻塞 → spawn_blocking）+ 会话初始化语句；连接超时覆盖握手/初始化。
        let cfg = self.cfg.clone();
        let init_sqls = self.init_sqls.clone();
        let fut = tokio::task::spawn_blocking(move || {
            let conn = oracledb::connect(cfg)?;
            for sql in &init_sqls {
                conn.execute(sql, &[])?;
            }
            Ok::<Connection, oracledb::Error>(conn)
        });
        let conn = match tokio::time::timeout(self.connect_timeout, fut).await {
            Ok(r) => r.map_err(OracleError::Join)?.map_err(OracleError::Driver)?,
            Err(_) => {
                return Err(OracleError::Timeout(format!(
                    "oracle connect/init timeout after {:?}",
                    self.connect_timeout
                )))
            }
        };
        self.created.fetch_add(1, Ordering::Relaxed);
        Ok(PooledOracleConn {
            conn: Some(conn),
            pool: self.clone(),
            _permit: permit,
            broken: false,
            dirty: false,
        })
    }

    fn return_conn(&self, conn: Connection, broken: bool, dirty: bool) {
        if broken {
            warn!(pool = %self.name, "discarding broken oracle connection");
            return;
        }
        let mut idle = self.idle.lock();
        if idle.len() < self.max_con {
            idle.push(IdleOracleConn {
                conn,
                returned_at: std::time::Instant::now(),
                dirty,
            });
        }
        // 超出上限的连接随 Drop 关闭
    }

    /// 池快照：(空闲连接数, 可用额度, 累计创建)。供 `show @@datasource` 管理命令。
    pub fn snapshot(&self) -> (usize, usize, u64) {
        let mut idle = self.idle.lock();
        let now = std::time::Instant::now();
        idle.retain(|c| now.duration_since(c.returned_at) <= self.idle_timeout);
        (
            idle.len(),
            self.permits.available_permits(),
            self.created.load(Ordering::Relaxed) as u64,
        )
    }
}

/// 池化连接（Drop 归还）。
pub struct PooledOracleConn {
    conn: Option<Connection>,
    pool: Arc<OraclePool>,
    _permit: tokio::sync::OwnedSemaphorePermit,
    broken: bool,
    /// 可能残留未提交事务（事务中出错/会话中断未收口），归还后由 acquire 回滚净化
    dirty: bool,
}

impl PooledOracleConn {
    pub fn mark_broken(&mut self) {
        self.broken = true;
    }

    /// 标记"可能残留未提交事务"（进入事务时置位，COMMIT/ROLLBACK 成功后清除）。
    pub fn set_dirty(&mut self, v: bool) {
        self.dirty = v;
    }

    /// 取出底层连接（事务固定场景：脱离池，由会话持有）。
    pub fn take_conn(&mut self) -> Option<Connection> {
        self.conn.take()
    }

    /// 执行一条语句并渲染结果（spawn_blocking）。
    pub async fn query(&mut self, sql: &str) -> OracleResult<OracleOutcome> {
        let conn = self.conn.take().ok_or_else(|| {
            OracleError::BadConnectString("oracle connection already taken".into())
        })?;
        let sql = sql.to_string();
        let outcome = tokio::task::spawn_blocking(move || {
            let mut conn = conn;
            let r = query_blocking(&mut conn, &sql);
            (r, conn)
        })
        .await;
        match outcome {
            Ok((r, conn)) => {
                self.conn = Some(conn);
                r
            }
            Err(e) => Err(OracleError::Join(e)),
        }
    }

    /// 事务控制（提交/回滚）。
    pub async fn commit(&mut self) -> OracleResult<()> {
        self.exec_blocking(|c| c.commit()).await
    }

    pub async fn rollback(&mut self) -> OracleResult<()> {
        self.exec_blocking(|c| c.rollback()).await
    }

    /// 任意事务控制语句（SAVEPOINT/RELEASE/ROLLBACK TO 等）。
    pub async fn exec_sql_blocking(&mut self, sql: &str) -> OracleResult<()> {
        let conn = self
            .conn
            .take()
            .ok_or_else(|| OracleError::BadConnectString("connection taken".into()))?;
        let sql = sql.to_string();
        let r = tokio::task::spawn_blocking(move || {
            let r = conn.execute(&sql, &[]);
            (r, conn)
        })
        .await;
        let (r, conn) = match r {
            Ok(x) => x,
            Err(e) => return Err(e.into()),
        };
        self.conn = Some(conn);
        r.map(|_| ()).map_err(OracleError::Driver)
    }

    async fn exec_blocking<F>(&mut self, f: F) -> OracleResult<()>
    where
        F: FnOnce(&Connection) -> Result<(), oracledb::Error> + Send + 'static,
    {
        let conn = self
            .conn
            .take()
            .ok_or_else(|| OracleError::BadConnectString("connection taken".into()))?;
        let r = tokio::task::spawn_blocking(move || {
            let r = f(&conn);
            (r, conn)
        })
        .await;
        let (r, conn) = match r {
            Ok(x) => x,
            Err(e) => return Err(e.into()),
        };
        self.conn = Some(conn);
        r.map_err(OracleError::Driver)
    }
}

impl Drop for PooledOracleConn {
    fn drop(&mut self) {
        if let Some(conn) = self.conn.take() {
            self.pool.return_conn(conn, self.broken, self.dirty);
        }
    }
}

/// 脱离池的独立连接（事务会话固定用；Drop 即回滚+关闭）。
pub struct DetachedOracleConn {
    conn: Option<Connection>,
    in_transaction: bool,
}

impl DetachedOracleConn {
    pub fn new(conn: Connection) -> Self {
        DetachedOracleConn {
            conn: Some(conn),
            in_transaction: false,
        }
    }
    pub fn in_transaction(&self) -> bool {
        self.in_transaction
    }
    pub fn set_in_transaction(&mut self, v: bool) {
        self.in_transaction = v;
    }

    pub async fn query(&mut self, sql: &str) -> OracleResult<OracleOutcome> {
        let conn = self
            .conn
            .take()
            .ok_or_else(|| OracleError::BadConnectString("connection closed".into()))?;
        let sql = sql.to_string();
        let r = tokio::task::spawn_blocking(move || {
            let mut conn = conn;
            let r = query_blocking(&mut conn, &sql);
            (r, conn)
        })
        .await;
        let (r, conn) = match r {
            Ok(x) => x,
            Err(e) => return Err(e.into()),
        };
        self.conn = Some(conn);
        r
    }

    pub async fn execute_tx(&mut self, sql: &str) -> OracleResult<()> {
        let conn = self
            .conn
            .take()
            .ok_or_else(|| OracleError::BadConnectString("connection closed".into()))?;
        let sql = sql.to_string();
        let r = tokio::task::spawn_blocking(move || {
            let r = conn.execute(&sql, &[]);
            (r, conn)
        })
        .await;
        let (r, conn) = match r {
            Ok(x) => x,
            Err(e) => return Err(e.into()),
        };
        self.conn = Some(conn);
        r.map(|_| ()).map_err(OracleError::Driver)
    }

    pub async fn commit(&mut self) -> OracleResult<()> {
        let conn = self
            .conn
            .take()
            .ok_or_else(|| OracleError::BadConnectString("connection closed".into()))?;
        let r = tokio::task::spawn_blocking(move || {
            let r = conn.commit();
            (r, conn)
        })
        .await;
        let (r, conn) = match r {
            Ok(x) => x,
            Err(e) => return Err(e.into()),
        };
        self.conn = Some(conn);
        self.in_transaction = false;
        r.map_err(OracleError::Driver)
    }

    pub async fn rollback(&mut self) -> OracleResult<()> {
        let conn = self
            .conn
            .take()
            .ok_or_else(|| OracleError::BadConnectString("connection closed".into()))?;
        let r = tokio::task::spawn_blocking(move || {
            let r = conn.rollback();
            (r, conn)
        })
        .await;
        let (r, conn) = match r {
            Ok(x) => x,
            Err(e) => return Err(e.into()),
        };
        self.conn = Some(conn);
        self.in_transaction = false;
        r.map_err(OracleError::Driver)
    }
}

impl Drop for DetachedOracleConn {
    fn drop(&mut self) {
        if let Some(mut conn) = self.conn.take() {
            let _ = conn.rollback(); // 未提交事务随关闭回滚（Oracle 语义）
            let _ = conn.close();
        }
    }
}

// ---------------------------------------------------------------------------
// 阻塞执行核心（spawn_blocking 内运行）
// ---------------------------------------------------------------------------

fn query_blocking(conn: &mut Connection, sql: &str) -> OracleResult<OracleOutcome> {
    // 按首关键字分派（不能用"失败回退"：Oracle 对 DML 走 query 失败后，
    // 同一连接再 execute 会报 ORA-01003 no statement parsed —— 语句状态被污染）
    let head = sql.trim_start();
    let upper = head.get(..6).unwrap_or(head).to_ascii_uppercase();
    if upper.starts_with("SELECT") || upper.starts_with("WITH") {
        let cursor = conn.query(sql, &[]).map_err(OracleError::Driver)?;
        render_cursor(cursor)
    } else {
        let result = conn.execute(sql, &[]).map_err(OracleError::Driver)?;
        Ok(OracleOutcome::Ok {
            affected_rows: result.rows_affected(),
        })
    }
}

/// 把游标渲染为字符串行模型。
/// 把游标渲染为字符串行模型。
/// 列元数据按**下标**预取（db_type 为 &'static），行渲染按索引取值：
/// 杜绝按名线性查找（O(R·C²)）与重名列取错值（静默数据错乱）。
fn render_cursor(cursor: Cursor) -> OracleResult<OracleOutcome> {
    let columns: Vec<OracleColumn> = cursor
        .columns()
        .iter()
        .map(|m: &Metadata| OracleColumn {
            name: m.name().to_string(),
            mysql_type: map_db_type(m.db_type()),
        })
        .collect();
    let types: Vec<&'static DbType> = cursor.columns().iter().map(|m| m.db_type()).collect();
    let mut rows = Vec::new();
    for row_result in cursor {
        let row: Row = row_result.map_err(OracleError::Driver)?;
        let mut values = Vec::with_capacity(columns.len());
        for (idx, t) in types.iter().enumerate() {
            values.push(render_value(&row, idx, t)?);
        }
        rows.push(values);
    }
    Ok(OracleOutcome::Rows { columns, rows })
}

/// 按列下标与类型渲染单个值为字符串。
fn render_value(row: &Row, idx: usize, t: &'static DbType) -> OracleResult<Option<String>> {
    use oracledb::*;
    match t {
        DB_TYPE_NUMBER => {
            let v: Option<oracledb::OracleNumber> = row.get(idx).ok();
            Ok(v.map(|n| n.to_string()))
        }
        DB_TYPE_BINARY_FLOAT => Ok(row
            .get::<Option<f32>>(idx)
            .map_err(OracleError::Driver)?
            .map(|v| v.to_string())),
        DB_TYPE_BINARY_DOUBLE => Ok(row
            .get::<Option<f64>>(idx)
            .map_err(OracleError::Driver)?
            .map(|v| v.to_string())),
        DB_TYPE_DATE | DB_TYPE_TIMESTAMP | DB_TYPE_TIMESTAMP_TZ | DB_TYPE_TIMESTAMP_LTZ => {
            let v: Option<oracledb::OracleTimestamp> = row.get(idx).ok();
            Ok(v.map(|ts| format_timestamp(&ts)))
        }
        DB_TYPE_BOOLEAN => Ok(row
            .get::<Option<bool>>(idx)
            .map_err(OracleError::Driver)?
            .map(|v| if v { "1" } else { "0" }.to_string())),
        DB_TYPE_BLOB | DB_TYPE_RAW | DB_TYPE_LONG_RAW | DB_TYPE_BFILE => {
            let v: Option<Vec<u8>> = row.get(idx).ok();
            Ok(v.map(hex_encode))
        }
        _ => Ok(row
            .get::<Option<String>>(idx)
            .map_err(OracleError::Driver)?),
    }
}

/// OracleTimestamp → "YYYY-MM-DD HH:MM:SS.ffffff"（去尾零与 Z 时区标记）。
fn format_timestamp(ts: &oracledb::OracleTimestamp) -> String {
    let raw = ts.to_string();
    let mut s = raw.clone();
    if let Some(pos) = s.find('T') {
        s.replace_range(pos..pos + 1, " ");
    }
    if let Some(dot) = s.find('.') {
        let mut head = s[..dot].to_string();
        let frac = s[dot + 1..].trim_end_matches(|c| c == '0' || c == 'Z');
        if !frac.is_empty() {
            head.push('.');
            head.push_str(frac);
        }
        s = head;
    } else {
        s = s.trim_end_matches('Z').to_string();
    }
    s
}

fn hex_encode(bytes: Vec<u8>) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut s = String::with_capacity(bytes.len() * 2 + 2);
    s.push_str("\\x");
    for b in &bytes {
        s.push(HEX[(b >> 4) as usize] as char);
        s.push(HEX[(b & 0x0f) as usize] as char);
    }
    s
}

/// 供 server 层直接建立一次性连接（心跳/DDL 场景）。
pub async fn connect_direct(cfg: Config) -> OracleResult<DetachedOracleConn> {
    let conn = tokio::task::spawn_blocking(move || oracledb::connect(cfg))
        .await
        .map_err(OracleError::Join)?
        .map_err(OracleError::Driver)?;
    Ok(DetachedOracleConn::new(conn))
}

/// 探活 SQL（Oracle 无 select 1，需 from dual）。
pub const ORACLE_PING_SQL: &str = "select 1 from dual";

/// 心跳探测：返回是否存活。
pub async fn ping_direct(cfg: Config, timeout: Duration) -> bool {
    match tokio::time::timeout(timeout, connect_direct(cfg)).await {
        Ok(Ok(mut conn)) => {
            let r = conn.query(ORACLE_PING_SQL).await;
            let ok = r.is_ok();
            drop(conn);
            ok
        }
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_jdbc_url_parsing() {
        let cfg = config_from_jdbc_url(
            "jdbc:oracle:thin:@//127.0.0.1:1521/FREEPDB1",
            "scott",
            "tiger",
        )
        .unwrap();
        // 校验内部连接串（Config 无公开字段，用 get_connect_descriptor 间接验证）
        let desc = cfg.get_connect_descriptor();
        assert!(desc.contains("127.0.0.1"), "{}", desc);
        assert!(desc.contains("FREEPDB1"), "{}", desc);
    }

    #[test]
    fn test_map_db_type() {
        use oracledb::*;
        assert_eq!(map_db_type(&DB_TYPE_NUMBER), field_type::NEWDECIMAL);
        assert_eq!(map_db_type(&DB_TYPE_DATE), field_type::DATETIME);
        assert_eq!(map_db_type(&DB_TYPE_BLOB), field_type::BLOB);
        assert_eq!(map_db_type(&DB_TYPE_VARCHAR), field_type::VAR_STRING);
    }

    #[test]
    fn test_timestamp_format() {
        use oracledb::OracleTimestamp;
        let ts = OracleTimestamp::new_timestamp(2026, 10, 17, 8, 30, 0, 0);
        assert_eq!(format_timestamp(&ts), "2026-10-17 08:30:00");
    }

    #[test]
    fn test_hex_encode() {
        assert_eq!(hex_encode(vec![0x01, 0xff]), "\\x01ff");
    }
}
