//! PostgreSQL 线协议前端（v3）。
//!
//! 对应 Java 版 `io.mycat.protocol.pg.PGProtocolHandler` + `PGResponse`，
//! 并在其基础上补齐 Java 版缺失的能力：
//! - Java 版扩展协议（Parse/Bind/Describe/Execute）是假 stub —— Pro 版真实实现
//!   （参数绑定 $n → 字面量替换 → 共享执行引擎，Describe 回 NoData、Execute 时发
//!   RowDescription，兼容 psql / pgjdbc / libpq 常规流程）
//! - Java 版所有列类型都是 TEXT(25) —— Pro 版按后端 MySQL 列类型映射 PG OID
//!   （int2/int4/int8/float4/float8/numeric/bool/date/time/timestamp/bytea/text）
//! - ReadyForQuery 事务状态字节真实反映会话事务状态（Java 恒为 'I'）
//! - MD5 挑战认证 + 空密码 trust（与 Java 一致）；SSLRequest 回 'N'（无 TLS，同 Java）
//!
//! 执行路径与 MySQL 前端完全一致：`engine::execute_sql` →
//! analyse → route → executor（对应 Java MycatProtocolBackend 门面）。

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use mycat3_protocol::consts::field_type;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tracing::{debug, info, warn};

use crate::engine::{
    execute_sql, execute_sql_streaming, EngineColumn, EngineError, EngineOutcome, EngineSession,
    EngineStreamOutcome,
};
use crate::session::SessionContext;
use crate::state::ServerState;

/// PG 类型 OID（完整常用集，未全部启用）。
#[allow(dead_code)]
mod oid {
    pub const BOOL: i32 = 16;
    pub const BYTEA: i32 = 17;
    pub const INT8: i32 = 20;
    pub const INT2: i32 = 21;
    pub const INT4: i32 = 23;
    pub const TEXT: i32 = 25;
    pub const OID_: i32 = 26;
    pub const FLOAT4: i32 = 700;
    pub const FLOAT8: i32 = 701;
    pub const VARCHAR: i32 = 1043;
    pub const DATE: i32 = 1082;
    pub const TIME: i32 = 1083;
    pub const TIMESTAMP: i32 = 1114;
    pub const NUMERIC: i32 = 1700;
}

/// 是否为真正的二进制列：charset=63(binary) 且列类型是字符串/BLOB 族。
/// 注意：MySQL 数值/时间列的 charset 也是 63，绝不能按 bytea 处理。
fn is_binary_column(col: &EngineColumn) -> bool {
    col.charset == 63
        && matches!(
            col.mysql_type,
            field_type::TINY_BLOB
                | field_type::MEDIUM_BLOB
                | field_type::LONG_BLOB
                | field_type::BLOB
                | field_type::VAR_STRING
                | field_type::STRING
                | field_type::GEOMETRY
                | field_type::BIT
        )
}

/// MySQL 列类型 → (PG OID, typlen)。
pub fn pg_type_of(col: &EngineColumn) -> (i32, i16) {
    if is_binary_column(col) {
        return (oid::BYTEA, -1);
    }
    match col.mysql_type {
        field_type::TINY => (oid::INT2, 2),
        field_type::SHORT | field_type::YEAR => (oid::INT2, 2),
        field_type::LONG | field_type::INT24 => (oid::INT4, 4),
        field_type::LONGLONG => (oid::INT8, 8),
        field_type::FLOAT => (oid::FLOAT4, 4),
        field_type::DOUBLE => (oid::FLOAT8, 8),
        field_type::DECIMAL | field_type::NEWDECIMAL => (oid::NUMERIC, -1),
        field_type::DATE | field_type::NEWDATE => (oid::DATE, 4),
        field_type::TIME => (oid::TIME, 8),
        field_type::DATETIME | field_type::TIMESTAMP => (oid::TIMESTAMP, 8),
        field_type::BIT => (oid::BOOL, 1),
        _ => (oid::TEXT, -1),
    }
}

/// 值编码：二进制列转 bytea 十六进制（\x..，查表零分配），其余文本原样。
fn encode_value(col: &EngineColumn, v: &str) -> String {
    if is_binary_column(col) {
        const HEX: &[u8; 16] = b"0123456789abcdef";
        let bytes = v.as_bytes();
        let mut s = String::with_capacity(bytes.len() * 2 + 2);
        s.push_str("\\x");
        for b in bytes {
            s.push(HEX[(b >> 4) as usize] as char);
            s.push(HEX[(b & 0x0f) as usize] as char);
        }
        s
    } else {
        v.to_string()
    }
}

// ---------------------------------------------------------------------------
// 消息构造
// ---------------------------------------------------------------------------

struct PgWriter {
    buf: Vec<u8>,
}

impl PgWriter {
    fn new() -> Self {
        PgWriter {
            buf: Vec::with_capacity(4096),
        }
    }

    /// 开始一条消息，返回写入点（用于回填长度）。
    fn begin(&mut self, tag: u8) -> usize {
        self.buf.push(tag);
        let pos = self.buf.len();
        self.buf.extend_from_slice(&[0, 0, 0, 0]);
        pos
    }

    fn end(&mut self, pos: usize) {
        let len = (self.buf.len() - pos) as u32;
        self.buf[pos..pos + 4].copy_from_slice(&len.to_be_bytes());
    }

    fn i16(&mut self, v: i16) {
        self.buf.extend_from_slice(&v.to_be_bytes());
    }
    fn i32(&mut self, v: i32) {
        self.buf.extend_from_slice(&v.to_be_bytes());
    }
    fn cstr(&mut self, s: &str) {
        self.buf.extend_from_slice(s.as_bytes());
        self.buf.push(0);
    }
    fn bytes(&mut self, b: &[u8]) {
        self.buf.extend_from_slice(b);
    }
    fn take(&mut self) -> Vec<u8> {
        std::mem::take(&mut self.buf)
    }
}

fn msg_authentication(kind: i32, extra: &[u8]) -> Vec<u8> {
    let mut w = PgWriter::new();
    let p = w.begin(b'R');
    w.i32(kind);
    w.bytes(extra);
    w.end(p);
    w.take()
}

fn msg_parameter_status(key: &str, value: &str) -> Vec<u8> {
    let mut w = PgWriter::new();
    let p = w.begin(b'S');
    w.cstr(key);
    w.cstr(value);
    w.end(p);
    w.take()
}

fn msg_backend_key(pid: i32, secret: i32) -> Vec<u8> {
    let mut w = PgWriter::new();
    let p = w.begin(b'K');
    w.i32(pid);
    w.i32(secret);
    w.end(p);
    w.take()
}

fn msg_ready(status: u8) -> Vec<u8> {
    let mut w = PgWriter::new();
    let p = w.begin(b'Z');
    w.bytes(&[status]);
    w.end(p);
    w.take()
}

fn msg_error(severity: &str, code: &str, message: &str) -> Vec<u8> {
    let mut w = PgWriter::new();
    let p = w.begin(b'E');
    w.bytes(b"S");
    w.cstr(severity);
    w.bytes(b"V");
    w.cstr(severity);
    w.bytes(b"C");
    w.cstr(code);
    w.bytes(b"M");
    w.cstr(message);
    w.bytes(&[0]); // 字段列表终止符（PG 协议要求）
    w.end(p);
    w.take()
}

fn msg_command_complete(tag: &str) -> Vec<u8> {
    let mut w = PgWriter::new();
    let p = w.begin(b'C');
    w.cstr(tag);
    w.end(p);
    w.take()
}

fn msg_simple(tag: u8) -> Vec<u8> {
    let mut w = PgWriter::new();
    let p = w.begin(tag);
    w.end(p);
    w.take()
}

fn msg_row_description(cols: &[EngineColumn]) -> Vec<u8> {
    let mut w = PgWriter::new();
    let p = w.begin(b'T');
    w.i16(cols.len() as i16);
    for c in cols {
        w.cstr(&c.name);
        w.i32(0); // table oid
        w.i16(0); // attnum
        let (oid, typlen) = pg_type_of(c);
        w.i32(oid);
        w.i16(typlen);
        w.i32(-1); // typmod
        w.i16(0); // format: text
    }
    w.end(p);
    w.take()
}

fn msg_data_row(cols: &[EngineColumn], row: &[Option<String>]) -> Vec<u8> {
    let mut w = PgWriter::new();
    let p = w.begin(b'D');
    w.i16(row.len() as i16);
    for (i, v) in row.iter().enumerate() {
        match v {
            None => w.i32(-1),
            Some(s) => {
                let encoded = if let Some(c) = cols.get(i) {
                    encode_value(c, s)
                } else {
                    s.clone()
                };
                w.i32(encoded.len() as i32);
                w.bytes(encoded.as_bytes());
            }
        }
    }
    w.end(p);
    w.take()
}

/// SQLSTATE：MySQL errno → PG 五位错误码（常用子集，其余回退 XX000）。
fn pg_sqlstate(errno: u16, sqlstate: &str) -> String {
    // MySQL 与 PG 的 SQLSTATE 大多同源于 SQL 标准，5 位码直接透传合法值；
    // HY000（MySQL 通用）与 42000（MySQL 语法/访问违例通用码，PG 中不精确）走 errno 映射
    if sqlstate.len() == 5
        && sqlstate.chars().all(|c| c.is_ascii_alphanumeric())
        && sqlstate != "HY000"
        && sqlstate != "42000"
    {
        return sqlstate.to_uppercase();
    }
    match errno {
        1064 => "42601".into(), // syntax_error
        1046 => "3D000".into(), // invalid_catalog_name
        1049 => "3D000".into(),
        1045 => "28P01".into(), // invalid_password
        1062 => "23505".into(), // unique_violation
        1146 => "42P01".into(), // undefined_table
        1235 => "0A000".into(), // feature_not_supported
        _ => "XX000".into(),    // internal_error
    }
}

// ---------------------------------------------------------------------------
// 连接处理
// ---------------------------------------------------------------------------

pub struct PgFrontend {
    socket: crate::session::ProtoStream,
    state: Arc<ServerState>,
    sess: EngineSession,
    /// 命名 prepared 语句（扩展协议）
    statements: HashMap<String, PgStatement>,
    /// 命名 portal（Bind 后的可执行体）
    portals: HashMap<String, String>,
    proc: Option<Arc<crate::state::ProcessInfo>>,
}

#[derive(Clone)]
struct PgStatement {
    sql: String,
    num_params: usize,
}

#[derive(Debug, thiserror::Error)]
enum PgError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("closed")]
    Closed,
    #[error("fatal: {0}")]
    Fatal(String),
}

/// PG 连接入口（prefeed 为嗅探阶段已消费字节）。
pub async fn serve_pg_connection(
    state: Arc<ServerState>,
    socket: TcpStream,
    prefeed: Vec<u8>,
    kill_handle: Option<std::net::TcpStream>,
) {
    let peer = socket
        .peer_addr()
        .map(|a| a.ip().to_string())
        .unwrap_or_default();
    let socket = crate::session::ProtoStream::Plain(socket);
    let conn_id = state.next_connection_id();
    state.stats.incr_conn();
    let ctx = SessionContext::new(conn_id, peer.clone());
    let mut fe = PgFrontend {
        socket,
        state: state.clone(),
        sess: EngineSession::new(ctx),
        statements: HashMap::new(),
        portals: HashMap::new(),
        proc: None,
    };
    info!(conn_id, peer = %peer, "postgresql client connected");
    let result = fe.run(prefeed, kill_handle).await;
    if let Err(e) = result {
        debug!("pg connection closed: {}", e);
    }
    state.processes.unregister(conn_id);
    if !fe.sess.ctx.user.is_empty() {
        state.decr_user_conn(&fe.sess.ctx.user);
    }
    fe.sess.close().await;
    state.stats.decr_conn();
}

impl PgFrontend {
    async fn run(
        &mut self,
        prefeed: Vec<u8>,
        kill_handle: Option<std::net::TcpStream>,
    ) -> Result<(), PgError> {
        // ---- Startup 阶段 ----
        let startup = self.read_startup(prefeed).await?;
        let (user, database) = parse_startup_params(&startup);

        // SSLRequest：配置了 TLS 证书 → 'S' + 升级；否则 'N'（与 Java 一致）
        let startup = if startup.len() == 8 && be_u32(&startup, 4) == 80877103 {
            let acceptor = self.state.tls_acceptor.read().clone();
            match acceptor {
                Some(acc) => {
                    self.socket.write_all(b"S").await?;
                    self.socket.flush().await?;
                    let plain = match std::mem::replace(
                        &mut self.socket,
                        crate::session::ProtoStream::Closed,
                    ) {
                        crate::session::ProtoStream::Plain(p) => p,
                        _ => return Err(PgError::Fatal("tls upgrade failed".into())),
                    };
                    let tls = acc
                        .accept(plain)
                        .await
                        .map_err(|e| PgError::Fatal(e.to_string()))?;
                    self.socket = crate::session::ProtoStream::Tls(Box::new(tls));
                    self.read_startup(Vec::new()).await?
                }
                None => {
                    self.socket.write_all(b"N").await?;
                    self.socket.flush().await?;
                    self.read_startup(Vec::new()).await?
                }
            }
        } else {
            startup
        };
        let (user, database) = {
            let (u, d) = parse_startup_params(&startup);
            (u.or(user), d.or(database))
        };

        // CancelRequest → 直接关闭
        if startup.len() >= 8 && be_u32(&startup, 4) == 80877102 {
            return Ok(());
        }

        let user = user.unwrap_or_else(|| "postgres".to_string());
        // 用户校验 + MD5 认证（对应 Java computePgMd5）
        let cfg = self.state.config();
        let user_cfg = cfg.users.get(&user).cloned();
        drop(cfg);
        let user_cfg = match user_cfg {
            Some(u) if u.ip_allowed(&self.sess.ctx.client_ip) => u,
            _ => {
                self.send(&msg_error(
                    "FATAL",
                    "28000",
                    &format!("no mycat user entry for \"{}\"", user),
                ))
                .await?;
                return Err(PgError::Fatal("auth failed".into()));
            }
        };
        self.sess.ctx.user = user.clone();
        if user_cfg.login_limit > 0
            && self.state.user_conn_count(&user) >= user_cfg.login_limit as u32
        {
            self.send(&msg_error(
                "FATAL",
                "53300",
                &format!("user {} exceeds login limit", user),
            ))
            .await?;
            return Err(PgError::Fatal("login limit".into()));
        }
        self.state.incr_user_conn(&user);
        self.sess.ctx.transaction_policy = user_cfg.transaction_type.clone();
        if let Some(db) = &database {
            if self.state.metadata().get_schema(db).is_some() {
                self.sess.ctx.schema = Some(db.clone());
            }
        }

        if !user_cfg.password.is_empty() {
            let salt: [u8; 4] = rand::random();
            self.send(&msg_authentication(5, &salt)).await?;
            // PasswordMessage 'p'
            let (tag, payload) = self.read_message().await?;
            if tag != b'p' {
                return Err(PgError::Fatal("expected PasswordMessage".into()));
            }
            let received = String::from_utf8_lossy(&payload)
                .trim_end_matches('\0')
                .to_string();
            let expected = compute_pg_md5(&user_cfg.decrypted_password(), &user, &salt);
            if received != expected {
                self.send(&msg_error(
                    "FATAL",
                    "28P01",
                    "password authentication failed",
                ))
                .await?;
                warn!(user = %user, "pg auth failed");
                return Err(PgError::Fatal("auth failed".into()));
            }
        }

        // 认证成功：AuthenticationOk + ParameterStatus* + BackendKeyData + ReadyForQuery
        let mut hello = msg_authentication(0, &[]);
        let version = self.state.config().server.server.server_version.clone();
        hello.extend(msg_parameter_status(
            "server_version",
            &format!("14.0 ({})", version),
        ));
        hello.extend(msg_parameter_status("server_encoding", "UTF8"));
        hello.extend(msg_parameter_status("client_encoding", "UTF8"));
        hello.extend(msg_parameter_status("DateStyle", "ISO, MDY"));
        hello.extend(msg_parameter_status("integer_datetimes", "on"));
        hello.extend(msg_parameter_status("standard_conforming_strings", "on"));
        hello.extend(msg_parameter_status("TimeZone", "UTC"));
        hello.extend(msg_backend_key(
            self.sess.ctx.connection_id as i32,
            rand::random(),
        ));
        hello.extend(msg_ready(self.tx_status()));
        self.send(&hello).await?;
        info!(user = %user, database = ?database, "postgresql client authenticated");
        let proc = self.state.processes.register(
            self.sess.ctx.connection_id,
            "PostgreSQL",
            self.sess.ctx.client_ip.clone(),
            kill_handle,
        );
        proc.set_user(user.clone());
        proc.set_db(self.sess.ctx.schema.clone());
        self.proc = Some(proc);

        // ---- 消息循环 ----
        loop {
            let (tag, payload) = self.read_message().await?;
            // pauseServer 挂起：排队等待 resumeServer
            crate::state::wait_if_paused(self.proc.as_deref()).await;
            let started = Instant::now();
            match tag {
                b'Q' => {
                    let sql = cstring(&payload);
                    if let Some(proc) = &self.proc {
                        proc.set_command("Query");
                        proc.set_sql(Some(sql.chars().take(1024).collect()));
                        proc.set_db(self.sess.ctx.schema.clone());
                    }
                    self.handle_simple_query(&sql).await?;
                }
                b'P' => self.handle_parse(&payload).await?,
                b'B' => {
                    self.handle_bind(&payload)?;
                    self.send(&msg_simple(b'2')).await?; // BindComplete
                }
                b'D' => self.handle_describe(&payload).await?,
                b'E' => self.handle_execute(&payload).await?,
                b'C' => {
                    self.handle_close(&payload).await?;
                }
                b'S' => {
                    // Sync
                    self.send(&msg_ready(self.tx_status())).await?;
                }
                b'H' => {
                    self.socket.flush().await?;
                }
                b'X' => return Ok(()),
                other => {
                    warn!("pg: unsupported message type '{}'", other as char);
                    self.send(&msg_error(
                        "ERROR",
                        "08P01",
                        &format!("unsupported message type '{}'", other as char),
                    ))
                    .await?;
                    self.send(&msg_ready(self.tx_status())).await?;
                }
            }
            if started.elapsed().as_millis() > 1000 {
                self.state.stats.incr_slow();
            }
        }
    }

    fn tx_status(&self) -> u8 {
        if self.sess.ctx.in_transaction || self.sess.tx.explicit_tx || !self.sess.ctx.autocommit {
            b'T'
        } else {
            b'I'
        }
    }

    // ---- 简单查询 ----
    /// PG 文本级 PREPARE/EXECUTE/DEALLOCATE（返回 true = 已处理）。
    /// PG 文本级 PREPARE/EXECUTE/DEALLOCATE（Some(wire) = 已处理）。
    async fn handle_pg_text_prepare(&mut self, sql: &str) -> Result<Option<Vec<u8>>, PgError> {
        let lower = sql.trim().to_ascii_lowercase();
        // PREPARE n [(types)] AS stmt
        if lower.starts_with("prepare ") {
            let Some((name, template)) = crate::prepared_text::parse_pg_prepare(sql) else {
                return Ok(None);
            };
            let params = count_pg_placeholders(&template);
            self.sess.ctx.text_prepares.insert(
                name.clone(),
                crate::prepared_text::TextPrepared {
                    sql: template,
                    params,
                },
            );
            return Ok(Some(msg_command_complete("PREPARE")));
        }
        // EXECUTE n(args)
        if lower.starts_with("execute ") {
            let Some((name, args)) = crate::prepared_text::parse_pg_execute(sql) else {
                return Ok(None);
            };
            let Some(stmt) = self.sess.ctx.text_prepares.get(&name).cloned() else {
                return Ok(Some(self.encode_error(&EngineError::new(
                    26000,
                    "26000",
                    format!("prepared statement \"{}\" does not exist", name),
                ))));
            };
            if args.len() != stmt.params {
                return Ok(Some(self.encode_error(&EngineError::new(
                    08,
                    "08P01",
                    format!(
                        "wrong number of parameters for prepared statement \"{}\" (expected {}, got {})",
                        name,
                        stmt.params,
                        args.len()
                    ),
                ))));
            }
            let Some(final_sql) = crate::prepared_text::substitute_dollar_params(&stmt.sql, &args)
            else {
                return Ok(Some(self.encode_error(&EngineError::new(
                    08,
                    "08P01",
                    "参数替换失败",
                ))));
            };
            // 替换产物走正常单语句执行（Box 打断异步递归）
            return Box::pin(self.run_simple_statement(&final_sql))
                .await
                .map(Some);
        }
        // DEALLOCATE n | ALL
        if lower.starts_with("deallocate ") {
            if let Some(target) = crate::prepared_text::parse_pg_deallocate(sql) {
                match target {
                    Some(name) => {
                        self.sess.ctx.text_prepares.remove(&name);
                    }
                    None => {
                        self.sess.ctx.text_prepares.clear();
                    }
                }
                return Ok(Some(msg_command_complete("DEALLOCATE")));
            }
        }
        Ok(None)
    }

    async fn handle_simple_query(&mut self, sql: &str) -> Result<(), PgError> {
        // 简单查询可携带多语句（psql -c "a; b; c" 整体一条 'Q'）：按顶层分号拆分
        // 逐条执行，末尾统一一次 ReadyForQuery。
        // 单条语句优先走流式执行（SQL Server 单分片读）；其余保持 buffer-first。
        let stmts = crate::prepared_text::split_statements(sql);
        if stmts.len() == 1 {
            return self.handle_single_simple(&stmts[0]).await;
        }
        let mut wire = Vec::new();
        for stmt in stmts {
            wire.extend(self.run_simple_statement(&stmt).await?);
        }
        wire.extend(msg_ready(self.tx_status()));
        self.send(&wire).await?;
        Ok(())
    }

    async fn handle_single_simple(&mut self, sql: &str) -> Result<(), PgError> {
        if let Some(w) = self.handle_pg_text_prepare(sql).await? {
            let mut wire = w;
            wire.extend(msg_ready(self.tx_status()));
            self.send(&wire).await?;
            return Ok(());
        }
        // 写语句继续走原有执行路径（保留序列填充/事务收尾语义）；只有
        // 只读语句尝试流式。
        let first = sql
            .trim_start()
            .split(|c: char| c.is_whitespace() || c == '(')
            .next()
            .unwrap_or("")
            .to_ascii_uppercase();
        if !matches!(first.as_str(), "SELECT" | "VALUES") {
            let mut wire = self.run_simple_statement(sql).await?;
            wire.extend(msg_ready(self.tx_status()));
            self.send(&wire).await?;
            return Ok(());
        }
        match execute_sql_streaming(&self.state, &mut self.sess, sql).await {
            Ok(EngineStreamOutcome::Outcomes(outcomes)) => {
                let mut wire = Vec::new();
                for outcome in &outcomes {
                    wire.extend(self.encode_outcome(outcome, sql));
                }
                wire.extend(msg_ready(self.tx_status()));
                self.send(&wire).await?;
                Ok(())
            }
            Ok(EngineStreamOutcome::Stream(mut stream)) => {
                self.send(&msg_row_description(&stream.columns)).await?;
                let mut batch = Vec::new();
                loop {
                    match stream.next_row().await {
                        Ok(Some(row)) => {
                            batch.extend(msg_data_row(&stream.columns, &row));
                            if batch.len() >= 64 * 1024 {
                                self.send(&batch).await?;
                                batch.clear();
                            }
                        }
                        Ok(None) => break,
                        Err(e) => {
                            self.state.stats.incr_error();
                            if !batch.is_empty() {
                                self.send(&batch).await?;
                            }
                            let err = self.encode_error(&e);
                            self.send(&err).await?;
                            self.send(&msg_ready(self.tx_status())).await?;
                            return Ok(());
                        }
                    }
                }
                batch.extend(msg_command_complete(&select_tag_rows(
                    stream.rows_read as usize,
                )));
                batch.extend(msg_ready(self.tx_status()));
                self.send(&batch).await?;
                Ok(())
            }
            Err(e) => {
                self.state.stats.incr_error();
                let err = self.encode_error(&e);
                self.send(&err).await?;
                self.send(&msg_ready(self.tx_status())).await?;
                Ok(())
            }
        }
    }

    /// 单条简单语句执行（不含 ReadyForQuery）。
    async fn run_simple_statement(&mut self, sql: &str) -> Result<Vec<u8>, PgError> {
        // 文本级 PREPARE（PG 方言）：会话本地语句表，不下发后端
        if let Some(w) = self.handle_pg_text_prepare(sql).await? {
            return Ok(w);
        }
        match execute_sql(&self.state, &mut self.sess, sql).await {
            Ok(outcomes) => {
                let mut wire = Vec::new();
                for outcome in outcomes {
                    wire.extend(self.encode_outcome(&outcome, sql));
                }
                Ok(wire)
            }
            Err(e) => {
                self.state.stats.incr_error();
                Ok(self.encode_error(&e))
            }
        }
    }

    fn encode_outcome(&self, outcome: &EngineOutcome, sql: &str) -> Vec<u8> {
        let mut wire = Vec::new();
        match outcome {
            EngineOutcome::Rows { columns, rows } => {
                wire.extend(msg_row_description(columns));
                for row in rows {
                    wire.extend(msg_data_row(columns, row));
                }
                wire.extend(msg_command_complete(&select_tag_rows(rows.len())));
            }
            EngineOutcome::Ok { affected_rows, .. } => {
                wire.extend(msg_command_complete(&dml_tag(sql, *affected_rows)));
            }
        }
        wire
    }

    fn encode_error(&self, e: &EngineError) -> Vec<u8> {
        msg_error("ERROR", &pg_sqlstate(e.errno, &e.sqlstate), &e.message)
    }

    // ---- 扩展协议 ----
    async fn handle_parse(&mut self, payload: &[u8]) -> Result<(), PgError> {
        let mut r = Cursor::new(payload);
        let name = r.cstr()?;
        let query = r.cstr()?;
        let ntypes = r.i16()?;
        for _ in 0..ntypes {
            r.i32()?;
        }
        let num_params = count_pg_placeholders(&query);
        self.statements.insert(
            name,
            PgStatement {
                sql: query,
                num_params,
            },
        );
        self.send(&msg_simple(b'1')).await?; // ParseComplete
        Ok(())
    }

    fn handle_bind(&mut self, payload: &[u8]) -> Result<(), PgError> {
        let mut r = Cursor::new(payload);
        let portal = r.cstr()?;
        let stmt_name = r.cstr()?;
        let stmt = self
            .statements
            .get(&stmt_name)
            .cloned()
            .map(|s| (s.sql, s.num_params))
            .ok_or_else(|| PgError::Fatal(format!("unknown statement '{}'", stmt_name)))?;
        // 参数格式码
        let nfmt = r.i16()? as usize;
        let mut formats = Vec::with_capacity(nfmt);
        for _ in 0..nfmt {
            formats.push(r.i16()?);
        }
        // 参数值
        let nparams = r.i16()? as usize;
        let mut params: Vec<Option<Vec<u8>>> = Vec::with_capacity(nparams);
        for i in 0..nparams {
            let len = r.i32()?;
            if len < 0 {
                params.push(None);
            } else {
                params.push(Some(r.fixed(len as usize)?.to_vec()));
            }
            let _ = i;
        }
        // 结果格式码（忽略：一律文本）
        // 参数替换：二进制格式参数按常见类型解码，其余按 UTF-8 文本
        // （PG 规则：格式码列表长度为 1 时适用于所有参数）
        let text_params: Vec<Option<String>> = params
            .iter()
            .enumerate()
            .map(|(i, p)| {
                p.as_ref().map(|bytes| {
                    let fmt = if formats.len() == 1 {
                        formats[0]
                    } else {
                        *formats.get(i).unwrap_or(&0)
                    };
                    if fmt == 1 {
                        decode_binary_param(bytes)
                    } else {
                        String::from_utf8_lossy(bytes).into_owned()
                    }
                })
            })
            .collect();
        let _ = nfmt;
        let sql = substitute_pg_params(&stmt.0, &text_params);
        self.portals.insert(portal, sql);
        Ok(())
    }

    async fn handle_describe(&mut self, payload: &[u8]) -> Result<(), PgError> {
        let kind = *payload.first().unwrap_or(&b'P');
        let mut r = Cursor::new(&payload[1..]);
        let name = r.cstr()?;
        let mut wire = Vec::new();
        match kind {
            b'S' => {
                // ParameterDescription + NoData（列元数据在 Execute 阶段随 'T' 下发）
                let n = self
                    .statements
                    .get(&name)
                    .map(|s| s.num_params)
                    .unwrap_or(0) as i16;
                let mut w = PgWriter::new();
                let p = w.begin(b't');
                w.i16(n);
                for _ in 0..n {
                    w.i32(oid::TEXT);
                }
                w.end(p);
                wire.extend(w.take());
                wire.extend(msg_simple(b'n')); // NoData
            }
            _ => {
                let _ = name;
                wire.extend(msg_simple(b'n')); // NoData（portal）
            }
        }
        self.send(&wire).await?;
        Ok(())
    }

    async fn handle_execute(&mut self, payload: &[u8]) -> Result<(), PgError> {
        let mut r = Cursor::new(payload);
        let portal = r.cstr()?;
        let _max_rows = r.i32()?; // >0 的 portal 挂起不实现：一次性返回全部行
        let sql = match self.portals.get(&portal) {
            Some(s) => s.clone(),
            None => {
                self.send(&msg_error(
                    "ERROR",
                    "34000",
                    &format!("portal '{}' does not exist", portal),
                ))
                .await?;
                return Ok(());
            }
        };
        match execute_sql(&self.state, &mut self.sess, &sql).await {
            Ok(outcomes) => {
                let mut wire = Vec::new();
                for outcome in outcomes {
                    wire.extend(self.encode_outcome(&outcome, &sql));
                }
                self.send(&wire).await?;
            }
            Err(e) => {
                self.state.stats.incr_error();
                self.send(&self.encode_error(&e)).await?;
            }
        }
        Ok(())
    }

    async fn handle_close(&mut self, payload: &[u8]) -> Result<(), PgError> {
        let kind = *payload.first().unwrap_or(&b'P');
        let name = cstring(&payload[1..]);
        match kind {
            b'S' => {
                self.statements.remove(&name);
            }
            _ => {
                self.portals.remove(&name);
            }
        }
        self.send(&msg_simple(b'3')).await?; // CloseComplete
        Ok(())
    }

    // ---- IO ----
    async fn read_startup(&mut self, mut prefeed: Vec<u8>) -> Result<Vec<u8>, PgError> {
        // StartupMessage: int32 长度（含自身）+ 内容
        loop {
            if prefeed.len() >= 4 {
                let len = be_u32(&prefeed, 0) as usize;
                if len < 8 || len > 1024 * 1024 {
                    return Err(PgError::Fatal("bad startup length".into()));
                }
                if prefeed.len() >= len {
                    return Ok(prefeed[..len].to_vec());
                }
            }
            let mut tmp = [0u8; 4096];
            let n = self.socket.read(&mut tmp).await?;
            if n == 0 {
                return Err(PgError::Closed);
            }
            self.state.stats.add_in(n as u64);
            prefeed.extend_from_slice(&tmp[..n]);
        }
    }

    async fn read_message(&mut self) -> Result<(u8, Vec<u8>), PgError> {
        let mut head = [0u8; 5];
        read_exact(&mut self.socket, &mut head).await?;
        let tag = head[0];
        let len = u32::from_be_bytes([head[1], head[2], head[3], head[4]]) as usize;
        if len < 4 || len > 64 * 1024 * 1024 {
            return Err(PgError::Fatal(format!("bad message length {}", len)));
        }
        let mut payload = vec![0u8; len - 4];
        read_exact(&mut self.socket, &mut payload).await?;
        self.state.stats.add_in((len + 1) as u64);
        Ok((tag, payload))
    }

    async fn send(&mut self, bytes: &[u8]) -> Result<(), PgError> {
        self.socket.write_all(bytes).await?;
        self.socket.flush().await?;
        self.state.stats.add_out(bytes.len() as u64);
        Ok(())
    }
}

async fn read_exact<S: tokio::io::AsyncRead + Unpin>(
    socket: &mut S,
    buf: &mut [u8],
) -> Result<(), PgError> {
    let mut off = 0;
    while off < buf.len() {
        let n = socket.read(&mut buf[off..]).await?;
        if n == 0 {
            return Err(PgError::Closed);
        }
        off += n;
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// 解析辅助
// ---------------------------------------------------------------------------

fn be_u32(b: &[u8], off: usize) -> u32 {
    u32::from_be_bytes([b[off], b[off + 1], b[off + 2], b[off + 3]])
}

struct Cursor<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    fn new(buf: &'a [u8]) -> Self {
        Cursor { buf, pos: 0 }
    }
    fn i16(&mut self) -> Result<i16, PgError> {
        if self.pos + 2 > self.buf.len() {
            return Err(PgError::Fatal("truncated int16".into()));
        }
        let v = i16::from_be_bytes([self.buf[self.pos], self.buf[self.pos + 1]]);
        self.pos += 2;
        Ok(v)
    }
    fn i32(&mut self) -> Result<i32, PgError> {
        if self.pos + 4 > self.buf.len() {
            return Err(PgError::Fatal("truncated int32".into()));
        }
        let v = i32::from_be_bytes([
            self.buf[self.pos],
            self.buf[self.pos + 1],
            self.buf[self.pos + 2],
            self.buf[self.pos + 3],
        ]);
        self.pos += 4;
        Ok(v)
    }
    fn cstr(&mut self) -> Result<String, PgError> {
        let start = self.pos;
        while self.pos < self.buf.len() && self.buf[self.pos] != 0 {
            self.pos += 1;
        }
        if self.pos >= self.buf.len() {
            return Err(PgError::Fatal("truncated cstring".into()));
        }
        let s = String::from_utf8_lossy(&self.buf[start..self.pos]).into_owned();
        self.pos += 1;
        Ok(s)
    }
    fn fixed(&mut self, n: usize) -> Result<&'a [u8], PgError> {
        if self.pos + n > self.buf.len() {
            return Err(PgError::Fatal("truncated fixed".into()));
        }
        let v = &self.buf[self.pos..self.pos + n];
        self.pos += n;
        Ok(v)
    }
}

fn cstring(b: &[u8]) -> String {
    let end = b.iter().position(|x| *x == 0).unwrap_or(b.len());
    String::from_utf8_lossy(&b[..end]).into_owned()
}

/// 解析 StartupMessage 参数（NUL 分隔 kv 对）。
pub fn parse_startup_params(msg: &[u8]) -> (Option<String>, Option<String>) {
    if msg.len() < 8 {
        return (None, None);
    }
    let body = &msg[8..];
    let parts: Vec<&[u8]> = body.split(|b| *b == 0).collect();
    let mut user = None;
    let mut database = None;
    let mut i = 0;
    while i + 1 < parts.len() {
        let k = String::from_utf8_lossy(parts[i]);
        let v = String::from_utf8_lossy(parts[i + 1]).into_owned();
        if k.is_empty() {
            break;
        }
        match k.as_ref() {
            "user" => user = Some(v),
            "database" => database = Some(v),
            _ => {}
        }
        i += 2;
    }
    (user, database)
}

/// PG MD5 认证：`"md5" + hex(md5(hex(md5(password + username)) + salt))`。
/// 对应 Java `PGProtocolHandler.computePgMd5`。
pub fn compute_pg_md5(password: &str, username: &str, salt: &[u8; 4]) -> String {
    let inner = format!("{:x}", md5::compute(format!("{}{}", password, username)));
    let mut combined = inner.into_bytes();
    combined.extend_from_slice(salt);
    format!("md5{:x}", md5::compute(&combined))
}

#[allow(dead_code)]
fn hex(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push_str(&format!("{:02x}", b));
    }
    s
}

/// 统计 SQL 中 `$n` 占位符最大编号（跳过引号内）。
pub fn count_pg_placeholders(sql: &str) -> usize {
    let mut max = 0usize;
    let bytes = sql.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'\'' | b'"' => {
                let q = bytes[i];
                i += 1;
                while i < bytes.len() {
                    if bytes[i] == b'\\' {
                        i += 2;
                        continue;
                    }
                    if bytes[i] == q {
                        if i + 1 < bytes.len() && bytes[i + 1] == q {
                            i += 2;
                            continue;
                        }
                        break;
                    }
                    i += 1;
                }
            }
            b'$' if i + 1 < bytes.len() && bytes[i + 1].is_ascii_digit() => {
                let start = i + 1;
                let mut j = start;
                while j < bytes.len() && bytes[j].is_ascii_digit() {
                    j += 1;
                }
                if let Ok(n) = sql[start..j].parse::<usize>() {
                    max = max.max(n);
                }
                i = j - 1;
            }
            _ => {}
        }
        i += 1;
    }
    max
}

/// 将 `$n` 替换为字面量（数字裸写、字符串加引号转义、NULL）。
pub fn substitute_pg_params(sql: &str, params: &[Option<String>]) -> String {
    let mut out = String::with_capacity(sql.len() + 32);
    let bytes = sql.as_bytes();
    let mut i = 0;
    let mut seg = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'\'' | b'"' => {
                let q = bytes[i];
                i += 1;
                while i < bytes.len() {
                    if bytes[i] == b'\\' {
                        i += 2;
                        continue;
                    }
                    if bytes[i] == q {
                        if i + 1 < bytes.len() && bytes[i + 1] == q {
                            i += 2;
                            continue;
                        }
                        break;
                    }
                    i += 1;
                }
                i = (i + 1).min(bytes.len());
            }
            b'$' if i + 1 < bytes.len() && bytes[i + 1].is_ascii_digit() => {
                let start = i + 1;
                let mut j = start;
                while j < bytes.len() && bytes[j].is_ascii_digit() {
                    j += 1;
                }
                let n: usize = sql[start..j].parse().unwrap_or(0);
                out.push_str(&sql[seg..i]);
                match params.get(n.saturating_sub(1)).cloned().flatten() {
                    Some(v) => out.push_str(&quote_pg_literal(&v)),
                    None => out.push_str("NULL"),
                }
                i = j;
                seg = i;
            }
            _ => i += 1,
        }
    }
    out.push_str(&sql[seg..]);
    out
}

fn quote_pg_literal(v: &str) -> String {
    if !v.is_empty()
        && v.bytes().all(|b| {
            b.is_ascii_digit() || b == b'.' || b == b'-' || b == b'+' || b == b'e' || b == b'E'
        })
        && v.bytes()
            .next()
            .map(|b| b.is_ascii_digit() || b == b'-' || b == b'+')
            .unwrap_or(false)
    {
        return v.to_string();
    }
    let mut s = String::with_capacity(v.len() + 2);
    s.push('\'');
    for ch in v.chars() {
        if ch == '\'' {
            s.push('\'');
        }
        s.push(ch);
    }
    s.push('\'');
    s
}

/// 二进制格式参数的保守解码（常见固定长类型；未知按 UTF-8 文本）。
fn decode_binary_param(bytes: &[u8]) -> String {
    match bytes.len() {
        1 => (bytes[0] != 0).to_string(), // bool（也可能是 int2 截断，PG 端 bool 最常见）
        2 => (i16::from_be_bytes([bytes[0], bytes[1]])).to_string(),
        4 => {
            // int4 与 float4 无法区分：按 int4（更常见）；驱动多用文本格式规避
            (i32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]])).to_string()
        }
        8 => (i64::from_be_bytes([
            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
        ]))
        .to_string(),
        _ => String::from_utf8_lossy(bytes).into_owned(),
    }
}

fn dml_tag(sql: &str, affected: u64) -> String {
    let head = sql
        .trim_start()
        .split_whitespace()
        .next()
        .unwrap_or("")
        .to_uppercase();
    match head.as_str() {
        "INSERT" => format!("INSERT 0 {}", affected),
        "UPDATE" => format!("UPDATE {}", affected),
        "DELETE" => format!("DELETE {}", affected),
        "BEGIN" | "START" => "BEGIN".into(),
        "COMMIT" | "END" => "COMMIT".into(),
        "ROLLBACK" => "ROLLBACK".into(),
        "CREATE" => "CREATE TABLE".into(),
        "DROP" => "DROP TABLE".into(),
        "ALTER" => "ALTER TABLE".into(),
        "TRUNCATE" => "TRUNCATE TABLE".into(),
        "SET" => "SET".into(),
        "USE" => "SET".into(),
        "SHOW" | "EXPLAIN" | "DESC" | "DESCRIBE" => format!("SELECT {}", affected),
        _ => format!("OK {}", affected),
    }
}

/// CommandComplete 标签（对应 Java commandTagFor 的 SELECT 分支）。
fn select_tag_rows(rows: usize) -> String {
    format!("SELECT {}", rows)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pg_md5_known_vector() {
        // 手工按算法验证一致性
        let salt = [1u8, 2, 3, 4];
        let got = compute_pg_md5("secret", "postgres", &salt);
        // 独立重算
        let inner = format!("{:x}", md5::compute(b"secretpostgres"));
        let mut combined = inner.into_bytes();
        combined.extend_from_slice(&salt);
        let expect = format!("md5{:x}", md5::compute(&combined));
        assert_eq!(got, expect);
        assert!(got.starts_with("md5") && got.len() == 35);
    }

    #[test]
    fn test_parse_startup_params() {
        let mut msg = Vec::new();
        msg.extend_from_slice(&0u32.to_be_bytes()); // len 占位
        msg.extend_from_slice(&196608u32.to_be_bytes());
        msg.extend_from_slice(b"user\0postgres\0database\0db1\0application_name\0psql\0\0");
        let (u, d) = parse_startup_params(&msg);
        assert_eq!(u.as_deref(), Some("postgres"));
        assert_eq!(d.as_deref(), Some("db1"));
    }

    #[test]
    fn test_count_placeholders() {
        assert_eq!(
            count_pg_placeholders("select * from t where a=$1 and b=$2"),
            2
        );
        assert_eq!(count_pg_placeholders("select '$1' , $3"), 3);
        assert_eq!(count_pg_placeholders("select 1"), 0);
        assert_eq!(count_pg_placeholders("insert into t values ($1,$2,$1)"), 2);
    }

    #[test]
    fn test_substitute_params() {
        assert_eq!(
            substitute_pg_params(
                "select * from t where id=$1 and name=$2",
                &[Some("5".into()), Some("o'brien".into())]
            ),
            "select * from t where id=5 and name='o''brien'"
        );
        assert_eq!(substitute_pg_params("select $1", &[None]), "select NULL");
        // 引号内 $1 不替换
        assert_eq!(
            substitute_pg_params("select '$1', $1", &[Some("x".into())]),
            "select '$1', 'x'"
        );
        // 重复引用
        assert_eq!(
            substitute_pg_params("select $1, $1", &[Some("a".into())]),
            "select 'a', 'a'"
        );
    }

    #[test]
    fn test_pg_type_mapping() {
        let c = EngineColumn {
            name: "a".into(),
            mysql_type: field_type::LONGLONG,
            charset: 33,
        };
        assert_eq!(pg_type_of(&c), (oid::INT8, 8));
        let c = EngineColumn {
            name: "a".into(),
            mysql_type: field_type::VAR_STRING,
            charset: 63,
        };
        assert_eq!(pg_type_of(&c).0, oid::BYTEA); // binary → bytea
        let c = EngineColumn {
            name: "a".into(),
            mysql_type: field_type::DATETIME,
            charset: 33,
        };
        assert_eq!(pg_type_of(&c).0, oid::TIMESTAMP);
        let c = EngineColumn {
            name: "a".into(),
            mysql_type: field_type::NEWDECIMAL,
            charset: 33,
        };
        assert_eq!(pg_type_of(&c).0, oid::NUMERIC);
    }

    #[test]
    fn test_encode_value_bytea() {
        let c = EngineColumn {
            name: "b".into(),
            mysql_type: field_type::BLOB,
            charset: 63,
        };
        assert_eq!(encode_value(&c, "\u{1}\u{ff}"), "\\x01c3bf"); // UTF-8 字节
        let c2 = EngineColumn {
            name: "t".into(),
            mysql_type: field_type::VAR_STRING,
            charset: 33,
        };
        assert_eq!(encode_value(&c2, "hello"), "hello");
    }

    #[test]
    fn test_row_description_wire() {
        let cols = vec![
            EngineColumn {
                name: "id".into(),
                mysql_type: field_type::LONGLONG,
                charset: 33,
            },
            EngineColumn {
                name: "name".into(),
                mysql_type: field_type::VAR_STRING,
                charset: 33,
            },
        ];
        let msg = msg_row_description(&cols);
        assert_eq!(msg[0], b'T');
        let len = u32::from_be_bytes([msg[1], msg[2], msg[3], msg[4]]) as usize;
        assert_eq!(len, msg.len() - 1);
        // 列数
        assert_eq!(&msg[5..7], &(2i16).to_be_bytes());
    }

    #[test]
    fn test_data_row_null_and_value() {
        let cols = vec![EngineColumn {
            name: "a".into(),
            mysql_type: field_type::VAR_STRING,
            charset: 33,
        }];
        let msg = msg_data_row(&cols, &[None]);
        assert_eq!(msg[0], b'D');
        assert_eq!(&msg[7..11], &(-1i32).to_be_bytes()); // NULL → len -1
        let msg = msg_data_row(&cols, &[Some("hi".into())]);
        assert_eq!(&msg[7..11], &(2i32).to_be_bytes());
        assert_eq!(&msg[11..13], b"hi");
    }

    #[test]
    fn test_error_message_wire() {
        let m = msg_error("ERROR", "42601", "syntax error");
        assert_eq!(m[0], b'E');
        // 长度字段与内容一致
        let len = u32::from_be_bytes([m[1], m[2], m[3], m[4]]) as usize;
        assert_eq!(len, m.len() - 1);
        // 以字段终止 NUL 结束
        assert_eq!(*m.last().unwrap(), 0);
        let body = String::from_utf8_lossy(&m[5..]);
        assert!(body.contains("42601") && body.contains("syntax error"));
    }

    #[test]
    fn test_sqlstate_mapping() {
        assert_eq!(pg_sqlstate(1064, "42000"), "42601"); // 语法错误 → PG syntax_error
        assert_eq!(pg_sqlstate(1105, "HY000"), "XX000");
        assert_eq!(pg_sqlstate(1235, "42000"), "0A000"); // 不支持 → feature_not_supported
        assert_eq!(pg_sqlstate(1062, "23000"), "23000"); // 标准码透传
        assert_eq!(pg_sqlstate(0, ""), "XX000");
    }

    #[test]
    fn test_dml_tags() {
        assert_eq!(dml_tag("insert into t values(1)", 1), "INSERT 0 1");
        assert_eq!(dml_tag("UPDATE t SET a=1", 3), "UPDATE 3");
        assert_eq!(dml_tag("commit", 0), "COMMIT");
        assert_eq!(select_tag_rows(7), "SELECT 7");
    }

    #[test]
    fn test_decode_binary_param() {
        assert_eq!(decode_binary_param(&[0, 0, 0, 42]), "42");
        assert_eq!(decode_binary_param(&[0, 5]), "5");
        assert_eq!(decode_binary_param(&[1]), "true");
    }

    // ---------- 端到端（本地命令，无后端依赖） ----------

    use crate::state::ServerState;
    use mycat3_config::default_config;
    use tokio::net::TcpListener;

    async fn spawn_pg_server() -> u16 {
        let state = ServerState::new(default_config()).unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            loop {
                let (sock, _) = listener.accept().await.unwrap();
                let st = state.clone();
                tokio::spawn(async move {
                    serve_pg_connection(st, sock, Vec::new(), None).await;
                });
            }
        });
        port
    }

    fn startup_msg(user: &str, db: &str) -> Vec<u8> {
        let mut payload = Vec::new();
        payload.extend_from_slice(&196608u32.to_be_bytes());
        payload.extend_from_slice(format!("user\0{}\0database\0{}\0\0", user, db).as_bytes());
        let mut msg = ((payload.len() + 4) as u32).to_be_bytes().to_vec();
        msg.extend_from_slice(&payload);
        msg
    }

    async fn read_msg(sock: &mut TcpStream) -> (u8, Vec<u8>) {
        let mut head = [0u8; 5];
        tokio::io::AsyncReadExt::read_exact(sock, &mut head)
            .await
            .unwrap();
        let len = u32::from_be_bytes([head[1], head[2], head[3], head[4]]) as usize;
        let mut payload = vec![0u8; len - 4];
        if !payload.is_empty() {
            tokio::io::AsyncReadExt::read_exact(sock, &mut payload)
                .await
                .unwrap();
        }
        (head[0], payload)
    }

    fn query_msg(sql: &str) -> Vec<u8> {
        let mut w = PgWriter::new();
        let p = w.begin(b'Q');
        w.cstr(sql);
        w.end(p);
        w.take()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_pg_e2e_trust_auth_and_query() {
        // default_config 的 root 密码为空 → trust 认证
        let port = spawn_pg_server().await;
        let mut sock = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
        sock.write_all(&startup_msg("root", "db1")).await.unwrap();
        // AuthenticationOk
        let (tag, payload) = read_msg(&mut sock).await;
        assert_eq!(tag, b'R');
        assert_eq!(be_u32(&payload, 0), 0);
        // ParameterStatus 序列 + BackendKeyData + ReadyForQuery
        let mut ready = None;
        for _ in 0..20 {
            let (tag, payload) = read_msg(&mut sock).await;
            if tag == b'Z' {
                ready = Some(payload[0]);
                break;
            }
            assert!(tag == b'S' || tag == b'K', "unexpected tag {}", tag as char);
        }
        assert_eq!(ready, Some(b'I'));

        // 简单查询：show databases（本地）
        sock.write_all(&query_msg("show databases")).await.unwrap();
        let mut saw_t = false;
        let mut rows = 0;
        loop {
            let (tag, payload) = read_msg(&mut sock).await;
            match tag {
                b'T' => {
                    saw_t = true;
                    assert_eq!(i16::from_be_bytes([payload[0], payload[1]]), 1);
                }
                b'D' => rows += 1,
                b'C' => {
                    assert!(
                        cstring(&payload).starts_with("SELECT"),
                        "tag={}",
                        cstring(&payload)
                    );
                }
                b'Z' => break,
                b'E' => panic!("error: {}", String::from_utf8_lossy(&payload)),
                _ => {}
            }
        }
        assert!(saw_t && rows >= 1);

        // 扩展协议：Parse/Bind/Describe/Execute/Sync（本地探针 select @@version）
        let mut w = PgWriter::new();
        let p = w.begin(b'P');
        w.cstr(""); // unnamed statement
        w.cstr("select @@version");
        w.i16(0);
        w.end(p);
        let p = w.begin(b'B');
        w.cstr(""); // portal
        w.cstr(""); // statement
        w.i16(0);
        w.i16(0);
        w.i16(0);
        w.end(p);
        let p = w.begin(b'D');
        w.bytes(b"P");
        w.cstr("");
        w.end(p);
        let p = w.begin(b'E');
        w.cstr("");
        w.i32(0);
        w.end(p);
        let p = w.begin(b'S');
        w.end(p);
        sock.write_all(&w.take()).await.unwrap();

        let mut got_parse = false;
        let mut got_bind = false;
        let mut got_rows = 0;
        loop {
            let (tag, payload) = read_msg(&mut sock).await;
            match tag {
                b'1' => got_parse = true,
                b'2' => got_bind = true,
                b'n' | b't' => {}
                b'T' => {}
                b'D' => got_rows += 1,
                b'C' => {}
                b'Z' => break,
                b'E' => panic!("extended protocol error: {}", cstring(&payload[2..])),
                _ => {}
            }
        }
        assert!(got_parse && got_bind && got_rows == 1);

        // Terminate
        let mut w = PgWriter::new();
        let p = w.begin(b'X');
        w.end(p);
        sock.write_all(&w.take()).await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_pg_e2e_md5_auth() {
        // 配置带密码用户
        let mut cfg = default_config();
        cfg.users.get_mut("root").unwrap().password = "pgpass".into();
        let state = ServerState::new(cfg).unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            loop {
                let (sock, _) = listener.accept().await.unwrap();
                let st = state.clone();
                tokio::spawn(async move {
                    serve_pg_connection(st, sock, Vec::new(), None).await;
                });
            }
        });

        // 错误密码 → FATAL 28P01
        let mut sock = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
        sock.write_all(&startup_msg("root", "db1")).await.unwrap();
        let (tag, payload) = read_msg(&mut sock).await;
        assert_eq!(tag, b'R');
        assert_eq!(be_u32(&payload, 0), 5); // MD5
        let salt: [u8; 4] = [payload[4], payload[5], payload[6], payload[7]];
        let wrong = compute_pg_md5("badpass", "root", &salt);
        let mut w = PgWriter::new();
        let p = w.begin(b'p');
        w.cstr(&wrong);
        w.end(p);
        sock.write_all(&w.take()).await.unwrap();
        let (tag, payload) = read_msg(&mut sock).await;
        assert_eq!(tag, b'E');
        assert!(String::from_utf8_lossy(&payload).contains("28P01"));

        // 正确密码 → AuthenticationOk
        let mut sock = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
        sock.write_all(&startup_msg("root", "db1")).await.unwrap();
        let (tag, payload) = read_msg(&mut sock).await;
        assert_eq!(tag, b'R');
        let salt: [u8; 4] = [payload[4], payload[5], payload[6], payload[7]];
        let good = compute_pg_md5("pgpass", "root", &salt);
        let mut w = PgWriter::new();
        let p = w.begin(b'p');
        w.cstr(&good);
        w.end(p);
        sock.write_all(&w.take()).await.unwrap();
        let (tag, payload) = read_msg(&mut sock).await;
        assert_eq!(tag, b'R');
        assert_eq!(be_u32(&payload, 0), 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_pg_ssl_request_refused() {
        let port = spawn_pg_server().await;
        let mut sock = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
        let mut msg = Vec::new();
        msg.extend_from_slice(&8u32.to_be_bytes());
        msg.extend_from_slice(&80877103u32.to_be_bytes());
        sock.write_all(&msg).await.unwrap();
        let mut resp = [0u8; 1];
        sock.read_exact(&mut resp).await.unwrap();
        assert_eq!(resp[0], b'N');
        // 随后发正常 Startup 应能继续（trust 认证）
        sock.write_all(&startup_msg("root", "db1")).await.unwrap();
        let (tag, _) = read_msg(&mut sock).await;
        assert_eq!(tag, b'R');
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_pg_error_response_for_bad_syntax() {
        let port = spawn_pg_server().await;
        let mut sock = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
        sock.write_all(&startup_msg("root", "db1")).await.unwrap();
        loop {
            let (tag, _) = read_msg(&mut sock).await;
            if tag == b'Z' {
                break;
            }
        }
        sock.write_all(&query_msg("selct bad")).await.unwrap();
        let (tag, payload) = read_msg(&mut sock).await;
        assert_eq!(tag, b'E');
        assert!(
            String::from_utf8_lossy(&payload).contains("42601"),
            "{}",
            String::from_utf8_lossy(&payload)
        );
        // 错误后仍收到 ReadyForQuery（连接可继续使用）
        let (tag, _) = read_msg(&mut sock).await;
        assert_eq!(tag, b'Z');
    }
}
