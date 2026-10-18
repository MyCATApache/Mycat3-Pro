//! 前端连接处理：握手认证 + 命令循环。
//!
//! 对应 Mycat3 `VertxMySQLAuthHandler` + `VertxMySQLPacketResolver` +
//! `MycatVertxMySQLHandler`（Pro 版仅实现 MySQL 协议；Java 版的多协议嗅探
//! TDS/TNS/PG 不在移植范围内）。
//!
//! 命令支持：COM_QUERY（含多语句）、COM_PING、COM_INIT_DB、COM_QUIT、
//! COM_FIELD_LIST、COM_SET_OPTION、COM_RESET_CONNECTION、
//! COM_STMT_PREPARE / EXECUTE / CLOSE / SEND_LONG_DATA（单节点路由，
//! 参数按文本替换执行，结果以二进制协议回写）。

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use mycat3_protocol::auth::{generate_seed, verify_caching_sha2, verify_native};
use mycat3_protocol::consts::command::*;
use mycat3_protocol::consts::{
    capability::*, error_code, AUTH_PLUGIN_CACHING_SHA2, AUTH_PLUGIN_NATIVE,
};
use mycat3_protocol::frame::FrameDecoder;
use mycat3_protocol::packets::{
    build_auth_switch_request, build_error, build_handshake_v10, build_prepare_ok,
    parse_auth_response, read_binary_params, ColumnDef,
};
use mycat3_sql::{analyse, SqlError, StmtType};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tracing::{debug, info, warn};

use crate::commands::{handle_local, LocalOutcome};
use crate::executor::{execute_route, ExecResponse};
use crate::session::{PacketSender, ProtoStream, SessionContext, SessionWriter};
use crate::state::ServerState;
use crate::tx::TransactionSession;

/// 预处理语句。对应 MycatDataContext 的 preparedStatement map。
struct PreparedStmt {
    sql: String,
    num_params: usize,
}

pub struct Frontend {
    socket: ProtoStream,
    sender: PacketSender,
    out: Vec<u8>,
    ctx: SessionContext,
    tx: TransactionSession,
    state: Arc<ServerState>,
    decoder: FrameDecoder,
    /// 已解码待消费的包（read_packet 一次读可能返回多个包；prefeed 也走这里）
    pending: std::collections::VecDeque<mycat3_protocol::frame::Packet>,
    prepared: HashMap<u32, PreparedStmt>,
    /// COM_STMT_SEND_LONG_DATA 缓存：stmt_id → (param_id → bytes)
    long_data: HashMap<u32, HashMap<u16, Vec<u8>>>,
    /// PROCESSLIST 注册项
    proc: Option<Arc<crate::state::ProcessInfo>>,
}

/// 接受新连接的入口（对应 MycatSessionManager 的 connectHandler）。
pub async fn serve_connection(state: Arc<ServerState>, socket: tokio::net::TcpStream) {
    serve_connection_prefeed(state, ProtoStream::Plain(socket), Vec::new(), None).await
}

/// 带嗅探预读字节的入口：sniff 阶段已消费的字节先喂入解码器
/// （对应 Java `installHandler(...).handle(sniffBuf)`）。
pub async fn serve_connection_prefeed(
    state: Arc<ServerState>,
    socket: ProtoStream,
    prefeed: Vec<u8>,
    kill_handle: Option<std::net::TcpStream>,
) {
    let peer = match &socket {
        ProtoStream::Plain(s) => s
            .peer_addr()
            .map(|a| a.ip().to_string())
            .unwrap_or_else(|_| "unknown".into()),
        _ => "tls".into(),
    };
    let conn_id = state.next_connection_id();
    state.stats.incr_conn();
    let mut fe = Frontend {
        socket,
        sender: PacketSender::new(),
        out: Vec::with_capacity(8192),
        ctx: SessionContext::new(conn_id, peer.clone()),
        tx: TransactionSession::new(),
        state: state.clone(),
        decoder: FrameDecoder::new(),
        pending: std::collections::VecDeque::new(),
        prepared: HashMap::new(),
        long_data: HashMap::new(),
        proc: None,
    };
    if !prefeed.is_empty() {
        match fe.decoder.feed(&prefeed) {
            Ok(packets) => fe.pending.extend(packets),
            Err(e) => {
                debug!(conn_id, "bad prefeed bytes: {}", e);
                state.stats.decr_conn();
                return;
            }
        }
    }
    info!(conn_id, peer = %peer, "client connected");
    let result = async {
        if !fe.handshake().await? {
            return Ok::<(), FrontendError>(());
        }
        // 认证成功 → 注册到 PROCESSLIST（对应 Java Process.PROCESS_MAP 注册）
        let proc = state
            .processes
            .register(conn_id, "MySQL", peer.clone(), kill_handle);
        proc.set_user(fe.ctx.user.clone());
        proc.set_db(fe.ctx.schema.clone());
        fe.proc = Some(proc);
        let r = fe.command_loop().await;
        state.processes.unregister(conn_id);
        r
    }
    .await;
    if let Err(e) = result {
        debug!(conn_id, "connection closed: {}", e);
    }
    // 会话清理：回滚未提交事务（对应 MycatDataContextImpl.close）
    fe.tx.close().await;
    state.stats.decr_conn();
    if !fe.ctx.user.is_empty() {
        state.decr_user_conn(&fe.ctx.user);
    }
}

/// execute_one/proxy_to 的两类失败：可回写客户端的错误包，或前端 IO 故障。
enum OneErr {
    Packet(Vec<u8>),
    Io(FrontendError),
}

#[derive(Debug, thiserror::Error)]
enum FrontendError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("protocol: {0}")]
    Protocol(#[from] mycat3_protocol::ProtocolError),
    #[error("closed")]
    Closed,
}

impl Frontend {
    // ------------------------------------------------------------------
    // 握手与认证（对应 VertxMySQLAuthHandler）
    // ------------------------------------------------------------------
    async fn handshake(&mut self) -> Result<bool, FrontendError> {
        let seed = generate_seed();
        let cfg = self.state.config();
        let server_version = cfg.server.server.server_version.clone();
        let mut caps = DEFAULT_SERVER_CAPABILITIES;
        if cfg.server.server.deprecate_eof {
            caps |= CLIENT_DEPRECATE_EOF;
        }
        // TLS 已配置 → 握手通告 CLIENT_SSL（客户端 ssl-mode=REQUIRED 依赖此位）
        if self.state.tls_acceptor.read().is_some() {
            caps |= CLIENT_SSL;
        }
        let payload = build_handshake_v10(
            &server_version,
            self.ctx.connection_id,
            &seed,
            caps,
            45,
            2,
            AUTH_PLUGIN_NATIVE,
        );
        self.sender.set_seq(0);
        // 握手包序号为 0：直接以 seq 0 写出
        let mut s = 0u8;
        let mut wire = Vec::new();
        mycat3_protocol::frame::encode_frames(&mut s, &payload, &mut wire);
        self.socket.write_all(&wire).await?;
        self.socket.flush().await?;
        self.sender.set_seq(0);

        // 读客户端认证包
        let (seq, mut auth_payload) = self.read_packet().await?;
        self.sender.set_seq(seq);

        // SSL 请求检测：载荷 ≤ 32 字节且带 CLIENT_SSL → Pro 不支持 TLS
        if auth_payload.len() >= 4 {
            let client_caps = u32::from_le_bytes([
                auth_payload[0],
                auth_payload[1],
                auth_payload[2],
                auth_payload[3],
            ]);
            if client_caps & CLIENT_SSL != 0 && auth_payload.len() <= 36 {
                // 客户端请求 TLS：已配置证书 → 升级；未配置 → 明确拒绝
                let acceptor = self.state.tls_acceptor.read().clone();
                match acceptor {
                    Some(acc) => {
                        let plain = match std::mem::replace(&mut self.socket, ProtoStream::Closed) {
                            ProtoStream::Plain(p) => p,
                            _ => {
                                return Ok(false);
                            }
                        };
                        match acc.accept(plain).await {
                            Ok(tls) => {
                                tracing::info!("tls upgrade ok");
                                self.socket = ProtoStream::Tls(Box::new(tls));
                                // 继续读取真正的认证响应（SSL 请求为占位）
                                let mut hdr = [0u8; 4];
                                read_exact(&mut self.socket, &mut hdr).await?;
                                let plen = u32::from_le_bytes([hdr[0], hdr[1], hdr[2], 0]) as usize;
                                let mut payload = vec![0u8; plen];
                                read_exact(&mut self.socket, &mut payload).await?;
                                // 序号同步（响应包序号从客户端认证包序号延续）
                                self.sender.set_seq(hdr[3]);
                                auth_payload = payload;
                            }
                            Err(e) => {
                                tracing::info!(error = %e, "tls upgrade failed");
                                self.write_error_and_flush(
                                    error_code::ER_HANDSHAKE_ERROR,
                                    "08S01",
                                    &format!("TLS handshake failed: {}", e),
                                )
                                .await?;
                                return Ok(false);
                            }
                        }
                    }
                    None => {
                        self.write_error_and_flush(
                            error_code::ER_HANDSHAKE_ERROR,
                            "08S01",
                            "SSL connection is not supported by Mycat3 Pro (sslCert/sslKey not configured)",
                        )
                        .await?;
                        return Ok(false);
                    }
                }
            }
            if client_caps & CLIENT_COMPRESS != 0 {
                // 与 Java 一致：拒绝压缩协议
                self.write_error_and_flush(
                    error_code::ER_HANDSHAKE_ERROR,
                    "08S01",
                    "Can Not Use Compression Protocol!",
                )
                .await?;
                return Ok(false);
            }
        }

        let mut resp = match parse_auth_response(&auth_payload) {
            Ok(r) => r,
            Err(e) => {
                self.write_error_and_flush(
                    error_code::ER_HANDSHAKE_ERROR,
                    "08S01",
                    &format!("bad handshake: {}", e),
                )
                .await?;
                return Ok(false);
            }
        };

        // 客户端使用其他认证插件 → AuthSwitchRequest 切回 native（Java 相同流程）
        let plugin = resp
            .auth_plugin_name
            .clone()
            .unwrap_or_else(|| AUTH_PLUGIN_NATIVE.to_string());
        if plugin != AUTH_PLUGIN_NATIVE && !plugin.is_empty() {
            let switch = build_auth_switch_request(AUTH_PLUGIN_NATIVE, &seed);
            self.sender.push(&mut self.out, &switch);
            self.flush().await?;
            let (seq2, scramble) = self.read_packet().await?;
            self.sender.set_seq(seq2); // 最终 OK 序号 = scramble 包序号 + 1
            resp.auth_response = scramble;
        }

        // 用户校验（对应 AuthenticatorImpl：密码 scramble 比对 + IP 正则）
        let users = self.state.config();
        let user_cfg = users.users.get(&resp.username).cloned();
        let authorized = match &user_cfg {
            None => false,
            Some(u) => {
                if !u.ip_allowed(&self.ctx.client_ip) {
                    false
                } else if plugin == AUTH_PLUGIN_CACHING_SHA2 {
                    verify_caching_sha2(&resp.auth_response, &u.decrypted_password(), &seed)
                        || verify_native(&resp.auth_response, &u.decrypted_password(), &seed)
                } else {
                    verify_native(&resp.auth_response, &u.decrypted_password(), &seed)
                }
            }
        };
        if !authorized {
            let using_password = !resp.auth_response.is_empty();
            self.write_error_and_flush(
                error_code::ER_ACCESS_DENIED_ERROR,
                "28000",
                &format!(
                    "Access denied for user '{}'@'{}' (using password: {})",
                    resp.username,
                    self.ctx.client_ip,
                    if using_password { "YES" } else { "NO" }
                ),
            )
            .await?;
            warn!(user = %resp.username, peer = %self.ctx.client_ip, "access denied");
            return Ok(false);
        }
        let user_cfg = user_cfg.unwrap();

        // loginLimit 强制：超出该用户活跃连接上限 → 拒绝
        if user_cfg.login_limit > 0
            && self.state.user_conn_count(&resp.username) >= user_cfg.login_limit as u32
        {
            self.write_error_and_flush(
                error_code::ER_ACCESS_DENIED_ERROR,
                "28000",
                &format!(
                    "user '{}' exceeds login limit ({})",
                    resp.username, user_cfg.login_limit
                ),
            )
            .await?;
            return Ok(false);
        }

        // 会话初始化（对应 auth 成功分支）
        self.ctx.user = resp.username.clone();
        self.state.incr_user_conn(&resp.username);
        self.ctx.capabilities = resp.capabilities;
        self.ctx.charset = resp.charset;
        self.ctx.deprecate_eof = resp.capabilities & CLIENT_DEPRECATE_EOF != 0
            && self.state.config().server.server.deprecate_eof;
        self.ctx.autocommit = true;
        self.ctx.isolation = user_cfg.isolation;
        self.ctx.transaction_policy = user_cfg.transaction_type.clone();
        // 默认 schema：认证库 > 用户配置 schema > 用户 schemas[0]
        let schema = resp
            .database
            .clone()
            .or_else(|| user_cfg.schema.clone())
            .or_else(|| user_cfg.schemas.first().cloned());
        if let Some(s) = &schema {
            // 认证指定库不存在 → ER_BAD_DB（MySQL 行为）
            if resp.database.is_some()
                && !self.state.is_proxy_mode()
                && self.state.metadata().get_schema(s).is_none()
            {
                self.write_error_and_flush(
                    error_code::ER_BAD_DB_ERROR,
                    "42000",
                    &format!("Unknown database '{}'", s),
                )
                .await?;
                return Ok(false);
            }
            self.ctx.schema = Some(s.clone());
        }

        // OK（序号 = 客户端最后包 + 1）
        {
            let mut w = self.writer();
            w.write_ok(0, 0, false);
        }
        self.flush().await?;
        info!(user = %self.ctx.user, schema = ?self.ctx.schema, "client authenticated");
        Ok(true)
    }

    // ------------------------------------------------------------------
    // 命令循环（对应 MycatVertxMySQLHandler.handle）
    // ------------------------------------------------------------------
    async fn command_loop(&mut self) -> Result<(), FrontendError> {
        let mut buf = vec![0u8; 32 * 1024];
        loop {
            let n = self.socket.read(&mut buf).await?;
            if n == 0 {
                return Ok(());
            }
            self.state.stats.add_in(n as u64);
            let packets = self.decoder.feed(&buf[..n])?;
            for p in packets {
                let payload = &p.payload;
                if payload.is_empty() {
                    continue;
                }
                self.sender.set_seq(p.seq);
                // pauseServer 挂起：排队等待 resumeServer（对应 Java MycatServer.pause）
                crate::state::wait_if_paused(self.proc.as_deref()).await;
                let cmd = payload[0];
                let body = &payload[1..];
                if let Some(proc) = &self.proc {
                    proc.set_command(cmd_name(cmd));
                }
                match cmd {
                    COM_QUIT => return Ok(()),
                    COM_QUERY => {
                        let sql = String::from_utf8_lossy(body).to_string();
                        self.handle_query(&sql).await?;
                    }
                    COM_PING => {
                        let mut w = self.writer();
                        w.write_ok(0, 0, false);
                        self.flush().await?;
                    }
                    COM_INIT_DB => {
                        let schema = String::from_utf8_lossy(body).to_string();
                        if self.state.is_proxy_mode() {
                            // 代理模式：任意库名接受，并把 USE 下推到固定连接
                            //（客户端普遍用 COM_INIT_DB 实现 -D/USE）
                            self.ctx.schema = Some(schema.clone());
                            let route = crate::engine::proxy_route(
                                &self.state,
                                &format!("USE `{}`", schema),
                            );
                            match self.execute_one_proxy(&route, "", false).await {
                                Ok(_) => {}
                                Err(e) => {
                                    self.finish_proxy_error(e).await?;
                                    return Ok(());
                                }
                            }
                        } else if self.state.metadata().get_schema(&schema).is_some() {
                            self.ctx.schema = Some(schema);
                            let mut w = self.writer();
                            w.write_ok(0, 0, false);
                        } else {
                            let mut w = self.writer();
                            w.write_error(
                                error_code::ER_BAD_DB_ERROR,
                                "42000",
                                &format!("Unknown database '{}'", schema),
                            );
                        }
                        self.flush().await?;
                    }
                    COM_FIELD_LIST => {
                        // 已废弃命令：返回空集（EOF）
                        let mut w = self.writer();
                        w.write_eof(false);
                        self.flush().await?;
                    }
                    COM_SET_OPTION => {
                        let mut w = self.writer();
                        w.write_eof(false);
                        self.flush().await?;
                    }
                    COM_RESET_CONNECTION => {
                        self.tx.close().await;
                        self.tx = TransactionSession::new();
                        self.ctx.in_transaction = false;
                        self.ctx.autocommit = true;
                        self.prepared.clear();
                        let mut w = self.writer();
                        w.write_ok(0, 0, false);
                        self.flush().await?;
                    }
                    COM_STMT_PREPARE => {
                        let sql = String::from_utf8_lossy(body).to_string();
                        self.handle_stmt_prepare(&sql).await?;
                    }
                    COM_STMT_EXECUTE => {
                        self.handle_stmt_execute(body).await?;
                    }
                    COM_STMT_SEND_LONG_DATA => {
                        if body.len() >= 6 {
                            let stmt_id = u32::from_le_bytes([body[0], body[1], body[2], body[3]]);
                            let param_id = u16::from_le_bytes([body[4], body[5]]);
                            self.long_data
                                .entry(stmt_id)
                                .or_default()
                                .insert(param_id, body[6..].to_vec());
                        }
                        // 无响应包（协议规定）
                    }
                    COM_STMT_CLOSE => {
                        if body.len() >= 4 {
                            let stmt_id = u32::from_le_bytes([body[0], body[1], body[2], body[3]]);
                            self.prepared.remove(&stmt_id);
                            self.long_data.remove(&stmt_id);
                        }
                        // 无响应包
                    }
                    COM_STMT_RESET => {
                        if body.len() >= 4 {
                            let stmt_id = u32::from_le_bytes([body[0], body[1], body[2], body[3]]);
                            self.long_data.remove(&stmt_id);
                        }
                        let mut w = self.writer();
                        w.write_ok(0, 0, false);
                        self.flush().await?;
                    }
                    COM_PROCESS_INFO | COM_STATISTICS | COM_PROCESS_KILL | COM_DEBUG
                    | COM_REFRESH => {
                        let mut w = self.writer();
                        w.write_error(
                            error_code::ER_NOT_SUPPORTED_YET,
                            "42000",
                            "command is not supported by Mycat3 Pro",
                        );
                        self.flush().await?;
                    }
                    _ => {
                        let mut w = self.writer();
                        w.write_error(
                            error_code::ER_UNKNOWN_COM_ERROR,
                            "08S01",
                            &format!("unsupported command: {}", cmd),
                        );
                        self.flush().await?;
                    }
                }
            }
        }
    }

    // ------------------------------------------------------------------
    // COM_QUERY（对应 handleQuery → MycatdbCommand.executeQuery）
    // ------------------------------------------------------------------
    /// 纯代理模式查询：多语句拆分 → hint → 本地命令 → 拦截 → 缓存 → 透传。
    async fn handle_query_proxy(
        &mut self,
        sql: &str,
        trace_id: u64,
        started: Instant,
    ) -> Result<(), FrontendError> {
        let mut total_rows = 0u64;
        let mut pending: Vec<String> = crate::prepared_text::split_statements(sql)
            .into_iter()
            .rev()
            .collect();
        while let Some(stmt) = pending.pop() {
            // hint 指令
            match crate::engine::run_hint_commands(&self.state, &self.ctx, &stmt, true).await {
                crate::engine::HintOutcome::Handled(r) => {
                    let more = !pending.is_empty();
                    if !self.write_proxy_outcome(r, &mut total_rows, more).await? {
                        return Ok(());
                    }
                    continue;
                }
                crate::engine::HintOutcome::Remainder(rem) => {
                    pending.push(rem);
                    continue;
                }
                crate::engine::HintOutcome::None => {}
            }
            // 本地命令（KILL/show @@/事务/SET/USE/探针）
            if let Some(r) =
                crate::engine::proxy_local_inner(&self.state, &mut self.ctx, &mut self.tx, &stmt)
                    .await
            {
                let more = !pending.is_empty();
                if !self.write_proxy_outcome(r, &mut total_rows, more).await? {
                    return Ok(());
                }
                continue;
            }
            // 拦截（必须在缓存之前，避免命中缓存绕过权限拒绝）
            if let Some(e) = crate::intercept::check(&self.state.intercepts(), &stmt) {
                self.state.stats.incr_error();
                let mut w = self.writer();
                w.write_error(e.errno, &e.sqlstate, &e.message);
                self.flush().await?;
                return Ok(());
            }
            // SQL 缓存
            if let Some(o) = self
                .state
                .sql_cache
                .lookup(
                    &stmt,
                    self.ctx.schema.as_deref(),
                    self.ctx.in_transaction,
                    self.ctx.autocommit,
                )
                .await
            {
                total_rows += Self::outcome_rows(&o);
                self.write_engine_outcome(&o, false).await?;
                continue;
            }
            // 权限（read_only / disallowSqlTypes）
            let stmt_type = crate::engine::proxy_stmt_type(&stmt);
            let fake = [mycat3_sql::RouteResult {
                stmt_type,
                tasks: Vec::new(),
                merge: None,
                use_schema: None,
                hint: None,
                explain_rows: Vec::new(),
            }];
            if let Some(e) = crate::engine::check_disallow(&self.state, &self.ctx, &fake) {
                self.state.stats.incr_error();
                let mut w = self.writer();
                w.write_error(e.errno, &e.sqlstate, &e.message);
                self.flush().await?;
                return Ok(());
            }
            // 透传执行（原始 SQL；会话固定连接，无重写）
            let route = crate::engine::proxy_route(&self.state, &stmt);
            let more = !pending.is_empty();
            match self.execute_one_proxy(&route, &stmt, more).await {
                Ok(rows) => total_rows += rows,
                Err(e) => {
                    self.finish_proxy_error(e).await?;
                    return Ok(());
                }
            }
            // 写入后缓存失效
            if matches!(
                stmt_type,
                StmtType::Insert
                    | StmtType::Update
                    | StmtType::Delete
                    | StmtType::Replace
                    | StmtType::Ddl
            ) {
                crate::engine::invalidate_cache_after_write(
                    &self.state,
                    self.ctx.schema.as_deref(),
                    &stmt,
                );
            }
        }
        crate::engine::record_slow_sql(
            &self.state,
            trace_id,
            &self.ctx.user,
            self.ctx.schema.as_deref(),
            sql,
            total_rows,
            started.elapsed(),
            &self.state.proxy_target(),
        );
        Ok(())
    }

    /// 写代理模式本地结果（或错误）。返回 true = 继续执行后续语句。
    async fn write_proxy_outcome(
        &mut self,
        r: Result<crate::engine::EngineOutcome, crate::engine::EngineError>,
        total_rows: &mut u64,
        more: bool,
    ) -> Result<bool, FrontendError> {
        match r {
            Ok(o) => {
                *total_rows += Self::outcome_rows(&o);
                self.write_engine_outcome(&o, more).await?;
                Ok(true)
            }
            Err(e) => {
                self.state.stats.incr_error();
                let mut w = self.writer();
                w.write_error(e.errno, &e.sqlstate, &e.message);
                self.flush().await?;
                Ok(false)
            }
        }
    }

    /// 代理模式执行错误回写（OneErr）。
    async fn finish_proxy_error(&mut self, e: OneErr) -> Result<(), FrontendError> {
        match e {
            OneErr::Packet(payload) => {
                self.state.stats.incr_error();
                let mut w = self.writer();
                w.write_error_payload(payload);
                self.flush().await?;
                Ok(())
            }
            OneErr::Io(io) => Err(io),
        }
    }

    async fn handle_query(&mut self, sql: &str) -> Result<(), FrontendError> {
        let started = Instant::now();
        let trace_id = self
            .state
            .stats
            .queries_total
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            + 1;
        let sql_trim = sql.trim();
        if let Some(proc) = &self.proc {
            proc.set_sql(Some(sql_trim.chars().take(1024).collect()));
            proc.set_db(self.ctx.schema.clone());
        }
        if sql_trim.is_empty() {
            let mut w = self.writer();
            w.write_error_payload(mycat3_protocol::packets::err_empty_query());
            self.flush().await?;
            return Ok(());
        }
        if self.state.config().server.monitor.sql_log {
            info!(conn = self.ctx.connection_id, sql = %sql_trim, "SQL");
        }

        // 文本级 PREPARE（MySQL 方言）：会话本地语句表，不下发后端
        if self.handle_text_prepare(sql_trim).await? {
            return Ok(());
        }

        // 纯代理模式：不分片/不改写，仅拦截/缓存/慢 SQL
        if self.state.is_proxy_mode() {
            return self.handle_query_proxy(sql_trim, trace_id, started).await;
        }

        // SQL 结果缓存（对应 Java SqlResultSetService；命中直接回缓存结果）
        if let Some(outcome) = self
            .state
            .sql_cache
            .lookup(
                sql_trim,
                self.ctx.schema.as_deref(),
                self.ctx.in_transaction,
                self.ctx.autocommit,
            )
            .await
        {
            self.write_engine_outcome(&outcome, false).await?;
            return Ok(());
        }

        // 服务器端 CSV 批量导入（对应 Java HintHandler.loaddata）与未知 hint 报错
        if let (Some(hint), _) = mycat3_sql::parse_hint(sql_trim) {
            // 未识别任何有效指令的 mycat hint：显式报错（Java 静默忽略 → 脚本假成功）
            if !hint.unknown.is_empty() && hint.is_empty() {
                self.state.stats.incr_error();
                let mut w = self.writer();
                w.write_error(
                    1235,
                    "42000",
                    &format!(
                        "未知 mycat hint 指令: {}（Pro 未实现该指令）",
                        hint.unknown.join(", ")
                    ),
                );
                self.flush().await?;
                return Ok(());
            }
            if let Some(json) = &hint.loaddata {
                let outcome =
                    crate::loaddata::run_import(&self.state, &mut self.ctx, &mut self.tx, json)
                        .await;
                match outcome {
                    Ok(out) => {
                        let rows = match &out {
                            crate::engine::EngineOutcome::Ok { affected_rows, .. } => {
                                *affected_rows
                            }
                            _ => 0,
                        };
                        crate::engine::record_slow_sql(
                            &self.state,
                            trace_id,
                            &self.ctx.user,
                            self.ctx.schema.as_deref(),
                            sql_trim,
                            rows,
                            started.elapsed(),
                            "",
                        );
                        self.write_engine_outcome(&out, false).await?;
                        return Ok(());
                    }
                    Err(e) => {
                        self.state.stats.incr_error();
                        let mut w = self.writer();
                        w.write_error(e.errno, &e.sqlstate, &e.message);
                        self.flush().await?;
                        return Ok(());
                    }
                }
            }
        }

        // 本地快路径：配置 DDL / reload / kill / information_schema / SHOW 扩展 / 变量探针
        if let Some(result) =
            crate::engine::try_local_fast_path(&self.state, &self.ctx, sql_trim).await
        {
            match result {
                Ok(outcome) => {
                    crate::engine::record_slow_sql(
                        &self.state,
                        trace_id,
                        &self.ctx.user,
                        self.ctx.schema.as_deref(),
                        sql_trim,
                        match &outcome {
                            crate::engine::EngineOutcome::Rows { rows, .. } => rows.len() as u64,
                            crate::engine::EngineOutcome::Ok { affected_rows, .. } => {
                                *affected_rows
                            }
                        },
                        started.elapsed(),
                        "",
                    );
                    self.write_engine_outcome(&outcome, false).await?;
                }
                Err(e) => {
                    self.state.stats.incr_error();
                    let (errno, sqlstate, msg) = (e.errno, e.sqlstate.clone(), e.message.clone());
                    let mut w = self.writer();
                    w.write_error(errno, &sqlstate, &msg);
                    self.flush().await?;
                }
            }
            return Ok(());
        }

        // INSERT 自增序列填充（与 engine.execute_sql 相同语义）
        let filled = crate::engine::fill_insert_sequences(&self.state, &self.ctx, sql_trim);
        let sql_trim: &str = filled.as_deref().unwrap_or(sql_trim);

        let meta = self.state.metadata();
        let routes = match analyse(sql_trim, self.ctx.schema.as_deref(), &meta) {
            Ok(r) => r,
            Err(e) => {
                self.state.stats.incr_error();
                let payload = sql_error_packet(&e);
                let mut w = self.writer();
                w.write_error_payload(payload);
                self.flush().await?;
                return Ok(());
            }
        };
        // 用户角色 SQL 类型黑名单（与 PG/TDS 前端经 engine 的同名检查一致）
        if let Some(err) = crate::engine::check_disallow(&self.state, &self.ctx, &routes) {
            self.state.stats.incr_error();
            let mut w = self.writer();
            w.write_error(err.errno, &err.sqlstate, &err.message);
            self.flush().await?;
            return Ok(());
        }

        let any_write = routes
            .iter()
            .any(|r| r.stmt_type.is_write() || matches!(r.stmt_type, StmtType::Ddl));
        let n = routes.len();
        let mut total_rows = 0u64;
        for (i, route) in routes.iter().enumerate() {
            let more = i + 1 < n;
            match self.execute_one(route, sql_trim, more).await {
                Ok(rows) => total_rows = total_rows.saturating_add(rows),
                Err(e) => match e {
                    OneErr::Packet(payload) => {
                        self.state.stats.incr_error();
                        let mut w = self.writer();
                        w.write_error_payload(payload);
                        self.flush().await?;
                        return Ok(());
                    }
                    OneErr::Io(io) => return Err(io),
                },
            }
        }
        // 写入后缓存失效（保守启发式，与 engine 路径一致）
        if any_write {
            crate::engine::invalidate_cache_after_write(
                &self.state,
                self.ctx.schema.as_deref(),
                sql_trim,
            );
        }
        // 慢 SQL 观测（阈值可经 setSqlTimeFilter 运行时调整；target 取首个任务的集群名）
        let target = routes
            .first()
            .and_then(|r| r.tasks.first())
            .map(|t| t.target_name.as_str())
            .unwrap_or("");
        crate::engine::record_slow_sql(
            &self.state,
            trace_id,
            &self.ctx.user,
            self.ctx.schema.as_deref(),
            sql_trim,
            total_rows,
            started.elapsed(),
            target,
        );
        Ok(())
    }

    /// 文本级 PREPARE/EXECUTE/DEALLOCATE（MySQL 方言；返回 true = 已处理）。
    async fn handle_text_prepare(&mut self, sql: &str) -> Result<bool, FrontendError> {
        let lower = sql.trim().to_ascii_lowercase();
        // PREPARE n FROM '...'
        if lower.starts_with("prepare ") {
            match crate::prepared_text::parse_mysql_prepare(sql) {
                Some((name, template)) => {
                    let params = template.matches('?').count();
                    self.ctx.text_prepares.insert(
                        name.clone(),
                        crate::prepared_text::TextPrepared {
                            sql: template,
                            params,
                        },
                    );
                    let mut w = self.writer();
                    w.write_ok(0, 0, false);
                    drop(w);
                    self.flush().await?;
                    return Ok(true);
                }
                None => {
                    // 非 MySQL PREPARE（如 PG 方言语句）：交给正常路径报错
                    return Ok(false);
                }
            }
        }
        // EXECUTE n [USING @a, ...]
        if lower.starts_with("execute ") {
            let Some((name, vars)) = crate::prepared_text::parse_mysql_execute(sql) else {
                return Ok(false);
            };
            let Some(stmt) = self.ctx.text_prepares.get(&name).cloned() else {
                let mut w = self.writer();
                w.write_error(
                    error_code::ER_PARSE_ERROR,
                    "HY000",
                    &format!(
                        "Unknown prepared statement handler ({}) given to EXECUTE",
                        name
                    ),
                );
                self.flush().await?;
                return Ok(true);
            };
            if vars.len() != stmt.params {
                let mut w = self.writer();
                w.write_error(
                    1210,
                    "HY000",
                    &format!(
                        "Incorrect arguments to EXECUTE (expected {}, got {})",
                        stmt.params,
                        vars.len()
                    ),
                );
                self.flush().await?;
                return Ok(true);
            }
            let mut values = Vec::with_capacity(vars.len());
            for v in &vars {
                match self.ctx.user_vars.get(v) {
                    Some(val) => values.push(val.clone()),
                    None => {
                        let mut w = self.writer();
                        w.write_error(
                            1331,
                            "HY000",
                            &format!("Variable '@{}' is not a variable", v),
                        );
                        self.flush().await?;
                        return Ok(true);
                    }
                }
            }
            let Some(final_sql) =
                crate::prepared_text::substitute_question_params(&stmt.sql, &values)
            else {
                let mut w = self.writer();
                w.write_error(error_code::ER_PARSE_ERROR, "42000", "参数替换失败");
                self.flush().await?;
                return Ok(true);
            };
            // 替换产物走正常查询路径（重新路由/方言翻译）
            Box::pin(self.handle_query(&final_sql)).await?;
            return Ok(true);
        }
        // DEALLOCATE PREPARE n
        if lower.starts_with("deallocate ") {
            if let Some(name) = crate::prepared_text::parse_mysql_deallocate(sql) {
                self.ctx.text_prepares.remove(&name);
                let mut w = self.writer();
                w.write_ok(0, 0, false);
                drop(w);
                self.flush().await?;
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// 代理模式单条语句执行（会话固定连接；返回影响/返回行数）。
    async fn execute_one_proxy(
        &mut self,
        route: &mycat3_sql::RouteResult,
        sql: &str,
        more: bool,
    ) -> Result<u64, OneErr> {
        let exec_timeout = {
            let cfg = self.state.config();
            let t = cfg.server.server.execute_timeout;
            if t > 0 {
                Some(std::time::Duration::from_millis(t))
            } else {
                None
            }
        };
        let resp = crate::executor::execute_route_proxy(
            &self.state,
            &mut self.ctx,
            &mut self.tx,
            route,
            exec_timeout,
        )
        .await
        .map_err(|e| {
            let (errno, sqlstate, msg) = e.to_packet_parts();
            OneErr::Packet(build_error(errno, &sqlstate, &msg))
        })?;
        let rows = match resp {
            ExecResponse::Ok {
                affected_rows,
                last_insert_id,
            } => {
                let mut w = self.writer();
                w.write_ok(affected_rows, last_insert_id, more);
                drop(w);
                self.flush().await.map_err(OneErr::Io)?;
                affected_rows
            }
            ExecResponse::Rows(rs) => {
                let n = rs.row_payloads.len() as u64;
                self.write_result_set(&rs, more).await.map_err(OneErr::Io)?;
                n
            }
            ExecResponse::Empty => {
                let mut w = self.writer();
                w.write_ok(0, 0, more);
                drop(w);
                self.flush().await.map_err(OneErr::Io)?;
                0
            }
        };
        let _ = sql;
        Ok(rows)
    }

    /// 执行单条语句路由结果。返回（影响/返回）行数（慢 SQL 观测用）。
    async fn execute_one(
        &mut self,
        route: &mycat3_sql::RouteResult,
        raw_sql: &str,
        more: bool,
    ) -> Result<u64, OneErr> {
        // 无后端任务的本地语句
        if route.tasks.is_empty() {
            let outcome =
                handle_local(&self.state, &mut self.ctx, &mut self.tx, route, raw_sql).await;
            // SHOW 的 Nothing：转发到 prototype（对应 Java 未知 SHOW → prototype）
            if matches!(outcome, LocalOutcome::Nothing) {
                let target = self.state.metadata().prototype.clone();
                self.proxy_to(raw_sql, &target, more).await?;
                return Ok(0);
            }
            self.write_local_outcome(outcome, more)
                .await
                .map_err(OneErr::Io)?;
            return Ok(0);
        }
        let resp = execute_route(&self.state, &mut self.ctx, &mut self.tx, route)
            .await
            .map_err(|e| {
                let (errno, sqlstate, msg) = e.to_packet_parts();
                OneErr::Packet(build_error(errno, &sqlstate, &msg))
            })?;
        let rows = match resp {
            ExecResponse::Ok {
                affected_rows,
                last_insert_id,
            } => {
                let mut w = self.writer();
                w.write_ok(affected_rows, last_insert_id, more);
                drop(w);
                self.flush().await.map_err(OneErr::Io)?;
                affected_rows
            }
            ExecResponse::Rows(rs) => {
                let n = rs.row_payloads.len() as u64;
                self.write_result_set(&rs, more).await.map_err(OneErr::Io)?;
                n
            }
            ExecResponse::Empty => {
                let mut w = self.writer();
                w.write_ok(0, 0, more);
                drop(w);
                self.flush().await.map_err(OneErr::Io)?;
                0
            }
        };
        // ALTER 后回读物理建表 SQL 刷新配置 create_sql（对应 Java AlterTableSQLHandler；
        // 物理 ALTER 已生效，回读尽力而为）
        if route.stmt_type == StmtType::Ddl {
            crate::ddl::refresh_create_sql_after_alter(
                &self.state,
                self.ctx.schema.as_deref(),
                raw_sql,
            )
            .await;
        }
        // 语句结束且非事务 → 释放语句级连接（对应 closeStatementState）
        self.tx.close_statement_state(self.ctx.autocommit);
        Ok(rows)
    }

    /// 将原始 SQL 透传到指定 target（prototype 元数据查询等）。
    async fn proxy_to(&mut self, sql: &str, target: &str, more: bool) -> Result<(), OneErr> {
        let route = mycat3_sql::RouteResult {
            stmt_type: StmtType::Other,
            tasks: vec![mycat3_sql::BackendTask::new(
                target.to_string(),
                sql.to_string(),
            )],
            merge: None,
            use_schema: None,
            hint: None,
            explain_rows: vec![],
        };
        let resp = execute_route(&self.state, &mut self.ctx, &mut self.tx, &route)
            .await
            .map_err(|e| {
                let (errno, sqlstate, msg) = e.to_packet_parts();
                OneErr::Packet(build_error(errno, &sqlstate, &msg))
            })?;
        match resp {
            ExecResponse::Ok {
                affected_rows,
                last_insert_id,
            } => {
                let mut w = self.writer();
                w.write_ok(affected_rows, last_insert_id, more);
                drop(w);
                self.flush().await.map_err(OneErr::Io)?;
            }
            ExecResponse::Rows(rs) => self.write_result_set(&rs, more).await.map_err(OneErr::Io)?,
            ExecResponse::Empty => {
                let mut w = self.writer();
                w.write_ok(0, 0, more);
                drop(w);
                self.flush().await.map_err(OneErr::Io)?;
            }
        }
        Ok(())
    }

    /// 引擎结果行数（慢 SQL/统计用）。
    fn outcome_rows(o: &crate::engine::EngineOutcome) -> u64 {
        match o {
            crate::engine::EngineOutcome::Rows { rows, .. } => rows.len() as u64,
            crate::engine::EngineOutcome::Ok { affected_rows, .. } => *affected_rows,
        }
    }

    /// 写本地结果集/错误。
    async fn write_local_outcome(
        &mut self,
        outcome: LocalOutcome,
        more: bool,
    ) -> Result<(), FrontendError> {
        let deprecate = self.ctx.deprecate_eof;
        match outcome {
            LocalOutcome::Ok | LocalOutcome::Nothing => {
                let mut w = self.writer();
                w.write_ok(0, 0, more);
            }
            LocalOutcome::Error {
                errno,
                sqlstate,
                message,
            } => {
                let mut w = self.writer();
                w.write_error(errno, &sqlstate, &message);
            }
            LocalOutcome::Rows { columns, rows } => {
                {
                    let mut w = self.writer();
                    w.write_column_count(columns.len());
                    for c in &columns {
                        w.write_column_def(c);
                    }
                    // 列结束 EOF：仅非 DEPRECATE_EOF 客户端需要
                    if !deprecate {
                        w.write_eof(false);
                    }
                }
                {
                    let mut w = self.writer();
                    for row in &rows {
                        let values: Vec<Option<&[u8]>> = row
                            .iter()
                            .map(|v| v.as_ref().map(|s| s.as_bytes()))
                            .collect();
                        w.write_text_row(&values);
                    }
                    w.write_eof(more);
                }
            }
        }
        self.flush().await?;
        Ok(())
    }

    /// 写后端结果集（原始载荷透传 + DEPRECATE_EOF 语义）。
    /// 对应 `MysqlPayloadObjectObserver.onNext/onComplete`。
    async fn write_result_set(
        &mut self,
        rs: &mycat3_client::ResultSet,
        more: bool,
    ) -> Result<(), FrontendError> {
        let deprecate = self.ctx.deprecate_eof;
        {
            let mut w = self.writer();
            if rs.column_count == 0 {
                // 空结果（后端返回 OK 的语句被归并为空集）→ 写 OK
                w.write_ok(0, 0, more);
            } else {
                w.write_column_count(rs.column_count);
                for payload in &rs.column_def_payloads {
                    w.write_column_def_raw(payload);
                }
                // 列结束 EOF（非 DEPRECATE_EOF 客户端）
                if !deprecate {
                    w.write_eof(false);
                }
                // 分块 flush：每 64KB 写一次（协议允许任意分段；峰值内存减半、
                // 首字节延迟从"整包传输时间"降为"首个分块时间"）
                for row in &rs.row_payloads {
                    w.write_row_raw(row);
                    if w.out.len() >= 64 * 1024 {
                        drop(w);
                        self.flush().await?;
                        w = self.writer();
                    }
                }
                w.write_eof(more);
            }
        }
        // MORE_RESULTS 状态位已由 write_eof/write_ok 内部处理
        self.flush().await?;
        Ok(())
    }

    // ------------------------------------------------------------------
    // 预处理语句（COM_STMT_*）
    // ------------------------------------------------------------------
    async fn handle_stmt_prepare(&mut self, sql: &str) -> Result<(), FrontendError> {
        let num_params = count_placeholders(sql);
        let stmt_id = self.ctx.next_stmt_id;
        self.ctx.next_stmt_id += 1;
        self.prepared.insert(
            stmt_id,
            PreparedStmt {
                sql: sql.to_string(),
                num_params,
            },
        );
        let deprecate = self.ctx.deprecate_eof;
        {
            let mut w = self.writer();
            // PrepareOK：列数 0（Pro 不预计算列元数据），参数 num_params
            let payload = build_prepare_ok(stmt_id, 0, num_params as u16, 0);
            w.sender.push(w.out, &payload);
            if num_params > 0 {
                let param_def =
                    ColumnDef::new("?", mycat3_protocol::consts::field_type::VAR_STRING).encode();
                for _ in 0..num_params {
                    w.sender.push(w.out, &param_def);
                }
                if !deprecate {
                    w.write_eof(false);
                }
            }
            // 无列定义：DEPRECATE_EOF 客户端不需要 EOF；否则补一个
            if !deprecate {
                w.write_eof(false);
            }
        }
        self.flush().await?;
        Ok(())
    }

    async fn handle_stmt_execute(&mut self, body: &[u8]) -> Result<(), FrontendError> {
        if body.len() < 4 {
            let mut w = self.writer();
            w.write_error(
                error_code::ER_UNKNOWN_COM_ERROR,
                "08S01",
                "bad COM_STMT_EXECUTE payload",
            );
            self.flush().await?;
            return Ok(());
        }
        let stmt_id = u32::from_le_bytes([body[0], body[1], body[2], body[3]]);
        let stmt = match self.prepared.get(&stmt_id) {
            Some(s) => (s.sql.clone(), s.num_params),
            None => {
                let mut w = self.writer();
                w.write_error(
                    error_code::ER_UNKNOWN_ERROR,
                    "HY000",
                    &format!("Unknown prepared statement handler ({})", stmt_id),
                );
                self.flush().await?;
                return Ok(());
            }
        };
        // 参数值：SEND_LONG_DATA 优先（Java BindValueUtil 相同语义）
        let params = match read_binary_params(body, stmt.1) {
            Ok(p) => p,
            Err(e) => {
                let mut w = self.writer();
                w.write_error(
                    error_code::ER_UNKNOWN_ERROR,
                    "HY000",
                    &format!("bad stmt params: {}", e),
                );
                self.flush().await?;
                return Ok(());
            }
        };
        let long = self.long_data.get(&stmt_id).cloned().unwrap_or_default();
        // 参数值带字段类型：字符串类加引号、BLOB 类 X'hex'、数值类裸写
        //（旧实现按内容猜类型，VARCHAR "123" 会被当数值裸写 → 隐式转换/索引失效）
        let params: Vec<Option<(String, u8)>> = params
            .into_iter()
            .enumerate()
            .map(|(i, v)| match (v, long.get(&(i as u16))) {
                (Some((_, t)), Some(bytes)) => Some((render_param_bytes(bytes, t), t)),
                (None, Some(bytes)) => Some((
                    String::from_utf8_lossy(bytes).to_string(),
                    mycat3_protocol::consts::field_type::VAR_STRING,
                )),
                (v, None) => v,
            })
            .collect();
        // 参数替换为字面量 → 走普通查询路径 → 结果转二进制协议回写
        let sql = substitute_params(&stmt.0, &params);
        debug!(stmt_id, sql = %sql, "stmt execute");

        let started = Instant::now();
        self.state.stats.incr_query();
        // 纯代理模式：预处理语句同样收敛为单目标透传（拦截/权限/缓存照常）
        if self.state.is_proxy_mode() {
            if let Some(e) = crate::intercept::check(&self.state.intercepts(), &sql) {
                self.state.stats.incr_error();
                let mut w = self.writer();
                w.write_error(e.errno, &e.sqlstate, &e.message);
                self.flush().await?;
                return Ok(());
            }
            let stmt_type = crate::engine::proxy_stmt_type(&sql);
            let fake = [mycat3_sql::RouteResult {
                stmt_type,
                tasks: Vec::new(),
                merge: None,
                use_schema: None,
                hint: None,
                explain_rows: Vec::new(),
            }];
            if let Some(e) = crate::engine::check_disallow(&self.state, &self.ctx, &fake) {
                self.state.stats.incr_error();
                let mut w = self.writer();
                w.write_error(e.errno, &e.sqlstate, &e.message);
                self.flush().await?;
                return Ok(());
            }
            let route = crate::engine::proxy_route(&self.state, &sql);
            let exec_timeout = {
                let cfg = self.state.config();
                let t = cfg.server.server.execute_timeout;
                if t > 0 {
                    Some(std::time::Duration::from_millis(t))
                } else {
                    None
                }
            };
            match crate::executor::execute_route_proxy(
                &self.state,
                &mut self.ctx,
                &mut self.tx,
                &route,
                exec_timeout,
            )
            .await
            {
                Ok(ExecResponse::Rows(rs)) => {
                    self.write_binary_result_set(&rs).await?;
                }
                Ok(ExecResponse::Ok {
                    affected_rows,
                    last_insert_id,
                }) => {
                    let mut w = self.writer();
                    w.write_ok(affected_rows, last_insert_id, false);
                    self.flush().await?;
                }
                Ok(ExecResponse::Empty) => {
                    let mut w = self.writer();
                    w.write_ok(0, 0, false);
                    self.flush().await?;
                }
                Err(e) => {
                    self.state.stats.incr_error();
                    let (errno, sqlstate, msg) = e.to_packet_parts();
                    let mut w = self.writer();
                    w.write_error(errno, &sqlstate, &msg);
                    self.flush().await?;
                }
            }
            self.tx.close_statement_state(self.ctx.autocommit);
            return Ok(());
        }
        let meta = self.state.metadata();
        let routes = match analyse(&sql, self.ctx.schema.as_deref(), &meta) {
            Ok(r) => r,
            Err(e) => {
                self.state.stats.incr_error();
                let mut w = self.writer();
                w.write_error_payload(sql_error_packet(&e));
                self.flush().await?;
                return Ok(());
            }
        };
        if routes.len() != 1 {
            let mut w = self.writer();
            w.write_error(
                error_code::ER_NOT_SUPPORTED_YET,
                "42000",
                "prepared multi-statement execution is not supported",
            );
            self.flush().await?;
            return Ok(());
        }
        let route = &routes[0];
        // 多节点路由已放开：读走归并、写按节点顺序执行（execute_route 语义）
        let _ = route.tasks.len();
        if route.tasks.is_empty() {
            // 本地语句（SET 等）
            let outcome = handle_local(&self.state, &mut self.ctx, &mut self.tx, route, &sql).await;
            self.write_local_outcome_binary(outcome).await?;
            return Ok(());
        }
        match execute_route(&self.state, &mut self.ctx, &mut self.tx, route).await {
            Ok(ExecResponse::Ok {
                affected_rows,
                last_insert_id,
            }) => {
                let mut w = self.writer();
                w.write_ok(affected_rows, last_insert_id, false);
                self.flush().await?;
            }
            Ok(ExecResponse::Rows(rs)) => {
                self.write_binary_result_set(&rs).await?;
            }
            Ok(ExecResponse::Empty) => {
                let mut w = self.writer();
                w.write_ok(0, 0, false);
                self.flush().await?;
            }
            Err(e) => {
                self.state.stats.incr_error();
                let (errno, sqlstate, msg) = e.to_packet_parts();
                let mut w = self.writer();
                w.write_error(errno, &sqlstate, &msg);
                self.flush().await?;
            }
        }
        let _ = started;
        self.tx.close_statement_state(self.ctx.autocommit);
        Ok(())
    }

    /// 二进制协议结果集：列类型统一改写为 VAR_STRING，行值按 lenenc 字符串编码。
    async fn write_binary_result_set(
        &mut self,
        rs: &mycat3_client::ResultSet,
    ) -> Result<(), FrontendError> {
        let deprecate = self.ctx.deprecate_eof;
        {
            let mut w = self.writer();
            w.write_column_count(rs.column_count);
            for c in &rs.columns {
                let mut def = c.clone();
                def.field_type = mycat3_protocol::consts::field_type::VAR_STRING;
                def.charset = mycat3_protocol::consts::charset::UTF8MB4_GENERAL_CI as u16;
                w.write_column_def(&def);
            }
            if !deprecate {
                w.write_eof(false);
            }
            for raw in &rs.row_payloads {
                let values = mycat3_client::decode_text_row(raw, rs.column_count);
                let binary_values: Vec<Option<Vec<u8>>> = values
                    .iter()
                    .map(|v| {
                        v.as_ref().map(|bytes| {
                            let mut enc = mycat3_protocol::PacketWriter::new();
                            enc.write_lenenc_bytes(bytes);
                            enc.into_bytes()
                        })
                    })
                    .collect();
                let row = mycat3_protocol::packets::build_binary_row(&binary_values);
                w.write_row_raw(&row);
            }
            w.write_eof(false);
        }
        self.flush().await?;
        Ok(())
    }

    async fn write_local_outcome_binary(
        &mut self,
        outcome: LocalOutcome,
    ) -> Result<(), FrontendError> {
        let deprecate = self.ctx.deprecate_eof;
        match outcome {
            LocalOutcome::Rows { columns, rows } => {
                {
                    let mut w = self.writer();
                    w.write_column_count(columns.len());
                    for c in &columns {
                        w.write_column_def(c);
                    }
                    if !deprecate {
                        w.write_eof(false);
                    }
                }
                {
                    let mut w = self.writer();
                    for row in &rows {
                        let binary_values: Vec<Option<Vec<u8>>> = row
                            .iter()
                            .map(|v| {
                                v.as_ref().map(|s| {
                                    let mut enc = mycat3_protocol::PacketWriter::new();
                                    enc.write_lenenc_bytes(s.as_bytes());
                                    enc.into_bytes()
                                })
                            })
                            .collect();
                        let payload = mycat3_protocol::packets::build_binary_row(&binary_values);
                        w.write_row_raw(&payload);
                    }
                    w.write_eof(false);
                }
                self.flush().await?;
            }
            LocalOutcome::Error {
                errno,
                sqlstate,
                message,
            } => {
                let mut w = self.writer();
                w.write_error(errno, &sqlstate, &message);
                self.flush().await?;
            }
            _ => {
                let mut w = self.writer();
                w.write_ok(0, 0, false);
                self.flush().await?;
            }
        }
        Ok(())
    }

    // ------------------------------------------------------------------
    // 基础 IO
    // ------------------------------------------------------------------
    async fn read_packet(&mut self) -> Result<(u8, Vec<u8>), FrontendError> {
        let mut buf = [0u8; 16 * 1024];
        loop {
            if let Some(p) = self.pending.pop_front() {
                return Ok((p.seq, p.payload));
            }
            let n = self.socket.read(&mut buf).await?;
            if n == 0 {
                return Err(FrontendError::Closed);
            }
            self.state.stats.add_in(n as u64);
            for p in self.decoder.feed(&buf[..n])? {
                self.pending.push_back(p);
            }
        }
    }

    fn writer(&mut self) -> SessionWriter<'_> {
        SessionWriter::new(&mut self.ctx, &mut self.sender, &mut self.out)
    }

    async fn flush(&mut self) -> Result<(), FrontendError> {
        let n = self.out.len() as u64;
        if n == 0 {
            return Ok(());
        }
        self.socket
            .write_all(&std::mem::take(&mut self.out))
            .await?;
        self.socket.flush().await?;
        self.out = Vec::with_capacity(8192);
        self.state.stats.add_out(n);
        Ok(())
    }

    async fn write_error_and_flush(
        &mut self,
        errno: u16,
        sqlstate: &str,
        msg: &str,
    ) -> Result<(), FrontendError> {
        {
            let mut w = self.writer();
            w.write_error(errno, sqlstate, msg);
        }
        self.flush().await
    }
}

/// 将协议无关执行结果写回 MySQL 前端（文本协议）。
impl Frontend {
    async fn write_engine_outcome(
        &mut self,
        outcome: &crate::engine::EngineOutcome,
        more: bool,
    ) -> Result<(), FrontendError> {
        match outcome {
            crate::engine::EngineOutcome::Ok {
                affected_rows,
                last_insert_id,
            } => {
                let mut w = self.writer();
                w.write_ok(*affected_rows, *last_insert_id, more);
                self.flush().await?;
            }
            crate::engine::EngineOutcome::Rows { columns, rows } => {
                let deprecate = self.ctx.deprecate_eof;
                {
                    let mut w = self.writer();
                    w.write_column_count(columns.len());
                    for c in columns {
                        let def = mycat3_protocol::packets::ColumnDef {
                            schema: String::new(),
                            table: String::new(),
                            org_table: String::new(),
                            name: c.name.clone(),
                            org_name: c.name.clone(),
                            charset: c.charset,
                            column_length: 255,
                            field_type: c.mysql_type,
                            flags: 0,
                            decimals: 0,
                        };
                        w.write_column_def(&def);
                    }
                    if !deprecate {
                        w.write_eof(false);
                    }
                }
                {
                    let mut w = self.writer();
                    for row in rows {
                        let values: Vec<Option<&[u8]>> = row
                            .iter()
                            .map(|v| v.as_ref().map(|s| s.as_bytes()))
                            .collect();
                        w.write_text_row(&values);
                    }
                    w.write_eof(more);
                }
                self.flush().await?;
            }
        }
        Ok(())
    }
}

/// 命令名（PROCESSLIST Command 列）。
fn cmd_name(cmd: u8) -> &'static str {
    match cmd {
        COM_QUERY => "Query",
        COM_PING => "Ping",
        COM_INIT_DB => "Init DB",
        COM_QUIT => "Quit",
        COM_STMT_PREPARE => "Prepare",
        COM_STMT_EXECUTE => "Execute",
        COM_STMT_CLOSE => "Close stmt",
        COM_FIELD_LIST => "Field List",
        _ => "Other",
    }
}

fn sql_error_packet(e: &SqlError) -> Vec<u8> {
    match e {
        SqlError::Parse(m) => mycat3_protocol::packets::err_parse(m),
        SqlError::UnknownDatabase(db) => mycat3_protocol::packets::err_bad_db(db),
        SqlError::NoDatabase => {
            build_error(error_code::ER_NO_DB_ERROR, "3D000", "No database selected")
        }
        SqlError::Unsupported(m) => mycat3_protocol::packets::err_not_supported(m),
        SqlError::Route(m) => build_error(error_code::ER_UNKNOWN_ERROR, "HY000", m),
    }
}

// ---------------------------------------------------------------------------
// 参数替换（COM_STMT_EXECUTE → 文本 SQL）
// ---------------------------------------------------------------------------

/// 统计 SQL 中字面量外的 `?` 数量（跳过引号/反引号/注释）。
pub fn count_placeholders(sql: &str) -> usize {
    let mut count = 0;
    let bytes = sql.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        let c = bytes[i];
        match c {
            b'\'' | b'"' | b'`' => {
                let quote = c;
                i += 1;
                while i < bytes.len() {
                    if bytes[i] == b'\\' && quote != b'`' {
                        i += 2;
                        continue;
                    }
                    if bytes[i] == quote {
                        // 双写转义 ''
                        if i + 1 < bytes.len() && bytes[i + 1] == quote {
                            i += 2;
                            continue;
                        }
                        break;
                    }
                    i += 1;
                }
            }
            b'-' if i + 1 < bytes.len() && bytes[i + 1] == b'-' => {
                while i < bytes.len() && bytes[i] != b'\n' {
                    i += 1;
                }
            }
            b'/' if i + 1 < bytes.len() && bytes[i + 1] == b'*' => {
                i += 2;
                while i + 1 < bytes.len() && !(bytes[i] == b'*' && bytes[i + 1] == b'/') {
                    i += 1;
                }
                i += 1;
            }
            b'?' => {
                count += 1;
            }
            _ => {}
        }
        i += 1;
    }
    count
}

/// 将参数按顺序替换为 SQL 字面量（转义单引号/反斜杠）。
/// 以字节扫描但仅在 ASCII 边界切片，多字节 UTF-8 原样保留。
pub fn substitute_params(sql: &str, params: &[Option<(String, u8)>]) -> String {
    let mut out = String::with_capacity(sql.len() + 32);
    let bytes = sql.as_bytes();
    let mut seg_start = 0usize;
    let mut i = 0usize;
    let mut p = 0usize;
    while i < bytes.len() {
        let c = bytes[i];
        if c == b'\'' || c == b'"' || c == b'`' {
            // 跳过引号段（含转义与双写引号）
            let quote = c;
            i += 1;
            while i < bytes.len() {
                if bytes[i] == b'\\' && quote != b'`' {
                    i += 2;
                    continue;
                }
                if bytes[i] == quote {
                    if i + 1 < bytes.len() && bytes[i + 1] == quote {
                        i += 2;
                        continue;
                    }
                    break;
                }
                i += 1;
            }
            i = (i + 1).min(bytes.len());
            continue;
        }
        if c == b'?' {
            out.push_str(&sql[seg_start..i]);
            match params.get(p) {
                Some(Some((v, t))) => out.push_str(&quote_literal(v, *t)),
                _ => out.push_str("NULL"),
            }
            p += 1;
            i += 1;
            seg_start = i;
            continue;
        }
        i += 1;
    }
    out.push_str(&sql[seg_start..]);
    out
}

/// 按字段类型渲染参数字面量：
/// - 数值类型：裸写（解析阶段已产出合法数值文本）
/// - BLOB 类：`X'hex'` 字面量（解析阶段已产出十六进制文本）
/// - 其余（字符串/时间/JSON/ENUM/SET）：`'...'` 转义引用
fn quote_literal(v: &str, t: u8) -> String {
    use mycat3_protocol::consts::field_type;
    if matches!(
        t,
        field_type::DECIMAL
            | field_type::TINY
            | field_type::SHORT
            | field_type::LONG
            | field_type::FLOAT
            | field_type::DOUBLE
            | field_type::LONGLONG
            | field_type::INT24
            | field_type::YEAR
            | field_type::NEWDECIMAL
    ) {
        return v.to_string();
    }
    if mycat3_protocol::packets::is_blob_field_type(t) {
        let mut s = String::with_capacity(v.len() + 3);
        s.push_str("X'");
        s.push_str(v);
        s.push('\'');
        return s;
    }
    let mut s = String::with_capacity(v.len() + 2);
    s.push('\'');
    for ch in v.chars() {
        match ch {
            '\'' => s.push_str("''"),
            '\\' => s.push_str("\\\\"),
            '\0' => s.push_str("\\0"),
            '\n' => s.push_str("\\n"),
            '\r' => s.push_str("\\r"),
            '\u{1a}' => s.push_str("\\Z"),
            c => s.push(c),
        }
    }
    s.push('\'');
    s
}

/// SEND_LONG_DATA 字节按声明类型渲染：BLOB 类 → 十六进制文本，其余 → 有损文本。
fn render_param_bytes(bytes: &[u8], t: u8) -> String {
    if mycat3_protocol::packets::is_blob_field_type(t) {
        mycat3_protocol::packets::hex_text(bytes)
    } else {
        String::from_utf8_lossy(bytes).to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_count_placeholders() {
        assert_eq!(count_placeholders("select * from t where a=? and b=?"), 2);
        assert_eq!(count_placeholders("select '?' from t where a=?"), 1);
        assert_eq!(count_placeholders("select * from t"), 0);
        assert_eq!(count_placeholders("insert into t values (?, '?', ?)"), 2);
        assert_eq!(count_placeholders("select * from t -- ?\n where a=?"), 1);
        assert_eq!(count_placeholders("select /* ? */ ? from t"), 1);
        assert_eq!(count_placeholders(r#"select "a?" from t where b=?"#), 1);
    }

    #[test]
    fn test_substitute_params() {
        use mycat3_protocol::consts::field_type as ft;
        assert_eq!(
            substitute_params(
                "select * from t where id=? and name=?",
                &[
                    Some(("5".into(), ft::LONGLONG)),
                    Some(("o'brien".into(), ft::VAR_STRING))
                ]
            ),
            "select * from t where id=5 and name='o''brien'"
        );
        assert_eq!(
            substitute_params("insert into t values (?)", &[None]),
            "insert into t values (NULL)"
        );
        // 引号内 ? 不替换
        assert_eq!(
            substitute_params("select '?' , ?", &[Some(("x".into(), ft::VAR_STRING))]),
            "select '?' , 'x'"
        );
        // 字符串参数带反斜杠
        assert_eq!(
            substitute_params("select ?", &[Some((r"a\b".into(), ft::VAR_STRING))]),
            r"select 'a\\b'"
        );
        // 参数不足 → NULL；字符串类型 "1" 必须加引号（类型语义）
        assert_eq!(
            substitute_params("select ?,?", &[Some(("1".into(), ft::VAR_STRING))]),
            "select '1',NULL"
        );
    }

    #[test]
    fn test_quote_literal_typed() {
        use mycat3_protocol::consts::field_type as ft;
        assert_eq!(quote_literal("123", ft::LONGLONG), "123");
        assert_eq!(quote_literal("-1.5", ft::DECIMAL), "-1.5");
        // 字符串类型即使"看起来像数字"也必须引号（旧实现按内容猜类型 → 语义错误）
        assert_eq!(quote_literal("123", ft::VAR_STRING), "'123'");
        assert_eq!(quote_literal("abc", ft::VAR_STRING), "'abc'");
        assert_eq!(quote_literal("12a", ft::VAR_STRING), "'12a'");
        assert_eq!(quote_literal("", ft::VAR_STRING), "''");
        // BLOB 类 → X'hex'
        assert_eq!(quote_literal("0a0b", ft::BLOB), "X'0a0b'");
    }
}

/// 精确读取 n 字节。
async fn read_exact<S: tokio::io::AsyncRead + Unpin>(
    sock: &mut S,
    buf: &mut [u8],
) -> Result<(), FrontendError> {
    use tokio::io::AsyncReadExt;
    sock.read_exact(buf).await?;
    Ok(())
}
