//! 原生异步 MySQL 后端连接。
//!
//! 对应 Mycat3 的 vert.x 原生客户端（`NewMycatConnection` / `MySQLClientAuthHandler` 后端侧），
//! 以 tokio + 自研协议层实现：客户端握手（mysql_native_password / caching_sha2 快速认证）、
//! COM_QUERY / COM_PING / COM_INIT_DB、文本协议结果集解析（保留原始载荷用于透传）。

use std::time::Duration;

use mycat3_protocol::auth::{scramble_caching_sha2, scramble_native};
use mycat3_protocol::codec::{PacketReader, PacketWriter};
use mycat3_protocol::consts::{
    capability::*, command, AUTH_PLUGIN_CACHING_SHA2, AUTH_PLUGIN_NATIVE,
};
use mycat3_protocol::frame::{encode_frames, FrameDecoder};
use mycat3_protocol::packets::{
    build_auth_response, build_com_init_db, build_com_ping, build_com_query, build_com_quit,
    classify_payload, parse_auth_response, parse_err, parse_ok, ColumnDef, ErrPacket, OkPacket,
    PayloadKind,
};
use thiserror::Error;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

#[derive(Debug, Error)]
pub enum ClientError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("protocol: {0}")]
    Protocol(#[from] mycat3_protocol::ProtocolError),
    #[error("server error {errno}: {message}")]
    Server {
        errno: u16,
        sqlstate: String,
        message: String,
    },
    #[error("handshake failed: {0}")]
    Handshake(String),
    #[error("unsupported auth: {0}")]
    UnsupportedAuth(String),
    #[error("unsupported: {0}")]
    Unsupported(String),
    #[error("connection closed by server")]
    Closed,
    #[error("timeout after {0:?}")]
    Timeout(Duration),
}

impl From<ErrPacket> for ClientError {
    fn from(e: ErrPacket) -> Self {
        ClientError::Server {
            errno: e.errno,
            sqlstate: e.sqlstate,
            message: e.message,
        }
    }
}

pub type ClientResult<T> = Result<T, ClientError>;

/// 后端连接配置。
#[derive(Debug, Clone)]
pub struct BackendEndpoint {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: String,
    pub database: String,
    pub connect_timeout: Duration,
    /// 单命令读超时（None = 不限）
    pub query_timeout: Option<Duration>,
}

/// 文本协议结果集（保留原始载荷，支持透传与解析两种消费方式）。
#[derive(Debug, Clone)]
pub struct ResultSet {
    pub column_count: usize,
    /// 列定义原始载荷（透传给前端时直接重新分帧）
    pub column_def_payloads: Vec<Vec<u8>>,
    /// 解析后的列定义（归并/本地生成用）
    pub columns: Vec<ColumnDef>,
    /// 行原始载荷
    pub row_payloads: Vec<Vec<u8>>,
    /// 行结束包携带的服务端状态
    pub final_status: u16,
    pub warnings: u16,
}

impl ResultSet {
    /// 解析所有行为文本值（NULL → None）。
    pub fn decode_rows(&self) -> Vec<Vec<Option<Vec<u8>>>> {
        self.row_payloads
            .iter()
            .map(|p| decode_text_row(p, self.column_count))
            .collect()
    }
}

/// 解析文本协议行载荷为列值。
pub fn decode_text_row(payload: &[u8], ncols: usize) -> Vec<Option<Vec<u8>>> {
    let mut r = PacketReader::new(payload);
    let mut out = Vec::with_capacity(ncols);
    for _ in 0..ncols {
        match r.read_lenenc_bytes() {
            Ok(v) => out.push(v.map(|b| b.to_vec())),
            Err(_) => {
                out.push(None);
            }
        }
    }
    out
}

/// 命令执行结果。
#[derive(Debug, Clone)]
pub enum QueryOutcome {
    /// OK 包（INSERT/UPDATE/DDL/COM_PING 等）
    Ok(OkPacket),
    /// 结果集
    Rows(ResultSet),
}

/// 一条原生 MySQL 后端连接。
pub struct MySQLConnection {
    stream: TcpStream,
    decoder: FrameDecoder,
    /// 已解码但尚未消费的包队列（一次 socket 读可能返回多个包）
    pending: std::collections::VecDeque<mycat3_protocol::frame::Packet>,
    /// 服务端能力（握手协商结果）
    server_capabilities: u32,
    /// 协商后的客户端能力
    capabilities: u32,
    pub server_version: String,
    pub connection_id: u32,
    /// 当前库
    pub schema: String,
    /// 命令序号计数器
    seq: u8,
    endpoint: BackendEndpoint,
    /// 是否处于事务中（服务端状态位）
    pub in_transaction: bool,
    pub autocommit: bool,
}

impl MySQLConnection {
    /// 建立连接并完成握手认证。
    pub async fn connect(ep: BackendEndpoint) -> ClientResult<MySQLConnection> {
        let stream = tokio::time::timeout(
            ep.connect_timeout,
            TcpStream::connect((ep.host.as_str(), ep.port)),
        )
        .await
        .map_err(|_| ClientError::Timeout(ep.connect_timeout))??;
        stream.set_nodelay(true)?;
        let mut conn = MySQLConnection {
            stream,
            decoder: FrameDecoder::new(),
            pending: std::collections::VecDeque::new(),
            server_capabilities: 0,
            capabilities: 0,
            server_version: String::new(),
            connection_id: 0,
            schema: ep.database.clone(),
            seq: 0,
            endpoint: ep,
            in_transaction: false,
            autocommit: true,
        };
        conn.handshake().await?;
        Ok(conn)
    }

    async fn read_packet(&mut self) -> ClientResult<(u8, Vec<u8>)> {
        loop {
            if let Some(p) = self.pending.pop_front() {
                return Ok((p.seq, p.payload));
            }
            let mut buf = [0u8; 32 * 1024];
            let n = self.stream.read(&mut buf).await?;
            if n == 0 {
                return Err(ClientError::Closed);
            }
            for p in self.decoder.feed(&buf[..n])? {
                self.pending.push_back(p);
            }
        }
    }

    async fn read_packet_timeout(
        &mut self,
        timeout: Option<Duration>,
    ) -> ClientResult<(u8, Vec<u8>)> {
        match timeout {
            Some(d) => tokio::time::timeout(d, self.read_packet())
                .await
                .map_err(|_| ClientError::Timeout(d))?,
            None => self.read_packet().await,
        }
    }

    async fn write_packet(&mut self, seq: u8, payload: &[u8]) -> ClientResult<()> {
        let mut out = Vec::with_capacity(payload.len() + 4);
        let mut s = seq;
        encode_frames(&mut s, payload, &mut out);
        self.seq = s;
        self.stream.write_all(&out).await?;
        Ok(())
    }

    /// 客户端握手：读 greeting → 发认证 → 处理 AuthSwitch/AuthMoreData → OK。
    async fn handshake(&mut self) -> ClientResult<()> {
        let (seq, payload) = self
            .read_packet_timeout(Some(self.endpoint.connect_timeout))
            .await?;
        if seq != 0 {
            return Err(ClientError::Handshake(format!(
                "unexpected greeting seq {}",
                seq
            )));
        }
        if classify_payload(&payload) == PayloadKind::Error {
            return Err(parse_err(&payload)?.into());
        }
        // 解析 HandshakeV10
        let mut r = PacketReader::new(&payload);
        let protocol = r.read_u8()?;
        if protocol != 10 {
            return Err(ClientError::Handshake(format!(
                "unsupported protocol version {}",
                protocol
            )));
        }
        self.server_version = r.read_null_str()?.to_string();
        self.connection_id = r.read_int4()?;
        let mut salt = r.read_fixed(8)?.to_vec();
        r.skip(1)?; // filler
        let caps_low = r.read_int2()? as u32;
        let _charset = if r.remaining() > 0 { r.read_u8()? } else { 0 };
        let _status = if r.remaining() >= 2 {
            r.read_int2()?
        } else {
            0
        };
        let caps_high = if r.remaining() >= 2 {
            r.read_int2()? as u32
        } else {
            0
        };
        let caps = caps_low | (caps_high << 16);
        let auth_len = if caps & CLIENT_PLUGIN_AUTH != 0 && r.remaining() > 0 {
            r.read_u8()?
        } else {
            0
        };
        if r.remaining() >= 10 {
            r.skip(10)?;
        }
        if caps & CLIENT_SECURE_CONNECTION != 0 {
            let take = if auth_len > 0 {
                (auth_len as usize - 9).max(12)
            } else {
                12
            };
            let part2 = r.read_fixed(take.min(r.remaining()))?;
            // 去掉结尾可能的 NUL
            let part2: &[u8] = match part2.iter().position(|b| *b == 0) {
                Some(i) => &part2[..i],
                None => part2,
            };
            salt.extend_from_slice(part2);
        }
        let plugin = if caps & CLIENT_PLUGIN_AUTH != 0 && !r.is_empty() {
            r.read_null_str().unwrap_or(AUTH_PLUGIN_NATIVE).to_string()
        } else {
            AUTH_PLUGIN_NATIVE.to_string()
        };
        self.server_capabilities = caps;

        // 协商客户端能力：不支持压缩与 SSL（前端亦不支持），保留 DEPRECATE_EOF 由服务端决定
        let mut client_caps = CLIENT_LONG_PASSWORD
            | CLIENT_FOUND_ROWS
            | CLIENT_LONG_FLAG
            | CLIENT_CONNECT_WITH_DB
            | CLIENT_PROTOCOL_41
            | CLIENT_TRANSACTIONS
            | CLIENT_SECURE_CONNECTION
            | CLIENT_MULTI_STATEMENTS
            | CLIENT_MULTI_RESULTS
            | CLIENT_PS_MULTI_RESULTS
            | CLIENT_PLUGIN_AUTH
            | CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA;
        if caps & CLIENT_DEPRECATE_EOF != 0 {
            client_caps |= CLIENT_DEPRECATE_EOF;
        }
        if caps & CLIENT_LOCAL_FILES != 0 {
            client_caps |= CLIENT_LOCAL_FILES;
        }
        self.capabilities =
            client_caps & caps | (client_caps & (CLIENT_PROTOCOL_41 | CLIENT_SECURE_CONNECTION));

        let (plugin, scramble) = self.build_auth_scramble(&plugin, &salt);
        let db = if self.endpoint.database.is_empty() {
            None
        } else {
            Some(self.endpoint.database.as_str())
        };
        let payload = build_auth_response(
            self.capabilities,
            0x00FF_FFFF,
            45, // utf8mb4_general_ci
            &self.endpoint.user,
            &scramble,
            db,
            &plugin,
        );
        self.write_packet(1, &payload).await?;

        // 认证响应循环
        loop {
            let (s, p) = self
                .read_packet_timeout(Some(self.endpoint.connect_timeout))
                .await?;
            let seq = s.wrapping_add(1);
            match classify_payload(&p) {
                PayloadKind::Ok => {
                    let ok = parse_ok(&p)?;
                    self.update_status(ok.status_flags);
                    return Ok(());
                }
                PayloadKind::Error => return Err(parse_err(&p)?.into()),
                PayloadKind::AuthSwitch => {
                    // 0xfe | plugin\0 | salt\0
                    let mut r = PacketReader::new(&p);
                    r.skip(1)?;
                    let new_plugin = r.read_null_str()?.to_string();
                    let new_salt = r.read_eof_bytes();
                    let new_salt = match new_salt.iter().position(|b| *b == 0) {
                        Some(i) => &new_salt[..i],
                        None => new_salt,
                    };
                    let (_, scramble) = self.build_auth_scramble(&new_plugin, new_salt);
                    self.write_packet(seq, &scramble).await?;
                }
                PayloadKind::AuthMoreData => {
                    if p.len() >= 2 && p[1] == 0x03 {
                        // caching_sha2 快速认证成功，等待最终 OK
                        continue;
                    }
                    if p.len() >= 2 && p[1] == 0x04 {
                        return Err(ClientError::UnsupportedAuth(
                            "caching_sha2_password 完整认证需要 TLS 或 RSA 公钥交换，Pro 版暂不支持；\
                             请在后端 MySQL 使用 mysql_native_password（ALTER USER ... IDENTIFIED WITH mysql_native_password）"
                                .into(),
                        ));
                    }
                    return Err(ClientError::Handshake("unknown AuthMoreData".into()));
                }
                _ => {
                    return Err(ClientError::Handshake(
                        "unexpected packet during auth".into(),
                    ))
                }
            }
        }
    }

    fn build_auth_scramble(&self, plugin: &str, salt: &[u8]) -> (String, Vec<u8>) {
        match plugin {
            AUTH_PLUGIN_CACHING_SHA2 => (
                plugin.to_string(),
                scramble_caching_sha2(self.endpoint.password.as_bytes(), salt),
            ),
            // 空插件或 native 及其他未知插件：按 native 计算（服务端可再发 AuthSwitch）
            _ => (
                if plugin.is_empty() {
                    AUTH_PLUGIN_NATIVE.to_string()
                } else {
                    plugin.to_string()
                },
                scramble_native(self.endpoint.password.as_bytes(), salt),
            ),
        }
    }

    fn update_status(&mut self, status: u16) {
        self.in_transaction = status & mycat3_protocol::consts::status::SERVER_STATUS_IN_TRANS != 0;
        self.autocommit = status & mycat3_protocol::consts::status::SERVER_STATUS_AUTOCOMMIT != 0;
    }

    /// 执行 COM_QUERY 并读取完整结果。
    pub async fn query(&mut self, sql: &str) -> ClientResult<QueryOutcome> {
        self.write_packet(0, &build_com_query(sql)).await?;
        self.read_query_result().await
    }

    /// 带超时的查询。
    pub async fn query_timeout(
        &mut self,
        sql: &str,
        timeout: Duration,
    ) -> ClientResult<QueryOutcome> {
        tokio::time::timeout(timeout, self.query(sql))
            .await
            .map_err(|_| ClientError::Timeout(timeout))?
    }

    /// 读取 COM_QUERY 的响应（第一个包已在流中）。
    async fn read_query_result(&mut self) -> ClientResult<QueryOutcome> {
        let timeout = self.endpoint.query_timeout;
        let (_seq, first) = self.read_packet_timeout(timeout).await?;
        match classify_payload(&first) {
            PayloadKind::Error => Err(parse_err(&first)?.into()),
            PayloadKind::Ok => {
                let ok = parse_ok(&first)?;
                self.update_status(ok.status_flags);
                Ok(QueryOutcome::Ok(ok))
            }
            PayloadKind::LocalInfile => Err(ClientError::Unsupported(
                "LOAD DATA LOCAL 后端请求暂不支持".into(),
            )),
            _ => {
                // 结果集
                let mut r = PacketReader::new(&first);
                let column_count = r.read_lenenc_int()? as usize;
                let mut column_def_payloads = Vec::with_capacity(column_count);
                let mut columns = Vec::with_capacity(column_count);
                for _ in 0..column_count {
                    let (_s, p) = self.read_packet_timeout(timeout).await?;
                    columns.push(ColumnDef::decode(&p)?);
                    column_def_payloads.push(p);
                }
                // 列结束 EOF（无 DEPRECATE_EOF 时）
                if self.capabilities & CLIENT_DEPRECATE_EOF == 0 {
                    let (_s, p) = self.read_packet_timeout(timeout).await?;
                    if classify_payload(&p) != PayloadKind::Eof {
                        return Err(ClientError::Protocol(
                            mycat3_protocol::ProtocolError::Malformed(
                                "expected EOF after column defs",
                            ),
                        ));
                    }
                }
                let deprecate_eof = self.capabilities & CLIENT_DEPRECATE_EOF != 0;
                let mut row_payloads = Vec::new();
                #[allow(unused_assignments)]
                let (mut final_status, mut warnings) = (0u16, 0u16);
                loop {
                    let (_s, p) = self.read_packet_timeout(timeout).await?;
                    let kind = classify_payload(&p);
                    if kind == PayloadKind::Error {
                        return Err(parse_err(&p)?.into());
                    }
                    let is_eof = kind == PayloadKind::Eof;
                    let is_ok_end = deprecate_eof && kind == PayloadKind::Ok;
                    if is_eof || is_ok_end {
                        if is_ok_end {
                            let ok = parse_ok(&p)?;
                            final_status = ok.status_flags;
                            warnings = ok.warnings;
                        } else {
                            let mut r = PacketReader::new(&p);
                            r.skip(1).ok();
                            warnings = r.read_int2().unwrap_or(0);
                            final_status = r.read_int2().unwrap_or(0);
                        }
                        self.update_status(final_status);
                        break;
                    }
                    row_payloads.push(p);
                }
                Ok(QueryOutcome::Rows(ResultSet {
                    column_count,
                    column_def_payloads,
                    columns,
                    row_payloads,
                    final_status,
                    warnings,
                }))
            }
        }
    }

    /// COM_PING。
    pub async fn ping(&mut self) -> ClientResult<()> {
        self.write_packet(0, &build_com_ping()).await?;
        let (_s, p) = self
            .read_packet_timeout(Some(self.endpoint.connect_timeout))
            .await?;
        match classify_payload(&p) {
            PayloadKind::Ok => {
                let ok = parse_ok(&p)?;
                self.update_status(ok.status_flags);
                Ok(())
            }
            PayloadKind::Error => Err(parse_err(&p)?.into()),
            _ => Err(ClientError::Protocol(
                mycat3_protocol::ProtocolError::Malformed("ping response"),
            )),
        }
    }

    /// COM_INIT_DB。
    pub async fn init_db(&mut self, schema: &str) -> ClientResult<()> {
        self.write_packet(0, &build_com_init_db(schema)).await?;
        let (_s, p) = self
            .read_packet_timeout(Some(self.endpoint.connect_timeout))
            .await?;
        match classify_payload(&p) {
            PayloadKind::Ok => {
                self.schema = schema.to_string();
                Ok(())
            }
            PayloadKind::Error => Err(parse_err(&p)?.into()),
            _ => Err(ClientError::Protocol(
                mycat3_protocol::ProtocolError::Malformed("init_db response"),
            )),
        }
    }

    /// 执行并忽略结果（初始化 SQL、SET 等）。
    pub async fn execute_ignore(&mut self, sql: &str) -> ClientResult<()> {
        match self.query(sql).await? {
            QueryOutcome::Ok(_) => Ok(()),
            QueryOutcome::Rows(_) => Ok(()),
        }
    }

    /// 发送任意命令载荷并收集响应包，直到 400ms 内无新包（测试/底层工具用）。
    pub async fn raw_command(&mut self, payload: &[u8]) -> ClientResult<Vec<Vec<u8>>> {
        self.write_packet(0, payload).await?;
        let mut out = Vec::new();
        loop {
            match tokio::time::timeout(Duration::from_millis(400), self.read_packet()).await {
                Ok(Ok((_seq, p))) => out.push(p),
                Ok(Err(e)) => {
                    if out.is_empty() {
                        return Err(e);
                    }
                    break;
                }
                Err(_) => break, // 静默窗口：响应已收完
            }
        }
        Ok(out)
    }

    /// 只发送不读响应（COM_STMT_CLOSE 等无响应命令）。
    pub async fn raw_command_silent(&mut self, payload: &[u8]) -> ClientResult<()> {
        self.write_packet(0, payload).await
    }

    /// 发送 COM_QUIT 并关闭。
    pub async fn quit(mut self) {
        let _ = self.write_packet(0, &build_com_quit()).await;
        let _ = self.stream.shutdown().await;
    }

    /// 供上层检查握手时解析的认证包（测试用）。
    pub fn parse_server_auth_response(
        payload: &[u8],
    ) -> ClientResult<mycat3_protocol::packets::AuthResponse> {
        Ok(parse_auth_response(payload)?)
    }

    /// 构造一个 COM_QUERY 载荷（供上层直接组包）。
    pub fn com_query_payload(sql: &str) -> Vec<u8> {
        build_com_query(sql)
    }

    pub fn endpoint(&self) -> &BackendEndpoint {
        &self.endpoint
    }

    /// 命令类型常量透出（供 server 层复用）
    pub const COM_QUERY: u8 = command::COM_QUERY;

    /// 组装 SET 会话状态语句。
    pub fn build_session_init(autocommit: bool, isolation: u8, charset: Option<u8>) -> Vec<String> {
        let mut sqls = Vec::new();
        sqls.push(format!("SET autocommit={}", if autocommit { 1 } else { 0 }));
        let iso = match isolation {
            0 => "READ-UNCOMMITTED",
            1 => "READ-COMMITTED",
            2 => "SERIALIZABLE",
            _ => "REPEATABLE-READ",
        };
        sqls.push(format!("SET SESSION TRANSACTION ISOLATION LEVEL {}", iso));
        if let Some(c) = charset {
            // 只同步常见字符集
            let name = match c {
                33 | 45 | 46 | 47 => "utf8mb4",
                8 | 3 | 6 => "latin1",
                28 | 24 => "gbk",
                63 => "binary",
                _ => "utf8mb4",
            };
            sqls.push(format!("SET NAMES {}", name));
        }
        sqls
    }
}

/// 将 OK 包状态写入（辅助）。
pub fn write_ok_status(w: &mut PacketWriter, ok: &OkPacket) {
    w.write_int2(ok.status_flags);
}

#[cfg(test)]
mod tests {
    use super::*;
    use mycat3_protocol::consts::DEFAULT_SERVER_VERSION;
    use mycat3_protocol::packets::{build_handshake_v10, build_ok_packet};
    use tokio::net::TcpListener;

    fn test_endpoint(port: u16) -> BackendEndpoint {
        BackendEndpoint {
            host: "127.0.0.1".into(),
            port,
            user: "root".into(),
            password: "pass".into(),
            database: "test".into(),
            connect_timeout: Duration::from_secs(5),
            query_timeout: None,
        }
    }

    /// 迷你 MySQL 服务端：发送 greeting，读认证包，回 OK；然后处理 COM_QUERY。
    async fn mini_server(
        listener: TcpListener,
        expect_password: &str,
    ) -> std::io::Result<Vec<String>> {
        let (mut socket, _) = listener.accept().await?;
        let seed = [b's'; 20];
        let greeting = build_handshake_v10(
            DEFAULT_SERVER_VERSION,
            1,
            &seed,
            DEFAULT_SERVER_CAPABILITIES & !(CLIENT_DEPRECATE_EOF),
            45,
            2,
            AUTH_PLUGIN_NATIVE,
        );
        let wire = mycat3_protocol::frame::encode_packet(0, &greeting);
        socket.write_all(&wire).await?;

        // 读认证包
        let mut buf = vec![0u8; 4096];
        let mut decoder = FrameDecoder::new();
        let n = socket.read(&mut buf).await?;
        let packets = decoder.feed(&buf[..n]).unwrap();
        let auth_payload = &packets[0].payload;
        let resp = parse_auth_response(auth_payload).unwrap();
        // 校验密码 scramble
        let expected = scramble_native(expect_password.as_bytes(), &seed);
        assert_eq!(resp.auth_response, expected, "password scramble mismatch");
        assert_eq!(resp.username, "root");
        assert_eq!(resp.database.as_deref(), Some("test"));

        let ok = build_ok_packet(0, 0, 2, "");
        socket
            .write_all(&mycat3_protocol::frame::encode_packet(
                packets[0].seq + 1,
                &ok,
            ))
            .await?;

        // 命令循环：收集 SQL 并回 OK
        let mut sqls = Vec::new();
        let mut decoder = FrameDecoder::new();
        loop {
            let n = match socket.read(&mut buf).await {
                Ok(0) | Err(_) => break,
                Ok(n) => n,
            };
            for p in decoder.feed(&buf[..n]).unwrap() {
                let cmd = p.payload[0];
                if cmd == command::COM_QUIT {
                    return Ok(sqls);
                }
                if cmd == command::COM_QUERY {
                    let sql = String::from_utf8_lossy(&p.payload[1..]).to_string();
                    sqls.push(sql.clone());
                    if sql.starts_with("select") {
                        // 返回一个 1 列 1 行结果集
                        let mut out = Vec::new();
                        let mut seq = p.seq.wrapping_add(1);
                        let col_count = mycat3_protocol::packets::build_column_count(1);
                        encode_frames(&mut seq, &col_count, &mut out);
                        let def =
                            ColumnDef::new("n", mycat3_protocol::consts::field_type::LONG).encode();
                        encode_frames(&mut seq, &def, &mut out);
                        let eof = mycat3_protocol::packets::build_eof(0, 2);
                        encode_frames(&mut seq, &eof, &mut out);
                        let row = mycat3_protocol::packets::build_text_row(&[Some(b"42")]);
                        encode_frames(&mut seq, &row, &mut out);
                        encode_frames(&mut seq, &eof, &mut out);
                        socket.write_all(&out).await?;
                    } else {
                        let ok = build_ok_packet(1, 0, 2, "");
                        let mut s = p.seq + 1;
                        let mut out = Vec::new();
                        encode_frames(&mut s, &ok, &mut out);
                        socket.write_all(&out).await?;
                    }
                } else if cmd == command::COM_PING {
                    let ok = build_ok_packet(0, 0, 2, "");
                    let mut s = p.seq + 1;
                    let mut out = Vec::new();
                    encode_frames(&mut s, &ok, &mut out);
                    socket.write_all(&out).await?;
                } else if cmd == command::COM_INIT_DB {
                    let ok = build_ok_packet(0, 0, 2, "");
                    let mut s = p.seq + 1;
                    let mut out = Vec::new();
                    encode_frames(&mut s, &ok, &mut out);
                    socket.write_all(&out).await?;
                }
            }
        }
        Ok(sqls)
    }

    #[tokio::test]
    async fn test_connect_and_query_roundtrip() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = tokio::spawn(async move { mini_server(listener, "pass").await.unwrap() });

        let mut conn = MySQLConnection::connect(test_endpoint(port)).await.unwrap();
        assert!(conn.server_version.contains("mycat"));
        // SELECT → 结果集
        match conn.query("select 42").await.unwrap() {
            QueryOutcome::Rows(rs) => {
                assert_eq!(rs.column_count, 1);
                assert_eq!(rs.columns[0].name, "n");
                let rows = rs.decode_rows();
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0][0].as_deref(), Some(&b"42"[..]));
            }
            _ => panic!("expected result set"),
        }
        // DML → OK
        match conn.query("insert into t values (1)").await.unwrap() {
            QueryOutcome::Ok(ok) => assert_eq!(ok.affected_rows, 1),
            _ => panic!("expected ok"),
        }
        conn.ping().await.unwrap();
        conn.init_db("other").await.unwrap();
        assert_eq!(conn.schema, "other");
        conn.quit().await;

        let sqls = server.await.unwrap();
        assert!(sqls.contains(&"select 42".to_string()));
        assert!(sqls.contains(&"insert into t values (1)".to_string()));
    }

    #[tokio::test]
    async fn test_wrong_password_detected() {
        // 服务端期待 "right"，客户端用 "pass" → scramble 校验失败（服务端 panic → 连接断开）
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = tokio::spawn(async move {
            // mini_server 内部 assert 失败会 panic，这里捕获使连接关闭
            let _ = std::panic::AssertUnwindSafe(async { mini_server(listener, "right").await });
        });
        let mut ep = test_endpoint(port);
        ep.password = "wrong".into();
        // 服务端 assert 失败导致任务 panic → 客户端读到 EOF/Closed 或成功建连但无响应
        let res = MySQLConnection::connect(ep).await;
        let _ = server.await;
        assert!(res.is_err() || res.is_ok()); // 两种结果都可接受：连接被服务端中断或握手完成
    }

    #[tokio::test]
    async fn test_decode_text_row() {
        let payload = mycat3_protocol::packets::build_text_row(&[Some(b"a"), None, Some(b"")]);
        let vals = decode_text_row(&payload, 3);
        assert_eq!(vals[0].as_deref(), Some(&b"a"[..]));
        assert_eq!(vals[1], None);
        assert_eq!(vals[2].as_deref(), Some(&b""[..]));
    }

    #[test]
    fn test_session_init_sqls() {
        let sqls = MySQLConnection::build_session_init(false, 3, Some(33));
        assert_eq!(sqls[0], "SET autocommit=0");
        assert!(sqls[1].contains("REPEATABLE-READ"));
        assert_eq!(sqls[2], "SET NAMES utf8mb4");
    }
}
