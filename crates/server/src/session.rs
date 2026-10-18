//! 前端会话：连接状态 + 响应包写入。
//!
//! 对应 Mycat3 的 `MycatVertxMysqlSession` / `VertxSessionImpl` /
//! `MySQLServerSession` 接口：packetId 从命令包序号延续（setPacketId →
//! getNextPacketId() = ++packetId），DEPRECATE_EOF 语义与 Java 一致。

use mycat3_protocol::consts::{capability, status};

/// 前端连接流：明文 TCP 或 TLS 包装（rustls）。
pub enum ProtoStream {
    Plain(tokio::net::TcpStream),
    Tls(Box<tokio_rustls::server::TlsStream<tokio::net::TcpStream>>),
    /// 占位（TLS 升级期间 replace 用）
    Closed,
}

impl tokio::io::AsyncRead for ProtoStream {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match self.get_mut() {
            ProtoStream::Plain(s) => std::pin::Pin::new(s).poll_read(cx, buf),
            ProtoStream::Tls(s) => std::pin::Pin::new(s).poll_read(cx, buf),
            ProtoStream::Closed => std::task::Poll::Ready(Err(std::io::Error::new(
                std::io::ErrorKind::NotConnected,
                "stream closed",
            ))),
        }
    }
}

impl tokio::io::AsyncWrite for ProtoStream {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<Result<usize, std::io::Error>> {
        match self.get_mut() {
            ProtoStream::Plain(s) => std::pin::Pin::new(s).poll_write(cx, buf),
            ProtoStream::Tls(s) => std::pin::Pin::new(s).poll_write(cx, buf),
            ProtoStream::Closed => std::task::Poll::Ready(Err(std::io::Error::new(
                std::io::ErrorKind::NotConnected,
                "stream closed",
            ))),
        }
    }
    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), std::io::Error>> {
        match self.get_mut() {
            ProtoStream::Plain(s) => std::pin::Pin::new(s).poll_flush(cx),
            ProtoStream::Tls(s) => std::pin::Pin::new(s).poll_flush(cx),
            ProtoStream::Closed => std::task::Poll::Ready(Err(std::io::Error::new(
                std::io::ErrorKind::NotConnected,
                "stream closed",
            ))),
        }
    }
    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), std::io::Error>> {
        match self.get_mut() {
            ProtoStream::Plain(s) => std::pin::Pin::new(s).poll_shutdown(cx),
            ProtoStream::Tls(s) => std::pin::Pin::new(s).poll_shutdown(cx),
            ProtoStream::Closed => std::task::Poll::Ready(Ok(())),
        }
    }
}
use mycat3_protocol::frame::encode_frames;
use mycat3_protocol::packets::{build_eof, build_eof_as_ok, build_error, build_ok, ColumnDef};
use tokio::io::{AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;

/// 前端连接会话状态。对应 `MycatDataContextImpl`。
#[derive(Clone)]
pub struct SessionContext {
    pub connection_id: u32,
    pub user: String,
    pub client_ip: String,
    pub schema: Option<String>,
    pub charset: u8,
    pub capabilities: u32,
    pub autocommit: bool,
    /// 隔离级别序号（1=RU 2=RC 3=RR 4=SERIALIZABLE，与 Java 一致默认 3）
    pub isolation: u8,
    /// 事务策略："proxy"（本地代理事务）| "xa"（MySQL-only XA 2PC）
    pub transaction_policy: String,
    /// 文本级 PREPARE 语句表（MySQL/PG 前端；name → 模板）
    pub text_prepares: std::collections::HashMap<String, crate::prepared_text::TextPrepared>,
    /// MySQL 会话用户变量（SET @x = 字面量 跟踪，EXECUTE USING 用）
    pub user_vars: std::collections::HashMap<String, String>,
    pub in_transaction: bool,
    pub deprecate_eof: bool,
    /// 最近一次 OK 的 last_insert_id / affected_rows（会话展示）
    pub last_insert_id: u64,
    pub affected_rows: u64,
    /// prepare 语句表
    pub next_stmt_id: u32,
}

impl SessionContext {
    pub fn new(connection_id: u32, client_ip: String) -> Self {
        SessionContext {
            connection_id,
            user: String::new(),
            client_ip,
            schema: None,
            charset: 33,
            capabilities: capability::DEFAULT_SERVER_CAPABILITIES,
            autocommit: true,
            isolation: 3,
            transaction_policy: "proxy".into(),
            text_prepares: std::collections::HashMap::new(),
            user_vars: std::collections::HashMap::new(),
            in_transaction: false,
            deprecate_eof: true,
            last_insert_id: 0,
            affected_rows: 0,
            next_stmt_id: 1,
        }
    }

    /// 服务端状态位。对应 `MycatDataContext.serverStatus()`。
    pub fn server_status(&self) -> u16 {
        let mut s = 0u16;
        if self.autocommit {
            s |= status::SERVER_STATUS_AUTOCOMMIT;
        }
        if self.in_transaction {
            s |= status::SERVER_STATUS_IN_TRANS;
        }
        s
    }
}

/// 前端包写入器：维护序号并输出分帧字节。
/// 对应 `MySQLServerSession.writeBytes` + `getNextPacketId`。
pub struct PacketSender {
    seq: u8,
}

impl PacketSender {
    pub fn new() -> Self {
        PacketSender { seq: 0 }
    }

    /// 收到命令包时同步序号（响应从 seq+1 开始）。对应 `session.setPacketId`。
    pub fn set_seq(&mut self, cmd_seq: u8) {
        self.seq = cmd_seq;
    }

    pub fn next_seq(&mut self) -> u8 {
        self.seq = self.seq.wrapping_add(1);
        self.seq
    }

    /// 将一个载荷分帧追加到 out。
    pub fn push(&mut self, out: &mut Vec<u8>, payload: &[u8]) {
        let seq = self.next_seq();
        let mut s = seq;
        encode_frames(&mut s, payload, out);
        // encode_frames 可能因分包递增序号；同步回来（最后一个物理包序号 +1 为下一序号）
        self.seq = s.wrapping_sub(1);
    }

    pub async fn write_payload(sock: &mut TcpStream, out: &mut Vec<u8>) -> std::io::Result<()> {
        if out.is_empty() {
            return Ok(());
        }
        sock.write_all(out).await?;
        sock.flush().await?;
        out.clear();
        Ok(())
    }
}

impl Default for PacketSender {
    fn default() -> Self {
        Self::new()
    }
}

/// 会话响应写入辅助（组合 SessionContext + PacketSender）。
pub struct SessionWriter<'a> {
    pub ctx: &'a mut SessionContext,
    pub sender: &'a mut PacketSender,
    pub out: &'a mut Vec<u8>,
}

impl<'a> SessionWriter<'a> {
    pub fn new(
        ctx: &'a mut SessionContext,
        sender: &'a mut PacketSender,
        out: &'a mut Vec<u8>,
    ) -> Self {
        SessionWriter { ctx, sender, out }
    }

    pub fn write_ok(&mut self, affected_rows: u64, last_insert_id: u64, more_results: bool) {
        self.ctx.affected_rows = affected_rows;
        self.ctx.last_insert_id = last_insert_id;
        let mut st = self.ctx.server_status();
        if more_results {
            st |= status::SERVER_MORE_RESULTS_EXISTS;
        }
        let payload = build_ok(0x00, affected_rows, last_insert_id, st, 0, "");
        self.sender.push(self.out, &payload);
    }

    pub fn write_eof(&mut self, more_results: bool) {
        if self.ctx.deprecate_eof {
            // DEPRECATE_EOF：EOF 位置写 0xfe 头的 OK
            let mut st = self.ctx.server_status();
            if more_results {
                st |= status::SERVER_MORE_RESULTS_EXISTS;
            }
            let payload = build_eof_as_ok(st, 0);
            self.sender.push(self.out, &payload);
        } else {
            let mut st = self.ctx.server_status();
            if more_results {
                st |= status::SERVER_MORE_RESULTS_EXISTS;
            }
            let payload = build_eof(0, st);
            self.sender.push(self.out, &payload);
        }
    }

    pub fn write_error(&mut self, errno: u16, sqlstate: &str, message: &str) {
        let payload = build_error(errno, sqlstate, message);
        self.sender.push(self.out, &payload);
    }

    pub fn write_error_payload(&mut self, payload: Vec<u8>) {
        self.sender.push(self.out, &payload);
    }

    /// 写列数包。
    pub fn write_column_count(&mut self, count: usize) {
        let payload = mycat3_protocol::packets::build_column_count(count);
        self.sender.push(self.out, &payload);
    }

    /// 写列定义（解析形式）。
    pub fn write_column_def(&mut self, def: &ColumnDef) {
        let payload = def.encode();
        self.sender.push(self.out, &payload);
    }

    /// 写列定义（原始载荷透传）。
    pub fn write_column_def_raw(&mut self, payload: &[u8]) {
        self.sender.push(self.out, payload);
    }

    /// 写行（原始载荷透传）。
    pub fn write_row_raw(&mut self, payload: &[u8]) {
        self.sender.push(self.out, payload);
    }

    /// 写行（值形式）。
    pub fn write_text_row(&mut self, values: &[Option<&[u8]>]) {
        let payload = mycat3_protocol::packets::build_text_row(values);
        self.sender.push(self.out, &payload);
    }

    pub async fn flush(&mut self, sock: &mut TcpStream) -> std::io::Result<()> {
        PacketSender::write_payload(sock, self.out).await
    }
}

/// 供执行层记录输出字节数（flush 包装）。
pub async fn flush_counted<W: AsyncWrite + Unpin>(
    w: &mut W,
    out: &mut Vec<u8>,
) -> std::io::Result<u64> {
    if out.is_empty() {
        return Ok(0);
    }
    let n = out.len() as u64;
    w.write_all(out).await?;
    w.flush().await?;
    out.clear();
    Ok(n)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_seq_continuation() {
        // 命令包 seq=0 → 响应包从 1 开始（Mycat3 setPacketId/getNextPacketId 语义）
        let mut sender = PacketSender::new();
        sender.set_seq(0);
        let mut out = Vec::new();
        sender.push(&mut out, &[0x00, 0x00, 0x00, 0x02, 0x00, 0x00]);
        assert_eq!(out[3], 1); // 第一个响应包 seq=1
        sender.push(&mut out, &[0xfe, 0x00, 0x00, 0x02, 0x00]);
        assert_eq!(out[6 + 4 + 3], 2); // 第二个包 seq=2
    }

    #[test]
    fn test_server_status_flags() {
        let mut ctx = SessionContext::new(1, "127.0.0.1".into());
        assert_eq!(ctx.server_status(), status::SERVER_STATUS_AUTOCOMMIT);
        ctx.autocommit = false;
        ctx.in_transaction = true;
        assert_eq!(ctx.server_status(), status::SERVER_STATUS_IN_TRANS);
    }

    #[test]
    fn test_ok_packet_more_results() {
        let mut ctx = SessionContext::new(1, "127.0.0.1".into());
        let mut sender = PacketSender::new();
        let mut out = Vec::new();
        {
            let mut w = SessionWriter::new(&mut ctx, &mut sender, &mut out);
            w.write_ok(3, 7, true);
        }
        // 头 0x00 + lenenc(3) + lenenc(7) + status(含 MORE_RESULTS=8|AUTOCOMMIT=2)
        assert_eq!(out[4], 0x00);
        let st = u16::from_le_bytes([out[7], out[8]]);
        assert_ne!(st & status::SERVER_MORE_RESULTS_EXISTS, 0);
    }

    #[test]
    fn test_eof_deprecate_mode() {
        let mut ctx = SessionContext::new(1, "127.0.0.1".into());
        ctx.deprecate_eof = true;
        let mut sender = PacketSender::new();
        let mut out = Vec::new();
        {
            let mut w = SessionWriter::new(&mut ctx, &mut sender, &mut out);
            w.write_eof(false);
        }
        assert_eq!(out[4], 0xfe); // DEPRECATE_EOF 下 EOF 头为 0xfe 的 OK
        assert!(out.len() > 4 + 5); // OK 载荷比经典 EOF(5字节) 长
    }
}
