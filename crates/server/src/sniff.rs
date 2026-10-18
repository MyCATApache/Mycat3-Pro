//! 多协议同端口嗅探。
//!
//! 精确移植 Java 版 `io.mycat.protocol.ProtocolDetector` + `MycatSessionManager`
//! 的嗅探时序：
//! - 检测顺序（首个命中生效）：MySQL → TDS → TNS → PostgreSQL
//! - `detectMySQL`：客户端 HandshakeResponse41 特征（seq==1 && len>32 && caps>0）
//! - `detectTDS`：首字节 ∈ {0x12 PRELOGIN, 0x10 LOGIN7, 0x01 SQL_BATCH, 0x03 RPC}
//! - `detectTNS`：BE u16 包长 ∈ (0,0xFFFF) 且 byte[4]==1 (CONNECT)
//! - `detectPG`：BE u32 长度 ∈ (0,0xFFFFFF) 且 BE u32 协议号 == 196608 (v3.0)
//!   或 80877103 (SSLRequest)
//! - 300ms 无字节 → MySQL（MySQL 是 server-speaks-first，正确客户端会先等 greeting）
//! - 2s 未识别或 >2KB 仍无法识别 → 关闭（绝不静默回退 MySQL）

use std::time::{Duration, Instant};

use tokio::io::AsyncReadExt;
use tokio::net::TcpStream;
use tracing::warn;

/// 嗅探出的前端协议。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DetectedProtocol {
    Mysql,
    /// TDS（SQL Server）
    Tds,
    /// TNS（Oracle）
    Tns,
    Postgres,
}

impl DetectedProtocol {
    pub fn name(&self) -> &'static str {
        match self {
            DetectedProtocol::Mysql => "mysql",
            DetectedProtocol::Tds => "sqlserver",
            DetectedProtocol::Tns => "oracle",
            DetectedProtocol::Postgres => "postgresql",
        }
    }
}

/// 嗅探失败原因。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SniffOutcome {
    /// 已识别协议；附带已消费的字节（需交给协议处理器继续解析）
    Detected(DetectedProtocol),
    /// 超时/超量仍无法识别 → 应关闭连接
    Undetectable,
    /// IO 错误/对端关闭
    Disconnected,
}

/// 首个包的等待窗口：300ms 内无字节 → MySQL（server-speaks-first）。
const FIRST_BYTE_TIMEOUT: Duration = Duration::from_millis(300);
/// 总嗅探上限（Java: 2s）。
const SNIFF_TIMEOUT: Duration = Duration::from_secs(2);
/// 嗅探字节上限（Java: 2KB）。
const SNIFF_MAX_BYTES: usize = 2048;

/// 在 socket 上执行嗅探。返回协议与已消费字节。
pub async fn sniff(socket: &mut TcpStream) -> (SniffOutcome, Vec<u8>) {
    let mut buf: Vec<u8> = Vec::new();
    let mut tmp = [0u8; 4096];
    let start = Instant::now();
    loop {
        // 首包 300ms 静默 → MySQL；后续用剩余总预算
        let wait = if buf.is_empty() {
            FIRST_BYTE_TIMEOUT
        } else {
            SNIFF_TIMEOUT.saturating_sub(start.elapsed())
        };
        if wait.is_zero() {
            warn!("protocol undetected after 2s, closing connection");
            return (SniffOutcome::Undetectable, buf);
        }
        match tokio::time::timeout(wait, socket.read(&mut tmp)).await {
            Err(_) => {
                // 300ms 静默且尚无字节 → MySQL 客户端在等 greeting
                if buf.is_empty() {
                    return (SniffOutcome::Detected(DetectedProtocol::Mysql), buf);
                }
                warn!("protocol undetected after 2s, closing connection");
                return (SniffOutcome::Undetectable, buf);
            }
            Ok(Err(_)) | Ok(Ok(0)) => return (SniffOutcome::Disconnected, buf),
            Ok(Ok(n)) => {
                buf.extend_from_slice(&tmp[..n]);
                if buf.len() >= 4 {
                    if let Some(p) = detect(&buf) {
                        return (SniffOutcome::Detected(p), buf);
                    }
                    if buf.len() > SNIFF_MAX_BYTES {
                        warn!("cannot detect protocol after {} bytes, closing", buf.len());
                        return (SniffOutcome::Undetectable, buf);
                    }
                }
            }
        }
    }
}

/// 字节级协议检测。逐条对应 Java `ProtocolDetector.detect`（顺序敏感）。
pub fn detect(buf: &[u8]) -> Option<DetectedProtocol> {
    if buf.len() < 4 {
        return None;
    }
    if detect_mysql(buf) {
        return Some(DetectedProtocol::Mysql);
    }
    if detect_tds(buf) {
        return Some(DetectedProtocol::Tds);
    }
    if detect_tns(buf) {
        return Some(DetectedProtocol::Tns);
    }
    if detect_pg(buf) {
        return Some(DetectedProtocol::Postgres);
    }
    None
}

/// MySQL HandshakeResponse41：3 字节 LE 长度 + seq==1 + caps 低 16 位 > 0。
fn detect_mysql(buf: &[u8]) -> bool {
    if buf.len() < 8 {
        return false;
    }
    let packet_length = u32::from_le_bytes([buf[0], buf[1], buf[2], 0]);
    let sequence_id = buf[3];
    if sequence_id != 1 || packet_length <= 32 {
        return false;
    }
    let cap_low = u16::from_le_bytes([buf[4], buf[5]]);
    cap_low > 0
}

/// TDS：首字节为已知包类型。
fn detect_tds(buf: &[u8]) -> bool {
    matches!(buf[0], 0x12 | 0x10 | 0x01 | 0x03)
}

/// TNS：BE u16 包长有效且 byte[4]==1 (CONNECT)。
fn detect_tns(buf: &[u8]) -> bool {
    if buf.len() < 10 {
        return false;
    }
    let len = u16::from_be_bytes([buf[0], buf[1]]);
    len > 0 && buf[4] == 1
}

/// PostgreSQL v3 StartupMessage / SSLRequest。
fn detect_pg(buf: &[u8]) -> bool {
    if buf.len() < 8 {
        return false;
    }
    let len = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]);
    if len == 0 || len > 0x00FF_FFFF {
        return false;
    }
    let version = u32::from_be_bytes([buf[4], buf[5], buf[6], buf[7]]);
    version == 196608 || version == 80877103
}

#[cfg(test)]
mod tests {
    use super::*;
    use mycat3_protocol::packets::build_auth_response;

    #[test]
    fn test_detect_pg_startup() {
        // StartupMessage: len(BE) + 196608 + "user\0postgres\0database\0db\0\0"
        let mut msg = Vec::new();
        let payload = {
            let mut p = Vec::new();
            p.extend_from_slice(&196608u32.to_be_bytes());
            p.extend_from_slice(b"user\0postgres\0database\0db1\0\0");
            p
        };
        msg.extend_from_slice(&((payload.len() + 4) as u32).to_be_bytes());
        msg.extend_from_slice(&payload);
        assert_eq!(detect(&msg), Some(DetectedProtocol::Postgres));
    }

    #[test]
    fn test_detect_pg_ssl_request() {
        let mut msg = Vec::new();
        msg.extend_from_slice(&8u32.to_be_bytes());
        msg.extend_from_slice(&80877103u32.to_be_bytes());
        assert_eq!(detect(&msg), Some(DetectedProtocol::Postgres));
    }

    #[test]
    fn test_detect_tds_prelogin() {
        let msg = [0x12, 0x01, 0x00, 0x2f, 0x00, 0x00, 0x01, 0x00, 0x01, 0x02];
        assert_eq!(detect(&msg), Some(DetectedProtocol::Tds));
        let mut login7 = vec![0x10, 0x01, 0x01, 0x00];
        login7.resize(40, 0);
        assert_eq!(detect(&login7), Some(DetectedProtocol::Tds));
    }

    #[test]
    fn test_detect_tns_connect() {
        // TNS Connect: len(BE)=58, 包长 2 字节头之后：len(2) 校验和(2) 类型(1)=01
        let mut msg = vec![0x00, 0x3a, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01, 0x3c];
        msg.resize(58, 0);
        assert_eq!(detect(&msg), Some(DetectedProtocol::Tns));
    }

    #[test]
    fn test_detect_mysql_auth_response() {
        // 真实 HandshakeResponse41：4 字节帧头(len>32, seq=1) + caps...
        let payload = build_auth_response(
            mycat3_protocol::consts::capability::DEFAULT_CLIENT_CAPABILITIES,
            1 << 24,
            33,
            "root",
            &[0u8; 20],
            Some("db1"),
            "mysql_native_password",
        );
        let packet = mycat3_protocol::frame::encode_packet(1, &payload);
        assert_eq!(detect(&packet), Some(DetectedProtocol::Mysql));
    }

    #[test]
    fn test_detect_order_and_unknown() {
        // 长度不足
        assert_eq!(detect(&[1, 2, 3]), None);
        // 全零（TNS len=0 无效、PG len=0 无效、TDS 0x00 不在集合）
        assert_eq!(detect(&[0, 0, 0, 0, 0, 0, 0, 0, 0, 0]), None);
        // 随机文本
        assert_eq!(detect(b"GET / HTTP/1.1\r\n\r\n"), None);
    }

    #[tokio::test]
    async fn test_sniff_first_byte_timeout_is_mysql() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let client = tokio::spawn(async move {
            let sock = tokio::net::TcpStream::connect(("127.0.0.1", port))
                .await
                .unwrap();
            // 保持静默（模拟等待 greeting 的 MySQL 客户端）
            tokio::time::sleep(Duration::from_secs(2)).await;
            drop(sock);
        });
        let (mut sock, _) = listener.accept().await.unwrap();
        let t = Instant::now();
        let (outcome, buf) = sniff(&mut sock).await;
        assert_eq!(outcome, SniffOutcome::Detected(DetectedProtocol::Mysql));
        assert!(buf.is_empty());
        assert!(
            t.elapsed() >= Duration::from_millis(280),
            "should wait ~300ms"
        );
        assert!(t.elapsed() < Duration::from_millis(600));
        client.await.unwrap();
    }

    #[tokio::test]
    async fn test_sniff_pg_bytes() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let client = tokio::spawn(async move {
            use tokio::io::AsyncWriteExt;
            let mut sock = tokio::net::TcpStream::connect(("127.0.0.1", port))
                .await
                .unwrap();
            let mut msg = Vec::new();
            let mut payload = Vec::new();
            payload.extend_from_slice(&196608u32.to_be_bytes());
            payload.extend_from_slice(b"user\0u\0database\0d\0\0");
            msg.extend_from_slice(&((payload.len() + 4) as u32).to_be_bytes());
            msg.extend_from_slice(&payload);
            sock.write_all(&msg).await.unwrap();
            tokio::time::sleep(Duration::from_millis(300)).await;
        });
        let (mut sock, _) = listener.accept().await.unwrap();
        let (outcome, buf) = sniff(&mut sock).await;
        assert_eq!(outcome, SniffOutcome::Detected(DetectedProtocol::Postgres));
        assert!(!buf.is_empty()); // 已消费字节需交还处理器
        client.await.unwrap();
    }

    #[tokio::test]
    async fn test_sniff_disconnect() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let client = tokio::spawn(async move {
            let sock = tokio::net::TcpStream::connect(("127.0.0.1", port))
                .await
                .unwrap();
            drop(sock); // 立即断开
        });
        let (mut sock, _) = listener.accept().await.unwrap();
        let (outcome, _) = sniff(&mut sock).await;
        assert_eq!(outcome, SniffOutcome::Disconnected);
        let _ = client.await;
    }
}
