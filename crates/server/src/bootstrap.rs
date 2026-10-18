//! 服务启动：TCP 监听与连接分发。
//!
//! 对应 `VertxMycatServer.start()`（Java 版按 reactorNumber 部署 N 个
//! SO_REUSEPORT NetServer；Pro 版使用 tokio 多线程 runtime + 单 accept 循环，
//! 由 runtime 工作线程并行处理连接，语义等价）。

use std::sync::Arc;

use tokio::net::TcpListener;
use tracing::{error, info};

use crate::frontend::serve_connection_prefeed;
use crate::sniff::{self, DetectedProtocol, SniffOutcome};
use crate::state::ServerState;

/// 启动前端 MySQL 协议监听，返回后台任务句柄。
pub async fn start_server(state: Arc<ServerState>) -> anyhow::Result<()> {
    let cfg = state.config();
    let ip = cfg.server.server.ip.clone();
    let port = cfg.server.server.port;
    let listener = TcpListener::bind((ip.as_str(), port)).await?;
    info!(%ip, port, "Mycat3 Pro MySQL protocol server listening");
    start_server_on(state, listener).await
}

/// 在已绑定的监听器上运行 accept 循环（测试可使用端口 0 由系统分配）。
pub async fn start_server_on(state: Arc<ServerState>, listener: TcpListener) -> anyhow::Result<()> {
    // XA 崩溃恢复：扫描孤儿分支并按 WAL 裁决，完成后才接受新连接
    if state.xa_recovery.load(std::sync::atomic::Ordering::Relaxed) {
        crate::xa::recover(&state).await;
    }
    loop {
        match listener.accept().await {
            Ok((socket, peer)) => {
                let _ = peer;
                // setAcceptConnect=0 / pauseServer 停止接入：新连接立即关闭
                //（对应 Java MycatServer.stopAcceptConnect）
                if state
                    .accept_paused
                    .load(std::sync::atomic::Ordering::Relaxed)
                {
                    drop(socket);
                    continue;
                }
                let state = state.clone();
                tokio::spawn(async move {
                    if let Err(e) = serve_connection_safe(state, socket).await {
                        error!("connection task failed: {}", e);
                    }
                });
            }
            Err(e) => {
                error!("accept error: {}", e);
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            }
        }
    }
}

/// 单连接处理：先做协议嗅探（对应 MycatSessionManager 的 ProtocolDetector 路径），
/// 再分发到 MySQL / PostgreSQL / TDS 前端；TNS 与未知流量明确关闭。
/// 将 tokio socket 转为 std 复制一份 kill 句柄（KILL 命令同步 shutdown 用），
/// 再转回 tokio（保持 nonblocking）。
fn split_kill_handle(
    socket: tokio::net::TcpStream,
) -> std::io::Result<(tokio::net::TcpStream, std::net::TcpStream)> {
    let std_sock = socket.into_std()?;
    let kill = std_sock.try_clone()?;
    let tok = tokio::net::TcpStream::from_std(std_sock)?;
    tok.set_nodelay(true)?;
    Ok((tok, kill))
}

async fn serve_connection_safe(
    state: Arc<ServerState>,
    mut socket: tokio::net::TcpStream,
) -> std::io::Result<()> {
    socket.set_nodelay(true)?;
    // protocolSniff=false：纯 MySQL 模式，零嗅探延迟
    if !state.config().server.server.protocol_sniff {
        serve_connection_prefeed(
            state,
            crate::session::ProtoStream::Plain(socket),
            Vec::new(),
            None,
        )
        .await;
        return Ok(());
    }
    let (outcome, prefeed) = sniff::sniff(&mut socket).await;
    let (mut socket, kill_handle) = match split_kill_handle(socket) {
        Ok(x) => x,
        Err(_) => return Ok(()),
    };
    match outcome {
        SniffOutcome::Detected(DetectedProtocol::Mysql) => {
            serve_connection_prefeed(
                state,
                crate::session::ProtoStream::Plain(socket),
                prefeed,
                Some(kill_handle),
            )
            .await;
        }
        SniffOutcome::Detected(DetectedProtocol::Postgres) => {
            crate::pg::serve_pg_connection(state, socket, prefeed, Some(kill_handle)).await;
        }
        SniffOutcome::Detected(DetectedProtocol::Tds) => {
            crate::tds::serve_tds_connection(state, socket, prefeed, Some(kill_handle)).await;
        }
        SniffOutcome::Detected(DetectedProtocol::Tns) => {
            // Java 版 TNS 为无认证 PoC（真实 Oracle 客户端说 TTC 二进制协议 + O5LOGON
            // 私有认证，Java 实现同样无法对接真实客户端）。Pro 版不做假实现：明确拒绝。
            tracing::warn!(
                "TNS(Oracle) protocol detected but not supported by Mycat3 Pro \
                 (real Oracle clients require TTC/O5LOGON); closing"
            );
            use tokio::io::AsyncWriteExt;
            let _ = socket
                .write_all(b"MYCAT3-PRO: TNS/Oracle frontend is not supported.\n")
                .await;
        }
        SniffOutcome::Undetectable => {
            tracing::warn!("undetected protocol traffic, connection closed");
        }
        SniffOutcome::Disconnected => {}
    }
    Ok(())
}
