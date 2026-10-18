//! HTTP 监控接口（Prometheus 文本格式 + JSON 状态 + 健康检查）。
//!
//! 对应 Mycat3 `mycat3/exporter` 的 `PrometheusExporter` + `HTTPServer`
//! （`/-/healthy` 端点）与 `ManagerConfig`（管理端口 9066）。
//! Pro 版直接以 tokio 实现极简 HTTP/1.1 响应，无额外依赖。

use std::sync::Arc;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tracing::{error, info};

use crate::state::ServerState;

/// 启动监控 HTTP 服务（monitor.open=true 时）。
pub async fn start_monitor(state: Arc<ServerState>, ip: String, port: u16) -> std::io::Result<()> {
    let listener = TcpListener::bind((ip.as_str(), port)).await?;
    info!(%ip, port, "monitor http server started (/-/healthy, /metrics, /api/status)");
    loop {
        let (mut socket, _peer) = listener.accept().await?;
        let state = state.clone();
        tokio::spawn(async move {
            // 完整读取请求（修复：原实现单次 4KB 读可能截断请求行/头）
            let mut raw: Vec<u8> = Vec::with_capacity(4096);
            let mut buf = [0u8; 4096];
            let header_end = loop {
                match raw.windows(4).position(|w| w == b"\r\n\r\n") {
                    Some(i) => break i + 4,
                    None => match socket.read(&mut buf).await {
                        Ok(0) | Err(_) => return,
                        Ok(n) => raw.extend_from_slice(&buf[..n]),
                    },
                }
            };
            let head = String::from_utf8_lossy(&raw[..header_end]).to_string();
            let content_len = head
                .lines()
                .find_map(|l| {
                    l.split_once(':')
                        .filter(|(k, _)| k.eq_ignore_ascii_case("content-length"))
                        .and_then(|(_, v)| v.trim().parse::<usize>().ok())
                })
                .unwrap_or(0);
            while raw.len() < header_end + content_len {
                match socket.read(&mut buf).await {
                    Ok(0) | Err(_) => return,
                    Ok(n) => raw.extend_from_slice(&buf[..n]),
                }
            }
            let body_text =
                String::from_utf8_lossy(&raw[header_end..header_end + content_len]).to_string();
            let mut req = head.lines().next().unwrap_or("").split_whitespace();
            let method = req.next().unwrap_or("GET").to_string();
            let path = req.next().unwrap_or("/").to_string();
            let (status, ctype, body) = handle_request(&state, &method, &path, &body_text).await;
            let resp = format!(
                "HTTP/1.1 {}\r\nContent-Type: {}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                status,
                ctype,
                body.len(),
                body
            );
            if let Err(e) = socket.write_all(resp.as_bytes()).await {
                error!("monitor write error: {}", e);
            }
            let _ = socket.flush().await;
        });
    }
}

/// 请求路由：/metrics /api/status /-/healthy /-/ready + 管理 API
/// （/api/config GET/PUT/DELETE、/api/sqlcaches、/api/reload、/api/backups、restore）。
async fn handle_request(
    state: &Arc<ServerState>,
    method: &str,
    path: &str,
    body: &str,
) -> (&'static str, &'static str, String) {
    let json = |v: String| ("200 OK", "application/json", v);
    let ok = |b: String| ("200 OK", "application/json", b);
    let err = |code: &'static str, m: String| {
        (
            code,
            "application/json",
            format!("{{\"error\":\"{}\"}}\n", m.replace('"', "'")),
        )
    };
    match path {
        "/-/healthy" | "/health" => ("200 OK", "text/plain", "ok\n".to_string()),
        "/-/ready" => {
            let alive = state
                .replica()
                .status_snapshot()
                .iter()
                .filter(|(_, a, _, _)| *a)
                .count();
            let total = state.replica().status_snapshot().len();
            if total > 0 && alive == 0 {
                (
                    "503 Service Unavailable",
                    "text/plain",
                    "no datasource alive\n".to_string(),
                )
            } else {
                (
                    "200 OK",
                    "text/plain",
                    format!("ready ({}/{} alive)\n", alive, total),
                )
            }
        }
        "/metrics" => ("200 OK", "text/plain; version=0.0.4", render_metrics(state)),
        "/api/status" => json(render_status_json(state)),
        "/api/sqlcaches" => match &state.sql_cache.show() {
            crate::engine::EngineOutcome::Rows { columns, rows } => {
                let cols: Vec<&str> = columns.iter().map(|c| c.name.as_str()).collect();
                let out: Vec<serde_json::Value> = rows
                    .iter()
                    .map(|r| {
                        let mut m = serde_json::Map::new();
                        for (i, c) in cols.iter().enumerate() {
                            m.insert(
                                c.to_string(),
                                r.get(i)
                                    .and_then(|v| v.clone())
                                    .map(serde_json::Value::String)
                                    .unwrap_or(serde_json::Value::Null),
                            );
                        }
                        serde_json::Value::Object(m)
                    })
                    .collect();
                json(serde_json::to_string_pretty(&out).unwrap_or_else(|_| "[]".into()))
            }
            _ => json("[]".into()),
        },
        "/api/config" if method == "GET" => match serde_json::to_string_pretty(&*state.config()) {
            Ok(j) => json(j),
            Err(e) => err("500 Internal Server Error", e.to_string()),
        },
        // PUT/DELETE /api/config/{kind}/{name}（kind: datasources|clusters|users|schemas|sequences|sqlcaches）
        _ if method == "PUT" && path.starts_with("/api/config/") => {
            let (kind, name) = match path["/api/config/".len()..].split_once('/') {
                Some(x) => x,
                None => return err("404 Not Found", "missing name".into()),
            };
            match apply_config_http(state, kind, name, body).await {
                Ok(()) => ok("{\"applied\":true}\n".into()),
                Err(e) => err("400 Bad Request", e.message),
            }
        }
        _ if method == "DELETE" && path.starts_with("/api/config/") => {
            let (kind, name) = match path["/api/config/".len()..].split_once('/') {
                Some(x) => x,
                None => return err("404 Not Found", "missing name".into()),
            };
            match remove_config_http(state, kind, name).await {
                Ok(()) => ok("{\"removed\":true}\n".into()),
                Err(e) => err("400 Bad Request", e.message),
            }
        }
        "/api/reload" if method == "POST" => match crate::ddl::reload_from_file(state).await {
            Ok(_) => json("{\"reloaded\":true}\n".to_string()),
            Err(e) => err("500 Internal Server Error", e.message),
        },
        "/api/backups" => match crate::ddl::list_backups(state) {
            Ok(v) => json(serde_json::to_string(&v).unwrap_or_else(|_| "[]".into())),
            Err(e) => err("500 Internal Server Error", e.to_string()),
        },
        _ if method == "POST" && path.starts_with("/api/config/restore/") => {
            let ts = &path["/api/config/restore/".len()..];
            match crate::ddl::restore_backup(state, ts).await {
                Ok(()) => ok("{\"restored\":true}\n".into()),
                Err(e) => err("400 Bad Request", e.message),
            }
        }
        _ => ("404 Not Found", "text/plain", "not found\n".to_string()),
    }
}

/// PUT /api/config/{kind}/{name}：JSON 配置对象写入（与 SQL 配置 hint 同逻辑）。
async fn apply_config_http(
    state: &Arc<ServerState>,
    kind: &str,
    _name: &str,
    body: &str,
) -> Result<(), crate::engine::EngineError> {
    let cmd = match kind {
        "datasources" | "datasource" => "createdatasource",
        "clusters" | "cluster" => "createcluster",
        "users" | "user" => "createuser",
        "schemas" | "schema" => "createschema",
        "sequences" | "sequence" => "setsequence",
        "sqlcaches" | "sqlcache" => "createsqlcache",
        other => {
            return Err(crate::engine::EngineError::general(format!(
                "未知配置类型: {}（支持 datasources/clusters/users/schemas/sequences/sqlcaches）",
                other
            )))
        }
    };
    let ctx = crate::session::SessionContext::new(0, "monitor-http".into());
    crate::ddl::handle_config_hint(state, &ctx, cmd, body)
        .await
        .map(|_| ())
}

/// DELETE /api/config/{kind}/{name}。
async fn remove_config_http(
    state: &Arc<ServerState>,
    kind: &str,
    name: &str,
) -> Result<(), crate::engine::EngineError> {
    let cmd = match kind {
        "datasources" | "datasource" => "dropdatasource",
        "clusters" | "cluster" => "dropcluster",
        "users" | "user" => "dropuser",
        "schemas" | "schema" => "dropschema",
        "sqlcaches" | "sqlcache" => "dropsqlcache",
        other => {
            return Err(crate::engine::EngineError::general(format!(
                "未知配置类型: {}",
                other
            )))
        }
    };
    let ctx = crate::session::SessionContext::new(0, "monitor-http".into());
    let body = serde_json::json!({ "name": name, "username": name, "schemaName": name });
    crate::ddl::handle_config_hint(state, &ctx, cmd, &body.to_string())
        .await
        .map(|_| ())
}

/// Prometheus 文本格式指标。对应 PrometheusExporter 注册的计数器。
fn render_metrics(state: &Arc<ServerState>) -> String {
    let s = state.stats.snapshot();
    let uptime = state.started_at.elapsed().as_secs();
    let mut out = String::with_capacity(2048);
    out.push_str("# HELP mycat3_uptime_seconds Server uptime in seconds\n# TYPE mycat3_uptime_seconds gauge\n");
    out.push_str(&format!("mycat3_uptime_seconds {}\n", uptime));
    out.push_str("# HELP mycat3_connections_total Total client connections accepted\n# TYPE mycat3_connections_total counter\n");
    out.push_str(&format!("mycat3_connections_total {}\n", s[0]));
    out.push_str("# HELP mycat3_connections_active Active client connections\n# TYPE mycat3_connections_active gauge\n");
    out.push_str(&format!("mycat3_connections_active {}\n", s[1]));
    out.push_str(
        "# HELP mycat3_queries_total Total queries executed\n# TYPE mycat3_queries_total counter\n",
    );
    out.push_str(&format!("mycat3_queries_total {}\n", s[2]));
    out.push_str("# HELP mycat3_query_errors_total Total query errors\n# TYPE mycat3_query_errors_total counter\n");
    out.push_str(&format!("mycat3_query_errors_total {}\n", s[3]));
    out.push_str("# HELP mycat3_slow_queries_total Queries slower than 1s\n# TYPE mycat3_slow_queries_total counter\n");
    out.push_str(&format!("mycat3_slow_queries_total {}\n", s[4]));
    out.push_str("# HELP mycat3_tx_commits_total Transaction commits\n# TYPE mycat3_tx_commits_total counter\n");
    out.push_str(&format!("mycat3_tx_commits_total {}\n", s[5]));
    out.push_str("# HELP mycat3_tx_rollbacks_total Transaction rollbacks\n# TYPE mycat3_tx_rollbacks_total counter\n");
    out.push_str(&format!("mycat3_tx_rollbacks_total {}\n", s[6]));
    out.push_str("# HELP mycat3_bytes_in_total Bytes received from clients\n# TYPE mycat3_bytes_in_total counter\n");
    out.push_str(&format!("mycat3_bytes_in_total {}\n", s[7]));
    out.push_str("# HELP mycat3_bytes_out_total Bytes sent to clients\n# TYPE mycat3_bytes_out_total counter\n");
    out.push_str(&format!("mycat3_bytes_out_total {}\n", s[8]));
    // 数据源存活
    out.push_str("# HELP mycat3_datasource_alive Datasource alive state (1/0)\n# TYPE mycat3_datasource_alive gauge\n");
    for (name, alive, select_read, active) in state.replica().status_snapshot() {
        out.push_str(&format!(
            "mycat3_datasource_alive{{datasource=\"{}\"}} {}\n",
            name,
            if alive { 1 } else { 0 }
        ));
        out.push_str(&format!(
            "mycat3_datasource_select_as_read{{datasource=\"{}\"}} {}\n",
            name,
            if select_read { 1 } else { 0 }
        ));
        out.push_str(&format!(
            "mycat3_datasource_active{{datasource=\"{}\"}} {}\n",
            name, active
        ));
    }
    // SQL 结果缓存
    let (ch, cm) = state.sql_cache.totals();
    out.push_str("# HELP mycat3_sql_cache_hits_total SQL cache hits\n# TYPE mycat3_sql_cache_hits_total counter\n");
    out.push_str(&format!("mycat3_sql_cache_hits_total {}\n", ch));
    out.push_str("# HELP mycat3_sql_cache_misses_total SQL cache misses\n# TYPE mycat3_sql_cache_misses_total counter\n");
    out.push_str(&format!("mycat3_sql_cache_misses_total {}\n", cm));
    // 连接池
    out.push_str("# HELP mycat3_pool_idle Idle connections per datasource pool\n# TYPE mycat3_pool_idle gauge\n");
    for (name, idle, permits, created, _) in state.conn_mgr.snapshot() {
        out.push_str(&format!(
            "mycat3_pool_idle{{datasource=\"{}\"}} {}\n",
            name, idle
        ));
        out.push_str(&format!(
            "mycat3_pool_available{{datasource=\"{}\"}} {}\n",
            name, permits
        ));
        out.push_str(&format!(
            "mycat3_pool_created{{datasource=\"{}\"}} {}\n",
            name, created
        ));
    }
    out
}

fn render_status_json(state: &Arc<ServerState>) -> String {
    let s = state.stats.snapshot();
    let cfg = state.config();
    let instances: Vec<String> = state
        .replica()
        .status_snapshot()
        .iter()
        .map(|(n, a, r, act)| {
            format!(
                "{{\"name\":\"{}\",\"alive\":{},\"selectAsRead\":{},\"active\":{}}}",
                n, a, r, act
            )
        })
        .collect();
    let pools: Vec<String> = state
        .conn_mgr
        .snapshot()
        .iter()
        .map(|(n, idle, avail, created, _)| {
            format!(
                "{{\"name\":\"{}\",\"idle\":{},\"available\":{},\"created\":{}}}",
                n, idle, avail, created
            )
        })
        .collect();
    format!(
        concat!(
            "{{\"version\":\"{}\",\"mode\":\"{}\",\"port\":{},\"uptimeSeconds\":{},",
            "\"connections\":{{\"total\":{},\"active\":{}}},",
            "\"queries\":{{\"total\":{},\"errors\":{},\"slow\":{}}},",
            "\"transactions\":{{\"commits\":{},\"rollbacks\":{}}},",
            "\"bytes\":{{\"in\":{},\"out\":{}}},",
            "\"datasources\":[{}],\"pools\":[{}],",
            "\"schemas\":[{}],\"clusters\":[{}]}}"
        ),
        cfg.server.server.server_version,
        cfg.server.mode,
        cfg.server.server.port,
        state.started_at.elapsed().as_secs(),
        s[0],
        s[1],
        s[2],
        s[3],
        s[4],
        s[5],
        s[6],
        s[7],
        s[8],
        instances.join(","),
        pools.join(","),
        state
            .metadata()
            .schema_names()
            .iter()
            .map(|n| format!("\"{}\"", n))
            .collect::<Vec<_>>()
            .join(","),
        cfg.clusters
            .keys()
            .map(|n| format!("\"{}\"", n))
            .collect::<Vec<_>>()
            .join(","),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use mycat3_config::default_config;

    #[tokio::test]
    async fn test_metrics_and_status_render() {
        let state = ServerState::new(default_config()).unwrap();
        state.stats.incr_query();
        state.stats.incr_conn();
        let m = render_metrics(&state);
        assert!(m.contains("mycat3_queries_total 1"));
        assert!(m.contains("mycat3_connections_active 1"));
        assert!(m.contains("mycat3_datasource_alive{datasource=\"prototypeDs\"} 1"));
        let j = render_status_json(&state);
        assert!(j.starts_with('{') && j.ends_with('}'));
        assert!(j.contains("\"port\":8066"));
        let v: serde_json::Value = serde_json::from_str(&j).unwrap();
        assert_eq!(v["queries"]["total"], 1);
    }

    #[tokio::test]
    async fn test_monitor_http_endpoints() {
        let state = ServerState::new(default_config()).unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);
        let st = state.clone();
        tokio::spawn(async move {
            let _ = start_monitor(st, "127.0.0.1".into(), port).await;
        });
        // 等待监听就绪
        for _ in 0..50 {
            if tokio::net::TcpStream::connect(("127.0.0.1", port))
                .await
                .is_ok()
            {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        for (path, expect) in [
            ("/-/healthy", "ok"),
            ("/metrics", "mycat3_uptime_seconds"),
            ("/api/status", "\"version\""),
        ] {
            let mut sock = tokio::net::TcpStream::connect(("127.0.0.1", port))
                .await
                .unwrap();
            use tokio::io::AsyncWriteExt as _;
            sock.write_all(format!("GET {} HTTP/1.1\r\nHost: x\r\n\r\n", path).as_bytes())
                .await
                .unwrap();
            let mut resp = String::new();
            sock.read_to_string(&mut resp).await.unwrap();
            assert!(resp.starts_with("HTTP/1.1 200"), "{} → {}", path, resp);
            assert!(resp.contains(expect), "{} missing {}", path, expect);
        }
        // 404
        let mut sock = tokio::net::TcpStream::connect(("127.0.0.1", port))
            .await
            .unwrap();
        use tokio::io::AsyncWriteExt as _;
        sock.write_all(b"GET /nope HTTP/1.1\r\nHost: x\r\n\r\n")
            .await
            .unwrap();
        let mut resp = String::new();
        sock.read_to_string(&mut resp).await.unwrap();
        assert!(resp.starts_with("HTTP/1.1 404"));
    }
}
