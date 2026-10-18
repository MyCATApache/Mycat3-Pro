//! 后端连接池。
//!
//! 对应 Mycat3 `datasource` 模块的 `JdbcConnectionManager` + Druid 连接池语义：
//! 每个数据源一个池（maxCon 上限、空闲复用、取用时校验、异常连接剔除）。
//! Pro 版使用原生协议连接而非 JDBC。

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use mycat3_config::DatasourceConfig;
use parking_lot::{Mutex, RwLock};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::conn::{BackendEndpoint, ClientError, ClientResult, MySQLConnection};

struct IdleConn {
    conn: MySQLConnection,
    returned_at: Instant,
}

/// 复用空闲连接时免 COM_PING 的空闲窗口（对应 Druid testOnBorrow=false：
/// 刚归还的连接直接交付，超窗才 ping 校验）。
pub const PING_AFTER_IDLE: Duration = Duration::from_secs(1);

/// 池运行统计。
#[derive(Debug, Default)]
pub struct PoolStats {
    pub created: AtomicU64,
    pub closed: AtomicU64,
    pub acquired: AtomicU64,
    pub ping_failed: AtomicU64,
}

/// 单数据源连接池。对应 `JdbcDataSource` + Druid pool。
pub struct DataSourcePool {
    pub name: String,
    endpoint: BackendEndpoint,
    init_sqls: Vec<String>,
    max_con: u32,
    min_con: u32,
    idle_timeout: Duration,
    idle: Mutex<Vec<IdleConn>>,
    permits: Arc<Semaphore>,
    stats: PoolStats,
}

impl DataSourcePool {
    pub fn new(cfg: &DatasourceConfig) -> ClientResult<Arc<Self>> {
        let url = cfg
            .parse_url()
            .map_err(|e| ClientError::Handshake(format!("bad datasource url: {}", e)))?;
        let endpoint = BackendEndpoint {
            host: url.host,
            port: url.port,
            user: cfg.user.clone(),
            password: cfg.password.clone(),
            database: url.database,
            connect_timeout: Duration::from_millis(cfg.max_connect_timeout.max(1000)),
            query_timeout: None,
        };
        Ok(Arc::new(DataSourcePool {
            name: cfg.name.clone(),
            endpoint,
            init_sqls: cfg.init_sqls.clone(),
            max_con: cfg.max_con.max(1),
            min_con: cfg.min_con.min(cfg.max_con.max(1)),
            idle_timeout: Duration::from_millis(cfg.idle_timeout.max(1000)),
            idle: Mutex::new(Vec::new()),
            permits: Arc::new(Semaphore::new(cfg.max_con.max(1) as usize)),
            stats: PoolStats::default(),
        }))
    }

    /// 用自定义 endpoint 构造（测试/心跳用）。
    pub fn with_endpoint(name: &str, endpoint: BackendEndpoint, max_con: u32) -> Arc<Self> {
        Arc::new(DataSourcePool {
            name: name.to_string(),
            endpoint,
            init_sqls: Vec::new(),
            max_con: max_con.max(1),
            min_con: 0,
            idle_timeout: Duration::from_secs(60),
            idle: Mutex::new(Vec::new()),
            permits: Arc::new(Semaphore::new(max_con.max(1) as usize)),
            stats: PoolStats::default(),
        })
    }

    pub fn endpoint(&self) -> &BackendEndpoint {
        &self.endpoint
    }

    pub fn idle_count(&self) -> usize {
        self.idle.lock().len()
    }

    pub fn available_permits(&self) -> usize {
        self.permits.available_permits()
    }

    pub fn stats(&self) -> (u64, u64, u64, u64) {
        (
            self.stats.created.load(Ordering::Relaxed),
            self.stats.closed.load(Ordering::Relaxed),
            self.stats.acquired.load(Ordering::Relaxed),
            self.stats.ping_failed.load(Ordering::Relaxed),
        )
    }

    /// 取出一条可用连接：优先复用空闲连接，否则新建。
    /// 刚归还的连接在 `PING_AFTER_IDLE` 窗口内**免 COM_PING 直用**（对应 Druid
    /// testOnBorrow=false 语义：省一次完整 RTT；超窗才 ping，坏连接按读错误剔除）。
    pub async fn acquire(self: &Arc<Self>) -> ClientResult<CheckedOutConnection> {
        let permit = self
            .permits
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| ClientError::Handshake("pool closed".into()))?;
        self.stats.acquired.fetch_add(1, Ordering::Relaxed);

        // 复用空闲连接（同步取出候选，避免锁跨 await）
        loop {
            let (candidate, expired) = self.take_idle();
            for c in expired {
                self.stats.closed.fetch_add(1, Ordering::Relaxed);
                self.spawn_quit(c);
            }
            match candidate {
                Some((mut conn, fresh)) => {
                    let alive = if fresh {
                        true
                    } else {
                        conn.ping().await.is_ok()
                    };
                    if alive {
                        return Ok(CheckedOutConnection {
                            conn: Some(conn),
                            pool: self.clone(),
                            _permit: permit,
                            broken: false,
                        });
                    }
                    self.stats.ping_failed.fetch_add(1, Ordering::Relaxed);
                    self.stats.closed.fetch_add(1, Ordering::Relaxed);
                    self.spawn_quit(conn);
                    continue; // 取下一个空闲连接
                }
                None => break,
            }
        }

        // 新建连接
        let mut conn = MySQLConnection::connect(self.endpoint.clone()).await?;
        self.stats.created.fetch_add(1, Ordering::Relaxed);
        for sql in &self.init_sqls {
            if let Err(e) = conn.execute_ignore(sql).await {
                return Err(e);
            }
        }
        Ok(CheckedOutConnection {
            conn: Some(conn),
            pool: self.clone(),
            _permit: permit,
            broken: false,
        })
    }

    /// 同步取出一个空闲候选（含免 ping 新鲜度），并顺带摘除所有超时空闲连接。
    fn take_idle(&self) -> (Option<(MySQLConnection, bool)>, Vec<MySQLConnection>) {
        let mut idle = self.idle.lock();
        let mut expired = Vec::new();
        let mut keep = Vec::with_capacity(idle.len());
        for c in idle.drain(..) {
            if c.returned_at.elapsed() > self.idle_timeout {
                expired.push(c.conn);
            } else {
                keep.push(c);
            }
        }
        let candidate = keep
            .pop()
            .map(|c| (c.conn, c.returned_at.elapsed() < PING_AFTER_IDLE));
        *idle = keep;
        (candidate, expired)
    }

    fn spawn_quit(&self, conn: MySQLConnection) {
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            handle.spawn(async move { conn.quit().await });
        }
    }

    /// 预热 min_con 条空闲连接（对应配置 minCon；失败静默）。
    pub async fn prewarm(self: &Arc<Self>) {
        for _ in 0..self.min_con {
            if let Ok(c) = self.acquire().await {
                drop(c); // 归还空闲
            }
        }
    }

    fn return_conn(&self, conn: MySQLConnection) {
        // 事务未结束的连接不能复用（Drop 语义下无法 await 回滚）：直接关闭
        if conn.in_transaction {
            self.stats.closed.fetch_add(1, Ordering::Relaxed);
            if let Ok(handle) = tokio::runtime::Handle::try_current() {
                handle.spawn(async move { conn.quit().await });
            }
            return;
        }
        let mut idle = self.idle.lock();
        if idle.len() < self.max_con as usize {
            idle.push(IdleConn {
                conn,
                returned_at: Instant::now(),
            });
        } else {
            drop(idle);
            self.stats.closed.fetch_add(1, Ordering::Relaxed);
            if let Ok(handle) = tokio::runtime::Handle::try_current() {
                handle.spawn(async move { conn.quit().await });
            }
        }
    }

    /// 显式关闭一条连接（异常场景）。
    pub fn discard(&self, conn: MySQLConnection) {
        self.stats.closed.fetch_add(1, Ordering::Relaxed);
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            handle.spawn(async move { conn.quit().await });
        }
    }
}

/// 取出的池化连接；Drop 时自动归还（broken 标记则丢弃）。
pub struct CheckedOutConnection {
    conn: Option<MySQLConnection>,
    pool: Arc<DataSourcePool>,
    _permit: OwnedSemaphorePermit,
    broken: bool,
}

// SemaphorePermit<'static> 通过 transmute 生命周期不可行 —— 改用 OwnedSemaphorePermit
impl CheckedOutConnection {
    pub fn conn(&mut self) -> &mut MySQLConnection {
        self.conn.as_mut().expect("connection taken")
    }

    /// 标记连接不可复用（协议错乱/网络错误）。
    pub fn mark_broken(&mut self) {
        self.broken = true;
    }

    /// 取出连接（不再归还池）。调用方负责关闭。
    pub fn take(mut self) -> MySQLConnection {
        self.conn.take().expect("connection taken")
    }
}

impl Drop for CheckedOutConnection {
    fn drop(&mut self) {
        if let Some(conn) = self.conn.take() {
            if self.broken {
                self.pool.discard(conn);
            } else {
                self.pool.return_conn(conn);
            }
        }
    }
}

/// 全部数据源池的管理器。对应 `JdbcConnectionManager`。
#[derive(Default)]
pub struct ConnectionManager {
    pools: RwLock<HashMap<String, Arc<DataSourcePool>>>,
}

impl ConnectionManager {
    pub fn new() -> Arc<Self> {
        Arc::new(ConnectionManager::default())
    }

    /// 获取（或创建）数据源池。
    pub fn get_pool(&self, cfg: &DatasourceConfig) -> ClientResult<Arc<DataSourcePool>> {
        if let Some(p) = self.pools.read().get(&cfg.name) {
            return Ok(p.clone());
        }
        let pool = DataSourcePool::new(cfg)?;
        self.pools.write().insert(cfg.name.clone(), pool.clone());
        // min_con 预热：后台建立 min_con 条空闲连接（原配置项未使用）
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            let p = pool.clone();
            handle.spawn(async move {
                p.prewarm().await;
            });
        }
        Ok(pool)
    }

    /// 直接按 endpoint 建连接（心跳等一次性场景）。
    pub async fn connect_direct(&self, cfg: &DatasourceConfig) -> ClientResult<MySQLConnection> {
        let url = cfg
            .parse_url()
            .map_err(|e| ClientError::Handshake(format!("bad datasource url: {}", e)))?;
        MySQLConnection::connect(BackendEndpoint {
            host: url.host,
            port: url.port,
            user: cfg.user.clone(),
            password: cfg.password.clone(),
            database: url.database,
            connect_timeout: Duration::from_millis(cfg.max_connect_timeout.max(1000)),
            query_timeout: None,
        })
        .await
    }

    /// 移除数据源（配置变更时）。
    pub fn remove(&self, name: &str) {
        self.pools.write().remove(name);
    }

    pub fn names(&self) -> Vec<String> {
        self.pools.read().keys().cloned().collect()
    }

    /// 所有池的统计快照。
    pub fn snapshot(&self) -> Vec<(String, usize, usize, u64, u64)> {
        self.pools
            .read()
            .iter()
            .map(|(name, p)| {
                let (created, closed, acquired, ping_failed) = p.stats();
                (
                    name.clone(),
                    p.idle_count(),
                    p.available_permits(),
                    created,
                    acquired + ping_failed + closed,
                )
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::conn::QueryOutcome;
    use mycat3_protocol::auth::scramble_native;
    use mycat3_protocol::consts::{
        capability::*, command, AUTH_PLUGIN_NATIVE, DEFAULT_SERVER_VERSION,
    };
    use mycat3_protocol::frame::{encode_frames, encode_packet, FrameDecoder};
    use mycat3_protocol::packets::{build_handshake_v10, build_ok_packet, parse_auth_response};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    /// 极简后端：接受任意密码，所有命令回 OK；统计并发峰值。
    async fn echo_server(listener: TcpListener, state: Arc<Mutex<(u32, u32)>>) {
        loop {
            let (mut socket, _) = match listener.accept().await {
                Ok(x) => x,
                Err(_) => return,
            };
            let state = state.clone();
            tokio::spawn(async move {
                {
                    let mut s = state.lock();
                    s.0 += 1;
                    if s.0 > s.1 {
                        s.1 = s.0;
                    }
                }
                let seed = [b'x'; 20];
                let greeting = build_handshake_v10(
                    DEFAULT_SERVER_VERSION,
                    7,
                    &seed,
                    DEFAULT_SERVER_CAPABILITIES,
                    45,
                    2,
                    AUTH_PLUGIN_NATIVE,
                );
                if socket
                    .write_all(&encode_packet(0, &greeting))
                    .await
                    .is_err()
                {
                    return;
                }
                let mut buf = vec![0u8; 8192];
                let mut decoder = FrameDecoder::new();
                loop {
                    let n = match socket.read(&mut buf).await {
                        Ok(0) => break,
                        Err(_) => break,
                        Ok(n) => n,
                    };
                    let packets = match decoder.feed(&buf[..n]) {
                        Ok(p) => p,
                        Err(_) => break,
                    };
                    for p in packets {
                        let cmd = *p.payload.first().unwrap_or(&0);
                        if cmd == command::COM_QUIT {
                            state.lock().0 -= 1;
                            return;
                        }
                        if cmd == 0x0a || p.payload.is_empty() {
                            // 认证包
                            let _ = parse_auth_response(&p.payload);
                        }
                        let ok = build_ok_packet(0, 0, 2, "");
                        let mut out = Vec::new();
                        let mut s = p.seq + 1;
                        encode_frames(&mut s, &ok, &mut out);
                        if socket.write_all(&out).await.is_err() {
                            state.lock().0 -= 1;
                            return;
                        }
                    }
                }
                state.lock().0 -= 1;
            });
        }
    }

    fn ds_config(name: &str, port: u16, max_con: u32) -> DatasourceConfig {
        DatasourceConfig {
            name: name.into(),
            url: format!("jdbc:mysql://127.0.0.1:{}/test", port),
            user: "root".into(),
            password: "x".into(),
            db_type: "mysql".into(),
            weight: 0,
            max_con,
            min_con: 0,
            max_retry_count: 5,
            max_connect_timeout: 5000,
            init_sqls: vec![],
            instance_type: mycat3_config::InstanceType::ReadWrite,
            idle_timeout: 60_000,
            r#type: "NATIVE".into(),
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_pool_reuse_and_limit() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let state = Arc::new(Mutex::new((0u32, 0u32)));
        tokio::spawn(echo_server(listener, state.clone()));

        let mgr = ConnectionManager::new();
        let cfg = ds_config("ds0", port, 2);
        let pool = mgr.get_pool(&cfg).unwrap();

        // 串行取用应复用同一连接
        {
            let mut c = pool.acquire().await.unwrap();
            let _ = c.conn().query("select 1").await.unwrap();
        }
        {
            let mut c = pool.acquire().await.unwrap();
            let _ = c.conn().query("select 1").await.unwrap();
        }
        assert_eq!(pool.stats().0, 1, "should reuse single connection");

        // 并发上限 = max_con
        let c1 = pool.acquire().await.unwrap();
        let c2 = pool.acquire().await.unwrap();
        let pool2 = pool.clone();
        let blocked = tokio::spawn(async move {
            // 第三次 acquire 应阻塞（信号量耗尽）
            let _c3 = pool2.acquire().await.unwrap();
        });
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(
            !blocked.is_finished(),
            "third acquire should block when pool exhausted"
        );
        drop(c1);
        drop(c2);
        tokio::time::timeout(Duration::from_secs(2), blocked)
            .await
            .unwrap()
            .unwrap();

        // 服务端看到的并发连接数不应超过 max_con（2 个复用连接 + 阻塞后新取用）
        let (_cur, peak) = *state.lock();
        assert!(peak <= 3, "peak connections {} exceeds expectation", peak);

        // broken 连接不归还
        let before = pool.stats().1;
        {
            let mut c = pool.acquire().await.unwrap();
            c.mark_broken();
            let _ = c.conn().ping().await;
        }
        assert!(
            pool.stats().1 > before,
            "broken conn should be counted closed"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_connection_manager_direct() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let state = Arc::new(Mutex::new((0u32, 0u32)));
        tokio::spawn(echo_server(listener, state));

        let mgr = ConnectionManager::new();
        let cfg = ds_config("ds0", port, 4);
        let mut conn = mgr.connect_direct(&cfg).await.unwrap();
        let out = conn.query("select 1").await.unwrap();
        assert!(matches!(out, QueryOutcome::Ok(_)));
        conn.quit().await;
        // scramble 校验发生在 echo_server（不校验密码，仅解析）
        let _ = scramble_native(b"x", &[b'x'; 20]);
    }
}
