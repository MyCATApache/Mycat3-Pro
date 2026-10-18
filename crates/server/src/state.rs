//! 服务器全局状态。对应 `MycatCore` 注册到 `MetaClusterCurrent` 的服务集合：
//! 配置快照、元数据、副本运行时、连接管理器、心跳检测器、监控统计。
//!
//! 支持运行时热重载（对应 Java `ConfigUpdater.loadConfigFromFile` + `ops.commit()` 的
//! 资源重建流程）：metadata/config/replica/heartbeat 均可原子替换，连接池按名字复用。

use std::collections::{HashMap, VecDeque};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use mycat3_client::ConnectionManager;
use mycat3_config::MycatRouterConfig;
use mycat3_replica::heartbeat::HeartbeatDetector;
use mycat3_replica::ReplicaSelectorRuntime;
use mycat3_sql::Metadata;
use parking_lot::RwLock;

use crate::sequence::SequenceGenerator;

/// 运行统计。对应 `MycatMonitor` + `MycatSQLLogMonitor` 的计数器部分。
#[derive(Debug, Default)]
pub struct ServerStats {
    pub connections_total: AtomicU64,
    pub connections_active: AtomicU64,
    pub queries_total: AtomicU64,
    pub query_errors: AtomicU64,
    pub slow_queries: AtomicU64,
    pub tx_commits: AtomicU64,
    pub tx_rollbacks: AtomicU64,
    pub bytes_in: AtomicU64,
    pub bytes_out: AtomicU64,
    /// SHOW STATISTIC 速率采样基准（上次采样时间 + 计数器快照）
    last_snapshot: parking_lot::Mutex<Option<(Instant, [u64; 9])>>,
}

impl ServerStats {
    /// 采样：返回 (距上次采样时长, 上次快照, 本次快照)，并更新基准
    ///（SHOW STATISTIC 的 QPS/错误率/网络速率据此计算）。
    pub fn sample_delta(&self) -> (std::time::Duration, [u64; 9], [u64; 9]) {
        let now = Instant::now();
        let cur = self.snapshot();
        let mut g = self.last_snapshot.lock();
        let (prev_time, prev) = g.map(|(t, s)| (t, s)).unwrap_or((now, cur));
        *g = Some((now, cur));
        (now.duration_since(prev_time), prev, cur)
    }
}

impl ServerStats {
    pub fn incr_conn(&self) {
        self.connections_total.fetch_add(1, Ordering::Relaxed);
        self.connections_active.fetch_add(1, Ordering::Relaxed);
    }
    pub fn decr_conn(&self) {
        self.connections_active.fetch_sub(1, Ordering::Relaxed);
    }
    pub fn incr_query(&self) {
        self.queries_total.fetch_add(1, Ordering::Relaxed);
    }
    pub fn incr_error(&self) {
        self.query_errors.fetch_add(1, Ordering::Relaxed);
    }
    pub fn incr_slow(&self) {
        self.slow_queries.fetch_add(1, Ordering::Relaxed);
    }
    pub fn add_in(&self, n: u64) {
        self.bytes_in.fetch_add(n, Ordering::Relaxed);
    }
    pub fn add_out(&self, n: u64) {
        self.bytes_out.fetch_add(n, Ordering::Relaxed);
    }
    pub fn snapshot(&self) -> [u64; 9] {
        [
            self.connections_total.load(Ordering::Relaxed),
            self.connections_active.load(Ordering::Relaxed),
            self.queries_total.load(Ordering::Relaxed),
            self.query_errors.load(Ordering::Relaxed),
            self.slow_queries.load(Ordering::Relaxed),
            self.tx_commits.load(Ordering::Relaxed),
            self.tx_rollbacks.load(Ordering::Relaxed),
            self.bytes_in.load(Ordering::Relaxed),
            self.bytes_out.load(Ordering::Relaxed),
        ]
    }
}

// ---------------------------------------------------------------------------
// 连接注册表（SHOW PROCESSLIST / KILL）
// ---------------------------------------------------------------------------

/// 单条前端连接信息。对应 `io.mycat.Process`。
pub struct ProcessInfo {
    pub id: u32,
    pub protocol: &'static str,
    pub user: RwLock<String>,
    pub host: String,
    pub db: RwLock<Option<String>>,
    pub command: RwLock<String>,
    pub sql: RwLock<Option<String>>,
    pub started: Instant,
    /// KILL 置位：前端命令循环检测到后主动断开
    pub killed: AtomicBool,
    /// pauseServer 暂停：置位后命令循环挂起，resumeServer 恢复
    pub paused: AtomicBool,
    /// 用于立即中断阻塞读（KILL 时同步 shutdown 的 std socket 副本）
    shutdown_sock: RwLock<Option<std::net::TcpStream>>,
}

impl ProcessInfo {
    pub fn elapsed_secs(&self) -> u64 {
        self.started.elapsed().as_secs()
    }
    pub fn set_sql(&self, sql: Option<String>) {
        *self.sql.write() = sql;
    }
    pub fn set_db(&self, db: Option<String>) {
        *self.db.write() = db;
    }
    pub fn set_user(&self, user: String) {
        *self.user.write() = user;
    }
    pub fn set_command(&self, cmd: &str) {
        *self.command.write() = cmd.to_string();
    }
}

/// 连接注册表。对应 `Process.PROCESS_MAP` + `MycatServer.kill/showConnections`。
#[derive(Default)]
pub struct ProcessRegistry {
    map: RwLock<HashMap<u32, Arc<ProcessInfo>>>,
}

impl ProcessRegistry {
    pub fn register(
        &self,
        id: u32,
        protocol: &'static str,
        host: String,
        sock_clone: Option<std::net::TcpStream>,
    ) -> Arc<ProcessInfo> {
        let info = Arc::new(ProcessInfo {
            id,
            protocol,
            user: RwLock::new(String::new()),
            host,
            db: RwLock::new(None),
            command: RwLock::new("Connect".into()),
            sql: RwLock::new(None),
            started: Instant::now(),
            killed: AtomicBool::new(false),
            paused: AtomicBool::new(false),
            shutdown_sock: RwLock::new(sock_clone),
        });
        self.map.write().insert(id, info.clone());
        info
    }

    pub fn unregister(&self, id: u32) {
        self.map.write().remove(&id);
    }

    /// KILL <id>：置位并 shutdown socket。返回是否命中。
    pub fn kill(&self, id: u32) -> bool {
        let info = self.map.read().get(&id).cloned();
        match info {
            Some(p) => {
                p.killed.store(true, Ordering::SeqCst);
                if let Some(sock) = p.shutdown_sock.read().as_ref() {
                    // 同步 shutdown 使阻塞中的 read 立即返回，连接任务退出
                    let _ = sock.shutdown(std::net::Shutdown::Both);
                }
                true
            }
            None => false,
        }
    }

    /// 全部连接快照（id 升序）。
    pub fn list(&self) -> Vec<Arc<ProcessInfo>> {
        let mut v: Vec<Arc<ProcessInfo>> = self.map.read().values().cloned().collect();
        v.sort_by_key(|p| p.id);
        v
    }
}

// ---------------------------------------------------------------------------
// 慢 SQL 观测（对应 Java SqlRecorderRuntime / MycatSQLLogMonitor）
// ---------------------------------------------------------------------------

/// 当前 epoch 毫秒。
pub fn epoch_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// 命令循环挂起检查：pauseServer 置位时等待（resumeServer 恢复）。
pub async fn wait_if_paused(proc: Option<&ProcessInfo>) {
    while proc
        .map(|p| p.paused.load(Ordering::SeqCst))
        .unwrap_or(false)
    {
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
}

/// 单条慢 SQL 记录。对应 Java `SqlEntry`。
#[derive(Debug, Clone)]
pub struct SlowEntry {
    pub trace_id: u64,
    pub user: String,
    pub schema: String,
    /// SQL（截断 1024 字符）
    pub sql: String,
    /// 影响/返回行数
    pub rows: u64,
    /// 开始时间（epoch 毫秒）
    pub start_time: u64,
    /// 结束时间（epoch 毫秒）
    pub end_time: u64,
    /// 执行耗时（毫秒）
    pub execute_ms: u64,
    pub target_name: String,
}

/// 慢 SQL 日志：固定容量环形缓冲 + 运行时可调阈值。
const SLOW_LOG_CAP: usize = 128;

pub struct SlowSqlLog {
    /// 慢 SQL 阈值（毫秒），默认 1000；`setSqlTimeFilter` 运行时调整
    pub threshold_ms: AtomicU64,
    entries: parking_lot::Mutex<VecDeque<SlowEntry>>,
}

impl SlowSqlLog {
    pub fn new() -> Self {
        SlowSqlLog {
            threshold_ms: AtomicU64::new(1000),
            entries: parking_lot::Mutex::new(VecDeque::new()),
        }
    }

    pub fn threshold(&self) -> u64 {
        self.threshold_ms.load(Ordering::Relaxed)
    }

    pub fn set_threshold(&self, ms: u64) {
        self.threshold_ms.store(ms, Ordering::Relaxed);
    }

    /// 超过阈值才落账；新记录在前，超出容量丢弃最旧。
    pub fn record_if(&self, e: SlowEntry) {
        if e.execute_ms < self.threshold() {
            return;
        }
        let mut q = self.entries.lock();
        if q.len() >= SLOW_LOG_CAP {
            q.pop_back();
        }
        q.push_front(e);
    }

    /// 快照（按执行耗时降序，与 Java showSlowSql 排序一致）。
    pub fn snapshot(&self) -> Vec<SlowEntry> {
        let mut v: Vec<SlowEntry> = self.entries.lock().iter().cloned().collect();
        v.sort_by(|a, b| b.execute_ms.cmp(&a.execute_ms));
        v
    }

    /// 当前记录数。
    pub fn len(&self) -> usize {
        self.entries.lock().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl Default for SlowSqlLog {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// ServerState
// ---------------------------------------------------------------------------

/// 运行时快照：config/metadata/replica/sequences 必须**原子替换**。
/// 分散在多个独立 RwLock 中逐个换入会让并发查询观察到"新元数据 + 旧副本运行时"
/// 的混合状态（路由到已删除数据源 / NoDatasource）。reload 只做一次 `snap` 写入。
pub struct RuntimeSnapshot {
    pub config: Arc<MycatRouterConfig>,
    pub metadata: Arc<Metadata>,
    pub replica: Arc<ReplicaSelectorRuntime>,
    pub sequences: Arc<SequenceGenerator>,
    /// 代理模式：是否启用
    pub proxy_mode: bool,
    /// 代理模式：透传目标（集群/数据源名）
    pub proxy_target: String,
    /// 代理模式：编译后的拦截规则（缓存之前判定）
    pub intercepts: Arc<Vec<crate::intercept::CompiledRule>>,
}

/// 全局服务注册表。对应 `MetaClusterCurrent` 上下文。
pub struct ServerState {
    snap: RwLock<Arc<RuntimeSnapshot>>,
    pub conn_mgr: Arc<ConnectionManager>,
    /// (心跳检测器, 停止标志)；reload 时停旧起新
    heartbeat: RwLock<(Arc<HeartbeatDetector>, Arc<AtomicBool>)>,
    pub stats: Arc<ServerStats>,
    /// 慢 SQL 观测（showSlowSql / setSqlTimeFilter）
    pub slow_log: Arc<SlowSqlLog>,
    /// setAcceptConnect=0 后拒绝新连接（bootstrap accept 循环检查）
    pub accept_paused: AtomicBool,
    /// 每用户活跃连接数（loginLimit 强制）
    user_conns: RwLock<HashMap<String, u32>>,
    /// setReadyToCloseSQL 探针 SQL：命中即回 OK（运维确认可关停）
    pub ready_to_close_sql: RwLock<Option<String>>,
    /// 前端 TLS 配置（server.json sslCert/sslKey 均为 PEM 路径时加载）
    pub tls_acceptor: RwLock<Option<Arc<tokio_rustls::TlsAcceptor>>>,
    /// 连接注册表（PROCESSLIST/KILL）
    pub processes: Arc<ProcessRegistry>,
    /// Oracle 数据源连接池（dbType=oracle）
    pub oracle_pools: RwLock<HashMap<String, Arc<mycat3_oracle::OraclePool>>>,
    /// PostgreSQL 数据源连接池（dbType=postgresql）
    pub pg_pools: RwLock<HashMap<String, Arc<mycat3_pgclient::PgDataSourcePool>>>,
    /// SQL 结果缓存（createSqlCache/showSqlCaches/dropSqlCache）
    pub sql_cache: Arc<crate::sqlcache::SqlCacheManager>,
    /// XA 协调者决策日志（MySQL-only 2PC 崩溃恢复）
    pub xa_log: Arc<crate::xa::XaLog>,
    /// 启动时是否执行 XA 恢复（默认 true；测试/特殊部署可关）
    pub xa_recovery: AtomicBool,
    /// 配置持久化目录（DDL 自动化写文件用；None = 仅内存，测试友好）
    pub home: RwLock<Option<PathBuf>>,
    /// 启动时间（监控展示）
    pub started_at: Instant,
    /// 下一个连接 ID。对应 MycatDataContextImpl 的全局 AtomicLong IDS。
    next_conn_id: AtomicU64,
}

impl ServerState {
    pub fn new(cfg: MycatRouterConfig) -> anyhow::Result<Arc<Self>> {
        Self::with_home(cfg, None)
    }

    pub fn with_home(cfg: MycatRouterConfig, home: Option<PathBuf>) -> anyhow::Result<Arc<Self>> {
        let metadata = Metadata::build(&cfg)?;
        let replica = ReplicaSelectorRuntime::build(&cfg);
        let conn_mgr = ConnectionManager::new();
        let heartbeat = HeartbeatDetector::new(replica.clone(), conn_mgr.clone());
        let sequences = SequenceGenerator::new(&cfg, 1);
        let ssl_cert = cfg.server.server.ssl_cert.clone();
        let ssl_key = cfg.server.server.ssl_key.clone();
        let proxy_mode = cfg.is_proxy_mode();
        let proxy_target = cfg.proxy_target();
        let intercepts = Arc::new(
            crate::intercept::compile(&cfg.server.proxy.intercepts)
                .map_err(|e| anyhow::anyhow!("intercept config invalid: {}", e))?,
        );
        let snap = RuntimeSnapshot {
            config: Arc::new(cfg),
            metadata: Arc::new(metadata),
            replica,
            sequences: Arc::new(sequences),
            proxy_mode,
            proxy_target,
            intercepts,
        };
        let sql_cache = Arc::new(crate::sqlcache::SqlCacheManager::new());
        let state = Arc::new(ServerState {
            snap: RwLock::new(Arc::new(snap)),
            conn_mgr,
            heartbeat: RwLock::new((heartbeat, Arc::new(AtomicBool::new(false)))),
            stats: Arc::new(ServerStats::default()),
            slow_log: Arc::new(SlowSqlLog::new()),
            accept_paused: AtomicBool::new(false),
            user_conns: RwLock::new(HashMap::new()),
            ready_to_close_sql: RwLock::new(None),
            tls_acceptor: RwLock::new(None),
            processes: Arc::new(ProcessRegistry::default()),
            oracle_pools: RwLock::new(HashMap::new()),
            pg_pools: RwLock::new(HashMap::new()),
            xa_log: Arc::new(crate::xa::XaLog::new(
                home.as_ref().map(|h| h.join("xa").join("xa-wal.log")),
            )),
            xa_recovery: AtomicBool::new(true),
            sql_cache,
            home: RwLock::new(home),
            started_at: Instant::now(),
            next_conn_id: AtomicU64::new(1),
        });
        // 挂接生产执行器与配置来源（幂等；无 home 时文件持久化自动禁用）
        state.sql_cache.attach(state.clone());
        // TLS 配置加载（相对路径按 home 解析；未配置 → None）
        *state.tls_acceptor.write() =
            load_tls_acceptor(state.home(), ssl_cert.as_deref(), ssl_key.as_deref());
        Ok(state)
    }

    pub fn next_connection_id(&self) -> u32 {
        self.next_conn_id.fetch_add(1, Ordering::Relaxed) as u32
    }

    /// 用户活跃连接计数 +1（认证成功后）。
    pub fn incr_user_conn(&self, user: &str) -> u32 {
        let mut m = self.user_conns.write();
        let v = m.entry(user.to_string()).or_insert(0);
        *v += 1;
        *v
    }

    /// 用户活跃连接计数 -1（连接关闭）。
    pub fn decr_user_conn(&self, user: &str) {
        let mut m = self.user_conns.write();
        if let Some(v) = m.get_mut(user) {
            *v = v.saturating_sub(1);
            if *v == 0 {
                m.remove(user);
            }
        }
    }

    /// 用户当前活跃连接数（loginLimit 判定）。
    pub fn user_conn_count(&self, user: &str) -> u32 {
        self.user_conns.read().get(user).copied().unwrap_or(0)
    }

    /// `pauseServer`：挂起指定连接（含自身会话；对应 Java MycatServer.pause）。
    /// 被挂起的连接保留、已到达的命令排队，`resumeServer` 恢复执行。
    pub fn pause_connections(&self, ids: &[u32], self_id: u32) {
        let map = self.processes.map.read();
        for id in ids.iter().chain(std::iter::once(&self_id)) {
            if let Some(p) = map.get(id) {
                p.paused.store(true, Ordering::SeqCst);
            }
        }
    }

    /// `resumeServer`：恢复全部被挂起的连接。
    pub fn resume_all_connections(&self) {
        let map = self.processes.map.read();
        for p in map.values() {
            p.paused.store(false, Ordering::SeqCst);
        }
    }

    /// 完整运行时快照（需要一致读多份对象时使用）。
    pub fn snapshot(&self) -> Arc<RuntimeSnapshot> {
        self.snap.read().clone()
    }

    /// 是否纯代理模式。
    pub fn is_proxy_mode(&self) -> bool {
        self.snap.read().proxy_mode
    }

    /// 代理模式透传目标。
    pub fn proxy_target(&self) -> String {
        self.snap.read().proxy_target.clone()
    }

    /// 代理模式拦截规则快照。
    pub fn intercepts(&self) -> Arc<Vec<crate::intercept::CompiledRule>> {
        self.snap.read().intercepts.clone()
    }

    pub fn metadata(&self) -> Arc<Metadata> {
        self.snap.read().metadata.clone()
    }

    pub fn config(&self) -> Arc<MycatRouterConfig> {
        self.snap.read().config.clone()
    }

    pub fn replica(&self) -> Arc<ReplicaSelectorRuntime> {
        self.snap.read().replica.clone()
    }

    pub fn sequences(&self) -> Arc<SequenceGenerator> {
        self.snap.read().sequences.clone()
    }

    pub fn heartbeat(&self) -> Arc<HeartbeatDetector> {
        self.heartbeat.read().0.clone()
    }

    pub fn home(&self) -> Option<PathBuf> {
        self.home.read().clone()
    }

    pub fn set_home(&self, home: PathBuf) {
        *self.home.write() = Some(home);
    }

    /// 热重载配置（对应 ConfigUpdater.loadConfigFromFile → ops.commit() 的资源重建）：
    /// 重建 metadata / replica / heartbeat / sequences，连接池按名字保留复用。
    /// config/metadata/replica/sequences 经 `RuntimeSnapshot` **单次原子换入**，
    /// 查询侧不会观察到新旧混合状态。
    /// `heartbeat_period` 为 None 时不重启心跳循环（测试场景）。
    pub fn reload(
        &self,
        cfg: MycatRouterConfig,
        heartbeat_period: Option<std::time::Duration>,
    ) -> anyhow::Result<()> {
        // 先验证新配置可编译，失败则保持旧配置（对应 commit 失败 giveup 语义）
        let metadata = Metadata::build(&cfg)?;
        let replica = ReplicaSelectorRuntime::build(&cfg);
        let heartbeat = HeartbeatDetector::new(replica.clone(), self.conn_mgr.clone());
        let mycat_id = cfg.server.server.mycat_id;
        let sequences = SequenceGenerator::new(&cfg, mycat_id);
        let proxy_mode = cfg.is_proxy_mode();
        // 运行模式切换（local↔proxy）要求重启：热重载各资源的构造顺序/语义不同，
        // 运行中切换会留下不一致状态
        if proxy_mode != self.snap.read().proxy_mode {
            anyhow::bail!(
                "server.mode cannot be changed at runtime (restart required to switch local/proxy)"
            );
        }
        let proxy_target = cfg.proxy_target();
        let intercepts = Arc::new(
            crate::intercept::compile(&cfg.server.proxy.intercepts)
                .map_err(|e| anyhow::anyhow!("intercept config invalid: {}", e))?,
        );

        *self.snap.write() = Arc::new(RuntimeSnapshot {
            config: Arc::new(cfg),
            metadata: Arc::new(metadata),
            replica,
            sequences: Arc::new(sequences),
            proxy_mode,
            proxy_target,
            intercepts,
        });

        // 停旧心跳循环，起新循环
        let new_stop = Arc::new(AtomicBool::new(false));
        {
            let mut g = self.heartbeat.write();
            g.1.store(true, Ordering::SeqCst);
            *g = (heartbeat, new_stop.clone());
        }
        if let Some(period) = heartbeat_period {
            let hb = self.heartbeat.read().0.clone();
            if let Ok(handle) = tokio::runtime::Handle::try_current() {
                handle.spawn(async move {
                    hb.run_loop(period, new_stop).await;
                });
            }
        }
        tracing::info!("config reloaded: metadata/replica/heartbeat/sequences rebuilt");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(ms: u64, id: u64) -> SlowEntry {
        SlowEntry {
            trace_id: id,
            user: "u".into(),
            schema: "db1".into(),
            sql: format!("select {}", id),
            rows: 1,
            start_time: 100,
            end_time: 100 + ms,
            execute_ms: ms,
            target_name: String::new(),
        }
    }

    #[test]
    fn test_slow_log_threshold_and_snapshot() {
        let log = SlowSqlLog::new();
        // 默认阈值 1000：低于阈值不落账
        log.record_if(entry(999, 1));
        assert_eq!(log.len(), 0);
        log.record_if(entry(1000, 1));
        log.record_if(entry(1500, 2));
        log.record_if(entry(1200, 3));
        assert_eq!(log.len(), 3);
        // 快照按耗时降序
        let snap = log.snapshot();
        assert_eq!(snap[0].trace_id, 2);
        assert_eq!(snap[2].trace_id, 1);
        // 阈值运行时调整
        log.set_threshold(1100);
        log.record_if(entry(1050, 4));
        assert_eq!(log.len(), 3); // 1050 < 1100 不落账
        assert_eq!(log.threshold(), 1100);
    }

    #[test]
    fn test_slow_log_capacity() {
        let log = SlowSqlLog::new();
        log.set_threshold(1);
        for i in 0..200u64 {
            log.record_if(entry(2, i));
        }
        assert_eq!(log.len(), 128); // 容量上限
        let snap = log.snapshot();
        assert_eq!(snap[0].trace_id, 199); // 最新在前
        assert_eq!(snap[127].trace_id, 72); // 最旧的被淘汰
    }
}

/// 加载前端 TLS 配置（PEM 证书 + 私钥；任一缺失 → None = TLS 关闭）。
fn load_tls_acceptor(
    home: Option<PathBuf>,
    cert: Option<&str>,
    key: Option<&str>,
) -> Option<Arc<tokio_rustls::TlsAcceptor>> {
    let (cert, key) = (cert?, key?);
    let cert_path = if std::path::Path::new(cert).is_absolute() {
        std::path::PathBuf::from(cert)
    } else {
        home.as_ref()?.join(cert)
    };
    let key_path = if std::path::Path::new(key).is_absolute() {
        std::path::PathBuf::from(key)
    } else {
        home.as_ref()?.join(key)
    };
    let certs: Vec<rustls::pki_types::CertificateDer<'static>> = rustls_pemfile::certs(
        &mut std::io::BufReader::new(std::fs::File::open(cert_path).ok()?),
    )
    .collect::<Result<_, _>>()
    .ok()?;
    let key = rustls_pemfile::private_key(&mut std::io::BufReader::new(
        std::fs::File::open(key_path).ok()?,
    ))
    .ok()??;
    let config = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .ok()?;
    tracing::info!("frontend TLS enabled (MySQL/PostgreSQL)");
    Some(Arc::new(tokio_rustls::TlsAcceptor::from(Arc::new(config))))
}
