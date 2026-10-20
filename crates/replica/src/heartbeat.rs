//! 心跳检测与故障切换。
//!
//! 移植自 Mycat3 `replica.heartbeat` 包：
//! - `HeartBeatStatus`：连续错误计数，达到 `maxRetry` 才判定故障
//! - `MySQLSingleHeartBeatStrategy`：`select 1`
//! - `MySQLMasterSlaveBeatStrategy`：主 `select 1`，从 `show slave status`
//!   （Slave_IO_Running == Slave_SQL_Running == Yes；Seconds_Behind_Master 超过
//!   slaveThreshold → 剔除读）
//! - `MySQLGaleraHeartBeatStrategy`：`show status like 'wsrep%'`
//! - `MGRHeartBeatStrategy`：performance_schema.replication_group_members + @@read_only
//! - `DefaultHeartbeatFlow`：超时/错误处理 + `sendDataSourceStatus`（切换节流）
//!
//! 心跳 SQL 常量与 Java `GlobalConst` 一致。

use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

use mycat3_client::{
    ClientError, ClientResult, ConnectionManager, MySQLConnection, QueryOutcome, ResultSet,
};
use mycat3_config::{DatasourceConfig, ReplicaType};
use parking_lot::RwLock;
use tokio::task::JoinHandle;
use tracing::{info, warn};

use crate::{PhysicsInstance, ReplicaSelectorRuntime};

/// GlobalConst 心跳 SQL。
pub const SINGLE_NODE_HEARTBEAT_SQL: &str = "select 1";
pub const MASTER_SLAVE_HEARTBEAT_SQL: &str = "show slave status";
pub const GARELA_CLUSTER_HEARTBEAT_SQL: &str = "show status like 'wsrep%'";
pub const GROUP_REPLICATION_HEARTBEAT_SQL: &str =
    "SELECT MEMBER_STATE FROM performance_schema.replication_group_members WHERE MEMBER_ID = @@server_uuid";

/// 数据源状态枚举。对应 `DatasourceEnum`。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DatasourceStatus {
    Ok,
    Error,
    Timeout,
    Init,
}

/// 单次心跳的检查结论。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HeartbeatOutcome {
    /// 健康
    Healthy,
    /// 健康但复制延迟超过阈值（不参与读，仍是活的）
    SlaveBehind { seconds: u64 },
    /// 复制线程停止 / 状态异常
    Unhealthy(String),
    /// 连接或查询失败
    Failed(String),
    /// 查询超时
    TimedOut,
}

impl HeartbeatOutcome {
    pub fn is_ok(&self) -> bool {
        matches!(
            self,
            HeartbeatOutcome::Healthy | HeartbeatOutcome::SlaveBehind { .. }
        )
    }
}

/// 连续错误计数。对应 `HeartBeatStatus`。
pub struct HeartBeatStatus {
    error_count: AtomicU32,
    max_retry: u32,
}

impl HeartBeatStatus {
    pub fn new(max_retry: u32) -> Self {
        HeartBeatStatus {
            error_count: AtomicU32::new(0),
            max_retry: max_retry.max(1),
        }
    }

    /// 记录一次成功：清零计数。对应 setStatus(OK)。
    pub fn record_ok(&self) {
        self.error_count.store(0, Ordering::Relaxed);
    }

    /// 记录一次失败：计数 +1，返回是否达到故障阈值。对应 setError/setTimeout。
    pub fn record_error(&self) -> bool {
        self.error_count.fetch_add(1, Ordering::Relaxed) + 1 >= self.max_retry
    }

    pub fn error_count(&self) -> u32 {
        self.error_count.load(Ordering::Relaxed)
    }
}

// ---------------------------------------------------------------------------
// 策略结果处理（纯函数，便于单测）
// ---------------------------------------------------------------------------

/// 将结果集首行转为 列名(小写)→值 映射。
pub fn first_row_map(rs: &ResultSet) -> HashMap<String, String> {
    let mut map = HashMap::new();
    if let Some(row) = rs.decode_rows().first() {
        for (i, col) in rs.columns.iter().enumerate() {
            let v = row
                .get(i)
                .and_then(|b| b.as_ref())
                .map(|b| String::from_utf8_lossy(b).to_string())
                .unwrap_or_default();
            map.insert(col.name.to_lowercase(), v);
        }
    }
    map
}

/// `show slave status` 检查。对应 `MySQLMasterSlaveBeatStrategy.process`。
pub fn check_slave_status(rs: &ResultSet, slave_threshold: u64) -> HeartbeatOutcome {
    let row = first_row_map(rs);
    if row.is_empty() {
        // 无输出：该实例其实是主库（无从库配置）→ 视为健康
        return HeartbeatOutcome::Healthy;
    }
    let io = row
        .get("slave_io_running")
        .map(|s| s.as_str())
        .unwrap_or("");
    let sql = row
        .get("slave_sql_running")
        .map(|s| s.as_str())
        .unwrap_or("");
    if !io.eq_ignore_ascii_case("Yes") || !sql.eq_ignore_ascii_case("Yes") {
        return HeartbeatOutcome::Unhealthy(format!(
            "replication stopped: Slave_IO_Running={}, Slave_SQL_Running={}",
            io, sql
        ));
    }
    let behind = row
        .get("seconds_behind_master")
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(0);
    if behind > slave_threshold {
        return HeartbeatOutcome::SlaveBehind { seconds: behind };
    }
    HeartbeatOutcome::Healthy
}

/// Galera 检查。对应 `MySQLGaleraHeartBeatStrategy.process`。
pub fn check_galera_status(rs: &ResultSet, slave_threshold: u64) -> HeartbeatOutcome {
    let mut vars: HashMap<String, String> = HashMap::new();
    for row in rs.decode_rows() {
        if row.len() >= 2 {
            let k = row[0]
                .as_ref()
                .map(|b| String::from_utf8_lossy(b).to_lowercase())
                .unwrap_or_default();
            let v = row[1]
                .as_ref()
                .map(|b| String::from_utf8_lossy(b).to_string())
                .unwrap_or_default();
            vars.insert(k, v);
        }
    }
    let cluster_status = vars
        .get("wsrep_cluster_status")
        .map(|s| s.as_str())
        .unwrap_or("");
    let connected = vars
        .get("wsrep_connected")
        .map(|s| s.as_str())
        .unwrap_or("");
    let ready = vars.get("wsrep_ready").map(|s| s.as_str()).unwrap_or("");
    if cluster_status.eq_ignore_ascii_case("Primary")
        && connected.eq_ignore_ascii_case("ON")
        && ready.eq_ignore_ascii_case("ON")
    {
        let paused = vars
            .get("wsrep_flow_control_paused")
            .and_then(|s| s.parse::<f64>().ok())
            .unwrap_or(0.0);
        if paused > slave_threshold as f64 {
            return HeartbeatOutcome::SlaveBehind {
                seconds: paused as u64,
            };
        }
        return HeartbeatOutcome::Healthy;
    }
    HeartbeatOutcome::Unhealthy(format!(
        "galera not primary: status={}, connected={}, ready={}",
        cluster_status, connected, ready
    ))
}

/// MGR 检查。对应 `MGRHeartBeatStrategy.process`（简化：MEMBER_STATE=ONLINE 即健康）。
pub fn check_mgr_status(rs: &ResultSet) -> HeartbeatOutcome {
    let rows = rs.decode_rows();
    if let Some(row) = rows.first() {
        let state = row[0]
            .as_ref()
            .map(|b| String::from_utf8_lossy(b).to_string())
            .unwrap_or_default();
        if state.eq_ignore_ascii_case("ONLINE") {
            return HeartbeatOutcome::Healthy;
        }
        return HeartbeatOutcome::Unhealthy(format!("MGR member state: {}", state));
    }
    HeartbeatOutcome::Unhealthy("MGR member not found".into())
}

// ---------------------------------------------------------------------------
// 心跳检测器
// ---------------------------------------------------------------------------

/// 心跳检测器。对应 `ReplicaSelectorRuntime` 的心跳调度 + `DefaultHeartbeatFlow`。
pub struct HeartbeatDetector {
    runtime: Arc<ReplicaSelectorRuntime>,
    conn_mgr: Arc<ConnectionManager>,
    statuses: RwLock<HashMap<String, Arc<HeartBeatStatus>>>,
    /// SQL Server 心跳复用连接（避免每 5s 重新 TLS/Login7 握手）。
    mssql_conns: tokio::sync::Mutex<HashMap<String, mycat3_mssql::MssqlConnection>>,
}

impl HeartbeatDetector {
    pub fn new(
        runtime: Arc<ReplicaSelectorRuntime>,
        conn_mgr: Arc<ConnectionManager>,
    ) -> Arc<Self> {
        Arc::new(HeartbeatDetector {
            runtime,
            conn_mgr,
            statuses: RwLock::new(HashMap::new()),
            mssql_conns: tokio::sync::Mutex::new(HashMap::new()),
        })
    }

    fn status_of(&self, ds: &str, max_retry: u32) -> Arc<HeartBeatStatus> {
        if let Some(s) = self.statuses.read().get(ds) {
            return s.clone();
        }
        let s = Arc::new(HeartBeatStatus::new(max_retry));
        self.statuses.write().insert(ds.to_string(), s.clone());
        s
    }

    /// 对单个数据源执行一轮心跳（它在每个所属集群中按角色检查）。
    /// 对应 `HeartbeatFlow.heartbeat()` + `sendDataSourceStatus`。
    pub async fn heartbeat_datasource(&self, ds_name: &str) {
        let cfg = match self.runtime.datasource_config(ds_name) {
            Some(c) => c,
            None => return,
        };
        let clusters = self.runtime.clusters_of(ds_name);
        if clusters.is_empty() {
            // 不属于任何集群的数据源：单机探活
            let outcome = self.probe(&cfg, ReplicaType::Single, true, 0).await;
            self.apply_outcome(ds_name, &outcome, 3, &[]);
            return;
        }
        for cluster in &clusters {
            let is_master = cluster.write_list().iter().any(|i| i.name == ds_name);
            let outcome = self
                .probe(
                    &cfg,
                    cluster.replica_type,
                    is_master,
                    cluster.heartbeat.slave_threshold,
                )
                .await;
            let switched = self.apply_outcome(
                ds_name,
                &outcome,
                cluster.heartbeat.max_retry,
                &[cluster.clone()],
            );
            if switched {
                info!(cluster = %cluster.name, datasource = %ds_name, "master switched by heartbeat");
            }
        }
    }

    /// 按集群类型/角色执行探测 SQL。对应各 HeartBeatStrategy。
    async fn probe(
        &self,
        cfg: &DatasourceConfig,
        replica_type: ReplicaType,
        is_master: bool,
        slave_threshold: u64,
    ) -> HeartbeatOutcome {
        // Oracle 数据源：单机探活语义（select 1 from dual），无主从复制探测
        if cfg.db_type.eq_ignore_ascii_case("oracle") {
            let timeout = Duration::from_millis(cfg.max_connect_timeout.clamp(1000, 30_000));
            let oracle_cfg =
                match mycat3_oracle::config_from_jdbc_url(&cfg.url, &cfg.user, &cfg.password) {
                    Ok(c) => c,
                    Err(e) => return HeartbeatOutcome::Failed(e.to_string()),
                };
            return if mycat3_oracle::ping_direct(oracle_cfg, timeout).await {
                HeartbeatOutcome::Healthy
            } else {
                HeartbeatOutcome::Failed("oracle ping failed".into())
            };
        }
        // SQL Server 数据源：单机探活语义（select 1），无 AG 复制探测
        if cfg.db_type.eq_ignore_ascii_case("mssql")
            || cfg.db_type.eq_ignore_ascii_case("sqlserver")
            || cfg.db_type.eq_ignore_ascii_case("mssqlserver")
        {
            let timeout = Duration::from_millis(cfg.max_connect_timeout.clamp(1000, 30_000));
            let ep = match mycat3_mssql::parse_mssql_jdbc_url(&cfg.url, &cfg.user, &cfg.password) {
                Ok(mut e) => {
                    e.connect_timeout = timeout;
                    e
                }
                Err(e) => return HeartbeatOutcome::Failed(e.to_string()),
            };
            let mut cache = self.mssql_conns.lock().await;
            if let Some(conn) = cache.get_mut(&cfg.name) {
                if conn.ping().await {
                    return HeartbeatOutcome::Healthy;
                }
                cache.remove(&cfg.name);
            }
            return match tokio::time::timeout(timeout, mycat3_mssql::MssqlConnection::connect(&ep))
                .await
            {
                Ok(Ok(mut conn)) => {
                    if conn.ping().await {
                        cache.insert(cfg.name.clone(), conn);
                        HeartbeatOutcome::Healthy
                    } else {
                        HeartbeatOutcome::Failed("sqlserver ping failed".into())
                    }
                }
                _ => HeartbeatOutcome::Failed("sqlserver connect failed".into()),
            };
        }
        // PostgreSQL 数据源：单机探活语义（select 1），无主从复制探测
        if cfg.db_type.eq_ignore_ascii_case("postgresql")
            || cfg.db_type.eq_ignore_ascii_case("postgres")
            || cfg.db_type.eq_ignore_ascii_case("pgsql")
        {
            let timeout = Duration::from_millis(cfg.max_connect_timeout.clamp(1000, 30_000));
            let ep = match mycat3_pgclient::parse_pg_jdbc_url(&cfg.url, &cfg.user, &cfg.password) {
                Ok(e) => e,
                Err(e) => return HeartbeatOutcome::Failed(e.to_string()),
            };
            return match tokio::time::timeout(timeout, mycat3_pgclient::PgConnection::connect(&ep))
                .await
            {
                Ok(Ok(mut conn)) => {
                    if conn.query("select 1").await.is_ok() {
                        HeartbeatOutcome::Healthy
                    } else {
                        HeartbeatOutcome::Failed("postgres ping failed".into())
                    }
                }
                _ => HeartbeatOutcome::Failed("postgres connect failed".into()),
            };
        }
        let (sql, checker) = match replica_type {
            ReplicaType::Galera => (
                GARELA_CLUSTER_HEARTBEAT_SQL,
                Checker::Galera(slave_threshold),
            ),
            ReplicaType::Mgr => (GROUP_REPLICATION_HEARTBEAT_SQL, Checker::Mgr),
            ReplicaType::MasterSlave | ReplicaType::Mha if !is_master => {
                (MASTER_SLAVE_HEARTBEAT_SQL, Checker::Slave(slave_threshold))
            }
            _ => (SINGLE_NODE_HEARTBEAT_SQL, Checker::Simple),
        };
        let timeout = Duration::from_millis(cfg.max_connect_timeout.clamp(1000, 30_000));
        let mut conn = match self.connect(cfg).await {
            Ok(c) => c,
            Err(ClientError::Timeout(_)) => return HeartbeatOutcome::TimedOut,
            Err(e) => return HeartbeatOutcome::Failed(e.to_string()),
        };
        match conn.query_timeout(sql, timeout).await {
            Ok(QueryOutcome::Rows(rs)) => checker.check(&rs),
            Ok(QueryOutcome::Ok(_)) => HeartbeatOutcome::Healthy,
            Err(ClientError::Timeout(_)) => HeartbeatOutcome::TimedOut,
            Err(e) => HeartbeatOutcome::Failed(e.to_string()),
        }
    }

    /// 取探测连接：优先复用连接池空闲连接（免新建握手，修掉每周期新建连接的
    /// 固定开销）；池无空闲/未建池时回落直连。
    async fn connect(&self, cfg: &DatasourceConfig) -> ClientResult<MySQLConnection> {
        if let Ok(pool) = self.conn_mgr.get_pool(cfg) {
            if let Ok(checked) = pool.acquire().await {
                // 脱离池（探活只读；关闭由 Drop 负责）
                return Ok(checked.take());
            }
        }
        self.conn_mgr.connect_direct(cfg).await
    }

    /// 应用心跳结论：更新实例状态 + 错误计数 + 触发切换。
    /// 对应 `DefaultHeartbeatFlow.sendDataSourceStatus`。返回是否发生切换。
    fn apply_outcome(
        &self,
        ds_name: &str,
        outcome: &HeartbeatOutcome,
        max_retry: u32,
        clusters: &[Arc<crate::ReplicaSelector>],
    ) -> bool {
        let instance = match self.runtime.instance(ds_name) {
            Some(i) => i,
            None => return false,
        };
        let status = self.status_of(ds_name, max_retry);
        match outcome {
            HeartbeatOutcome::Healthy => {
                status.record_ok();
                instance.set_alive(true);
                instance.set_select_read(true);
            }
            HeartbeatOutcome::SlaveBehind { seconds } => {
                status.record_ok();
                instance.set_alive(true);
                // 延迟超过阈值：存活但不参与读
                instance.set_select_read(false);
                warn!(datasource = %ds_name, seconds, "slave behind master, removed from read balance");
            }
            HeartbeatOutcome::Unhealthy(reason) | HeartbeatOutcome::Failed(reason) => {
                warn!(datasource = %ds_name, reason, "heartbeat error");
                if status.record_error() {
                    instance.set_alive(false);
                    let mut switched = false;
                    for c in clusters {
                        switched |= c.try_switch();
                    }
                    if clusters.is_empty() {
                        for c in self.runtime.clusters_of(ds_name) {
                            switched |= c.try_switch();
                        }
                    }
                    return switched;
                }
            }
            HeartbeatOutcome::TimedOut => {
                warn!(datasource = %ds_name, "heartbeat timeout");
                if status.record_error() {
                    instance.set_alive(false);
                    let mut switched = false;
                    for c in clusters {
                        switched |= c.try_switch();
                    }
                    return switched;
                }
            }
        }
        false
    }

    /// 启动周期心跳任务。对应 `ReplicaSelectorRuntime.start()` 的定时调度。
    pub fn spawn_loop(self: &Arc<Self>, period: Duration) -> JoinHandle<()> {
        let this = self.clone();
        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        tokio::spawn(async move {
            this.run_loop(period, stop).await;
        })
    }

    /// 可停止的心跳循环（配置热重载时停旧起新）。
    pub async fn run_loop(
        self: &Arc<Self>,
        period: Duration,
        stop: Arc<std::sync::atomic::AtomicBool>,
    ) {
        let mut ticker = tokio::time::interval(period);
        loop {
            ticker.tick().await;
            if stop.load(std::sync::atomic::Ordering::SeqCst) {
                info!("heartbeat loop stopped (config reload)");
                return;
            }
            let names = self.runtime.all_datasource_names();
            for name in names {
                let this = self.clone();
                tokio::spawn(async move {
                    this.heartbeat_datasource(&name).await;
                });
            }
        }
    }

    /// 状态快照（监控）。
    pub fn snapshot(&self) -> Vec<(String, u32)> {
        self.statuses
            .read()
            .iter()
            .map(|(k, v)| (k.clone(), v.error_count()))
            .collect()
    }
}

enum Checker {
    Simple,
    Slave(u64),
    Galera(u64),
    Mgr,
}

impl Checker {
    fn check(&self, rs: &ResultSet) -> HeartbeatOutcome {
        match self {
            Checker::Simple => HeartbeatOutcome::Healthy,
            Checker::Slave(t) => check_slave_status(rs, *t),
            Checker::Galera(t) => check_galera_status(rs, *t),
            Checker::Mgr => check_mgr_status(rs),
        }
    }
}

/// 供 server 层做活跃计数的辅助。
pub fn with_active<F, R>(inst: &PhysicsInstance, f: F) -> R
where
    F: FnOnce() -> R,
{
    inst.incr_active();
    let r = f();
    inst.decr_active();
    r
}

#[cfg(test)]
mod tests {
    use super::*;
    use mycat3_protocol::consts::field_type;
    use mycat3_protocol::packets::{build_text_row, ColumnDef};

    /// 构造一个 show slave status 风格的结果集。
    fn slave_status_rs(io: &str, sql: &str, behind: &str) -> ResultSet {
        let columns = vec![
            ColumnDef::new("Slave_IO_Running", field_type::VAR_STRING),
            ColumnDef::new("Slave_SQL_Running", field_type::VAR_STRING),
            ColumnDef::new("Seconds_Behind_Master", field_type::LONGLONG),
        ];
        let row = build_text_row(&[
            Some(io.as_bytes()),
            Some(sql.as_bytes()),
            Some(behind.as_bytes()),
        ]);
        ResultSet {
            column_count: 3,
            column_def_payloads: columns.iter().map(|c| c.encode()).collect(),
            columns,
            row_payloads: vec![row],
            final_status: 2,
            warnings: 0,
        }
    }

    #[test]
    fn test_check_slave_status_healthy() {
        let rs = slave_status_rs("Yes", "Yes", "0");
        assert_eq!(check_slave_status(&rs, 0), HeartbeatOutcome::Healthy);
    }

    #[test]
    fn test_check_slave_status_behind() {
        let rs = slave_status_rs("Yes", "Yes", "5");
        assert_eq!(
            check_slave_status(&rs, 0),
            HeartbeatOutcome::SlaveBehind { seconds: 5 }
        );
        // 阈值内仍健康
        assert_eq!(check_slave_status(&rs, 10), HeartbeatOutcome::Healthy);
    }

    #[test]
    fn test_check_slave_status_broken() {
        let rs = slave_status_rs("No", "Yes", "0");
        assert!(matches!(
            check_slave_status(&rs, 0),
            HeartbeatOutcome::Unhealthy(_)
        ));
    }

    #[test]
    fn test_check_slave_status_master_empty() {
        // 主库上 show slave status 为空 → 健康
        let rs = ResultSet {
            column_count: 0,
            column_def_payloads: vec![],
            columns: vec![],
            row_payloads: vec![],
            final_status: 2,
            warnings: 0,
        };
        assert_eq!(check_slave_status(&rs, 0), HeartbeatOutcome::Healthy);
    }

    #[test]
    fn test_check_galera() {
        let columns = vec![
            ColumnDef::new("Variable_name", field_type::VAR_STRING),
            ColumnDef::new("Value", field_type::VAR_STRING),
        ];
        let rows = vec![
            build_text_row(&[Some(b"wsrep_cluster_status"), Some(b"Primary")]),
            build_text_row(&[Some(b"wsrep_connected"), Some(b"ON")]),
            build_text_row(&[Some(b"wsrep_ready"), Some(b"ON")]),
            build_text_row(&[Some(b"wsrep_flow_control_paused"), Some(b"0")]),
        ];
        let rs = ResultSet {
            column_count: 2,
            column_def_payloads: columns.iter().map(|c| c.encode()).collect(),
            columns,
            row_payloads: rows,
            final_status: 2,
            warnings: 0,
        };
        assert_eq!(check_galera_status(&rs, 0), HeartbeatOutcome::Healthy);
    }

    #[test]
    fn test_check_galera_not_primary() {
        let columns = vec![
            ColumnDef::new("Variable_name", field_type::VAR_STRING),
            ColumnDef::new("Value", field_type::VAR_STRING),
        ];
        let rows = vec![build_text_row(&[
            Some(b"wsrep_cluster_status"),
            Some(b"non-Primary"),
        ])];
        let rs = ResultSet {
            column_count: 2,
            column_def_payloads: columns.iter().map(|c| c.encode()).collect(),
            columns,
            row_payloads: rows,
            final_status: 2,
            warnings: 0,
        };
        assert!(matches!(
            check_galera_status(&rs, 0),
            HeartbeatOutcome::Unhealthy(_)
        ));
    }

    #[test]
    fn test_check_mgr() {
        let columns = vec![ColumnDef::new("MEMBER_STATE", field_type::VAR_STRING)];
        let rs = ResultSet {
            column_count: 1,
            column_def_payloads: columns.iter().map(|c| c.encode()).collect(),
            columns,
            row_payloads: vec![build_text_row(&[Some(b"ONLINE")])],
            final_status: 2,
            warnings: 0,
        };
        assert_eq!(check_mgr_status(&rs), HeartbeatOutcome::Healthy);
    }

    #[test]
    fn test_error_count_threshold() {
        // maxRetry=3：前两次不触发，第三次触发（对应 HeartBeatStatus.errorCount >= maxRetry）
        let s = HeartBeatStatus::new(3);
        assert!(!s.record_error());
        assert!(!s.record_error());
        assert!(s.record_error());
        assert_eq!(s.error_count(), 3);
        s.record_ok();
        assert_eq!(s.error_count(), 0);
        assert!(!s.record_error());
    }

    #[tokio::test]
    async fn test_apply_outcome_updates_instance_and_switch() {
        use crate::heartbeat::tests_support::build_runtime_with_dead_master;
        let (runtime, conn_mgr) = build_runtime_with_dead_master();
        let detector = HeartbeatDetector::new(runtime.clone(), conn_mgr);
        // 连续 3 次失败 → ds0 死亡 + 切换
        for _ in 0..2 {
            assert!(!detector.apply_outcome("ds0", &HeartbeatOutcome::Failed("x".into()), 3, &[]));
        }
        assert!(detector.apply_outcome("ds0", &HeartbeatOutcome::Failed("x".into()), 3, &[]));
        assert!(!runtime.instance("ds0").unwrap().is_alive());
        // 切换后写走 ds1
        assert_eq!(
            runtime.resolve("c0", true, false, false).as_deref(),
            Some("ds1")
        );
        // 恢复：一次 OK 即复活
        detector.apply_outcome("ds0", &HeartbeatOutcome::Healthy, 3, &[]);
        assert!(runtime.instance("ds0").unwrap().is_alive());
    }

    #[tokio::test]
    async fn test_apply_outcome_slave_behind() {
        use crate::heartbeat::tests_support::build_runtime_with_dead_master;
        let (runtime, conn_mgr) = build_runtime_with_dead_master();
        let detector = HeartbeatDetector::new(runtime.clone(), conn_mgr);
        detector.apply_outcome("ds1", &HeartbeatOutcome::SlaveBehind { seconds: 9 }, 3, &[]);
        let inst = runtime.instance("ds1").unwrap();
        assert!(inst.is_alive());
        assert!(!inst.as_select_read());
    }
}

/// 测试辅助（心跳测试与 server 集成测试共用）。
#[doc(hidden)]
pub mod tests_support {
    use super::*;
    use mycat3_config::{
        BalanceType, ClusterConfig, DatasourceConfig, HeartbeatConfig, InstanceType,
        MycatRouterConfig, ReplicaSwitchType, ReplicaType,
    };

    pub fn build_runtime_with_dead_master() -> (Arc<ReplicaSelectorRuntime>, Arc<ConnectionManager>)
    {
        let mut cfg = MycatRouterConfig::new();
        for (name, port) in [("ds0", 3306u16), ("ds1", 3307)] {
            cfg.datasources.insert(
                name.into(),
                DatasourceConfig {
                    name: name.into(),
                    url: format!("jdbc:mysql://127.0.0.1:{}/db", port),
                    user: "root".into(),
                    password: "".into(),
                    db_type: "mysql".into(),
                    weight: 0,
                    max_con: 10,
                    min_con: 1,
                    max_retry_count: 3,
                    max_connect_timeout: 1000,
                    init_sqls: vec![],
                    instance_type: InstanceType::ReadWrite,
                    idle_timeout: 60_000,
                    r#type: "NATIVE".into(),
                },
            );
        }
        cfg.clusters.insert(
            "c0".into(),
            ClusterConfig {
                name: "c0".into(),
                cluster_type: ReplicaType::MasterSlave,
                switch_type: ReplicaSwitchType::Switch,
                read_balance_type: BalanceType::All,
                read_balance_name: None,
                write_balance_name: None,
                masters: vec!["ds0".into(), "ds1".into()],
                replicas: vec![],
                heartbeat: HeartbeatConfig {
                    max_retry: 3,
                    min_switch_time_interval: 0,
                    ..Default::default()
                },
                max_con: 10,
            },
        );
        let runtime = ReplicaSelectorRuntime::build(&cfg);
        (runtime, ConnectionManager::new())
    }
}
