//! 副本选择与高可用运行时。
//!
//! 移植自 Mycat3 `replica` 模块：
//! - [`PhysicsInstance`] 对应 `PhysicsInstanceImpl`（存活/可读状态、权重、活跃计数）
//! - [`ReplicaSelector`] 对应 `ReplicaDataSourceSelector`（读写实例列表、BalanceType、主从切换）
//! - [`ReplicaSelectorRuntime`] 对应 `ReplicaSelectorRuntime`
//!   （`getDatasourceNameByReplicaName`：集群名 → 物理数据源名）
//! - [`heartbeat`] 对应 `replica.heartbeat.*`（心跳检测、错误计数、切换节流）
//! - [`balance`] 对应 `plug.loadBalance.*` 负载均衡策略

pub mod balance;
pub mod heartbeat;

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use mycat3_config::{
    BalanceType, DatasourceConfig, InstanceType, MycatRouterConfig, ReplicaSwitchType, ReplicaType,
};
use parking_lot::{Mutex, RwLock};

use crate::balance::{create_strategy, LoadBalance};

/// 物理实例状态。对应 `PhysicsInstanceImpl`。
pub struct PhysicsInstance {
    pub name: String,
    pub instance_type: InstanceType,
    pub weight: i32,
    /// 是否为集群主库成员（masters 列表）
    pub is_master: bool,
    alive: AtomicBool,
    select_as_read: AtomicBool,
    active: AtomicUsize,
}

impl PhysicsInstance {
    pub fn new(
        name: String,
        instance_type: InstanceType,
        weight: i32,
        is_master: bool,
    ) -> Arc<Self> {
        Arc::new(PhysicsInstance {
            name,
            instance_type,
            weight,
            is_master,
            alive: AtomicBool::new(true),
            select_as_read: AtomicBool::new(true),
            active: AtomicUsize::new(0),
        })
    }

    #[doc(hidden)]
    pub fn new_test(name: &str, weight: i32, is_master: bool, t: InstanceType) -> Arc<Self> {
        Self::new(name.to_string(), t, weight, is_master)
    }

    pub fn is_alive(&self) -> bool {
        self.alive.load(Ordering::Acquire)
    }

    pub fn set_alive(&self, alive: bool) {
        self.alive.store(alive, Ordering::Release);
        if !alive {
            self.select_as_read.store(false, Ordering::Release);
        } else {
            self.select_as_read.store(true, Ordering::Release);
        }
    }

    pub fn as_select_read(&self) -> bool {
        self.select_as_read.load(Ordering::Acquire)
    }

    pub fn set_select_read(&self, v: bool) {
        self.select_as_read.store(v, Ordering::Release);
    }

    pub fn active(&self) -> usize {
        self.active.load(Ordering::Relaxed)
    }

    pub fn incr_active(&self) {
        self.active.fetch_add(1, Ordering::Relaxed);
    }

    pub fn decr_active(&self) {
        self.active.fetch_sub(1, Ordering::Relaxed);
    }
}

/// 单集群副本选择器。对应 `ReplicaDataSourceSelector`。
pub struct ReplicaSelector {
    pub name: String,
    pub replica_type: ReplicaType,
    pub switch_type: ReplicaSwitchType,
    pub balance_type: BalanceType,
    /// 写实例列表（写走第一个存活者；切换即重排）。对应 writeDataSourceList。
    pub(crate) write_list: RwLock<Vec<Arc<PhysicsInstance>>>,
    read_list: RwLock<Vec<Arc<PhysicsInstance>>>,
    read_strategy: Box<dyn LoadBalance>,
    /// 上次切换时间（min_switch_time_interval 节流）
    last_switch: Mutex<Option<Instant>>,
    pub min_switch_interval: Duration,
    pub heartbeat: mycat3_config::HeartbeatConfig,
}

impl ReplicaSelector {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        name: String,
        replica_type: ReplicaType,
        switch_type: ReplicaSwitchType,
        balance_type: BalanceType,
        write_list: Vec<Arc<PhysicsInstance>>,
        read_list: Vec<Arc<PhysicsInstance>>,
        read_balance_name: Option<&str>,
        heartbeat: mycat3_config::HeartbeatConfig,
    ) -> Arc<Self> {
        let min_switch = Duration::from_secs(heartbeat.min_switch_time_interval);
        Arc::new(ReplicaSelector {
            name,
            replica_type,
            switch_type,
            balance_type,
            write_list: RwLock::new(write_list),
            read_list: RwLock::new(read_list),
            read_strategy: create_strategy(read_balance_name.unwrap_or("BalanceRandom")),
            last_switch: Mutex::new(None),
            min_switch_interval: min_switch,
            heartbeat,
        })
    }

    /// 选择写数据源：write_list 第一个存活实例。对应 `getWriteDataSourceByReplicaType`。
    pub fn pick_write(&self) -> Option<Arc<PhysicsInstance>> {
        let list = self.write_list.read();
        list.iter()
            .find(|i| i.is_alive())
            .cloned()
            .or_else(|| list.first().cloned())
    }

    /// 选择读数据源：按 BalanceType 过滤候选后用读策略选择。
    /// 对应 `getDataSourceByLoadBalacneType` + `getReadDataSource`。
    pub fn pick_read(&self) -> Option<Arc<PhysicsInstance>> {
        let writes = self.write_list.read();
        let reads = self.read_list.read();
        let candidates: Vec<Arc<PhysicsInstance>> = match self.balance_type {
            // 全部实例参与（可读状态过滤）
            BalanceType::All => writes
                .iter()
                .chain(reads.iter())
                .filter(|i| i.is_alive() && i.as_select_read())
                .cloned()
                .collect(),
            // 只走写实例
            BalanceType::None => writes.iter().filter(|i| i.is_alive()).cloned().collect(),
            // 只走从库；无从库回退主库
            BalanceType::AllRead => {
                let replicas: Vec<_> = reads
                    .iter()
                    .filter(|i| i.is_alive() && i.as_select_read())
                    .cloned()
                    .collect();
                if replicas.is_empty() {
                    writes.iter().filter(|i| i.is_alive()).cloned().collect()
                } else {
                    replicas
                }
            }
            // 可读实例（未延迟从库 + 主库）
            BalanceType::ReadWrite => writes
                .iter()
                .chain(reads.iter())
                .filter(|i| i.is_alive() && i.as_select_read())
                .cloned()
                .collect(),
        };
        self.read_strategy.select(&candidates)
    }

    /// 心跳状态回报后的主从切换。对应 `notifySwitchReplicaDataSource` + `switchWriteDataSource`：
    /// 将死亡主库移到列表尾部，存活者顶上；受 min_switch_time_interval 节流。
    /// 返回是否发生了顺序切换。
    pub fn try_switch(&self) -> bool {
        if self.switch_type == ReplicaSwitchType::NotSwitch {
            return false;
        }
        let mut last = self.last_switch.lock();
        if let Some(t) = *last {
            if t.elapsed() < self.min_switch_interval {
                return false;
            }
        }
        let mut list = self.write_list.write();
        let head_alive = list.first().map(|i| i.is_alive()).unwrap_or(true);
        if head_alive {
            return false;
        }
        // 按存活分区：存活者在前（保持相对顺序），死亡者在后
        let (alive, dead): (Vec<_>, Vec<_>) = list.drain(..).partition(|i| i.is_alive());
        if alive.is_empty() {
            *list = dead;
            return false;
        }
        *list = alive.into_iter().chain(dead).collect();
        *last = Some(Instant::now());
        true
    }

    pub fn write_list(&self) -> Vec<Arc<PhysicsInstance>> {
        self.write_list.read().clone()
    }

    pub fn read_list(&self) -> Vec<Arc<PhysicsInstance>> {
        self.read_list.read().clone()
    }

    pub fn all_instances(&self) -> Vec<Arc<PhysicsInstance>> {
        let mut v = self.write_list();
        v.extend(self.read_list());
        v
    }
}

/// 全局副本运行时。对应 `ReplicaSelectorRuntime`。
pub struct ReplicaSelectorRuntime {
    /// 集群名 → 选择器
    selectors: RwLock<HashMap<String, Arc<ReplicaSelector>>>,
    /// 数据源名 → 实例
    instances: RwLock<HashMap<String, Arc<PhysicsInstance>>>,
    /// 数据源配置
    datasources: RwLock<HashMap<String, DatasourceConfig>>,
}

impl ReplicaSelectorRuntime {
    /// 从配置构建。对应 `ReplicaSelectorRuntime` 构造 + `registerReplica`。
    pub fn build(cfg: &MycatRouterConfig) -> Arc<Self> {
        let runtime = Arc::new(ReplicaSelectorRuntime {
            selectors: RwLock::new(HashMap::new()),
            instances: RwLock::new(HashMap::new()),
            datasources: RwLock::new(
                cfg.datasources
                    .iter()
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect(),
            ),
        });
        // 注册全部数据源实例（主库角色由集群配置决定）
        for (name, ds) in &cfg.datasources {
            runtime.instances.write().insert(
                name.clone(),
                PhysicsInstance::new(name.clone(), ds.instance_type, ds.weight, false),
            );
        }
        for (name, cluster) in &cfg.clusters {
            let mut write_list = Vec::new();
            for m in &cluster.masters {
                let existing = runtime.instances.read().get(m).cloned();
                if let Some(inst) = existing {
                    if !inst.is_master {
                        // 升级为主库角色实例
                        let master_inst = PhysicsInstance::new(
                            inst.name.clone(),
                            inst.instance_type,
                            inst.weight,
                            true,
                        );
                        master_inst.set_alive(inst.is_alive());
                        master_inst.set_select_read(inst.as_select_read());
                        runtime
                            .instances
                            .write()
                            .insert(m.clone(), master_inst.clone());
                        write_list.push(master_inst);
                    } else {
                        write_list.push(inst);
                    }
                }
            }
            let mut read_list = Vec::new();
            for r in &cluster.replicas {
                let existing = runtime.instances.read().get(r).cloned();
                if let Some(inst) = existing {
                    read_list.push(inst);
                }
            }
            let selector = ReplicaSelector::new(
                name.clone(),
                cluster.cluster_type,
                cluster.switch_type,
                cluster.read_balance_type,
                write_list,
                read_list,
                cluster.read_balance_name.as_deref(),
                cluster.heartbeat.clone(),
            );
            runtime.selectors.write().insert(name.clone(), selector);
        }
        runtime
    }

    /// 逻辑 target → 物理数据源名。
    /// 对应 `getDatasourceNameByReplicaName(replicaName, master, balanceType, strategy)`：
    /// 集群名按主/读选择实例；非集群名视为数据源名直接返回。
    pub fn resolve(
        &self,
        target: &str,
        master: bool,
        hint_master: bool,
        hint_slave: bool,
    ) -> Option<String> {
        if let Some(selector) = self.selectors.read().get(target) {
            let inst = if master || hint_master {
                selector.pick_write()
            } else if hint_slave {
                // 强制从库：只从 read_list 选
                let reads = selector.read_list();
                let candidates: Vec<_> = reads.into_iter().filter(|i| i.is_alive()).collect();
                candidates
                    .first()
                    .cloned()
                    .or_else(|| selector.pick_write())
            } else {
                selector.pick_read().or_else(|| selector.pick_write())
            };
            return inst.map(|i| i.name.clone());
        }
        // 非集群名：若为已注册数据源直接返回（Java 行为一致）
        if self.instances.read().contains_key(target) {
            return Some(target.to_string());
        }
        None
    }

    pub fn instance(&self, name: &str) -> Option<Arc<PhysicsInstance>> {
        self.instances.read().get(name).cloned()
    }

    pub fn selector(&self, cluster: &str) -> Option<Arc<ReplicaSelector>> {
        self.selectors.read().get(cluster).cloned()
    }

    pub fn selectors(&self) -> Vec<Arc<ReplicaSelector>> {
        self.selectors.read().values().cloned().collect()
    }

    /// 找到数据源所属的全部集群（一个数据源可能出现在多个集群）。
    pub fn clusters_of(&self, datasource: &str) -> Vec<Arc<ReplicaSelector>> {
        self.selectors
            .read()
            .values()
            .filter(|s| {
                s.write_list().iter().any(|i| i.name == datasource)
                    || s.read_list().iter().any(|i| i.name == datasource)
            })
            .cloned()
            .collect()
    }

    pub fn datasource_config(&self, name: &str) -> Option<DatasourceConfig> {
        self.datasources.read().get(name).cloned()
    }

    pub fn all_datasource_names(&self) -> Vec<String> {
        self.datasources.read().keys().cloned().collect()
    }

    /// 实例状态总览（监控用）。
    pub fn status_snapshot(&self) -> Vec<(String, bool, bool, usize)> {
        self.instances
            .read()
            .iter()
            .map(|(n, i)| (n.clone(), i.is_alive(), i.as_select_read(), i.active()))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mycat3_config::{ClusterConfig, HeartbeatConfig};

    fn cfg_2ds() -> MycatRouterConfig {
        let mut cfg = MycatRouterConfig::new();
        for (name, url) in [
            ("ds0", "jdbc:mysql://127.0.0.1:3306/db"),
            ("ds1", "jdbc:mysql://127.0.0.1:3307/db"),
        ] {
            cfg.datasources.insert(
                name.into(),
                DatasourceConfig {
                    name: name.into(),
                    url: url.into(),
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
                masters: vec!["ds0".into()],
                replicas: vec!["ds1".into()],
                heartbeat: HeartbeatConfig {
                    min_switch_time_interval: 0,
                    ..Default::default()
                },
                max_con: 10,
            },
        );
        cfg
    }

    #[test]
    fn test_resolve_write_and_read() {
        let rt = ReplicaSelectorRuntime::build(&cfg_2ds());
        // 写 → 主库 ds0
        assert_eq!(rt.resolve("c0", true, false, false).as_deref(), Some("ds0"));
        // 读（BALANCE_ALL）→ ds0/ds1 之一
        for _ in 0..50 {
            let r = rt.resolve("c0", false, false, false).unwrap();
            assert!(r == "ds0" || r == "ds1");
        }
        // 强制主库 hint
        assert_eq!(rt.resolve("c0", false, true, false).as_deref(), Some("ds0"));
        // 强制从库 hint
        assert_eq!(rt.resolve("c0", false, false, true).as_deref(), Some("ds1"));
        // 数据源名直通
        assert_eq!(
            rt.resolve("ds1", false, false, false).as_deref(),
            Some("ds1")
        );
        // 未知 target
        assert_eq!(rt.resolve("nonexistent", false, false, false), None);
    }

    #[test]
    fn test_master_failover_switch() {
        let mut cfg = cfg_2ds();
        // 两个主库便于验证切换重排
        cfg.clusters.get_mut("c0").unwrap().masters = vec!["ds0".into(), "ds1".into()];
        cfg.clusters.get_mut("c0").unwrap().replicas = vec![];
        let rt = ReplicaSelectorRuntime::build(&cfg);
        let sel = rt.selector("c0").unwrap();
        assert_eq!(sel.pick_write().unwrap().name, "ds0");
        // ds0 死亡 → 写立即落到 ds1（pick_write 找第一个存活者）
        rt.instance("ds0").unwrap().set_alive(false);
        assert_eq!(sel.pick_write().unwrap().name, "ds1");
        // try_switch 将死亡者移到尾部
        assert!(sel.try_switch());
        assert_eq!(sel.write_list()[0].name, "ds1");
        // 头存活后再调用不切换
        assert!(!sel.try_switch());
        // ds0 恢复存活但排在尾部，不影响当前写
        rt.instance("ds0").unwrap().set_alive(true);
        assert_eq!(sel.pick_write().unwrap().name, "ds1");
    }

    #[test]
    fn test_not_switch_type() {
        let mut cfg = cfg_2ds();
        cfg.clusters.get_mut("c0").unwrap().switch_type = ReplicaSwitchType::NotSwitch;
        cfg.clusters.get_mut("c0").unwrap().masters = vec!["ds0".into(), "ds1".into()];
        let rt = ReplicaSelectorRuntime::build(&cfg);
        let sel = rt.selector("c0").unwrap();
        rt.instance("ds0").unwrap().set_alive(false);
        assert!(!sel.try_switch());
        assert_eq!(sel.write_list()[0].name, "ds0"); // 顺序不变
    }

    #[test]
    fn test_switch_interval_throttle() {
        let mut cfg = cfg_2ds();
        cfg.clusters.get_mut("c0").unwrap().masters = vec!["ds0".into(), "ds1".into()];
        cfg.clusters
            .get_mut("c0")
            .unwrap()
            .heartbeat
            .min_switch_time_interval = 300;
        let rt = ReplicaSelectorRuntime::build(&cfg);
        let sel = rt.selector("c0").unwrap();
        rt.instance("ds0").unwrap().set_alive(false);
        assert!(sel.try_switch()); // 第一次允许
                                   // ds1 也死、ds0 复活 → 想切回但被 300s 节流
        rt.instance("ds1").unwrap().set_alive(false);
        rt.instance("ds0").unwrap().set_alive(true);
        assert!(!sel.try_switch());
    }

    #[test]
    fn test_balance_none_reads_master_only() {
        let mut cfg = cfg_2ds();
        cfg.clusters.get_mut("c0").unwrap().read_balance_type = BalanceType::None;
        let rt = ReplicaSelectorRuntime::build(&cfg);
        for _ in 0..20 {
            assert_eq!(
                rt.resolve("c0", false, false, false).as_deref(),
                Some("ds0")
            );
        }
    }

    #[test]
    fn test_all_read_fallback_to_master() {
        let mut cfg = cfg_2ds();
        cfg.clusters.get_mut("c0").unwrap().read_balance_type = BalanceType::AllRead;
        let rt = ReplicaSelectorRuntime::build(&cfg);
        // 从库存活 → 读从库
        assert_eq!(
            rt.resolve("c0", false, false, false).as_deref(),
            Some("ds1")
        );
        // 从库死亡 → 回退主库
        rt.instance("ds1").unwrap().set_alive(false);
        assert_eq!(
            rt.resolve("c0", false, false, false).as_deref(),
            Some("ds0")
        );
    }

    #[test]
    fn test_slave_lag_removed_from_read() {
        let cfg = cfg_2ds();
        let rt = ReplicaSelectorRuntime::build(&cfg);
        // 从库延迟（select_as_read=false）→ 读只剩主库
        rt.instance("ds1").unwrap().set_select_read(false);
        for _ in 0..20 {
            assert_eq!(
                rt.resolve("c0", false, false, false).as_deref(),
                Some("ds0")
            );
        }
    }

    #[test]
    fn test_clusters_of() {
        let rt = ReplicaSelectorRuntime::build(&cfg_2ds());
        let cs = rt.clusters_of("ds1");
        assert_eq!(cs.len(), 1);
        assert_eq!(cs[0].name, "c0");
    }
}
