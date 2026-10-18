//! 负载均衡策略。移植自 Mycat3 `plug` 模块 `io.mycat.plug.loadBalance.*`：
//! BalanceRandom / BalanceRoundRobin（权重）/ BalanceLeastActive /
//! BalanceRunOnMaster / BalanceRunOnReplica / BalanceRunOnRandomMaster。

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use parking_lot::Mutex;
use rand::seq::SliceRandom;

use crate::PhysicsInstance;

/// 负载均衡策略。对应 `LoadBalanceStrategy` 接口。
pub trait LoadBalance: Send + Sync {
    fn name(&self) -> &str;
    /// 从候选实例中选择一个；候选为空返回 None。
    fn select(&self, candidates: &[Arc<PhysicsInstance>]) -> Option<Arc<PhysicsInstance>>;
}

/// 随机。对应 `BalanceRandom`。
pub struct BalanceRandom;

impl LoadBalance for BalanceRandom {
    fn name(&self) -> &str {
        "BalanceRandom"
    }
    fn select(&self, candidates: &[Arc<PhysicsInstance>]) -> Option<Arc<PhysicsInstance>> {
        candidates.choose(&mut rand::thread_rng()).cloned()
    }
}

/// 权重轮询。对应 `BalanceRoundRobin`（nginx 风格平滑加权轮询，
/// 修复了 Java 版内层循环 `for (index = 0; i < length; i++)` 的下标 bug）。
pub struct BalanceRoundRobin {
    state: Mutex<Vec<(String, i64)>>, // (实例名, 当前权重)
}

impl BalanceRoundRobin {
    pub fn new() -> Self {
        BalanceRoundRobin {
            state: Mutex::new(Vec::new()),
        }
    }
}

impl Default for BalanceRoundRobin {
    fn default() -> Self {
        Self::new()
    }
}

impl LoadBalance for BalanceRoundRobin {
    fn name(&self) -> &str {
        "BalanceRoundRobin"
    }
    fn select(&self, candidates: &[Arc<PhysicsInstance>]) -> Option<Arc<PhysicsInstance>> {
        if candidates.is_empty() {
            return None;
        }
        // 全零权重退化为普通轮询
        let total: i64 = candidates.iter().map(|c| c.weight.max(0) as i64).sum();
        if total == 0 {
            let seq = SEQ.fetch_add(1, Ordering::Relaxed);
            return Some(candidates[seq % candidates.len()].clone());
        }
        let mut state = self.state.lock();
        // 清理已消失实例、补齐新实例
        state.retain(|(n, _)| candidates.iter().any(|c| &c.name == n));
        for c in candidates {
            if !state.iter().any(|(n, _)| n == &c.name) {
                state.push((c.name.clone(), 0));
            }
        }
        // 平滑加权：每个实例 current += weight，选 current 最大者，再减 total
        let mut best: Option<(usize, i64)> = None;
        for (i, (name, cur)) in state.iter_mut().enumerate() {
            let weight = candidates
                .iter()
                .find(|c| &c.name == name)
                .map(|c| c.weight.max(1) as i64)
                .unwrap_or(1);
            *cur += weight;
            if best.map(|(_, b)| *cur > b).unwrap_or(true) {
                best = Some((i, *cur));
            }
        }
        let (idx, _) = best?;
        let total_effective: i64 = candidates.iter().map(|c| c.weight.max(1) as i64).sum();
        state[idx].1 -= total_effective;
        let name = &state[idx].0;
        candidates.iter().find(|c| &c.name == name).cloned()
    }
}

static SEQ: AtomicUsize = AtomicUsize::new(0);

/// 最少活跃连接。对应 `BalanceLeastActive`（相同最小值随机）。
pub struct BalanceLeastActive;

impl LoadBalance for BalanceLeastActive {
    fn name(&self) -> &str {
        "BalanceLeastActive"
    }
    fn select(&self, candidates: &[Arc<PhysicsInstance>]) -> Option<Arc<PhysicsInstance>> {
        if candidates.is_empty() {
            return None;
        }
        let min = candidates.iter().map(|c| c.active()).min()?;
        let mins: Vec<&Arc<PhysicsInstance>> =
            candidates.iter().filter(|c| c.active() == min).collect();
        mins.choose(&mut rand::thread_rng()).map(|c| (*c).clone())
    }
}

/// 只选主库（第一个 is_master）。对应 `BalanceRunOnMaster`。
pub struct BalanceRunOnMaster;

impl LoadBalance for BalanceRunOnMaster {
    fn name(&self) -> &str {
        "BalanceRunOnMaster"
    }
    fn select(&self, candidates: &[Arc<PhysicsInstance>]) -> Option<Arc<PhysicsInstance>> {
        candidates.iter().find(|c| c.is_master).cloned()
    }
}

/// 只选可读从库。对应 `BalanceRunOnReplica`。
pub struct BalanceRunOnReplica;

impl LoadBalance for BalanceRunOnReplica {
    fn name(&self) -> &str {
        "BalanceRunOnReplica"
    }
    fn select(&self, candidates: &[Arc<PhysicsInstance>]) -> Option<Arc<PhysicsInstance>> {
        candidates
            .iter()
            .find(|c| c.as_select_read() && !c.is_master)
            .cloned()
    }
}

/// 主库中随机。对应 `BalanceRunOnRandomMaster`。
pub struct BalanceRunOnRandomMaster;

impl LoadBalance for BalanceRunOnRandomMaster {
    fn name(&self) -> &str {
        "BalanceRunOnRandomMaster"
    }
    fn select(&self, candidates: &[Arc<PhysicsInstance>]) -> Option<Arc<PhysicsInstance>> {
        let masters: Vec<&Arc<PhysicsInstance>> =
            candidates.iter().filter(|c| c.is_master).collect();
        masters
            .choose(&mut rand::thread_rng())
            .map(|c| (*c).clone())
    }
}

/// 按名称构造策略。对应 `LoadBalanceManager`。
pub fn create_strategy(name: &str) -> Box<dyn LoadBalance> {
    match name {
        "BalanceRoundRobin" => Box::new(BalanceRoundRobin::new()),
        "BalanceLeastActive" => Box::new(BalanceLeastActive),
        "BalanceRunOnMaster" => Box::new(BalanceRunOnMaster),
        "BalanceRunOnReplica" => Box::new(BalanceRunOnReplica),
        "BalanceRunOnRandomMaster" => Box::new(BalanceRunOnRandomMaster),
        // 默认 BalanceRandom（与 Java LoadBalanceManager 回退一致）
        _ => Box::new(BalanceRandom),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mycat3_config::InstanceType;

    fn inst(name: &str, weight: i32, master: bool) -> Arc<PhysicsInstance> {
        PhysicsInstance::new_test(name, weight, master, InstanceType::ReadWrite)
    }

    #[test]
    fn test_random_distribution() {
        let c = vec![inst("a", 0, false), inst("b", 0, false)];
        let lb = BalanceRandom;
        let mut hits = [0, 0];
        for _ in 0..1000 {
            let s = lb.select(&c).unwrap();
            if s.name == "a" {
                hits[0] += 1;
            } else {
                hits[1] += 1;
            }
        }
        assert!((350..650).contains(&hits[0]), "hits={:?}", hits);
    }

    #[test]
    fn test_round_robin_weighted() {
        let c = vec![inst("a", 5, false), inst("b", 1, false)];
        let lb = BalanceRoundRobin::new();
        let mut counts = std::collections::HashMap::new();
        for _ in 0..600 {
            let s = lb.select(&c).unwrap();
            *counts.entry(s.name.clone()).or_insert(0) += 1;
        }
        // 权重 5:1 → 500:100
        assert_eq!(counts["a"], 500);
        assert_eq!(counts["b"], 100);
    }

    #[test]
    fn test_round_robin_equal_weights_cycles() {
        let c = vec![
            inst("a", 1, false),
            inst("b", 1, false),
            inst("c", 1, false),
        ];
        let lb = BalanceRoundRobin::new();
        let names: Vec<String> = (0..6)
            .map(|_| lb.select(&c).unwrap().name.clone())
            .collect();
        // 平滑加权在等权时表现为轮转（顺序可能为 a,b,c 循环）
        assert_eq!(names.iter().filter(|n| *n == "a").count(), 2);
        assert_eq!(names.iter().filter(|n| *n == "b").count(), 2);
        assert_eq!(names.iter().filter(|n| *n == "c").count(), 2);
    }

    #[test]
    fn test_least_active() {
        let a = inst("a", 0, false);
        let b = inst("b", 0, false);
        a.incr_active();
        a.incr_active();
        b.incr_active();
        let lb = BalanceLeastActive;
        for _ in 0..10 {
            assert_eq!(lb.select(&[a.clone(), b.clone()]).unwrap().name, "b");
        }
        b.incr_active();
        // 打平 → 随机，不 panic 且返回其一
        let s = lb.select(&[a.clone(), b.clone()]).unwrap();
        assert!(s.name == "a" || s.name == "b");
    }

    #[test]
    fn test_run_on_master_and_replica() {
        let m = inst("m", 0, true);
        let r = inst("r", 0, false);
        r.set_select_read(true);
        assert_eq!(
            BalanceRunOnMaster
                .select(&[m.clone(), r.clone()])
                .unwrap()
                .name,
            "m"
        );
        assert_eq!(
            BalanceRunOnReplica
                .select(&[m.clone(), r.clone()])
                .unwrap()
                .name,
            "r"
        );
        assert_eq!(
            BalanceRunOnRandomMaster
                .select(&[m.clone(), r.clone()])
                .unwrap()
                .name,
            "m"
        );
    }

    #[test]
    fn test_empty_candidates() {
        let empty: Vec<Arc<PhysicsInstance>> = vec![];
        assert!(BalanceRandom.select(&empty).is_none());
        assert!(BalanceRoundRobin::new().select(&empty).is_none());
        assert!(BalanceLeastActive.select(&empty).is_none());
    }

    #[test]
    fn test_create_strategy_by_name() {
        assert_eq!(
            create_strategy("BalanceRoundRobin").name(),
            "BalanceRoundRobin"
        );
        assert_eq!(create_strategy("unknown").name(), "BalanceRandom");
    }
}
