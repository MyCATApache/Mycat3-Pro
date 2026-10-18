//! 序列生成器：Snowflake 时间序列 + 简单计数序列。
//!
//! 对应 Java 版 `plug.sequence.SequenceGenerator` / `TimeBasedSequence`
//! （imadcn Snowflake，workerId = `server.json.mycatId`）与 `SequenceConfig`。
//! Java 的取号入口：`LogicTable.nextSequence()`（INSERT 自增填充、GLOBAL 表
//! GLOBAL_SEQUENCE 类型），序列名 = 表唯一名 `schema.table`，无配置时默认走
//! time/Snowflake —— Pro 保持相同语义。

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use mycat3_config::{MycatRouterConfig, SequenceConfig};

/// Snowflake：41 位毫秒时间戳（自定义纪元 2020-01-01）+ 10 位 workerId + 12 位序列。
///
/// 无锁 CAS 实现：状态打包为 (last_millis << 12 | seq) 单原子量，全部会话并发取号
/// 无互斥、无 `thread::sleep`（旧实现 Mutex + 回拨 sleep 会阻塞 tokio worker）。
/// 时钟回拨时沿用上一毫秒继续递增 seq（与旧实现大幅回拨语义一致，保证单调性）。
pub struct Snowflake {
    worker_id: u64,
    state: AtomicI64, // (last_millis_abs << SEQ_BITS) | seq
}

/// 自定义纪元：2020-01-01T00:00:00Z（毫秒）
const EPOCH_MILLIS: i64 = 1_577_836_800_000;
const WORKER_BITS: u64 = 10;
const SEQ_BITS: u64 = 12;
const MAX_WORKER: u64 = (1 << WORKER_BITS) - 1;
const MAX_SEQ: u64 = (1 << SEQ_BITS) - 1;

impl Snowflake {
    pub fn new(worker_id: u64) -> Self {
        Snowflake {
            worker_id: worker_id & MAX_WORKER,
            state: AtomicI64::new(0),
        }
    }

    fn now_millis() -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0)
    }

    fn compose(&self, millis: i64, seq: u64) -> i64 {
        ((millis - EPOCH_MILLIS) << (WORKER_BITS + SEQ_BITS))
            | ((self.worker_id as i64) << SEQ_BITS)
            | (seq as i64)
    }

    pub fn next_id(&self) -> i64 {
        loop {
            let cur = self.state.load(Ordering::Relaxed);
            let last_millis = cur >> SEQ_BITS;
            let seq = (cur & MAX_SEQ as i64) as u64;
            let now = Self::now_millis();
            if now > last_millis {
                // 新毫秒：抢占 (now, seq=0)
                let new = now << SEQ_BITS;
                if self
                    .state
                    .compare_exchange_weak(cur, new, Ordering::Relaxed, Ordering::Relaxed)
                    .is_ok()
                {
                    return self.compose(now, 0);
                }
            } else {
                // 同一毫秒，或时钟回拨（沿用 last_millis 保证单调递增）
                let next_seq = seq + 1;
                if next_seq > MAX_SEQ {
                    // 本毫秒序列耗尽：推进到 last_millis+1（不等待真实时钟）
                    let new = (last_millis + 1) << SEQ_BITS;
                    if self
                        .state
                        .compare_exchange_weak(cur, new, Ordering::Relaxed, Ordering::Relaxed)
                        .is_ok()
                    {
                        return self.compose(last_millis + 1, 0);
                    }
                    continue;
                }
                let new = (last_millis << SEQ_BITS) | next_seq as i64;
                if self
                    .state
                    .compare_exchange_weak(cur, new, Ordering::Relaxed, Ordering::Relaxed)
                    .is_ok()
                {
                    return self.compose(last_millis, next_seq);
                }
            }
        }
    }
}

/// 简单内存计数序列（SequenceConfig time=false 时按 startWith/incrementBy 递增；
/// 非持久化，重启回到 startWith —— Pro 简化，Java 的 group/表序列未移植）。
struct CounterSequence {
    current: AtomicI64,
    increment_by: i64,
}

/// 序列生成器。对应 `SequenceGenerator`。
pub struct SequenceGenerator {
    snowflake: Snowflake,
    counters: BTreeMap<String, CounterSequence>,
}

impl SequenceGenerator {
    pub fn new(cfg: &MycatRouterConfig, mycat_id: u32) -> Self {
        let mut counters = BTreeMap::new();
        for (name, sc) in &cfg.sequences {
            // Java: config.time==true 或 clazz 含 Snowflake → 时间序列；否则计数序列
            let is_time =
                sc.clazz.is_empty() || sc.clazz.contains("Snowflake") || sc.clazz.contains("Time");
            if !is_time {
                counters.insert(
                    name.to_lowercase(),
                    CounterSequence {
                        current: AtomicI64::new(sc.start_with as i64),
                        increment_by: if sc.increment_by == 0 {
                            1
                        } else {
                            sc.increment_by as i64
                        },
                    },
                );
            }
        }
        SequenceGenerator {
            snowflake: Snowflake::new(mycat_id as u64),
            counters,
        }
    }

    /// 取下一个 ID。`unique_name` = `schema.table`（对应 LogicTable.getUniqueName）；
    /// 无匹配配置时默认 Snowflake（与 Java 相同）。
    pub fn next_id(&self, unique_name: &str) -> i64 {
        if let Some(c) = self.counters.get(&unique_name.to_lowercase()) {
            // 先返回当前值再递增（start_with 语义）
            return c.current.fetch_add(c.increment_by, Ordering::Relaxed);
        }
        self.snowflake.next_id()
    }

    /// 直接取 Snowflake ID（未注册表的场景）。
    pub fn next_snowflake(&self) -> i64 {
        self.snowflake.next_id()
    }
}

/// 解析 `CREATE SEQUENCE` 语句为配置（对应 Java CreateSequenceHandler 接受的子集）。
/// 语法：CREATE SEQUENCE [IF NOT EXISTS] name [INCREMENT BY n] [START WITH n]
///       [MINVALUE n | NO MINVALUE] [MAXVALUE n | NO MAXVALUE] [CYCLE] [TIME | SIMPLE | GROUP]
pub fn parse_create_sequence(sql: &str) -> Option<SequenceConfig> {
    let norm = sql.trim().trim_end_matches(';');
    let upper = norm.to_uppercase();
    if !upper.starts_with("CREATE SEQUENCE") {
        return None;
    }
    let mut tokens = norm.split_whitespace().peekable();
    // 跳过 CREATE SEQUENCE [IF NOT EXISTS]
    let _ = tokens.next();
    let _ = tokens.next();
    if tokens
        .peek()
        .map(|t| t.eq_ignore_ascii_case("IF"))
        .unwrap_or(false)
    {
        let _ = tokens.next();
        let _ = tokens.next(); // NOT
        let _ = tokens.next(); // EXISTS
    }
    let name = tokens.next()?.trim_matches('`').to_lowercase();
    let mut cfg = SequenceConfig {
        name: name.clone(),
        clazz: String::new(),
        target_name: "prototype".into(),
        schema_name: None,
        step: 1,
        start_with: 1,
        increment_by: 1,
    };
    let mut is_time = true;
    while let Some(tok) = tokens.next() {
        let up = tok.to_uppercase();
        match up.as_str() {
            "INCREMENT" => {
                let _ = tokens.next(); // BY
                if let Some(v) = tokens.next() {
                    cfg.increment_by = v.parse().unwrap_or(1);
                }
            }
            "START" => {
                let _ = tokens.next(); // WITH
                if let Some(v) = tokens.next() {
                    cfg.start_with = v.parse().unwrap_or(1);
                }
            }
            "STEP" => {
                if let Some(v) = tokens.next() {
                    cfg.step = v.parse().unwrap_or(1);
                }
            }
            "TIME" => is_time = true,
            "SIMPLE" | "GROUP" => is_time = false,
            _ => {}
        }
    }
    if !is_time {
        cfg.clazz = "simple".into();
    }
    Some(cfg)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_snowflake_unique_and_monotonic() {
        let sf = Snowflake::new(1);
        let mut prev = 0;
        let mut seen = std::collections::HashSet::new();
        for _ in 0..20000 {
            let id = sf.next_id();
            assert!(id > prev, "not monotonic: {} <= {}", id, prev);
            assert!(seen.insert(id), "duplicate id {}", id);
            prev = id;
        }
    }

    #[test]
    fn test_snowflake_worker_isolation() {
        let a = Snowflake::new(1);
        let b = Snowflake::new(2);
        // 同一毫秒内不同 worker 不冲突
        let ia = a.next_id();
        let ib = b.next_id();
        assert_ne!(ia, ib);
    }

    #[test]
    fn test_generator_default_snowflake() {
        let cfg = MycatRouterConfig::new();
        let g = SequenceGenerator::new(&cfg, 1);
        let v = g.next_id("db1.unknown_table");
        assert!(v > 0);
        assert_ne!(v, g.next_id("db1.unknown_table"));
    }

    #[test]
    fn test_generator_counter_sequence() {
        let mut cfg = MycatRouterConfig::new();
        cfg.sequences.insert(
            "db1.orders".into(),
            SequenceConfig {
                name: "db1.orders".into(),
                clazz: "simple".into(),
                target_name: "prototype".into(),
                schema_name: None,
                step: 1,
                start_with: 1000,
                increment_by: 10,
            },
        );
        let g = SequenceGenerator::new(&cfg, 1);
        assert_eq!(g.next_id("db1.orders"), 1000);
        assert_eq!(g.next_id("db1.orders"), 1010);
        assert_eq!(g.next_id("DB1.ORDERS"), 1020); // 大小写不敏感
    }

    #[test]
    fn test_parse_create_sequence() {
        let c =
            parse_create_sequence("CREATE SEQUENCE myseq INCREMENT BY 5 START WITH 100").unwrap();
        assert_eq!(c.name, "myseq");
        assert_eq!(c.increment_by, 5);
        assert_eq!(c.start_with, 100);
        assert!(c.clazz.is_empty()); // 默认 time/snowflake

        let c = parse_create_sequence("create sequence if not exists `s2` simple start with 7")
            .unwrap();
        assert_eq!(c.name, "s2");
        assert_eq!(c.clazz, "simple");
        assert_eq!(c.start_with, 7);

        assert!(parse_create_sequence("select 1").is_none());
    }
}
