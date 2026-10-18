//! Mycat3 Pro 路由模块：分片算法与物理节点模型。
//!
//! 移植自 Mycat3 `router` 模块（`io.mycat.router.*`）：
//! - [`node`]：`IndexDataNode`（= `io.mycat.Partition`）与后端节点展开
//! - [`function`]：`RuleFunction` / `RangeVariable` / `SingleValueFunction` / `PartitionUtil`
//! - [`mycat1x`]：Mycat 1.x 兼容分片函数（Mod/Long/HashMod/MurmurHash/RangeMod/Date/Month/
//!   CRC32PreSlot/JumpConsistentHash/Pattern/FileMap/SubString/String/HotDate 等）
//! - [`auto`]：现代 AutoFunction（dbMethod/tableMethod/mappingFormat）
//! - [`hash`]：Java 兼容哈希（String.hashCode / Wang / Guava murmur3_32 / jump / crc32）
//!
//! [`build_rule_function`] 对应 `PartitionRuleFunctionManager.getRuleAlgorithm`：
//! `clazz` 为 Java 全限定类名（按类名后缀匹配），`name` 为函数别名；
//! 两者都为空时构造 AutoFunction（与 Java 行为一致）。

pub mod auto;
pub mod datefmt;
pub mod function;
pub mod hash;
pub mod mycat1x;
pub mod node;

use std::collections::BTreeMap;
use std::sync::Arc;

pub use auto::{expand_auto_nodes, render_mapping, AutoFunction, MethodSpec};
pub use function::{
    Operator, RangeVariable, RouterError, RouterResult, RuleFunction, SingleValueAlgorithm,
    SingleValueFunction,
};
pub use mycat1x::wrap;
pub use node::{expand_backend_nodes, IndexDataNode};

use mycat3_config::ShardingFunctionConfig;

/// 判断配置是否为 AutoFunction（name 与 clazz 均为空）。
pub fn is_auto_function(cfg: &ShardingFunctionConfig) -> bool {
    cfg.name.as_deref().unwrap_or("").is_empty() && cfg.clazz.as_deref().unwrap_or("").is_empty()
}

/// 依据配置构造分片路由函数。
///
/// `data_nodes` 必须已按 `expand_backend_nodes`（legacy）或 `expand_auto_nodes`（auto）展开。
pub fn build_rule_function(
    cfg: &ShardingFunctionConfig,
    data_nodes: Arc<Vec<IndexDataNode>>,
) -> RouterResult<Box<dyn RuleFunction>> {
    let props: &BTreeMap<String, String> = &cfg.properties;
    let ranges: &BTreeMap<String, String> = &cfg.ranges;

    // AutoFunction：name 与 clazz 都为空（Java: AutoFunctionFactory.getTableFunction）
    if is_auto_function(cfg) {
        return Ok(Box::new(AutoFunction::build(props, data_nodes)?));
    }

    // clazz / name → 具体实现（Java 用 Class.forName，这里按类名后缀/别名匹配）
    let key = cfg
        .clazz
        .as_deref()
        .filter(|s| !s.is_empty())
        .or(cfg.name.as_deref())
        .unwrap_or("");
    let short = key.rsplit('.').next().unwrap_or(key);
    let column = props.get("columnName").cloned().unwrap_or_default();

    let algo: Box<dyn SingleValueAlgorithm> = match normalize_alias(short).as_str() {
        "PartitionByMod" => mycat1x::PartitionByMod::algo(&column, props)?,
        "PartitionByLong" => mycat1x::PartitionByLong::algo(&column, props)?,
        "SimplePartitionByLong" => mycat1x::SimplePartitionByLong::algo(&column, props)?,
        "PartitionByHashMod" => mycat1x::PartitionByHashMod::algo(&column, props)?,
        "PartitionByMurmurHash" => mycat1x::PartitionByMurmurHash::algo(&column, props, ranges)?,
        "PartitionByRangeMod" => mycat1x::PartitionByRangeMod::algo(&column, props, ranges)?,
        "AutoPartitionByLong" => mycat1x::AutoPartitionByLong::algo(&column, props, ranges)?,
        "PartitionByPattern" => mycat1x::PartitionByPattern::algo(&column, props, ranges)?,
        "PartitionByPrefixPattern" => mycat1x::PartitionByPrefixPattern::algo(&column, props, ranges)?,
        "PartitionByFileMap" => mycat1x::PartitionByFileMap::algo(&column, props, ranges)?,
        "PartitionDirectBySubString" => mycat1x::PartitionDirectBySubString::algo(&column, props)?,
        "PartitionByString" => mycat1x::PartitionByString::algo(&column, props)?,
        "PartitionByDate" => mycat1x::PartitionByDate::algo(&column, props)?,
        "PartitionByMonth" => mycat1x::PartitionByMonth::algo(&column, props)?,
        "PartitionByCRC32PreSlot" => mycat1x::PartitionByCrc32PreSlot::algo(&column, props, ranges)?,
        "PartitionByJumpConsistentHash" => mycat1x::PartitionByJumpConsistentHash::algo(&column, props)?,
        "PartitionByHotDate" => mycat1x::PartitionByHotDate::algo(&column, props)?,
        other => {
            return Err(RouterError::InvalidConfig(format!(
                "unsupported sharding function: {} (Pro 支持: PartitionByMod, PartitionByLong, \
                 SimplePartitionByLong, PartitionByHashMod, PartitionByMurmurHash, PartitionByRangeMod, \
                 AutoPartitionByLong, PartitionByPattern, PartitionByPrefixPattern, PartitionByFileMap, \
                 PartitionDirectBySubString, PartitionByString, PartitionByDate, PartitionByMonth, \
                 PartitionByCRC32PreSlot, PartitionByJumpConsistentHash, PartitionByHotDate, AutoFunction)",
                other
            )))
        }
    };
    if column.is_empty() {
        return Err(RouterError::InvalidConfig(format!(
            "{} requires property columnName",
            short
        )));
    }
    Ok(Box::new(SingleValueFunction::new(algo, data_nodes)))
}

/// Mycat2/1.x 风格别名 → Pro 内部函数名。
fn normalize_alias(name: &str) -> String {
    match name.to_ascii_lowercase().as_str() {
        "mod" | "partitionbymod" => "PartitionByMod",
        "long" | "partitionbylong" => "PartitionByLong",
        "simplepartitionbylong" | "auto-sharding-long2" => "SimplePartitionByLong",
        "hash-int" | "hashint" | "partitionbyhashmod" => "PartitionByHashMod",
        "murmur" | "partitionbymurmurhash" => "PartitionByMurmurHash",
        "rangemod-hash" | "rangemod" | "partitionbyrangemod" => "PartitionByRangeMod",
        "auto-sharding-long" | "autopartitionbylong" => "AutoPartitionByLong",
        "pattern" | "partitionbypattern" => "PartitionByPattern",
        "prefix-pattern" | "partitionbyprefixpattern" => "PartitionByPrefixPattern",
        "filemap" | "enum" | "partitionbyfilemap" => "PartitionByFileMap",
        "substring" | "partitiondirectbysubstring" => "PartitionDirectBySubString",
        "string" | "partitionbystring" => "PartitionByString",
        "date" | "partitionbydate" => "PartitionByDate",
        "month" | "partitionbymonth" => "PartitionByMonth",
        "crc32slot" | "partitionbycrc32preslot" => "PartitionByCRC32PreSlot",
        "jump-consistent-hash" | "partitionbyjumpconsistenthash" => "PartitionByJumpConsistentHash",
        "hotdate" | "partitionbyhotdate" => "PartitionByHotDate",
        other => return other.to_string(),
    }
    .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn nodes(n: usize) -> Arc<Vec<IndexDataNode>> {
        Arc::new(
            (0..n)
                .map(|i| IndexDataNode::new("c0", "db1", format!("t{}", i), i as i32))
                .collect(),
        )
    }

    fn calc(f: &Box<dyn RuleFunction>, col: &str, v: &str) -> Vec<IndexDataNode> {
        let mut values = BTreeMap::new();
        values.insert(col.to_string(), RangeVariable::equal(col, v));
        f.calculate(&values).unwrap()
    }

    fn cfg(clazz: &str, props: &[(&str, &str)], ranges: &[(&str, &str)]) -> ShardingFunctionConfig {
        ShardingFunctionConfig {
            name: None,
            clazz: Some(clazz.into()),
            properties: props
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
            ranges: ranges
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
        }
    }

    #[test]
    fn test_mod_function() {
        let f = build_rule_function(
            &cfg(
                "io.mycat.router.mycat1xfunction.PartitionByMod",
                &[("columnName", "id"), ("count", "4")],
                &[],
            ),
            nodes(4),
        )
        .unwrap();
        assert_eq!(calc(&f, "id", "0")[0].table_name, "t0");
        assert_eq!(calc(&f, "id", "5")[0].table_name, "t1");
        assert_eq!(calc(&f, "id", "-4")[0].table_name, "t0");
        // 超过 i64 的大数
        assert_eq!(
            calc(&f, "id", "123456789012345678901234567890")[0].table_name,
            "t2"
        );
        // 无分片条件 → 全表扫描
        assert_eq!(f.calculate(&BTreeMap::new()).unwrap().len(), 4);
    }

    #[test]
    fn test_alias_resolution() {
        // Mycat2 风格 name = "mod"
        let c = ShardingFunctionConfig {
            name: Some("mod".into()),
            clazz: None,
            properties: [
                ("columnName".to_string(), "id".to_string()),
                ("count".to_string(), "2".to_string()),
            ]
            .into_iter()
            .collect(),
            ranges: BTreeMap::new(),
        };
        let f = build_rule_function(&c, nodes(2)).unwrap();
        assert_eq!(calc(&f, "id", "3")[0].table_name, "t1");
    }

    #[test]
    fn test_long_function() {
        let f = build_rule_function(
            &cfg(
                "PartitionByLong",
                &[
                    ("columnName", "id"),
                    ("partitionCount", "2,4"),
                    ("partitionLength", "256,128"),
                ],
                &[],
            ),
            nodes(6),
        )
        .unwrap();
        // key=1 → (1>>>32)^1 = 1 → segment[1] = 0
        assert_eq!(calc(&f, "id", "1")[0].table_name, "t0");
        // key=512 → segment[512] = 2
        assert_eq!(calc(&f, "id", "512")[0].table_name, "t2");
    }

    #[test]
    fn test_simple_long_function() {
        let f = build_rule_function(
            &cfg(
                "SimplePartitionByLong",
                &[("columnName", "id"), ("partitionCount", "4")],
                &[],
            ),
            nodes(4),
        )
        .unwrap();
        // 每分区 256 槽：hash=1→0 号，hash=256→1 号
        assert_eq!(calc(&f, "id", "1")[0].table_name, "t0");
        assert_eq!(calc(&f, "id", "256")[0].table_name, "t1");
        assert_eq!(calc(&f, "id", "1023")[0].table_name, "t3");
    }

    #[test]
    fn test_hash_mod_function() {
        let f = build_rule_function(
            &cfg(
                "PartitionByHashMod",
                &[("columnName", "id"), ("count", "4")],
                &[],
            ),
            nodes(4),
        )
        .unwrap();
        // 结果确定性且在界内
        for v in ["abc", "root", "12345", "mycat"] {
            let r = calc(&f, "id", v);
            assert_eq!(r.len(), 1);
        }
        // 与 Java 算法一致性抽查：hash("abc".hashCode()) wang → abs % 4
        let h = hash::wang_hash(hash::java_string_hash("abc"));
        let expect = ((h as i64).abs() & 3) as usize; // count=4 为 2 的幂 → watch=true
        assert_eq!(calc(&f, "id", "abc")[0].table_name, format!("t{}", expect));
    }

    #[test]
    fn test_murmur_function() {
        let f = build_rule_function(
            &cfg(
                "PartitionByMurmurHash",
                &[
                    ("columnName", "id"),
                    ("seed", "0"),
                    ("count", "4"),
                    ("virtualBucketTimes", "10"),
                ],
                &[],
            ),
            nodes(4),
        )
        .unwrap();
        // 所有值都路由到 0..4
        for v in ["a", "bb", "ccc", "123", "mycat3"] {
            let r = calc(&f, "id", v);
            assert_eq!(r.len(), 1);
            assert!(r[0].table_name.starts_with('t'));
        }
        // 分布均衡性：1000 个值应覆盖全部 4 个节点
        let mut hit = [false; 4];
        for i in 0..1000 {
            let r = calc(&f, "id", &format!("key-{}", i));
            let idx: usize = r[0].table_name[1..].parse().unwrap();
            hit[idx] = true;
        }
        assert!(hit.iter().all(|x| *x));
    }

    #[test]
    fn test_rangemod_function() {
        let f = build_rule_function(
            &cfg(
                "PartitionByRangeMod",
                &[("columnName", "id"), ("defaultNode", "4")],
                &[("0-99", "2"), ("100-199", "2")],
            ),
            nodes(5),
        )
        .unwrap();
        // 0-99 组内取模 2：50 → nodeIndex0 + 50%2=0 → t0
        assert_eq!(calc(&f, "id", "50")[0].table_name, "t0");
        // 100-199：offset=2（前一组 groupSize 累计），151 → 2 + 151%2=1 → t3
        assert_eq!(calc(&f, "id", "151")[0].table_name, "t3");
        // 超出范围 → defaultNode=4
        assert_eq!(calc(&f, "id", "500")[0].table_name, "t4");
    }

    #[test]
    fn test_auto_partition_by_long() {
        let f = build_rule_function(
            &cfg(
                "AutoPartitionByLong",
                &[("columnName", "id"), ("defaultNode", "2")],
                &[("0-999", "0"), ("1000-1999", "1")],
            ),
            nodes(3),
        )
        .unwrap();
        assert_eq!(calc(&f, "id", "500")[0].table_name, "t0");
        assert_eq!(calc(&f, "id", "1500")[0].table_name, "t1");
        assert_eq!(calc(&f, "id", "2500")[0].table_name, "t2");
        // RANGE 条件：calculateSequenceRange
        let mut values = BTreeMap::new();
        values.insert(
            "id".to_string(),
            RangeVariable::range("id", Some("500".into()), Some("1500".into())),
        );
        let r = f.calculate(&values).unwrap();
        assert_eq!(r.len(), 2); // 下标 0..=1
    }

    #[test]
    fn test_pattern_function() {
        let f = build_rule_function(
            &cfg(
                "PartitionByPattern",
                &[
                    ("columnName", "id"),
                    ("patternValue", "64"),
                    ("defaultNode", "1"),
                ],
                &[("0-31", "0"), ("32-63", "1")],
            ),
            nodes(2),
        )
        .unwrap();
        assert_eq!(calc(&f, "id", "16")[0].table_name, "t0"); // 16%64=16 ∈ [0,31]
        assert_eq!(calc(&f, "id", "48")[0].table_name, "t1"); // 48 ∈ [32,63]
        assert_eq!(calc(&f, "id", "abc")[0].table_name, "t1"); // 非数字 → default
    }

    #[test]
    fn test_filemap_function() {
        let f = build_rule_function(
            &cfg(
                "PartitionByFileMap",
                &[("columnName", "city"), ("defaultNode", "0")],
                &[("beijing", "1"), ("shanghai", "2")],
            ),
            nodes(3),
        )
        .unwrap();
        assert_eq!(calc(&f, "city", "beijing")[0].table_name, "t1");
        assert_eq!(calc(&f, "city", "shanghai")[0].table_name, "t2");
        assert_eq!(calc(&f, "city", "guangzhou")[0].table_name, "t0");
    }

    #[test]
    fn test_substring_function() {
        let f = build_rule_function(
            &cfg(
                "PartitionDirectBySubString",
                &[
                    ("columnName", "code"),
                    ("startIndex", "0"),
                    ("size", "2"),
                    ("partitionCount", "4"),
                    ("defaultNode", "0"),
                ],
                &[],
            ),
            nodes(4),
        )
        .unwrap();
        assert_eq!(calc(&f, "code", "02abc")[0].table_name, "t2");
        assert_eq!(calc(&f, "code", "12345")[0].table_name, "t0"); // "12" = 12 ≥ partitionCount 4 → defaultNode
    }

    #[test]
    fn test_date_function() {
        let f = build_rule_function(
            &cfg(
                "PartitionByDate",
                &[
                    ("columnName", "created"),
                    ("beginDate", "2021-01-01"),
                    ("endDate", "2021-01-31"),
                    ("partionDay", "10"),
                    ("dateFormat", "yyyy-MM-dd"),
                ],
                &[],
            ),
            nodes(4),
        )
        .unwrap();
        // beginDate day_of_year=1 → begin=1*ONE_DAY；2021-01-01 → (1-1)*ONE_DAY/10d = 0
        assert_eq!(calc(&f, "created", "2021-01-01")[0].table_name, "t0");
        // 2021-01-12 → day 12 → (12-1)/10 = 1
        assert_eq!(calc(&f, "created", "2021-01-12")[0].table_name, "t1");
    }

    #[test]
    fn test_month_function_scene1() {
        // 无 begin/end → 返回 0 基月份
        let f = build_rule_function(
            &cfg(
                "PartitionByMonth",
                &[("columnName", "created"), ("dateFormat", "yyyy-MM-dd")],
                &[],
            ),
            nodes(12),
        )
        .unwrap();
        assert_eq!(calc(&f, "created", "2021-01-15")[0].table_name, "t0");
        assert_eq!(calc(&f, "created", "2021-12-15")[0].table_name, "t11");
    }

    #[test]
    fn test_month_function_cyclic() {
        let f = build_rule_function(
            &cfg(
                "PartitionByMonth",
                &[
                    ("columnName", "created"),
                    ("beginDate", "2021-01-01"),
                    ("endDate", "2021-04-01"),
                    ("dateFormat", "yyyy-MM-dd"),
                ],
                &[],
            ),
            nodes(4),
        )
        .unwrap();
        // nPartition = 4；2021-03 → 2；2021-05 → (4)%4=0
        assert_eq!(calc(&f, "created", "2021-03-10")[0].table_name, "t2");
        assert_eq!(calc(&f, "created", "2021-05-10")[0].table_name, "t0");
    }

    #[test]
    fn test_crc32_preslot() {
        let f = build_rule_function(
            &cfg(
                "PartitionByCRC32PreSlot",
                &[("columnName", "id"), ("count", "4")],
                &[],
            ),
            nodes(4),
        )
        .unwrap();
        // crc32("123456789")=0xCBF43926=3421780262 → %102400 → slot
        let slot = (hash::crc32("123456789") % 102400) as i64;
        let expect = slot / (102400 / 4);
        assert_eq!(
            calc(&f, "id", "123456789")[0].table_name,
            format!("t{}", expect)
        );
        // 稳定性
        assert_eq!(
            calc(&f, "id", "abc")[0].table_name,
            calc(&f, "id", "abc")[0].table_name
        );
    }

    #[test]
    fn test_jump_consistent_hash_function() {
        let f = build_rule_function(
            &cfg(
                "PartitionByJumpConsistentHash",
                &[("columnName", "id"), ("totalBuckets", "4")],
                &[],
            ),
            nodes(4),
        )
        .unwrap();
        for v in ["a", "b", "100", "mycat"] {
            let r = calc(&f, "id", v);
            assert_eq!(r.len(), 1);
            let expected = hash::jump_consistent_hash(hash::java_string_hash(v) as i64, 4);
            assert_eq!(r[0].table_name, format!("t{}", expected));
        }
    }

    #[test]
    fn test_string_function() {
        let f = build_rule_function(
            &cfg(
                "PartitionByString",
                &[
                    ("columnName", "code"),
                    ("partitionCount", "2"),
                    ("partitionLength", "512"),
                    ("hashSlice", "0:4"),
                ],
                &[],
            ),
            nodes(2),
        )
        .unwrap();
        // 取前 4 字符做 31 进制哈希 → 1024 槽
        let h = hash::string_util_hash("abcdef", 0, 4);
        let expect = if (h as u64 & 1023) < 512 { 0 } else { 1 };
        assert_eq!(
            calc(&f, "code", "abcdef")[0].table_name,
            format!("t{}", expect)
        );
    }

    #[test]
    fn test_auto_function_via_registry() {
        let c = ShardingFunctionConfig {
            name: None,
            clazz: None,
            properties: [
                ("dbNum".to_string(), "1".to_string()),
                ("tableNum".to_string(), "4".to_string()),
                ("storeNum".to_string(), "1".to_string()),
                (
                    "mappingFormat".to_string(),
                    "c0/db1/tb_${tableIndex}".to_string(),
                ),
                ("tableMethod".to_string(), "hash(id)".to_string()),
            ]
            .into_iter()
            .collect(),
            ranges: BTreeMap::new(),
        };
        let nodes = Arc::new(expand_auto_nodes(&c.properties).unwrap());
        let f = build_rule_function(&c, nodes).unwrap();
        assert_eq!(f.name(), "AutoFunction");
        assert_eq!(calc(&f, "id", "7")[0].table_name, "tb_3"); // 7%4=3
    }

    #[test]
    fn test_unsupported_function() {
        let c = cfg(
            "io.mycat.router.UnknownFunction",
            &[("columnName", "id")],
            &[],
        );
        assert!(build_rule_function(&c, nodes(1)).is_err());
    }
}
