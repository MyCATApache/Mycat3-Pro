//! 路由函数抽象。
//!
//! - [`RangeVariable`] 对应 `io.mycat.RangeVariable`（分片列上的 EQUAL/RANGE 条件）
//! - [`RuleFunction`] 对应 `io.mycat.router.CustomRuleFunction`
//! - [`SingleValueFunction`] 对应 `io.mycat.router.Mycat1xSingleValueRuleFunction`
//!   的通用 calculate 流程：按列名匹配条件 → 计算下标 → 映射到 dataNodes；
//!   无匹配条件或无法精确路由时返回全部节点（全表扫描）。

use std::collections::BTreeMap;
use std::sync::Arc;

use thiserror::Error;

use crate::node::IndexDataNode;

#[derive(Debug, Error)]
pub enum RouterError {
    #[error("invalid sharding function config: {0}")]
    InvalidConfig(String),
    #[error("sharding value error: {0}")]
    InvalidValue(String),
    #[error("sharding index out of bound: index={index}, nodes={nodes}")]
    IndexOutOfBound { index: i64, nodes: usize },
}

pub type RouterResult<T> = Result<T, RouterError>;

/// 条件运算符。对应 `RangeVariable.RangeType`。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Operator {
    Equal,
    Range,
}

/// SQL 中提取出的分片列条件。对应 `io.mycat.RangeVariable`。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RangeVariable {
    pub column_name: String,
    pub operator: Operator,
    /// EQUAL 时为等值；RANGE 时为下界（可空 = -∞）
    pub begin: Option<String>,
    /// RANGE 上界（可空 = +∞）
    pub end: Option<String>,
}

impl RangeVariable {
    pub fn equal(column: impl Into<String>, value: impl Into<String>) -> Self {
        RangeVariable {
            column_name: column.into(),
            operator: Operator::Equal,
            begin: Some(value.into()),
            end: None,
        }
    }
    pub fn range(column: impl Into<String>, begin: Option<String>, end: Option<String>) -> Self {
        RangeVariable {
            column_name: column.into(),
            operator: Operator::Range,
            begin,
            end,
        }
    }
}

/// 分片路由函数。对应 `CustomRuleFunction`。
pub trait RuleFunction: Send + Sync {
    /// 函数名（如 "PartitionByMod"）
    fn name(&self) -> &str;

    /// 该函数关注的分片列（用于 SQL 条件提取）
    fn sharding_columns(&self) -> Vec<String>;

    /// 根据条件计算命中的物理节点集合。
    /// 返回全部节点表示无法裁剪（全表扫描）。
    fn calculate(
        &self,
        values: &BTreeMap<String, RangeVariable>,
    ) -> RouterResult<Vec<IndexDataNode>>;
}

/// 单列单值函数的算法部分。对应 `Mycat1xSingleValueRuleFunction` 的两个抽象方法。
pub trait SingleValueAlgorithm: Send + Sync {
    fn name(&self) -> &str;
    fn column_name(&self) -> &str;
    /// 计算分片下标；-1 表示无法确定（回退全表扫描）
    fn calculate_index(&self, column_value: &str) -> RouterResult<i64>;
    /// 范围下标集合；None 表示无法裁剪
    fn calculate_index_range(&self, _begin: &str, _end: &str) -> Option<Vec<i64>> {
        None
    }
}

/// 通用单值路由函数包装。移植 `Mycat1xSingleValueRuleFunction.calculate` 的完整流程。
pub struct SingleValueFunction {
    algo: Box<dyn SingleValueAlgorithm>,
    data_nodes: Arc<Vec<IndexDataNode>>,
}

impl SingleValueFunction {
    pub fn new(algo: Box<dyn SingleValueAlgorithm>, data_nodes: Arc<Vec<IndexDataNode>>) -> Self {
        SingleValueFunction { algo, data_nodes }
    }

    fn node_at(&self, i: i64) -> RouterResult<IndexDataNode> {
        let size = self.data_nodes.len();
        if i >= 0 && (i as usize) < size {
            Ok(self.data_nodes[i as usize].clone())
        } else {
            Err(RouterError::IndexOutOfBound {
                index: i,
                nodes: size,
            })
        }
    }
}

impl RuleFunction for SingleValueFunction {
    fn name(&self) -> &str {
        self.algo.name()
    }

    fn sharding_columns(&self) -> Vec<String> {
        vec![self.algo.column_name().to_string()]
    }

    fn calculate(
        &self,
        values: &BTreeMap<String, RangeVariable>,
    ) -> RouterResult<Vec<IndexDataNode>> {
        let column = self.algo.column_name();
        let mut res: Vec<IndexDataNode> = Vec::new();
        let mut matched = false;
        for var in values.values() {
            if !var.column_name.eq_ignore_ascii_case(column) {
                continue;
            }
            matched = true;
            match var.operator {
                Operator::Equal => {
                    let begin = var.begin.clone().unwrap_or_default();
                    let idx = self.algo.calculate_index(&begin)?;
                    if idx == -1 {
                        // 无法精确路由 → 全表扫描（Java: return getTable().dataNodes()）
                        return Ok(self.data_nodes.as_ref().clone());
                    }
                    let node = self.node_at(idx)?;
                    if !res.contains(&node) {
                        res.push(node);
                    }
                }
                Operator::Range => {
                    let begin = var.begin.clone().unwrap_or_default();
                    let end = var.end.clone().unwrap_or_default();
                    match self.algo.calculate_index_range(&begin, &end) {
                        None => return Ok(self.data_nodes.as_ref().clone()),
                        Some(idxs) if idxs.is_empty() => {
                            return Ok(self.data_nodes.as_ref().clone())
                        }
                        Some(idxs) => {
                            for idx in idxs {
                                if idx == -1 {
                                    continue;
                                }
                                let node = self.node_at(idx)?;
                                if !res.contains(&node) {
                                    res.push(node);
                                }
                            }
                        }
                    }
                }
            }
        }
        if !matched || res.is_empty() {
            return Ok(self.data_nodes.as_ref().clone());
        }
        Ok(res)
    }
}

// ---------------------------------------------------------------------------
// PartitionUtil（1024 槽段表）
// ---------------------------------------------------------------------------

/// 数据分区工具。精确移植 `io.mycat.router.util.PartitionUtil`：
/// 1024 个槽位按 `count[i] × length[i]` 分段，`Σ(count·length) = 1024`。
pub struct PartitionUtil {
    segment: Vec<i32>,
    partition_num: usize,
}

pub const PARTITION_LENGTH: usize = 1024;

impl PartitionUtil {
    pub fn new(count: &[i32], length: &[i32]) -> RouterResult<Self> {
        if count.len() != length.len() || count.is_empty() {
            return Err(RouterError::InvalidConfig(
                "error, check your scope & scopeLength definition.".into(),
            ));
        }
        let partition_num: usize = count.iter().map(|c| *c as usize).sum();
        let mut ai = vec![0i32; partition_num + 1];
        let mut index = 0usize;
        for i in 0..count.len() {
            for _ in 0..count[i] {
                index += 1;
                ai[index] = ai[index - 1] + length[i];
            }
        }
        if ai[ai.len() - 1] as usize != PARTITION_LENGTH {
            return Err(RouterError::InvalidConfig(
                "error, check your partitionScope definition.".into(),
            ));
        }
        let mut segment = vec![0i32; PARTITION_LENGTH];
        for i in 1..ai.len() {
            for j in ai[i - 1]..ai[i] {
                segment[j as usize] = (i - 1) as i32;
            }
        }
        Ok(PartitionUtil {
            segment,
            partition_num,
        })
    }

    pub fn partition(&self, hash: i64) -> i32 {
        self.segment[((hash as u64) & (PARTITION_LENGTH as u64 - 1)) as usize]
    }

    pub fn partition_num(&self) -> usize {
        self.partition_num
    }

    pub fn all(&self) -> Vec<i64> {
        (0..self.partition_num as i64).collect()
    }
}

// ---------------------------------------------------------------------------
// 范围解析工具
// ---------------------------------------------------------------------------

/// `NodeIndexRange`：值区间 → 节点下标。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeIndexRange {
    pub node_index: i64,
    pub value_start: i64,
    pub value_end: i64,
}

/// 解析 ranges 表：key 为 `"start-end"`，value 为节点下标。按 start 排序。
pub fn parse_node_ranges(ranges: &BTreeMap<String, String>) -> RouterResult<Vec<NodeIndexRange>> {
    let mut out = Vec::new();
    for (k, v) in ranges {
        let (a, b) = k
            .split_once('-')
            .ok_or_else(|| RouterError::InvalidConfig(format!("bad range key: {}", k)))?;
        out.push(NodeIndexRange {
            node_index: v
                .trim()
                .parse()
                .map_err(|_| RouterError::InvalidConfig(format!("bad node index: {}", v)))?,
            value_start: a
                .trim()
                .parse()
                .map_err(|_| RouterError::InvalidConfig(format!("bad range start: {}", a)))?,
            value_end: b
                .trim()
                .parse()
                .map_err(|_| RouterError::InvalidConfig(format!("bad range end: {}", b)))?,
        });
    }
    out.sort_by_key(|r| r.value_start);
    Ok(out)
}

/// `GroupSizeRange`：值区间 → 组大小（PartitionByRangeMod 用）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupSizeRange {
    pub group_size: i64,
    pub value_start: i64,
    pub value_end: i64,
}

pub fn parse_group_ranges(ranges: &BTreeMap<String, String>) -> RouterResult<Vec<GroupSizeRange>> {
    let mut out = Vec::new();
    for (k, v) in ranges {
        let (a, b) = k
            .split_once('-')
            .ok_or_else(|| RouterError::InvalidConfig(format!("bad range key: {}", k)))?;
        out.push(GroupSizeRange {
            group_size: v
                .trim()
                .parse()
                .map_err(|_| RouterError::InvalidConfig(format!("bad group size: {}", v)))?,
            value_start: a
                .trim()
                .parse()
                .map_err(|_| RouterError::InvalidConfig(format!("bad range start: {}", a)))?,
            value_end: b
                .trim()
                .parse()
                .map_err(|_| RouterError::InvalidConfig(format!("bad range end: {}", b)))?,
        });
    }
    out.sort_by_key(|r| r.value_start);
    Ok(out)
}

/// 解析 csv 整数组，如 "2,4" → [2,4]。对应 `toIntArray`。
pub fn to_int_array(s: &str) -> RouterResult<Vec<i32>> {
    s.split(',')
        .map(|x| {
            x.trim()
                .parse::<i32>()
                .map_err(|_| RouterError::InvalidConfig(format!("bad int in csv: {}", s)))
        })
        .collect()
}

/// 读取属性（必填）。
pub fn require_prop<'a>(props: &'a BTreeMap<String, String>, key: &str) -> RouterResult<&'a str> {
    props
        .get(key)
        .map(|s| s.as_str())
        .ok_or_else(|| RouterError::InvalidConfig(format!("missing property: {}", key)))
}

/// 读取属性（带默认）。
pub fn prop_or<'a>(props: &'a BTreeMap<String, String>, key: &str, default: &'a str) -> &'a str {
    props
        .get(key)
        .map(|s| s.as_str())
        .filter(|s| !s.is_empty())
        .unwrap_or(default)
}

/// 大整数取模（支持超出 i64 的数字字符串），对应 `BigInteger(value).abs().mod(count)`。
pub fn big_abs_mod(value: &str, count: i64) -> RouterResult<i64> {
    let trimmed = value.trim().trim_start_matches('+');
    let (negative, digits) = match trimmed.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, trimmed),
    };
    if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
        // 尝试浮点形式（Java BigInteger 会抛错，这里保持一致行为）
        return Err(RouterError::InvalidValue(format!(
            "columnValue:{} Please eliminate any quote and non number within it.",
            value
        )));
    }
    let mut m: i64 = 0;
    let count = count.abs();
    for b in digits.bytes() {
        m = (m * 10 + (b - b'0') as i64) % count;
    }
    let _ = negative; // abs() 后取模，符号无影响
    Ok(m)
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

    struct ModAlgo {
        count: i64,
    }
    impl SingleValueAlgorithm for ModAlgo {
        fn name(&self) -> &str {
            "mod"
        }
        fn column_name(&self) -> &str {
            "id"
        }
        fn calculate_index(&self, v: &str) -> RouterResult<i64> {
            big_abs_mod(v, self.count)
        }
    }

    #[test]
    fn test_single_value_equal_route() {
        let f = SingleValueFunction::new(Box::new(ModAlgo { count: 4 }), nodes(4));
        let mut values = BTreeMap::new();
        values.insert("id".to_string(), RangeVariable::equal("id", "5"));
        let res = f.calculate(&values).unwrap();
        assert_eq!(res.len(), 1);
        assert_eq!(res[0].table_name, "t1"); // 5 % 4 = 1
    }

    #[test]
    fn test_single_value_no_condition_full_scan() {
        let f = SingleValueFunction::new(Box::new(ModAlgo { count: 4 }), nodes(4));
        let res = f.calculate(&BTreeMap::new()).unwrap();
        assert_eq!(res.len(), 4);
    }

    #[test]
    fn test_single_value_case_insensitive_column() {
        let f = SingleValueFunction::new(Box::new(ModAlgo { count: 4 }), nodes(4));
        let mut values = BTreeMap::new();
        values.insert("ID".to_string(), RangeVariable::equal("ID", "8"));
        let res = f.calculate(&values).unwrap();
        assert_eq!(res.len(), 1);
        assert_eq!(res[0].table_name, "t0"); // 8 % 4 = 0
    }

    #[test]
    fn test_single_value_out_of_bound() {
        let f = SingleValueFunction::new(Box::new(ModAlgo { count: 8 }), nodes(4));
        let mut values = BTreeMap::new();
        values.insert("id".to_string(), RangeVariable::equal("id", "6"));
        assert!(matches!(
            f.calculate(&values),
            Err(RouterError::IndexOutOfBound { .. })
        ));
    }

    #[test]
    fn test_partition_util() {
        // 2 分区各 512 槽
        let p = PartitionUtil::new(&[1, 1], &[512, 512]).unwrap();
        assert_eq!(p.partition_num(), 2);
        assert_eq!(p.partition(0), 0);
        assert_eq!(p.partition(511), 0);
        assert_eq!(p.partition(512), 1);
        assert_eq!(p.partition(1023), 1);
        // count=[2,4] length=[128,192]：2*128+4*192=1024 → 合法
        assert!(PartitionUtil::new(&[2, 4], &[128, 192]).is_ok());
        // 点积不为 1024 → 报错
        assert!(PartitionUtil::new(&[2, 4], &[100, 100]).is_err());
        // count=[2,4] length=[256,128] → 2*256+4*128=1024
        let p = PartitionUtil::new(&[2, 4], &[256, 128]).unwrap();
        assert_eq!(p.partition_num(), 6);
        assert_eq!(p.partition(0), 0);
        assert_eq!(p.partition(255), 0);
        assert_eq!(p.partition(256), 1);
        assert_eq!(p.partition(512), 2);
        assert_eq!(p.partition(1023), 5);
    }

    #[test]
    fn test_big_abs_mod() {
        assert_eq!(big_abs_mod("123456789012345678901234567890", 8).unwrap(), 2);
        assert_eq!(big_abs_mod("-5", 4).unwrap(), 1);
        assert_eq!(big_abs_mod("7", 4).unwrap(), 3);
        assert!(big_abs_mod("abc", 4).is_err());
    }

    #[test]
    fn test_parse_ranges() {
        let mut ranges = BTreeMap::new();
        ranges.insert("100-199".to_string(), "1".to_string());
        ranges.insert("0-99".to_string(), "0".to_string());
        let r = parse_node_ranges(&ranges).unwrap();
        assert_eq!(r[0].value_start, 0);
        assert_eq!(r[1].node_index, 1);
    }
}
