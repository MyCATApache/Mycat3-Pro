//! Mycat 1.x 兼容分片函数集合。
//!
//! 逐一移植 `io.mycat.router.mycat1xfunction` 下的算法：
//! PartitionByMod / PartitionByLong / SimplePartitionByLong / PartitionByHashMod /
//! PartitionByMurmurHash / PartitionByRangeMod / AutoPartitionByLong / PartitionByPattern /
//! PartitionByPrefixPattern / PartitionByFileMap / PartitionDirectBySubString /
//! PartitionByString / PartitionByDate / PartitionByMonth / PartitionByCRC32PreSlot /
//! PartitionByJumpConsistentHash

use std::collections::BTreeMap;
use std::sync::Arc;

use chrono::Datelike;

use crate::datefmt::{parse_datetime, split_patterns};
use crate::function::{
    big_abs_mod, parse_group_ranges, parse_node_ranges, prop_or, require_prop, to_int_array,
    GroupSizeRange, NodeIndexRange, PartitionUtil, RouterError, RouterResult, SingleValueAlgorithm,
    SingleValueFunction,
};
use crate::hash;
use crate::node::IndexDataNode;

type Props = BTreeMap<String, String>;

const ONE_DAY_MS: i64 = 86_400_000;

// ---------------------------------------------------------------------------
// PartitionByMod：abs(BigInteger(value)) mod count
// ---------------------------------------------------------------------------

pub struct PartitionByMod {
    column_name: String,
    count: i64,
}

impl PartitionByMod {
    pub fn algo(column: &str, props: &Props) -> RouterResult<Box<dyn SingleValueAlgorithm>> {
        let count: i64 = require_prop(props, "count")?
            .parse()
            .map_err(|_| RouterError::InvalidConfig("PartitionByMod: bad count".into()))?;
        Ok(Box::new(PartitionByMod {
            column_name: column.to_string(),
            count,
        }))
    }
}

impl SingleValueAlgorithm for PartitionByMod {
    fn name(&self) -> &str {
        "PartitionByMod"
    }
    fn column_name(&self) -> &str {
        &self.column_name
    }
    fn calculate_index(&self, v: &str) -> RouterResult<i64> {
        big_abs_mod(v, self.count)
    }
}

// ---------------------------------------------------------------------------
// PartitionByLong：1024 槽段表，hash = (v >>> 32) ^ v
// ---------------------------------------------------------------------------

pub struct PartitionByLong {
    column_name: String,
    partition_util: PartitionUtil,
}

impl PartitionByLong {
    pub fn algo(column: &str, props: &Props) -> RouterResult<Box<dyn SingleValueAlgorithm>> {
        let count = to_int_array(require_prop(props, "partitionCount")?)?;
        let length = to_int_array(require_prop(props, "partitionLength")?)?;
        Ok(Box::new(PartitionByLong {
            column_name: column.to_string(),
            partition_util: PartitionUtil::new(&count, &length)?,
        }))
    }
}

impl SingleValueAlgorithm for PartitionByLong {
    fn name(&self) -> &str {
        "PartitionByLong"
    }
    fn column_name(&self) -> &str {
        &self.column_name
    }
    fn calculate_index(&self, v: &str) -> RouterResult<i64> {
        let key: i64 = v.trim().parse().map_err(|_| {
            RouterError::InvalidValue(format!(
                "columnValue:{} Please eliminate any quote and non number within it.",
                v
            ))
        })?;
        // Java: key = (key >>> 32) ^ key （逻辑右移）
        let folded = ((key as u64) >> 32) as i64 ^ key;
        Ok(self.partition_util.partition(folded) as i64)
    }
    fn calculate_index_range(&self, _b: &str, _e: &str) -> Option<Vec<i64>> {
        Some(self.partition_util.all())
    }
}

// ---------------------------------------------------------------------------
// SimplePartitionByLong：partitionCount N → 每分区 1024/N 槽
// ---------------------------------------------------------------------------

pub struct SimplePartitionByLong;

impl SimplePartitionByLong {
    pub fn algo(column: &str, props: &Props) -> RouterResult<Box<dyn SingleValueAlgorithm>> {
        let n: i32 = require_prop(props, "partitionCount")?
            .parse()
            .map_err(|_| {
                RouterError::InvalidConfig("SimplePartitionByLong: bad partitionCount".into())
            })?;
        // 等价 PartitionByLong(count=[N], length=[1024/N])
        let count = vec![n];
        let length = vec![1024 / n];
        Ok(Box::new(PartitionByLong {
            column_name: column.to_string(),
            partition_util: PartitionUtil::new(&count, &length)?,
        }))
    }
}

// ---------------------------------------------------------------------------
// PartitionByHashMod：Thomas-Wang hash(javaHashCode) 取模/按位与
// ---------------------------------------------------------------------------

pub struct PartitionByHashMod {
    column_name: String,
    count: i64,
    watch: bool, // count 为 2 的幂时用 & (count-1)
}

impl PartitionByHashMod {
    pub fn algo(column: &str, props: &Props) -> RouterResult<Box<dyn SingleValueAlgorithm>> {
        let count: i64 = require_prop(props, "count")?
            .parse()
            .map_err(|_| RouterError::InvalidConfig("PartitionByHashMod: bad count".into()))?;
        let watch = (count & (count - 1)) == 0;
        Ok(Box::new(PartitionByHashMod {
            column_name: column.to_string(),
            count,
            watch,
        }))
    }
}

impl SingleValueAlgorithm for PartitionByHashMod {
    fn name(&self) -> &str {
        "PartitionByHashMod"
    }
    fn column_name(&self) -> &str {
        &self.column_name
    }
    fn calculate_index(&self, v: &str) -> RouterResult<i64> {
        // Java: BigInteger.valueOf(hash(v.hashCode())).abs()
        let h = hash::wang_hash(hash::java_string_hash(v));
        let abs = (h as i64).abs();
        if self.watch {
            Ok(abs & (self.count - 1))
        } else {
            Ok(abs % self.count)
        }
    }
}

// ---------------------------------------------------------------------------
// PartitionByMurmurHash：Guava murmur3_32 一致性哈希环
// ---------------------------------------------------------------------------

pub struct PartitionByMurmurHash {
    column_name: String,
    seed: u32,
    /// 有序 (bucketHash → nodeIndex) 环
    ring: Vec<(i32, i64)>,
}

impl PartitionByMurmurHash {
    pub fn algo(
        column: &str,
        props: &Props,
        ranges: &Props,
    ) -> RouterResult<Box<dyn SingleValueAlgorithm>> {
        let seed: u32 = require_prop(props, "seed")?
            .parse()
            .map_err(|_| RouterError::InvalidConfig("PartitionByMurmurHash: bad seed".into()))?;
        let count: i64 = require_prop(props, "count")?
            .parse()
            .map_err(|_| RouterError::InvalidConfig("PartitionByMurmurHash: bad count".into()))?;
        let virtual_bucket_times: i64 = require_prop(props, "virtualBucketTimes")?
            .parse()
            .map_err(|_| {
                RouterError::InvalidConfig("PartitionByMurmurHash: bad virtualBucketTimes".into())
            })?;
        // 权重表：bucket → weight（默认 1）
        let mut weight_map: BTreeMap<i64, i64> = BTreeMap::new();
        for (k, v) in ranges {
            let w: i64 = v.trim().parse().unwrap_or(1);
            weight_map.insert(k.trim().parse().unwrap_or(-1), if w > 0 { w } else { 1 });
        }
        let mut ring: Vec<(i32, i64)> = Vec::new();
        for i in 0..count {
            let prefix = format!("SHARD-{}-NODE-", i);
            let weight = weight_map.get(&i).copied().unwrap_or(1);
            let shard = virtual_bucket_times * weight;
            for n in 0..shard {
                let key = hash::murmur3_32_unencoded_chars(seed, &format!("{}{}", prefix, n));
                ring.push((key, i));
            }
        }
        // TreeMap 语义：按 key 排序；重复 key 后写入者覆盖（保留最后一次 put 的值）
        ring.sort_by_key(|(k, _)| *k);
        ring.dedup_by(|later, earlier| {
            if later.0 == earlier.0 {
                *earlier = later.clone();
                true
            } else {
                false
            }
        });
        Ok(Box::new(PartitionByMurmurHash {
            column_name: column.to_string(),
            seed,
            ring,
        }))
    }
}

impl SingleValueAlgorithm for PartitionByMurmurHash {
    fn name(&self) -> &str {
        "PartitionByMurmurHash"
    }
    fn column_name(&self) -> &str {
        &self.column_name
    }
    fn calculate_index(&self, v: &str) -> RouterResult<i64> {
        let h = hash::murmur3_32_unencoded_chars(self.seed, v);
        // tailMap(h)：第一个 >= h 的键；无则回绕到 firstKey
        match self.ring.binary_search_by_key(&h, |(k, _)| *k) {
            Ok(i) => Ok(self.ring[i].1),
            Err(i) => {
                if i < self.ring.len() {
                    Ok(self.ring[i].1)
                } else {
                    Ok(self.ring[0].1)
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// PartitionByRangeMod（"rangemod-hash"）：范围 + 组内取模
// ---------------------------------------------------------------------------

pub struct PartitionByRangeMod {
    column_name: String,
    ranges: Vec<GroupSizeRange>,
    default_node: i64,
}

impl PartitionByRangeMod {
    pub fn algo(
        column: &str,
        props: &Props,
        ranges: &Props,
    ) -> RouterResult<Box<dyn SingleValueAlgorithm>> {
        let default_node: i64 = prop_or(props, "defaultNode", "-1").parse().unwrap_or(-1);
        Ok(Box::new(PartitionByRangeMod {
            column_name: column.to_string(),
            ranges: parse_group_ranges(ranges)?,
            default_node,
        }))
    }
}

impl SingleValueAlgorithm for PartitionByRangeMod {
    fn name(&self) -> &str {
        "PartitionByRangeMod"
    }
    fn column_name(&self) -> &str {
        &self.column_name
    }
    fn calculate_index(&self, v: &str) -> RouterResult<i64> {
        let value: i64 = v
            .trim()
            .parse()
            .map_err(|_| RouterError::InvalidValue(format!("bad range value: {}", v)))?;
        let mut node_index = 0i64;
        for r in &self.ranges {
            if value >= r.value_start && value <= r.value_end {
                let inner = big_abs_mod(v, r.group_size)?;
                return Ok(node_index + inner);
            } else {
                node_index += r.group_size;
            }
        }
        if self.default_node >= 0 {
            return Ok(self.default_node);
        }
        Ok(-1)
    }
}

// ---------------------------------------------------------------------------
// AutoPartitionByLong：值区间 → 节点
// ---------------------------------------------------------------------------

pub struct AutoPartitionByLong {
    column_name: String,
    ranges: Vec<NodeIndexRange>,
    default_node: i64,
}

impl AutoPartitionByLong {
    pub fn algo(
        column: &str,
        props: &Props,
        ranges: &Props,
    ) -> RouterResult<Box<dyn SingleValueAlgorithm>> {
        let default_node: i64 = prop_or(props, "defaultNode", "-1").parse().unwrap_or(-1);
        Ok(Box::new(AutoPartitionByLong {
            column_name: column.to_string(),
            ranges: parse_node_ranges(ranges)?,
            default_node,
        }))
    }
}

impl SingleValueAlgorithm for AutoPartitionByLong {
    fn name(&self) -> &str {
        "AutoPartitionByLong"
    }
    fn column_name(&self) -> &str {
        &self.column_name
    }
    fn calculate_index(&self, v: &str) -> RouterResult<i64> {
        let value: i64 = v
            .trim()
            .parse()
            .map_err(|_| RouterError::InvalidValue(format!("bad value: {}", v)))?;
        for r in &self.ranges {
            if value >= r.value_start && value <= r.value_end {
                return Ok(r.node_index);
            }
        }
        Ok(self.default_node)
    }
    fn calculate_index_range(&self, begin: &str, end: &str) -> Option<Vec<i64>> {
        // calculateSequenceRange：begin..=end 的连续下标
        let b = self.calculate_index(begin).ok()?;
        let e = self.calculate_index(end).ok()?;
        if e >= b {
            Some((b..=e).collect())
        } else {
            Some(vec![])
        }
    }
}

// ---------------------------------------------------------------------------
// PartitionByPattern：value % patternValue → 区间查找
// ---------------------------------------------------------------------------

pub struct PartitionByPattern {
    column_name: String,
    pattern_value: i64,
    ranges: Vec<NodeIndexRange>,
    default_node: i64,
}

impl PartitionByPattern {
    pub fn algo(
        column: &str,
        props: &Props,
        ranges: &Props,
    ) -> RouterResult<Box<dyn SingleValueAlgorithm>> {
        let pattern_value: i64 = prop_or(props, "patternValue", "1024")
            .parse()
            .unwrap_or(1024);
        let default_node: i64 = prop_or(props, "defaultNode", "-1").parse().unwrap_or(-1);
        Ok(Box::new(PartitionByPattern {
            column_name: column.to_string(),
            pattern_value,
            ranges: parse_node_ranges(ranges)?,
            default_node,
        }))
    }
}

impl SingleValueAlgorithm for PartitionByPattern {
    fn name(&self) -> &str {
        "PartitionByPattern"
    }
    fn column_name(&self) -> &str {
        &self.column_name
    }
    fn calculate_index(&self, v: &str) -> RouterResult<i64> {
        let value: i64 = match v.trim().parse() {
            Ok(x) => x,
            Err(_) => return Ok(self.default_node), // 非数字 → defaultNode
        };
        let hash = value % self.pattern_value;
        for r in &self.ranges {
            if hash >= r.value_start && hash <= r.value_end {
                return Ok(r.node_index);
            }
        }
        Ok(self.default_node)
    }
}

// ---------------------------------------------------------------------------
// PartitionByPrefixPattern：前缀 ASCII 求和 % patternValue → 区间查找
// ---------------------------------------------------------------------------

pub struct PartitionByPrefixPattern {
    column_name: String,
    pattern_value: i64,
    prefix_length: usize,
    ranges: Vec<NodeIndexRange>,
    default_node: i64,
}

impl PartitionByPrefixPattern {
    pub fn algo(
        column: &str,
        props: &Props,
        ranges: &Props,
    ) -> RouterResult<Box<dyn SingleValueAlgorithm>> {
        let pattern_value: i64 = prop_or(props, "patternValue", "1024")
            .parse()
            .unwrap_or(1024);
        let prefix_length: usize = require_prop(props, "prefixLength")?
            .parse()
            .map_err(|_| RouterError::InvalidConfig("bad prefixLength".into()))?;
        let default_node: i64 = prop_or(props, "defaultNode", "-1").parse().unwrap_or(-1);
        Ok(Box::new(PartitionByPrefixPattern {
            column_name: column.to_string(),
            pattern_value,
            prefix_length,
            ranges: parse_node_ranges(ranges)?,
            default_node,
        }))
    }
}

impl SingleValueAlgorithm for PartitionByPrefixPattern {
    fn name(&self) -> &str {
        "PartitionByPrefixPattern"
    }
    fn column_name(&self) -> &str {
        &self.column_name
    }
    fn calculate_index(&self, v: &str) -> RouterResult<i64> {
        let sum: i64 = v.chars().take(self.prefix_length).map(|c| c as i64).sum();
        let hash = sum % self.pattern_value;
        for r in &self.ranges {
            if hash >= r.value_start && hash <= r.value_end {
                return Ok(r.node_index);
            }
        }
        Ok(self.default_node)
    }
}

// ---------------------------------------------------------------------------
// PartitionByFileMap：枚举映射 value → node
// ---------------------------------------------------------------------------

pub struct PartitionByFileMap {
    column_name: String,
    map: BTreeMap<String, i64>,
    default_node: i64,
}

impl PartitionByFileMap {
    pub fn algo(
        column: &str,
        props: &Props,
        ranges: &Props,
    ) -> RouterResult<Box<dyn SingleValueAlgorithm>> {
        let default_node: i64 = prop_or(props, "defaultNode", "-1").parse().unwrap_or(-1);
        let mut map = BTreeMap::new();
        for (k, v) in ranges {
            map.insert(k.clone(), v.trim().parse().unwrap_or(-1));
        }
        Ok(Box::new(PartitionByFileMap {
            column_name: column.to_string(),
            map,
            default_node,
        }))
    }
}

impl SingleValueAlgorithm for PartitionByFileMap {
    fn name(&self) -> &str {
        "PartitionByFileMap"
    }
    fn column_name(&self) -> &str {
        &self.column_name
    }
    fn calculate_index(&self, v: &str) -> RouterResult<i64> {
        Ok(self.map.get(v.trim()).copied().unwrap_or(self.default_node))
    }
}

// ---------------------------------------------------------------------------
// PartitionDirectBySubString：截子串直接作为节点号
// ---------------------------------------------------------------------------

pub struct PartitionDirectBySubString {
    column_name: String,
    start_index: usize,
    size: usize,
    partition_count: i64,
    default_node: i64,
}

impl PartitionDirectBySubString {
    pub fn algo(column: &str, props: &Props) -> RouterResult<Box<dyn SingleValueAlgorithm>> {
        let start_index: usize = prop_or(props, "startIndex", "0").parse().unwrap_or(0);
        let size: usize = require_prop(props, "size")?
            .parse()
            .map_err(|_| RouterError::InvalidConfig("bad size".into()))?;
        let partition_count: i64 = prop_or(props, "partitionCount", "0").parse().unwrap_or(0);
        let default_node: i64 = prop_or(props, "defaultNode", "-1").parse().unwrap_or(-1);
        Ok(Box::new(PartitionDirectBySubString {
            column_name: column.to_string(),
            start_index,
            size,
            partition_count,
            default_node,
        }))
    }
}

impl SingleValueAlgorithm for PartitionDirectBySubString {
    fn name(&self) -> &str {
        "PartitionDirectBySubString"
    }
    fn column_name(&self) -> &str {
        &self.column_name
    }
    fn calculate_index(&self, v: &str) -> RouterResult<i64> {
        let chars: Vec<char> = v.chars().collect();
        if chars.len() < self.start_index + self.size {
            return Ok(self.default_node);
        }
        let sub: String = chars[self.start_index..self.start_index + self.size]
            .iter()
            .collect();
        match sub.parse::<i64>() {
            Ok(idx) => {
                if self.partition_count > 0 && idx >= self.partition_count {
                    Ok(self.default_node)
                } else {
                    Ok(idx)
                }
            }
            Err(_) => Ok(self.default_node),
        }
    }
}

// ---------------------------------------------------------------------------
// PartitionByString：hashSlice 子串 → StringUtil.hash → 1024 槽段表
// ---------------------------------------------------------------------------

pub struct PartitionByString {
    column_name: String,
    partition_util: PartitionUtil,
    hash_slice_start: i64,
    hash_slice_end: i64,
}

impl PartitionByString {
    pub fn algo(column: &str, props: &Props) -> RouterResult<Box<dyn SingleValueAlgorithm>> {
        let count = to_int_array(prop_or(props, "partitionCount", "1"))?;
        let length = to_int_array(prop_or(props, "partitionLength", "1024"))?;
        // hashSlice "start:end"，负数表示从末尾数
        let (mut start, mut end) = (0i64, i64::MAX);
        if let Some(slice) = props.get("hashSlice") {
            let parts: Vec<&str> = slice.split(':').collect();
            if !parts.is_empty() && !parts[0].is_empty() {
                start = parts[0].parse().unwrap_or(0);
            }
            if parts.len() > 1 && !parts[1].is_empty() {
                end = parts[1].parse().unwrap_or(i64::MAX);
            }
        }
        Ok(Box::new(PartitionByString {
            column_name: column.to_string(),
            partition_util: PartitionUtil::new(&count, &length)?,
            hash_slice_start: start,
            hash_slice_end: end,
        }))
    }
}

impl SingleValueAlgorithm for PartitionByString {
    fn name(&self) -> &str {
        "PartitionByString"
    }
    fn column_name(&self) -> &str {
        &self.column_name
    }
    fn calculate_index(&self, v: &str) -> RouterResult<i64> {
        let len = v.chars().count() as i64;
        let norm = |mut i: i64| -> i64 {
            if i < 0 {
                i += len;
            }
            i.clamp(0, len)
        };
        let s = norm(self.hash_slice_start) as usize;
        let e = norm(self.hash_slice_end) as usize;
        let h = hash::string_util_hash(v, s, e);
        Ok(self.partition_util.partition(h) as i64)
    }
}

// ---------------------------------------------------------------------------
// PartitionByDate：按天数分区
// ---------------------------------------------------------------------------

pub struct PartitionByDate {
    column_name: String,
    patterns: Vec<String>,
    begin_date: i64, // day_of_year * ONE_DAY（Java 原样，见 getTime）
    end_date: i64,
    partition_time: i64,
    n_count: i64,
}

impl PartitionByDate {
    pub fn algo(column: &str, props: &Props) -> RouterResult<Box<dyn SingleValueAlgorithm>> {
        let patterns = split_patterns(require_prop(props, "dateFormat")?);
        let begin_date = Self::get_time(require_prop(props, "beginDate")?, &patterns)?;
        let end_str = props.get("endDate").cloned().unwrap_or_default();
        let partition_day: i64 = require_prop(props, "partionDay")?
            .parse()
            .map_err(|_| RouterError::InvalidConfig("bad partionDay".into()))?;
        let partition_time = partition_day * ONE_DAY_MS;
        let mut end_date = 0i64;
        let mut n_count = 0i64;
        if !end_str.is_empty() && end_str != "null" {
            end_date = Self::get_time(&end_str, &patterns)?;
            n_count = (end_date - begin_date) / partition_time + 1;
        }
        Ok(Box::new(PartitionByDate {
            column_name: column.to_string(),
            patterns,
            begin_date,
            end_date,
            partition_time,
            n_count,
        }))
    }

    /// Java getTime：解析后取 DAY_OF_YEAR * ONE_DAY（保留原实现的"忽略年份"特性）。
    fn get_time(value: &str, patterns: &[String]) -> RouterResult<i64> {
        if value == "null" || value.trim().is_empty() {
            return Ok(-1);
        }
        parse_datetime(value, patterns)
            .map(|dt| dt.ordinal() as i64 * ONE_DAY_MS)
            .ok_or_else(|| {
                RouterError::InvalidValue(format!(
                    "columnValue:{} Please check if the format satisfied.",
                    value
                ))
            })
    }

    fn inner_calculate(&self, target_time: i64) -> i64 {
        let mut target_partition = (target_time - self.begin_date) / self.partition_time;
        if target_time > self.end_date && self.n_count != 0 {
            target_partition %= self.n_count;
        }
        target_partition
    }
}

impl SingleValueAlgorithm for PartitionByDate {
    fn name(&self) -> &str {
        "PartitionByDate"
    }
    fn column_name(&self) -> &str {
        &self.column_name
    }
    fn calculate_index(&self, v: &str) -> RouterResult<i64> {
        Ok(self.inner_calculate(Self::get_time(v, &self.patterns)?))
    }
    fn calculate_index_range(&self, begin: &str, end: &str) -> Option<Vec<i64>> {
        let mut b = Self::get_time(begin, &self.patterns).ok()?;
        let mut e = Self::get_time(end, &self.patterns).ok()?;
        if e == -1 {
            e = self.end_date;
        }
        if b == -1 {
            b = self.begin_date;
        }
        let mut list = Vec::new();
        while b <= e {
            let idx = self.inner_calculate(b);
            if !list.contains(&idx) {
                list.push(idx);
            }
            b += self.partition_time;
        }
        Some(list)
    }
}

// ---------------------------------------------------------------------------
// PartitionByMonth：按月分区
// ---------------------------------------------------------------------------

pub struct PartitionByMonth {
    column_name: String,
    patterns: Vec<String>,
    /// scene=1：无 begin/end，返回 0 基月份
    scene1: bool,
    begin_year: i32,
    begin_month0: i32,
    n_partition: i64,
}

impl PartitionByMonth {
    pub fn algo(column: &str, props: &Props) -> RouterResult<Box<dyn SingleValueAlgorithm>> {
        let begin = props.get("beginDate").cloned().unwrap_or_default();
        let end = props.get("endDate").cloned().unwrap_or_default();
        let date_format = prop_or(props, "dateFormat", "yyyy-MM-dd").to_string();
        let patterns = split_patterns(&date_format);
        if begin.is_empty() && end.is_empty() {
            return Ok(Box::new(PartitionByMonth {
                column_name: column.to_string(),
                patterns,
                scene1: true,
                begin_year: 0,
                begin_month0: 0,
                n_partition: 12,
            }));
        }
        let begin_dt = parse_datetime(&begin, &patterns)
            .ok_or_else(|| RouterError::InvalidConfig(format!("bad beginDate: {}", begin)))?;
        let mut n_partition = -1i64;
        if !end.is_empty() {
            let end_dt = parse_datetime(&end, &patterns)
                .ok_or_else(|| RouterError::InvalidConfig(format!("bad endDate: {}", end)))?;
            n_partition = (end_dt.year() - begin_dt.year()) as i64 * 12 + end_dt.month0() as i64
                - begin_dt.month0() as i64
                + 1;
            if n_partition <= 0 {
                return Err(RouterError::InvalidConfig(
                    "Incorrect time range for month partitioning!".into(),
                ));
            }
        }
        Ok(Box::new(PartitionByMonth {
            column_name: column.to_string(),
            patterns,
            scene1: false,
            begin_year: begin_dt.year(),
            begin_month0: begin_dt.month0() as i32,
            n_partition,
        }))
    }

    fn recalculate(&self, mut target: i64) -> i64 {
        if self.n_partition == -1 {
            return target;
        }
        if target < 0 {
            target = self.n_partition - (-target) % self.n_partition;
        }
        if target >= self.n_partition {
            target %= self.n_partition;
        }
        target
    }
}

impl SingleValueAlgorithm for PartitionByMonth {
    fn name(&self) -> &str {
        "PartitionByMonth"
    }
    fn column_name(&self) -> &str {
        &self.column_name
    }
    fn calculate_index(&self, v: &str) -> RouterResult<i64> {
        let dt = parse_datetime(v, &self.patterns).ok_or_else(|| {
            RouterError::InvalidValue(format!(
                "columnValue:{} Please check if the format satisfied.",
                v
            ))
        })?;
        if self.scene1 {
            return Ok(dt.month0() as i64);
        }
        let mut target = (dt.year() - self.begin_year) as i64 * 12 + dt.month0() as i64
            - self.begin_month0 as i64;
        if self.n_partition > 0 {
            target = self.recalculate(target);
        }
        if target < 0 {
            target = 0;
        }
        Ok(target)
    }
    fn calculate_index_range(&self, begin: &str, end: &str) -> Option<Vec<i64>> {
        let b = self.calculate_index(begin).ok()?;
        let e = self.calculate_index(end).ok()?;
        if e >= b {
            Some((b..=e).collect())
        } else {
            Some(vec![])
        }
    }
}

// ---------------------------------------------------------------------------
// PartitionByCRC32PreSlot：CRC32 → 102400 预分槽
// ---------------------------------------------------------------------------

pub const DEFAULT_SLOTS_NUM: i64 = 102_400;

pub struct PartitionByCrc32PreSlot {
    column_name: String,
    range_map2: Vec<i64>,
}

impl PartitionByCrc32PreSlot {
    pub fn algo(
        column: &str,
        props: &Props,
        ranges: &Props,
    ) -> RouterResult<Box<dyn SingleValueAlgorithm>> {
        let mut range_map2 = vec![0i64; DEFAULT_SLOTS_NUM as usize];
        let long_ranges: Vec<NodeIndexRange> = if let Some(count_text) = props.get("count") {
            let count: i64 = count_text.parse().map_err(|_| {
                RouterError::InvalidConfig("PartitionByCRC32PreSlot: bad count".into())
            })?;
            let slot_size = DEFAULT_SLOTS_NUM / count;
            (0..count)
                .map(|i| {
                    let end = if i == count - 1 {
                        DEFAULT_SLOTS_NUM - 1
                    } else {
                        (i + 1) * slot_size - 1
                    };
                    NodeIndexRange {
                        node_index: i,
                        value_start: i * slot_size,
                        value_end: end,
                    }
                })
                .collect()
        } else {
            parse_node_ranges(ranges)?
        };
        for r in &long_ranges {
            for i in r.value_start..=r.value_end {
                if i >= 0 && (i as usize) < range_map2.len() {
                    range_map2[i as usize] = r.node_index;
                }
            }
        }
        Ok(Box::new(PartitionByCrc32PreSlot {
            column_name: column.to_string(),
            range_map2,
        }))
    }
}

impl SingleValueAlgorithm for PartitionByCrc32PreSlot {
    fn name(&self) -> &str {
        "PartitionByCRC32PreSlot"
    }
    fn column_name(&self) -> &str {
        &self.column_name
    }
    fn calculate_index(&self, v: &str) -> RouterResult<i64> {
        let h = hash::crc32(v);
        let slot = (h % DEFAULT_SLOTS_NUM as u64) as usize;
        Ok(self.range_map2[slot])
    }
}

// ---------------------------------------------------------------------------
// PartitionByJumpConsistentHash
// ---------------------------------------------------------------------------

pub struct PartitionByJumpConsistentHash {
    column_name: String,
    total_buckets: i32,
}

impl PartitionByJumpConsistentHash {
    pub fn algo(column: &str, props: &Props) -> RouterResult<Box<dyn SingleValueAlgorithm>> {
        let total_buckets: i32 = require_prop(props, "totalBuckets")?
            .parse()
            .map_err(|_| RouterError::InvalidConfig("bad totalBuckets".into()))?;
        Ok(Box::new(PartitionByJumpConsistentHash {
            column_name: column.to_string(),
            total_buckets,
        }))
    }
}

impl SingleValueAlgorithm for PartitionByJumpConsistentHash {
    fn name(&self) -> &str {
        "PartitionByJumpConsistentHash"
    }
    fn column_name(&self) -> &str {
        &self.column_name
    }
    fn calculate_index(&self, v: &str) -> RouterResult<i64> {
        Ok(hash::jump_consistent_hash(hash::java_string_hash(v) as i64, self.total_buckets) as i64)
    }
}

// ---------------------------------------------------------------------------
// PartitionByLatestMonth / PartitionByHotDate（简化移植）
// ---------------------------------------------------------------------------

/// 最近 N 天热数据在 0 号节点，历史按 partionDay 分段。
pub struct PartitionByHotDate {
    column_name: String,
    patterns: Vec<String>,
    last_days: i64,
    partition_day: i64,
}

impl PartitionByHotDate {
    pub fn algo(column: &str, props: &Props) -> RouterResult<Box<dyn SingleValueAlgorithm>> {
        let last_days: i64 = require_prop(props, "sLastDay")?
            .parse()
            .map_err(|_| RouterError::InvalidConfig("bad sLastDay".into()))?;
        let partition_day: i64 = require_prop(props, "sPartionDay")?
            .parse()
            .map_err(|_| RouterError::InvalidConfig("bad sPartionDay".into()))?;
        let patterns = split_patterns(prop_or(props, "dateFormat", "yyyy-MM-dd"));
        Ok(Box::new(PartitionByHotDate {
            column_name: column.to_string(),
            patterns,
            last_days,
            partition_day,
        }))
    }
}

impl SingleValueAlgorithm for PartitionByHotDate {
    fn name(&self) -> &str {
        "PartitionByHotDate"
    }
    fn column_name(&self) -> &str {
        &self.column_name
    }
    fn calculate_index(&self, v: &str) -> RouterResult<i64> {
        let dt = parse_datetime(v, &self.patterns)
            .ok_or_else(|| RouterError::InvalidValue(format!("bad date value: {}", v)))?;
        let today = chrono::Local::now().naive_local();
        let days = (today.date() - dt.date()).num_days();
        if days <= self.last_days {
            return Ok(0);
        }
        let rest = days - self.last_days;
        Ok(rest / self.partition_day + 1)
    }
}

/// 便于外部构造的辅助：把 algo 包装成带节点的完整函数。
pub fn wrap(
    algo: Box<dyn SingleValueAlgorithm>,
    data_nodes: Arc<Vec<IndexDataNode>>,
) -> SingleValueFunction {
    SingleValueFunction::new(algo, data_nodes)
}
