//! 现代 AutoFunction（`dbpartition by ... tbpartition by ...` 风格）。
//!
//! 移植自 `io.mycat.router.function.AutoFunction` + `AutoFunctionFactory`：
//! - properties：`dbNum` / `tableNum` / `storeNum` / `mappingFormat` / `dbMethod` / `tableMethod`
//! - mappingFormat 模板：`c${targetIndex}/db1_${dbIndex}/sharding_${tableIndex}`
//! - 方法：MOD_HASH(hash) / UNI_HASH / RIGHT_SHIFT(col,n) / STR_HASH(col,start,end,valType,seed)
//!   / YYYYMM / YYYYDD / YYYYWEEK / MM / DD / MMDD / WEEK
//! - 当模板含 `${index}` 且方法属于 FLATTEN_MAPPING 家族时，使用扁平化索引空间
//!   （index = value % (dbNum*tableNum)，dbIndex = index/tableNum，tableIndex = index%tableNum）。

use std::collections::BTreeMap;
use std::sync::Arc;

use chrono::Datelike;

use crate::datefmt::parse_datetime;
use crate::function::{
    prop_or, require_prop, Operator, RangeVariable, RouterError, RouterResult, RuleFunction,
};
use crate::hash;
use crate::node::IndexDataNode;

/// FLATTEN_MAPPING 家族（对应 `AutoFunctionFactory.FLATTEN_MAPPING`）。
const FLATTEN_FAMILY: &[&str] = &[
    "MOD_HASH",
    "UNI_HASH",
    "RIGHT_SHIFT",
    "YYYYMM",
    "YYYYDD",
    "YYYYWEEK",
];

/// 可枚举日期的方法（RANGE 条件下逐日枚举）。
const ENUM_FAMILY: &[&str] = &["YYYYMM", "YYYYDD", "YYYYWEEK", "MM", "DD", "MMDD", "WEEK"];

/// 解析后的分片方法。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MethodSpec {
    /// 大写方法名
    pub name: String,
    /// 分片列名
    pub column: String,
    /// 附加参数（right_shift 的 n、str_hash 的 start/end/valType/seed 等）
    pub args: Vec<String>,
}

impl MethodSpec {
    /// 解析形如 `hash(id)`、`RIGHT_SHIFT(id,3)`、`str_hash(name,0,4,0,31)` 的方法表达式。
    pub fn parse(expr: &str) -> RouterResult<MethodSpec> {
        let expr = expr.trim();
        let (name, rest) = match expr.find('(') {
            Some(i) => (&expr[..i], &expr[i + 1..]),
            None => (expr, ""),
        };
        let name = name.trim().to_uppercase();
        let rest = rest.trim_end_matches(')');
        let mut args: Vec<String> = rest
            .split(',')
            .map(|s| s.trim().trim_matches('\'').to_string())
            .filter(|s| !s.is_empty())
            .collect();
        // 别名归一化：HASH → MOD_HASH
        let name = match name.as_str() {
            "HASH" => "MOD_HASH".to_string(),
            other => other.to_string(),
        };
        if args.is_empty() {
            return Err(RouterError::InvalidConfig(format!(
                "missing column in method: {}",
                expr
            )));
        }
        const SUPPORTED: &[&str] = &[
            "MOD_HASH",
            "UNI_HASH",
            "YYYYMM",
            "YYYYDD",
            "YYYYWEEK",
            "MM",
            "DD",
            "MMDD",
            "WEEK",
            "RIGHT_SHIFT",
            "STR_HASH",
            "RANGE_HASH",
        ];
        if !SUPPORTED.contains(&name.as_str()) {
            return Err(RouterError::InvalidConfig(format!(
                "unsupported sharding method: {}",
                expr
            )));
        }
        let column = args.remove(0);
        Ok(MethodSpec { name, column, args })
    }

    pub fn is_flatten_family(&self) -> bool {
        FLATTEN_FAMILY.contains(&self.name.as_str())
    }

    pub fn is_enum_family(&self) -> bool {
        ENUM_FAMILY.contains(&self.name.as_str())
    }

    /// 对单个列值计算 `value % num` 空间中的散列结果（未取模 num 前的原始序号按方法而定）。
    pub fn evaluate(&self, value: &str, num: i64) -> RouterResult<i64> {
        match self.name.as_str() {
            "MOD_HASH" | "UNI_HASH" => Ok(single_mod_hash(num, value)),
            "RIGHT_SHIFT" => {
                let shift: i64 = self.args.first().and_then(|s| s.parse().ok()).unwrap_or(0);
                single_right_shift(num, shift, value)
            }
            "STR_HASH" => {
                // str_hash(col, start, end, valType, randSeed)
                let start: i64 = self.args.first().and_then(|s| s.parse().ok()).unwrap_or(0);
                let end: i64 = self
                    .args
                    .get(1)
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(i64::MAX);
                let val_type: i64 = self.args.get(2).and_then(|s| s.parse().ok()).unwrap_or(0);
                let rand_seed: i32 = self.args.get(3).and_then(|s| s.parse().ok()).unwrap_or(31);
                Ok(str_hash(num, value, start, end, val_type, rand_seed))
            }
            "YYYYMM" | "YYYYDD" | "YYYYWEEK" | "MM" | "DD" | "MMDD" | "WEEK" => {
                let dt = parse_datetime(value, &[]).ok_or_else(|| {
                    RouterError::InvalidValue(format!(
                        "bad date value for {}: {}",
                        self.name, value
                    ))
                })?;
                Ok(date_method(&self.name, &dt, num))
            }
            other => Err(RouterError::InvalidConfig(format!(
                "method not supported yet: {}",
                other
            ))),
        }
    }
}

/// `singleModHash`：数字取模，字符串按 Java hashCode 取模。
pub fn single_mod_hash(num: i64, value: &str) -> i64 {
    let v = value.trim();
    if let Ok(n) = v.parse::<i64>() {
        (n % num).abs()
    } else if let Ok(f) = v.parse::<f64>() {
        ((f as i64) % num).abs()
    } else {
        (hash::java_string_hash(v) as i64 % num).abs()
    }
}

/// `singleRightShift`：abs((value >> shift) % num)。
pub fn single_right_shift(num: i64, shift: i64, value: &str) -> RouterResult<i64> {
    let v: i64 = value.trim().parse().map_err(|_| {
        RouterError::InvalidValue(format!("right_shift expects number, got: {}", value))
    })?;
    Ok(((v >> shift) % num).abs())
}

/// `STR_HASH`：截取子串后按 valType 哈希取模。
pub fn str_hash(num: i64, value: &str, start: i64, end: i64, val_type: i64, rand_seed: i32) -> i64 {
    let chars: Vec<char> = value.chars().collect();
    let len = chars.len() as i64;
    let norm = |mut i: i64| -> usize {
        if i < 0 {
            i += len;
        }
        i.clamp(0, len) as usize
    };
    let s = norm(start);
    let e = if end == i64::MAX {
        len as usize
    } else {
        norm(end)
    };
    let sub: String = chars[s..e.max(s)].iter().collect();
    if val_type == 1 {
        let n: i64 = sub.parse().unwrap_or(0);
        (n % num).abs()
    } else {
        (hash::java_string_hash_seed(&sub, rand_seed) as i64 % num).abs()
    }
}

/// 日期族方法：YYYYMM/YYYYDD/YYYYWEEK/MM/DD/MMDD/WEEK。
pub fn date_method(name: &str, dt: &chrono::NaiveDateTime, num: i64) -> i64 {
    let y = dt.year() as i64;
    let m = dt.month() as i64; // 1-12
    let d = dt.day() as i64;
    let doy = dt.ordinal() as i64;
    let week = dt.iso_week().week() as i64;
    let raw = match name {
        "YYYYMM" => y * 12 + m,
        "YYYYDD" => y * 366 + doy,
        "YYYYWEEK" => y * 54 + week,
        "MM" => m,
        "DD" => d,
        "MMDD" => m * 100 + d,
        "WEEK" => week,
        _ => 0,
    };
    (raw % num).abs()
}

/// `AutoFunction`：二维（库×表）分片函数。
pub struct AutoFunction {
    db_method: MethodSpec,
    table_method: MethodSpec,
    db_num: i64,
    table_num: i64,
    mapping_format: String,
    flatten: bool,
    data_nodes: Arc<Vec<IndexDataNode>>,
}

impl AutoFunction {
    /// mappingFormat 模板（EXPLAIN 展示用）
    pub fn mapping_format(&self) -> &str {
        &self.mapping_format
    }
}

/// 渲染 mappingFormat 模板：替换 ${targetIndex} ${dbIndex} ${tableIndex} ${index}。
pub fn render_mapping(
    format: &str,
    target_index: i64,
    db_index: i64,
    table_index: i64,
    index: i64,
) -> String {
    format
        .replace("${targetIndex}", &target_index.to_string())
        .replace("${dbIndex}", &db_index.to_string())
        .replace("${tableIndex}", &table_index.to_string())
        .replace("${index}", &index.to_string())
}

/// 依据 AutoFunction properties 生成物理节点列表。
/// 对应 Java `AutoFunctionFactory` 的 dataNodes 构造 + Groovy 模板渲染。
pub fn expand_auto_nodes(props: &BTreeMap<String, String>) -> RouterResult<Vec<IndexDataNode>> {
    let db_num: i64 = require_prop(props, "dbNum")?
        .parse()
        .map_err(|_| RouterError::InvalidConfig("bad dbNum".into()))?;
    let table_num: i64 = require_prop(props, "tableNum")?
        .parse()
        .map_err(|_| RouterError::InvalidConfig("bad tableNum".into()))?;
    let store_num: i64 = prop_or(props, "storeNum", "1")
        .parse()
        .map_err(|_| RouterError::InvalidConfig("bad storeNum".into()))?;
    let mapping_format = require_prop(props, "mappingFormat")?;
    let total = db_num * table_num;
    let store_db_num = (total / store_num.max(1)).max(1);
    let mut nodes = Vec::with_capacity(total as usize);
    for index in 0..total {
        let db_index = index / table_num;
        let table_index = index % table_num;
        let target_index = index / store_db_num;
        let rendered = render_mapping(mapping_format, target_index, db_index, table_index, index);
        let parts: Vec<&str> = rendered.split('/').collect();
        if parts.len() != 3 {
            return Err(RouterError::InvalidConfig(format!(
                "mappingFormat must render to target/schema/table, got: {}",
                rendered
            )));
        }
        let mut node = IndexDataNode::new(parts[0], parts[1], parts[2], index as i32);
        node.db_index = db_index as i32;
        node.table_index = table_index as i32;
        nodes.push(node);
    }
    Ok(nodes)
}

impl AutoFunction {
    pub fn build(
        props: &BTreeMap<String, String>,
        data_nodes: Arc<Vec<IndexDataNode>>,
    ) -> RouterResult<Self> {
        let db_num: i64 = prop_or(props, "dbNum", "1").parse().unwrap_or(1);
        let table_num: i64 = prop_or(props, "tableNum", "1").parse().unwrap_or(1);
        let mapping_format = prop_or(props, "mappingFormat", "").to_string();
        let db_expr = props.get("dbMethod").cloned().unwrap_or_default();
        let table_expr = props.get("tableMethod").cloned().unwrap_or_default();
        // 允许只有 tableMethod（单库分表）
        let (db_method, table_method) = if db_expr.is_empty() && table_expr.is_empty() {
            return Err(RouterError::InvalidConfig(
                "AutoFunction requires dbMethod/tableMethod".into(),
            ));
        } else if db_expr.is_empty() {
            let tm = MethodSpec::parse(&table_expr)?;
            (tm.clone(), tm)
        } else if table_expr.is_empty() {
            let dm = MethodSpec::parse(&db_expr)?;
            (dm.clone(), dm)
        } else {
            (
                MethodSpec::parse(&db_expr)?,
                MethodSpec::parse(&table_expr)?,
            )
        };
        let flatten = mapping_format.contains("${index}")
            && db_method.is_flatten_family()
            && table_method.is_flatten_family();
        Ok(AutoFunction {
            db_method,
            table_method,
            db_num,
            table_num,
            mapping_format,
            flatten,
            data_nodes,
        })
    }

    fn find_var<'a>(
        &self,
        values: &'a BTreeMap<String, RangeVariable>,
        column: &str,
    ) -> Option<&'a RangeVariable> {
        values
            .values()
            .find(|v| v.column_name.eq_ignore_ascii_case(column))
    }

    /// 扁平模式下由列值算出全局 index。
    fn flatten_index(&self, value: &str) -> RouterResult<i64> {
        let total = self.db_num * self.table_num;
        Ok(self.table_method.evaluate(value, total)?)
    }

    fn db_index_of(&self, value: &str) -> RouterResult<i64> {
        if self.flatten {
            Ok(self.flatten_index(value)? / self.table_num)
        } else {
            self.db_method.evaluate(value, self.db_num)
        }
    }

    fn table_index_of(&self, value: &str) -> RouterResult<i64> {
        if self.flatten {
            Ok(self.flatten_index(value)? % self.table_num)
        } else {
            self.table_method.evaluate(value, self.table_num)
        }
    }

    fn node_by_db_table(&self, db_index: i64, table_index: i64) -> Option<IndexDataNode> {
        self.data_nodes
            .iter()
            .find(|n| n.db_index == db_index as i32 && n.table_index == table_index as i32)
            .cloned()
    }

    /// RANGE 且为日期枚举族：逐日枚举 begin..=end 计算命中节点。
    /// `dim` 指明按库维度还是表维度过滤（避免 db/table 方法同名时混淆）。
    fn enum_range(
        &self,
        method: &MethodSpec,
        dim: Dim,
        begin: &str,
        end: &str,
    ) -> Option<Vec<IndexDataNode>> {
        if !method.is_enum_family() {
            return None;
        }
        let b = parse_datetime(begin, &[])?;
        let e = parse_datetime(end, &[])?;
        if e < b {
            return Some(vec![]);
        }
        let num = match dim {
            Dim::Db => self.db_num,
            Dim::Table => self.table_num,
        };
        let days = (e.date() - b.date()).num_days();
        let step = (days / 400).max(1);
        let mut out: Vec<IndexDataNode> = Vec::new();
        let mut cur = b;
        let mut guard = 0;
        while cur <= e && guard < 10000 {
            if let Ok(idx) = method.evaluate(&cur.format("%Y-%m-%d %H:%M:%S").to_string(), num) {
                let candidates: Vec<IndexDataNode> = match dim {
                    Dim::Db => self
                        .data_nodes
                        .iter()
                        .filter(|n| n.db_index == idx as i32)
                        .cloned()
                        .collect(),
                    Dim::Table => self
                        .data_nodes
                        .iter()
                        .filter(|n| n.table_index == idx as i32)
                        .cloned()
                        .collect(),
                };
                for c in candidates {
                    if !out.contains(&c) {
                        out.push(c);
                    }
                }
            }
            cur = cur + chrono::Duration::days(step);
            guard += 1;
        }
        Some(out)
    }
}

/// 枚举维度：按分库下标还是分表下标过滤节点。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Dim {
    Db,
    Table,
}

impl RuleFunction for AutoFunction {
    fn name(&self) -> &str {
        "AutoFunction"
    }

    fn sharding_columns(&self) -> Vec<String> {
        let mut cols = vec![self.db_method.column.clone()];
        if self.table_method.column != self.db_method.column {
            cols.push(self.table_method.column.clone());
        }
        cols
    }

    fn calculate(
        &self,
        values: &BTreeMap<String, RangeVariable>,
    ) -> RouterResult<Vec<IndexDataNode>> {
        let all = || Ok(self.data_nodes.as_ref().clone());
        let db_var = self.find_var(values, &self.db_method.column);
        let table_var = self.find_var(values, &self.table_method.column);

        // 同为等值：精确定位单节点
        if let (Some(dv), Some(tv)) = (db_var, table_var) {
            if dv.operator == Operator::Equal && tv.operator == Operator::Equal {
                let d = dv.begin.clone().unwrap_or_default();
                let t = tv.begin.clone().unwrap_or_default();
                if self.flatten {
                    // 扁平模式：两列相同值时 index 直接由值决定
                    let idx = self.flatten_index(&d)?;
                    let db_index = idx / self.table_num;
                    let table_index = idx % self.table_num;
                    return match self.node_by_db_table(db_index, table_index) {
                        Some(n) => Ok(vec![n]),
                        None => Err(RouterError::IndexOutOfBound {
                            index: idx,
                            nodes: self.data_nodes.len(),
                        }),
                    };
                }
                let db_index = self.db_method.evaluate(&d, self.db_num)?;
                let table_index = self.table_method.evaluate(&t, self.table_num)?;
                return match self.node_by_db_table(db_index, table_index) {
                    Some(n) => Ok(vec![n]),
                    None => Err(RouterError::IndexOutOfBound {
                        index: db_index * self.table_num + table_index,
                        nodes: self.data_nodes.len(),
                    }),
                };
            }
            // 双方都是 RANGE：分别枚举后取交集
            if dv.operator == Operator::Range && tv.operator == Operator::Range {
                let dn = self.enum_range(
                    &self.db_method,
                    Dim::Db,
                    &dv.begin.clone().unwrap_or_default(),
                    &dv.end.clone().unwrap_or_default(),
                );
                let tn = self.enum_range(
                    &self.table_method,
                    Dim::Table,
                    &tv.begin.clone().unwrap_or_default(),
                    &tv.end.clone().unwrap_or_default(),
                );
                if let (Some(dn), Some(tn)) = (dn, tn) {
                    let inter: Vec<IndexDataNode> =
                        dn.iter().filter(|n| tn.contains(n)).cloned().collect();
                    if !inter.is_empty() {
                        return Ok(inter);
                    }
                    if !tn.is_empty() {
                        return Ok(tn);
                    }
                    if !dn.is_empty() {
                        return Ok(dn);
                    }
                }
            }
            // 其中一方为 RANGE 且可枚举
            if dv.operator == Operator::Equal {
                let d = dv.begin.clone().unwrap_or_default();
                let db_index = self.db_index_of(&d)?;
                if tv.operator == Operator::Range {
                    if let Some(mut nodes) = self.enum_range(
                        &self.table_method,
                        Dim::Table,
                        &tv.begin.clone().unwrap_or_default(),
                        &tv.end.clone().unwrap_or_default(),
                    ) {
                        nodes.retain(|n| n.db_index == db_index as i32);
                        if nodes.is_empty() {
                            return Ok(self
                                .data_nodes
                                .iter()
                                .filter(|n| n.db_index == db_index as i32)
                                .cloned()
                                .collect());
                        }
                        return Ok(nodes);
                    }
                }
                return Ok(self
                    .data_nodes
                    .iter()
                    .filter(|n| n.db_index == db_index as i32)
                    .cloned()
                    .collect());
            }
            if tv.operator == Operator::Equal {
                let t = tv.begin.clone().unwrap_or_default();
                let table_index = self.table_index_of(&t)?;
                return Ok(self
                    .data_nodes
                    .iter()
                    .filter(|n| n.table_index == table_index as i32)
                    .cloned()
                    .collect());
            }
        }

        // 只有 db 条件
        if let Some(dv) = db_var {
            if dv.operator == Operator::Equal {
                let d = dv.begin.clone().unwrap_or_default();
                let db_index = self.db_index_of(&d)?;
                let nodes: Vec<IndexDataNode> = self
                    .data_nodes
                    .iter()
                    .filter(|n| n.db_index == db_index as i32)
                    .cloned()
                    .collect();
                if !nodes.is_empty() {
                    return Ok(nodes);
                }
            } else if let Some(nodes) = self.enum_range(
                &self.db_method,
                Dim::Db,
                &dv.begin.clone().unwrap_or_default(),
                &dv.end.clone().unwrap_or_default(),
            ) {
                if !nodes.is_empty() {
                    return Ok(nodes);
                }
            }
        }
        // 只有 table 条件
        if let Some(tv) = table_var {
            if tv.operator == Operator::Equal {
                let t = tv.begin.clone().unwrap_or_default();
                let table_index = self.table_index_of(&t)?;
                let nodes: Vec<IndexDataNode> = self
                    .data_nodes
                    .iter()
                    .filter(|n| n.table_index == table_index as i32)
                    .cloned()
                    .collect();
                if !nodes.is_empty() {
                    return Ok(nodes);
                }
            } else if let Some(nodes) = self.enum_range(
                &self.table_method,
                Dim::Table,
                &tv.begin.clone().unwrap_or_default(),
                &tv.end.clone().unwrap_or_default(),
            ) {
                if !nodes.is_empty() {
                    return Ok(nodes);
                }
            }
        }
        all()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn auto_props() -> BTreeMap<String, String> {
        let mut p = BTreeMap::new();
        p.insert("dbNum".into(), "2".into());
        p.insert("tableNum".into(), "4".into());
        p.insert("storeNum".into(), "1".into());
        p.insert(
            "mappingFormat".into(),
            "c${targetIndex}/db1_${dbIndex}/sharding_${tableIndex}".into(),
        );
        p.insert("dbMethod".into(), "hash(id)".into());
        p.insert("tableMethod".into(), "hash(id)".into());
        p
    }

    /// internalexample/HashFunctionTest 的期望：id=15 → c0.db1_1.sharding_3, index=7
    #[test]
    fn test_auto_function_hash_id_15() {
        let props = auto_props();
        let nodes = Arc::new(expand_auto_nodes(&props).unwrap());
        assert_eq!(nodes.len(), 8);
        let f = AutoFunction::build(&props, nodes).unwrap();
        assert!(!f.flatten); // mappingFormat 无 ${index} → 非扁平
        let mut values = BTreeMap::new();
        values.insert("id".into(), RangeVariable::equal("id", "15"));
        let res = f.calculate(&values).unwrap();
        assert_eq!(res.len(), 1);
        // 非扁平：dbIndex = 15%2=1, tableIndex = 15%4=3 → db1_1.sharding_3
        assert_eq!(res[0].target_name, "c0");
        assert_eq!(res[0].schema_name, "db1_1");
        assert_eq!(res[0].table_name, "sharding_3");
    }

    #[test]
    fn test_auto_function_flatten_index() {
        let mut props = auto_props();
        props.insert(
            "mappingFormat".into(),
            "c${targetIndex}/db1_${dbIndex}/sharding_${tableIndex}_${index}".into(),
        );
        let nodes = Arc::new(expand_auto_nodes(&props).unwrap());
        let f = AutoFunction::build(&props, nodes).unwrap();
        assert!(f.flatten);
        let mut values = BTreeMap::new();
        values.insert("id".into(), RangeVariable::equal("id", "15"));
        let res = f.calculate(&values).unwrap();
        // 扁平：index = 15 % 8 = 7 → dbIndex=7/4=1, tableIndex=7%4=3
        assert_eq!(res.len(), 1);
        assert_eq!(res[0].index, 7);
        assert_eq!(res[0].schema_name, "db1_1");
        assert_eq!(res[0].table_name, "sharding_3_7");
    }

    #[test]
    fn test_auto_function_full_scan_without_condition() {
        let props = auto_props();
        let nodes = Arc::new(expand_auto_nodes(&props).unwrap());
        let f = AutoFunction::build(&props, nodes).unwrap();
        let res = f.calculate(&BTreeMap::new()).unwrap();
        assert_eq!(res.len(), 8);
    }

    #[test]
    fn test_auto_function_string_value() {
        let props = auto_props();
        let nodes = Arc::new(expand_auto_nodes(&props).unwrap());
        let f = AutoFunction::build(&props, nodes).unwrap();
        let mut values = BTreeMap::new();
        values.insert("id".into(), RangeVariable::equal("id", "abc"));
        let res = f.calculate(&values).unwrap();
        assert_eq!(res.len(), 1);
        // 字符串按 Java hashCode 取模
        let h = hash::java_string_hash("abc") as i64;
        let db = (h % 2).abs();
        assert_eq!(res[0].db_index as i64, db);
    }

    #[test]
    fn test_expand_nodes_store_num() {
        let mut props = auto_props();
        props.insert("storeNum".into(), "2".into());
        let nodes = expand_auto_nodes(&props).unwrap();
        // total=8, storeDbNum=8/2=4 → targetIndex = index/4 ∈ {0,1}
        assert_eq!(nodes[0].target_name, "c0");
        assert_eq!(nodes[4].target_name, "c1");
    }

    #[test]
    fn test_method_parse() {
        let m = MethodSpec::parse("hash(id)").unwrap();
        assert_eq!(m.name, "MOD_HASH");
        assert_eq!(m.column, "id");
        let m = MethodSpec::parse("RIGHT_SHIFT(user_id, 3)").unwrap();
        assert_eq!(m.name, "RIGHT_SHIFT");
        assert_eq!(m.args, vec!["3"]);
        let m = MethodSpec::parse("str_hash(name,0,4,0,31)").unwrap();
        assert_eq!(m.args, vec!["0", "4", "0", "31"]);
    }

    #[test]
    fn test_single_mod_hash() {
        assert_eq!(single_mod_hash(4, "15"), 3);
        assert_eq!(single_mod_hash(4, "-15"), 3);
        // 字符串: "abc".hashCode()=96354, 96354%4=2
        assert_eq!(single_mod_hash(4, "abc"), 2);
    }

    #[test]
    fn test_right_shift() {
        // (16 >> 3) % 4 = 2 % 4 = 2
        assert_eq!(single_right_shift(4, 3, "16").unwrap(), 2);
    }

    #[test]
    fn test_yyyymm_method() {
        let m = MethodSpec::parse("yyyymm(created_at)").unwrap();
        let v = m.evaluate("2021-03-15", 12).unwrap();
        // (2021*12+3) % 12 = 3
        assert_eq!(v, 3);
    }

    #[test]
    fn test_mmdd() {
        let m = MethodSpec::parse("MMDD(d)").unwrap();
        let v = m.evaluate("2021-03-15", 100).unwrap();
        // (3*100+15) % 100 = 15
        assert_eq!(v, 15);
    }

    #[test]
    fn test_str_hash() {
        // valType=1: parseLong(substring) % num
        assert_eq!(str_hash(4, "123456", 0, 4, 1, 31), 1234 % 4);
        // 负索引从末尾数
        assert_eq!(str_hash(4, "123456", -2, i64::MAX, 1, 31), 56 % 4);
    }

    #[test]
    fn test_enum_range_yyyymm() {
        let mut props = BTreeMap::new();
        props.insert("dbNum".into(), "1".into());
        props.insert("tableNum".into(), "12".into());
        props.insert("storeNum".into(), "1".into());
        props.insert("mappingFormat".into(), "c0/db1/t_${tableIndex}".into());
        props.insert("dbMethod".into(), "yyyymm(created_at)".into());
        props.insert("tableMethod".into(), "yyyymm(created_at)".into());
        let nodes = Arc::new(expand_auto_nodes(&props).unwrap());
        let f = AutoFunction::build(&props, nodes).unwrap();
        let mut values = BTreeMap::new();
        values.insert(
            "created_at".into(),
            RangeVariable::range(
                "created_at",
                Some("2021-01-05".into()),
                Some("2021-03-20".into()),
            ),
        );
        let res = f.calculate(&values).unwrap();
        // 2021-01..2021-03 → tableIndex (2021*12+M)%12 = M%12 → 1,2,3
        assert_eq!(res.len(), 3);
    }
}
