//! 分片节点模型与元数据展开。
//!
//! - [`IndexDataNode`] 对应 `io.mycat.Partition` / `router.function.IndexDataNode`
//! - [`expand_backend_nodes`] 对应 `MetadataManager.getBackendTableInfos`：
//!   `data` 显式节点优先，否则 targetNames × schemaNames × tableNames 笛卡尔积
//!   （名字支持 `db$0-3` 展开，见 `mycat3_config::split_expand`）。

use mycat3_config::ShardingBackendConfig;
use serde::{Deserialize, Serialize};

/// 物理分片节点（目标集群 + 物理库 + 物理表）。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexDataNode {
    /// 目标名（集群名或数据源名）
    pub target_name: String,
    /// 物理 schema
    pub schema_name: String,
    /// 物理表名
    pub table_name: String,
    /// 分库下标（-1 表示不适用）
    pub db_index: i32,
    /// 分表下标（-1 表示不适用）
    pub table_index: i32,
    /// 全局节点下标
    pub index: i32,
}

impl IndexDataNode {
    pub fn new(
        target: impl Into<String>,
        schema: impl Into<String>,
        table: impl Into<String>,
        index: i32,
    ) -> Self {
        IndexDataNode {
            target_name: target.into(),
            schema_name: schema.into(),
            table_name: table.into(),
            db_index: -1,
            table_index: -1,
            index,
        }
    }

    /// `target.schema.table`，对应 `Partition.getUniqueName()`。
    pub fn unique_name(&self) -> String {
        format!(
            "{}.{}.{}",
            self.target_name, self.schema_name, self.table_name
        )
    }
}

fn json_str(v: &serde_json::Value) -> String {
    match v {
        serde_json::Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}

fn json_i32(v: &serde_json::Value, default: i32) -> i32 {
    match v {
        serde_json::Value::Number(n) => n.as_i64().unwrap_or(default as i64) as i32,
        serde_json::Value::String(s) => s.parse().unwrap_or(default),
        _ => default,
    }
}

/// 将分片表后端配置展开为物理节点列表。
///
/// 对应 Java：`data` 每行 `[target, schema, table]` 或
/// `[target, schema, table, dbIndex, tableIndex, index]`；
/// 否则做三列表的笛卡尔积（每列先经 `split_expand`）。
pub fn expand_backend_nodes(partition: &ShardingBackendConfig) -> Vec<IndexDataNode> {
    if !partition.data.is_empty() {
        let mut nodes = Vec::with_capacity(partition.data.len());
        for (i, row) in partition.data.iter().enumerate() {
            if row.len() < 3 {
                continue;
            }
            let mut node = IndexDataNode::new(
                json_str(&row[0]),
                json_str(&row[1]),
                json_str(&row[2]),
                i as i32,
            );
            if row.len() >= 6 {
                node.db_index = json_i32(&row[3], -1);
                node.table_index = json_i32(&row[4], -1);
                node.index = json_i32(&row[5], i as i32);
            }
            nodes.push(node);
        }
        return nodes;
    }
    let targets = expand_names(&partition.target_names);
    let schemas = expand_names(&partition.schema_names);
    let tables = expand_names(&partition.table_names);
    let mut nodes = Vec::new();
    let mut index = 0i32;
    for t in &targets {
        for s in &schemas {
            for tb in &tables {
                nodes.push(IndexDataNode::new(t.clone(), s.clone(), tb.clone(), index));
                index += 1;
            }
        }
    }
    nodes
}

fn expand_names(names: &[String]) -> Vec<String> {
    let mut out = Vec::new();
    for n in names {
        out.extend(mycat3_config::split_expand(n));
    }
    if out.is_empty() {
        out.push(String::new());
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_expand_cross_product() {
        let cfg: ShardingBackendConfig = serde_json::from_str(
            r#"{"targetNames":["c0"],"schemaNames":["db1$0-1"],"tableNames":["t$0-2"]}"#,
        )
        .unwrap();
        let nodes = expand_backend_nodes(&cfg);
        assert_eq!(nodes.len(), 6);
        assert_eq!(nodes[0].unique_name(), "c0.db10.t0");
        assert_eq!(nodes[5].unique_name(), "c0.db11.t2");
        assert_eq!(nodes[3].index, 3);
    }

    #[test]
    fn test_expand_explicit_data() {
        let cfg: ShardingBackendConfig =
            serde_json::from_str(r#"{"data":[["c0","db1","t0",0,0,0],["c1","db2","t1",1,1,1]]}"#)
                .unwrap();
        let nodes = expand_backend_nodes(&cfg);
        assert_eq!(nodes.len(), 2);
        assert_eq!(nodes[1].db_index, 1);
        assert_eq!(nodes[1].target_name, "c1");
    }

    #[test]
    fn test_expand_multi_target() {
        let cfg: ShardingBackendConfig = serde_json::from_str(
            r#"{"targetNames":["c0","c1"],"schemaNames":["db1"],"tableNames":["t0"]}"#,
        )
        .unwrap();
        let nodes = expand_backend_nodes(&cfg);
        assert_eq!(nodes.len(), 2);
        assert_eq!(nodes[0].target_name, "c0");
        assert_eq!(nodes[1].target_name, "c1");
    }
}
