//! 逻辑库表元数据。对应 `hbt` 模块的 `io.mycat.MetadataManager`：
//! 将 `MycatRouterConfig` 中的 schema/表配置编译为运行时路由元数据。

use std::collections::BTreeMap;
use std::sync::Arc;

use mycat3_config::{
    GlobalTableConfig, LogicSchemaConfig, MycatRouterConfig, NormalTableConfig, ShardingTableConfig,
};
use mycat3_router::{
    build_rule_function, expand_auto_nodes, expand_backend_nodes, is_auto_function, IndexDataNode,
    RouterError, RuleFunction,
};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum MetaError {
    #[error("router error: {0}")]
    Router(#[from] RouterError),
    #[error("config error: {0}")]
    Config(String),
}

pub type MetaResult<T> = Result<T, MetaError>;

/// 表类型。对应 `ShardingTableType` / MetadataManager 中的表分类。
pub enum TableMeta {
    /// 单表：只存在于一个 target（对应 NormalTable）
    Normal {
        target: String,
        schema: String,
        table: String,
        create_sql: Option<String>,
    },
    /// 广播表：每个 target 一份全量（对应 GlobalTable）
    Global {
        targets: Vec<String>,
        schema: String,
        /// 每广播节点的 (target, 物理 schema)——物理 SQL 重写用
        target_schemas: Vec<(String, String)>,
        create_sql: Option<String>,
        /// NO_SEQUENCE / GLOBAL_SEQUENCE / FIRST_SEQUENCE（自增序列填充判定）
        sequence_type: Option<String>,
    },
    /// 分片表（对应 ShardingTable）
    Sharding {
        nodes: Arc<Vec<IndexDataNode>>,
        function: Box<dyn RuleFunction>,
        create_sql: Option<String>,
    },
    /// 视图
    View { create_sql: Option<String> },
}

impl TableMeta {
    pub fn table_kind(&self) -> &'static str {
        match self {
            TableMeta::Normal { .. } => "NORMAL",
            TableMeta::Global { .. } => "GLOBAL",
            TableMeta::Sharding { .. } => "SHARDING",
            TableMeta::View { .. } => "VIEW",
        }
    }
}

/// 逻辑库元数据。
pub struct SchemaMeta {
    pub name: String,
    /// 库级默认 target（未配置 locality 的表/未知表都路由到这里）
    pub target_name: String,
    /// 表名统一小写存储（MySQL 表名不区分大小写路由）
    pub tables: BTreeMap<String, TableMeta>,
}

impl SchemaMeta {
    pub fn get_table(&self, name: &str) -> Option<&TableMeta> {
        self.tables.get(&name.to_lowercase())
    }
}

/// 全部逻辑库元数据。对应 `MetadataManager`。
#[derive(Default)]
pub struct Metadata {
    pub schemas: BTreeMap<String, SchemaMeta>,
    /// 原型（prototype）target 名
    pub prototype: String,
}

impl Metadata {
    /// 从运行时配置构建元数据。对应 `MetadataManager` 的表注册逻辑。
    pub fn build(cfg: &MycatRouterConfig) -> MetaResult<Metadata> {
        let mut meta = Metadata {
            schemas: BTreeMap::new(),
            prototype: cfg.prototype.clone(),
        };
        for (name, schema_cfg) in &cfg.schemas {
            meta.schemas
                .insert(name.to_lowercase(), build_schema(name, schema_cfg)?);
        }
        Ok(meta)
    }

    pub fn get_schema(&self, name: &str) -> Option<&SchemaMeta> {
        self.schemas.get(&name.to_lowercase())
    }

    pub fn schema_names(&self) -> Vec<&str> {
        self.schemas.keys().map(|s| s.as_str()).collect()
    }
}

fn build_schema(name: &str, cfg: &LogicSchemaConfig) -> MetaResult<SchemaMeta> {
    let mut tables = BTreeMap::new();
    for (tname, t) in &cfg.normal_tables {
        tables.insert(tname.to_lowercase(), build_normal(t, &cfg.target_name));
    }
    for (tname, t) in &cfg.global_tables {
        tables.insert(
            tname.to_lowercase(),
            build_global(t, &cfg.target_name, name, tname),
        );
    }
    for (tname, t) in &cfg.sharding_tables {
        tables.insert(tname.to_lowercase(), build_sharding(t)?);
    }
    for (tname, v) in &cfg.views {
        tables.insert(
            tname.to_lowercase(),
            TableMeta::View {
                create_sql: v.create_view_sql.clone(),
            },
        );
    }
    Ok(SchemaMeta {
        name: name.to_string(),
        target_name: cfg.target_name.clone(),
        tables,
    })
}

fn build_normal(cfg: &NormalTableConfig, default_target: &str) -> TableMeta {
    let locality = cfg.locality.clone().unwrap_or_default();
    let target = if locality.target_name.is_empty() {
        default_target.to_string()
    } else {
        locality.target_name
    };
    let schema = locality.schema_name.unwrap_or_default();
    let table = locality.table_name.unwrap_or_default();
    TableMeta::Normal {
        target,
        schema,
        table,
        create_sql: cfg.create_table_sql.clone(),
    }
}

fn build_global(
    cfg: &GlobalTableConfig,
    default_target: &str,
    logical_schema: &str,
    logical_table: &str,
) -> TableMeta {
    let target_schemas: Vec<(String, String)> = if cfg.broadcast.is_empty() {
        vec![(default_target.to_string(), logical_schema.to_string())]
    } else {
        cfg.broadcast
            .iter()
            .filter(|b| !b.target_name.is_empty())
            .map(|b| {
                (
                    b.target_name.clone(),
                    b.schema_name
                        .clone()
                        .unwrap_or_else(|| logical_schema.to_string()),
                )
            })
            .collect()
    };
    let targets: Vec<String> = target_schemas.iter().map(|(t, _)| t.clone()).collect();
    let schema = target_schemas
        .first()
        .map(|(_, s)| s.clone())
        .unwrap_or_else(|| logical_schema.to_string());
    let _ = logical_table;
    TableMeta::Global {
        targets,
        schema,
        target_schemas,
        create_sql: cfg.create_table_sql.clone(),
        sequence_type: cfg.sequence_type.clone(),
    }
}

fn build_sharding(cfg: &ShardingTableConfig) -> MetaResult<TableMeta> {
    // AutoFunction（name/clazz 均空）：节点由 mappingFormat 生成；否则由 partition 配置展开
    let nodes = if is_auto_function(&cfg.function) {
        expand_auto_nodes(&cfg.function.properties)?
    } else {
        expand_backend_nodes(&cfg.partition)
    };
    if nodes.is_empty() {
        return Err(MetaError::Config(
            "sharding table has no backend nodes (check partition/mappingFormat config)".into(),
        ));
    }
    let function = build_rule_function(&cfg.function, Arc::new(nodes.clone()))?;
    Ok(TableMeta::Sharding {
        nodes: Arc::new(nodes),
        function,
        create_sql: cfg.create_table_sql.clone(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use mycat3_config::default_config;

    #[test]
    fn test_build_default_metadata() {
        let cfg = default_config();
        let meta = Metadata::build(&cfg).unwrap();
        let s = meta.get_schema("db1").unwrap();
        assert_eq!(s.target_name, "c0");
        assert!(matches!(
            s.get_table("travelrecord").unwrap(),
            TableMeta::Normal { .. }
        ));
    }

    #[test]
    fn test_build_sharding_metadata() {
        let json = r#"{
          "schemaName": "db1",
          "targetName": "c0",
          "shardingTables": {
            "orders": {
              "partition": {"targetNames": ["c0"], "schemaNames": ["db1"], "tableNames": ["orders_$0-3"]},
              "function": {"clazz": "io.mycat.router.mycat1xfunction.PartitionByMod",
                            "properties": {"columnName": "id", "count": "4"}}
            }
          }
        }"#;
        let schema: LogicSchemaConfig = serde_json::from_str(json).unwrap();
        let meta_table = build_sharding(schema.sharding_tables.get("orders").unwrap()).unwrap();
        match meta_table {
            TableMeta::Sharding {
                nodes, function, ..
            } => {
                assert_eq!(nodes.len(), 4);
                assert_eq!(function.sharding_columns(), vec!["id".to_string()]);
            }
            _ => panic!("expected sharding table"),
        }
    }

    #[test]
    fn test_build_auto_sharding_metadata() {
        let json = r#"{
          "schemaName": "db1",
          "targetName": "c0",
          "shardingTables": {
            "t_order": {
              "function": {
                "properties": {
                  "dbNum": "1", "tableNum": "4", "storeNum": "1",
                  "mappingFormat": "c0/db1_${dbIndex}/t_order_${tableIndex}",
                  "dbMethod": "hash(id)", "tableMethod": "hash(id)"
                }
              }
            }
          }
        }"#;
        let schema: LogicSchemaConfig = serde_json::from_str(json).unwrap();
        let t = build_sharding(schema.sharding_tables.get("t_order").unwrap()).unwrap();
        match t {
            TableMeta::Sharding { nodes, .. } => {
                assert_eq!(nodes.len(), 4);
                assert_eq!(nodes[3].table_name, "t_order_3");
            }
            _ => panic!(),
        }
    }

    #[test]
    fn test_global_table() {
        let json = r#"{
          "schemaName": "db1", "targetName": "c0",
          "globalTables": {
            "dict": {"broadcast": [{"targetName": "c0"}, {"targetName": "c1"}]}
          }
        }"#;
        let schema: LogicSchemaConfig = serde_json::from_str(json).unwrap();
        let t = build_global(
            schema.global_tables.get("dict").unwrap(),
            "c0",
            "db1",
            "dict",
        );
        match t {
            TableMeta::Global {
                targets,
                target_schemas,
                ..
            } => {
                assert_eq!(targets, vec!["c0", "c1"]);
                // broadcast 无 schemaName → 回退逻辑库名
                assert_eq!(target_schemas[0], ("c0".to_string(), "db1".to_string()));
            }
            _ => panic!(),
        }
    }
}
