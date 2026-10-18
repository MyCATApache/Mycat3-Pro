//! Mycat3 Pro SQL 层：解析、分片路由与 SQL 重写。
//!
//! 对应 Mycat3 的 `MycatdbCommand` / `HackRouter` / `ShardingSQLHandler` /
//! `ValueIndexCondition` / `MysqlTableReplacer`，以 `sqlparser`（MySQL 方言）
//! 代替 Java 版的 Druid + Calcite：
//! - [`meta`]：逻辑库表元数据（`MetadataManager`）
//! - [`hint`]：`/*!mycat:...*/` 提示解析
//! - [`route`]：路由分析与物理 SQL 生成（`QueryBackendTask`）
//!
//! # Pro 版能力边界（相对 Java 版 Calcite 引擎）
//! - 支持：单分片表任意 CRUD 下推、分片键裁剪（= / IN / BETWEEN / 范围）、
//!   广播表读写扇出、单表跨分片 ORDER BY + LIMIT 归并、COUNT/SUM/MAX/MIN 简单聚合归并
//! - 不支持（返回明确错误）：多分片表 JOIN、跨分片 GROUP BY / AVG / 复杂子查询
//!   —— 这些在 Java 版由 Calcite 分布式执行引擎处理

pub mod cache_key;
pub mod hint;
pub mod meta;
pub mod oracle;
pub mod pg;
pub mod route;

pub use cache_key::{normalize_cache_query, normalize_cache_sql, NormalizedQuery};
pub use hint::{parse_hint, Hint};
pub use meta::{MetaError, Metadata, SchemaMeta, TableMeta};
pub use oracle::{translate_sql as translate_oracle_sql, DialectError};
pub use pg::translate_sql as translate_pg_sql;
pub use route::{
    analyse, parse_rename_table, BackendTask, LimitSpec, MergeOp, MergePlan, OrderKey, RouteResult,
    SqlError, SqlResult, StmtType,
};

#[cfg(test)]
mod tests {
    use mycat3_config::{LogicSchemaConfig, MycatRouterConfig};

    use super::*;

    /// 构造测试元数据：
    /// - db1.orders：4 节点分片表（PartitionByMod id % 4，c0 上 db1.orders_0..3）
    /// - db1.dict：广播表（c0、c1）
    /// - db1.user：单表（c0/db1/user）
    /// - db2.events：跨库分片（c0/c1 各 2 表）
    fn test_meta() -> Metadata {
        let schema_json = r#"{
          "schemaName": "db1",
          "targetName": "c0",
          "shardingTables": {
            "orders": {
              "partition": {"targetNames": ["c0"], "schemaNames": ["db1"], "tableNames": ["orders_$0-3"]},
              "function": {"clazz": "io.mycat.router.mycat1xfunction.PartitionByMod",
                            "properties": {"columnName": "id", "count": "4"}}
            }
          },
          "globalTables": {
            "dict": {"broadcast": [{"targetName": "c0"}, {"targetName": "c1"}]}
          },
          "normalTables": {
            "user": {"locality": {"targetName": "c0", "schemaName": "db1", "tableName": "user"}}
          }
        }"#;
        let schema2_json = r#"{
          "schemaName": "db2",
          "targetName": "c0",
          "shardingTables": {
            "events": {
              "partition": {"targetNames": ["c0","c1"], "schemaNames": ["db2"], "tableNames": ["events_$0-1"]},
              "function": {"clazz": "PartitionByMod",
                            "properties": {"columnName": "id", "count": "4"}}
            }
          }
        }"#;
        let s1: LogicSchemaConfig = serde_json::from_str(schema_json).unwrap();
        let s2: LogicSchemaConfig = serde_json::from_str(schema2_json).unwrap();
        let mut cfg = MycatRouterConfig::new();
        cfg.schemas.insert("db1".into(), s1);
        cfg.schemas.insert("db2".into(), s2);
        Metadata::build(&cfg).unwrap()
    }

    fn one(sql: &str, schema: &str) -> RouteResult {
        let meta = test_meta();
        let mut rs =
            analyse(sql, Some(schema), &meta).unwrap_or_else(|e| panic!("{} => {}", sql, e));
        assert_eq!(rs.len(), 1, "expected single statement");
        rs.pop().unwrap()
    }

    fn targets(r: &RouteResult) -> Vec<String> {
        r.tasks.iter().map(|t| t.target_name.clone()).collect()
    }

    // ---------- SELECT ----------

    #[test]
    fn test_select_sharding_equal() {
        let r = one("select * from orders where id = 5", "db1");
        assert_eq!(r.stmt_type, StmtType::Select);
        assert_eq!(r.tasks.len(), 1); // 5 % 4 = 1
        assert_eq!(r.tasks[0].target_name, "c0");
        assert!(
            r.tasks[0].sql.contains("db1.orders_1"),
            "{}",
            r.tasks[0].sql
        );
    }

    #[test]
    fn test_select_sharding_full_scan() {
        let r = one("select * from orders where name = 'x'", "db1");
        assert_eq!(r.tasks.len(), 4); // 无分片键 → 全节点
        for (i, t) in r.tasks.iter().enumerate() {
            assert!(t.sql.contains(&format!("orders_{}", i)), "{}", t.sql);
        }
        assert!(r.merge.is_some());
    }

    #[test]
    fn test_select_sharding_in() {
        let r = one("select * from orders where id in (1, 5, 9)", "db1");
        // 1%4=1, 5%4=1, 9%4=1 → 单节点
        assert_eq!(r.tasks.len(), 1);
        assert!(r.tasks[0].sql.contains("orders_1"));
    }

    #[test]
    fn test_select_sharding_in_multi_nodes() {
        let r = one("select * from orders where id in (0, 1)", "db1");
        assert_eq!(r.tasks.len(), 2);
    }

    #[test]
    fn test_select_sharding_range() {
        let r = one("select * from orders where id between 4 and 7", "db1");
        // mod 函数 RANGE 不可裁剪 → 全节点
        assert_eq!(r.tasks.len(), 4);
    }

    #[test]
    fn test_select_with_alias_and_qualifier() {
        let r = one("select o.name from orders o where o.id = 6", "db1");
        assert_eq!(r.tasks.len(), 1); // 6%4=2
        assert!(r.tasks[0].sql.contains("orders_2"));
    }

    #[test]
    fn test_select_or_full_scan() {
        let r = one("select * from orders where id = 4 or name = 'a'", "db1");
        assert_eq!(r.tasks.len(), 4); // OR → 放弃裁剪
    }

    #[test]
    fn test_select_schema_qualified() {
        let r = one("select * from db1.orders where id = 7", "db2");
        assert_eq!(r.tasks.len(), 1); // 7%4=3
        assert!(r.tasks[0].sql.contains("db1.orders_3"));
    }

    #[test]
    fn test_select_normal_table() {
        let r = one("select * from user", "db1");
        assert_eq!(r.tasks.len(), 1);
        assert_eq!(r.tasks[0].target_name, "c0");
        assert!(r.tasks[0].sql.contains("db1.user"));
    }

    #[test]
    fn test_select_global_table_read_first_target() {
        let r = one("select * from dict", "db1");
        assert_eq!(r.tasks.len(), 1); // 读走第一个广播节点
        assert_eq!(r.tasks[0].target_name, "c0");
    }

    #[test]
    fn test_select_no_table_prototype() {
        let r = one("select 1", "db1");
        assert_eq!(r.tasks[0].target_name, "prototype");
    }

    #[test]
    fn test_select_multi_sharding_join_error() {
        let err = analyse(
            "select * from orders o join db2.events e on o.id = e.id",
            Some("db1"),
            &test_meta(),
        );
        assert!(err.is_err(), "expected unsupported error");
    }

    #[test]
    fn test_select_order_limit_merge_plan() {
        let r = one(
            "select id, name from orders where name='x' order by id desc limit 10 offset 5",
            "db1",
        );
        assert_eq!(r.tasks.len(), 4);
        let merge = r.merge.unwrap();
        assert_eq!(merge.op, MergeOp::Concat);
        assert_eq!(
            merge.order_by,
            vec![OrderKey {
                index: Some(0),
                name: None,
                desc: true
            }]
        );
        assert_eq!(
            merge.limit,
            Some(LimitSpec {
                offset: 5,
                count: 10
            })
        );
        // 下推 SQL 已改写为 LIMIT 15（offset+count）
        for t in &r.tasks {
            assert!(t.sql.to_uppercase().contains("LIMIT 15"), "{}", t.sql);
        }
    }

    #[test]
    fn test_select_count_merge() {
        let r = one("select count(*) from orders", "db1");
        assert_eq!(r.tasks.len(), 4);
        let merge = r.merge.unwrap();
        assert_eq!(merge.op, MergeOp::CountSum);
    }

    #[test]
    fn test_select_group_by_unsupported() {
        let err = analyse(
            "select name, count(*) from orders group by name",
            Some("db1"),
            &test_meta(),
        );
        assert!(err.is_err());
    }

    #[test]
    fn test_cross_target_sharding() {
        let r = one("select * from events where id = 3", "db2");
        // 节点展开顺序：c0.events_0, c0.events_1, c1.events_0, c1.events_1 → 3%4=3 → c1.events_1
        assert_eq!(r.tasks.len(), 1);
        assert_eq!(r.tasks[0].target_name, "c1");
        assert!(r.tasks[0].sql.contains("events_1"));
    }

    // ---------- INSERT ----------

    #[test]
    fn test_insert_sharding_single_row() {
        let r = one("insert into orders (id, name) values (5, 'a')", "db1");
        assert_eq!(r.stmt_type, StmtType::Insert);
        assert_eq!(r.tasks.len(), 1);
        assert!(
            r.tasks[0].sql.contains("db1.orders_1"),
            "{}",
            r.tasks[0].sql
        );
        assert!(r.tasks[0].sql.contains("'a'"));
    }

    #[test]
    fn test_insert_sharding_multi_rows_split() {
        let r = one(
            "insert into orders (id, name) values (4, 'a'), (5, 'b'), (8, 'c')",
            "db1",
        );
        // 4%4=0, 8%4=0 → orders_0 两行；5%4=1 → orders_1 一行
        assert_eq!(r.tasks.len(), 2);
        let t0 = r.tasks.iter().find(|t| t.sql.contains("orders_0")).unwrap();
        assert!(t0.sql.contains("'a'") && t0.sql.contains("'c'"));
        let t1 = r.tasks.iter().find(|t| t.sql.contains("orders_1")).unwrap();
        assert!(t1.sql.contains("'b'"));
    }

    #[test]
    fn test_insert_missing_shard_column() {
        let err = analyse(
            "insert into orders (name) values ('a')",
            Some("db1"),
            &test_meta(),
        );
        assert!(err.is_err());
    }

    #[test]
    fn test_insert_global_broadcast() {
        let r = one("insert into dict (id, name) values (1, 'x')", "db1");
        assert_eq!(r.tasks.len(), 2);
        assert_eq!(targets(&r), vec!["c0".to_string(), "c1".to_string()]);
    }

    #[test]
    fn test_insert_normal() {
        let r = one("insert into user (id) values (1)", "db1");
        assert_eq!(r.tasks.len(), 1);
        assert!(r.tasks[0].sql.contains("db1.user"));
    }

    // ---------- UPDATE / DELETE ----------

    #[test]
    fn test_update_sharding_pruned() {
        let r = one("update orders set name = 'b' where id = 9", "db1");
        assert_eq!(r.stmt_type, StmtType::Update);
        assert_eq!(r.tasks.len(), 1); // 9%4=1
        assert!(r.tasks[0].sql.contains("orders_1"));
    }

    #[test]
    fn test_update_sharding_full_scan() {
        let r = one("update orders set name = 'b' where amount > 10", "db1");
        assert_eq!(r.tasks.len(), 4);
    }

    #[test]
    fn test_delete_sharding_pruned() {
        let r = one("delete from orders where id = 2", "db1");
        assert_eq!(r.stmt_type, StmtType::Delete);
        assert_eq!(r.tasks.len(), 1); // 2%4=2
        assert!(r.tasks[0].sql.contains("orders_2"));
    }

    #[test]
    fn test_update_global_broadcast() {
        let r = one("update dict set name = 'y' where id = 1", "db1");
        assert_eq!(r.tasks.len(), 2);
    }

    // ---------- DDL ----------

    #[test]
    fn test_create_table_fanout_sharding() {
        let r = one(
            "create table orders (id bigint primary key, name varchar(32))",
            "db1",
        );
        assert_eq!(r.stmt_type, StmtType::Ddl);
        assert_eq!(r.tasks.len(), 4);
        assert!(r.tasks[0].sql.contains("orders_0"));
    }

    #[test]
    fn test_drop_table_fanout() {
        let r = one("drop table if exists orders", "db1");
        assert_eq!(r.tasks.len(), 4);
    }

    #[test]
    fn test_truncate_normal() {
        let r = one("truncate table user", "db1");
        assert_eq!(r.stmt_type, StmtType::Truncate);
        assert_eq!(r.tasks.len(), 1);
    }

    // ---------- 控制语句 ----------

    #[test]
    fn test_use_schema() {
        let r = one("use db2", "db1");
        assert_eq!(r.stmt_type, StmtType::Use);
        assert_eq!(r.use_schema.as_deref(), Some("db2"));
        assert!(r.tasks.is_empty());
    }

    #[test]
    fn test_set_local() {
        let r = one("set autocommit = 0", "db1");
        assert_eq!(r.stmt_type, StmtType::Set);
        assert!(r.tasks.is_empty());
    }

    #[test]
    fn test_tx_statements() {
        assert_eq!(one("begin", "db1").stmt_type, StmtType::TxBegin);
        assert_eq!(one("commit", "db1").stmt_type, StmtType::TxCommit);
        assert_eq!(one("rollback", "db1").stmt_type, StmtType::TxRollback);
    }

    #[test]
    fn test_show_local() {
        assert_eq!(one("show tables", "db1").stmt_type, StmtType::Show);
    }

    #[test]
    fn test_explain() {
        let r = one("explain select * from orders where id = 5", "db1");
        assert_eq!(r.stmt_type, StmtType::Explain);
        assert_eq!(r.explain_rows.len(), 1);
        assert_eq!(r.explain_rows[0][0], "c0");
        assert!(r.explain_rows[0][1].contains("orders_1"));
    }

    #[test]
    fn test_multi_statement() {
        let meta = test_meta();
        let rs = analyse(
            "select * from orders where id=1; select * from orders where id=2",
            Some("db1"),
            &meta,
        )
        .unwrap();
        assert_eq!(rs.len(), 2);
        assert!(rs[0].tasks[0].sql.contains("orders_1"));
        assert!(rs[1].tasks[0].sql.contains("orders_2"));
    }

    // ---------- hint ----------

    #[test]
    fn test_hint_target_passthrough() {
        let r = one("/*!mycat:target=c1*/ select * from orders", "db1");
        assert_eq!(r.tasks.len(), 1);
        assert_eq!(r.tasks[0].target_name, "c1");
        assert_eq!(r.tasks[0].sql, "select * from orders"); // 不重写
        assert!(r.hint.is_some());
    }

    #[test]
    fn test_hint_master_flag() {
        let r = one("/*!mycat:master*/ select * from orders where id=1", "db1");
        assert!(r.hint.unwrap().master);
        assert_eq!(r.tasks.len(), 1);
    }

    #[test]
    fn test_unknown_db_error() {
        let err = analyse("select * from t", Some("nonexistent"), &test_meta());
        assert!(err.is_err());
    }

    #[test]
    fn test_parse_error() {
        let err = analyse("selct * from", Some("db1"), &test_meta());
        assert!(matches!(err, Err(SqlError::Parse(_))));
    }

    // ---------- 索引/DDL 扇出 ----------

    #[test]
    fn test_create_index_fanout() {
        let r = one("CREATE INDEX idx_name ON orders (name)", "db1");
        assert_eq!(r.stmt_type, StmtType::Ddl);
        // 4 个物理节点全部扇出，表名重写为物理名，索引名保持逻辑名
        //（MySQL 不支持 IF NOT EXISTS 索引语法，不自动补）
        assert_eq!(r.tasks.len(), 4);
        for (i, t) in r.tasks.iter().enumerate() {
            assert_eq!(t.target_name, "c0");
            assert!(t.sql.to_uppercase().contains("CREATE INDEX"), "{}", t.sql);
            assert!(
                t.sql.contains(&format!("ON `db1`.`orders_{}`", i))
                    || t.sql.contains(&format!("ON db1.orders_{}", i)),
                "{}",
                t.sql
            );
            assert!(t.sql.contains("idx_name"), "{}", t.sql);
        }
    }

    #[test]
    fn test_drop_index_fanout() {
        let r = one("DROP INDEX idx_name ON orders", "db1");
        assert_eq!(r.stmt_type, StmtType::Ddl);
        assert_eq!(r.tasks.len(), 4);
        for (i, t) in r.tasks.iter().enumerate() {
            assert!(
                t.sql.contains(&format!("ON `db1`.`orders_{}`", i))
                    || t.sql.contains(&format!("ON db1.orders_{}", i)),
                "{}",
                t.sql
            );
            assert!(t.sql.contains("idx_name"), "{}", t.sql);
        }
        // 单名形式（标准 SQL）透传默认 target，保持旧行为
        let r = one("DROP INDEX idx_name", "db1");
        assert_eq!(r.tasks.len(), 1);
        assert_eq!(r.tasks[0].target_name, "c0");
    }

    #[test]
    fn test_parse_rename_table() {
        assert_eq!(
            parse_rename_table("rename table a to b").unwrap(),
            vec![("a".to_string(), "b".to_string())]
        );
        assert_eq!(
            parse_rename_table("RENAME TABLE `db1`.`t1` TO `db1`.`t2`, x TO y;").unwrap(),
            vec![
                ("db1.t1".to_string(), "db1.t2".to_string()),
                ("x".to_string(), "y".to_string())
            ]
        );
        assert_eq!(parse_rename_table("select 1"), None);
        assert_eq!(parse_rename_table("rename table a to"), None);
    }

    #[test]
    fn test_check_table_fanout() {
        let rs = analyse("check table orders", Some("db1"), &test_meta()).unwrap();
        let r = &rs[0];
        assert_eq!(r.tasks.len(), 4, "分片表扇出 4 节点");
        for (i, t) in r.tasks.iter().enumerate() {
            assert!(
                t.sql.contains(&format!("CHECK TABLE `db1`.`orders_{}`", i)),
                "{}",
                t.sql
            );
        }
        // 未注册表：默认 target 原样
        let rs = analyse("check table unknown_t", Some("db1"), &test_meta()).unwrap();
        assert_eq!(rs[0].tasks.len(), 1);
        assert_eq!(rs[0].tasks[0].target_name, "c0");
        // 解析器
        let v = parse_rename_table("rename table a to b").unwrap();
        assert_eq!(v.len(), 1);
    }
}
