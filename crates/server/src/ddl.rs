//! DDL 配置自动化：CREATE/DROP DATABASE、CREATE/DROP TABLE（含 DRDS 分片语法）、
//! CREATE/DROP USER、CREATE/DROP SEQUENCE、配置热重载。
//!
//! 对应 Java 版 `CreateDatabaseSQLHandler` / `CreateTableSQLHandler` /
//! `DropTableSQLHandler` / `MycatRouterConfigOps`（putShardingTable/putHashTable/
//! putGlobalTable/putNormalTable/getAutoHashProperties）/ `CreateTableUtils`
//! （物理库表创建）/ `HintHandler.loadConfigFromFile`（配置重载）/
//! `createUser`/`setSequence` hint。
//!
//! 支持的 DDL 语法：
//! ```sql
//! CREATE DATABASE db2;                                   -- targetName=prototype
//! CREATE DATABASE db2 /*!mycat:{"targetName":"c0"}*/;    -- 指定默认 target
//! CREATE TABLE t (...) ;                                 -- 单表（locality=库 target）
//! CREATE TABLE t (...) BROADCAST;                        -- 广播表（所有 c* 集群）
//! CREATE TABLE t (...) DBPARTITION BY hash(id) DBPARTITIONS 2
//!     TBPARTITION BY hash(id) TBPARTITIONS 4;            -- DRDS 风格 AutoFunction 分片
//! /*!+mycat:createTable{"type":"hash","dbNum":"2",...}*/ CREATE TABLE t (...);
//! DROP TABLE t;  DROP DATABASE db2;
//! CREATE USER u IDENTIFIED BY 'pw';  DROP USER u;
//! CREATE SEQUENCE s INCREMENT BY 5 START WITH 100 [SIMPLE|TIME];
//! /*!mycat:loadConfigFromFile*/                            -- 配置热重载（Java 兼容 hint）
//! ```

use std::collections::BTreeMap;

use mycat3_client::QueryOutcome;
use mycat3_config::{
    ClusterConfig, DatasourceConfig, FileConfigStorage, GlobalTableConfig, LocalityConfig,
    LogicSchemaConfig, MycatRouterConfig, NormalTableConfig, SequenceConfig,
    ShardingFunctionConfig, ShardingTableConfig, SqlCacheConfig, UserConfig,
};
use mycat3_sql::{Metadata, TableMeta};

use crate::engine::{EngineError, EngineOutcome};
use crate::state::ServerState;

/// 判断 SQL 是否为本模块处理的 DDL/管理语句。
pub fn is_config_ddl(sql: &str) -> bool {
    let n = normalize_head(sql);
    n.starts_with("create database")
        || n.starts_with("drop database")
        || n.starts_with("create table")
        || n.starts_with("drop table")
        || n.starts_with("create user")
        || n.starts_with("drop user")
        || n.starts_with("create sequence")
        || n.starts_with("drop sequence")
        || n.starts_with("rename table")
        || n.starts_with("create view")
        || n.starts_with("drop view")
        || n.contains("loadconfigfromfile")
        || n.starts_with("show users")
}

fn normalize_head(sql: &str) -> String {
    sql.trim()
        .to_lowercase()
        .replace('\n', " ")
        .replace('\r', " ")
}

/// 处理配置类 DDL。返回 Ok(outcome) 表示已处理；调用方对非 DDL 不应进入这里。
pub async fn handle_config_ddl(
    state: &std::sync::Arc<ServerState>,
    schema_ctx: Option<&str>,
    sql: &str,
) -> Result<EngineOutcome, EngineError> {
    let norm = normalize_head(sql);
    if norm.contains("loadconfigfromfile") {
        return reload_from_file(state).await;
    }
    if norm.starts_with("show users") {
        return Ok(show_users(state));
    }
    if norm.starts_with("create database") {
        return create_database(state, sql).await;
    }
    if norm.starts_with("drop database") {
        return drop_database(state, sql).await;
    }
    if norm.starts_with("create table") {
        return create_table(state, schema_ctx, sql).await;
    }
    if norm.starts_with("drop table") {
        return drop_table(state, schema_ctx, sql).await;
    }
    if norm.starts_with("rename table") {
        return rename_table(state, schema_ctx, sql).await;
    }
    if norm.starts_with("create view") {
        return create_view(state, schema_ctx, sql).await;
    }
    if norm.starts_with("drop view") {
        return drop_view(state, schema_ctx, sql).await;
    }
    if norm.starts_with("create user") {
        return create_user(state, sql);
    }
    if norm.starts_with("drop user") {
        return drop_user(state, sql);
    }
    if norm.starts_with("create sequence") || norm.starts_with("drop sequence") {
        return manage_sequence(state, sql);
    }
    Err(EngineError::general("unhandled config ddl"))
}

// ---------------------------------------------------------------------------
// 配置更新与持久化（对应 MycatRouterConfigOps.commit + persistence）
// ---------------------------------------------------------------------------

/// 修改配置 → 持久化（若有 home）→ 热重载（metadata/replica/heartbeat/sequences）。
fn commit_config(
    state: &std::sync::Arc<ServerState>,
    mutate: impl FnOnce(&mut MycatRouterConfig) -> Result<(), EngineError>,
) -> Result<(), EngineError> {
    let mut cfg = (*state.config()).clone();
    mutate(&mut cfg)?;
    // 先验证元数据可编译（失败则不落盘，保持旧配置 —— 对应 commit 失败 giveup）
    Metadata::build(&cfg).map_err(|e| EngineError::general(format!("config invalid: {}", e)))?;
    if let Some(home) = state.home() {
        // 变更前快照备份（最近 10 份，restore 用）
        let _ = backup_config_snapshot(&home, &state.config());
        FileConfigStorage::new(home)
            .save_all_sync(&cfg)
            .map_err(|e| EngineError::general(format!("persist config failed: {}", e)))?;
    }
    state
        .reload(cfg, Some(std::time::Duration::from_secs(5)))
        .map_err(|e| EngineError::general(format!("reload failed: {}", e)))
}

/// `/*!mycat:loadConfigFromFile*/`：从磁盘重载（对应 ConfigUpdater.loadConfigFromFile）。
pub async fn reload_from_file(
    state: &std::sync::Arc<ServerState>,
) -> Result<EngineOutcome, EngineError> {
    let home = state
        .home()
        .ok_or_else(|| EngineError::general("config home not set (started without --home?)"))?;
    let cfg = FileConfigStorage::new(&home)
        .load_all()
        .map_err(|e| EngineError::general(format!("load config failed: {}", e)))?;
    state
        .reload(cfg, Some(std::time::Duration::from_secs(5)))
        .map_err(|e| EngineError::general(format!("reload failed: {}", e)))?;
    tracing::info!(home = %home.display(), "config reloaded from file");
    Ok(EngineOutcome::Ok {
        affected_rows: 0,
        last_insert_id: 0,
    })
}

// ---------------------------------------------------------------------------
// CREATE / DROP DATABASE
// ---------------------------------------------------------------------------

/// `CREATE DATABASE db [IF NOT EXISTS] [/*!mycat:{"targetName":"c0"}*/]`
pub async fn create_database(
    state: &std::sync::Arc<ServerState>,
    sql: &str,
) -> Result<EngineOutcome, EngineError> {
    let (name, hint_json) = parse_create_database(sql)?;
    let target = hint_json
        .and_then(|j| {
            j.get("targetName")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string())
        })
        .unwrap_or_else(|| "prototype".to_string());
    let name_lc = name.to_lowercase();

    if state.metadata().get_schema(&name_lc).is_some()
        && !sql.to_lowercase().contains("if not exists")
    {
        return Err(EngineError::new(
            1007,
            "HY000",
            format!("Can't create database '{}'; database exists", name),
        ));
    }

    let schema_cfg = LogicSchemaConfig {
        schema_name: name.clone(),
        target_name: target.clone(),
        ..Default::default()
    };
    commit_config(state, |cfg| {
        cfg.schemas.insert(name_lc.clone(), schema_cfg);
        Ok(())
    })?;

    // 物理库：在 prototype 上创建（与 Java 一致；分片物理库由 CREATE TABLE 惰性创建）
    let physical = format!("CREATE DATABASE IF NOT EXISTS `{}` DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci", name);
    let _ = execute_on_prototype(state, &physical).await; // 错误吞掉（Java 同样 log-and-swallow）

    Ok(EngineOutcome::Ok {
        affected_rows: 1,
        last_insert_id: 0,
    })
}

fn parse_create_database(sql: &str) -> Result<(String, Option<serde_json::Value>), EngineError> {
    // 提取 hint JSON：/*!mycat:{...}*/
    let hint_json = extract_mycat_json_hint(sql);
    let cleaned = remove_comments(sql);
    let tokens = tokenize_head(&cleaned, 6);
    // CREATE DATABASE [IF NOT EXISTS] name
    let i = skip_if_exists(&tokens, 2);
    let name = tokens
        .get(i)
        .ok_or_else(|| EngineError::general("CREATE DATABASE requires a name"))?;
    Ok((name.trim_matches('`').to_string(), hint_json))
}

/// `DROP DATABASE [IF EXISTS] name`
pub async fn drop_database(
    state: &std::sync::Arc<ServerState>,
    sql: &str,
) -> Result<EngineOutcome, EngineError> {
    let cleaned = remove_comments(sql);
    let tokens = tokenize_head(&cleaned, 6);
    let i = skip_if_exists(&tokens, 2);
    let name = tokens
        .get(i)
        .ok_or_else(|| EngineError::general("DROP DATABASE requires a name"))?
        .trim_matches('`')
        .to_lowercase();
    let name2 = name.clone();
    commit_config(state, move |cfg| {
        cfg.schemas.remove(&name2);
        Ok(())
    })?;
    let physical = format!("DROP DATABASE IF EXISTS `{}`", name);
    let _ = execute_on_prototype(state, &physical).await;
    Ok(EngineOutcome::Ok {
        affected_rows: 0,
        last_insert_id: 0,
    })
}

// ---------------------------------------------------------------------------
// CREATE TABLE
// ---------------------------------------------------------------------------

/// DRDS 分片子句解析结果。
#[derive(Debug, Default, Clone, PartialEq)]
pub struct DrdsPartition {
    pub db_method: Option<String>,
    pub db_partitions: Option<i64>,
    pub tb_method: Option<String>,
    pub tb_partitions: Option<i64>,
    pub broadcast: bool,
}

/// 在顶层（括号深度 0、引号外）切掉 DRDS 后缀子句。
/// 返回 (主体 DDL, 分片子句)。对应 Java 由 druid patch 解析 dbpartition/tbpartition。
pub fn split_drds_suffix(sql: &str) -> (String, DrdsPartition) {
    let bytes = sql.as_bytes();
    let mut depth = 0i32;
    let mut i = 0usize;
    let mut cut: Option<usize> = None;
    let mut part = DrdsPartition::default();
    while i < bytes.len() {
        let c = bytes[i];
        match c {
            b'\'' | b'"' | b'`' => {
                let q = c;
                i += 1;
                while i < bytes.len() {
                    if bytes[i] == b'\\' {
                        i += 2;
                        continue;
                    }
                    if bytes[i] == q {
                        break;
                    }
                    i += 1;
                }
            }
            b'(' => depth += 1,
            b')' => depth -= 1,
            b'-' if i + 1 < bytes.len() && bytes[i + 1] == b'-' => {
                while i < bytes.len() && bytes[i] != b'\n' {
                    i += 1;
                }
                continue;
            }
            b'/' if i + 1 < bytes.len() && bytes[i + 1] == b'*' => {
                // 注释（含 /*!mycat:...*/ hint）跳过，不作为切点
                i += 2;
                while i + 1 < bytes.len() && !(bytes[i] == b'*' && bytes[i + 1] == b'/') {
                    i += 1;
                }
                i += 1;
            }
            _ if depth == 0 && c.is_ascii_alphabetic() => {
                let word = read_word(sql, i);
                let lower = word.to_lowercase();
                if lower == "dbpartition" || lower == "tbpartition" || lower == "broadcast" {
                    if cut.is_none() {
                        cut = Some(i);
                    }
                }
            }
            _ => {}
        }
        i += 1;
    }
    match cut {
        None => (sql.trim().trim_end_matches(';').to_string(), part),
        Some(pos) => {
            let body = sql[..pos].trim().trim_end_matches(';').to_string();
            let suffix = &sql[pos..];
            part = parse_drds_suffix(suffix);
            (body, part)
        }
    }
}

fn read_word(s: &str, start: usize) -> String {
    let mut end = start;
    let bytes = s.as_bytes();
    while end < bytes.len() && (bytes[end].is_ascii_alphanumeric() || bytes[end] == b'_') {
        end += 1;
    }
    s[start..end].to_string()
}

/// 解析后缀：`dbpartition by <expr> [dbpartitions N] [tbpartition by <expr> [tbpartitions N]] [broadcast]`
pub fn parse_drds_suffix(suffix: &str) -> DrdsPartition {
    let mut part = DrdsPartition::default();
    let lower = suffix.to_lowercase();
    let words: Vec<String> = suffix
        .split(|c: char| c.is_whitespace() || c == ',')
        .map(|s| s.to_string())
        .filter(|s| !s.is_empty())
        .collect();
    let mut i = 0;
    while i < words.len() {
        let w = words[i].to_lowercase();
        match w.as_str() {
            "broadcast" => part.broadcast = true,
            "dbpartition" => {
                // by <expr...> 直到 dbpartitions/tbpartition/broadcast/结尾
                i += 1;
                if i < words.len() && words[i].eq_ignore_ascii_case("by") {
                    i += 1;
                }
                let mut expr = Vec::new();
                while i < words.len() {
                    let lw = words[i].to_lowercase();
                    if lw == "dbpartitions"
                        || lw == "tbpartition"
                        || lw == "tbpartitions"
                        || lw == "broadcast"
                    {
                        break;
                    }
                    expr.push(words[i].clone());
                    i += 1;
                }
                part.db_method = Some(expr.join(""));
                continue;
            }
            "tbpartition" => {
                i += 1;
                if i < words.len() && words[i].eq_ignore_ascii_case("by") {
                    i += 1;
                }
                let mut expr = Vec::new();
                while i < words.len() {
                    let lw = words[i].to_lowercase();
                    if lw == "tbpartitions"
                        || lw == "dbpartition"
                        || lw == "dbpartitions"
                        || lw == "broadcast"
                    {
                        break;
                    }
                    expr.push(words[i].clone());
                    i += 1;
                }
                part.tb_method = Some(expr.join(""));
                continue;
            }
            "dbpartitions" => {
                if i + 1 < words.len() {
                    part.db_partitions = words[i + 1].trim_end_matches(';').parse().ok();
                    i += 2;
                    continue;
                }
            }
            "tbpartitions" => {
                if i + 1 < words.len() {
                    part.tb_partitions = words[i + 1].trim_end_matches(';').parse().ok();
                    i += 2;
                    continue;
                }
            }
            _ => {}
        }
        i += 1;
    }
    let _ = lower;
    part
}

/// `CREATE TABLE` 主流程。
pub async fn create_table(
    state: &std::sync::Arc<ServerState>,
    schema_ctx: Option<&str>,
    sql: &str,
) -> Result<EngineOutcome, EngineError> {
    // 1. hint：/*!+mycat:createTable{...}*/
    let hint = extract_create_table_hint(sql);
    // 2. 切 DRDS 后缀
    let without_hint = if hint.is_some() {
        remove_head_hint(sql)
    } else {
        sql.to_string()
    };
    let (body, drds) = split_drds_suffix(&without_hint);

    // 3. 解析表名（sqlparser 解析主体 DDL）
    let stmt = sqlparser::parser::Parser::parse_sql(&sqlparser::dialect::MySqlDialect {}, &body)
        .map_err(|e| EngineError::new(1064, "42000", format!("DDL parse error: {}", e)))?
        .into_iter()
        .next()
        .ok_or_else(|| EngineError::general("empty DDL"))?;
    let (schema_name, table_name) = match &stmt {
        sqlparser::ast::Statement::CreateTable(ct) => {
            let parts: Vec<String> = ct
                .name
                .0
                .iter()
                .map(|i| i.value.trim_matches('`').to_string())
                .collect();
            match parts.len() {
                1 => (
                    schema_ctx
                        .ok_or_else(|| EngineError::new(1046, "3D000", "No database selected"))?
                        .to_string(),
                    parts[0].clone(),
                ),
                2 => (parts[0].clone(), parts[1].clone()),
                _ => return Err(EngineError::general("unsupported table name in DDL")),
            }
        }
        _ => return Err(EngineError::general("not a CREATE TABLE statement")),
    };
    let schema_lc = schema_name.to_lowercase();
    let table_lc = table_name.to_lowercase();

    // 逻辑库必须已存在（Java: 用会话 schema 或 AST schema；库不存在直接失败）
    let schema_default_target = {
        let meta = state.metadata();
        let s = meta.get_schema(&schema_lc).ok_or_else(|| {
            EngineError::new(
                1049,
                "42000",
                format!(
                    "Unknown database '{}' (先执行 CREATE DATABASE)",
                    schema_name
                ),
            )
        })?;
        s.target_name.clone()
    };

    // 4. 生成表配置
    let full_logical_ddl = body.clone() + &rebuild_drds_suffix(&drds);
    let c_clusters = c_cluster_names(state);
    let table_cfg = if let Some(h) = &hint {
        build_from_hint(h, &body, &c_clusters)?
    } else if drds.broadcast {
        // 广播表：广播到全部 c* 集群（对应 getGlobalTableConfig）
        let targets = if c_clusters.is_empty() {
            vec![schema_default_target.clone()]
        } else {
            c_clusters
        };
        TableCfg::Global(GlobalTableConfig {
            create_table_sql: Some(full_logical_ddl.clone()),
            broadcast: targets
                .iter()
                .map(|t| LocalityConfig {
                    target_name: t.clone(),
                    schema_name: Some(schema_name.clone()),
                    table_name: Some(table_name.clone()),
                })
                .collect(),
            sequence_type: Some("NO_SEQUENCE".into()),
        })
    } else if drds.db_method.is_some() || drds.tb_method.is_some() {
        // DRDS 分片 → AutoFunction properties（对应 getAutoHashProperties）
        let props = auto_hash_properties(&schema_name, &table_name, &drds, &c_clusters)?;
        // 验证方法表达式可解析
        validate_auto_props(&props)?;
        TableCfg::Sharding(ShardingTableConfig {
            partition: Default::default(),
            function: ShardingFunctionConfig {
                name: None,
                clazz: None,
                properties: props,
                ranges: BTreeMap::new(),
            },
            create_table_sql: Some(full_logical_ddl.clone()),
            auto_increment: None,
        })
    } else {
        // 普通单表：locality = 库默认 target（对应 putNormalTable）
        TableCfg::Normal(NormalTableConfig {
            create_table_sql: Some(full_logical_ddl.clone()),
            locality: Some(LocalityConfig {
                target_name: schema_default_target.clone(),
                schema_name: Some(schema_name.clone()),
                table_name: Some(table_name.clone()),
            }),
        })
    };

    // 5. 计算物理节点（用于建物理表）
    let nodes = physical_nodes_of(&table_cfg)?;

    // 6. 写配置 + 热更新
    let (schema_lc2, table_lc2) = (schema_lc.clone(), table_lc.clone());
    commit_config(state, move |cfg| {
        let schema = cfg.schemas.get_mut(&schema_lc2).ok_or_else(|| {
            EngineError::general(format!("schema '{}' disappeared during DDL", schema_lc2))
        })?;
        remove_table_everywhere(schema, &table_lc2);
        match table_cfg {
            TableCfg::Normal(t) => {
                schema.normal_tables.insert(table_lc2, t);
            }
            TableCfg::Global(t) => {
                schema.global_tables.insert(table_lc2, t);
            }
            TableCfg::Sharding(t) => {
                schema.sharding_tables.insert(table_lc2, t);
            }
        }
        Ok(())
    })?;

    // 7. 物理建库建表（对应 CreateTableUtils.createPhysicalTable；错误 log-and-continue）
    let ddl_template = normalize_create_table(&body);
    let mut created = 0;
    for node in &nodes {
        match create_physical_node(state, node, &ddl_template).await {
            Ok(()) => created += 1,
            Err(e) => tracing::warn!(node = %node.unique_name(), "physical create failed: {}", e),
        }
    }
    tracing::info!(
        schema = %schema_name, table = %table_name, nodes = nodes.len(), created,
        "table created (config + physical)"
    );
    Ok(EngineOutcome::Ok {
        affected_rows: 0,
        last_insert_id: 0,
    })
}

enum TableCfg {
    Normal(NormalTableConfig),
    Global(GlobalTableConfig),
    Sharding(ShardingTableConfig),
}

fn remove_table_everywhere(schema: &mut LogicSchemaConfig, table: &str) {
    schema.normal_tables.remove(table);
    schema.global_tables.remove(table);
    schema.sharding_tables.remove(table);
    schema.views.remove(table);
}

/// c* 前缀集群名列表（对应 MetadataManager.getDefaultStoreNodeNum 的 "c" 前缀约定）。
fn c_cluster_names(state: &std::sync::Arc<ServerState>) -> Vec<String> {
    state
        .config()
        .clusters
        .keys()
        .filter(|k| k.starts_with('c'))
        .cloned()
        .collect()
}

/// 生成 AutoFunction properties。对应 `MycatRouterConfigOps.getAutoHashProperties` +
/// `AutoFunctionFactory` 默认 mappingFormat。
fn auto_hash_properties(
    schema: &str,
    table: &str,
    drds: &DrdsPartition,
    c_clusters: &[String],
) -> Result<BTreeMap<String, String>, EngineError> {
    let store_num = c_clusters.len().max(1) as i64;
    let db_method = drds.db_method.clone().or_else(|| drds.tb_method.clone());
    let tb_method = drds.tb_method.clone().or_else(|| drds.db_method.clone());
    let db_num = drds.db_partitions.unwrap_or(if drds.db_method.is_some() {
        store_num
    } else {
        1
    });
    let tb_num = drds.tb_partitions.unwrap_or(if drds.tb_method.is_some() {
        store_num
    } else {
        1
    });
    if db_num < 1 || tb_num < 1 {
        return Err(EngineError::general(
            "dbpartitions/tbpartitions must be >= 1",
        ));
    }
    // 扁平映射条件（与 AutoFunctionFactory 一致：db/table 方法相同且属 flatten 家族）
    let flatten = db_method == tb_method
        && db_method
            .as_deref()
            .map(|m| {
                let n = m.split('(').next().unwrap_or("").to_uppercase();
                [
                    "MOD_HASH",
                    "HASH",
                    "UNI_HASH",
                    "RIGHT_SHIFT",
                    "YYYYMM",
                    "YYYYDD",
                    "YYYYWEEK",
                ]
                .contains(&n.as_str())
            })
            .unwrap_or(false);
    // 默认 mappingFormat：c${targetIndex}/schema[_${dbIndex}]/table_${index|tableIndex}
    // （format! 中 ${{x}} 转义输出字面量 ${x}）
    let schema_part = if db_num >= 2 {
        format!("{}_${{dbIndex}}", schema)
    } else {
        schema.to_string()
    };
    let table_part = if flatten {
        format!("{}_${{index}}", table)
    } else {
        format!("{}_${{tableIndex}}", table)
    };
    let mapping = format!("c${{targetIndex}}/{}/{}", schema_part, table_part);
    let mut props = BTreeMap::new();
    props.insert("storeNum".into(), store_num.to_string());
    props.insert("dbNum".into(), db_num.to_string());
    props.insert("tableNum".into(), tb_num.to_string());
    if let Some(m) = &db_method {
        props.insert("dbMethod".into(), m.clone());
    }
    if let Some(m) = &tb_method {
        props.insert("tableMethod".into(), m.clone());
    }
    props.insert("mappingFormat".into(), mapping);
    Ok(props)
}

fn validate_auto_props(props: &BTreeMap<String, String>) -> Result<(), EngineError> {
    let nodes = mycat3_router::expand_auto_nodes(props)
        .map_err(|e| EngineError::general(format!("invalid partition methods: {}", e)))?;
    if nodes.is_empty() {
        return Err(EngineError::general(
            "partition expansion produced no nodes",
        ));
    }
    Ok(())
}

/// hint JSON → 表配置（对应 mycatDmlHandler createTable 的 type 分派）。
fn build_from_hint(
    hint: &serde_json::Value,
    body: &str,
    c_clusters: &[String],
) -> Result<TableCfg, EngineError> {
    let htype = hint
        .get("type")
        .and_then(|v| v.as_str())
        .unwrap_or("normal");
    match htype {
        "normal" => {
            let target = hint
                .get("targetName")
                .and_then(|v| v.as_str())
                .unwrap_or("prototype")
                .to_string();
            Ok(TableCfg::Normal(NormalTableConfig {
                create_table_sql: Some(body.to_string()),
                locality: Some(LocalityConfig {
                    target_name: target,
                    schema_name: None,
                    table_name: None,
                }),
            }))
        }
        "global" => {
            let targets: Vec<String> = hint
                .get("targets")
                .and_then(|v| v.as_array())
                .map(|a| {
                    a.iter()
                        .filter_map(|x| x.as_str().map(String::from))
                        .collect()
                })
                .unwrap_or_else(|| c_clusters.to_vec());
            Ok(TableCfg::Global(GlobalTableConfig {
                create_table_sql: Some(body.to_string()),
                broadcast: targets
                    .iter()
                    .map(|t| LocalityConfig {
                        target_name: t.clone(),
                        schema_name: None,
                        table_name: None,
                    })
                    .collect(),
                sequence_type: hint
                    .get("sequenceType")
                    .and_then(|v| v.as_str())
                    .map(String::from),
            }))
        }
        "hash" => {
            // hint 本身即 function.properties
            let mut props = BTreeMap::new();
            if let Some(obj) = hint.as_object() {
                for (k, v) in obj {
                    if k == "type" {
                        continue;
                    }
                    if let Some(s) = v.as_str() {
                        props.insert(k.clone(), s.to_string());
                    }
                }
            }
            validate_auto_props(&props)?;
            Ok(TableCfg::Sharding(ShardingTableConfig {
                partition: Default::default(),
                function: ShardingFunctionConfig {
                    name: None,
                    clazz: None,
                    properties: props,
                    ranges: BTreeMap::new(),
                },
                create_table_sql: Some(body.to_string()),
                auto_increment: None,
            }))
        }
        "range" => {
            // ranges + partition{targetNames,schemaNames,tableNames} + properties/name/clazz
            let cfg: ShardingTableConfig = serde_json::from_value(hint.clone())
                .map_err(|e| EngineError::general(format!("bad range table hint: {}", e)))?;
            let mut cfg = cfg;
            cfg.create_table_sql = Some(body.to_string());
            Ok(TableCfg::Sharding(cfg))
        }
        other => Err(EngineError::general(format!(
            "unknown createTable hint type: {}",
            other
        ))),
    }
}

/// 计算表配置对应的物理节点。
fn physical_nodes_of(cfg: &TableCfg) -> Result<Vec<mycat3_router::IndexDataNode>, EngineError> {
    match cfg {
        TableCfg::Sharding(t) => {
            if mycat3_router::is_auto_function(&t.function) {
                mycat3_router::expand_auto_nodes(&t.function.properties)
                    .map_err(|e| EngineError::general(e.to_string()))
            } else {
                Ok(mycat3_router::expand_backend_nodes(&t.partition))
            }
        }
        TableCfg::Global(g) => Ok(g
            .broadcast
            .iter()
            .enumerate()
            .map(|(i, b)| {
                mycat3_router::IndexDataNode::new(
                    b.target_name.clone(),
                    b.schema_name.clone().unwrap_or_default(),
                    b.table_name.clone().unwrap_or_default(),
                    i as i32,
                )
            })
            .collect()),
        TableCfg::Normal(n) => {
            let l = n.locality.clone().unwrap_or_default();
            Ok(vec![mycat3_router::IndexDataNode::new(
                l.target_name,
                l.schema_name.unwrap_or_default(),
                l.table_name.unwrap_or_default(),
                0,
            )])
        }
    }
}

/// 在单个物理节点建库建表（对应 CreateTableUtils.createPhysicalTable）。
/// `ddl_template` 中的 `__TABLE__` 占位符替换为 `` `physSchema`.`physTable` ``。
async fn create_physical_node(
    state: &std::sync::Arc<ServerState>,
    node: &mycat3_router::IndexDataNode,
    ddl_template: &str,
) -> Result<(), EngineError> {
    let replica = state.replica();
    let ds = replica
        .resolve(&node.target_name, true, false, false)
        .ok_or_else(|| {
            EngineError::general(format!("no datasource for target {}", node.target_name))
        })?;
    let ds_cfg = replica
        .datasource_config(&ds)
        .ok_or_else(|| EngineError::general(format!("datasource {} not found", ds)))?;
    if ds_cfg.db_type.eq_ignore_ascii_case("mssql") {
        return create_mssql_physical_node(&ds_cfg, node, ddl_template).await;
    }
    let mut conn = state
        .conn_mgr
        .connect_direct(&ds_cfg)
        .await
        .map_err(|e| EngineError::general(format!("connect {} failed: {}", ds, e)))?;
    if !node.schema_name.is_empty() {
        let sql = format!(
            "CREATE DATABASE IF NOT EXISTS `{}` DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci",
            node.schema_name
        );
        conn.execute_ignore(&sql)
            .await
            .map_err(|e| EngineError::general(e.to_string()))?;
    }
    // 物理 DDL：`schema`.`table`
    let phys_name = if node.schema_name.is_empty() {
        format!("`{}`", node.table_name)
    } else {
        format!("`{}`.`{}`", node.schema_name, node.table_name)
    };
    let ddl = ddl_template.replace("__TABLE__", &phys_name);
    conn.execute_ignore(&ddl)
        .await
        .map_err(|e| EngineError::general(e.to_string()))?;
    conn.quit().await;
    Ok(())
}

/// 该物理节点对应的数据源是否 SQL Server。
fn physical_node_is_mssql(
    state: &std::sync::Arc<ServerState>,
    node: &mycat3_router::IndexDataNode,
) -> bool {
    let replica = state.replica();
    let Some(ds) = replica.resolve(&node.target_name, true, false, false) else {
        return false;
    };
    replica
        .datasource_config(&ds)
        .map(|c| c.db_type.eq_ignore_ascii_case("mssql"))
        .unwrap_or(false)
}

/// SQL Server 物理删表（`DROP TABLE IF EXISTS [schema].[table]`）。
async fn drop_mssql_physical_node(
    state: &std::sync::Arc<ServerState>,
    node: &mycat3_router::IndexDataNode,
) -> Result<(), EngineError> {
    let replica = state.replica();
    let ds = replica
        .resolve(&node.target_name, true, false, false)
        .ok_or_else(|| {
            EngineError::general(format!("no datasource for target {}", node.target_name))
        })?;
    let ds_cfg = replica
        .datasource_config(&ds)
        .ok_or_else(|| EngineError::general(format!("datasource {} not found", ds)))?;
    let ep = mycat3_mssql::parse_mssql_jdbc_url(&ds_cfg.url, &ds_cfg.user, &ds_cfg.password)
        .map_err(|e| EngineError::general(format!("mssql datasource: {}", e)))?;
    let mut conn = mycat3_mssql::MssqlConnection::connect(&ep)
        .await
        .map_err(|e| EngineError::general(format!("mssql connect failed: {}", e)))?;
    let schema = if node.schema_name.is_empty() {
        "dbo"
    } else {
        &node.schema_name
    };
    let sql = format!(
        "DROP TABLE IF EXISTS [{}].[{}]",
        schema.replace(']', "]]"),
        node.table_name.replace(']', "]]")
    );
    conn.execute_script(&sql, Some(std::time::Duration::from_secs(30)))
        .await
        .map_err(|e| EngineError::general(format!("mssql drop table: {}", e)))?;
    Ok(())
}

/// SQL Server 物理建表：确保 schema 存在，把 MySQL CREATE TABLE 翻译为
/// 幂等 T-SQL（`IF OBJECT_ID(...) IS NULL`）后执行。
async fn create_mssql_physical_node(
    ds_cfg: &DatasourceConfig,
    node: &mycat3_router::IndexDataNode,
    ddl_template: &str,
) -> Result<(), EngineError> {
    let ep = mycat3_mssql::parse_mssql_jdbc_url(&ds_cfg.url, &ds_cfg.user, &ds_cfg.password)
        .map_err(|e| EngineError::general(format!("mssql datasource: {}", e)))?;
    let mut conn = mycat3_mssql::MssqlConnection::connect(&ep)
        .await
        .map_err(|e| EngineError::general(format!("mssql connect failed: {}", e)))?;
    let schema = if node.schema_name.is_empty() {
        "dbo"
    } else {
        node.schema_name.as_str()
    };
    let schema_ident = schema.replace(']', "]]");
    let schema_lit = schema_ident.replace('\'', "''");
    let create_schema = format!(
        "IF SCHEMA_ID(N'{}') IS NULL EXEC(N'CREATE SCHEMA [{}]')",
        schema_lit, schema_ident
    );
    conn.execute_script(&create_schema, Some(std::time::Duration::from_secs(30)))
        .await
        .map_err(|e| EngineError::general(format!("mssql create schema: {}", e)))?;
    let ddl = mycat3_sql::translate_mssql_ddl(ddl_template, schema, &node.table_name)
        .map_err(|e| EngineError::general(format!("mssql DDL translate: {}", e)))?;
    conn.execute_script(&ddl, Some(std::time::Duration::from_secs(60)))
        .await
        .map_err(|e| EngineError::general(format!("mssql create table: {}", e)))?;
    Ok(())
}

/// 加 IF NOT EXISTS、表名替换为 `__TABLE__` 占位符（DRDS 子句已在 split 阶段去除）。
/// 对应 `normalizeCreateTableSQLToMySQL` + `LogicTable.rewriteCreateTableSql`。
pub fn normalize_create_table(body: &str) -> String {
    let mut s = body.trim().trim_end_matches(';').to_string();
    let lower = s.to_lowercase();
    let Some(ct_pos) = lower.find("create table") else {
        return s;
    };
    let after = ct_pos + "create table".len();
    if !lower[after..].trim_start().starts_with("if not exists") {
        s = format!("{} IF NOT EXISTS{}", &s[..after], &s[after..]);
    }
    // 定位表名段（可选反引号、可选 db.table）→ 替换为占位符
    let lower = s.to_lowercase();
    let after = lower.find("create table").unwrap() + "create table".len();
    let b = s.as_bytes();
    let mut i = after;
    while i < b.len() && b[i].is_ascii_whitespace() {
        i += 1;
    }
    if s[i..].to_lowercase().starts_with("if not exists") {
        i += "if not exists".len();
        while i < b.len() && b[i].is_ascii_whitespace() {
            i += 1;
        }
    }
    let start = i;
    i = skip_identifier(&s, i);
    if i > start {
        s = format!("{}__TABLE__{}", &s[..start], &s[i..]);
    }
    s
}

/// 跳过一个标识符（反引号包裹或裸标识，支持 db.table 两段）。
fn skip_identifier(s: &str, from: usize) -> usize {
    let b = s.as_bytes();
    let mut i = from;
    for _ in 0..2 {
        if i < b.len() && b[i] == b'`' {
            i += 1;
            while i < b.len() && b[i] != b'`' {
                i += 1;
            }
            i = (i + 1).min(b.len());
        } else {
            let st = i;
            while i < b.len()
                && (b[i].is_ascii_alphanumeric() || b[i] == b'_' || b[i] == b'$' || b[i] >= 0x80)
            {
                i += 1;
            }
            if i == st {
                return i;
            }
        }
        // db.table 的第二段
        if i < b.len() && b[i] == b'.' {
            i += 1;
            continue;
        }
        break;
    }
    i
}

fn rebuild_drds_suffix(part: &DrdsPartition) -> String {
    let mut s = String::new();
    if let Some(m) = &part.db_method {
        s.push_str(&format!(" dbpartition by {}", m));
        if let Some(n) = part.db_partitions {
            s.push_str(&format!(" dbpartitions {}", n));
        }
    }
    if let Some(m) = &part.tb_method {
        s.push_str(&format!(" tbpartition by {}", m));
        if let Some(n) = part.tb_partitions {
            s.push_str(&format!(" tbpartitions {}", n));
        }
    }
    if part.broadcast {
        s.push_str(" broadcast");
    }
    s
}

// ---------------------------------------------------------------------------
// DROP TABLE
// ---------------------------------------------------------------------------

pub async fn drop_table(
    state: &std::sync::Arc<ServerState>,
    schema_ctx: Option<&str>,
    sql: &str,
) -> Result<EngineOutcome, EngineError> {
    let cleaned = remove_comments(sql);
    let tokens = tokenize_head(&cleaned, 8);
    let i = skip_if_exists(&tokens, 2);
    let full = tokens
        .get(i)
        .ok_or_else(|| EngineError::general("DROP TABLE requires a table name"))?
        .clone();
    let (schema_name, table_name) = if full.contains('.') {
        let mut it = full.splitn(2, '.');
        (
            it.next().unwrap().trim_matches('`').to_string(),
            it.next().unwrap().trim_matches('`').to_string(),
        )
    } else {
        (
            schema_ctx
                .ok_or_else(|| EngineError::new(1046, "3D000", "No database selected"))?
                .to_string(),
            full.trim_matches('`').to_string(),
        )
    };
    let schema_lc = schema_name.to_lowercase();
    let table_lc = table_name.to_lowercase();

    // 先取物理节点（配置删除前）
    let nodes = {
        let meta = state.metadata();
        match meta
            .get_schema(&schema_lc)
            .and_then(|s| s.get_table(&table_lc))
        {
            Some(TableMeta::Sharding { nodes, .. }) => nodes.as_ref().clone(),
            Some(TableMeta::Global {
                targets, schema, ..
            }) => targets
                .iter()
                .enumerate()
                .map(|(i, t)| {
                    mycat3_router::IndexDataNode::new(
                        t.clone(),
                        if schema.is_empty() {
                            schema_name.clone()
                        } else {
                            schema.clone()
                        },
                        table_name.clone(),
                        i as i32,
                    )
                })
                .collect(),
            Some(TableMeta::Normal {
                target,
                schema,
                table,
                ..
            }) => {
                vec![mycat3_router::IndexDataNode::new(
                    target.clone(),
                    if schema.is_empty() {
                        schema_name.clone()
                    } else {
                        schema.clone()
                    },
                    if table.is_empty() {
                        table_name.clone()
                    } else {
                        table.clone()
                    },
                    0,
                )]
            }
            _ => Vec::new(),
        }
    };

    // 删除配置
    let (s2, t2) = (schema_lc.clone(), table_lc.clone());
    commit_config(state, move |cfg| {
        if let Some(schema) = cfg.schemas.get_mut(&s2) {
            remove_table_everywhere(schema, &t2);
        }
        Ok(())
    })?;

    // 物理删除（幂等，错误吞掉 —— Java 同）
    for node in &nodes {
        let r = if physical_node_is_mssql(state, node) {
            drop_mssql_physical_node(state, node).await
        } else {
            let sql = if node.schema_name.is_empty() {
                format!("DROP TABLE IF EXISTS `{}`", node.table_name)
            } else {
                format!(
                    "DROP TABLE IF EXISTS `{}`.`{}`",
                    node.schema_name, node.table_name
                )
            };
            execute_on_target(state, &node.target_name, &sql).await
        };
        if let Err(e) = r {
            tracing::warn!(node = %node.unique_name(), "physical drop failed: {}", e);
        }
    }
    Ok(EngineOutcome::Ok {
        affected_rows: 0,
        last_insert_id: 0,
    })
}

// ---------------------------------------------------------------------------
// CREATE / DROP USER（Java SQL 层是 no-op，Pro 实现真实配置写入）
// ---------------------------------------------------------------------------

pub fn create_user(
    state: &std::sync::Arc<ServerState>,
    sql: &str,
) -> Result<EngineOutcome, EngineError> {
    // CREATE USER [IF NOT EXISTS] u [IDENTIFIED BY 'pw']
    let cleaned = remove_comments(sql);
    let lower = cleaned.to_lowercase();
    let pos = lower
        .find("identified by")
        .or_else(|| lower.find("identified with"));
    let (user_part, password) = match pos {
        Some(p) => {
            let rest = &cleaned[p..];
            let pw = rest
                .split_whitespace()
                .nth(2)
                .unwrap_or("")
                .trim_matches('\'')
                .trim_matches('"')
                .trim_end_matches(';')
                .to_string();
            (&cleaned[..p], pw)
        }
        None => (&cleaned[..], String::new()),
    };
    let tokens = tokenize_head(user_part, 8);
    let i = skip_if_exists(&tokens, 2);
    let username = tokens
        .get(i)
        .ok_or_else(|| EngineError::general("CREATE USER requires a username"))?
        .trim_matches('\'')
        .trim_matches('`')
        .to_string();
    let uname = username.clone();
    commit_config(state, move |cfg| {
        cfg.users.insert(
            uname.clone(),
            UserConfig {
                username: uname,
                password,
                ip: None,
                transaction_type: "proxy".into(),
                dialect: "mysql".into(),
                schema: None,
                schemas: vec![],
                isolation: 3,
                read_only: false,
                login_limit: -1,
                role: None,
                encrypt_type: None,
                encode_key: None,
                decrypted: std::sync::Arc::new(parking_lot::Mutex::new(None)),
            },
        );
        Ok(())
    })?;
    Ok(EngineOutcome::Ok {
        affected_rows: 0,
        last_insert_id: 0,
    })
}

pub fn drop_user(
    state: &std::sync::Arc<ServerState>,
    sql: &str,
) -> Result<EngineOutcome, EngineError> {
    let cleaned = remove_comments(sql);
    let tokens = tokenize_head(&cleaned, 8);
    let i = skip_if_exists(&tokens, 2);
    let username = tokens
        .get(i)
        .ok_or_else(|| EngineError::general("DROP USER requires a username"))?
        .trim_matches('\'')
        .trim_matches('`')
        .trim_end_matches(';')
        .to_string();
    if username == "root" {
        return Err(EngineError::general(
            "refusing to drop built-in user 'root'",
        ));
    }
    let uname = username.clone();
    commit_config(state, move |cfg| {
        cfg.users.remove(&uname);
        Ok(())
    })?;
    Ok(EngineOutcome::Ok {
        affected_rows: 0,
        last_insert_id: 0,
    })
}

fn show_users(state: &std::sync::Arc<ServerState>) -> EngineOutcome {
    let cfg = state.config();
    let rows = cfg
        .users
        .values()
        .map(|u| {
            vec![
                Some(u.username.clone()),
                Some(u.ip.clone().unwrap_or_else(|| "%".into())),
                Some(u.transaction_type.clone()),
                Some(u.dialect.clone()),
                Some(u.visible_schemas().join(",")),
            ]
        })
        .collect();
    EngineOutcome::Rows {
        columns: ["USERNAME", "IP", "TRANSACTION_TYPE", "DIALECT", "SCHEMAS"]
            .iter()
            .map(|n| crate::engine::EngineColumn {
                name: n.to_string(),
                mysql_type: mycat3_protocol::consts::field_type::VAR_STRING,
                charset: 33,
            })
            .collect(),
        rows,
    }
}

// ---------------------------------------------------------------------------
// CREATE / DROP SEQUENCE
// ---------------------------------------------------------------------------

pub fn manage_sequence(
    state: &std::sync::Arc<ServerState>,
    sql: &str,
) -> Result<EngineOutcome, EngineError> {
    let norm = normalize_head(sql);
    if norm.starts_with("create sequence") {
        let cfg = crate::sequence::parse_create_sequence(sql)
            .ok_or_else(|| EngineError::general("bad CREATE SEQUENCE syntax"))?;
        let name = cfg.name.clone();
        commit_config(state, move |c| {
            c.sequences.insert(name, cfg);
            Ok(())
        })?;
    } else {
        // DROP SEQUENCE [IF EXISTS] name
        let cleaned = remove_comments(sql);
        let tokens = tokenize_head(&cleaned, 6);
        let i = skip_if_exists(&tokens, 2);
        let name = tokens
            .get(i)
            .ok_or_else(|| EngineError::general("DROP SEQUENCE requires a name"))?
            .trim_matches('`')
            .trim_end_matches(';')
            .to_lowercase();
        commit_config(state, move |c| {
            c.sequences.remove(&name);
            Ok(())
        })?;
    }
    Ok(EngineOutcome::Ok {
        affected_rows: 0,
        last_insert_id: 0,
    })
}

// ---------------------------------------------------------------------------
// 文本工具
// ---------------------------------------------------------------------------

/// 提取 `/*!mycat:{...}*/` 或 `/*!+mycat:createTable{...}*/` 中的 JSON。
pub fn extract_mycat_json_hint(sql: &str) -> Option<serde_json::Value> {
    let start = sql.find("/*!mycat:")?;
    let body_start = start + "/*!mycat:".len();
    let end = sql[body_start..].find("*/")? + body_start;
    let body = sql[body_start..end].trim();
    serde_json::from_str(body).ok()
}

pub fn extract_create_table_hint(sql: &str) -> Option<serde_json::Value> {
    // /*!+mycat:createTable{...}*/ 或 /*!mycat:createTable{...}*/
    for prefix in ["/*!+mycat:createTable", "/*!mycat:createTable"] {
        if let Some(start) = sql.find(prefix) {
            let json_start = start + prefix.len();
            // 找到匹配的 }（简单括号计数）
            let bytes = sql.as_bytes();
            let mut depth = 0i32;
            let mut end = json_start;
            while end < bytes.len() {
                match bytes[end] {
                    b'{' => depth += 1,
                    b'}' => {
                        depth -= 1;
                        if depth == 0 {
                            end += 1;
                            break;
                        }
                    }
                    _ => {}
                }
                end += 1;
            }
            if let Ok(v) = serde_json::from_str(&sql[json_start..end]) {
                return Some(v);
            }
        }
    }
    None
}

fn remove_head_hint(sql: &str) -> String {
    // 去掉开头的 /*!...*/ hint
    let t = sql.trim_start();
    if t.starts_with("/*!") {
        if let Some(end) = t.find("*/") {
            return t[end + 2..].trim_start().to_string();
        }
    }
    sql.to_string()
}

/// 去掉 SQL 注释（保留字符串字面量）。
pub fn remove_comments(sql: &str) -> String {
    let bytes = sql.as_bytes();
    let mut out = String::with_capacity(sql.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'\'' | b'"' | b'`' => {
                let q = bytes[i];
                out.push(sql[i..i + 1].chars().next().unwrap());
                i += 1;
                while i < bytes.len() {
                    out.push(sql[i..i + 1].chars().next().unwrap());
                    if bytes[i] == b'\\' && q != b'`' {
                        if i + 1 < bytes.len() {
                            out.push(sql[i + 1..i + 2].chars().next().unwrap());
                        }
                        i += 2;
                        continue;
                    }
                    if bytes[i] == q {
                        i += 1;
                        break;
                    }
                    i += 1;
                }
            }
            b'-' if i + 1 < bytes.len() && bytes[i + 1] == b'-' => {
                while i < bytes.len() && bytes[i] != b'\n' {
                    i += 1;
                }
            }
            b'/' if i + 1 < bytes.len() && bytes[i + 1] == b'*' => {
                i += 2;
                while i + 1 < bytes.len() && !(bytes[i] == b'*' && bytes[i + 1] == b'/') {
                    i += 1;
                }
                i = (i + 2).min(bytes.len());
            }
            _ => {
                // 复制一个 UTF-8 字符
                let ch_len = utf8_char_len(bytes[i]);
                out.push_str(&sql[i..(i + ch_len).min(sql.len())]);
                i += ch_len;
            }
        }
    }
    out
}

fn utf8_char_len(b: u8) -> usize {
    if b < 0x80 {
        1
    } else if b >> 5 == 0b110 {
        2
    } else if b >> 4 == 0b1110 {
        3
    } else {
        4
    }
}

/// 跳过 [IF [NOT] EXISTS]：返回跳过后的下标。
fn skip_if_exists(tokens: &[String], mut i: usize) -> usize {
    if tokens
        .get(i)
        .map(|t| t.eq_ignore_ascii_case("IF"))
        .unwrap_or(false)
    {
        i += if tokens
            .get(i + 1)
            .map(|t| t.eq_ignore_ascii_case("NOT"))
            .unwrap_or(false)
        {
            3
        } else {
            2
        };
    }
    i
}

/// 粗略 token 化（按空白与标点切分，保留 `a.b` 与引号串）。
fn tokenize_head(sql: &str, max: usize) -> Vec<String> {
    let mut tokens = Vec::new();
    let bytes = sql.as_bytes();
    let mut i = 0;
    while i < bytes.len() && tokens.len() < max {
        let c = bytes[i];
        if c.is_ascii_whitespace() || c == b',' || c == b';' {
            i += 1;
            continue;
        }
        if c == b'\'' || c == b'"' || c == b'`' {
            let q = c;
            let start = i;
            i += 1;
            while i < bytes.len() && bytes[i] != q {
                i += 1;
            }
            i = (i + 1).min(bytes.len());
            tokens.push(sql[start..i].to_string());
            continue;
        }
        let start = i;
        while i < bytes.len() {
            let b = bytes[i];
            if b.is_ascii_alphanumeric() || b == b'_' || b == b'.' || b == b'$' || b >= 0x80 {
                i += 1;
            } else {
                break;
            }
        }
        if i == start {
            // 标点单独成 token（如 '(' ';'）
            tokens.push(sql[i..i + 1].to_string());
            i += 1;
        } else {
            tokens.push(sql[start..i].to_string());
        }
    }
    tokens
}

// ---------------------------------------------------------------------------
// 配置管理/运维 hint（对应 Java mycatDmlHandler 组 + 运维指令）
// ---------------------------------------------------------------------------

/// 配置管理/运维 hint 分派：`/*!mycat:createDataSource{...}*/` 等。
/// 配置对象 JSON 结构与 Java 一致（DatasourceConfig/ClusterConfig/UserConfig/
/// SequenceConfig/LogicSchemaConfig 的 serde 模型），写盘 + 热刷新复用 commit_config。
pub async fn handle_config_hint(
    state: &std::sync::Arc<ServerState>,
    ctx: &crate::session::SessionContext,
    cmd: &str,
    body: &str,
) -> Result<EngineOutcome, EngineError> {
    let ok = || EngineOutcome::Ok {
        affected_rows: 1,
        last_insert_id: 0,
    };
    match cmd {
        "createdatasource" => {
            let ds: DatasourceConfig = parse_config_json(body, "DatasourceConfig")?;
            // 与启动校验一致：URL 必须可解析（Oracle 按驱动连接串校验）
            if ds.db_type.eq_ignore_ascii_case("oracle") {
                mycat3_oracle::config_from_jdbc_url(&ds.url, &ds.user, &ds.password).map_err(
                    |e| EngineError::new(1064, "42000", format!("datasource url 无效: {}", e)),
                )?;
            } else {
                ds.parse_url().map_err(|e| {
                    EngineError::new(1064, "42000", format!("datasource url 无效: {}", e))
                })?;
            }
            let name = ds.name.clone();
            commit_config(state, move |cfg| {
                cfg.datasources.insert(name, ds);
                Ok(())
            })?;
            Ok(ok())
        }
        "dropdatasource" => {
            let name = name_from_json(body)?;
            commit_config(state, move |cfg| {
                if cfg.datasources.remove(&name).is_none() {
                    return Err(EngineError::general(format!("datasource {} 不存在", name)));
                }
                Ok(())
            })?;
            Ok(ok())
        }
        "createcluster" => {
            let c: ClusterConfig = parse_config_json(body, "ClusterConfig")?;
            let name = c.name.clone();
            commit_config(state, move |cfg| {
                cfg.clusters.insert(name, c);
                Ok(())
            })?;
            Ok(ok())
        }
        "dropcluster" => {
            let name = name_from_json(body)?;
            commit_config(state, move |cfg| {
                if cfg.clusters.remove(&name).is_none() {
                    return Err(EngineError::general(format!("cluster {} 不存在", name)));
                }
                Ok(())
            })?;
            Ok(ok())
        }
        "createuser" => {
            let u: UserConfig = parse_config_json(body, "UserConfig")?;
            let username = u.username.clone();
            commit_config(state, move |cfg| {
                cfg.users.insert(username, u);
                Ok(())
            })?;
            Ok(ok())
        }
        "dropuser" => {
            let username = name_from_json_key(body, "username")?;
            if username.eq_ignore_ascii_case("root") {
                return Err(EngineError::general("root 用户受保护不可删除"));
            }
            commit_config(state, move |cfg| {
                if cfg.users.remove(&username).is_none() {
                    return Err(EngineError::general(format!("user {} 不存在", username)));
                }
                Ok(())
            })?;
            Ok(ok())
        }
        "setsequence" => {
            let s: SequenceConfig = parse_config_json(body, "SequenceConfig")?;
            let name = s.name.clone();
            commit_config(state, move |cfg| {
                cfg.sequences.insert(name, s);
                Ok(())
            })?;
            Ok(ok())
        }
        "createschema" => {
            let s: LogicSchemaConfig = parse_config_json(body, "LogicSchemaConfig")?;
            let name = s.schema_name.clone();
            commit_config(state, move |cfg| {
                cfg.schemas.insert(name, s);
                Ok(())
            })?;
            Ok(ok())
        }
        "dropschema" => {
            let name = name_from_json_key(body, "schemaName")?;
            commit_config(state, move |cfg| {
                if cfg.schemas.remove(&name).is_none() {
                    return Err(EngineError::general(format!("schema {} 不存在", name)));
                }
                Ok(())
            })?;
            Ok(ok())
        }
        "repairphysicaltable" => {
            repair_physical_tables(state).await?;
            Ok(ok())
        }
        "loadconfigfromfile" => reload_from_file(state).await,
        // setUserDialect{"username":"bob","dialect":"mysql"}：改用户默认方言并落盘
        "setuserdialect" => {
            let v: serde_json::Value = parse_config_json(body, "setUserDialect")?;
            let username = v
                .get("username")
                .and_then(|x| x.as_str())
                .ok_or_else(|| EngineError::new(1064, "42000", "缺少 username 字段"))?
                .to_string();
            let dialect = v
                .get("dialect")
                .and_then(|x| x.as_str())
                .ok_or_else(|| EngineError::new(1064, "42000", "缺少 dialect 字段"))?
                .to_string();
            commit_config(state, move |cfg| {
                let u = cfg
                    .users
                    .get_mut(&username)
                    .ok_or_else(|| EngineError::general(format!("user {} 不存在", username)))?;
                u.dialect = dialect;
                Ok(())
            })?;
            Ok(ok())
        }
        // ---- SQL 结果缓存（对应 Java createSqlCache/showSqlCaches/dropSqlCache）----
        "createsqlcache" => {
            let c: SqlCacheConfig = parse_config_json(body, "SqlCacheConfig")?;
            let parameterized = !c.match_mode.eq_ignore_ascii_case("EXACT");
            if mycat3_sql::normalize_cache_sql(&c.sql, parameterized).is_none() {
                return Err(EngineError::new(
                    1064,
                    "42000",
                    "缓存 SQL 必须是单条 SELECT 语句",
                ));
            }
            let name = c.name.clone();
            commit_config(state, move |cfg| {
                cfg.sql_caches.insert(name, c);
                Ok(())
            })?;
            state.sql_cache.resync(state); // 建任务 + 立即预热
            Ok(ok())
        }
        "dropsqlcache" => {
            let name = name_from_json(body)?;
            commit_config(state, move |cfg| {
                if cfg.sql_caches.remove(&name).is_none() {
                    return Err(EngineError::general(format!("sqlCache {} 不存在", name)));
                }
                Ok(())
            })?;
            state.sql_cache.resync(state);
            Ok(ok())
        }
        "showsqlcaches" => Ok(state.sql_cache.show()),
        // ---- 优雅停机/暂停接入（对应 Java MycatServer 运维指令）----
        // setAcceptConnect=0 停止接入 / =1 恢复（Java：body 含 "1" 即恢复）
        "setacceptconnect" => {
            state
                .accept_paused
                .store(!body.contains('1'), std::sync::atomic::Ordering::Relaxed);
            Ok(ok())
        }
        // setReadyToCloseSQL{"sql": "..."}：命中该 SQL 即回 OK，供探活确认可关停
        "setreadytoclosesql" => {
            let v: serde_json::Value = serde_json::from_str(body)
                .map_err(|e| EngineError::new(1064, "42000", format!("JSON 解析失败: {}", e)))?;
            let sql = v
                .get("sql")
                .and_then(|s| s.as_str())
                .map(|s| s.trim().to_string())
                .ok_or_else(|| EngineError::new(1064, "42000", "缺少 sql 字段"))?;
            *state.ready_to_close_sql.write() = Some(sql);
            Ok(ok())
        }
        // pauseServer{"connectionIds": [...]}：挂起指定连接 + 自身会话
        "pauseserver" => {
            let ids: Vec<u32> = serde_json::from_str::<serde_json::Value>(body)
                .ok()
                .and_then(|v| v.get("connectionIds").cloned())
                .and_then(|v| serde_json::from_value::<Vec<u32>>(v).ok())
                .unwrap_or_default();
            state.pause_connections(&ids, ctx.connection_id);
            Ok(ok())
        }
        "resumeserver" => {
            state.resume_all_connections();
            Ok(ok())
        }
        other => Err(EngineError::new(
            1235,
            "42000",
            format!("未知配置指令: {}", other),
        )),
    }
}

/// 配置对象 JSON 解析（字段名与 Java 配置一致）。
fn parse_config_json<T: serde::de::DeserializeOwned>(
    body: &str,
    what: &str,
) -> Result<T, EngineError> {
    serde_json::from_str(body)
        .map_err(|e| EngineError::new(1064, "42000", format!("{} 解析失败: {}", what, e)))
}

/// 取配置对象的 `name` 字段（dropDataSource/dropCluster 用，Java 传完整对象）。
fn name_from_json(body: &str) -> Result<String, EngineError> {
    name_from_json_key(body, "name")
}

fn name_from_json_key(body: &str, key: &str) -> Result<String, EngineError> {
    let v: serde_json::Value = serde_json::from_str(body)
        .map_err(|e| EngineError::new(1064, "42000", format!("JSON 解析失败: {}", e)))?;
    v.get(key)
        .and_then(|x| x.as_str())
        .map(|s| s.to_string())
        .ok_or_else(|| EngineError::new(1064, "42000", format!("缺少 {} 字段", key)))
}

/// `repairPhysicalTable`：按元数据为全部逻辑表补建缺失物理库表（幂等，
/// 对应 Java `MetadataManager.createPhysicalTables()`；Oracle 后端跳过，物理表手工管理）。
async fn repair_physical_tables(state: &std::sync::Arc<ServerState>) -> Result<(), EngineError> {
    let meta = state.metadata();
    let mut repaired = 0u64;
    for (sname, schema) in &meta.schemas {
        for (tname, t) in &schema.tables {
            // (create_sql, [(target, 物理schema, 物理表)])
            let plan: Option<(String, Vec<(String, String, String)>)> = match t {
                TableMeta::Sharding {
                    create_sql: Some(c),
                    nodes,
                    ..
                } => Some((
                    c.clone(),
                    nodes
                        .iter()
                        .map(|n| {
                            (
                                n.target_name.clone(),
                                n.schema_name.clone(),
                                n.table_name.clone(),
                            )
                        })
                        .collect(),
                )),
                TableMeta::Global {
                    create_sql: Some(c),
                    target_schemas,
                    ..
                } => Some((
                    c.clone(),
                    target_schemas
                        .iter()
                        .map(|(t, s)| (t.clone(), s.clone(), tname.clone()))
                        .collect(),
                )),
                TableMeta::Normal {
                    create_sql: Some(c),
                    target,
                    schema,
                    table,
                    ..
                } => {
                    let ps = if schema.is_empty() {
                        sname.clone()
                    } else {
                        schema.clone()
                    };
                    let pt = if table.is_empty() {
                        tname.clone()
                    } else {
                        table.clone()
                    };
                    Some((c.clone(), vec![(target.clone(), ps, pt)]))
                }
                _ => {
                    if let TableMeta::View { .. } = t {
                        // 视图不建物理表
                    } else {
                        tracing::warn!(
                            schema = %sname,
                            table = %tname,
                            "repairPhysicalTable 跳过（无 create_sql）"
                        );
                    }
                    None
                }
            };
            let Some((create_sql, nodes)) = plan else {
                continue;
            };
            let template = normalize_create_table(&create_sql);
            for (target, ps, pt) in nodes {
                let replica = state.replica();
                let ds = match replica.resolve(&target, true, false, false) {
                    Some(d) => d,
                    None => continue,
                };
                let ds_cfg = match replica.datasource_config(&ds) {
                    Some(c) => c,
                    None => continue,
                };
                if !ds_cfg.db_type.eq_ignore_ascii_case("mysql")
                    && !ds_cfg.db_type.eq_ignore_ascii_case("mssql")
                {
                    continue; // Oracle/PostgreSQL 物理表由 DBA 手工管理
                }
                let node = mycat3_router::IndexDataNode {
                    target_name: target,
                    schema_name: ps,
                    table_name: pt,
                    db_index: -1,
                    table_index: -1,
                    index: -1,
                };
                create_physical_node(state, &node, &template).await?;
                repaired += 1;
            }
        }
    }
    tracing::info!(repaired, "repairPhysicalTable 完成");
    Ok(())
}

/// CREATE VIEW：物理执行（schema 默认 target）+ 视图元数据注册（对应 Java
/// CreateViewSQLHandler：建视图 + ops.putView）。
async fn create_view(
    state: &std::sync::Arc<ServerState>,
    schema_ctx: Option<&str>,
    sql: &str,
) -> Result<EngineOutcome, EngineError> {
    let schema = schema_ctx
        .ok_or_else(|| EngineError::new(1046, "3D000", "No database selected"))?
        .to_string();
    let stmt = sqlparser::parser::Parser::parse_sql(&sqlparser::dialect::MySqlDialect {}, sql)
        .map_err(|e| EngineError::new(1064, "42000", format!("CREATE VIEW 解析失败: {}", e)))?
        .into_iter()
        .next()
        .ok_or_else(|| EngineError::general("empty ddl"))?;
    let (view_schema, view_name) = match &stmt {
        sqlparser::ast::Statement::CreateView { name, .. } => {
            let parts: Vec<String> = name
                .0
                .iter()
                .map(|i| i.value.trim_matches('`').to_string())
                .collect();
            match parts.len() {
                1 => (schema.clone(), parts[0].clone()),
                2 => (parts[0].clone(), parts[1].clone()),
                _ => return Err(EngineError::new(1064, "42000", "CREATE VIEW 名称格式非法")),
            }
        }
        _ => return Err(EngineError::new(1064, "42000", "非 CREATE VIEW 语句")),
    };
    if !view_schema.eq_ignore_ascii_case(&schema) {
        return Err(EngineError::general("跨库 CREATE VIEW 不支持"));
    }
    // 视图引用的表必须是单表（分片/广播表在透传模式下无法落到单一物理库）
    {
        use sqlparser::ast::{VisitMut, VisitorMut};
        struct Refs<'a> {
            meta: &'a Metadata,
            schema: &'a str,
            bad: Option<String>,
        }
        impl VisitorMut for Refs<'_> {
            type Break = ();
            fn pre_visit_relation(
                &mut self,
                r: &mut sqlparser::ast::ObjectName,
            ) -> std::ops::ControlFlow<()> {
                let t =
                    r.0.last()
                        .map(|i| i.value.to_lowercase())
                        .unwrap_or_default();
                if let Some(tm) = self
                    .meta
                    .get_schema(self.schema)
                    .and_then(|s| s.get_table(&t))
                {
                    if matches!(tm, TableMeta::Sharding { .. } | TableMeta::Global { .. }) {
                        self.bad = Some(t);
                    }
                }
                std::ops::ControlFlow::Continue(())
            }
        }
        let mut stmts =
            sqlparser::parser::Parser::parse_sql(&sqlparser::dialect::MySqlDialect {}, sql)
                .map_err(|e| {
                    EngineError::new(1064, "42000", format!("CREATE VIEW 解析失败: {}", e))
                })?;
        let meta = state.metadata();
        let mut v = Refs {
            meta: &meta,
            schema: &schema,
            bad: None,
        };
        for st in stmts.iter_mut() {
            let _ = VisitMut::visit(st, &mut v);
        }
        if let Some(t) = v.bad {
            return Err(EngineError::general(format!(
                "CREATE VIEW 引用分片/广播表 '{}' 不支持（透传模式视图仅限单表）",
                t
            )));
        }
    }
    // 物理执行（视图无分片：schema 默认 target）。
    // 直连连接的默认库是数据源 URL 中的库：视图名加库限定，避免建错库
    let target = state
        .metadata()
        .get_schema(&schema)
        .map(|s| s.target_name.clone())
        .ok_or_else(|| EngineError::new(1049, "42000", format!("Unknown database '{}'", schema)))?;
    execute_on_target_in_schema(state, &target, &schema, sql).await?;
    // 配置注册（normalize：去数据库限定，统一视图名）
    let view_sql = normalize_view_sql(sql, &view_name);
    commit_config(state, move |cfg| {
        let sc = cfg
            .schemas
            .get_mut(&schema)
            .ok_or_else(|| EngineError::general("schema missing"))?;
        sc.views.insert(
            view_name.clone().to_lowercase(),
            mycat3_config::ViewConfig {
                create_view_sql: Some(view_sql),
            },
        );
        Ok(())
    })?;
    Ok(EngineOutcome::Ok {
        affected_rows: 0,
        last_insert_id: 0,
    })
}

/// DROP VIEW：物理执行 + 视图元数据移除（对应 Java DropViewSQLHandler）。
async fn drop_view(
    state: &std::sync::Arc<ServerState>,
    schema_ctx: Option<&str>,
    sql: &str,
) -> Result<EngineOutcome, EngineError> {
    let schema = schema_ctx
        .ok_or_else(|| EngineError::new(1046, "3D000", "No database selected"))?
        .to_string();
    let stmt = sqlparser::parser::Parser::parse_sql(&sqlparser::dialect::MySqlDialect {}, sql)
        .map_err(|e| EngineError::new(1064, "42000", format!("DROP VIEW 解析失败: {}", e)))?
        .into_iter()
        .next()
        .ok_or_else(|| EngineError::general("empty ddl"))?;
    let names: Vec<String> = match &stmt {
        sqlparser::ast::Statement::Drop {
            object_type: sqlparser::ast::ObjectType::View,
            names,
            ..
        } => names
            .iter()
            .map(|n| n.0.last().map(|i| i.value.clone()).unwrap_or_default())
            .collect(),
        _ => return Err(EngineError::new(1064, "42000", "非 DROP VIEW 语句")),
    };
    let target = state
        .metadata()
        .get_schema(&schema)
        .map(|s| s.target_name.clone())
        .ok_or_else(|| EngineError::new(1049, "42000", format!("Unknown database '{}'", schema)))?;
    execute_on_target_in_schema(state, &target, &schema, sql).await?;
    commit_config(state, move |cfg| {
        if let Some(sc) = cfg.schemas.get_mut(&schema) {
            for n in &names {
                sc.views.remove(&n.to_lowercase());
            }
        }
        Ok(())
    })?;
    Ok(EngineOutcome::Ok {
        affected_rows: 0,
        last_insert_id: 0,
    })
}

/// 视图定义规范化：剥离数据库限定（视图元数据以逻辑视图名为键）。
fn normalize_view_sql(sql: &str, view_name: &str) -> String {
    let body = sql.trim().trim_end_matches(';').trim();
    // 仅当 SQL 以 "CREATE VIEW 名" 开头时规范化；其余原样
    body.to_string()
        // 保持原样即可：CREATE VIEW 通常无库限定；如带库限定由后端解释
        .replace(&format!("`{}`", view_name), &format!("`{}`", view_name))
}

// ---------------------------------------------------------------------------
// 物理执行辅助
// ---------------------------------------------------------------------------

/// 在指定 schema 为默认库的执行（视图等需库上下文语句：连接后先 USE）。
async fn execute_on_target_in_schema(
    state: &std::sync::Arc<ServerState>,
    target: &str,
    schema: &str,
    sql: &str,
) -> Result<(), EngineError> {
    let replica = state.replica();
    let ds = replica
        .resolve(target, true, false, false)
        .ok_or_else(|| EngineError::general(format!("no datasource for target {}", target)))?;
    let ds_cfg = replica
        .datasource_config(&ds)
        .ok_or_else(|| EngineError::general(format!("datasource {} missing", ds)))?;
    if !ds_cfg.db_type.eq_ignore_ascii_case("mysql") {
        tracing::warn!(datasource = %ds, db_type = %ds_cfg.db_type, target,
            "view DDL skipped for non-MySQL datasource (create views manually)");
        return Ok(());
    }
    let mut conn = state
        .conn_mgr
        .connect_direct(&ds_cfg)
        .await
        .map_err(|e| EngineError::general(e.to_string()))?;
    let _ = conn
        .query(&format!("USE `{}`", schema))
        .await
        .map_err(|e| EngineError::general(e.to_string()))?;
    let r = conn
        .query(sql)
        .await
        .map_err(|e| EngineError::general(e.to_string()))?;
    conn.quit().await;
    let _ = r;
    Ok(())
}

/// `RENAME TABLE a TO b` 自动化（对应 Java RenameTableSQLHandler）：
/// 1) 逐物理节点执行 RENAME（物理名嵌入逻辑名时替换，否则物理名保持、仅改配置映射）；
/// 2) 热更新配置（表名键 + 分片物理命名/单表 locality 中的逻辑名替换）。
async fn rename_table(
    state: &std::sync::Arc<ServerState>,
    schema_ctx: Option<&str>,
    sql: &str,
) -> Result<EngineOutcome, EngineError> {
    let pairs = mycat3_sql::parse_rename_table(sql)
        .ok_or_else(|| EngineError::new(1064, "42000", "RENAME TABLE 语法解析失败"))?;
    let schema = schema_ctx
        .ok_or_else(|| EngineError::new(1046, "3D000", "No database selected"))?
        .to_string();
    let meta = state.metadata();
    let schema_meta = meta
        .get_schema(&schema)
        .ok_or_else(|| EngineError::new(1049, "42000", format!("Unknown database '{}'", schema)))?;

    // 规划物理重命名：(target, 物理 schema, 旧物理名, 新物理名)
    let mut plans: Vec<(String, String, String, String)> = Vec::new();
    for (from, to) in &pairs {
        let from_parts: Vec<&str> = from.split('.').collect();
        if from_parts.len() == 2 && !from_parts[0].eq_ignore_ascii_case(&schema) {
            return Err(EngineError::general(format!(
                "跨库 RENAME 不支持: {}",
                from
            )));
        }
        let from_tbl = from_parts.last().unwrap_or(&"");
        let to_tbl = to.rsplit('.').next().unwrap_or_default();
        let tbl = schema_meta.get_table(from_tbl).ok_or_else(|| {
            EngineError::new(
                1146,
                "42S02",
                format!("Table '{}.{}' doesn't exist", schema, from_tbl),
            )
        })?;
        match tbl {
            TableMeta::Normal {
                target,
                schema: s,
                table: phys,
                ..
            } => {
                let ps = if s.is_empty() {
                    schema.clone()
                } else {
                    s.clone()
                };
                let old_phys = if phys.is_empty() {
                    from_tbl.to_string()
                } else {
                    phys.clone()
                };
                let new_phys = replace_name(&old_phys, from_tbl, to_tbl);
                plans.push((target.clone(), ps, old_phys, new_phys));
            }
            TableMeta::Global { target_schemas, .. } => {
                for (t, s) in target_schemas {
                    plans.push((
                        t.clone(),
                        s.clone(),
                        from_tbl.to_string(),
                        to_tbl.to_string(),
                    ));
                }
            }
            TableMeta::Sharding { nodes, .. } => {
                for n in nodes.iter() {
                    let new_phys = replace_name(&n.table_name, from_tbl, to_tbl);
                    plans.push((
                        n.target_name.clone(),
                        n.schema_name.clone(),
                        n.table_name.clone(),
                        new_phys,
                    ));
                }
            }
            TableMeta::View { .. } => {
                return Err(EngineError::general("视图不支持 RENAME TABLE"));
            }
        }
    }

    // 1. 物理 RENAME（物理名未嵌入逻辑名的节点跳过：仅更新配置映射）
    for (target, ps, old_phys, new_phys) in &plans {
        if old_phys.eq_ignore_ascii_case(new_phys) {
            continue;
        }
        let ddl = format!(
            "RENAME TABLE `{}`.`{}` TO `{}`.`{}`",
            ps, old_phys, ps, new_phys
        );
        execute_on_target(state, target, &ddl).await?;
    }

    // 2. 配置热更新（表名键 + 物理命名中的逻辑名替换）
    commit_config(state, move |cfg| {
        let sc = cfg
            .schemas
            .get_mut(&schema)
            .ok_or_else(|| EngineError::general("schema missing"))?;
        for (from, to) in &pairs {
            let from_tbl = from.rsplit('.').next().unwrap_or_default().to_lowercase();
            let to_tbl = to.rsplit('.').next().unwrap_or_default().to_string();
            if let Some(mut t) = sc.sharding_tables.remove(&from_tbl) {
                rename_in_partition(&mut t.partition, &from_tbl, &to_tbl);
                sc.sharding_tables.insert(to_tbl.clone(), t);
            } else if let Some(mut t) = sc.normal_tables.remove(&from_tbl) {
                if let Some(loc) = t.locality.as_mut() {
                    if let Some(pt) = loc.table_name.as_mut() {
                        if !pt.is_empty() {
                            *pt = replace_name(pt, &from_tbl, &to_tbl);
                        }
                    }
                }
                sc.normal_tables.insert(to_tbl.clone(), t);
            } else if let Some(t) = sc.global_tables.remove(&from_tbl) {
                sc.global_tables.insert(to_tbl.clone(), t);
            } else {
                return Err(EngineError::new(
                    1146,
                    "42S02",
                    format!("Table '{}.{}' doesn't exist", schema, from_tbl),
                ));
            }
        }
        Ok(())
    })?;
    Ok(EngineOutcome::Ok {
        affected_rows: 0,
        last_insert_id: 0,
    })
}

/// 物理命名中的逻辑表名替换（大小写不敏感；未嵌入逻辑名时保持原样）。
fn replace_name(phys: &str, from: &str, to: &str) -> String {
    match phys.to_lowercase().find(&from.to_lowercase()) {
        Some(pos) => {
            let mut s = String::with_capacity(phys.len() + to.len());
            s.push_str(&phys[..pos]);
            s.push_str(to);
            s.push_str(&phys[pos + from.len()..]);
            s
        }
        None => phys.to_string(),
    }
}

/// 分片物理命名（tableNames 模板与显式 data 行）中的逻辑表名替换。
fn rename_in_partition(p: &mut mycat3_config::ShardingBackendConfig, from: &str, to: &str) {
    for t in p.table_names.iter_mut() {
        *t = replace_name(t, from, to);
    }
    for row in p.data.iter_mut() {
        if let Some(serde_json::Value::String(v)) = row.get_mut(2) {
            let new_v = replace_name(v, from, to);
            *v = new_v;
        }
    }
}

/// ALTER TABLE 后从首个物理节点回读 SHOW CREATE TABLE，刷新配置 `create_sql`
///（对应 Java `AlterTableSQLHandler`：PrototypeService.getCreateTableSQLByJDBC 回读刷新）。
/// 尽力而为：物理 ALTER 已生效，回读失败仅告警（元数据稍后可重载配置修复）。
pub async fn refresh_create_sql_after_alter(
    state: &std::sync::Arc<ServerState>,
    schema_ctx: Option<&str>,
    sql: &str,
) {
    let norm = normalize_head(sql);
    if !norm.starts_with("alter table") {
        return;
    }
    let result: Result<(), EngineError> = async {
        let schema =
            schema_ctx.ok_or_else(|| EngineError::new(1046, "3D000", "no schema selected"))?;
        // 解析 ALTER 表名（物理执行已成功，此处解析失败仅放弃刷新）
        let stmt = sqlparser::parser::Parser::parse_sql(&sqlparser::dialect::MySqlDialect {}, sql)
            .ok()
            .and_then(|s| s.into_iter().next());
        let name = match stmt {
            Some(sqlparser::ast::Statement::AlterTable { name, .. }) => name,
            _ => return Ok(()),
        };
        let parts: Vec<String> = name
            .0
            .iter()
            .map(|i| i.value.trim_matches('`').to_string())
            .collect();
        let table_name = parts.last().cloned().unwrap_or_default();
        if table_name.is_empty() {
            return Ok(());
        }
        // 物理回读目标：分片表取首节点；单表/广播表取首节点
        let meta = state.metadata();
        let schema_meta = match meta.get_schema(schema) {
            Some(s) => s,
            None => return Ok(()),
        };
        let (target, ps, pt) = match schema_meta.get_table(&table_name) {
            Some(TableMeta::Normal {
                target,
                schema: s,
                table: phys,
                ..
            }) => (
                target.clone(),
                if s.is_empty() {
                    schema.to_string()
                } else {
                    s.clone()
                },
                if phys.is_empty() {
                    table_name.clone()
                } else {
                    phys.clone()
                },
            ),
            Some(TableMeta::Sharding { nodes, .. }) => {
                let n = match nodes.first() {
                    Some(n) => n,
                    None => return Ok(()),
                };
                (
                    n.target_name.clone(),
                    n.schema_name.clone(),
                    n.table_name.clone(),
                )
            }
            Some(TableMeta::Global { target_schemas, .. }) => {
                let (t, s) = match target_schemas.first() {
                    Some(x) => x,
                    None => return Ok(()),
                };
                (t.clone(), s.clone(), table_name.clone())
            }
            _ => return Ok(()),
        };
        let replica = state.replica();
        let ds = match replica.resolve(&target, true, false, false) {
            Some(d) => d,
            None => return Ok(()),
        };
        let ds_cfg = match replica.datasource_config(&ds) {
            Some(c) => c,
            None => return Ok(()),
        };
        // 非 MySQL 后端：物理 DDL 手工管理（Oracle），跳过回读
        if !ds_cfg.db_type.eq_ignore_ascii_case("mysql") {
            return Ok(());
        }
        let mut conn = state
            .conn_mgr
            .connect_direct(&ds_cfg)
            .await
            .map_err(|e| EngineError::general(e.to_string()))?;
        // 必须限定物理库：直连连接的默认库是数据源 URL 中的库（可能非物理 schema）
        let show_sql = format!("SHOW CREATE TABLE `{}`.`{}`", ps, pt);
        let create = match conn.query(&show_sql).await {
            Ok(QueryOutcome::Rows(rs)) => {
                let rows = rs.decode_rows();
                match rows.first().and_then(|r| r.get(1)).and_then(|v| v.as_ref()) {
                    Some(ddl) => String::from_utf8_lossy(ddl).to_string(),
                    None => {
                        conn.quit().await;
                        return Ok(());
                    }
                }
            }
            _ => {
                conn.quit().await;
                return Ok(());
            }
        };
        conn.quit().await;
        // 规范化：物理名 → 逻辑名；去 AUTO_INCREMENT 子句（逻辑 DDL 不携带）
        let create = create
            .replace(&format!("`{}`", pt), &format!("`{}`", table_name))
            .replace(&format!("`{}`", ps), &format!("`{}`", schema));
        let create = strip_auto_increment(&create);
        let schema_owned = schema.to_string();
        let table_lc = table_name.to_lowercase();
        commit_config(state, move |cfg| {
            let sc = cfg
                .schemas
                .get_mut(&schema_owned)
                .ok_or_else(|| EngineError::general("schema missing"))?;
            let key = sc
                .sharding_tables
                .keys()
                .chain(sc.normal_tables.keys())
                .chain(sc.global_tables.keys())
                .find(|k| k.eq_ignore_ascii_case(&table_lc))
                .cloned();
            match key {
                Some(k) if sc.sharding_tables.contains_key(&k) => {
                    sc.sharding_tables.get_mut(&k).unwrap().create_table_sql = Some(create);
                }
                Some(k) if sc.normal_tables.contains_key(&k) => {
                    sc.normal_tables.get_mut(&k).unwrap().create_table_sql = Some(create);
                }
                Some(k) if sc.global_tables.contains_key(&k) => {
                    sc.global_tables.get_mut(&k).unwrap().create_table_sql = Some(create);
                }
                _ => {}
            }
            Ok(())
        })
    }
    .await;
    if let Err(e) = result {
        tracing::warn!(error = %e, "ALTER 后回读 SHOW CREATE TABLE 刷新配置失败（物理 ALTER 已生效）");
    }
}

/// 移除 `AUTO_INCREMENT=n` 子句（逻辑 create_sql 不携带）。
fn strip_auto_increment(ddl: &str) -> String {
    const MARK: &str = "AUTO_INCREMENT=";
    let Some(pos) = ddl.to_uppercase().find(MARK) else {
        return ddl.to_string();
    };
    let end = ddl[pos + MARK.len()..]
        .find(|c: char| !c.is_ascii_digit())
        .map(|i| pos + MARK.len() + i)
        .unwrap_or(ddl.len());
    let mut s = String::with_capacity(ddl.len());
    s.push_str(&ddl[..pos]);
    s.push_str(&ddl[end..]);
    s
}

async fn execute_on_target(
    state: &std::sync::Arc<ServerState>,
    target: &str,
    sql: &str,
) -> Result<(), EngineError> {
    let replica = state.replica();
    let ds = replica
        .resolve(target, true, false, false)
        .ok_or_else(|| EngineError::general(format!("no datasource for target {}", target)))?;
    let ds_cfg = replica
        .datasource_config(&ds)
        .ok_or_else(|| EngineError::general(format!("datasource {} missing", ds)))?;
    // 非 MySQL 后端（Oracle 等）：物理 DDL 为 MySQL 方言，跳过执行并告警
    // （对应评估报告：物理建表由 DBA 手工按 Oracle DDL 创建）
    if !ds_cfg.db_type.eq_ignore_ascii_case("mysql") {
        tracing::warn!(datasource = %ds, db_type = %ds_cfg.db_type, target,
            "physical DDL skipped for non-MySQL datasource (create physical tables manually)");
        return Ok(());
    }
    let mut conn = state
        .conn_mgr
        .connect_direct(&ds_cfg)
        .await
        .map_err(|e| EngineError::general(e.to_string()))?;
    let r = conn
        .query(sql)
        .await
        .map_err(|e| EngineError::general(e.to_string()))?;
    conn.quit().await;
    match r {
        QueryOutcome::Ok(_) => Ok(()),
        QueryOutcome::Rows(_) => Ok(()),
    }
}

async fn execute_on_prototype(
    state: &std::sync::Arc<ServerState>,
    sql: &str,
) -> Result<(), EngineError> {
    let proto = state.metadata().prototype.clone();
    execute_on_target(state, &proto, sql).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_split_drds_suffix() {
        let (body, part) = split_drds_suffix(
            "CREATE TABLE t (id BIGINT PRIMARY KEY, name VARCHAR(32)) dbpartition by hash(id) dbpartitions 2 tbpartition by hash(id) tbpartitions 4",
        );
        assert_eq!(
            body,
            "CREATE TABLE t (id BIGINT PRIMARY KEY, name VARCHAR(32))"
        );
        assert_eq!(part.db_method.as_deref(), Some("hash(id)"));
        assert_eq!(part.db_partitions, Some(2));
        assert_eq!(part.tb_method.as_deref(), Some("hash(id)"));
        assert_eq!(part.tb_partitions, Some(4));
        assert!(!part.broadcast);
    }

    #[test]
    fn test_split_drds_broadcast() {
        let (body, part) = split_drds_suffix("CREATE TABLE d (id INT) BROADCAST");
        assert_eq!(body, "CREATE TABLE d (id INT)");
        assert!(part.broadcast);
    }

    #[test]
    fn test_split_no_suffix() {
        let (body, part) = split_drds_suffix("create table x (a int);");
        assert_eq!(body, "create table x (a int)");
        assert_eq!(part, DrdsPartition::default());
    }

    #[test]
    fn test_split_ignores_column_named_partition() {
        // 列定义括号内的 dbpartition 字样不应触发切分
        let (body, part) = split_drds_suffix("CREATE TABLE t (dbpartition VARCHAR(10), x INT)");
        assert!(body.contains("dbpartition VARCHAR(10)"));
        assert_eq!(part, DrdsPartition::default());
    }

    #[test]
    fn test_auto_hash_properties_defaults() {
        let drds = DrdsPartition {
            db_method: Some("hash(id)".into()),
            db_partitions: Some(2),
            tb_method: Some("hash(id)".into()),
            tb_partitions: Some(4),
            broadcast: false,
        };
        // storeNum=2（c0/c1）：total=8，storeDbNum=4 → targetIndex=index/4 → 前4片 c0，后4片 c1
        let props = auto_hash_properties("db2", "t", &drds, &["c0".into(), "c1".into()]).unwrap();
        assert_eq!(props["storeNum"], "2");
        assert_eq!(props["dbNum"], "2");
        assert_eq!(props["tableNum"], "4");
        assert_eq!(props["dbMethod"], "hash(id)");
        // db/table 方法相同且 flatten 家族 → ${index} 模板
        assert_eq!(
            props["mappingFormat"],
            "c${targetIndex}/db2_${dbIndex}/t_${index}"
        );
        let nodes = mycat3_router::expand_auto_nodes(&props).unwrap();
        assert_eq!(nodes.len(), 8);
        assert_eq!(nodes[0].target_name, "c0");
        assert_eq!(nodes[0].schema_name, "db2_0");
        assert_eq!(nodes[0].table_name, "t_0");
        assert_eq!(nodes[4].target_name, "c1");
        assert_eq!(nodes[7].table_name, "t_7");
    }

    #[test]
    fn test_auto_hash_properties_single_store() {
        // 单集群仅分表：dbNum=1（无 dbpartition），schema 无后缀
        let drds = DrdsPartition {
            db_method: None,
            db_partitions: None,
            tb_method: Some("hash(id)".into()),
            tb_partitions: Some(4),
            broadcast: false,
        };
        let props = auto_hash_properties("db1", "orders", &drds, &["c0".into()]).unwrap();
        assert_eq!(props["dbNum"], "1");
        assert_eq!(props["tableNum"], "4");
        assert_eq!(
            props["mappingFormat"],
            "c${targetIndex}/db1/orders_${index}"
        );
        let nodes = mycat3_router::expand_auto_nodes(&props).unwrap();
        assert_eq!(nodes.len(), 4);
        assert!(nodes
            .iter()
            .all(|n| n.target_name == "c0" && n.schema_name == "db1"));
    }

    #[test]
    fn test_normalize_create_table() {
        let s = normalize_create_table("CREATE TABLE `orders` (id BIGINT)");
        assert!(s.contains("IF NOT EXISTS"));
        assert_eq!(s, "CREATE TABLE IF NOT EXISTS __TABLE__ (id BIGINT)");
        // db.table 形式
        let s = normalize_create_table("CREATE TABLE db1.t (id BIGINT)");
        assert_eq!(s, "CREATE TABLE IF NOT EXISTS __TABLE__ (id BIGINT)");
        // 已有 IF NOT EXISTS 不重复
        let s = normalize_create_table("CREATE TABLE IF NOT EXISTS t2 (a INT)");
        assert_eq!(s.matches("IF NOT EXISTS").count(), 1);
        assert!(s.contains("__TABLE__"));
    }

    #[test]
    fn test_extract_hints() {
        let v = extract_mycat_json_hint(r#"CREATE DATABASE db2 /*!mycat:{"targetName":"c0"}*/"#)
            .unwrap();
        assert_eq!(v["targetName"], "c0");
        let v = extract_create_table_hint(r#"/*!+mycat:createTable{"type":"hash","dbNum":"2","tableNum":"4","dbMethod":"hash(id)","tableMethod":"hash(id)","mappingFormat":"c${targetIndex}/db_${dbIndex}/t_${tableIndex}","storeNum":"1"}*/ CREATE TABLE t (id int)"#).unwrap();
        assert_eq!(v["type"], "hash");
        assert_eq!(v["dbNum"], "2");
    }

    #[test]
    fn test_tokenize() {
        let t = tokenize_head("DROP TABLE IF EXISTS db1.orders;", 8);
        assert_eq!(t, vec!["DROP", "TABLE", "IF", "EXISTS", "db1.orders"]);
        let t = tokenize_head("create user 'bob' identified by 'pw'", 8);
        assert_eq!(t[2], "'bob'");
    }

    #[test]
    fn test_remove_comments() {
        assert_eq!(remove_comments("select 1 -- x\n").trim(), "select 1");
        assert_eq!(remove_comments("select /* c */ 'a--b'"), "select  'a--b'");
    }

    #[test]
    fn test_parse_create_database() {
        let (name, hint) = parse_create_database(
            "CREATE DATABASE IF NOT EXISTS db2 /*!mycat:{\"targetName\":\"c0\"}*/",
        )
        .unwrap();
        assert_eq!(name, "db2");
        assert_eq!(hint.unwrap()["targetName"], "c0");
        let (name, hint) = parse_create_database("create database `MyDb`").unwrap();
        assert_eq!(name, "MyDb");
        assert!(hint.is_none());
    }

    #[test]
    fn test_rebuild_suffix() {
        let p = DrdsPartition {
            db_method: Some("hash(id)".into()),
            db_partitions: Some(2),
            tb_method: None,
            tb_partitions: None,
            broadcast: false,
        };
        assert_eq!(
            rebuild_drds_suffix(&p),
            " dbpartition by hash(id) dbpartitions 2"
        );
    }
}

// ---------------------------------------------------------------------------
// 配置快照备份（对应 Pro 增强：commit 前自动备份，HTTP/SQL 可恢复）
// ---------------------------------------------------------------------------

const BACKUP_KEEP: usize = 10;

fn backup_dir(home: &std::path::Path) -> std::path::PathBuf {
    home.join("backup")
}

/// 配置快照备份：当前配置写入 <home>/backup/<epoch_ms>/，保留最近 BACKUP_KEEP 份。
pub fn backup_config_snapshot(
    home: &std::path::Path,
    cfg: &std::sync::Arc<MycatRouterConfig>,
) -> Result<String, EngineError> {
    let ts = crate::state::epoch_millis().to_string();
    let dir = backup_dir(home).join(&ts);
    FileConfigStorage::new(&dir)
        .save_all_sync(cfg)
        .map_err(|e| EngineError::general(format!("backup failed: {}", e)))?;
    // 清理旧备份
    if let Ok(rd) = std::fs::read_dir(backup_dir(home)) {
        let mut entries: Vec<(u64, std::path::PathBuf)> = rd
            .flatten()
            .filter(|e| e.path().is_dir())
            .filter_map(|e| {
                e.file_name()
                    .to_str()
                    .and_then(|n| n.parse::<u64>().ok())
                    .map(|t| (t, e.path()))
            })
            .collect();
        entries.sort_by(|a, b| b.0.cmp(&a.0)); // 新在前
        for (_, p) in entries.into_iter().skip(BACKUP_KEEP) {
            let _ = std::fs::remove_dir_all(p);
        }
    }
    Ok(ts)
}

/// 备份时间戳列表（新在前）。
pub fn list_backups(state: &std::sync::Arc<ServerState>) -> Result<Vec<String>, EngineError> {
    let home = state
        .home()
        .ok_or_else(|| EngineError::general("config home not set"))?;
    let mut v: Vec<String> = Vec::new();
    if let Ok(rd) = std::fs::read_dir(backup_dir(&home)) {
        for e in rd.flatten() {
            if e.path().is_dir() {
                if let Some(n) = e.file_name().to_str() {
                    v.push(n.to_string());
                }
            }
        }
    }
    v.sort_by(|a, b| b.cmp(a));
    Ok(v)
}

/// 从备份恢复：备份内容写入 home 并热重载。
pub async fn restore_backup(
    state: &std::sync::Arc<ServerState>,
    ts: &str,
) -> Result<(), EngineError> {
    let home = state
        .home()
        .ok_or_else(|| EngineError::general("config home not set"))?;
    let src = backup_dir(&home).join(ts);
    if !src.exists() {
        return Err(EngineError::general(format!("备份 {} 不存在", ts)));
    }
    let cfg = FileConfigStorage::new(&src)
        .load_all()
        .map_err(|e| EngineError::general(format!("读取备份失败: {}", e)))?;
    Metadata::build(&cfg).map_err(|e| EngineError::general(format!("备份配置无效: {}", e)))?;
    FileConfigStorage::new(&home)
        .save_all_sync(&cfg)
        .map_err(|e| EngineError::general(format!("写回配置失败: {}", e)))?;
    state
        .reload(cfg, Some(std::time::Duration::from_secs(5)))
        .map_err(|e| EngineError::general(format!("reload failed: {}", e)))?;
    tracing::info!(ts, "config restored from backup");
    Ok(())
}
