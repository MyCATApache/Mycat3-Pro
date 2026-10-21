//! SQL 路由分析与重写。
//!
//! 对应 Mycat3 的 `MycatdbCommand` + `HackRouter` + `ShardingSQLHandler` 的
//! 单表下推路径（Pro 版以 sqlparser 代替 Druid/Calcite）：
//! 1. 解析 SQL → 提取逻辑表引用
//! 2. 按元数据判定表类型（NORMAL/GLOBAL/SHARDING）
//! 3. 提取分片列条件（=、IN、BETWEEN、>、<）交给分片函数裁剪节点
//! 4. 重写 AST 中的逻辑表名为物理表名，按节点生成 `QueryBackendTask`
//! 5. 多节点 SELECT 生成归并计划（ORDER BY/LIMIT 归并排序、COUNT/SUM/MAX/MIN 聚合归并）

use std::collections::BTreeMap;
use std::ops::ControlFlow;

use sqlparser::ast::{
    Expr, FromTable, FunctionArg, FunctionArgExpr, GroupByExpr, Ident, ObjectName,
    Offset as SqlOffset, Query, SelectItem, SetExpr, Statement, TableFactor, TableWithJoins, Use,
    Value, VisitMut, VisitorMut,
};
use sqlparser::dialect::MySqlDialect;
use sqlparser::parser::Parser;
use thiserror::Error;

use mycat3_router::{IndexDataNode, Operator, RangeVariable};

use crate::hint::{parse_hint, Hint};
use crate::meta::{Metadata, SchemaMeta, TableMeta};

#[derive(Debug, Error)]
pub enum SqlError {
    #[error("SQL parse error: {0}")]
    Parse(String),
    #[error("unknown database: {0}")]
    UnknownDatabase(String),
    #[error("no database selected")]
    NoDatabase,
    #[error("{0}")]
    Unsupported(String),
    #[error("route error: {0}")]
    Route(String),
}

pub type SqlResult<T> = Result<T, SqlError>;

/// 语句类型。对应 `MycatdbCommand.sqlHandlerMap` 的 SQLHandler 分类。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StmtType {
    Select,
    Insert,
    Update,
    Delete,
    Replace,
    Truncate,
    Ddl,
    Set,
    Use,
    Show,
    TxBegin,
    TxCommit,
    TxRollback,
    /// SAVEPOINT name（转发到事务中全部固定连接）
    TxSavepoint,
    /// RELEASE SAVEPOINT name
    TxReleaseSavepoint,
    /// ROLLBACK TO SAVEPOINT name
    TxRollbackSavepoint,
    Explain,
    /// 其他语句：透传到默认 target（Java: 未知 SHOW → prototype）
    Other,
}

impl StmtType {
    /// 写操作（事务中必须走主库，对应 ReceiverImpl 的 master 判定）
    pub fn is_write(&self) -> bool {
        matches!(
            self,
            StmtType::Insert
                | StmtType::Update
                | StmtType::Delete
                | StmtType::Replace
                | StmtType::Truncate
                | StmtType::Ddl
        )
    }
}

/// 路由单元。对应 `io.mycat.QueryBackendTask`。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackendTask {
    /// 逻辑 target 名（集群名或数据源名）
    pub target_name: String,
    /// 重写后的物理 SQL
    pub sql: String,
}

impl BackendTask {
    pub fn new(target: impl Into<String>, sql: impl Into<String>) -> Self {
        BackendTask {
            target_name: target.into(),
            sql: sql.into(),
        }
    }
}

/// 多节点结果的归并方式。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MergeOp {
    /// 行拼接（可含全局排序/limit）
    Concat,
    /// COUNT：各行第一列求和
    CountSum,
    /// SUM：数值求和
    NumericSum,
    /// MAX
    Max,
    /// MIN
    Min,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderKey {
    /// 结果集中的列下标（0 基）；投影为 * 时无法静态确定，改存 name 由执行层解析
    pub index: Option<usize>,
    /// ORDER BY 列名（小写），执行层按结果集列定义解析
    pub name: Option<String>,
    pub desc: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LimitSpec {
    pub offset: u64,
    pub count: u64,
}

/// 多节点归并计划。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MergePlan {
    pub op: MergeOp,
    /// 全局排序键（Concat 时生效）
    pub order_by: Vec<OrderKey>,
    /// 全局 offset/limit（Concat 时生效；下推 SQL 已改写为 LIMIT offset+count）
    pub limit: Option<LimitSpec>,
}

/// 单条语句的路由结果。
#[derive(Debug, Clone)]
pub struct RouteResult {
    pub stmt_type: StmtType,
    /// 空 tasks 表示该语句由前端本地处理（SET/USE/事务控制/EXPLAIN 等）
    pub tasks: Vec<BackendTask>,
    /// tasks > 1 时的归并计划
    pub merge: Option<MergePlan>,
    /// USE 语句的目标 schema
    pub use_schema: Option<String>,
    /// hint（供执行层选择主/从、强制 target）
    pub hint: Option<Hint>,
    /// EXPLAIN 输出行
    pub explain_rows: Vec<Vec<String>>,
}

impl RouteResult {
    pub fn empty(stmt_type: StmtType) -> Self {
        RouteResult {
            stmt_type,
            tasks: Vec::new(),
            merge: None,
            use_schema: None,
            hint: None,
            explain_rows: Vec::new(),
        }
    }
}

// ---------------------------------------------------------------------------
// 顶层入口
// ---------------------------------------------------------------------------

/// 分析 SQL 文本（可含 hint 与多语句），返回每条语句的路由结果。
///
/// 对应 `MycatdbCommand.executeQuery` 的 Druid parse + 逐句 execute 流程。
pub fn analyse(
    sql: &str,
    default_schema: Option<&str>,
    meta: &Metadata,
) -> SqlResult<Vec<RouteResult>> {
    let (hint, sql) = parse_hint(sql);
    let sql = sql.trim();
    if sql.is_empty() {
        return Err(SqlError::Parse("Query was empty".into()));
    }

    // hint 强制 target：整体透传（HackRouter 的 TARGET 语义）
    if let Some(h) = &hint {
        if let Some(target) = &h.target {
            let inner = h.inner_sql.clone().unwrap_or_else(|| sql.to_string());
            return Ok(vec![RouteResult {
                stmt_type: StmtType::Other,
                tasks: vec![BackendTask::new(target.clone(), inner)],
                merge: None,
                use_schema: None,
                hint: hint.clone(),
                explain_rows: Vec::new(),
            }]);
        }
        if let Some(inner) = &h.inner_sql {
            // sql= 透传：仍然走路由（schema 可被 hint 覆盖）
            let schema = h.schema.as_deref().or(default_schema);
            let mut results = analyse_raw(inner, schema, meta)?;
            for r in results.iter_mut() {
                r.hint = hint.clone();
            }
            return Ok(results);
        }
    }

    let schema = hint
        .as_ref()
        .and_then(|h| h.schema.clone())
        .or_else(|| default_schema.map(|s| s.to_string()));
    let mut results = analyse_raw(sql, schema.as_deref(), meta)?;
    if let Some(h) = &hint {
        for r in results.iter_mut() {
            r.hint = Some(h.clone());
        }
    }
    Ok(results)
}

fn analyse_raw(
    sql: &str,
    default_schema: Option<&str>,
    meta: &Metadata,
) -> SqlResult<Vec<RouteResult>> {
    // sqlparser 0.53 不解析 MySQL 的 `DROP INDEX idx ON tbl`：文本级预处理
    //（对应 Java DropIndexSQLHandler 的扇出语义）
    if let Some((index, table)) = parse_drop_index(sql) {
        return Ok(vec![drop_index_route(
            &index,
            &table,
            default_schema,
            meta,
        )?]);
    }
    // sqlparser 0.53 不解析 MySQL 的 `CHECK TABLE tbl`：文本级预处理，
    // 分片/广播/单表扇出到全部物理节点（对应 Java MySQLCheckHandler）
    if let Some(tables) = parse_check_table(sql) {
        return check_table_route(&tables, default_schema, meta);
    }
    let stmts =
        Parser::parse_sql(&MySqlDialect {}, sql).map_err(|e| SqlError::Parse(e.to_string()))?;
    let mut out = Vec::with_capacity(stmts.len());
    for stmt in stmts {
        out.push(route_statement(&stmt, default_schema, meta, sql)?);
    }
    Ok(out)
}

/// `CHECK TABLE tbl [, tbl2 ...]` 文本级解析（引号感知；返回逻辑表名列表）。
pub fn parse_check_table(sql: &str) -> Option<Vec<String>> {
    let s = sql.trim().trim_end_matches(';');
    if !s.to_uppercase().starts_with("CHECK TABLE ") {
        return None;
    }
    let rest = &s["CHECK TABLE".len()..];
    let mut tables = Vec::new();
    let mut cur = String::new();
    let mut quote: Option<char> = None;
    let mut word = false;
    for ch in rest.chars() {
        match quote {
            Some(q) => {
                if ch == q {
                    quote = None;
                } else {
                    cur.push(ch);
                }
            }
            None => match ch {
                '`' | '"' => {
                    quote = Some(ch);
                    word = true;
                }
                c if c.is_whitespace() || c == ',' => {
                    if word && !cur.is_empty() {
                        tables.push(std::mem::take(&mut cur));
                        word = false;
                    }
                }
                _ => {
                    cur.push(ch);
                    word = true;
                }
            },
        }
    }
    if word && !cur.is_empty() {
        tables.push(cur);
    }
    if tables.is_empty() {
        None
    } else {
        Some(tables)
    }
}

/// CHECK TABLE 路由：每张表按元数据扇出物理节点（各节点执行物理 CHECK TABLE，
/// 结果归并拼接 TABLE/Op/Msg_type/Msg_text 四列）。
fn check_table_route(
    tables: &[String],
    default_schema: Option<&str>,
    meta: &Metadata,
) -> SqlResult<Vec<RouteResult>> {
    let mut tasks = Vec::new();
    for t in tables {
        let obj = ObjectName(
            t.split('.')
                .map(|p| Ident::new(p.trim().to_string()))
                .collect(),
        );
        let (schema_name, logical) = resolve_name(&obj, default_schema)?;
        let schema_meta = meta
            .get_schema(&schema_name)
            .ok_or_else(|| SqlError::UnknownDatabase(schema_name.clone()))?;
        match schema_meta.get_table(&logical) {
            Some(TableMeta::Sharding { nodes, .. }) => {
                for n in nodes.iter() {
                    tasks.push(BackendTask::new(
                        n.target_name.clone(),
                        format!("CHECK TABLE `{}`.`{}`", n.schema_name, n.table_name),
                    ));
                }
            }
            Some(TableMeta::Global { target_schemas, .. }) => {
                for (tg, s) in target_schemas {
                    tasks.push(BackendTask::new(
                        tg.clone(),
                        format!("CHECK TABLE `{}`.`{}`", s, logical),
                    ));
                }
            }
            Some(TableMeta::Normal {
                target,
                schema,
                table: phys,
                ..
            }) => {
                let (ps, pt) = if schema.is_empty() {
                    (schema_name.clone(), logical.clone())
                } else {
                    (schema.clone(), phys.clone())
                };
                tasks.push(BackendTask::new(
                    target.clone(),
                    format!("CHECK TABLE `{}`.`{}`", ps, pt),
                ));
            }
            _ => {
                tasks.push(BackendTask::new(
                    schema_meta.target_name.clone(),
                    format!("CHECK TABLE `{}`", t),
                ));
            }
        }
    }
    Ok(vec![RouteResult {
        stmt_type: StmtType::Other,
        tasks,
        ..RouteResult::empty(StmtType::Other)
    }])
}

/// `DROP INDEX [IF EXISTS] idx ON [schema.]tbl` 文本级解析。
/// 返回 (索引名, 表名)；非该语法返回 None。
pub(crate) fn parse_drop_index(sql: &str) -> Option<(String, String)> {
    let s = sql.trim();
    let upper = s.to_uppercase();
    if !upper.starts_with("DROP INDEX ") {
        return None;
    }
    // 粗切：DROP INDEX <idx> ON <table>
    let after = &s["DROP INDEX".len()..];
    let after = after.trim_start();
    let after = if after.to_uppercase().starts_with("IF EXISTS ") {
        &after["IF EXISTS".len()..].trim_start()
    } else {
        after
    };
    let on_pos = after
        .to_uppercase()
        .find(" ON ")
        .map(|i| i)
        .or_else(|| after.to_uppercase().find("\nON "))
        .or_else(|| after.to_uppercase().find(" ON\n"))?;
    let index = after[..on_pos].trim().trim_matches('`').trim();
    let table = after[on_pos + " ON".len()..].trim().trim_end_matches(';');
    let table = table.trim();
    if index.is_empty() || table.is_empty() {
        return None;
    }
    Some((index.to_string(), table.trim_matches('`').to_string()))
}

/// DROP INDEX 路由：分片/广播/单表按物理节点扇出，自动补 IF EXISTS。
fn drop_index_route(
    index: &str,
    table: &str,
    default_schema: Option<&str>,
    meta: &Metadata,
) -> SqlResult<RouteResult> {
    let table_obj = ObjectName(
        table
            .split('.')
            .map(|p| Ident::new(p.trim().to_string()))
            .collect(),
    );
    let (schema_name, logical) = resolve_name(&table_obj, default_schema)?;
    let sql_for = |ps: &str, pt: &str| {
        // MySQL 不支持 DROP INDEX IF EXISTS（MariaDB 扩展）：与原生行为一致
        format!("DROP INDEX `{}` ON `{}`.`{}`", index, ps, pt)
    };
    let schema_meta = meta
        .get_schema(&schema_name)
        .ok_or_else(|| SqlError::UnknownDatabase(schema_name.clone()))?;
    let mut tasks = Vec::new();
    match schema_meta.get_table(&logical) {
        Some(TableMeta::Sharding { nodes, .. }) => {
            for n in nodes.iter() {
                tasks.push(BackendTask::new(
                    n.target_name.clone(),
                    sql_for(&n.schema_name, &n.table_name),
                ));
            }
        }
        Some(TableMeta::Global { target_schemas, .. }) => {
            for (t, s) in target_schemas {
                tasks.push(BackendTask::new(t.clone(), sql_for(s, &logical)));
            }
        }
        Some(TableMeta::Normal {
            target,
            schema,
            table: phys,
            ..
        }) => {
            let (ps, pt) = if schema.is_empty() {
                (schema_name.clone(), logical.clone())
            } else {
                (schema.clone(), phys.clone())
            };
            tasks.push(BackendTask::new(target.clone(), sql_for(&ps, &pt)));
        }
        _ => tasks.push(BackendTask::new(
            schema_meta.target_name.clone(),
            format!("DROP INDEX IF EXISTS `{}` ON `{}`", index, table),
        )),
    }
    Ok(RouteResult {
        stmt_type: StmtType::Ddl,
        tasks,
        ..RouteResult::empty(StmtType::Ddl)
    })
}

// ---------------------------------------------------------------------------
// 语句分派（对应 sqlHandlerMap）
// ---------------------------------------------------------------------------

fn route_statement(
    stmt: &Statement,
    default_schema: Option<&str>,
    meta: &Metadata,
    raw_sql: &str,
) -> SqlResult<RouteResult> {
    match stmt {
        Statement::Query(q) => route_select(q, default_schema, meta),
        Statement::Insert(insert) => route_insert(insert, default_schema, meta),
        Statement::Update {
            table, selection, ..
        } => route_update(table, selection.as_ref(), stmt, default_schema, meta),
        Statement::Delete(delete) => route_delete(delete, default_schema, meta),
        Statement::SetVariable { .. } => Ok(RouteResult::empty(StmtType::Set)),
        Statement::StartTransaction { .. } => Ok(RouteResult::empty(StmtType::TxBegin)),
        Statement::Commit { .. } => Ok(RouteResult::empty(StmtType::TxCommit)),
        Statement::Rollback {
            savepoint: Some(name),
            ..
        } => {
            let mut r = RouteResult::empty(StmtType::TxRollbackSavepoint);
            r.use_schema = Some(name.value.clone()); // 借用字段携带 savepoint 名
            Ok(r)
        }
        Statement::Rollback { .. } => Ok(RouteResult::empty(StmtType::TxRollback)),
        Statement::Savepoint { name } => {
            let mut r = RouteResult::empty(StmtType::TxSavepoint);
            r.use_schema = Some(name.value.clone());
            Ok(r)
        }
        Statement::ReleaseSavepoint { name } => {
            let mut r = RouteResult::empty(StmtType::TxReleaseSavepoint);
            r.use_schema = Some(name.value.clone());
            Ok(r)
        }
        Statement::Use(u) => Ok(RouteResult {
            stmt_type: StmtType::Use,
            use_schema: Some(use_name(u)),
            ..RouteResult::empty(StmtType::Use)
        }),
        Statement::Explain { statement, .. } => {
            let inner = route_statement(statement, default_schema, meta, raw_sql)?;
            let mut rows = Vec::new();
            for t in &inner.tasks {
                rows.push(vec![t.target_name.clone(), t.sql.clone()]);
            }
            Ok(RouteResult {
                stmt_type: StmtType::Explain,
                explain_rows: rows,
                ..RouteResult::empty(StmtType::Explain)
            })
        }
        Statement::Truncate { table_names, .. } => {
            let mut tasks = Vec::new();
            for t in table_names {
                let (schema_name, table) = resolve_name(&t.name, default_schema)?;
                tasks.extend(fanout_write_tasks(
                    &schema_name,
                    &table,
                    meta,
                    stmt,
                    default_schema,
                )?);
            }
            Ok(RouteResult {
                stmt_type: StmtType::Truncate,
                tasks,
                ..RouteResult::empty(StmtType::Truncate)
            })
        }
        Statement::CreateTable(ct) => {
            let (schema_name, table) = resolve_name(&ct.name, default_schema)?;
            let tasks = fanout_write_tasks(&schema_name, &table, meta, stmt, default_schema)?;
            Ok(RouteResult {
                stmt_type: StmtType::Ddl,
                tasks,
                ..RouteResult::empty(StmtType::Ddl)
            })
        }
        Statement::Drop {
            object_type: sqlparser::ast::ObjectType::Table,
            names,
            ..
        } => {
            let mut tasks = Vec::new();
            for n in names {
                let (schema_name, table) = resolve_name(n, default_schema)?;
                tasks.extend(fanout_write_tasks(
                    &schema_name,
                    &table,
                    meta,
                    stmt,
                    default_schema,
                )?);
            }
            Ok(RouteResult {
                stmt_type: StmtType::Ddl,
                tasks,
                ..RouteResult::empty(StmtType::Ddl)
            })
        }
        Statement::Drop {
            object_type: sqlparser::ast::ObjectType::Index,
            names,
            ..
        } => {
            // 标准 SQL `DROP INDEX idx`（sqlparser 可解析的形式）：透传默认 target。
            // MySQL 方言 `DROP INDEX idx ON tbl` 在 analyse_raw 前置文本级解析
            //（见 parse_drop_index / drop_index_route）。
            if names.len() >= 2 {
                let table_name = names.last().unwrap();
                let (schema_name, table) = resolve_name(table_name, default_schema)?;
                let tasks = fanout_index_tasks(stmt, &schema_name, &table, meta, default_schema)?;
                Ok(RouteResult {
                    stmt_type: StmtType::Ddl,
                    tasks,
                    ..RouteResult::empty(StmtType::Ddl)
                })
            } else {
                Ok(RouteResult {
                    stmt_type: StmtType::Ddl,
                    tasks: vec![BackendTask::new(
                        default_target(default_schema, meta),
                        raw_sql.to_string(),
                    )],
                    ..RouteResult::empty(StmtType::Ddl)
                })
            }
        }
        Statement::CreateIndex(idx) => {
            // 分片表建索引扇出全部物理节点（对应 Java CreateIndexSQLHandler；
            // 注意 MySQL 不支持 CREATE INDEX IF NOT EXISTS——那是 MariaDB 扩展，
            // 重复建索引由后端返回 1061，与原生 MySQL 行为一致）
            let (schema_name, table) = resolve_name(&idx.table_name, default_schema)?;
            let tasks = fanout_index_tasks(stmt, &schema_name, &table, meta, default_schema)?;
            Ok(RouteResult {
                stmt_type: StmtType::Ddl,
                tasks,
                ..RouteResult::empty(StmtType::Ddl)
            })
        }
        Statement::Drop { .. } => {
            // DROP VIEW/INDEX/FUNCTION/SCHEMA 等：透传到默认 target
            Ok(RouteResult {
                stmt_type: StmtType::Ddl,
                tasks: vec![BackendTask::new(
                    default_target(default_schema, meta),
                    raw_sql.to_string(),
                )],
                ..RouteResult::empty(StmtType::Ddl)
            })
        }
        Statement::CreateDatabase { .. } => {
            // 逻辑库操作：物理 SQL 发到 prototype（配置层由 server 处理）
            Ok(RouteResult {
                stmt_type: StmtType::Ddl,
                tasks: vec![BackendTask::new(
                    meta.prototype.clone(),
                    raw_sql.to_string(),
                )],
                ..RouteResult::empty(StmtType::Ddl)
            })
        }
        Statement::AlterTable { name, .. } => {
            let (schema_name, table) = resolve_name(name, default_schema)?;
            let tasks = fanout_write_tasks(&schema_name, &table, meta, stmt, default_schema)?;
            Ok(RouteResult {
                stmt_type: StmtType::Ddl,
                tasks,
                ..RouteResult::empty(StmtType::Ddl)
            })
        }
        Statement::ShowTables { .. }
        | Statement::ShowColumns { .. }
        | Statement::ShowFunctions { .. }
        | Statement::ShowVariable { .. }
        | Statement::ShowVariables { .. }
        | Statement::ShowStatus { .. }
        | Statement::ShowCreate { .. }
        | Statement::ShowDatabases { .. }
        | Statement::ShowCollation { .. } => Ok(RouteResult::empty(StmtType::Show)),
        _ => {
            // 未知语句：透传到当前 schema 的默认 target（Java: prototype）
            let target = default_target(default_schema, meta);
            Ok(RouteResult {
                stmt_type: StmtType::Other,
                tasks: vec![BackendTask::new(target, raw_sql.to_string())],
                ..RouteResult::empty(StmtType::Other)
            })
        }
    }
}

fn use_name(u: &Use) -> String {
    let name = match u {
        Use::Object(n)
        | Use::Catalog(n)
        | Use::Schema(n)
        | Use::Database(n)
        | Use::Warehouse(n)
        | Use::Role(n) => n,
        Use::SecondaryRoles(_) | Use::Default => return String::new(),
    };
    name.0.last().map(|i| i.value.clone()).unwrap_or_default()
}

fn default_target(default_schema: Option<&str>, meta: &Metadata) -> String {
    default_schema
        .and_then(|s| meta.get_schema(s))
        .map(|s| s.target_name.clone())
        .unwrap_or_else(|| meta.prototype.clone())
}

/// 解析 ObjectName → (逻辑 schema 名, 表名)。
fn resolve_name(name: &ObjectName, default_schema: Option<&str>) -> SqlResult<(String, String)> {
    let parts: Vec<String> = name.0.iter().map(|i| i.value.clone()).collect();
    match parts.len() {
        1 => {
            let schema = default_schema.ok_or(SqlError::NoDatabase)?.to_string();
            Ok((schema, parts.into_iter().next().unwrap()))
        }
        2 => Ok((parts[0].clone(), parts[1].clone())),
        _ => Err(SqlError::Unsupported(format!(
            "unsupported table name: {}",
            name
        ))),
    }
}

fn get_schema<'a>(meta: &'a Metadata, schema: &str) -> SqlResult<&'a SchemaMeta> {
    meta.get_schema(schema)
        .ok_or_else(|| SqlError::UnknownDatabase(schema.to_string()))
}

// ---------------------------------------------------------------------------
// 写语句（DDL/TRUNCATE/DROP）扇出
// ---------------------------------------------------------------------------

/// 生成写扇出任务：
/// - SHARDING：每个物理节点重写后执行
/// - GLOBAL：每个广播 target 执行（表名相同）
/// - NORMAL/未知：默认 target 执行
fn fanout_write_tasks(
    schema_name: &str,
    table: &str,
    meta: &Metadata,
    stmt: &Statement,
    default_schema: Option<&str>,
) -> SqlResult<Vec<BackendTask>> {
    fanout_tasks(
        stmt,
        schema_name,
        table,
        meta,
        default_schema,
        |s, ps, pt| rewrite_stmt(s, schema_name, table, ps, pt),
    )
}

/// 通用扇出：按表元数据逐物理节点生成任务，每节点 SQL 由 `rewrite` 闭包构造。
fn fanout_tasks(
    stmt: &Statement,
    schema_name: &str,
    table: &str,
    meta: &Metadata,
    default_schema: Option<&str>,
    rewrite: impl Fn(&Statement, &str, &str) -> String,
) -> SqlResult<Vec<BackendTask>> {
    let schema_meta = match meta.get_schema(schema_name) {
        Some(s) => s,
        None => {
            return Ok(vec![BackendTask::new(
                default_target(default_schema, meta),
                stmt.to_string(),
            )])
        }
    };
    match schema_meta.get_table(table) {
        Some(TableMeta::Sharding { nodes, .. }) => Ok(nodes
            .iter()
            .map(|node| {
                BackendTask::new(
                    node.target_name.clone(),
                    rewrite(stmt, &node.schema_name, &node.table_name),
                )
            })
            .collect()),
        Some(TableMeta::Global { target_schemas, .. }) => Ok(target_schemas
            .iter()
            .map(|(t, s)| BackendTask::new(t.clone(), rewrite(stmt, s, table)))
            .collect()),
        Some(TableMeta::Normal {
            target,
            schema,
            table: phys,
            ..
        }) => {
            let (phys_schema, phys_table) = if schema.is_empty() {
                (schema_name.to_string(), table.to_string())
            } else {
                (schema.clone(), phys.clone())
            };
            Ok(vec![BackendTask::new(
                target.clone(),
                rewrite(stmt, &phys_schema, &phys_table),
            )])
        }
        _ => Ok(vec![BackendTask::new(
            schema_meta.target_name.clone(),
            stmt.to_string(),
        )]),
    }
}

/// CREATE/DROP INDEX 扇出：**表名**按节点重写、索引名保持逻辑名
/// （索引名仅需表内唯一，各物理表独立，无需加节点后缀）。
fn fanout_index_tasks(
    stmt: &Statement,
    schema_name: &str,
    table: &str,
    meta: &Metadata,
    default_schema: Option<&str>,
) -> SqlResult<Vec<BackendTask>> {
    fanout_tasks(
        stmt,
        schema_name,
        table,
        meta,
        default_schema,
        |s, ps, pt| {
            let mut cloned = s.clone();
            match &mut cloned {
                Statement::CreateIndex(idx) => {
                    rewrite_obj_name(&mut idx.table_name, schema_name, table, ps, pt);
                }
                Statement::Drop { names, .. } => {
                    if let Some(last) = names.last_mut() {
                        rewrite_obj_name(last, schema_name, table, ps, pt);
                    }
                }
                _ => {}
            }
            cloned.to_string()
        },
    )
}

/// 逻辑表名 → 物理表名（仅当名称匹配逻辑 schema.table / table 时改写）。
fn rewrite_obj_name(
    name: &mut ObjectName,
    schema: &str,
    logical: &str,
    phys_schema: &str,
    phys_table: &str,
) {
    let parts: Vec<String> = name.0.iter().map(|i| i.value.to_lowercase()).collect();
    let matched = match parts.len() {
        1 => parts[0] == logical,
        2 => parts[0] == schema.to_lowercase() && parts[1] == logical,
        _ => false,
    };
    if matched {
        *name = ObjectName(vec![
            Ident::new(phys_schema.to_string()),
            Ident::new(phys_table.to_string()),
        ]);
    }
}

/// `RENAME TABLE a TO b, c TO d` 文本级解析（sqlparser 0.53 无该语法）。
/// 返回 (逻辑源表, 逻辑目标表) 对；非 RENAME TABLE 或畸形输入返回 None。
/// 引号感知（`db`.`t` 为一个词），供 server 层 DDL 自动化（ddl.rs）使用。
pub fn parse_rename_table(sql: &str) -> Option<Vec<(String, String)>> {
    let rest = sql.trim_start().trim_end_matches(';');
    let upper = rest.to_uppercase();
    if !upper.starts_with("RENAME TABLE ") && upper != "RENAME TABLE" {
        return None;
    }
    let mut parts: Vec<String> = Vec::new();
    let mut cur = String::new();
    let mut quote: Option<char> = None;
    let mut word_active = false;
    for ch in rest.chars().skip("RENAME TABLE".len()) {
        match quote {
            Some(q) => {
                if ch == q {
                    quote = None;
                } else {
                    cur.push(ch);
                }
            }
            None => match ch {
                '`' | '\'' | '"' => {
                    quote = Some(ch);
                    word_active = true;
                }
                c if c.is_whitespace() || c == ',' => {
                    if word_active {
                        let w = std::mem::take(&mut cur);
                        if !w.eq_ignore_ascii_case("TO") {
                            parts.push(w);
                        }
                        word_active = false;
                    }
                }
                _ => {
                    cur.push(ch);
                    word_active = true;
                }
            },
        }
    }
    if word_active && !cur.is_empty() && !cur.eq_ignore_ascii_case("TO") {
        parts.push(cur);
    }
    if parts.len() < 2 || parts.len() % 2 != 0 {
        return None;
    }
    Some(
        parts
            .chunks(2)
            .map(|c| (c[0].clone(), c[1].clone()))
            .collect(),
    )
}

// ---------------------------------------------------------------------------
// AST 重写：逻辑表名 → 物理表名
// ---------------------------------------------------------------------------

/// 关系名重写器：通过 `pre_visit_relation` 钩子统一改写
/// FROM/JOIN/INSERT INTO/UPDATE/DROP/ALTER/TRUNCATE 中的表引用。
struct TableRewriter<'a> {
    schema: &'a str,
    logical: String,
    phys_schema: &'a str,
    phys_table: &'a str,
}

impl VisitorMut for TableRewriter<'_> {
    type Break = ();

    fn pre_visit_relation(&mut self, name: &mut ObjectName) -> ControlFlow<()> {
        let parts: Vec<String> = name.0.iter().map(|i| i.value.to_lowercase()).collect();
        let matched = match parts.len() {
            1 => parts[0] == self.logical,
            2 => parts[0] == self.schema.to_lowercase() && parts[1] == self.logical,
            _ => false,
        };
        if matched {
            *name = ObjectName(vec![
                Ident::new(self.phys_schema.to_string()),
                Ident::new(self.phys_table.to_string()),
            ]);
        }
        ControlFlow::Continue(())
    }
}

fn rewrite_stmt(
    stmt: &Statement,
    schema: &str,
    logical: &str,
    phys_schema: &str,
    phys_table: &str,
) -> String {
    let mut cloned = stmt.clone();
    let mut rewriter = TableRewriter {
        schema,
        logical: logical.to_lowercase(),
        phys_schema,
        phys_table,
    };
    let _ = cloned.visit(&mut rewriter);
    cloned.to_string()
}

// ---------------------------------------------------------------------------
// SELECT 路由
// ---------------------------------------------------------------------------

/// FROM 中收集到的表引用。
#[derive(Debug, Clone, PartialEq, Eq)]
struct TableRef {
    schema: Option<String>,
    name: String,
    alias: Option<String>,
}

fn collect_table_refs(set_expr: &SetExpr, out: &mut Vec<TableRef>) {
    match set_expr {
        SetExpr::Select(select) => {
            for twj in &select.from {
                collect_from_table_factor(&twj.relation, out);
                for j in &twj.joins {
                    collect_from_table_factor(&j.relation, out);
                }
            }
            // 子查询中的表
            for item in &select.projection {
                if let SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } = item
                {
                    collect_from_expr(expr, out);
                }
            }
            if let Some(sel) = &select.selection {
                collect_from_expr(sel, out);
            }
        }
        SetExpr::SetOperation { left, right, .. } => {
            collect_table_refs(left, out);
            collect_table_refs(right, out);
        }
        SetExpr::Query(q) => collect_table_refs(&q.body, out),
        _ => {}
    }
}

fn collect_from_table_factor(tf: &TableFactor, out: &mut Vec<TableRef>) {
    match tf {
        TableFactor::Table { name, alias, .. } => {
            let parts: Vec<String> = name.0.iter().map(|i| i.value.clone()).collect();
            match parts.len() {
                1 => out.push(TableRef {
                    schema: None,
                    name: parts[0].clone(),
                    alias: alias.as_ref().map(|a| a.name.value.clone()),
                }),
                2 => out.push(TableRef {
                    schema: Some(parts[0].clone()),
                    name: parts[1].clone(),
                    alias: alias.as_ref().map(|a| a.name.value.clone()),
                }),
                _ => {}
            }
        }
        TableFactor::Derived { subquery, .. } => collect_table_refs(&subquery.body, out),
        TableFactor::NestedJoin {
            table_with_joins, ..
        } => {
            collect_from_table_factor(&table_with_joins.relation, out);
            for j in &table_with_joins.joins {
                collect_from_table_factor(&j.relation, out);
            }
        }
        _ => {}
    }
}

fn collect_from_expr(expr: &Expr, out: &mut Vec<TableRef>) {
    match expr {
        Expr::Subquery(sub) => collect_table_refs(&sub.body, out),
        Expr::Exists { subquery, .. } => collect_table_refs(&subquery.body, out),
        _ => {}
    }
}

fn route_select(
    q: &Query,
    default_schema: Option<&str>,
    meta: &Metadata,
) -> SqlResult<RouteResult> {
    let mut refs = Vec::new();
    collect_table_refs(&q.body, &mut refs);

    // 无表查询（select 1 / select @@version）→ prototype
    if refs.is_empty() {
        return Ok(RouteResult {
            stmt_type: StmtType::Select,
            tasks: vec![BackendTask::new(meta.prototype.clone(), q.to_string())],
            ..RouteResult::empty(StmtType::Select)
        });
    }

    // 解析所有表引用到元数据
    struct Resolved {
        r#ref: TableRef,
        schema: String,
        meta_kind: ResolvedKind,
    }
    enum ResolvedKind {
        Normal {
            target: String,
            phys_schema: String,
            phys_table: String,
        },
        Global {
            target_schemas: Vec<(String, String)>,
        },
        Sharding,
        Passthrough {
            target: String,
        },
        /// 视图（库限定重写）
        View {
            target: String,
        },
    }

    let mut resolved: Vec<Resolved> = Vec::new();
    let mut sharding_tables: Vec<(String, String, usize)> = Vec::new(); // (schema, table, resolved idx)
    for r in refs {
        let schema_name = match &r.schema {
            Some(s) => s.clone(),
            None => default_schema.ok_or(SqlError::NoDatabase)?.to_string(),
        };
        let schema_meta = get_schema(meta, &schema_name)?;
        let kind = match schema_meta.get_table(&r.name) {
            Some(TableMeta::Normal {
                target,
                schema,
                table,
                ..
            }) => ResolvedKind::Normal {
                target: target.clone(),
                phys_schema: if schema.is_empty() {
                    schema_name.clone()
                } else {
                    schema.clone()
                },
                phys_table: if table.is_empty() {
                    r.name.clone()
                } else {
                    table.clone()
                },
            },
            Some(TableMeta::Global { target_schemas, .. }) => ResolvedKind::Global {
                target_schemas: target_schemas.clone(),
            },
            Some(TableMeta::Sharding { .. }) => {
                let idx = sharding_tables.len();
                sharding_tables.push((schema_name.clone(), r.name.clone(), idx));
                ResolvedKind::Sharding
            }
            Some(TableMeta::View { .. }) => {
                // 视图：库默认 target，物理名 = 逻辑名（库限定重写，避免依赖连接默认库）
                ResolvedKind::View {
                    target: schema_meta.target_name.clone(),
                }
            }
            None => {
                // 未知表：透传到库默认 target
                ResolvedKind::Passthrough {
                    target: schema_meta.target_name.clone(),
                }
            }
        };
        resolved.push(Resolved {
            r#ref: r,
            schema: schema_name,
            meta_kind: kind,
        });
    }

    if sharding_tables.len() > 1 {
        return Err(SqlError::Unsupported(
            "Pro 透传模式暂不支持多分片表 JOIN（请使用单分片表查询或 Calcite 引擎版 Mycat3）"
                .into(),
        ));
    }

    // ---- 无分片表：确定单一 target 后整体下推 ----
    if sharding_tables.is_empty() {
        let mut targets: Vec<String> = Vec::new();
        for res in &resolved {
            match &res.meta_kind {
                ResolvedKind::Normal { target, .. } => push_unique(&mut targets, target),
                ResolvedKind::Global { target_schemas: ts } => {
                    if let Some((t, _)) = ts.first() {
                        push_unique(&mut targets, t);
                    }
                }
                ResolvedKind::Passthrough { target } => push_unique(&mut targets, target),
                ResolvedKind::View { target } => push_unique(&mut targets, target),
                _ => {}
            }
        }
        if targets.len() > 1 {
            return Err(SqlError::Unsupported(format!(
                "Pro 透传模式暂不支持跨 target({}) JOIN",
                targets.join(",")
            )));
        }
        let chosen_target = targets.first().cloned();
        let target = chosen_target
            .clone()
            .unwrap_or_else(|| meta.prototype.clone());
        // 重写 NORMAL/GLOBAL 表的物理名（GLOBAL 用所选 target 对应的物理 schema）
        let mut cloned = Statement::Query(Box::new(q.clone()));
        for res in &resolved {
            let phys = match &res.meta_kind {
                ResolvedKind::Normal {
                    phys_schema,
                    phys_table,
                    ..
                } => Some((phys_schema.clone(), phys_table.clone())),
                ResolvedKind::Global { target_schemas } => target_schemas
                    .iter()
                    .find(|(t, _)| Some(t) == chosen_target.as_ref())
                    .or_else(|| target_schemas.first())
                    .map(|(_, s)| (s.clone(), res.r#ref.name.clone())),
                ResolvedKind::View { .. } => {
                    // 视图：物理名 = `逻辑库`.`视图名`（连接默认库未知）
                    Some((res.schema.clone(), res.r#ref.name.clone()))
                }
                _ => None,
            };
            if let Some((phys_schema, phys_table)) = phys {
                let schema_of_ref = res
                    .r#ref
                    .schema
                    .clone()
                    .unwrap_or_else(|| res.schema.clone());
                let mut rewriter = TableRewriter {
                    schema: &schema_of_ref,
                    logical: res.r#ref.name.to_lowercase(),
                    phys_schema: &phys_schema,
                    phys_table: &phys_table,
                };
                let _ = cloned.visit(&mut rewriter);
            }
        }
        let sql = match cloned {
            Statement::Query(q2) => q2.to_string(),
            _ => unreachable!(),
        };
        return Ok(RouteResult {
            stmt_type: StmtType::Select,
            tasks: vec![BackendTask::new(target, sql)],
            ..RouteResult::empty(StmtType::Select)
        });
    }

    // ---- 单分片表路由 ----
    let (shard_schema, shard_table, _) = sharding_tables.into_iter().next().unwrap();
    let schema_meta = get_schema(meta, &shard_schema)?;
    let function = match schema_meta.get_table(&shard_table) {
        Some(TableMeta::Sharding { function, .. }) => function,
        _ => unreachable!(),
    };

    // 找到该分片表的别名
    let shard_ref = resolved
        .iter()
        .find(|r| {
            r.schema.eq_ignore_ascii_case(&shard_schema)
                && r.r#ref.name.eq_ignore_ascii_case(&shard_table)
        })
        .map(|r| r.r#ref.clone())
        .unwrap();
    let qualifiers: Vec<String> = {
        let mut v = vec![shard_table.to_lowercase()];
        if let Some(a) = &shard_ref.alias {
            v.push(a.to_lowercase());
        }
        v
    };

    // 提取分片条件
    let selection = extract_selection(&q.body);
    let (values_map, in_values) = extract_conditions(selection, &qualifiers);

    // 计算命中节点
    let hit_nodes: Vec<IndexDataNode> = if in_values.1.is_empty() {
        function
            .calculate(&values_map)
            .map_err(|e| SqlError::Route(e.to_string()))?
    } else {
        // IN 多值：逐值计算后取并集
        let mut union: Vec<IndexDataNode> = Vec::new();
        let column = in_values.0.clone();
        for v in &in_values.1 {
            let mut m = values_map.clone();
            m.insert(column.clone(), RangeVariable::equal(&column, v.clone()));
            for n in function
                .calculate(&m)
                .map_err(|e| SqlError::Route(e.to_string()))?
            {
                if !union.contains(&n) {
                    union.push(n);
                }
            }
        }
        union
    };

    // 其他表（非分片）target 校验：多节点时其他表必须与节点同 target
    let other_targets: Vec<String> = resolved
        .iter()
        .filter_map(|r| match &r.meta_kind {
            ResolvedKind::Normal { target, .. } => Some(target.clone()),
            ResolvedKind::Passthrough { target } => Some(target.clone()),
            ResolvedKind::View { target } => Some(target.clone()),
            ResolvedKind::Global { .. } => None,
            ResolvedKind::Sharding { .. } => None,
        })
        .collect();

    let mut tasks = Vec::with_capacity(hit_nodes.len());
    for node in &hit_nodes {
        for t in &other_targets {
            if *t != node.target_name {
                return Err(SqlError::Unsupported(format!(
                    "分片表 {} 命中节点 {} 与关联表 target {} 不一致，Pro 透传模式不支持",
                    shard_table, node.target_name, t
                )));
            }
        }
        let sql = rewrite_stmt(
            &Statement::Query(Box::new(q.clone())),
            &shard_schema,
            &shard_table,
            &node.schema_name,
            &node.table_name,
        );
        tasks.push(BackendTask::new(node.target_name.clone(), sql));
    }

    // 多节点：生成归并计划并改写 LIMIT
    let merge = if tasks.len() > 1 {
        Some(build_merge_plan(q, &mut tasks)?)
    } else {
        None
    };

    Ok(RouteResult {
        stmt_type: StmtType::Select,
        tasks,
        merge,
        ..RouteResult::empty(StmtType::Select)
    })
}

fn push_unique(v: &mut Vec<String>, s: &str) {
    if !v.iter().any(|x| x == s) {
        v.push(s.to_string());
    }
}

fn extract_selection(body: &SetExpr) -> Option<&Expr> {
    match body {
        SetExpr::Select(s) => s.selection.as_ref(),
        SetExpr::Query(q) => extract_selection(&q.body),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// 分片条件提取（对应 ValueIndexCondition）
// ---------------------------------------------------------------------------

fn literal_to_string(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Value(v) => match v {
            Value::Number(n, _) => Some(n.to_string()),
            Value::SingleQuotedString(s) | Value::DoubleQuotedString(s) => Some(s.clone()),
            Value::Boolean(b) => Some(if *b { "1" } else { "0" }.to_string()),
            Value::Null => None,
            _ => None,
        },
        // 负数字面量：UnaryOp(-, Number)
        Expr::UnaryOp {
            op: sqlparser::ast::UnaryOperator::Minus,
            expr,
        } => literal_to_string(expr).map(|s| format!("-{}", s)),
        Expr::Cast { expr, .. } => literal_to_string(expr),
        _ => None,
    }
}

/// 提取 WHERE 中的分片列条件。
/// 返回 (等值/范围条件表, IN 条件 (列名, 值列表))。
/// 仅处理 AND 连接的谓词；OR 出现时放弃裁剪（全表扫描），与 Java ValueIndexCondition 一致。
fn extract_conditions(
    selection: Option<&Expr>,
    qualifiers: &[String],
) -> (BTreeMap<String, RangeVariable>, (String, Vec<String>)) {
    let mut map: BTreeMap<String, RangeVariable> = BTreeMap::new();
    let mut in_vals = (String::new(), Vec::new());
    let mut or_seen = false;

    fn walk<'a>(
        expr: &'a Expr,
        map: &mut BTreeMap<String, RangeVariable>,
        in_vals: &mut (String, Vec<String>),
        or_seen: &mut bool,
        qualifiers: &[String],
    ) {
        match expr {
            Expr::BinaryOp {
                op: sqlparser::ast::BinaryOperator::And,
                left,
                right,
            } => {
                walk(left, map, in_vals, or_seen, qualifiers);
                walk(right, map, in_vals, or_seen, qualifiers);
            }
            Expr::BinaryOp {
                op: sqlparser::ast::BinaryOperator::Or,
                ..
            } => {
                *or_seen = true;
            }
            Expr::Nested(inner) => walk(inner, map, in_vals, or_seen, qualifiers),
            Expr::BinaryOp { op, left, right } => {
                // col OP literal 或 literal OP col
                let (col, lit, flipped) = match (
                    column_name(left, qualifiers),
                    column_name(right, qualifiers),
                ) {
                    (Some(c), None) => (c, right, false),
                    (None, Some(c)) => (c, left, true),
                    _ => return,
                };
                let value = match literal_to_string(lit) {
                    Some(v) => v,
                    None => return,
                };
                use sqlparser::ast::BinaryOperator::*;
                let (flip, op) = (flipped, op);
                let effective = if flip {
                    match op {
                        Gt => Lt,
                        GtEq => LtEq,
                        Lt => Gt,
                        LtEq => GtEq,
                        other => other.clone(),
                    }
                } else {
                    op.clone()
                };
                match effective {
                    Eq => {
                        let var = RangeVariable::equal(col.clone(), value);
                        map.insert(col, var);
                    }
                    Gt | GtEq => merge_range(map, &col, Some(value), None, matches!(effective, Gt)),
                    Lt | LtEq => merge_range(map, &col, None, Some(value), matches!(effective, Lt)),
                    _ => {}
                }
            }
            Expr::InList {
                expr,
                list,
                negated,
            } => {
                if *negated {
                    return;
                }
                if let Some(col) = column_name(expr, qualifiers) {
                    let mut vals = Vec::new();
                    for item in list {
                        if let Some(v) = literal_to_string(item) {
                            vals.push(v);
                        } else {
                            return; // 含非字面量 → 放弃裁剪
                        }
                    }
                    *in_vals = (col, vals);
                }
            }
            Expr::Between {
                expr,
                negated,
                low,
                high,
            } => {
                if *negated {
                    return;
                }
                if let Some(col) = column_name(expr, qualifiers) {
                    if let (Some(l), Some(h)) = (literal_to_string(low), literal_to_string(high)) {
                        let var = RangeVariable::range(col.clone(), Some(l), Some(h));
                        map.insert(col, var);
                    }
                }
            }
            Expr::IsNull(inner) | Expr::IsNotNull(inner) => {
                let _ = inner; // NULL 条件不参与裁剪
            }
            _ => {}
        }
    }

    if let Some(sel) = selection {
        walk(sel, &mut map, &mut in_vals, &mut or_seen, qualifiers);
    }
    if or_seen {
        // 有 OR：无法保证裁剪正确性 → 清空条件（全表扫描）
        map.clear();
        in_vals = (String::new(), Vec::new());
    }
    (map, in_vals)
}

fn merge_range(
    map: &mut BTreeMap<String, RangeVariable>,
    col: &str,
    begin: Option<String>,
    end: Option<String>,
    _exclusive: bool,
) {
    // 与已有范围条件合并（取交集边界）
    match map.get(col) {
        Some(existing) if existing.operator == Operator::Range => {
            let nb = match (&existing.begin, begin) {
                (Some(a), Some(b)) => Some(a.max(&b).clone()),
                (Some(a), None) => Some(a.clone()),
                (None, b) => b,
            };
            let ne = match (&existing.end, end) {
                (Some(a), Some(b)) => Some(a.min(&b).clone()),
                (Some(a), None) => Some(a.clone()),
                (None, e) => e,
            };
            map.insert(
                col.to_string(),
                RangeVariable::range(col.to_string(), nb, ne),
            );
        }
        Some(existing) if existing.operator == Operator::Equal => { /* 等值优先，保留 */ }
        _ => {
            map.insert(
                col.to_string(),
                RangeVariable::range(col.to_string(), begin, end),
            );
        }
    }
}

/// 若表达式是指向分片表的列引用，返回列名（小写）。
fn column_name(expr: &Expr, qualifiers: &[String]) -> Option<String> {
    match expr {
        Expr::Identifier(i) => Some(i.value.to_lowercase()),
        Expr::CompoundIdentifier(parts) => {
            if parts.len() == 2
                && qualifiers
                    .iter()
                    .any(|q| *q == parts[0].value.to_lowercase())
            {
                Some(parts[1].value.to_lowercase())
            } else {
                None
            }
        }
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// 归并计划（多节点 SELECT）
// ---------------------------------------------------------------------------

fn build_merge_plan(q: &Query, tasks: &mut Vec<BackendTask>) -> SqlResult<MergePlan> {
    // 聚合检测：单 Select、无 GROUP BY，投影全部为 COUNT/SUM/MAX/MIN
    if let SetExpr::Select(select) = q.body.as_ref() {
        let has_group = match &select.group_by {
            GroupByExpr::Expressions(e, _) => !e.is_empty(),
            GroupByExpr::All(_) => true,
        };
        if !has_group {
            if let Some(op) = detect_aggregate(&select.projection) {
                if q.order_by.is_none() || order_exprs(q).is_empty() {
                    return Ok(MergePlan {
                        op,
                        order_by: Vec::new(),
                        limit: None,
                    });
                }
            } else if !select.projection.is_empty() && has_aggregate_any(&select.projection) {
                return Err(SqlError::Unsupported(
                    "Pro 透传模式暂不支持跨分片复杂聚合（GROUP BY / AVG / 混合投影），请使用单分片路由或 Mycat3 Calcite 引擎".into(),
                ));
            }
        } else if has_aggregate_any(&select.projection) {
            return Err(SqlError::Unsupported(
                "Pro 透传模式暂不支持跨分片 GROUP BY 聚合".into(),
            ));
        }
    }

    // 普通行归并：ORDER BY + LIMIT
    let mut order_by = Vec::new();
    let limit_spec;
    {
        let order_exprs = order_exprs(q);
        if !order_exprs.is_empty() {
            let projection_names = projection_names(q);
            let has_wildcard = projection_names
                .iter()
                .any(|p| p == "*" || p.ends_with(".*"));
            for (expr, desc) in &order_exprs {
                match expr {
                    // ORDER BY 1 → 列序号
                    Expr::Value(Value::Number(n, _)) => {
                        match n
                            .to_string()
                            .parse::<usize>()
                            .ok()
                            .map(|i| i.saturating_sub(1))
                        {
                            Some(i) => order_by.push(OrderKey {
                                index: Some(i),
                                name: None,
                                desc: *desc,
                            }),
                            None => {
                                return Err(SqlError::Unsupported(
                                    "跨分片 ORDER BY 列序号非法".into(),
                                ))
                            }
                        }
                    }
                    // ORDER BY name → 先按投影解析，投影为 * 时留给执行层按结果列解析
                    Expr::Identifier(i) => {
                        let idx = projection_names
                            .iter()
                            .position(|p| p.eq_ignore_ascii_case(&i.value));
                        match idx {
                            Some(k) => order_by.push(OrderKey {
                                index: Some(k),
                                name: None,
                                desc: *desc,
                            }),
                            None if has_wildcard => order_by.push(OrderKey {
                                index: None,
                                name: Some(i.value.to_lowercase()),
                                desc: *desc,
                            }),
                            None => {
                                return Err(SqlError::Unsupported(
                                    "跨分片 ORDER BY 仅支持投影列名或序号".into(),
                                ))
                            }
                        }
                    }
                    _ => {
                        return Err(SqlError::Unsupported(
                            "跨分片 ORDER BY 仅支持投影列名或序号".into(),
                        ))
                    }
                }
            }
        }
        limit_spec = extract_limit(q);
    }

    // 改写每个下推 SQL：offset=0, limit=offset+count（保证全局归并正确）
    if let Some(spec) = &limit_spec {
        let total = spec.offset + spec.count;
        for t in tasks.iter_mut() {
            if let Ok(mut stmts) = Parser::parse_sql(&MySqlDialect {}, &t.sql) {
                if let Some(Statement::Query(q2)) = stmts.first_mut() {
                    q2.offset = Some(SqlOffset {
                        value: Expr::Value(Value::Number("0".into(), false)),
                        rows: sqlparser::ast::OffsetRows::None,
                    });
                    q2.limit = Some(Expr::Value(Value::Number(total.to_string(), false)));
                    t.sql = q2.to_string();
                }
            }
        }
    }

    Ok(MergePlan {
        op: MergeOp::Concat,
        order_by,
        limit: limit_spec,
    })
}

fn order_exprs(q: &Query) -> Vec<(Expr, bool)> {
    match &q.order_by {
        Some(ob) => ob
            .exprs
            .iter()
            .map(|e| (e.expr.clone(), e.asc == Some(false)))
            .collect(),
        None => Vec::new(),
    }
}

fn extract_limit(q: &Query) -> Option<LimitSpec> {
    let count = match &q.limit {
        Some(Expr::Value(Value::Number(n, _))) => n.to_string().parse::<u64>().ok()?,
        _ => return None,
    };
    let offset = match &q.offset {
        Some(o) => match &o.value {
            Expr::Value(Value::Number(n, _)) => n.to_string().parse::<u64>().unwrap_or(0),
            _ => 0,
        },
        None => 0,
    };
    Some(LimitSpec { offset, count })
}

fn projection_names(q: &Query) -> Vec<String> {
    match q.body.as_ref() {
        SetExpr::Select(s) => s
            .projection
            .iter()
            .map(|item| match item {
                SelectItem::UnnamedExpr(e) => expr_display_name(e),
                SelectItem::ExprWithAlias { alias, .. } => alias.value.clone(),
                SelectItem::QualifiedWildcard(name, _) => name.to_string(),
                SelectItem::Wildcard(_) => "*".to_string(),
            })
            .collect(),
        _ => Vec::new(),
    }
}

fn expr_display_name(e: &Expr) -> String {
    match e {
        Expr::Identifier(i) => i.value.clone(),
        Expr::CompoundIdentifier(parts) => {
            parts.last().map(|p| p.value.clone()).unwrap_or_default()
        }
        other => other.to_string(),
    }
}

/// 检测纯聚合投影（单列 COUNT/SUM/MAX/MIN），返回归并操作。
fn detect_aggregate(projection: &[SelectItem]) -> Option<MergeOp> {
    if projection.len() != 1 {
        return None;
    }
    let expr = match &projection[0] {
        SelectItem::UnnamedExpr(e) | SelectItem::ExprWithAlias { expr: e, .. } => e,
        _ => return None,
    };
    let func = match expr {
        Expr::Function(f) => f,
        _ => return None,
    };
    let name = func.name.0.last()?.value.to_uppercase();
    // 排除 DISTINCT / 复杂参数
    let list = match &func.args {
        sqlparser::ast::FunctionArguments::List(l) => l,
        _ => return None,
    };
    if list.duplicate_treatment.is_some() {
        // COUNT(DISTINCT x) 无法跨分片简单求和归并
        return None;
    }
    if list.args.len() != 1 {
        return None;
    }
    match &list.args[0] {
        FunctionArg::Unnamed(FunctionArgExpr::Wildcard) => {}
        FunctionArg::Unnamed(FunctionArgExpr::Expr(_)) => {}
        _ => return None,
    }
    if func.filter.is_some() || func.over.is_some() {
        return None;
    }
    match name.as_str() {
        "COUNT" => Some(MergeOp::CountSum),
        "SUM" => Some(MergeOp::NumericSum),
        "MAX" => Some(MergeOp::Max),
        "MIN" => Some(MergeOp::Min),
        _ => None,
    }
}

const AGG_FUNCS: &[&str] = &["COUNT", "SUM", "AVG", "MAX", "MIN", "GROUP_CONCAT"];

fn has_aggregate_any(projection: &[SelectItem]) -> bool {
    fn walk_expr(e: &Expr) -> bool {
        match e {
            Expr::Function(f) => AGG_FUNCS.contains(&f.name.to_string().to_uppercase().as_str()),
            Expr::BinaryOp { left, right, .. } => walk_expr(left) || walk_expr(right),
            Expr::UnaryOp { expr, .. } => walk_expr(expr),
            Expr::Nested(inner) => walk_expr(inner),
            Expr::Cast { expr, .. } => walk_expr(expr),
            _ => false,
        }
    }
    projection.iter().any(|item| match item {
        SelectItem::UnnamedExpr(e) | SelectItem::ExprWithAlias { expr: e, .. } => walk_expr(e),
        _ => false,
    })
}

// ---------------------------------------------------------------------------
// INSERT 路由
// ---------------------------------------------------------------------------

fn route_insert(
    insert: &sqlparser::ast::Insert,
    default_schema: Option<&str>,
    meta: &Metadata,
) -> SqlResult<RouteResult> {
    let (schema_name, table) = resolve_name(&insert.table_name, default_schema)?;
    let schema_meta = get_schema(meta, &schema_name)?;
    let stmt = Statement::Insert(insert.clone());

    match schema_meta.get_table(&table) {
        Some(TableMeta::Normal {
            target,
            schema,
            table: phys,
            ..
        }) => {
            let (ps, pt) = if schema.is_empty() {
                (schema_name.clone(), table.clone())
            } else {
                (schema.clone(), phys.clone())
            };
            let sql = rewrite_stmt(&stmt, &schema_name, &table, &ps, &pt);
            Ok(RouteResult {
                stmt_type: StmtType::Insert,
                tasks: vec![BackendTask::new(target.clone(), sql)],
                ..RouteResult::empty(StmtType::Insert)
            })
        }
        Some(TableMeta::Global { target_schemas, .. }) => {
            // 广播表：写入所有 target（物理 schema 重写）
            let tasks = target_schemas
                .iter()
                .map(|(t, s)| {
                    BackendTask::new(
                        t.clone(),
                        rewrite_stmt(&stmt, &schema_name, &table, s, &table),
                    )
                })
                .collect();
            Ok(RouteResult {
                stmt_type: StmtType::Insert,
                tasks,
                ..RouteResult::empty(StmtType::Insert)
            })
        }
        Some(TableMeta::Sharding { function, .. }) => {
            route_sharding_insert(insert, function.as_ref())
        }
        _ => {
            // 未注册表：透传到库默认 target
            Ok(RouteResult {
                stmt_type: StmtType::Insert,
                tasks: vec![BackendTask::new(
                    schema_meta.target_name.clone(),
                    stmt.to_string(),
                )],
                ..RouteResult::empty(StmtType::Insert)
            })
        }
    }
}

fn route_sharding_insert(
    insert: &sqlparser::ast::Insert,
    function: &dyn mycat3_router::RuleFunction,
) -> SqlResult<RouteResult> {
    let columns: Vec<String> = insert
        .columns
        .iter()
        .map(|c| c.value.to_lowercase())
        .collect();
    let shard_cols = function.sharding_columns();
    if shard_cols.len() != 1 {
        return Err(SqlError::Unsupported(
            "Pro 仅支持单分片列的 INSERT 路由".into(),
        ));
    }
    let shard_col = &shard_cols[0];
    let col_idx = columns.iter().position(|c| c == shard_col).ok_or_else(|| {
        SqlError::Route(format!(
            "INSERT 缺少分片列 {}（Pro 版不支持分片列自增序列填充）",
            shard_col
        ))
    })?;

    // VALUES 行按节点分组
    let values = match insert.source.as_deref() {
        Some(q) => match q.body.as_ref() {
            SetExpr::Values(v) => v,
            _ => {
                return Err(SqlError::Unsupported(
                    "INSERT ... SELECT 跨分片写入暂不支持（单节点场景请确保 WHERE 命中单分片）"
                        .into(),
                ))
            }
        },
        None => return Err(SqlError::Parse("INSERT without VALUES".into())),
    };

    let mut groups: BTreeMap<String, (IndexDataNode, Vec<Vec<Expr>>)> = BTreeMap::new();
    for row in &values.rows {
        let val_expr = row
            .get(col_idx)
            .ok_or_else(|| SqlError::Route("INSERT 行列数与列名不匹配".into()))?;
        let val = literal_to_string(val_expr)
            .ok_or_else(|| SqlError::Route(format!("分片列 {} 的值必须是字面量", shard_col)))?;
        let mut m = BTreeMap::new();
        m.insert(
            shard_col.clone(),
            RangeVariable::equal(shard_col.clone(), val.clone()),
        );
        let nodes = function
            .calculate(&m)
            .map_err(|e| SqlError::Route(e.to_string()))?;
        if nodes.len() != 1 {
            return Err(SqlError::Route(format!(
                "分片列值 {} 命中 {} 个节点，INSERT 必须命中单节点",
                val,
                nodes.len()
            )));
        }
        let node = nodes.into_iter().next().unwrap();
        let key = node.unique_name();
        groups
            .entry(key)
            .or_insert_with(|| (node.clone(), Vec::new()))
            .1
            .push(row.clone());
    }

    let mut tasks = Vec::with_capacity(groups.len());
    for (_, (node, rows)) in groups {
        let mut ins = insert.clone();
        ins.table_name = ObjectName(vec![
            Ident::new(node.schema_name.clone()),
            Ident::new(node.table_name.clone()),
        ]);
        let values_exprs: Vec<Vec<Expr>> = rows;
        ins.source = Some(Box::new(Query {
            with: None,
            body: Box::new(SetExpr::Values(sqlparser::ast::Values {
                explicit_row: false,
                rows: values_exprs,
            })),
            order_by: None,
            limit: None,
            limit_by: Vec::new(),
            offset: None,
            fetch: None,
            locks: Vec::new(),
            for_clause: None,
            settings: None,
            format_clause: None,
        }));
        tasks.push(BackendTask::new(
            node.target_name.clone(),
            Statement::Insert(ins).to_string(),
        ));
    }
    Ok(RouteResult {
        stmt_type: StmtType::Insert,
        tasks,
        ..RouteResult::empty(StmtType::Insert)
    })
}

// ---------------------------------------------------------------------------
// UPDATE / DELETE 路由
// ---------------------------------------------------------------------------

fn route_update(
    table: &TableWithJoins,
    selection: Option<&Expr>,
    stmt: &Statement,
    default_schema: Option<&str>,
    meta: &Metadata,
) -> SqlResult<RouteResult> {
    let refs = collect_from_relations(&[table.clone()]);
    route_dml_by_refs(
        refs,
        selection,
        stmt,
        StmtType::Update,
        default_schema,
        meta,
    )
}

fn route_delete(
    delete: &sqlparser::ast::Delete,
    default_schema: Option<&str>,
    meta: &Metadata,
) -> SqlResult<RouteResult> {
    let relations = match &delete.from {
        FromTable::WithFromKeyword(rels) | FromTable::WithoutKeyword(rels) => rels.clone(),
    };
    let refs = collect_from_relations(&relations);
    route_dml_by_refs(
        refs,
        delete.selection.as_ref(),
        &Statement::Delete(delete.clone()),
        StmtType::Delete,
        default_schema,
        meta,
    )
}

fn collect_from_relations(relations: &[TableWithJoins]) -> Vec<TableRef> {
    let mut out = Vec::new();
    for twj in relations {
        collect_from_table_factor(&twj.relation, &mut out);
        for j in &twj.joins {
            collect_from_table_factor(&j.relation, &mut out);
        }
    }
    out
}

/// UPDATE/DELETE 共用路由：单表 → 分片裁剪；多表 → 不支持。
fn route_dml_by_refs(
    refs: Vec<TableRef>,
    selection: Option<&Expr>,
    stmt: &Statement,
    stmt_type: StmtType,
    default_schema: Option<&str>,
    meta: &Metadata,
) -> SqlResult<RouteResult> {
    if refs.len() != 1 {
        return Err(SqlError::Unsupported(
            "Pro 透传模式仅支持单表 UPDATE/DELETE".into(),
        ));
    }
    let r = refs.into_iter().next().unwrap();
    let schema_name = match &r.schema {
        Some(s) => s.clone(),
        None => default_schema.ok_or(SqlError::NoDatabase)?.to_string(),
    };
    let schema_meta = get_schema(meta, &schema_name)?;
    match schema_meta.get_table(&r.name) {
        Some(TableMeta::Sharding { function, .. }) => {
            let qualifiers: Vec<String> = {
                let mut v = vec![r.name.to_lowercase()];
                if let Some(a) = &r.alias {
                    v.push(a.to_lowercase());
                }
                v
            };
            let (values_map, in_values) = extract_conditions(selection, &qualifiers);
            let nodes = if in_values.1.is_empty() {
                function
                    .calculate(&values_map)
                    .map_err(|e| SqlError::Route(e.to_string()))?
            } else {
                let mut union: Vec<IndexDataNode> = Vec::new();
                for v in &in_values.1 {
                    let mut m = values_map.clone();
                    m.insert(
                        in_values.0.clone(),
                        RangeVariable::equal(in_values.0.clone(), v.clone()),
                    );
                    for n in function
                        .calculate(&m)
                        .map_err(|e| SqlError::Route(e.to_string()))?
                    {
                        if !union.contains(&n) {
                            union.push(n);
                        }
                    }
                }
                union
            };
            let tasks = nodes
                .iter()
                .map(|n| {
                    BackendTask::new(
                        n.target_name.clone(),
                        rewrite_stmt(stmt, &schema_name, &r.name, &n.schema_name, &n.table_name),
                    )
                })
                .collect();
            Ok(RouteResult {
                stmt_type,
                tasks,
                ..RouteResult::empty(stmt_type)
            })
        }
        Some(TableMeta::Global { target_schemas, .. }) => {
            // 广播表更新：全部 target 执行（物理 schema 重写），affectedRows 求和（Java 语义相同）
            let tasks = target_schemas
                .iter()
                .map(|(t, s)| {
                    BackendTask::new(
                        t.clone(),
                        rewrite_stmt(stmt, &schema_name, &r.name, s, &r.name),
                    )
                })
                .collect();
            Ok(RouteResult {
                stmt_type,
                tasks,
                ..RouteResult::empty(stmt_type)
            })
        }
        Some(TableMeta::Normal {
            target,
            schema,
            table,
            ..
        }) => {
            let (ps, pt) = if schema.is_empty() {
                (schema_name.clone(), r.name.clone())
            } else {
                (schema.clone(), table.clone())
            };
            let sql = rewrite_stmt(stmt, &schema_name, &r.name, &ps, &pt);
            Ok(RouteResult {
                stmt_type,
                tasks: vec![BackendTask::new(target.clone(), sql)],
                ..RouteResult::empty(stmt_type)
            })
        }
        _ => Ok(RouteResult {
            stmt_type,
            tasks: vec![BackendTask::new(
                schema_meta.target_name.clone(),
                stmt.to_string(),
            )],
            ..RouteResult::empty(stmt_type)
        }),
    }
}
