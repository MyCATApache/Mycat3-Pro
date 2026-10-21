//! 协议无关的 SQL 执行层。
//!
//! 对应 Java 版多协议前端共享的 `MycatProtocolBackend` → `MycatdbCommand.executeQuery`
//! 门面：PostgreSQL / TDS 前端与 MySQL 前端走同一条
//! 解析 → 路由 → 分片 → 执行 → 归并 管道（`AbstractMycatProtocolResponse` 的
//! buffer-first 模型），结果以协议无关的字符串值形式返回，由各协议前端编码回写。
//!
//! MySQL 前端仍保留原始载荷透传快路径（frontend.rs），本模块服务于非 MySQL 协议。

use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Instant;

use mycat3_client::QueryOutcome;
use mycat3_protocol::consts::field_type;
use mycat3_sql::{analyse, BackendTask, RouteResult, StmtType, TableMeta};

use crate::commands::{handle_local, handle_show, probe_local_select, LocalOutcome};
use crate::executor::{execute_route, ExecResponse};
use crate::session::SessionContext;
use crate::state::ServerState;
use crate::tx::TransactionSession;

/// 结果列元数据（协议前端做类型映射用）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EngineColumn {
    pub name: String,
    /// MySQL 字段类型（protocol::consts::field_type）
    pub mysql_type: u8,
    /// MySQL 排序规则索引（63 = binary）
    pub charset: u16,
}

/// 单条语句的执行结果。
#[derive(Debug, Clone, PartialEq)]
pub enum EngineOutcome {
    /// DML/DDL/控制语句
    Ok {
        affected_rows: u64,
        last_insert_id: u64,
    },
    /// 结果集（文本值；None = NULL）
    Rows {
        columns: Vec<EngineColumn>,
        rows: Vec<Vec<Option<String>>>,
    },
}

/// 执行错误（携带各协议都能表达的三要素）。
#[derive(Debug, Clone)]
pub struct EngineError {
    pub errno: u16,
    pub sqlstate: String,
    pub message: String,
}

impl EngineError {
    pub fn new(errno: u16, sqlstate: &str, msg: impl Into<String>) -> Self {
        EngineError {
            errno,
            sqlstate: sqlstate.to_string(),
            message: msg.into(),
        }
    }
    pub fn unsupported(msg: impl Into<String>) -> Self {
        Self::new(
            mycat3_protocol::consts::error_code::ER_NOT_SUPPORTED_YET,
            "42000",
            msg,
        )
    }
    pub fn general(msg: impl Into<String>) -> Self {
        Self::new(
            mycat3_protocol::consts::error_code::ER_UNKNOWN_ERROR,
            "HY000",
            msg,
        )
    }
}

impl std::fmt::Display for EngineError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} ({}): {}", self.sqlstate, self.errno, self.message)
    }
}

/// 协议无关会话：MySQL 会话上下文 + 事务会话的组合。
pub struct EngineSession {
    pub ctx: SessionContext,
    pub tx: TransactionSession,
}

impl EngineSession {
    pub fn new(ctx: SessionContext) -> Self {
        EngineSession {
            ctx,
            tx: TransactionSession::new(),
        }
    }

    pub async fn close(&mut self) {
        self.tx.close().await;
    }
}

/// 引擎侧流式结果：列元数据 + 逐行拉取。
pub struct EngineRowStream {
    pub columns: Vec<EngineColumn>,
    pub column_count: usize,
    pub rows_read: u64,
    inner: crate::executor::StreamingResult,
}

impl EngineRowStream {
    pub async fn next_row(&mut self) -> Result<Option<Vec<Option<String>>>, EngineError> {
        let row = self.inner.next_row().await.map_err(|e| {
            let (errno, sqlstate, message) = e.to_parts();
            EngineError {
                errno,
                sqlstate,
                message,
            }
        })?;
        if row.is_some() {
            self.rows_read += 1;
        }
        Ok(row)
    }
}

/// 单条语句的流式/物化二选一结果。
pub enum EngineStreamOutcome {
    Outcomes(Vec<EngineOutcome>),
    Stream(Box<EngineRowStream>),
}

/// 尝试以流式方式执行一条（单语句）SQL。
///
/// 只有“非代理模式、事务外、单分片、无归并、SQL Server 后端”的读语句
/// 会返回 `Stream`；其余情况返回与 `execute_sql` 等价的物化结果。
pub async fn execute_sql_streaming(
    state: &Arc<ServerState>,
    sess: &mut EngineSession,
    sql: &str,
) -> Result<EngineStreamOutcome, EngineError> {
    let trace_id = state.stats.queries_total.fetch_add(1, Ordering::Relaxed) + 1;
    let started = Instant::now();
    let sql_trim = sql.trim();
    if sql_trim.is_empty() {
        return Err(EngineError::new(
            mycat3_protocol::consts::error_code::ER_EMPTY_QUERY,
            "42000",
            "Query was empty",
        ));
    }
    if state.is_proxy_mode() {
        return Ok(EngineStreamOutcome::Outcomes(
            execute_sql(state, sess, sql).await?,
        ));
    }
    if let Some(outcome) = state
        .sql_cache
        .lookup(
            sql_trim,
            sess.ctx.schema.as_deref(),
            sess.ctx.in_transaction,
            sess.ctx.autocommit,
        )
        .await
    {
        return Ok(EngineStreamOutcome::Outcomes(vec![(*outcome).clone()]));
    }
    if mycat3_sql::parse_hint(sql_trim).0.is_some() {
        return Ok(EngineStreamOutcome::Outcomes(
            execute_sql(state, sess, sql).await?,
        ));
    }
    if let Some(result) = try_local_fast_path(state, &sess.ctx, sql_trim).await {
        let out = result?;
        record_slow_sql(
            state,
            trace_id,
            &sess.ctx.user,
            sess.ctx.schema.as_deref(),
            sql_trim,
            engine_rows(&out),
            started.elapsed(),
            "",
        );
        return Ok(EngineStreamOutcome::Outcomes(vec![out]));
    }
    let meta = state.metadata();
    let routes = match analyse(sql_trim, sess.ctx.schema.as_deref(), &meta) {
        Ok(r) => r,
        Err(e) => return Err(sql_error(e)),
    };
    if routes.len() != 1 {
        return Ok(EngineStreamOutcome::Outcomes(
            execute_sql(state, sess, sql).await?,
        ));
    }
    if let Some(err) = check_disallow(state, &sess.ctx, &routes) {
        return Err(err);
    }
    let route = &routes[0];
    if route.tasks.len() == 1 && !route.stmt_type.is_write() && route.merge.is_none() {
        match crate::executor::execute_route_stream(state, &mut sess.ctx, &mut sess.tx, route).await
        {
            Ok(Some(inner)) => {
                let columns: Vec<EngineColumn> = inner
                    .columns
                    .iter()
                    .map(|c| EngineColumn {
                        name: c.name.clone(),
                        mysql_type: c.field_type,
                        charset: c.charset,
                    })
                    .collect();
                return Ok(EngineStreamOutcome::Stream(Box::new(EngineRowStream {
                    column_count: columns.len(),
                    columns,
                    rows_read: 0,
                    inner,
                })));
            }
            Ok(None) => {}
            Err(e) => {
                let (errno, sqlstate, message) = e.to_packet_parts();
                state.stats.incr_error();
                return Err(EngineError {
                    errno,
                    sqlstate,
                    message,
                });
            }
        }
    }
    // 不可流式：直接执行已分析出的路由（避免重复缓存/本地快路径副作用）。
    let any_ddl = routes
        .iter()
        .any(|r| matches!(r.stmt_type, mycat3_sql::StmtType::Ddl));
    let any_write = routes
        .iter()
        .any(|r| r.stmt_type.is_write() || matches!(r.stmt_type, mycat3_sql::StmtType::Ddl));
    let target = routes
        .first()
        .and_then(|r| r.tasks.first())
        .map(|t| t.target_name.clone())
        .unwrap_or_default();
    let mut out = Vec::with_capacity(routes.len());
    for route in routes {
        out.push(execute_route_engine(state, sess, &route, sql_trim).await?);
    }
    if any_write {
        invalidate_cache_after_write(state, sess.ctx.schema.as_deref(), sql_trim);
    }
    if any_ddl {
        crate::ddl::refresh_create_sql_after_alter(state, sess.ctx.schema.as_deref(), sql_trim)
            .await;
    }
    record_slow_sql(
        state,
        trace_id,
        &sess.ctx.user,
        sess.ctx.schema.as_deref(),
        sql_trim,
        out.iter().map(engine_rows).sum(),
        started.elapsed(),
        &target,
    );
    Ok(EngineStreamOutcome::Outcomes(out))
}

/// 执行一段 SQL（可含多语句），返回每条语句的结果。
///
/// 对应 `MycatdbCommand.executeQuery` + `AbstractMycatProtocolResponse` 的
/// buffer-first 结果模型。
pub async fn execute_sql(
    state: &Arc<ServerState>,
    sess: &mut EngineSession,
    sql: &str,
) -> Result<Vec<EngineOutcome>, EngineError> {
    execute_sql_impl(state, sess, sql, false).await
}

/// 执行入口。`bypass_cache=true` 供缓存预热递归调用，防止缓存 SQL 自身命中缓存。
pub async fn execute_sql_impl(
    state: &Arc<ServerState>,
    sess: &mut EngineSession,
    sql: &str,
    bypass_cache: bool,
) -> Result<Vec<EngineOutcome>, EngineError> {
    let trace_id = state.stats.queries_total.fetch_add(1, Ordering::Relaxed) + 1;
    let started = Instant::now();
    let sql_trim = sql.trim();
    if sql_trim.is_empty() {
        return Err(EngineError::new(
            mycat3_protocol::consts::error_code::ER_EMPTY_QUERY,
            "42000",
            "Query was empty",
        ));
    }

    // 纯代理模式（server.mode=proxy）：不分片/不改写，仅保留拦截/缓存/慢日志
    if state.is_proxy_mode() {
        return execute_sql_proxy(state, sess, sql_trim, trace_id, started, bypass_cache).await;
    }

    // SQL 结果缓存（对应 Java SqlResultSetService；命中直接返回缓存结果，
    // 不再触达后端——事务/非 autocommit/hint 语句不参与）
    if !bypass_cache {
        if let Some(outcome) = state
            .sql_cache
            .lookup(
                sql_trim,
                sess.ctx.schema.as_deref(),
                sess.ctx.in_transaction,
                sess.ctx.autocommit,
            )
            .await
        {
            return Ok(vec![(*outcome).clone()]);
        }
    }

    // 服务器端 CSV 批量导入（对应 Java HintHandler.loaddata）与未知 hint 报错
    if let (Some(hint), _) = mycat3_sql::parse_hint(sql_trim) {
        // 未识别任何有效指令的 mycat hint：显式报错
        //（Java 版静默忽略——脚本会"成功但没结果"，Pro 拒绝静默给错结果）
        if !hint.unknown.is_empty() && hint.is_empty() {
            return Err(EngineError::new(
                1235,
                "42000",
                format!(
                    "未知 mycat hint 指令: {}（Pro 未实现该指令）",
                    hint.unknown.join(", ")
                ),
            ));
        }
        if let Some(json) = &hint.loaddata {
            let outcome =
                crate::loaddata::run_import(state, &mut sess.ctx, &mut sess.tx, json).await?;
            record_slow_sql(
                state,
                trace_id,
                &sess.ctx.user,
                sess.ctx.schema.as_deref(),
                sql_trim,
                match &outcome {
                    EngineOutcome::Ok { affected_rows, .. } => *affected_rows,
                    _ => 0,
                },
                started.elapsed(),
                "",
            );
            return Ok(vec![outcome]);
        }
    }

    // 本地快路径：配置 DDL / reload / kill / information_schema / SHOW 扩展 / 变量探针
    if let Some(result) = try_local_fast_path(state, &sess.ctx, sql_trim).await {
        let out = match result {
            Ok(o) => vec![o],
            Err(e) => return Err(e),
        };
        record_slow_sql(
            state,
            trace_id,
            &sess.ctx.user,
            sess.ctx.schema.as_deref(),
            sql_trim,
            out.iter().map(engine_rows).sum(),
            started.elapsed(),
            "",
        );
        return Ok(out);
    }

    // INSERT 自增序列填充（对应 VertxExecuter.needFillAutoIncrement/explainGlobalInsertSequence01）
    let filled = fill_insert_sequences(state, &sess.ctx, sql_trim);
    let sql_trim = filled.as_deref().unwrap_or(sql_trim);

    let meta = state.metadata();
    let routes = match analyse(sql_trim, sess.ctx.schema.as_deref(), &meta) {
        Ok(r) => r,
        Err(e) => return Err(sql_error(e)),
    };
    // 用户角色 SQL 类型黑名单（对应 Java RoleConfig.disallowSqlTypes）
    if let Some(err) = check_disallow(state, &sess.ctx, &routes) {
        return Err(err);
    }

    let any_ddl = routes
        .iter()
        .any(|r| matches!(r.stmt_type, mycat3_sql::StmtType::Ddl));
    let any_write = routes
        .iter()
        .any(|r| r.stmt_type.is_write() || matches!(r.stmt_type, mycat3_sql::StmtType::Ddl));
    let target = routes
        .first()
        .and_then(|r| r.tasks.first())
        .map(|t| t.target_name.clone())
        .unwrap_or_default();
    let mut out = Vec::with_capacity(routes.len());
    for route in routes {
        out.push(execute_route_engine(state, sess, &route, sql_trim).await?);
    }
    // 写入后缓存失效（保守启发式）+ ALTER 后回读刷新配置
    if any_write {
        invalidate_cache_after_write(state, sess.ctx.schema.as_deref(), sql_trim);
    }
    if any_ddl {
        crate::ddl::refresh_create_sql_after_alter(state, sess.ctx.schema.as_deref(), sql_trim)
            .await;
    }
    record_slow_sql(
        state,
        trace_id,
        &sess.ctx.user,
        sess.ctx.schema.as_deref(),
        sql_trim,
        out.iter().map(engine_rows).sum(),
        started.elapsed(),
        &target,
    );
    Ok(out)
}

/// 引擎结果的（影响/返回）行数。
fn engine_rows(o: &EngineOutcome) -> u64 {
    match o {
        EngineOutcome::Ok { affected_rows, .. } => *affected_rows,
        EngineOutcome::Rows { rows, .. } => rows.len() as u64,
    }
}

/// 用户权限检查（对应 Java `RoleConfig.disallowSqlTypes` + Pro 增强的 readOnly）：
/// - disallowSqlTypes：SQL 类型黑名单（1235 与 Java 同文案）；
/// - readOnly=true：写语句/DML 显式拒绝（Java 配置有 readOnly 但未强制，Pro 落地）。
pub fn check_disallow(
    state: &Arc<ServerState>,
    ctx: &SessionContext,
    routes: &[mycat3_sql::RouteResult],
) -> Option<EngineError> {
    let cfg = state.config();
    let user = cfg.users.get(&ctx.user)?;
    let disallow = user
        .role
        .as_ref()
        .map(|r| r.disallow_sql_types.clone())
        .unwrap_or_default();
    for r in routes {
        let name = format!("{:?}", r.stmt_type).to_uppercase();
        if disallow.iter().any(|d| d.eq_ignore_ascii_case(&name)) {
            return Some(EngineError::new(
                1235,
                "42000",
                format!(
                    "statement is sqlType:{}, for the user {}, it is forbidden.",
                    name, ctx.user
                ),
            ));
        }
        // readOnly：写语句（含 DDL/事务写入路径）拒绝
        if user.read_only
            && (r.stmt_type.is_write() || matches!(r.stmt_type, mycat3_sql::StmtType::Ddl))
        {
            return Some(EngineError::new(
                1290,
                "HY000",
                format!("The MySQL server is running with the --read-only option (user {} is read_only)", ctx.user),
            ));
        }
    }
    None
}

/// 三协议共享的本地快路径（不进入 SQL 路由的语句）。
/// 顺序与 Java MycatdbCommand 的 special-prefix → hint → handler 分派一致：
/// 1. 配置类 DDL（CREATE/DROP DATABASE/TABLE/USER/SEQUENCE、loadConfigFromFile hint）
///    —— 必须先于 analyse：DRDS 分片语法 sqlparser 无法解析
/// 2. KILL <id>
/// 3. information_schema.SCHEMATA/TABLES/COLUMNS 本地视图
/// 4. SHOW CREATE TABLE / SHOW [FULL] TABLES / SHOW COLUMNS / DESC（元数据本地生成）
/// 5. select @@变量 / database() / user() 等探针（多变量支持）
pub async fn try_local_fast_path(
    state: &Arc<ServerState>,
    ctx: &SessionContext,
    sql: &str,
) -> Option<Result<EngineOutcome, EngineError>> {
    match run_hint_commands(state, ctx, sql, false).await {
        HintOutcome::Handled(r) => Some(r),
        HintOutcome::Remainder(_) => None, // 交回 analyse（analyse 自身会处理 hint 前缀）
        HintOutcome::None => try_local_inner(state, ctx, sql).await,
    }
}

/// hint 指令处理结果。
pub enum HintOutcome {
    /// 已完全处理（含错误）
    Handled(Result<EngineOutcome, EngineError>),
    /// 指令已执行，但后面还带 SQL（需继续执行剩余语句）
    Remainder(String),
    /// 无 hint 指令
    None,
}

/// 执行 hint 指令前缀（对应 Java HintHandler）：
/// - `showConfigText`：导出配置全文 JSON
/// - `showSlowSql[{"trace_id": n}]`：慢 SQL 记录
/// - `setSqlTimeFilter{"value": n}` / `getSqlTimeFilter`：慢 SQL 阈值
/// - `showTopology{表}`：输出表物理分片布局
/// - `createDataSource{...}` 等配置/运维指令（ddl::handle_config_hint）
/// - `schema=xx`：本地 SHOW/元数据命令按 hint schema 解析（仅分片模式）
///
/// `proxy_mode=true`：指令后的剩余 SQL 返回 `Remainder` 由代理路径接管；
/// 其他情况：本地语句继续走 `try_local_inner`，非本地返回 `Remainder` 交回路由。
pub async fn run_hint_commands(
    state: &Arc<ServerState>,
    ctx: &SessionContext,
    sql: &str,
    proxy_mode: bool,
) -> HintOutcome {
    let (hint, effective_sql) = mycat3_sql::parse_hint(sql);
    let Some(hint) = hint else {
        return HintOutcome::None;
    };
    // 未识别任何有效指令的 mycat hint：显式报错（Java 静默忽略）
    if !hint.unknown.is_empty() && hint.is_empty() {
        return HintOutcome::Handled(Err(EngineError::new(
            1235,
            "42000",
            format!(
                "未知 mycat hint 指令: {}（Pro 未实现该指令）",
                hint.unknown.join(", ")
            ),
        )));
    }
    // loaddata 需要分片元数据：代理模式下显式拒绝
    if hint.loaddata.is_some() && proxy_mode {
        return HintOutcome::Handled(Err(EngineError::new(
            1235,
            "42000",
            "代理模式不支持 loaddata（需要分片元数据）",
        )));
    }
    let handled: Option<Result<EngineOutcome, EngineError>> = if hint.show_config_text {
        Some(show_config_text(state))
    } else if let Some(filter) = &hint.show_slow_sql {
        Some(show_slow_sql(state, *filter))
    } else if let Some(ms) = hint.set_sql_time_filter {
        state.slow_log.set_threshold(ms);
        Some(Ok(EngineOutcome::Ok {
            affected_rows: 0,
            last_insert_id: 0,
        }))
    } else if hint.get_sql_time_filter {
        Some(Ok(get_sql_time_filter(state)))
    } else if let Some((cmd, body)) = &hint.command {
        Some(crate::ddl::handle_config_hint(state, ctx, cmd, body).await)
    } else if let Some(tbl) = &hint.show_topology {
        Some(show_topology(state, ctx, tbl))
    } else {
        None
    };
    if let Some(handled) = handled {
        if effective_sql.trim().is_empty() {
            return HintOutcome::Handled(handled);
        }
        if proxy_mode {
            return HintOutcome::Remainder(effective_sql);
        }
        if let Some(r) = try_local_inner(state, ctx, &effective_sql).await {
            return HintOutcome::Handled(r);
        }
        return HintOutcome::Remainder(effective_sql);
    }
    if !proxy_mode {
        if let Some(schema) = &hint.schema {
            let mut ctx2 = ctx.clone();
            ctx2.schema = Some(schema.clone());
            if let Some(r) = try_local_inner(state, &ctx2, &effective_sql).await {
                return HintOutcome::Handled(r);
            }
        }
    }
    HintOutcome::None
}

// ---------------------------------------------------------------------------
// 纯代理模式执行（server.mode=proxy）：不分片/不改写，仅拦截/缓存/慢 SQL
// ---------------------------------------------------------------------------

/// 生成单目标透传路由（无表名重写）。
pub fn proxy_route(state: &Arc<ServerState>, sql: &str) -> RouteResult {
    RouteResult {
        stmt_type: StmtType::Other,
        tasks: vec![BackendTask::new(state.proxy_target(), sql.to_string())],
        merge: None,
        use_schema: None,
        hint: None,
        explain_rows: Vec::new(),
    }
}

/// 语句类型映射（供 read_only / disallowSqlTypes 检查）。
pub fn proxy_stmt_type(sql: &str) -> StmtType {
    use crate::intercept::{classify, StmtClass};
    match classify(sql) {
        StmtClass::Select => StmtType::Select,
        StmtClass::Ddl => StmtType::Ddl,
        StmtClass::Dml => {
            let w: String = sql
                .trim_start()
                .chars()
                .take_while(|c| c.is_ascii_alphabetic())
                .collect::<String>()
                .to_ascii_uppercase();
            match w.as_str() {
                "INSERT" => StmtType::Insert,
                "UPDATE" => StmtType::Update,
                "DELETE" => StmtType::Delete,
                "REPLACE" => StmtType::Replace,
                _ => StmtType::Other,
            }
        }
        StmtClass::Other => StmtType::Other,
    }
}

/// 代理模式本地处理（不进入后端）：KILL / show @@* / 事务命令 / SET / USE /
/// setReadyToCloseSQL 探针 / 变量探针。返回 None 表示需透传到后端。
pub async fn proxy_local_inner(
    state: &Arc<ServerState>,
    ctx: &mut SessionContext,
    tx: &mut TransactionSession,
    sql: &str,
) -> Option<Result<EngineOutcome, EngineError>> {
    let norm = sql.trim().trim_end_matches(';').trim().to_lowercase();
    if let Some(id) = parse_kill(sql) {
        return Some(do_kill(state, id));
    }
    if norm.starts_with("show @@") {
        let mut c = ctx.clone();
        return Some(convert_local(
            crate::commands::handle_show(state, &mut c, sql).await,
        ));
    }
    // 会话命令（事务/SET/USE）：**本地镜像 + 原样透传**
    // 代理模式下连接固定且持久，后端自身处理会话语义（USE 切换库、SET 生效、
    // BEGIN/COMMIT 由后端执行）；本层只同步会话状态用于缓存判定与展示。
    if norm == "begin" || norm.starts_with("start transaction") {
        ctx.in_transaction = true;
        return None;
    }
    if norm == "commit" || norm == "rollback" {
        ctx.in_transaction = false;
        return None;
    }
    if norm.starts_with("rollback to savepoint")
        || norm.starts_with("savepoint ")
        || norm.starts_with("release savepoint ")
    {
        return None;
    }
    if norm.starts_with("use ") {
        let name = sql.trim().trim_end_matches(';').trim()[3..]
            .trim()
            .trim_matches('`')
            .to_string();
        if !name.is_empty() {
            ctx.schema = Some(name);
        }
        return None; // 透传到后端，真正切换连接当前库
    }
    if norm.starts_with("set ") {
        // 本地应用（autocommit/names/@var 等状态镜像），随后透传后端。
        // Mycat 私有状态不能发给 PG/Oracle，否则会中止事务。
        let route = RouteResult::empty(StmtType::Set);
        let out = crate::commands::handle_local(state, ctx, tx, &route, sql).await;
        if norm.starts_with("set @") || norm.contains("transaction_policy") {
            return Some(convert_local(out));
        }
        return None;
    }
    if let Some(ready) = state.ready_to_close_sql.read().as_ref() {
        if sql.trim() == ready.trim() {
            return Some(Ok(EngineOutcome::Ok {
                affected_rows: 0,
                last_insert_id: 0,
            }));
        }
    }
    if let Some(outcome) = probe_local_select(state, ctx, sql) {
        return Some(convert_local(outcome));
    }
    None
}

/// 单条代理语句：拦截（缓存之前）→ 缓存 → 权限 → 透传执行 → 写后缓存失效。
async fn proxy_statement(
    state: &Arc<ServerState>,
    sess: &mut EngineSession,
    sql: &str,
    bypass_cache: bool,
) -> Result<EngineOutcome, EngineError> {
    let rules = state.intercepts();
    if let Some(e) = crate::intercept::check(&rules, sql) {
        return Err(e);
    }
    if !bypass_cache {
        if let Some(cached) = state
            .sql_cache
            .lookup(
                sql,
                sess.ctx.schema.as_deref(),
                sess.ctx.in_transaction,
                sess.ctx.autocommit,
            )
            .await
        {
            return Ok((*cached).clone());
        }
    }
    let stmt_type = proxy_stmt_type(sql);
    let fake = [RouteResult {
        stmt_type,
        tasks: Vec::new(),
        merge: None,
        use_schema: None,
        hint: None,
        explain_rows: Vec::new(),
    }];
    if let Some(e) = check_disallow(state, &sess.ctx, &fake) {
        return Err(e);
    }
    let route = proxy_route(state, sql);
    let out = execute_route_engine(state, sess, &route, sql).await?;
    if matches!(
        stmt_type,
        StmtType::Insert | StmtType::Update | StmtType::Delete | StmtType::Replace | StmtType::Ddl
    ) {
        invalidate_cache_after_write(state, sess.ctx.schema.as_deref(), sql);
    }
    Ok(out)
}

/// 代理模式执行入口：多语句拆分 → hint → 本地命令 → 拦截/缓存/透传。
async fn execute_sql_proxy(
    state: &Arc<ServerState>,
    sess: &mut EngineSession,
    sql: &str,
    trace_id: u64,
    started: Instant,
    bypass_cache: bool,
) -> Result<Vec<EngineOutcome>, EngineError> {
    let mut out = Vec::new();
    // 逆序入栈，按原顺序弹出
    let mut pending: Vec<String> = crate::prepared_text::split_statements(sql)
        .into_iter()
        .rev()
        .collect();
    while let Some(stmt) = pending.pop() {
        match run_hint_commands(state, &sess.ctx, &stmt, true).await {
            HintOutcome::Handled(r) => {
                out.push(r?);
                continue;
            }
            HintOutcome::Remainder(rem) => {
                pending.push(rem);
                continue;
            }
            HintOutcome::None => {}
        }
        if let Some(r) = proxy_local_inner(state, &mut sess.ctx, &mut sess.tx, &stmt).await {
            out.push(r?);
            continue;
        }
        out.push(proxy_statement(state, sess, &stmt, bypass_cache).await?);
    }
    record_slow_sql(
        state,
        trace_id,
        &sess.ctx.user,
        sess.ctx.schema.as_deref(),
        sql,
        out.iter().map(engine_rows).sum(),
        started.elapsed(),
        &state.proxy_target(),
    );
    Ok(out)
}

/// `/*!mycat:showConfigText*/`：配置全文 JSON（一列 CONFIG_TEXT）。
fn show_config_text(state: &Arc<ServerState>) -> Result<EngineOutcome, EngineError> {
    let cfg = state.config();
    let text = serde_json::to_string_pretty(&*cfg)
        .map_err(|e| EngineError::general(format!("config serialize failed: {}", e)))?;
    Ok(EngineOutcome::Rows {
        columns: vec![EngineColumn {
            name: "CONFIG_TEXT".into(),
            mysql_type: field_type::VAR_STRING,
            charset: 33,
        }],
        rows: vec![vec![Some(text)]],
    })
}

/// `/*!mycat:showSlowSql*/`：慢 SQL 记录（列与 Java 一致：
/// trace_id/sql/sql_rows/start_time/end_time/execute_time/target_name，按耗时降序；
/// 支持 {"trace_id": n} 过滤——Java 版此处是对象恒等比较的 bug，Pro 用值比较）。
fn show_slow_sql(
    state: &Arc<ServerState>,
    filter: Option<u64>,
) -> Result<EngineOutcome, EngineError> {
    let columns: Vec<EngineColumn> = [
        "trace_id",
        "sql",
        "sql_rows",
        "start_time",
        "end_time",
        "execute_time",
        "target_name",
    ]
    .iter()
    .map(|n| EngineColumn {
        name: n.to_string(),
        mysql_type: field_type::VAR_STRING,
        charset: 33,
    })
    .collect();
    let rows: Vec<Vec<Option<String>>> = state
        .slow_log
        .snapshot()
        .into_iter()
        .filter(|e| filter.map(|id| e.trace_id == id).unwrap_or(true))
        .map(|e| {
            vec![
                Some(e.trace_id.to_string()),
                Some(e.sql),
                Some(e.rows.to_string()),
                Some(e.start_time.to_string()),
                Some(e.end_time.to_string()),
                Some(e.execute_ms.to_string()),
                Some(e.target_name),
            ]
        })
        .collect();
    Ok(EngineOutcome::Rows { columns, rows })
}

/// `/*!mycat:getSqlTimeFilter*/`：当前慢 SQL 阈值（列名 value，与 Java 一致）。
fn get_sql_time_filter(state: &Arc<ServerState>) -> EngineOutcome {
    EngineOutcome::Rows {
        columns: vec![EngineColumn {
            name: "value".into(),
            mysql_type: field_type::VAR_STRING,
            charset: 33,
        }],
        rows: vec![vec![Some(state.slow_log.threshold().to_string())]],
    }
}

/// `SHOW [FULL] STATISTIC`：Mycat 专有性能指标表（列与 Java ShowStatisticHandler 一致；
/// Java 只回列头不回数据行，Pro 返回一行按采样间隔计算的真实速率）。
fn show_statistic(state: &Arc<ServerState>, full: bool) -> EngineOutcome {
    let (elapsed, prev, cur) = state.stats.sample_delta();
    let secs = elapsed.as_secs_f64().max(0.001);
    // snapshot 下标：[0]conn_total [1]conn_active [2]queries [3]errors
    //               [4]slow [5]tx_commit [6]tx_rollback [7]bytes_in [8]bytes_out
    let rate = |idx: usize| (cur[idx].saturating_sub(prev[idx]) as f64 / secs).round() as u64;
    let kb =
        |idx: usize| (cur[idx].saturating_sub(prev[idx]) as f64 / secs / 1024.0).round() as u64;
    let mut cols: Vec<&str> = vec![
        "QPS",
        "RDS_QPS",
        "SLOW_QPS",
        "PHYSICAL_SLOW_QPS",
        "ERROR_PER_SECOND",
        "MERGE_QUERY_PER_SECOND",
        "ACTIVE_CONNECTIONS",
        "RT(MS)",
        "RDS_RT(MS)",
        "NET_IN(KB/S)",
        "NET_OUT(KB/S)",
        "THREAD_RUNNING",
    ];
    let mut vals: Vec<u64> = vec![
        rate(2),
        0, // RDS_QPS（物理后端计数未采集）
        rate(4),
        0,
        rate(3),
        0,
        cur[1],
        0, // RT(MS)（无聚合计时）
        0,
        kb(7),
        kb(8),
        0,
    ];
    if full {
        cols.extend([
            "VIOLATION_PER_SECOND",
            "CONNECTION_CREATE_PER_SECOND",
            "HINT_USED_PER_SECOND",
            "HINT_USED_COUNT",
            "AGGREGATE_QUERY_PER_SECOND",
            "AGGREGATE_QUERY_COUNT",
            "TEMP_TABLE_CREATE_PER_SECOND",
            "TEMP_TABLE_CREATE_COUNT",
            "MULTI_DB_JOIN_PER_SECOND",
            "MULTI_DB_JOIN_COUNT",
            "CPU",
            "FREEMEM",
            "FULLGCCOUNT",
            "FULLGCTIME",
        ]);
        vals.extend([0, rate(0), 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]);
    }
    let columns: Vec<EngineColumn> = cols
        .iter()
        .map(|n| EngineColumn {
            name: n.to_string(),
            mysql_type: field_type::VAR_STRING,
            charset: 33,
        })
        .collect();
    let rows: Vec<Vec<Option<String>>> =
        vec![vals.into_iter().map(|v| Some(v.to_string())).collect()];
    EngineOutcome::Rows { columns, rows }
}

/// 写语句成功后：提取受影响逻辑表并触发结果缓存失效（启发式，保守安全）。
pub fn invalidate_cache_after_write(state: &Arc<ServerState>, schema: Option<&str>, sql: &str) {
    use sqlparser::ast::{VisitMut, VisitorMut};
    struct Tables {
        out: Vec<(String, String)>,
    }
    impl VisitorMut for Tables {
        type Break = ();
        fn pre_visit_relation(
            &mut self,
            r: &mut sqlparser::ast::ObjectName,
        ) -> std::ops::ControlFlow<()> {
            let parts: Vec<String> = r.0.iter().map(|i| i.value.to_lowercase()).collect();
            match parts.len() {
                1 => self.out.push(("".to_string(), parts[0].clone())),
                2 => self.out.push((parts[0].clone(), parts[1].clone())),
                _ => {}
            }
            std::ops::ControlFlow::Continue(())
        }
    }
    let Ok(mut stmts) =
        sqlparser::parser::Parser::parse_sql(&sqlparser::dialect::MySqlDialect {}, sql)
    else {
        return;
    };
    let mut v = Tables { out: Vec::new() };
    for stmt in stmts.iter_mut() {
        let _ = VisitMut::visit(stmt, &mut v);
    }
    for (s, t) in v.out {
        let schema = if s.is_empty() {
            schema.unwrap_or("").to_string()
        } else {
            s
        };
        state.sql_cache.invalidate_table(&schema, &t);
    }
}

/// 记录慢 SQL（超过阈值才落账）。`trace_id` 由调用方从查询计数取得。
pub fn record_slow_sql(
    state: &Arc<ServerState>,
    trace_id: u64,
    user: &str,
    schema: Option<&str>,
    sql: &str,
    rows: u64,
    elapsed: std::time::Duration,
    target: &str,
) {
    let ms = elapsed.as_millis() as u64;
    if ms < state.slow_log.threshold() {
        return;
    }
    let now = crate::state::epoch_millis();
    state.slow_log.record_if(crate::state::SlowEntry {
        trace_id,
        user: user.to_string(),
        schema: schema.unwrap_or_default().to_string(),
        sql: sql.chars().take(1024).collect(),
        rows,
        start_time: now.saturating_sub(ms),
        end_time: now,
        execute_ms: ms,
        target_name: target.to_string(),
    });
}

/// 本地快路径主体（不含 hint 预处理）。
async fn try_local_inner(
    state: &Arc<ServerState>,
    ctx: &SessionContext,
    sql: &str,
) -> Option<Result<EngineOutcome, EngineError>> {
    // SHOW [FULL] STATISTIC：Mycat 专有性能指标表（Java ShowStatisticHandler）
    let norm_stat = sql.trim().to_ascii_lowercase();
    if norm_stat == "show statistic" || norm_stat == "show full statistic" {
        return Some(Ok(show_statistic(
            state,
            norm_stat.starts_with("show full"),
        )));
    }
    if crate::ddl::is_config_ddl(sql) {
        return Some(crate::ddl::handle_config_ddl(state, ctx.schema.as_deref(), sql).await);
    }
    if let Some(id) = parse_kill(sql) {
        return Some(do_kill(state, id));
    }
    if let Some(r) = crate::infoschema::try_information_schema(state, ctx, sql) {
        return Some(r);
    }
    if let Some(outcome) = crate::infoschema::try_processlist(state, ctx, sql) {
        return Some(Ok(outcome));
    }
    if let Some(outcome) = crate::infoschema::try_show_local(state, ctx, sql) {
        return Some(Ok(outcome));
    }
    // setReadyToCloseSQL 探针：命中即回 OK（运维发布窗口确认可关停）
    if let Some(ready) = state.ready_to_close_sql.read().as_ref() {
        if sql.trim() == ready.trim() {
            return Some(Ok(EngineOutcome::Ok {
                affected_rows: 0,
                last_insert_id: 0,
            }));
        }
    }
    if let Some(outcome) = probe_local_select(state, ctx, sql) {
        return Some(convert_local(outcome));
    }
    None
}

/// `/*!mycat:showTopology{表名}*/`：输出表物理分片布局。
/// 列与 Java `HintHandler.showTopology` 一致：
/// TARGET_NAME / SCHEMA_NAME / TABLE_NAME / DB_INDEX / TABLE_INDEX / INDEX。
fn show_topology(
    state: &Arc<ServerState>,
    ctx: &SessionContext,
    table: &str,
) -> Result<EngineOutcome, EngineError> {
    use mycat3_protocol::consts::field_type;
    let schema = ctx
        .schema
        .clone()
        .ok_or_else(|| EngineError::new(1046, "3D000", "No database selected"))?;
    let meta = state.metadata();
    let schema_meta = meta
        .get_schema(&schema)
        .ok_or_else(|| EngineError::new(1049, "42000", format!("Unknown database '{}'", schema)))?;
    let tbl = table
        .trim()
        .trim_matches(|c| c == '`' || c == '\'' || c == '"');
    let columns: Vec<EngineColumn> = [
        "TARGET_NAME",
        "SCHEMA_NAME",
        "TABLE_NAME",
        "DB_INDEX",
        "TABLE_INDEX",
        "INDEX",
    ]
    .iter()
    .map(|n| EngineColumn {
        name: n.to_string(),
        mysql_type: field_type::VAR_STRING,
        charset: 33,
    })
    .collect();
    let rows: Vec<Vec<Option<String>>> = match schema_meta.get_table(tbl) {
        Some(mycat3_sql::TableMeta::Sharding { nodes, .. }) => nodes
            .iter()
            .enumerate()
            .map(|(i, n)| {
                vec![
                    Some(n.target_name.clone()),
                    Some(n.schema_name.clone()),
                    Some(n.table_name.clone()),
                    Some(n.db_index.to_string()),
                    Some(n.table_index.to_string()),
                    Some(n.index.max(i as i32).to_string()),
                ]
            })
            .collect(),
        Some(mycat3_sql::TableMeta::Global { target_schemas, .. }) => target_schemas
            .iter()
            .enumerate()
            .map(|(i, (t, s))| {
                vec![
                    Some(t.clone()),
                    Some(s.clone()),
                    Some(tbl.to_string()),
                    Some("-1".into()),
                    Some("-1".into()),
                    Some(i.to_string()),
                ]
            })
            .collect(),
        Some(mycat3_sql::TableMeta::Normal {
            target,
            schema: s,
            table: phys,
            ..
        }) => vec![vec![
            Some(target.clone()),
            Some(if s.is_empty() {
                schema.clone()
            } else {
                s.clone()
            }),
            Some(phys.clone()),
            Some("-1".into()),
            Some("-1".into()),
            Some("0".into()),
        ]],
        _ => {
            return Err(EngineError::new(
                1146,
                "42S02",
                format!("Table '{}.{}' doesn't exist", schema, tbl),
            ))
        }
    };
    Ok(EngineOutcome::Rows { columns, rows })
}

/// 解析 `KILL [CONNECTION|QUERY] <id>`。
pub fn parse_kill(sql: &str) -> Option<u32> {
    let norm = sql.trim().trim_end_matches(';');
    let lower = norm.to_lowercase();
    if !lower.starts_with("kill ") {
        return None;
    }
    let rest = norm[5..].trim();
    let rest = rest
        .strip_prefix("connection")
        .or_else(|| rest.strip_prefix("CONNECTION"))
        .or_else(|| rest.strip_prefix("query"))
        .or_else(|| rest.strip_prefix("QUERY"))
        .map(|s| s.trim())
        .unwrap_or(rest);
    rest.split_whitespace()
        .next()
        .and_then(|t| t.parse::<u32>().ok())
}

fn do_kill(state: &Arc<ServerState>, id: u32) -> Result<EngineOutcome, EngineError> {
    if state.processes.kill(id) {
        Ok(EngineOutcome::Ok {
            affected_rows: 0,
            last_insert_id: 0,
        })
    } else {
        Err(EngineError::new(
            1094,
            "HY000",
            format!("Unknown thread id: {}", id),
        ))
    }
}

async fn execute_route_engine(
    state: &Arc<ServerState>,
    sess: &mut EngineSession,
    route: &RouteResult,
    raw_sql: &str,
) -> Result<EngineOutcome, EngineError> {
    // 本地语句
    if route.tasks.is_empty() {
        let outcome = handle_local(state, &mut sess.ctx, &mut sess.tx, route, raw_sql).await;
        if matches!(outcome, LocalOutcome::Nothing) {
            // SHOW 转发 prototype（与 MySQL 前端一致）
            let target = state.metadata().prototype.clone();
            let proxy = RouteResult {
                stmt_type: StmtType::Other,
                tasks: vec![BackendTask::new(target, raw_sql.to_string())],
                merge: None,
                use_schema: None,
                hint: None,
                explain_rows: vec![],
            };
            return exec_backend(state, sess, &proxy).await;
        }
        return convert_local(outcome);
    }
    exec_backend(state, sess, route).await
}

async fn exec_backend(
    state: &Arc<ServerState>,
    sess: &mut EngineSession,
    route: &RouteResult,
) -> Result<EngineOutcome, EngineError> {
    let resp = execute_route(state, &mut sess.ctx, &mut sess.tx, route)
        .await
        .map_err(|e| {
            let (errno, sqlstate, message) = e.to_packet_parts();
            state.stats.incr_error();
            EngineError {
                errno,
                sqlstate,
                message,
            }
        })?;
    // 非事务语句结束即释放语句级连接
    sess.tx.close_statement_state(sess.ctx.autocommit);
    match resp {
        ExecResponse::Ok {
            affected_rows,
            last_insert_id,
        } => Ok(EngineOutcome::Ok {
            affected_rows,
            last_insert_id,
        }),
        ExecResponse::Rows(rs) => Ok(result_set_to_outcome(rs)),
        ExecResponse::Empty => Ok(EngineOutcome::Ok {
            affected_rows: 0,
            last_insert_id: 0,
        }),
    }
}

/// 结果集（原始载荷）→ 协议无关字符串行。
pub fn result_set_to_outcome(rs: mycat3_client::ResultSet) -> EngineOutcome {
    let columns: Vec<EngineColumn> = rs
        .columns
        .iter()
        .map(|c| EngineColumn {
            name: c.name.clone(),
            mysql_type: c.field_type,
            charset: c.charset,
        })
        .collect();
    let rows: Vec<Vec<Option<String>>> = rs
        .decode_rows()
        .into_iter()
        .map(|row| {
            row.into_iter()
                .map(|v| v.map(|b| String::from_utf8_lossy(&b).into_owned()))
                .collect()
        })
        .collect();
    EngineOutcome::Rows { columns, rows }
}

fn convert_local(outcome: LocalOutcome) -> Result<EngineOutcome, EngineError> {
    match outcome {
        LocalOutcome::Ok | LocalOutcome::Nothing => Ok(EngineOutcome::Ok {
            affected_rows: 0,
            last_insert_id: 0,
        }),
        LocalOutcome::Error {
            errno,
            sqlstate,
            message,
        } => Err(EngineError {
            errno,
            sqlstate,
            message,
        }),
        LocalOutcome::Rows { columns, rows } => Ok(EngineOutcome::Rows {
            columns: columns
                .iter()
                .map(|c| EngineColumn {
                    name: c.name.clone(),
                    mysql_type: c.field_type,
                    charset: c.charset,
                })
                .collect(),
            rows,
        }),
    }
}

// ---------------------------------------------------------------------------
// INSERT 自增序列填充
// ---------------------------------------------------------------------------

/// 若为需要序列填充的 INSERT，返回重写后的 SQL；否则 None。
///
/// 对应 Java `VertxExecuter.needFillAutoIncrement` + `explainGlobalInsertSequence01`：
/// - GLOBAL 表（sequenceType=GLOBAL_SEQUENCE 且多广播节点）与 SHARDING 表
///   （autoIncrement 配置或 DDL 含 AUTO_INCREMENT 列）
/// - 列缺失 → 追加列并为每行追加序列值；列值为 NULL/0 → 替换为序列值
/// - NORMAL 表不填充（依赖物理 auto_increment）
/// - 填充发生在分片路由之前（序列值可作为分片键 —— 与 Java 顺序一致）
pub fn fill_insert_sequences(
    state: &Arc<ServerState>,
    ctx: &SessionContext,
    sql: &str,
) -> Option<String> {
    let lower = sql.trim_start().to_lowercase();
    if !(lower.starts_with("insert") || lower.starts_with("replace")) {
        return None;
    }
    let mut stmts =
        sqlparser::parser::Parser::parse_sql(&sqlparser::dialect::MySqlDialect {}, sql).ok()?;
    let stmt = stmts.first_mut()?;
    let sqlparser::ast::Statement::Insert(insert) = stmt else {
        return None;
    };
    let parts: Vec<String> = insert
        .table_name
        .0
        .iter()
        .map(|i| i.value.clone())
        .collect();
    let (schema_name, table_name) = match parts.len() {
        1 => (ctx.schema.clone()?, parts[0].clone()),
        2 => (parts[0].clone(), parts[1].clone()),
        _ => return None,
    };
    let meta = state.metadata();
    let schema_meta = meta.get_schema(&schema_name)?;
    let tmeta = schema_meta.get_table(&table_name)?;

    // 判定是否需要填充 + 找自增列
    let (need_fill, auto_col, create_sql) = match tmeta {
        TableMeta::Global {
            targets,
            sequence_type,
            create_sql,
            ..
        } => {
            let st = sequence_type.as_deref().unwrap_or("NO_SEQUENCE");
            (
                st == "GLOBAL_SEQUENCE" && targets.len() > 1,
                None,
                create_sql.as_deref(),
            )
        }
        TableMeta::Sharding { create_sql, .. } => (true, None, create_sql.as_deref()),
        _ => return None, // NORMAL/VIEW 不填充
    };
    if !need_fill {
        return None;
    }
    // 自增列：DDL 中第一个 AUTO_INCREMENT 列
    let ddl = create_sql?;
    let auto_col = auto_col.or_else(|| {
        crate::infoschema::parse_create_table_columns(ddl)
            .into_iter()
            .find(|c| c.auto_increment)
            .map(|c| c.name)
    })?;

    // VALUES 行处理
    let q = insert.source.as_mut()?;
    let sqlparser::ast::SetExpr::Values(values) = q.body.as_mut() else {
        return None;
    };
    let existing_idx = insert
        .columns
        .iter()
        .position(|c| c.value.eq_ignore_ascii_case(&auto_col));
    let gen = state.sequences();
    let unique = format!("{}.{}", schema_name, table_name);
    let lit =
        |v: i64| sqlparser::ast::Expr::Value(sqlparser::ast::Value::Number(v.to_string(), false));
    match existing_idx {
        Some(idx) => {
            // 列存在：NULL/0 值替换
            let mut changed = false;
            for row in values.rows.iter_mut() {
                if let Some(cell) = row.get_mut(idx) {
                    let is_null_or_zero = match cell {
                        sqlparser::ast::Expr::Value(sqlparser::ast::Value::Null) => true,
                        sqlparser::ast::Expr::Value(sqlparser::ast::Value::Number(n, _)) => {
                            n.parse::<i64>().map(|x| x == 0).unwrap_or(false)
                        }
                        _ => false,
                    };
                    if is_null_or_zero {
                        *cell = lit(gen.next_id(&unique));
                        changed = true;
                    }
                }
            }
            if !changed {
                return None;
            }
        }
        None => {
            // 列缺失：仅当显式列清单存在时追加（INSERT 无列清单时无法安全定位列序）
            if insert.columns.is_empty() {
                return None;
            }
            insert
                .columns
                .push(sqlparser::ast::Ident::new(auto_col.clone()));
            for row in values.rows.iter_mut() {
                row.push(lit(gen.next_id(&unique)));
            }
        }
    }
    Some(stmt.to_string())
}

fn sql_error(e: mycat3_sql::SqlError) -> EngineError {
    use mycat3_sql::SqlError::*;
    match e {
        Parse(m) => EngineError::new(
            mycat3_protocol::consts::error_code::ER_PARSE_ERROR,
            "42000",
            m,
        ),
        UnknownDatabase(db) => EngineError::new(
            mycat3_protocol::consts::error_code::ER_BAD_DB_ERROR,
            "42000",
            format!("Unknown database '{}'", db),
        ),
        NoDatabase => EngineError::new(
            mycat3_protocol::consts::error_code::ER_NO_DB_ERROR,
            "3D000",
            "No database selected",
        ),
        Unsupported(m) => EngineError::unsupported(m),
        Route(m) => EngineError::general(m),
    }
}

/// MySQL 字段类型是否为"数值型"（PG/TDS 类型映射用）。
pub fn is_numeric_mysql_type(t: u8) -> bool {
    matches!(
        t,
        field_type::TINY
            | field_type::SHORT
            | field_type::LONG
            | field_type::FLOAT
            | field_type::DOUBLE
            | field_type::LONGLONG
            | field_type::INT24
            | field_type::YEAR
            | field_type::NEWDECIMAL
            | field_type::DECIMAL
    )
}

/// 是否时间类型。
pub fn is_temporal_mysql_type(t: u8) -> bool {
    matches!(
        t,
        field_type::DATE
            | field_type::DATETIME
            | field_type::TIMESTAMP
            | field_type::TIME
            | field_type::NEWDATE
    )
}

/// 让 handle_show 可从 engine 调用（重导出，避免协议前端依赖 commands 细节）。
pub async fn proxy_show(
    state: &Arc<ServerState>,
    sess: &mut EngineSession,
    sql: &str,
) -> Option<EngineOutcome> {
    match handle_show(state, &mut sess.ctx, sql).await {
        LocalOutcome::Nothing => None,
        other => convert_local(other).ok(),
    }
}

/// 供 TDS/PG 判断 SELECT 行数的 CommandTag 生成（对应 PGResponse.commandTagFor）。
pub fn command_tag(stmt: StmtType, affected: u64, rows: usize) -> String {
    match stmt {
        StmtType::Insert => format!("INSERT 0 {}", affected),
        StmtType::Update => format!("UPDATE {}", affected),
        StmtType::Delete => format!("DELETE {}", affected),
        StmtType::Select => format!("SELECT {}", rows),
        _ => "OK".to_string(),
    }
}

#[allow(dead_code)]
fn outcome_from_query(out: QueryOutcome) -> EngineOutcome {
    match out {
        QueryOutcome::Ok(ok) => EngineOutcome::Ok {
            affected_rows: ok.affected_rows,
            last_insert_id: ok.last_insert_id,
        },
        QueryOutcome::Rows(rs) => result_set_to_outcome(rs),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mycat3_config::default_config;
    use mycat3_protocol::packets::{build_text_row, ColumnDef};

    fn state() -> Arc<ServerState> {
        ServerState::new(default_config()).unwrap()
    }

    fn session() -> EngineSession {
        let mut ctx = SessionContext::new(1, "127.0.0.1".into());
        ctx.user = "root".into();
        ctx.schema = Some("db1".into());
        EngineSession::new(ctx)
    }

    #[tokio::test]
    async fn test_local_commands_via_engine() {
        let st = state();
        let mut sess = session();
        // show databases → Rows
        let out = execute_sql(&st, &mut sess, "show databases").await.unwrap();
        match &out[0] {
            EngineOutcome::Rows { columns, rows } => {
                assert_eq!(columns[0].name, "Database");
                assert!(rows.iter().any(|r| r[0].as_deref() == Some("db1")));
            }
            _ => panic!("expected rows"),
        }
        // probe select
        let out = execute_sql(&st, &mut sess, "select @@version")
            .await
            .unwrap();
        match &out[0] {
            EngineOutcome::Rows { rows, .. } => {
                assert!(rows[0][0].as_ref().unwrap().contains("mycat"));
            }
            _ => panic!(),
        }
        // explain → Rows(DATA_NODE, SQL)；default_config 的 db1.travelrecord 在 c0（无该集群数据源，
        // explain 只做路由分析不执行）
        let out = execute_sql(&st, &mut sess, "explain select * from travelrecord")
            .await
            .unwrap();
        match &out[0] {
            EngineOutcome::Rows { columns, rows } => {
                assert_eq!(columns[0].name, "DATA_NODE");
                assert_eq!(rows[0][0].as_deref(), Some("c0"));
            }
            _ => panic!(),
        }
        // SET → Ok
        let out = execute_sql(&st, &mut sess, "set autocommit=0")
            .await
            .unwrap();
        assert!(matches!(out[0], EngineOutcome::Ok { .. }));
        assert!(!sess.ctx.autocommit);
    }

    #[tokio::test]
    async fn test_error_via_engine() {
        let st = state();
        let mut sess = session();
        let e = execute_sql(&st, &mut sess, "selct bad syntax")
            .await
            .unwrap_err();
        assert_eq!(e.errno, 1064);
        let e = execute_sql(&st, &mut sess, "select * from t").await; // 无 schema 问题：db1 存在，t 未知 → 透传 c0 → 无数据源
        assert!(e.is_err());
        // 多语句
        let out = execute_sql(&st, &mut sess, "show databases; show tables")
            .await
            .unwrap();
        assert_eq!(out.len(), 2);
    }

    #[tokio::test]
    async fn test_backend_unreachable_error_mapping() {
        // default_config 的 prototypeDs 指向 127.0.0.1:3306（无 MySQL）→ 连接错误应转为 EngineError
        let st = state();
        let mut sess = session();
        let e = execute_sql(&st, &mut sess, "select * from travelrecord")
            .await
            .unwrap_err();
        assert!(!e.message.is_empty());
        assert_eq!(e.sqlstate, "HY000");
    }

    #[test]
    fn test_result_set_conversion() {
        let columns = vec![
            ColumnDef::new("a", field_type::LONG),
            ColumnDef::new("b", field_type::VAR_STRING),
        ];
        let rs = mycat3_client::ResultSet {
            column_count: 2,
            column_def_payloads: columns.iter().map(|c| c.encode()).collect(),
            columns,
            row_payloads: vec![build_text_row(&[Some(b"1"), None])],
            final_status: 2,
            warnings: 0,
        };
        match result_set_to_outcome(rs) {
            EngineOutcome::Rows { columns, rows } => {
                assert_eq!(columns[0].mysql_type, field_type::LONG);
                assert_eq!(rows[0][0].as_deref(), Some("1"));
                assert_eq!(rows[0][1], None); // NULL 保真（Java TDS 版会丢）
            }
            _ => panic!(),
        }
    }

    #[test]
    fn test_command_tags() {
        assert_eq!(command_tag(StmtType::Insert, 3, 0), "INSERT 0 3");
        assert_eq!(command_tag(StmtType::Update, 2, 0), "UPDATE 2");
        assert_eq!(command_tag(StmtType::Select, 0, 7), "SELECT 7");
    }

    #[test]
    fn test_type_classification() {
        assert!(is_numeric_mysql_type(field_type::LONGLONG));
        assert!(!is_numeric_mysql_type(field_type::VAR_STRING));
        assert!(is_temporal_mysql_type(field_type::DATETIME));
    }
}
