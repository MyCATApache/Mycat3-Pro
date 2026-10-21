//! 执行引擎：路由任务 → 后端执行 → 结果归并。
//!
//! 对应 Mycat3 `ReceiverImpl.execute(ExplainDetail)` + `VertxExecuter`：
//! - target（集群名）经 `ReplicaSelectorRuntime` 语义解析为物理数据源
//!   （写/事务中 → 主库；读 → 负载均衡 + 会话粘性）
//! - 后端按数据源 dbType 分派（`backend::BackendPool`）：MySQL 原生协议（原始载荷
//!   透传快路径）与 Oracle（官方 rust-oracledb thin 驱动；SQL 经
//!   `translate_oracle_sql` 方言翻译；结果合成 MySQL 文本行载荷，归并零改动）
//! - 事务内连接按数据源固定（`TransactionSession`）
//! - 多任务读按数据源分组并行；写顺序执行，affectedRows 求和（对应 CompositeFuture.join）

use std::sync::Arc;
use std::time::Duration;

use mycat3_client::{decode_text_row, ResultSet};
use mycat3_mssql::{MssqlColumn, MssqlRowStream};
use mycat3_protocol::packets::build_text_row;
use mycat3_protocol::packets::ColumnDef;
use mycat3_replica::PhysicsInstance;
use mycat3_sql::{MergeOp, MergePlan, RouteResult, StmtType};
use thiserror::Error;

use crate::backend::{
    mssql_err, translate_for_backend, BackendError, BackendOutcome, BackendPool, BackendSession,
};
use crate::session::SessionContext;
use crate::state::ServerState;
use crate::tx::TransactionSession;

/// active 连接计数 RAII 守卫：acquire 失败等提前返回路径也必须对齐 decr_active，
/// 否则 `show @@cluster` 的 ACTIVE 与 BalanceLeastActive 只增不减。
struct ActiveGuard {
    inst: Option<Arc<PhysicsInstance>>,
}

impl ActiveGuard {
    fn new(inst: Option<Arc<PhysicsInstance>>) -> Self {
        if let Some(i) = &inst {
            i.incr_active();
        }
        ActiveGuard { inst }
    }
}

impl Drop for ActiveGuard {
    fn drop(&mut self) {
        if let Some(i) = &self.inst {
            i.decr_active();
        }
    }
}

#[derive(Debug, Error)]
pub enum ExecError {
    #[error("backend error {errno} ({sqlstate}): {message}")]
    Backend {
        errno: u16,
        sqlstate: String,
        message: String,
    },
    #[error("datasource not found for target: {0}")]
    NoDatasource(String),
    #[error("connection error: {0}")]
    Conn(String),
    #[error("{0}")]
    Other(String),
}

impl ExecError {
    /// 转为 ERR 包三元组 (errno, sqlstate, message)。
    pub fn to_packet_parts(&self) -> (u16, String, String) {
        match self {
            ExecError::Backend {
                errno,
                sqlstate,
                message,
            } => (*errno, sqlstate.clone(), message.clone()),
            ExecError::NoDatasource(t) => (
                mycat3_protocol::consts::error_code::ER_UNKNOWN_ERROR,
                "HY000".into(),
                format!("no available datasource for target '{}'", t),
            ),
            other => (
                mycat3_protocol::consts::error_code::ER_UNKNOWN_ERROR,
                "HY000".into(),
                other.to_string(),
            ),
        }
    }
}

impl From<BackendError> for ExecError {
    fn from(e: BackendError) -> Self {
        match e {
            BackendError::Server {
                errno,
                sqlstate,
                message,
            } => ExecError::Backend {
                errno,
                sqlstate,
                message,
            },
            BackendError::Conn(m) => ExecError::Conn(m),
        }
    }
}

/// 执行结果（前端负责写回协议包）。
pub enum ExecResponse {
    /// OK 包
    Ok {
        affected_rows: u64,
        last_insert_id: u64,
    },
    /// 结果集（原始载荷透传形式；Oracle 结果已合成为文本行载荷）
    Rows(ResultSet),
    /// 无需响应（不应出现；前端兜底）
    Empty,
}

/// 单分片、无归并的 SQL Server 流式结果。
///
/// 列元数据在返回前已就绪；行通过有界 channel 逐行获取，后台任务持有
/// 连接并负责归还/丢弃。`ActiveGuard` 让负载均衡的 active 计数覆盖整个
/// 流式读取周期。
pub struct StreamingResult {
    pub columns: Vec<ColumnDef>,
    pub column_def_payloads: Vec<Vec<u8>>,
    pub column_count: usize,
    stream: MssqlRowStream,
    _guard: Option<ActiveGuard>,
}

impl StreamingResult {
    pub async fn next_row(&mut self) -> Result<Option<Vec<Option<String>>>, BackendError> {
        self.stream.next_row().await.map_err(mssql_err)
    }
}

fn mssql_stream_columns(columns: &[MssqlColumn]) -> (Vec<ColumnDef>, Vec<Vec<u8>>) {
    let defs: Vec<ColumnDef> = columns
        .iter()
        .map(|c| {
            let mut def = ColumnDef::new(&c.name, c.mysql_type);
            if c.mysql_type == mycat3_protocol::consts::field_type::BLOB {
                def.charset = mycat3_protocol::consts::charset::BINARY as u16;
            }
            def
        })
        .collect();
    let payloads = defs.iter().map(|c| c.encode()).collect();
    (defs, payloads)
}

/// 尝试为单分片 SQL Server 读语句建立流式结果。
///
/// 返回 `Ok(None)` 表示不满足流式条件（多分片/归并/事务/非 SQL Server），
/// 调用方应走原有物化路径。事务内连接固定语义、多分片归并和缓存仍走物化。
pub async fn execute_route_stream(
    state: &Arc<ServerState>,
    ctx: &mut SessionContext,
    tx: &mut TransactionSession,
    route: &RouteResult,
) -> Result<Option<StreamingResult>, ExecError> {
    if route.tasks.len() != 1
        || route.stmt_type != StmtType::Select
        || route.merge.is_some()
        || route.hint.is_some()
        || !ctx.autocommit
        || ctx.in_transaction
        || tx.explicit_tx
    {
        return Ok(None);
    }
    let task = &route.tasks[0];
    let sticky = tx.nearness_get(&task.target_name).map(String::from);
    let ds = match sticky {
        Some(ds)
            if state
                .replica()
                .instance(&ds)
                .map(|i| i.is_alive())
                .unwrap_or(false) =>
        {
            ds
        }
        _ => {
            let ds = state
                .replica()
                .resolve(&task.target_name, false, false, false)
                .ok_or_else(|| ExecError::NoDatasource(task.target_name.clone()))?;
            tx.nearness_put(&task.target_name, &ds);
            ds
        }
    };
    if tx.contains(&ds) {
        return Ok(None);
    }
    let pool = match BackendPool::get(state, &ds)? {
        BackendPool::Mssql(p) => p,
        _ => return Ok(None),
    };
    let sql =
        translate_for_backend(state, &ds, &task.sql).map_err(|e| ExecError::Conn(e.to_string()))?;
    let timeout = exec_timeout(state, route);
    let mut stream = pool
        .stream_query(sql, timeout, 128)
        .await
        .map_err(|e| ExecError::from(mssql_err(e)))?;
    let columns = stream
        .ready()
        .await
        .map_err(|e| ExecError::from(mssql_err(e)))?
        .to_vec();
    let (defs, payloads) = mssql_stream_columns(&columns);
    tracing::debug!(datasource = %ds, columns = defs.len(), "SQL Server streaming result started");
    Ok(Some(StreamingResult {
        column_count: defs.len(),
        columns: defs,
        column_def_payloads: payloads,
        stream,
        _guard: Some(ActiveGuard::new(state.replica().instance(&ds))),
    }))
}

fn exec_timeout(state: &Arc<ServerState>, route: &RouteResult) -> Option<Duration> {
    let t = state.config().server.server.execute_timeout;
    let configured = if t > 0 {
        Some(Duration::from_millis(t))
    } else {
        None
    };
    route
        .hint
        .as_ref()
        .and_then(|h| h.execute_timeout_ms)
        .map(Duration::from_millis)
        .or(configured)
}

/// 执行一条已路由语句。
pub async fn execute_route(
    state: &Arc<ServerState>,
    ctx: &mut SessionContext,
    tx: &mut TransactionSession,
    route: &RouteResult,
) -> Result<ExecResponse, ExecError> {
    if route.tasks.is_empty() {
        return Ok(ExecResponse::Empty);
    }
    let exec_timeout = {
        let cfg = state.config();
        let t = cfg.server.server.execute_timeout;
        if t > 0 {
            Some(Duration::from_millis(t))
        } else {
            None
        }
    };
    // hint 级超时覆盖（/*!mycat:executeTimeout=n*/，对应 Java EXECUTE_TIMEOUT）
    let exec_timeout = route
        .hint
        .as_ref()
        .and_then(|h| h.execute_timeout_ms)
        .map(Duration::from_millis)
        .or(exec_timeout);

    // 1. 解析每个 task 的物理数据源 + 后端方言翻译。
    //    master = 写操作 || 事务中 || autocommit=0（ReceiverImpl.resolveDatasourceTargetName 语义）
    let master_needed = route.stmt_type.is_write() || ctx.in_transaction || !ctx.autocommit;
    let (hint_master, hint_slave) = route
        .hint
        .as_ref()
        .map(|h| (h.master, h.slave))
        .unwrap_or((false, false));
    let mut resolved: Vec<(String, String)> = Vec::with_capacity(route.tasks.len());
    for task in &route.tasks {
        // 会话粘性：同一 target 复用上次解析的数据源（DataSourceNearnessImpl）
        let sticky = if !master_needed {
            tx.nearness_get(&task.target_name).map(String::from)
        } else {
            None
        };
        let ds = match sticky {
            Some(ds)
                if state
                    .replica()
                    .instance(&ds)
                    .map(|i| i.is_alive())
                    .unwrap_or(false) =>
            {
                ds
            }
            _ => {
                let ds = state
                    .replica()
                    .resolve(&task.target_name, master_needed, hint_master, hint_slave)
                    .ok_or_else(|| ExecError::NoDatasource(task.target_name.clone()))?;
                tx.nearness_put(&task.target_name, &ds);
                ds
            }
        };
        // 方言翻译：MySQL 直传；Oracle → translate_oracle_sql
        let sql = translate_for_backend(state, &ds, &task.sql)
            .map_err(|e| ExecError::Conn(e.to_string()))?;
        resolved.push((ds, sql));
    }

    // 2. 单任务读：透传（MySQL 原始载荷 / Oracle 合成载荷）
    let is_read = !route.stmt_type.is_write();
    if resolved.len() == 1 && is_read && route.merge.is_none() {
        let (ds, sql) = &resolved[0];
        let rs = run_query(state, ctx, tx, ds, sql, exec_timeout).await?;
        return Ok(ExecResponse::Rows(rs));
    }

    // 3. 写操作：顺序执行，affectedRows 求和、lastInsertId 取最大
    if !is_read {
        let mut affected = 0u64;
        let mut insert_id = 0u64;
        let need_tx = tx.explicit_tx || !ctx.autocommit;
        for (ds, sql) in &resolved {
            let (a, i) = run_update(state, ctx, tx, ds, sql, need_tx, exec_timeout).await?;
            affected = affected.saturating_add(a);
            insert_id = insert_id.max(i);
        }
        return Ok(ExecResponse::Ok {
            affected_rows: affected,
            last_insert_id: insert_id,
        });
    }

    // 4. 多任务读：事务中串行（共享固定连接）；非事务按数据源分组并行
    let need_tx = tx.explicit_tx || !ctx.autocommit;
    let sets = if need_tx {
        let mut sets = Vec::with_capacity(resolved.len());
        for (ds, sql) in &resolved {
            sets.push(run_query(state, ctx, tx, ds, sql, exec_timeout).await?);
        }
        sets
    } else {
        // 按数据源分组：组内共用一个连接串行，组间并行
        let mut groups: Vec<(String, Vec<String>)> = Vec::new();
        for (ds, sql) in resolved {
            if let Some(g) = groups.iter_mut().find(|(d, _)| *d == ds) {
                g.1.push(sql);
            } else {
                groups.push((ds, vec![sql]));
            }
        }
        let mut handles = Vec::with_capacity(groups.len());
        for (ds, sqls) in groups {
            let state = state.clone();
            handles.push(tokio::spawn(async move {
                let pool = BackendPool::get(&state, &ds)?;
                let _guard = ActiveGuard::new(state.replica().instance(&ds));
                let mut session = pool.acquire().await?;
                let mut out = Vec::with_capacity(sqls.len());
                for sql in &sqls {
                    out.push(exec_query(&mut session, sql, exec_timeout).await?);
                }
                Ok::<Vec<ResultSet>, ExecError>(out)
            }));
        }
        let mut sets = Vec::new();
        for h in handles {
            match h.await {
                Ok(Ok(mut v)) => sets.append(&mut v),
                Ok(Err(e)) => return Err(e),
                Err(join_err) => {
                    return Err(ExecError::Conn(format!("task join error: {}", join_err)))
                }
            }
        }
        sets
    };

    let merged = merge_result_sets(sets, route.merge.as_ref())?;
    Ok(ExecResponse::Rows(merged))
}

// ---------------------------------------------------------------------------
// 纯代理模式执行：会话固定单连接（USE/SET/事务/临时表语义原样生效）
// ---------------------------------------------------------------------------

/// 执行一条代理语句（单任务 route）。首条语句前按会话 schema 发 `USE`；
/// 连接在会话内固定，事务命令直接下发后端并同步本地事务标志。
pub async fn execute_route_proxy(
    state: &Arc<ServerState>,
    ctx: &mut SessionContext,
    tx: &mut TransactionSession,
    route: &RouteResult,
    timeout: Option<Duration>,
) -> Result<ExecResponse, ExecError> {
    let task = route
        .tasks
        .first()
        .ok_or_else(|| ExecError::Other("proxy route requires a task".into()))?;
    let ds = state
        .replica()
        .resolve(&task.target_name, true, false, false)
        .ok_or_else(|| ExecError::NoDatasource(task.target_name.clone()))?;
    // 首次语句：建立并固定连接（按会话默认库发 USE；-D 库名由此生效）
    if tx.proxy_conn_mut().is_none() {
        let pool = BackendPool::get(state, &ds)?;
        let mut session = pool.acquire().await?;
        if let Some(schema) = &ctx.schema {
            if !schema.is_empty() {
                let use_sql = format!("USE `{}`", schema);
                let _ = session.query(&use_sql, timeout).await;
            }
        }
        tx.proxy_pin(ds.clone(), session);
    }
    let sql = task.sql.as_str();
    let session = tx
        .proxy_conn_mut()
        .ok_or_else(|| ExecError::Conn("proxy connection missing".into()))?;
    let out = session.query(sql, timeout).await;
    // 事务状态跟踪（语义与后端一致；缓存命中判定/收尾用）
    let head = sql.trim_start();
    let upper = head.get(..16).unwrap_or(head).to_ascii_uppercase();
    match out {
        Ok(BackendOutcome::Rows(rs)) => Ok(ExecResponse::Rows(rs)),
        Ok(BackendOutcome::Ok {
            affected_rows,
            last_insert_id,
        }) => {
            if upper.starts_with("BEGIN") || upper.starts_with("START TRANSACTION") {
                if let Some(s) = tx.proxy_conn_mut() {
                    s.set_in_transaction(true);
                }
                ctx.in_transaction = true;
            } else if upper.starts_with("COMMIT") || upper.starts_with("ROLLBACK") {
                if let Some(s) = tx.proxy_conn_mut() {
                    s.set_in_transaction(false);
                }
                ctx.in_transaction = false;
            }
            Ok(ExecResponse::Ok {
                affected_rows,
                last_insert_id,
            })
        }
        Err(e) => {
            // 连接级错误：丢弃固定连接（下次语句重建）
            if matches!(e, BackendError::Conn(_)) {
                tx.proxy_take();
            }
            Err(e.into())
        }
    }
}

// ---------------------------------------------------------------------------
// 查询/更新执行（含事务固定语义）
// ---------------------------------------------------------------------------

/// 执行查询并返回结果集；OK 响应转为空结果集。
async fn run_query(
    state: &Arc<ServerState>,
    ctx: &mut SessionContext,
    tx: &mut TransactionSession,
    datasource: &str,
    sql: &str,
    timeout: Option<Duration>,
) -> Result<ResultSet, ExecError> {
    let need_tx = tx.explicit_tx || !ctx.autocommit;
    let xa_mode = ctx.transaction_policy.eq_ignore_ascii_case("xa");
    if tx.contains(datasource) {
        if need_tx && !tx.get_mut(datasource).unwrap().in_transaction() {
            tx.ensure_pinned_started(datasource, xa_mode).await?;
        }
        let session = tx.get_mut(datasource).unwrap();
        let r = exec_query(session, sql, timeout).await;
        // 仅连接级错误剔除固定连接（重建）；SQL 级错误连接可复用、事务继续
        if matches!(r, Err(ExecError::Conn(_))) {
            tx.unpin(datasource);
        }
        return r;
    }
    let pool = BackendPool::get(state, datasource)?;
    let _guard = ActiveGuard::new(state.replica().instance(datasource));
    let mut session = pool.acquire().await?;
    let result = async {
        if need_tx && !session.in_transaction() {
            tx.ensure_new_session(datasource, &mut session, xa_mode)
                .await?;
        }
        exec_query(&mut session, sql, timeout).await
    }
    .await;
    let rs = result?;
    if need_tx {
        // 事务中：固定连接（BackendSession 仍持有池 permit）
        tx.pin(datasource, session);
    }
    Ok(rs)
}

/// 执行更新，返回 (affectedRows, lastInsertId)。
async fn run_update(
    state: &Arc<ServerState>,
    ctx: &mut SessionContext,
    tx: &mut TransactionSession,
    datasource: &str,
    sql: &str,
    need_tx: bool,
    timeout: Option<Duration>,
) -> Result<(u64, u64), ExecError> {
    let xa_mode = ctx.transaction_policy.eq_ignore_ascii_case("xa");
    if tx.contains(datasource) {
        if need_tx && !tx.get_mut(datasource).unwrap().in_transaction() {
            tx.ensure_pinned_started(datasource, xa_mode).await?;
        }
        let session = tx.get_mut(datasource).unwrap();
        let r = exec_update(session, sql, timeout).await;
        // 仅连接级错误剔除固定连接；SQL 级错误（如唯一键冲突）事务继续
        if matches!(r, Err(ExecError::Conn(_))) {
            tx.unpin(datasource);
        }
        return r;
    }
    let pool = BackendPool::get(state, datasource)?;
    let _guard = ActiveGuard::new(state.replica().instance(datasource));
    let mut session = pool.acquire().await?;
    let r = async {
        if need_tx && !session.in_transaction() {
            tx.ensure_new_session(datasource, &mut session, xa_mode)
                .await?;
        }
        exec_update(&mut session, sql, timeout).await
    }
    .await;
    let out = r?;
    if need_tx {
        tx.pin(datasource, session);
    }
    Ok(out)
}

/// 查询执行：Rows → 结果集；Ok → 空结果集。
async fn exec_query(
    session: &mut BackendSession,
    sql: &str,
    timeout: Option<Duration>,
) -> Result<ResultSet, ExecError> {
    match session.query(sql, timeout).await? {
        BackendOutcome::Rows(rs) => Ok(rs),
        BackendOutcome::Ok { .. } => Ok(ResultSet {
            column_count: 0,
            column_def_payloads: vec![],
            columns: vec![],
            row_payloads: vec![],
            final_status: 0,
            warnings: 0,
        }),
    }
}

/// 更新执行。
async fn exec_update(
    session: &mut BackendSession,
    sql: &str,
    timeout: Option<Duration>,
) -> Result<(u64, u64), ExecError> {
    match session.query(sql, timeout).await? {
        BackendOutcome::Ok {
            affected_rows,
            last_insert_id,
        } => Ok((affected_rows, last_insert_id)),
        BackendOutcome::Rows(_) => Ok((0, 0)),
    }
}

pub fn merge_result_sets(
    sets: Vec<ResultSet>,
    plan: Option<&MergePlan>,
) -> Result<ResultSet, ExecError> {
    let mut sets = sets;
    if sets.is_empty() {
        return Err(ExecError::Other("no result sets to merge".into()));
    }
    if sets.len() == 1 {
        return Ok(sets.pop().unwrap());
    }
    let base = &sets[0];
    match plan.map(|p| &p.op) {
        None | Some(MergeOp::Concat) => merge_concat(sets, plan),
        Some(MergeOp::CountSum) => merge_count(base, &sets),
        Some(MergeOp::NumericSum) => merge_sum(base, &sets),
        Some(MergeOp::Max) => merge_extreme(base, &sets, true),
        Some(MergeOp::Min) => merge_extreme(base, &sets, false),
    }
}

fn merge_concat(
    mut sets: Vec<ResultSet>,
    plan: Option<&MergePlan>,
) -> Result<ResultSet, ExecError> {
    let base = sets.remove(0);
    let ncols = base.column_count;
    let order_by = plan.map(|p| p.order_by.clone()).unwrap_or_default();
    let limit = plan.and_then(|p| p.limit.clone());

    // 无 ORDER BY：直接拼接/截断原始行载荷，零解码零重编码（memcpy 级）
    if order_by.is_empty() {
        let mut payloads = base.row_payloads;
        for rs in sets {
            payloads.extend(rs.row_payloads);
        }
        if let Some(l) = &limit {
            let start = (l.offset as usize).min(payloads.len());
            let end = (start + l.count as usize).min(payloads.len());
            payloads.drain(end..);
            payloads.drain(..start);
        }
        return Ok(ResultSet {
            column_count: ncols,
            column_def_payloads: base.column_def_payloads,
            columns: base.columns,
            row_payloads: payloads,
            final_status: base.final_status,
            warnings: 0,
        });
    }

    // 有 ORDER BY：解码全部行，排序键一次性计算（避免每次比较重复 UTF-8 校验/f64 解析）
    let mut rows: Vec<Vec<Option<Vec<u8>>>> = base.decode_rows();
    for rs in sets {
        rows.extend(rs.decode_rows());
    }
    // 解析排序键下标：index 直用；name 按首个结果集列定义解析（select * 场景）
    let mut key_idx: Vec<(usize, bool)> = Vec::with_capacity(order_by.len());
    for k in &order_by {
        let idx = if let Some(i) = k.index {
            i
        } else if let Some(name) = &k.name {
            base.columns
                .iter()
                .position(|c| c.name.eq_ignore_ascii_case(name))
                .ok_or_else(|| {
                    ExecError::Other(format!(
                        "ORDER BY column '{}' not found in merged result",
                        name
                    ))
                })?
        } else {
            return Err(ExecError::Other(
                "ORDER BY key without index or name".into(),
            ));
        };
        if idx >= ncols {
            return Err(ExecError::Other(format!(
                "ORDER BY column index {} out of range ({} columns)",
                idx, ncols
            )));
        }
        key_idx.push((idx, k.desc));
    }
    // 每列排序策略：数值类型列（且全列可解析）→ 数值比较；否则字节比较。
    // 策略按列统一，保证比较器满足全序（混合策略会导致非传递性 → 排序错误/panic）。
    let numeric_cols: Vec<bool> = key_idx
        .iter()
        .map(|(idx, _)| {
            base.columns
                .get(*idx)
                .map(|c| is_numeric_field_type(c.field_type))
                .unwrap_or(false)
                && rows.iter().all(|r| {
                    r.get(*idx)
                        .and_then(|v| v.as_deref())
                        .map(|v| parse_numeric(v).is_some())
                        .unwrap_or(true) // NULL 不参与数值判定
                })
        })
        .collect();
    let mut indexed: Vec<(Vec<SortKey>, usize, Vec<Option<Vec<u8>>>)> =
        Vec::with_capacity(rows.len());
    for (i, row) in rows.into_iter().enumerate() {
        let keys = key_idx
            .iter()
            .enumerate()
            .map(|(j, (idx, _))| {
                let v = row.get(*idx).and_then(|c| c.as_deref());
                match v {
                    None => SortKey::Null,
                    Some(raw) => {
                        if numeric_cols[j] {
                            SortKey::Num(parse_numeric(raw).unwrap_or(f64::NAN))
                        } else {
                            SortKey::Bytes(raw.to_vec())
                        }
                    }
                }
            })
            .collect();
        indexed.push((keys, i, row));
    }
    indexed.sort_by(|(ka, ia, _), (kb, ib, _)| {
        for (j, (_, desc)) in key_idx.iter().enumerate() {
            let ord = compare_sort_keys(&ka[j], &kb[j]);
            let ord = if *desc { ord.reverse() } else { ord };
            if ord != std::cmp::Ordering::Equal {
                return ord;
            }
        }
        ia.cmp(ib) // 稳定性
    });
    let mut rows: Vec<Vec<Option<Vec<u8>>>> = indexed.into_iter().map(|(_, _, r)| r).collect();
    if let Some(l) = &limit {
        let start = (l.offset as usize).min(rows.len());
        let end = (start + l.count as usize).min(rows.len());
        rows = rows[start..end].to_vec();
    }
    let row_payloads: Vec<Vec<u8>> = rows
        .iter()
        .map(|r| build_text_row(&r.iter().map(|c| c.as_deref()).collect::<Vec<_>>()))
        .collect();
    Ok(ResultSet {
        column_count: ncols,
        column_def_payloads: base.column_def_payloads,
        columns: base.columns,
        row_payloads,
        final_status: base.final_status,
        warnings: 0,
    })
}

/// 排序键（每列策略统一，保证全序）：
/// - 数值列：`Num`（f64，NaN 由 `total_cmp` 处理）
/// - 其它列：`Bytes`（字节序，与 MySQL 排序规则的二进制形式一致）
enum SortKey {
    Null,
    Num(f64),
    Bytes(Vec<u8>),
}

fn compare_sort_keys(a: &SortKey, b: &SortKey) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    match (a, b) {
        (SortKey::Null, SortKey::Null) => Ordering::Equal,
        (SortKey::Null, _) => Ordering::Less,
        (_, SortKey::Null) => Ordering::Greater,
        (SortKey::Num(x), SortKey::Num(y)) => x.total_cmp(y),
        (SortKey::Bytes(x), SortKey::Bytes(y)) => x.cmp(y),
        // 列内策略统一（numeric_cols 预扫描），混型不可达
        _ => {
            debug_assert!(false, "sort key strategy must be homogeneous per column");
            Ordering::Equal
        }
    }
}

/// MySQL 数值字段类型（DECIMAL/整数/浮点/YEAR/BIT）。
fn is_numeric_field_type(t: u8) -> bool {
    use mycat3_protocol::consts::field_type;
    matches!(
        t,
        field_type::DECIMAL
            | field_type::TINY
            | field_type::SHORT
            | field_type::LONG
            | field_type::FLOAT
            | field_type::DOUBLE
            | field_type::LONGLONG
            | field_type::INT24
            | field_type::YEAR
            | field_type::NEWDECIMAL
            | field_type::BIT
    )
}

/// 按列类型比较两个值（merge_extreme 用）：数值列数值比较，否则字节比较。
fn compare_typed(a: &[u8], b: &[u8], numeric: bool) -> std::cmp::Ordering {
    if numeric {
        match (parse_numeric(a), parse_numeric(b)) {
            (Some(x), Some(y)) => x.total_cmp(&y),
            _ => a.cmp(b),
        }
    } else {
        a.cmp(b)
    }
}

fn parse_numeric(v: &[u8]) -> Option<f64> {
    std::str::from_utf8(v)
        .ok()
        .and_then(|s| s.trim().parse::<f64>().ok())
}

/// 只解码第一行第一个值（聚合归并：COUNT/SUM/MAX/MIN 每分片仅需一个值，
/// 不必为取首值而解码整个结果集）。
fn first_value(rs: &ResultSet) -> Option<Option<Vec<u8>>> {
    let payload = rs.row_payloads.first()?;
    decode_text_row(payload, rs.column_count).into_iter().next()
}

fn merge_count(base: &ResultSet, sets: &[ResultSet]) -> Result<ResultSet, ExecError> {
    let mut total: u64 = 0;
    for rs in sets {
        if let Some(Some(v)) = first_value(rs) {
            total = total.saturating_add(
                String::from_utf8_lossy(&v)
                    .trim()
                    .parse::<u64>()
                    .unwrap_or(0),
            );
        }
    }
    Ok(single_row_result(base, total.to_string().as_bytes()))
}

fn merge_sum(base: &ResultSet, sets: &[ResultSet]) -> Result<ResultSet, ExecError> {
    let mut sum = 0f64;
    let mut all_int = true;
    let mut any = false;
    for rs in sets {
        if let Some(Some(v)) = first_value(rs) {
            let s = String::from_utf8_lossy(&v);
            let t = s.trim();
            if t.is_empty() || t.eq_ignore_ascii_case("null") {
                continue;
            }
            any = true;
            if t.parse::<i64>().is_err() {
                all_int = false;
            }
            sum += t.parse::<f64>().unwrap_or(0.0);
        }
    }
    if !any {
        return Ok(single_row_result(base, b"NULL"));
    }
    let text = if all_int && sum.fract() == 0.0 {
        format!("{}", sum as i64)
    } else {
        // 去掉多余的尾零
        let s = format!("{:.6}", sum);
        s.trim_end_matches('0').trim_end_matches('.').to_string()
    };
    Ok(single_row_result(base, text.as_bytes()))
}

fn merge_extreme(base: &ResultSet, sets: &[ResultSet], max: bool) -> Result<ResultSet, ExecError> {
    // 数值列按数值语义取极值（与 MySQL MIN/MAX 一致），其余字节序
    let numeric = base
        .columns
        .first()
        .map(|c| is_numeric_field_type(c.field_type))
        .unwrap_or(false);
    let mut best: Option<Vec<u8>> = None;
    for rs in sets {
        if let Some(Some(v)) = first_value(rs) {
            let dominated = match &best {
                None => true,
                Some(b) => {
                    let ord = compare_typed(v.as_slice(), b.as_slice(), numeric);
                    if max {
                        ord == std::cmp::Ordering::Greater
                    } else {
                        ord == std::cmp::Ordering::Less
                    }
                }
            };
            if dominated {
                best = Some(v);
            }
        }
    }
    match best {
        Some(v) => Ok(single_row_result(base, &v)),
        None => Ok(single_row_result(base, b"NULL")),
    }
}

/// 用单行单值构造结果集（复用 base 的列定义）。
fn single_row_result(base: &ResultSet, value: &[u8]) -> ResultSet {
    let is_null = value == b"NULL";
    let row = if is_null {
        build_text_row(&[None])
    } else {
        build_text_row(&[Some(value)])
    };
    ResultSet {
        column_count: 1,
        column_def_payloads: base.column_def_payloads.iter().take(1).cloned().collect(),
        columns: base.columns.iter().take(1).cloned().collect(),
        row_payloads: vec![row],
        final_status: base.final_status,
        warnings: 0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mycat3_protocol::consts::field_type;
    use mycat3_protocol::packets::ColumnDef;
    use mycat3_sql::{LimitSpec, MergeOp, MergePlan, OrderKey};

    /// 由字符串行构造 ResultSet（测试辅助）
    fn rs_of(cols: &[&str], rows: &[Vec<Option<&str>>]) -> ResultSet {
        rs_of_typed(
            &cols
                .iter()
                .map(|c| (*c, field_type::VAR_STRING))
                .collect::<Vec<_>>(),
            rows,
        )
    }

    /// 带列类型构造 ResultSet（验证数值/字符串排序语义差异）
    fn rs_of_typed(cols: &[(&str, u8)], rows: &[Vec<Option<&str>>]) -> ResultSet {
        let columns: Vec<ColumnDef> = cols.iter().map(|(c, t)| ColumnDef::new(c, *t)).collect();
        let column_def_payloads = columns.iter().map(|c| c.encode()).collect();
        let row_payloads = rows
            .iter()
            .map(|r| {
                build_text_row(
                    &r.iter()
                        .map(|v| v.map(|s| s.as_bytes()))
                        .collect::<Vec<_>>(),
                )
            })
            .collect();
        ResultSet {
            column_count: cols.len(),
            column_def_payloads,
            columns,
            row_payloads,
            final_status: 2,
            warnings: 0,
        }
    }

    #[test]
    fn test_merge_concat_plain() {
        let a = rs_of(&["id"], &[vec![Some("1")], vec![Some("3")]]);
        let b = rs_of(&["id"], &[vec![Some("2")]]);
        let merged = merge_result_sets(vec![a, b], None).unwrap();
        let rows = merged.decode_rows();
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn test_merge_concat_sorted_with_limit() {
        let a = rs_of(
            &["id", "name"],
            &[vec![Some("3"), Some("c")], vec![Some("1"), Some("a")]],
        );
        let b = rs_of(&["id", "name"], &[vec![Some("2"), Some("b")]]);
        let plan = MergePlan {
            op: MergeOp::Concat,
            order_by: vec![OrderKey {
                index: Some(0),
                name: None,
                desc: false,
            }],
            limit: Some(LimitSpec {
                offset: 1,
                count: 2,
            }),
        };
        let merged = merge_result_sets(vec![a, b], Some(&plan)).unwrap();
        let rows = merged.decode_rows();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0][0].as_deref(), Some(&b"2"[..]));
        assert_eq!(rows[1][0].as_deref(), Some(&b"3"[..]));
    }

    #[test]
    fn test_merge_concat_desc() {
        // 数值列（LONGLONG）：数值语义排序 100 > 10 > 9
        let a = rs_of_typed(
            &[("v", field_type::LONGLONG)],
            &[vec![Some("10")], vec![Some("9")]],
        );
        let b = rs_of_typed(&[("v", field_type::LONGLONG)], &[vec![Some("100")]]);
        let plan = MergePlan {
            op: MergeOp::Concat,
            order_by: vec![OrderKey {
                index: Some(0),
                name: None,
                desc: true,
            }],
            limit: None,
        };
        let merged = merge_result_sets(vec![a, b], Some(&plan)).unwrap();
        let rows = merged.decode_rows();
        assert_eq!(rows[0][0].as_deref(), Some(&b"100"[..]));
        assert_eq!(rows[1][0].as_deref(), Some(&b"10"[..]));
        assert_eq!(rows[2][0].as_deref(), Some(&b"9"[..]));

        // 字符串列（VAR_STRING）：字节序排序（与 MySQL VARCHAR 排序一致），不做数值强转
        let a = rs_of(&["v"], &[vec![Some("10")], vec![Some("9")]]);
        let b = rs_of(&["v"], &[vec![Some("100")]]);
        let merged = merge_result_sets(vec![a, b], Some(&plan)).unwrap();
        let rows = merged.decode_rows();
        assert_eq!(rows[0][0].as_deref(), Some(&b"9"[..])); // '9' > '1'
        assert_eq!(rows[1][0].as_deref(), Some(&b"100"[..]));
        assert_eq!(rows[2][0].as_deref(), Some(&b"10"[..]));
    }

    /// 混合可解析/不可解析值必须保持全序（旧实现按"两侧都能解析"切换策略，
    /// 会产生 a<b<c<a 的非传递性循环 → debug panic / release 乱序）。
    #[test]
    fn test_merge_sort_total_order_with_mixed_values() {
        let vals: Vec<Vec<Option<&str>>> = ["10", "2", "1x", "9", "abc"]
            .iter()
            .map(|v| vec![Some(*v)])
            .collect();
        let rs = rs_of(&["v"], &vals);
        let plan = MergePlan {
            op: MergeOp::Concat,
            order_by: vec![OrderKey {
                index: Some(0),
                name: None,
                desc: false,
            }],
            limit: None,
        };
        // 不 panic 即证明比较器全序（列策略统一为字节序）
        let merged = merge_result_sets(vec![rs.clone(), rs], Some(&plan)).unwrap();
        let rows = merged.decode_rows();
        assert_eq!(rows.len(), 10);
        // 字节序：'1' 开头的 "10" < "1x"；"2" < "9" < "abc"
        let firsts: Vec<String> = rows
            .iter()
            .map(|r| String::from_utf8_lossy(r[0].as_deref().unwrap()).into_owned())
            .collect();
        let mut sorted = firsts.clone();
        sorted.sort();
        assert_eq!(firsts, sorted, "结果必须与字节序全排序一致");
    }

    #[test]
    fn test_merge_count_sum() {
        let a = rs_of(&["c"], &[vec![Some("3")]]);
        let b = rs_of(&["c"], &[vec![Some("4")]]);
        let plan = MergePlan {
            op: MergeOp::CountSum,
            order_by: vec![],
            limit: None,
        };
        let merged = merge_result_sets(vec![a, b], Some(&plan)).unwrap();
        let rows = merged.decode_rows();
        assert_eq!(rows[0][0].as_deref(), Some(&b"7"[..]));
    }

    #[test]
    fn test_merge_numeric_sum() {
        let a = rs_of(&["s"], &[vec![Some("1.5")]]);
        let b = rs_of(&["s"], &[vec![Some("2.5")]]);
        let plan = MergePlan {
            op: MergeOp::NumericSum,
            order_by: vec![],
            limit: None,
        };
        let merged = merge_result_sets(vec![a, b], Some(&plan)).unwrap();
        assert_eq!(merged.decode_rows()[0][0].as_deref(), Some(&b"4"[..]));
    }

    #[test]
    fn test_merge_max_min() {
        // 数值列（LONGLONG）：数值语义取极值
        let a = rs_of_typed(&[("m", field_type::LONGLONG)], &[vec![Some("5")]]);
        let b = rs_of_typed(&[("m", field_type::LONGLONG)], &[vec![Some("12")]]);
        let c = rs_of_typed(&[("m", field_type::LONGLONG)], &[vec![None]]);
        let maxp = MergePlan {
            op: MergeOp::Max,
            order_by: vec![],
            limit: None,
        };
        let merged = merge_result_sets(vec![a.clone(), b.clone(), c.clone()], Some(&maxp)).unwrap();
        assert_eq!(merged.decode_rows()[0][0].as_deref(), Some(&b"12"[..]));
        let minp = MergePlan {
            op: MergeOp::Min,
            order_by: vec![],
            limit: None,
        };
        let merged = merge_result_sets(vec![a, b, c], Some(&minp)).unwrap();
        assert_eq!(merged.decode_rows()[0][0].as_deref(), Some(&b"5"[..]));
        // 字符串列：字节序取极值（"12" < "5"）
        let s1 = rs_of(&["m"], &[vec![Some("5")]]);
        let s2 = rs_of(&["m"], &[vec![Some("12")]]);
        let merged = merge_result_sets(vec![s1, s2], Some(&maxp)).unwrap();
        assert_eq!(merged.decode_rows()[0][0].as_deref(), Some(&b"5"[..]));
    }

    #[test]
    fn test_sort_keys_null_and_numeric() {
        // NULL 最小
        assert_eq!(
            compare_sort_keys(&SortKey::Null, &SortKey::Num(1.0)),
            std::cmp::Ordering::Less
        );
        assert_eq!(
            compare_sort_keys(&SortKey::Num(10.0), &SortKey::Num(9.0)),
            std::cmp::Ordering::Greater
        );
        assert_eq!(
            compare_sort_keys(
                &SortKey::Bytes(b"abc".to_vec()),
                &SortKey::Bytes(b"abd".to_vec())
            ),
            std::cmp::Ordering::Less
        );
        // NaN 由 total_cmp 定义（不影响全序）
        assert_eq!(
            compare_sort_keys(&SortKey::Num(f64::NAN), &SortKey::Num(1.0)),
            std::cmp::Ordering::Greater
        );
        // compare_typed：数值列数值比较 / 非数值列字节比较
        assert_eq!(
            compare_typed(b"10", b"9", true),
            std::cmp::Ordering::Greater
        );
        assert_eq!(compare_typed(b"10", b"9", false), std::cmp::Ordering::Less);
    }

    #[test]
    fn test_single_set_passthrough() {
        let a = rs_of(&["id"], &[vec![Some("1")]]);
        let merged = merge_result_sets(vec![a.clone()], None).unwrap();
        assert_eq!(merged.row_payloads.len(), 1);
        assert_eq!(merged.columns[0].name, "id");
        assert_eq!(merged.columns[0].field_type, field_type::VAR_STRING);
        let _ = ColumnDef::new("x", field_type::LONG);
    }
}
