//! SQL 结果缓存引擎（对应 Java `SqlResultSetService`，Pro 语义增强）：
//!
//! **模板语义**（见 `mycat3_sql::cache_key`）：
//! - 缓存模板 SQL 中的 `?` 是**自由参数槽位**：形状匹配的查询都进缓存，但
//!   **每个参数值组合有自己独立的缓存结果**（绝不共享）；
//! - 模板含 `?` 时参数不可预测，**无法预读**——按需回填 + 变体级 TTL 惰性刷新；
//! - 模板无 `?`（具体 SQL）：注册后立即预执行 + 周期刷新（先成功后原子换入，
//!   失败保留旧值），固定字面量逐位匹配（不同字面量不命中）；
//! - `matchMode=EXACT`：SQL 文本精确匹配（Java 语义，单结果可预读）。
//!
//! 其它设计：
//! - 冷缓存每键（预读/每变体）共享一次在途执行，等待者零重复执行；
//! - 结果持久化本地二进制文件：具体模板 `<name>.bin`，变体 `<name>_<hash>.bin`
//!   （文件头含变体键，mmap 恢复，损坏忽略，drop 时清理）；
//! - 热路径：无缓存项零解析短路；原始文本 O(1) 快路径（仅可预读条目）；
//!   命中仅 Arc 克隆，三前端复用同一 EngineOutcome。

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;

use mycat3_config::SqlCacheConfig;
use mycat3_protocol::consts::field_type;
use parking_lot::{Mutex, RwLock};
use tokio::sync::Notify;

use crate::engine::{EngineError, EngineOutcome};
use crate::state::ServerState;

pub type BoxFuture<'a, T> = std::pin::Pin<Box<dyn std::future::Future<Output = T> + Send + 'a>>;

/// 缓存执行器抽象：生产走 engine 完整链路（旁路缓存防递归），测试注入假执行器。
pub trait CacheExecutor: Send + Sync {
    fn execute<'a>(
        &'a self,
        sql: &'a str,
        schema: Option<&'a str>,
    ) -> BoxFuture<'a, Result<EngineOutcome, EngineError>>;
}

/// 生产执行器：独立 EngineSession 走完整 `execute_sql_impl`（bypass_cache=true）。
struct EngineCacheExecutor {
    state: Arc<ServerState>,
}

impl CacheExecutor for EngineCacheExecutor {
    fn execute<'a>(
        &'a self,
        sql: &'a str,
        schema: Option<&'a str>,
    ) -> BoxFuture<'a, Result<EngineOutcome, EngineError>> {
        Box::pin(async move {
            let mut ctx = crate::session::SessionContext::new(0, "sqlcache".into());
            ctx.schema = schema.map(|s| s.to_string());
            let mut sess = crate::engine::EngineSession::new(ctx);
            let out = crate::engine::execute_sql_impl(&self.state, &mut sess, sql, true).await?;
            out.into_iter()
                .next()
                .ok_or_else(|| EngineError::general(format!("缓存 SQL 未返回结果集: {}", sql)))
        })
    }
}

// ---------------------------------------------------------------------------
// 二进制缓存文件（mmap 读取；文件头含变体键）
// ---------------------------------------------------------------------------

const MAGIC: &[u8; 8] = b"M3SC\x01\x00\x00\x00";
/// 单条目变体上限（超出逐出最旧变体）。
const VARIANT_CAP: usize = 4096;

/// 结果是否在配置的大小限制内。
fn within_limits(o: &EngineOutcome, cfg: &SqlCacheConfig) -> bool {
    let EngineOutcome::Rows { rows, .. } = o else {
        return false;
    };
    if cfg.max_rows > 0 && rows.len() as u64 > cfg.max_rows {
        return false;
    }
    if cfg.max_bytes > 0 {
        let bytes: usize = rows
            .iter()
            .flat_map(|r| r.iter())
            .filter_map(|c| c.as_ref())
            .map(|s| s.len())
            .sum();
        if bytes as u64 > cfg.max_bytes {
            return false;
        }
    }
    true
}

/// 流式写二进制缓存文件（temp + rename 原子；文件头含变体键）。返回写入字节数。
fn write_file_sync(
    path: &PathBuf,
    variant_key: &str,
    outcome: &EngineOutcome,
) -> std::io::Result<u64> {
    use std::io::{BufWriter, Write};
    let EngineOutcome::Rows { columns, rows } = outcome else {
        return Ok(0);
    };
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir)?;
    }
    let tmp = path.with_extension("bin.tmp");
    let mut w = BufWriter::with_capacity(64 * 1024, std::fs::File::create(&tmp)?);
    w.write_all(MAGIC)?;
    let vk = variant_key.as_bytes();
    w.write_all(&(vk.len() as u16).to_le_bytes())?;
    w.write_all(vk)?;
    w.write_all(&(columns.len() as u32).to_le_bytes())?;
    for c in columns {
        let name = c.name.as_bytes();
        w.write_all(&(name.len() as u16).to_le_bytes())?;
        w.write_all(name)?;
        w.write_all(&[c.mysql_type])?;
        w.write_all(&c.charset.to_le_bytes())?;
    }
    w.write_all(&(rows.len() as u64).to_le_bytes())?;
    let mut scratch: Vec<u8> = Vec::with_capacity(256);
    for row in rows {
        scratch.clear();
        for cell in row {
            match cell {
                None => scratch.push(0),
                Some(s) => {
                    scratch.push(1);
                    scratch.extend_from_slice(&(s.len() as u32).to_le_bytes());
                    scratch.extend_from_slice(s.as_bytes());
                }
            }
        }
        w.write_all(&(scratch.len() as u32).to_le_bytes())?;
        w.write_all(&scratch)?;
    }
    w.flush()?;
    drop(w);
    std::fs::rename(&tmp, path)?;
    Ok(std::fs::metadata(path).map(|m| m.len()).unwrap_or(0))
}

/// 带边界检查的游标（损坏/截断文件安全失败）。
struct Cur<'a> {
    b: &'a [u8],
    pos: usize,
}

impl<'a> Cur<'a> {
    fn new(b: &'a [u8]) -> Self {
        Cur { b, pos: 0 }
    }
    fn u8(&mut self) -> Option<u8> {
        let v = *self.b.get(self.pos)?;
        self.pos += 1;
        Some(v)
    }
    fn u16(&mut self) -> Option<u16> {
        let s = self.b.get(self.pos..self.pos + 2)?;
        self.pos += 2;
        Some(u16::from_le_bytes([s[0], s[1]]))
    }
    fn u32(&mut self) -> Option<u32> {
        let s = self.b.get(self.pos..self.pos + 4)?;
        self.pos += 4;
        Some(u32::from_le_bytes([s[0], s[1], s[2], s[3]]))
    }
    fn u64(&mut self) -> Option<u64> {
        let s = self.b.get(self.pos..self.pos + 8)?;
        self.pos += 8;
        Some(u64::from_le_bytes([
            s[0], s[1], s[2], s[3], s[4], s[5], s[6], s[7],
        ]))
    }
    fn bytes(&mut self, n: usize) -> Option<&'a [u8]> {
        let s = self.b.get(self.pos..self.pos + n)?;
        self.pos += n;
        Some(s)
    }
}

/// mmap 读取缓存文件（零拷贝；mmap 不可用整载回退；损坏即 Err）。
/// 返回 (变体键, 结果)。
fn read_file_sync(path: &PathBuf) -> std::io::Result<(String, EngineOutcome)> {
    use std::io::Read;
    let file = std::fs::File::open(path)?;
    // mmap 零拷贝优先；不可用/失败整载回退
    if let Ok(mm) = unsafe { memmap2::MmapOptions::new().map(&file) } {
        return parse_cache_file(&mm[..]);
    }
    let mut f = file;
    let mut buf = Vec::new();
    f.read_to_end(&mut buf)?;
    parse_cache_file(&buf)
}

/// 解析缓存文件主体（magic + 变体键 + 结果）。
fn parse_cache_file(data: &[u8]) -> std::io::Result<(String, EngineOutcome)> {
    let corrupt = || std::io::Error::new(std::io::ErrorKind::InvalidData, "cache file corrupt");
    let mut cur = Cur::new(data);
    if cur.bytes(8).ok_or_else(corrupt)? != MAGIC {
        return Err(corrupt());
    }
    let klen = cur.u16().ok_or_else(corrupt)? as usize;
    let vkey = String::from_utf8_lossy(cur.bytes(klen).ok_or_else(corrupt)?).into_owned();
    let outcome = parse_outcome(&mut cur).ok_or_else(corrupt)?;
    Ok((vkey, outcome))
}

fn parse_outcome(cur: &mut Cur<'_>) -> Option<EngineOutcome> {
    let ncols = cur.u32()? as usize;
    let mut columns = Vec::with_capacity(ncols.min(4096));
    for _ in 0..ncols {
        let nlen = cur.u16()? as usize;
        let name = String::from_utf8_lossy(cur.bytes(nlen)?).into_owned();
        let mysql_type = cur.u8()?;
        let charset = cur.u16()?;
        columns.push(crate::engine::EngineColumn {
            name,
            mysql_type,
            charset,
        });
    }
    let nrows = cur.u64()? as usize;
    let mut rows = Vec::with_capacity(nrows.min(10_000_000));
    for _ in 0..nrows {
        let rlen = cur.u32()? as usize;
        let rb = cur.bytes(rlen)?;
        let mut rc = Cur::new(rb);
        let mut row = Vec::with_capacity(ncols);
        for _ in 0..ncols {
            match rc.u8()? {
                0 => row.push(None),
                _ => {
                    let len = rc.u32()? as usize;
                    let s = String::from_utf8_lossy(rc.bytes(len)?).into_owned();
                    row.push(Some(s));
                }
            }
        }
        rows.push(row);
    }
    Some(EngineOutcome::Rows { columns, rows })
}

// ---------------------------------------------------------------------------
// 缓存项与全局管理器
// ---------------------------------------------------------------------------

/// 变体缓存单元。
struct VariantCell {
    outcome: Arc<EngineOutcome>,
    updated: Instant,
}

struct CacheEntry {
    cfg: SqlCacheConfig,
    /// 形状键（见 cache_key）
    shape: String,
    /// 模板槽位（None = 自由参数）
    slots: Vec<Option<String>>,
    /// 模板是否可预读（无自由参数）
    pre_readable: bool,
    /// 预读/固定结果（无自由参数的模板）
    concrete: RwLock<Option<Arc<EngineOutcome>>>,
    /// 预读在途标记
    loading: AtomicBool,
    /// 变体缓存：变体键 → 结果
    variants: RwLock<HashMap<String, VariantCell>>,
    /// 变体在途执行（每变体一个 Notify 做请求合并）
    in_flight: Mutex<HashMap<String, Arc<Notify>>>,
    /// 等待通知（预读完成/停止）
    notify: Notify,
    stop: AtomicBool,
    hits: AtomicU64,
    misses: AtomicU64,
    /// 最近一次命中的会话 schema（后台刷新缺省库上下文用）
    last_schema: RwLock<Option<String>>,
    /// 具体结果文件路径（变体文件同目录派生）
    file_path: Option<PathBuf>,
}

impl CacheEntry {
    /// 变体 TTL（秒；0 = 不过期）。
    fn ttl_secs(&self) -> u64 {
        if self.cfg.refresh_interval == 0 {
            u64::MAX
        } else {
            self.cfg.refresh_interval
        }
    }

    /// 变体文件路径：<safe>_<hash>.bin
    fn variant_file(&self, vkey: &str) -> Option<PathBuf> {
        self.file_path.as_ref().map(|p| {
            let hash = variant_hash(vkey);
            let name = p
                .file_stem()
                .map(|s| s.to_string_lossy().into_owned())
                .unwrap_or_else(|| "entry".into());
            p.with_file_name(format!("{}_{:016x}.bin", name, hash))
        })
    }

    /// 等待并确保预读结果（无自由参数模板；请求合并）。
    async fn ensure_concrete(
        self: &Arc<Self>,
        exec: &Arc<dyn CacheExecutor>,
        schema: Option<&str>,
    ) -> Option<Arc<EngineOutcome>> {
        loop {
            if let Some(v) = self.concrete.read().clone() {
                self.hits.fetch_add(1, Ordering::Relaxed);
                return Some(v);
            }
            if self.try_start_loading() {
                let r = exec
                    .execute(&self.cfg.sql, schema.or(self.cfg.schema_name.as_deref()))
                    .await;
                match r {
                    Ok(outcome) if within_limits(&outcome, &self.cfg) => {
                        if let Some(p) = &self.file_path {
                            let outcome_clone = outcome.clone();
                            let p = p.clone();
                            let _ = tokio::task::spawn_blocking(move || {
                                let _ = write_file_sync(&p, "", &outcome_clone);
                            })
                            .await;
                        }
                        let v = Arc::new(outcome);
                        *self.concrete.write() = Some(v.clone());
                        self.finish_loading();
                        self.hits.fetch_add(1, Ordering::Relaxed);
                        return Some(v);
                    }
                    Ok(_) => {
                        tracing::warn!(name = %self.cfg.name, "缓存结果超限，不缓存");
                        self.finish_loading();
                        return None;
                    }
                    Err(e) => {
                        tracing::warn!(name = %self.cfg.name, error = %e, "缓存 SQL 执行失败，回落正常执行");
                        self.finish_loading();
                        return None;
                    }
                }
            }
            self.notify.notified().await;
            if self.stop.load(Ordering::SeqCst) {
                return None;
            }
        }
    }

    /// 取变体结果：命中且未过期直接返回；过期/未缓存 → 每变体合并执行
    /// **来查询原文**（结果必须对应当前参数值）。
    async fn ensure_variant(
        self: &Arc<Self>,
        exec: &Arc<dyn CacheExecutor>,
        query_sql: &str,
        vkey: String,
        schema: Option<&str>,
    ) -> Option<Arc<EngineOutcome>> {
        loop {
            {
                let vars = self.variants.read();
                if let Some(cell) = vars.get(&vkey) {
                    if cell.updated.elapsed().as_secs() < self.ttl_secs() {
                        self.hits.fetch_add(1, Ordering::Relaxed);
                        return Some(cell.outcome.clone());
                    }
                    // 过期：不删除旧值（刷新失败保留旧值），继续走加载路径
                }
            }
            // 每变体请求合并：插入者成为加载者，其余等待
            let (notify, is_loader) = {
                let mut inf = self.in_flight.lock();
                if let Some(n) = inf.get(&vkey) {
                    (n.clone(), false)
                } else {
                    let n = Arc::new(Notify::new());
                    inf.insert(vkey.clone(), n.clone());
                    (n, true)
                }
            };
            if is_loader {
                // 执行来查询原文（不是模板 SQL）：变体结果对应当前参数
                let r = exec
                    .execute(query_sql, schema.or(self.cfg.schema_name.as_deref()))
                    .await;
                match r {
                    Ok(outcome) if within_limits(&outcome, &self.cfg) => {
                        if let Some(p) = self.variant_file(&vkey) {
                            let outcome_clone = outcome.clone();
                            let vk = vkey.clone();
                            let _ = tokio::task::spawn_blocking(move || {
                                let _ = write_file_sync(&p, &vk, &outcome_clone);
                            })
                            .await;
                        }
                        let outcome = Arc::new(outcome);
                        self.variants.write().insert(
                            vkey.clone(),
                            VariantCell {
                                outcome: outcome.clone(),
                                updated: Instant::now(),
                            },
                        );
                        self.evict_if_needed();
                        self.in_flight.lock().remove(&vkey);
                        notify.notify_waiters();
                        return Some(outcome);
                    }
                    Ok(_) => {
                        tracing::warn!(name = %self.cfg.name, "变体结果超限，不缓存");
                        self.in_flight.lock().remove(&vkey);
                        notify.notify_waiters();
                        return None;
                    }
                    Err(e) => {
                        tracing::warn!(name = %self.cfg.name, error = %e, "变体执行失败，回落正常执行");
                        self.in_flight.lock().remove(&vkey);
                        notify.notify_waiters();
                        return None;
                    }
                }
            }
            notify.notified().await;
            if self.stop.load(Ordering::SeqCst) {
                return None;
            }
        }
    }

    /// 变体超上限：逐出最旧（更新时间最早）变体。
    fn evict_if_needed(&self) {
        let mut vars = self.variants.write();
        if vars.len() <= VARIANT_CAP {
            return;
        }
        let oldest = vars
            .iter()
            .min_by_key(|(_, c)| c.updated)
            .map(|(k, _)| k.clone());
        if let Some(k) = oldest {
            vars.remove(&k);
        }
    }

    fn try_start_loading(&self) -> bool {
        self.loading
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
    }

    fn finish_loading(&self) {
        self.loading.store(false, Ordering::SeqCst);
        self.notify.notify_waiters();
    }

    /// 后台周期刷新（仅可预读模板）：先成功后原子换入（失败保留旧值）。
    async fn refresh_once(self: &Arc<Self>, exec: &Arc<dyn CacheExecutor>) {
        let schema = self
            .last_schema
            .read()
            .clone()
            .or_else(|| self.cfg.schema_name.clone());
        match exec.execute(&self.cfg.sql, schema.as_deref()).await {
            Ok(outcome) if within_limits(&outcome, &self.cfg) => {
                if let Some(p) = &self.file_path {
                    let outcome_clone = outcome.clone();
                    let p = p.clone();
                    let _ = tokio::task::spawn_blocking(move || {
                        let _ = write_file_sync(&p, "", &outcome_clone);
                    })
                    .await;
                }
                *self.concrete.write() = Some(Arc::new(outcome));
                self.notify.notify_waiters();
            }
            Ok(_) => {
                tracing::warn!(name = %self.cfg.name, "刷新结果超限，保留旧值");
            }
            Err(e) => {
                tracing::warn!(name = %self.cfg.name, error = %e, "缓存刷新失败，保留旧值");
            }
        }
    }
}

/// 后台刷新循环（仅可预读模板：initialDelay 后立即执行，随后按 refreshInterval 周期）。
async fn run_refresh_loop(entry: Arc<CacheEntry>, exec: Arc<dyn CacheExecutor>) {
    use std::time::Duration;
    let initial = Duration::from_millis(entry.cfg.initial_delay.saturating_mul(1000));
    let interval = Duration::from_millis(entry.cfg.refresh_interval.saturating_mul(1000));
    if !initial.is_zero() {
        tokio::select! {
            _ = tokio::time::sleep(initial) => {}
            _ = entry.notify.notified() => {}
        }
        if entry.stop.load(Ordering::SeqCst) {
            return;
        }
    }
    entry.refresh_once(&exec).await;
    loop {
        tokio::select! {
            _ = tokio::time::sleep(interval) => {
                if entry.stop.load(Ordering::SeqCst) {
                    break;
                }
                entry.refresh_once(&exec).await;
            }
            _ = entry.notify.notified() => {
                if entry.stop.load(Ordering::SeqCst) {
                    break;
                }
            }
        }
    }
}

fn variant_hash(vkey: &str) -> u64 {
    // FNV-1a（键内容哈希，跨进程稳定；恢复时以文件头变体键为准，哈希仅命名用）
    let mut h: u64 = 0xcbf29ce484222325;
    for b in vkey.as_bytes() {
        h ^= *b as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    h
}

/// 词边界包含检查（表名 token 匹配；下划线/字母数字视为词内字符）。
fn contains_word(haystack: &str, word: &str) -> bool {
    if word.is_empty() {
        return false;
    }
    let hb = haystack.as_bytes();
    let wb = word.as_bytes();
    if hb.len() < wb.len() {
        return false;
    }
    let is_word_char = |b: u8| b.is_ascii_alphanumeric() || b == b'_';
    for i in 0..=hb.len() - wb.len() {
        if &hb[i..i + wb.len()] != wb {
            continue;
        }
        let before_ok = i == 0 || !is_word_char(hb[i - 1]);
        let after_ok = i + wb.len() == hb.len() || !is_word_char(hb[i + wb.len()]);
        if before_ok && after_ok {
            return true;
        }
    }
    false
}

fn trim_sql(sql: &str) -> String {
    sql.trim().trim_end_matches(';').trim().to_string()
}

/// 模板 vs 来查询匹配：形状一致 + 固定槽位逐位一致。
/// 返回变体键（自由参数处查询值 JSON 数组；无自由参数为 ""）。
fn match_template(
    template: &mycat3_sql::NormalizedQuery,
    query: &mycat3_sql::NormalizedQuery,
) -> Option<String> {
    if template.shape != query.shape || template.slots.len() != query.slots.len() {
        return None;
    }
    let mut free_vals: Vec<String> = Vec::new();
    for (t, q) in template.slots.iter().zip(query.slots.iter()) {
        match (t, q) {
            (None, Some(v)) => free_vals.push(v.clone()),
            (Some(fixed), Some(v)) if fixed == v => {}
            _ => return None,
        }
    }
    if free_vals.is_empty() {
        Some(String::new())
    } else {
        serde_json::to_string(&free_vals).ok()
    }
}

/// SQL 结果缓存管理器。
pub struct SqlCacheManager {
    /// name → entry
    entries: RwLock<HashMap<String, Arc<CacheEntry>>>,
    /// 形状键 → entry（PARAMETERIZED 与 EXACT 键空间天然分离：EXACT 形状含字面量）
    key_map: RwLock<HashMap<String, Arc<CacheEntry>>>,
    /// 原始 SQL 文本 → entry（O(1) 零解析快路径；仅可预读条目）
    raw_map: RwLock<HashMap<String, Arc<CacheEntry>>>,
    executor: Mutex<Option<Arc<dyn CacheExecutor>>>,
    /// 服务状态（惰性重同步 + home 来源）
    state: Mutex<Option<Arc<ServerState>>>,
    /// 配置 Arc 指针指纹（reload 后变化 → 惰性重同步）
    last_cfg_ptr: AtomicUsize,
}

impl SqlCacheManager {
    pub fn new() -> Self {
        SqlCacheManager {
            entries: RwLock::new(HashMap::new()),
            key_map: RwLock::new(HashMap::new()),
            raw_map: RwLock::new(HashMap::new()),
            executor: Mutex::new(None),
            state: Mutex::new(None),
            last_cfg_ptr: AtomicUsize::new(0),
        }
    }

    /// 挂接服务状态（生产执行器 + 配置来源）；幂等。
    pub fn attach(self: &Arc<Self>, state: Arc<ServerState>) {
        {
            let mut g = self.executor.lock();
            if g.is_none() {
                *g = Some(Arc::new(EngineCacheExecutor {
                    state: state.clone(),
                }));
            }
        }
        *self.state.lock() = Some(state.clone());
        self.resync(&state);
    }

    /// 测试注入：自定义执行器。
    pub fn set_executor(&self, exec: Arc<dyn CacheExecutor>) {
        *self.executor.lock() = Some(exec);
    }

    /// 与配置同步：新增项建任务（可预读模板含立即预热），移除项停止任务并清理文件。
    pub fn resync(&self, state: &Arc<ServerState>) {
        let cfg = state.config();
        let ptr = Arc::as_ptr(&cfg) as usize;
        if self.last_cfg_ptr.swap(ptr, Ordering::Relaxed) == ptr && !self.entries.read().is_empty()
        {
            return;
        }
        let exec = match self.executor.lock().clone() {
            Some(e) => e,
            None => return,
        };
        let mut entries = self.entries.write();
        let mut removed_files: Vec<PathBuf> = Vec::new();
        entries.retain(|name, entry| {
            let keep = cfg
                .sql_caches
                .get(name)
                .map(|c| {
                    c.sql == entry.cfg.sql
                        && c.refresh_interval == entry.cfg.refresh_interval
                        && c.match_mode == entry.cfg.match_mode
                        && c.schema_name == entry.cfg.schema_name
                })
                .unwrap_or(false);
            if !keep {
                entry.stop.store(true, Ordering::SeqCst);
                entry.notify.notify_waiters();
                // 清理 <safe>.bin 与 <safe>_*.bin
                if let Some(p) = entry.file_path.as_ref() {
                    if let Some(dir) = p.parent() {
                        let prefix = p
                            .file_stem()
                            .map(|s| s.to_string_lossy().into_owned())
                            .unwrap_or_default();
                        if let Ok(rd) = std::fs::read_dir(dir) {
                            for e in rd.flatten() {
                                let name = e.file_name().to_string_lossy().into_owned();
                                let own = name == format!("{}.bin", prefix)
                                    || (name.starts_with(&format!("{}_", prefix))
                                        && name.ends_with(".bin"));
                                if own {
                                    removed_files.push(e.path());
                                }
                            }
                        }
                    }
                }
            }
            keep
        });
        for p in removed_files {
            let _ = std::fs::remove_file(p);
        }
        let home = state.home();
        for (name, c) in &cfg.sql_caches {
            if entries.contains_key(name) {
                continue;
            }
            let exact = c.match_mode.eq_ignore_ascii_case("EXACT");
            let nq = match mycat3_sql::normalize_cache_query(&c.sql, exact) {
                Some(n) => n,
                None => {
                    tracing::warn!(name = %name, "缓存配置 SQL 非单条 SELECT，忽略");
                    continue;
                }
            };
            // 形状冲突检查（同形状多条目会让命中歧义）
            {
                let km = self.key_map.read();
                if let Some(other) = km.get(&nq.shape) {
                    tracing::warn!(
                        name = %name,
                        conflict = %other.cfg.name,
                        shape = %nq.shape,
                        "缓存形状与已有条目冲突，忽略"
                    );
                    continue;
                }
            }
            let pre_readable = nq.slots.iter().all(|s| s.is_some());
            let safe: String = name
                .chars()
                .map(|ch| {
                    if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' {
                        ch
                    } else {
                        '_'
                    }
                })
                .collect();
            let file_path = home.as_ref().map(|h| {
                h.join("cache")
                    .join("sqlcache")
                    .join(format!("{}.bin", safe))
            });
            // 文件恢复：具体结果 + 全部变体
            let concrete: Option<Arc<EngineOutcome>> = file_path.as_ref().and_then(|p| {
                read_file_sync(p).ok().and_then(|(k, o)| {
                    if k.is_empty() {
                        Some(Arc::new(o))
                    } else {
                        None
                    }
                })
            });
            let mut variants: HashMap<String, VariantCell> = HashMap::new();
            if let Some(p) = file_path.as_ref() {
                if let Some(dir) = p.parent() {
                    if let Ok(rd) = std::fs::read_dir(dir) {
                        for e in rd.flatten() {
                            let fname = e.file_name().to_string_lossy().into_owned();
                            if fname.starts_with(&format!("{}_", safe)) && fname.ends_with(".bin") {
                                if let Ok((vk, o)) = read_file_sync(&e.path()) {
                                    variants.insert(
                                        vk,
                                        VariantCell {
                                            outcome: Arc::new(o),
                                            updated: Instant::now(),
                                        },
                                    );
                                }
                            }
                        }
                    }
                }
            }
            let entry = Arc::new(CacheEntry {
                cfg: c.clone(),
                shape: nq.shape,
                slots: nq.slots,
                pre_readable,
                concrete: RwLock::new(concrete),
                loading: AtomicBool::new(false),
                variants: RwLock::new(variants),
                in_flight: Mutex::new(HashMap::new()),
                notify: Notify::new(),
                stop: AtomicBool::new(false),
                hits: AtomicU64::new(0),
                misses: AtomicU64::new(0),
                last_schema: RwLock::new(None),
                file_path,
            });
            entries.insert(name.clone(), entry.clone());
            // 仅可预读模板跑后台预热循环；含 ? 模板无法预读（按需回填）
            if pre_readable {
                if let Ok(handle) = tokio::runtime::Handle::try_current() {
                    handle.spawn(run_refresh_loop(entry, exec.clone()));
                }
            }
        }
        drop(entries);
        self.rebuild_key_maps();
    }

    fn rebuild_key_maps(&self) {
        let mut km = self.key_map.write();
        let mut kr = self.raw_map.write();
        km.clear();
        kr.clear();
        for entry in self.entries.read().values() {
            km.insert(entry.shape.clone(), entry.clone());
            if entry.pre_readable {
                kr.insert(trim_sql(&entry.cfg.sql), entry.clone());
            }
        }
    }

    /// 查询入口：命中返回缓存结果（Arc），未命中/不可缓存返回 None（走正常执行）。
    pub async fn lookup(
        &self,
        sql: &str,
        schema: Option<&str>,
        in_tx: bool,
        autocommit: bool,
    ) -> Option<Arc<EngineOutcome>> {
        // 配置热重载后惰性重同步（配置 Arc 指针指纹，O(1) 判定）
        if let Some(st) = self.state.lock().clone() {
            let ptr = Arc::as_ptr(&st.config()) as usize;
            if ptr != self.last_cfg_ptr.load(Ordering::Relaxed) {
                self.resync(&st);
            }
        }
        if self.key_map.read().is_empty() {
            return None;
        }
        if in_tx || !autocommit {
            return None;
        }
        if sql.trim_start().starts_with("/*!") || sql.trim_start().starts_with("/*+") {
            return None;
        }
        let exec = self.executor.lock().clone()?;
        // 原始文本快路径（仅可预读条目；文本一致 → 固定字面量必匹配）
        if !self.raw_map.read().is_empty() {
            let k = trim_sql(sql);
            let entry = self.raw_map.read().get(&k).cloned();
            if let Some(entry) = entry {
                if schema_matches(&entry.cfg, schema) {
                    *entry.last_schema.write() = schema.map(|s| s.to_string());
                    return entry.ensure_concrete(&exec, schema).await;
                }
            }
        }
        // PARAMETERIZED 形状匹配 + 槽位约束
        if let Some(q) = mycat3_sql::normalize_cache_query(sql, false) {
            let entry = self.key_map.read().get(&q.shape).cloned();
            if let Some(entry) = entry {
                if !entry.cfg.match_mode.eq_ignore_ascii_case("EXACT") {
                    let template = entry_template(&entry);
                    if let Some(vkey) = match_template(&template, &q) {
                        if schema_matches(&entry.cfg, schema) {
                            *entry.last_schema.write() = schema.map(|s| s.to_string());
                            if entry.pre_readable {
                                return entry.ensure_concrete(&exec, schema).await;
                            }
                            return entry.ensure_variant(&exec, sql, vkey, schema).await;
                        }
                    }
                }
            }
        }
        // EXACT 条目：按精确文本归一查找（单结果可预读）
        if let Some(qe) = mycat3_sql::normalize_cache_query(sql, true) {
            let entry = self.key_map.read().get(&qe.shape).cloned();
            if let Some(entry) = entry {
                if entry.cfg.match_mode.eq_ignore_ascii_case("EXACT")
                    && schema_matches(&entry.cfg, schema)
                {
                    *entry.last_schema.write() = schema.map(|s| s.to_string());
                    return entry.ensure_concrete(&exec, schema).await;
                }
            }
        }
        None
    }

    /// DML/DDL 写入后使相关缓存失效（启发式：模板 SQL 含该表名 token 即失效；
    /// 失效 = 清内存结果 + 删缓存文件，保守安全——宁可多失效不可给旧数据）。
    pub fn invalidate_table(&self, _schema: &str, table: &str) {
        let t = table.to_lowercase();
        for entry in self.entries.read().values() {
            let sql_l = entry.cfg.sql.to_lowercase();
            if !contains_word(&sql_l, &t) {
                continue;
            }
            *entry.concrete.write() = None;
            entry.variants.write().clear();
            // 清理缓存文件（具体 + 变体）
            if let Some(p) = entry.file_path.as_ref() {
                let _ = std::fs::remove_file(p);
                if let Some(dir) = p.parent() {
                    let prefix = p
                        .file_stem()
                        .map(|st| st.to_string_lossy().into_owned())
                        .unwrap_or_default();
                    if let Ok(rd) = std::fs::read_dir(dir) {
                        for e in rd.flatten() {
                            let name = e.file_name().to_string_lossy().into_owned();
                            if name.starts_with(&format!("{}_", prefix)) && name.ends_with(".bin") {
                                let _ = std::fs::remove_file(e.path());
                            }
                        }
                    }
                }
            }
            entry.notify.notify_waiters();
        }
    }

    /// 命中/未命中计数聚合（Prometheus 指标用）。
    pub fn totals(&self) -> (u64, u64) {
        let mut h = 0;
        let mut m = 0;
        for e in self.entries.read().values() {
            h += e.hits.load(Ordering::Relaxed);
            m += e.misses.load(Ordering::Relaxed);
        }
        (h, m)
    }

    /// showSqlCaches 输出。
    pub fn show(&self) -> EngineOutcome {
        let mut rows: Vec<Vec<Option<String>>> = Vec::new();
        for e in self.entries.read().values() {
            rows.push(vec![
                Some(e.cfg.name.clone()),
                Some(e.cfg.sql.clone()),
                Some(e.cfg.refresh_interval.to_string()),
                Some(e.cfg.match_mode.clone()),
                Some(
                    if e.pre_readable {
                        if e.concrete.read().is_some() {
                            "true"
                        } else {
                            "false"
                        }
                    } else {
                        "N/A" // 含 ? 模板不可预读
                    }
                    .to_string(),
                ),
                Some(e.variants.read().len().to_string()),
                Some(e.hits.load(Ordering::Relaxed).to_string()),
                Some(e.misses.load(Ordering::Relaxed).to_string()),
            ]);
        }
        rows.sort();
        let columns = [
            "NAME",
            "SQL",
            "REFRESH_INTERVAL_S",
            "MATCH_MODE",
            "HAS_CACHE",
            "VARIANT_COUNT",
            "HITS",
            "MISSES",
        ]
        .iter()
        .map(|n| crate::engine::EngineColumn {
            name: n.to_string(),
            mysql_type: field_type::VAR_STRING,
            charset: 33,
        })
        .collect();
        EngineOutcome::Rows { columns, rows }
    }
}

impl Default for SqlCacheManager {
    fn default() -> Self {
        Self::new()
    }
}

/// 模板槽位视图（匹配用）。
fn entry_template(entry: &CacheEntry) -> mycat3_sql::NormalizedQuery {
    mycat3_sql::NormalizedQuery {
        shape: entry.shape.clone(),
        slots: entry.slots.clone(),
    }
}

/// 缓存项的 schema 与当前会话一致才命中（修掉 Java 跨库撞键缺陷）。
fn schema_matches(cfg: &SqlCacheConfig, schema: Option<&str>) -> bool {
    match (&cfg.schema_name, schema) {
        (None, _) => true,
        (Some(a), Some(b)) => a.eq_ignore_ascii_case(b),
        (Some(_), None) => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::EngineColumn;
    use mycat3_config::MycatRouterConfig;
    use std::sync::atomic::AtomicU64;
    use std::time::Duration;

    fn outcome(v: &str) -> EngineOutcome {
        EngineOutcome::Rows {
            columns: vec![EngineColumn {
                name: "c".into(),
                mysql_type: field_type::VAR_STRING,
                charset: 33,
            }],
            rows: vec![vec![Some(v.to_string())]],
        }
    }

    /// 假执行器：每次执行计数 +1；前 fail_times 次失败；返回 (执行 SQL, 序号)。
    struct FakeExec {
        count: AtomicU64,
        delay_ms: u64,
        fail_times: AtomicU64,
    }

    impl CacheExecutor for FakeExec {
        fn execute<'a>(
            &'a self,
            sql: &'a str,
            _schema: Option<&'a str>,
        ) -> BoxFuture<'a, Result<EngineOutcome, EngineError>> {
            let delay = self.delay_ms;
            Box::pin(async move {
                if delay > 0 {
                    tokio::time::sleep(Duration::from_millis(delay)).await;
                }
                let n = self.count.fetch_add(1, Ordering::Relaxed) + 1;
                if n <= self.fail_times.load(Ordering::Relaxed) {
                    return Err(EngineError::general("fake exec failure"));
                }
                Ok(outcome(&format!("{}-{}", sql, n)))
            })
        }
    }

    fn test_cfg(name: &str, sql: &str, initial_delay: u64, refresh: u64) -> SqlCacheConfig {
        SqlCacheConfig {
            name: name.into(),
            sql: sql.into(),
            refresh_interval: refresh,
            initial_delay,
            match_mode: "PARAMETERIZED".into(),
            max_rows: 1000,
            max_bytes: 0,
            schema_name: None,
        }
    }

    /// 空配置建 state（attach 不建条目），注入默认假执行器后 reload 带缓存配置，
    /// 条目构建/后台刷新在首次 lookup 的惰性重同步时使用当前注入的执行器。
    fn state_with_caches(cfgs: Vec<SqlCacheConfig>) -> Arc<ServerState> {
        let st = ServerState::new(MycatRouterConfig::new()).unwrap();
        st.sql_cache.set_executor(Arc::new(FakeExec {
            count: AtomicU64::new(0),
            delay_ms: 0,
            fail_times: AtomicU64::new(0),
        }));
        let mut c = MycatRouterConfig::new();
        for sc in cfgs {
            c.sql_caches.insert(sc.name.clone(), sc);
        }
        st.reload(c, None).unwrap();
        st
    }

    #[tokio::test]
    async fn test_lookup_miss_without_entries() {
        let m = SqlCacheManager::new();
        assert!(m.lookup("select 1", None, false, true).await.is_none());
    }

    #[tokio::test]
    async fn test_transaction_not_served() {
        let st = state_with_caches(vec![test_cfg("t", "select 'x' from dual", 3600, 3600)]);
        assert!(st
            .sql_cache
            .lookup("select 'x' from dual", None, true, true)
            .await
            .is_none());
        assert!(st
            .sql_cache
            .lookup("select 'x' from dual", None, false, false)
            .await
            .is_none());
    }

    #[tokio::test]
    async fn test_concrete_fixed_literal_constraint() {
        let exec = Arc::new(FakeExec {
            count: AtomicU64::new(0),
            delay_ms: 0,
            fail_times: AtomicU64::new(0),
        });
        let st = state_with_caches(vec![test_cfg("t", "select 'x' from dual", 3600, 3600)]);
        st.sql_cache.set_executor(exec.clone());
        // 文本一致 → 预读执行（1 次）
        let a = st
            .sql_cache
            .lookup("select 'x' from dual", None, false, true)
            .await
            .unwrap();
        assert_eq!(row0(&a), "select 'x' from dual-1");
        // 固定字面量不匹配（'y' != 'x'）→ 不命中，走正常执行
        assert!(st
            .sql_cache
            .lookup("select 'y' from dual", None, false, true)
            .await
            .is_none());
        assert_eq!(exec.count.load(Ordering::Relaxed), 1);
        // 文本一致再查 → 命中
        let b = st
            .sql_cache
            .lookup("select 'x' from dual", None, false, true)
            .await
            .unwrap();
        assert_eq!(row0(&b), "select 'x' from dual-1");
    }

    #[tokio::test]
    async fn test_template_placeholder_variants() {
        // 模板含 ?：不同参数值 → 各自独立结果（来查询原文执行）
        let exec = Arc::new(FakeExec {
            count: AtomicU64::new(0),
            delay_ms: 0,
            fail_times: AtomicU64::new(0),
        });
        let st = state_with_caches(vec![test_cfg(
            "t",
            "select * from t where id = ?",
            3600,
            3600,
        )]);
        st.sql_cache.set_executor(exec.clone());
        let a = st
            .sql_cache
            .lookup("select * from t where id = 1", None, false, true)
            .await
            .unwrap();
        assert_eq!(row0(&a), "select * from t where id = 1-1");
        let b = st
            .sql_cache
            .lookup("select * from t where id = 2", None, false, true)
            .await
            .unwrap();
        assert_eq!(
            row0(&b),
            "select * from t where id = 2-2",
            "不同参数结果不同"
        );
        // 重复 id=1 → 命中变体（不再执行）
        let a2 = st
            .sql_cache
            .lookup("select * from t where id = 1", None, false, true)
            .await
            .unwrap();
        assert_eq!(row0(&a2), "select * from t where id = 1-1");
        assert_eq!(exec.count.load(Ordering::Relaxed), 2);
    }

    #[tokio::test]
    async fn test_variant_coalescing_single_execution() {
        let exec = Arc::new(FakeExec {
            count: AtomicU64::new(0),
            delay_ms: 100,
            fail_times: AtomicU64::new(0),
        });
        let st = state_with_caches(vec![test_cfg(
            "t",
            "select * from t where id = ?",
            3600,
            3600,
        )]);
        st.sql_cache.set_executor(exec.clone());
        let handles: Vec<_> = (0..8)
            .map(|_| {
                let st2 = st.clone();
                tokio::spawn(async move {
                    st2.sql_cache
                        .lookup("select * from t where id = 7", None, false, true)
                        .await
                })
            })
            .collect();
        for h in handles {
            let got = h.await.unwrap().unwrap();
            assert_eq!(row0(&got), "select * from t where id = 7-1");
        }
        assert_eq!(
            exec.count.load(Ordering::Relaxed),
            1,
            "同变体并发共享一次执行"
        );
    }

    #[tokio::test]
    async fn test_variant_ttl_lazy_refresh() {
        let exec = Arc::new(FakeExec {
            count: AtomicU64::new(0),
            delay_ms: 0,
            fail_times: AtomicU64::new(0),
        });
        // refreshInterval=1s：变体 1s 过期，访问时惰性重执行
        let st = state_with_caches(vec![test_cfg("t", "select * from t where id = ?", 3600, 1)]);
        st.sql_cache.set_executor(exec.clone());
        let a = st
            .sql_cache
            .lookup("select * from t where id = 9", None, false, true)
            .await
            .unwrap();
        assert_eq!(row0(&a), "select * from t where id = 9-1");
        tokio::time::sleep(Duration::from_millis(1200)).await;
        let b = st
            .sql_cache
            .lookup("select * from t where id = 9", None, false, true)
            .await
            .unwrap();
        assert_eq!(row0(&b), "select * from t where id = 9-2", "过期后惰性刷新");
    }

    #[test]
    fn test_file_roundtrip_with_variant_key() {
        let dir = std::env::temp_dir().join(format!("m3cache_test_{}", std::process::id()));
        let path = dir.join("t1_abc.bin");
        let o = EngineOutcome::Rows {
            columns: vec![
                EngineColumn {
                    name: "id".into(),
                    mysql_type: field_type::LONGLONG,
                    charset: 63,
                },
                EngineColumn {
                    name: "name".into(),
                    mysql_type: field_type::VAR_STRING,
                    charset: 33,
                },
            ],
            rows: vec![
                vec![Some("1".into()), None],
                vec![None, Some("汉字/emoji-🚀".into())],
            ],
        };
        write_file_sync(&path, "[\"42\",\"x\"]", &o).unwrap();
        let (k, back) = read_file_sync(&path).unwrap();
        assert_eq!(k, "[\"42\",\"x\"]", "变体键往返");
        assert_eq!(back, o, "结果往返一致");
        // 截断损坏 → 读取失败
        std::fs::write(&path, &MAGIC[..4]).unwrap();
        assert!(read_file_sync(&path).is_err());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn test_over_limit_falls_through() {
        let exec = Arc::new(FakeExec {
            count: AtomicU64::new(0),
            delay_ms: 0,
            fail_times: AtomicU64::new(0),
        });
        let mut cfg = test_cfg("t", "select 'x' from dual", 3600, 3600);
        cfg.max_bytes = 1; // 结果必超 1 字节 → 不缓存 → 回落正常执行
        let st = state_with_caches(vec![cfg]);
        st.sql_cache.set_executor(exec.clone());
        assert!(st
            .sql_cache
            .lookup("select 'x' from dual", None, false, true)
            .await
            .is_none());
        assert_eq!(exec.count.load(Ordering::Relaxed), 1);
    }

    fn row0(o: &EngineOutcome) -> String {
        match o {
            EngineOutcome::Rows { rows, .. } => rows[0][0].clone().unwrap(),
            _ => panic!("expected rows"),
        }
    }
}
