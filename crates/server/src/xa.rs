//! MySQL-only XA 分布式事务（2PC + 崩溃恢复）。
//!
//! 对应 Java `va` 模块（XaSqlConnection/XaLog/ImmutableCoordinatorLog）：
//! - 分支语义：事务按数据源固定连接，XA 模式首次写语句前对每分支发 `XA START`；
//! - 提交：各分支 `XA END` + `XA PREPARE`（单参与者 `XA COMMIT ... ONE PHASE` 短路）；
//!   全部 prepare 成功后**先落 coordinator WAL（fsync）再逐分支 `XA COMMIT`**；
//! - 回滚：各分支 `XA END`（忽略错误）+ `XA ROLLBACK`（未 prepare 的分支随连接
//!   关闭自动回滚，无需 WAL）；
//! - 崩溃恢复：启动时对每个 MySQL 数据源 `XA RECOVER` 扫描孤儿 xid——
//!   WAL 中有 commit 决策 → 补 `XA COMMIT`；无决策（prepare 阶段崩溃）→ `XA ROLLBACK`；
//!   已提交完成的决策标记 done（压缩时移除）。
//!
//! 边界（文档化）：仅支持 MySQL 后端（Oracle 后端分支显式报错）；
//! 恢复窗口内参与者持锁；TM 崩溃在 prepare 与 WAL 之间 → 孤儿事务回滚（一致性保持，
//! 代价为锁）；WAL 写入后崩溃 → 恢复补提交。**故障时刻不存在 100% 原子**（2PC 固有）。

use std::io::{BufRead, BufReader, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use parking_lot::Mutex;
use serde::{Deserialize, Serialize};

use mycat3_client::QueryOutcome;

use crate::engine::EngineError;
use crate::state::ServerState;

/// XA formatID（"M3XP"）：孤儿 xid 归属判定。
pub const XA_FORMAT_ID: u32 = 0x4D33_5850;
static GTX_SEQ: AtomicU64 = AtomicU64::new(0);

/// 生成全局事务 ID（16 位十六进制：8B 毫秒时间戳 + 8B 原子序号）。
pub fn new_gtrid() -> String {
    let ts = crate::state::epoch_millis();
    let seq = GTX_SEQ.fetch_add(1, Ordering::Relaxed);
    format!("{:016x}{:016x}", ts, seq)
}

/// XA 语句构造（xid 三元组按 `XA START 'gtrid','bqual',fmt` 形式）。
pub fn xa_sql(op: &str, gtrid: &str, bqual: &str) -> String {
    format!("{} '{}','{}',{}", op, gtrid, bqual, XA_FORMAT_ID)
}

pub fn xa_start(gtrid: &str, bqual: &str) -> String {
    xa_sql("XA START", gtrid, bqual)
}
pub fn xa_end(gtrid: &str, bqual: &str) -> String {
    xa_sql("XA END", gtrid, bqual)
}
pub fn xa_prepare(gtrid: &str, bqual: &str) -> String {
    xa_sql("XA PREPARE", gtrid, bqual)
}
pub fn xa_commit(gtrid: &str, bqual: &str) -> String {
    xa_sql("XA COMMIT", gtrid, bqual)
}
pub fn xa_commit_one_phase(gtrid: &str, bqual: &str) -> String {
    format!("{} ONE PHASE", xa_sql("XA COMMIT", gtrid, bqual))
}
pub fn xa_rollback(gtrid: &str, bqual: &str) -> String {
    xa_sql("XA ROLLBACK", gtrid, bqual)
}

/// 从 `XA RECOVER` 结果行的 data 列解析 gtrid。
/// MySQL 的 data = gtrid(gtrid_len) + bqual(bqual_len)（**不含** formatID 前缀，
/// formatID 为独立列，由调用方校验归属）。
pub fn parse_recover_gtrid(data: &[u8], gtrid_len: usize) -> Option<String> {
    if data.len() < gtrid_len {
        return None;
    }
    let g = &data[..gtrid_len];
    if g.iter().all(|b| b.is_ascii_hexdigit()) {
        Some(String::from_utf8_lossy(g).into_owned())
    } else {
        None
    }
}

// ---------------------------------------------------------------------------
// Coordinator WAL
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
struct LogLine {
    op: String, // "commit" | "done"
    gtrid: String,
    #[serde(default)]
    participants: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct CommitDecision {
    pub gtrid: String,
    pub participants: Vec<String>,
}

/// 协调者决策日志：`commit`（含参与数据源）先于广播 COMMIT 落盘（fsync）；
/// 全部参与者提交成功后记 `done`（加载时折叠、压缩）。
pub struct XaLog {
    path: Option<PathBuf>,
    pending: Mutex<Vec<CommitDecision>>,
}

impl XaLog {
    /// 新建并加载既有日志（fold done；损坏行忽略）。`path=None` = 纯内存（测试）。
    pub fn new(path: Option<PathBuf>) -> Self {
        let mut pending = Vec::new();
        if let Some(p) = &path {
            if let Ok(f) = std::fs::File::open(p) {
                for line in BufReader::new(f).lines().flatten() {
                    let l: LogLine = match serde_json::from_str(&line) {
                        Ok(l) => l,
                        Err(_) => continue,
                    };
                    match l.op.as_str() {
                        "commit" => pending.push(CommitDecision {
                            gtrid: l.gtrid,
                            participants: l.participants,
                        }),
                        "done" => pending.retain(|d| d.gtrid != l.gtrid),
                        _ => {}
                    }
                }
            }
        }
        Self {
            path,
            pending: Mutex::new(pending),
        }
    }

    /// 记录 commit 决策（append + fsync，先于广播 XA COMMIT）。
    pub fn commit_decision(&self, gtrid: &str, participants: &[String]) -> std::io::Result<()> {
        let line = LogLine {
            op: "commit".into(),
            gtrid: gtrid.into(),
            participants: participants.to_vec(),
        };
        self.append(&line)?;
        self.pending.lock().push(CommitDecision {
            gtrid: gtrid.into(),
            participants: participants.to_vec(),
        });
        Ok(())
    }

    /// 全部参与者提交成功：记 done 并从 pending 移除。
    pub fn mark_done(&self, gtrid: &str) -> std::io::Result<()> {
        let line = LogLine {
            op: "done".into(),
            gtrid: gtrid.into(),
            participants: vec![],
        };
        self.append(&line)?;
        self.pending.lock().retain(|d| d.gtrid != gtrid);
        Ok(())
    }

    /// 待裁决的 commit 决策快照。
    pub fn pending(&self) -> Vec<CommitDecision> {
        self.pending.lock().clone()
    }

    fn append(&self, line: &LogLine) -> std::io::Result<()> {
        let Some(p) = &self.path else {
            return Ok(()); // 纯内存模式
        };
        if let Some(dir) = p.parent() {
            std::fs::create_dir_all(dir)?;
        }
        let mut f = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(p)?;
        writeln!(f, "{}", serde_json::to_string(line).unwrap())?;
        f.sync_all() // 决策必须先于 COMMIT 广播持久化
    }

    /// 压缩：重写日志仅保留 pending（启动加载后调用）。
    pub fn compact(&self) {
        let Some(p) = &self.path else {
            return;
        };
        let pending = self.pending();
        let tmp = p.with_extension("tmp");
        let r = (|| -> std::io::Result<()> {
            if let Some(dir) = p.parent() {
                std::fs::create_dir_all(dir)?;
            }
            let mut f = std::fs::File::create(&tmp)?;
            for d in &pending {
                writeln!(
                    f,
                    "{}",
                    serde_json::to_string(&LogLine {
                        op: "commit".into(),
                        gtrid: d.gtrid.clone(),
                        participants: d.participants.clone(),
                    })
                    .unwrap()
                )?;
            }
            f.sync_all()?;
            std::fs::rename(&tmp, p)?;
            Ok(())
        })();
        if let Err(e) = r {
            tracing::warn!(error = %e, "xa wal compact failed");
        }
    }
}

// ---------------------------------------------------------------------------
// 崩溃恢复
// ---------------------------------------------------------------------------

/// 启动恢复：对每个 MySQL 数据源 `XA RECOVER`，孤儿 xid 按 WAL 裁决
///（有 commit 决策 → 补 COMMIT；无 → ROLLBACK）；WAL 中已消失的决策标记 done。
/// 在接受任何新连接前执行（bootstrap 调用）；失败仅告警不阻断启动。
pub async fn recover(state: &Arc<ServerState>) {
    let pending = state.xa_log.pending();
    // 注意：即使无 WAL 决策也必须扫描 RECOVER——prepare 阶段崩溃留下的
    // 孤儿分支（无 commit 决策）需要被回滚
    let cfg = state.config();
    for (name, ds) in &cfg.datasources {
        let is_mysql = ds.db_type.eq_ignore_ascii_case("mysql");
        let is_pg = ds.db_type.eq_ignore_ascii_case("postgresql")
            || ds.db_type.eq_ignore_ascii_case("postgres")
            || ds.db_type.eq_ignore_ascii_case("pgsql");
        if is_pg {
            recover_pg_datasource(state, name, ds, &pending).await;
            continue;
        }
        if !is_mysql {
            continue;
        }
        let r = async {
            let mut conn = state
                .conn_mgr
                .connect_direct(ds)
                .await
                .map_err(|e| EngineError::general(e.to_string()))?;
            let outcome = conn
                .query("XA RECOVER")
                .await
                .map_err(|e| EngineError::general(e.to_string()))?;
            conn.quit().await;
            Ok::<QueryOutcome, EngineError>(outcome)
        }
        .await;
        let rs = match r {
            Ok(QueryOutcome::Rows(rs)) => rs,
            Ok(_) => continue,
            Err(e) => {
                tracing::debug!(datasource = %name, error = %e, "xa recover 查询失败");
                continue;
            }
        };
        // 列：formatID, gtrid_length, bqual_length, data（二进制）
        for row in rs.decode_rows() {
            // 列：formatID, gtrid_length, bqual_length, data（gtrid+bqual 二进制）
            let fmt = row
                .get(0)
                .and_then(|v| v.as_deref())
                .and_then(|b| String::from_utf8_lossy(b).trim().parse::<u32>().ok());
            if fmt != Some(XA_FORMAT_ID) {
                continue; // 非本实例 xid
            }
            let Some(gtrid_len) = row
                .get(1)
                .and_then(|v| v.as_deref())
                .and_then(|b| String::from_utf8_lossy(b).trim().parse::<usize>().ok())
            else {
                continue;
            };
            let Some(data) = row.get(3).and_then(|v| v.as_deref()) else {
                continue;
            };
            let Some(gtrid) = parse_recover_gtrid(data, gtrid_len) else {
                continue;
            };
            let decision = pending.iter().find(|d| d.gtrid == gtrid);
            let mut conn = match state.conn_mgr.connect_direct(ds).await {
                Ok(c) => c,
                Err(e) => {
                    tracing::debug!(datasource = %name, error = %e, "xa recover 重连失败");
                    continue;
                }
            };
            match decision {
                Some(d) => {
                    let bqual = String::from_utf8_lossy(&data[gtrid_len..]).into_owned();
                    let r = conn.query(&xa_commit(&gtrid, &bqual)).await;
                    tracing::info!(gtrid = %gtrid, datasource = %name, ok = %r.is_ok(), "xa 恢复：补提交");
                    let _ = d;
                }
                None => {
                    let bqual = String::from_utf8_lossy(&data[gtrid_len..]).into_owned();
                    let r = conn.query(&xa_rollback(&gtrid, &bqual)).await;
                    tracing::info!(gtrid = %gtrid, datasource = %name, ok = %r.is_ok(), "xa 恢复：无决策回滚");
                }
            }
            conn.quit().await;
        }
    }
    // 决策在 RECOVER 中未出现（此前已全部提交）→ done
    for d in pending {
        let _ = state.xa_log.mark_done(&d.gtrid);
    }
    state.xa_log.compact();
}

/// PG 数据源恢复：扫描 pg_prepared_xacts（gid 即我们的 gtrid，32 位十六进制），
/// 有决策 → COMMIT PREPARED；无决策 → ROLLBACK PREPARED。
async fn recover_pg_datasource(
    _state: &Arc<ServerState>,
    name: &str,
    ds: &mycat3_config::DatasourceConfig,
    pending: &[CommitDecision],
) {
    let ep = match mycat3_pgclient::parse_pg_jdbc_url(&ds.url, &ds.user, &ds.password) {
        Ok(e) => e,
        Err(e) => {
            tracing::debug!(datasource = %name, error = %e, "xa recover url 解析失败");
            return;
        }
    };
    let mut conn = match mycat3_pgclient::PgConnection::connect(&ep).await {
        Ok(c) => c,
        Err(e) => {
            tracing::debug!(datasource = %name, error = %e, "xa recover 连接失败");
            return;
        }
    };
    let gids = match conn.query("select gid from pg_prepared_xacts").await {
        Ok(mycat3_pgclient::PgOutcome::Rows { rows, .. }) => rows
            .into_iter()
            .filter_map(|r| r.into_iter().next().flatten())
            .collect::<Vec<String>>(),
        _ => Vec::new(),
    };
    for gid in gids {
        // 仅处理本实例格式的 gid：<gtrid32>_<分支名>（gtrid 为 32 位十六进制）
        let (gtrid, branch) = match gid.split_once('_') {
            Some((g, b)) if g.len() == 32 && g.chars().all(|c| c.is_ascii_hexdigit()) => (g, b),
            _ => continue,
        };
        let has_decision = pending.iter().any(|d| d.gtrid == gtrid);
        let sql = if has_decision {
            format!("COMMIT PREPARED '{}_{}'", gtrid, branch)
        } else {
            format!("ROLLBACK PREPARED '{}_{}'", gtrid, branch)
        };
        let r = conn.query(&sql).await;
        tracing::info!(
            gtrid = %gtrid,
            datasource = %name,
            commit = %has_decision,
            ok = %r.is_ok(),
            "xa 恢复（postgres）"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_xid_roundtrip() {
        let g = new_gtrid();
        assert_eq!(g.len(), 32);
        assert!(g.chars().all(|c| c.is_ascii_hexdigit()));
        assert_ne!(g, new_gtrid(), "全局 ID 唯一");
        assert_eq!(xa_start("abc", "ds0"), "XA START 'abc','ds0',1295210576");
        assert_eq!(
            xa_commit_one_phase("abc", "ds0"),
            "XA COMMIT 'abc','ds0',1295210576 ONE PHASE"
        );
    }

    #[test]
    fn test_parse_recover_gtrid() {
        // MySQL data = gtrid + bqual（无 formatID 前缀，formatID 为独立列）
        let mut data = Vec::new();
        data.extend_from_slice(b"0123456789abcdef");
        data.extend_from_slice(b"ds0");
        assert_eq!(
            parse_recover_gtrid(&data, 16).as_deref(),
            Some("0123456789abcdef")
        );
        // 非十六进制 → None
        assert!(parse_recover_gtrid(b"zzzzzzzzzzzzzzzz", 16).is_none());
        // 截断 → None
        assert!(parse_recover_gtrid(&[0x30], 16).is_none());
    }

    #[test]
    fn test_wal_memory_mode() {
        let log = XaLog::new(None);
        log.commit_decision("g1", &["ds0".into(), "ds1".into()])
            .unwrap();
        log.commit_decision("g2", &["ds0".into()]).unwrap();
        assert_eq!(log.pending().len(), 2);
        log.mark_done("g1").unwrap();
        let p = log.pending();
        assert_eq!(p.len(), 1);
        assert_eq!(p[0].gtrid, "g2");
    }

    #[test]
    fn test_wal_file_persistence() {
        let dir = std::env::temp_dir().join(format!("m3xa_test_{}", std::process::id()));
        let path = dir.join("xa-wal.log");
        {
            let log = XaLog::new(Some(path.clone()));
            log.commit_decision("g1", &["ds0".into()]).unwrap();
            log.commit_decision("g2", &["ds1".into()]).unwrap();
            log.mark_done("g1").unwrap();
        }
        // 重新加载：g1 done 折叠，仅剩 g2
        let log = XaLog::new(Some(path.clone()));
        let p = log.pending();
        assert_eq!(p.len(), 1);
        assert_eq!(p[0].gtrid, "g2");
        log.compact();
        let log2 = XaLog::new(Some(path.clone()));
        assert_eq!(log2.pending().len(), 1);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
