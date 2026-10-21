//! 事务会话：按数据源固定后端连接。
//!
//! 对应 Mycat3 `datasource.transactionsession` 包的 `TransactionSessionTemplate` /
//! `LocalTransactionSession`：
//! - `getConnection(datasource)`：事务内同一数据源复用同一连接（updateConnectionMap）
//! - `openStatementState` / `closeStatementState`：autocommit=0 隐式开启事务；
//!   语句结束且不在事务中时归还连接
//! - `begin/commit/rollback`：对所有已固定连接广播事务命令
//!
//! 后端无关：固定的是 `BackendSession`（MySQL/Oracle 双后端），事务命令经
//! `tx_command` 分派（MySQL 下推 BEGIN/COMMIT SQL；Oracle 隐式事务 + 驱动 commit/rollback）。

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::backend::{BackendError, BackendSession};
use crate::xa::XaLog;

/// 会话级事务状态。
pub struct TransactionSession {
    /// 数据源名 → 固定连接
    pinned: HashMap<String, BackendSession>,
    /// 是否显式开启事务（BEGIN / autocommit=0）
    pub explicit_tx: bool,
    /// 会话粘性路由：target → 上次解析出的数据源
    /// （对应 Java DataSourceNearnessImpl，读己所写一致性）
    nearness: HashMap<String, String>,
    /// XA 模式上下文（transaction_policy=xa 时非空）
    xa: Option<XaCtx>,
    /// 代理模式：会话级固定后端连接（数据源名, 连接）。
    /// 会话生命周期内保持同一连接，使 USE/SET/事务/临时表等会话语义原样生效。
    proxy_conn: Option<(String, BackendSession)>,
}

/// XA 分支上下文。
struct XaCtx {
    /// 全局事务 ID（16 位十六进制）
    gtrid: String,
    /// 已 XA START 的数据源分支
    started: HashSet<String>,
}

impl TransactionSession {
    pub fn new() -> Self {
        TransactionSession {
            pinned: HashMap::new(),
            explicit_tx: false,
            nearness: HashMap::new(),
            xa: None,
            proxy_conn: None,
        }
    }

    /// 代理模式：已固定的后端连接（数据源名, 连接）。
    pub fn proxy_conn_mut(&mut self) -> Option<&mut BackendSession> {
        self.proxy_conn.as_mut().map(|(_, c)| c)
    }

    /// 代理模式：固定后端连接。
    pub fn proxy_pin(&mut self, datasource: String, conn: BackendSession) {
        self.proxy_conn = Some((datasource, conn));
    }

    /// 代理模式：取出（丢弃）固定连接。
    pub fn proxy_take(&mut self) -> Option<BackendSession> {
        self.proxy_conn.take().map(|(_, c)| c)
    }

    /// 代理模式：当前固定连接的数据源名。
    pub fn proxy_datasource(&self) -> Option<&str> {
        self.proxy_conn.as_ref().map(|(d, _)| d.as_str())
    }

    /// 是否处于 XA 模式事务中。
    pub fn xa_active(&self) -> bool {
        self.xa.is_some()
    }

    /// 进入 XA 模式（BEGIN 或 autocommit=0 隐式开启时惰性初始化）。
    fn ensure_xa(&mut self) {
        if self.xa.is_none() {
            self.xa = Some(XaCtx {
                gtrid: crate::xa::new_gtrid(),
                started: HashSet::new(),
            });
        }
    }

    /// 粘性路由查询（对应 DataSourceNearness.getDataSourceNearness）
    pub fn nearness_get(&self, target: &str) -> Option<&str> {
        self.nearness.get(target).map(|s| s.as_str())
    }

    /// 记录 target → datasource 粘性映射
    pub fn nearness_put(&mut self, target: &str, datasource: &str) {
        self.nearness
            .insert(target.to_string(), datasource.to_string());
    }

    /// 事务中已固定的数据源数。
    pub fn pinned_count(&self) -> usize {
        self.pinned.len()
    }

    pub fn pinned_names(&self) -> Vec<String> {
        self.pinned.keys().cloned().collect()
    }

    /// 获取（或固定）某数据源的连接。对应 `updateConnectionMap`。
    pub fn pin(&mut self, datasource: &str, conn: BackendSession) {
        self.pinned.insert(datasource.to_string(), conn);
    }

    /// 已固定的连接（可变更用）。
    pub fn get_mut(&mut self, datasource: &str) -> Option<&mut BackendSession> {
        self.pinned.get_mut(datasource)
    }

    pub fn contains(&self, datasource: &str) -> bool {
        self.pinned.contains_key(datasource)
    }

    /// 移除固定连接（后端错误后剔除 broken 连接）。
    pub fn unpin(&mut self, datasource: &str) {
        self.pinned.remove(datasource);
    }

    /// 对应 `openStatementState`：autocommit=0 时首次取连接即隐式 BEGIN。
    pub async fn open_statement_state(
        &mut self,
        datasource: &str,
        autocommit: bool,
    ) -> Result<(), BackendError> {
        if autocommit || self.explicit_tx {
            return Ok(());
        }
        if let Some(session) = self.pinned.get_mut(datasource) {
            if !session.in_transaction() {
                session.tx_command("BEGIN").await?;
                session.set_in_transaction(true);
            }
        }
        Ok(())
    }

    /// 对应 `closeStatementState`：不在事务中则归还全部连接。
    pub fn close_statement_state(&mut self, autocommit: bool) {
        if autocommit && !self.explicit_tx {
            self.pinned.clear(); // Drop → 归还连接池
        }
    }

    /// 对所有固定连接执行 BEGIN（显式事务开始）。`xa_mode` 对应
    /// `transaction_policy=xa`（分支 XA START 延迟到首条语句/新固定连接时）。
    pub async fn begin(&mut self, xa_mode: bool) -> Result<(), BackendError> {
        self.explicit_tx = true;
        if xa_mode {
            self.ensure_xa();
            return Ok(());
        }
        // 已固定的连接立即 BEGIN；后续新固定的连接由 ensure_tx 处理
        for session in self.pinned.values_mut() {
            if !session.in_transaction() {
                session.tx_command("BEGIN").await?;
                session.set_in_transaction(true);
            }
        }
        Ok(())
    }

    /// 事务开始保证（**未固定**的新连接；acquire 路径调用）：
    /// XA 模式 → 对该会话发 XA START（Oracle 后端显式拒绝）；代理模式 → BEGIN。
    pub async fn ensure_new_session(
        &mut self,
        datasource: &str,
        session: &mut BackendSession,
        xa_mode: bool,
    ) -> Result<(), BackendError> {
        if xa_mode {
            self.ensure_xa();
            let ctx = self.xa.as_mut().unwrap();
            if ctx.started.insert(datasource.to_string()) {
                let gtrid = ctx.gtrid.clone();
                session.xa_start_branch(&gtrid, datasource).await?;
                session.set_in_transaction(true);
            }
            return Ok(());
        }
        session.tx_command("BEGIN").await?;
        session.set_in_transaction(true);
        Ok(())
    }

    /// 事务开始保证（**已固定**连接；pinned 路径调用）：
    /// XA 模式 → 确保该分支已 XA START（含 Oracle 后端显式拒绝）；
    /// 代理模式 → 缺省 BEGIN。连接须已固定。
    pub async fn ensure_pinned_started(
        &mut self,
        datasource: &str,
        xa_mode: bool,
    ) -> Result<(), BackendError> {
        if xa_mode {
            self.ensure_xa();
            let need_start = !self.xa.as_ref().unwrap().started.contains(datasource);
            if need_start {
                let session = self.pinned.get_mut(datasource).ok_or_else(|| {
                    BackendError::Conn(format!("datasource {} 未固定", datasource))
                })?;
                let gtrid = self.xa.as_ref().unwrap().gtrid.clone();
                session
                    .xa_command(&crate::xa::xa_start(&gtrid, datasource))
                    .await?;
                session.set_in_transaction(true);
                self.xa
                    .as_mut()
                    .unwrap()
                    .started
                    .insert(datasource.to_string());
            }
            return Ok(());
        }
        let session = self
            .pinned
            .get_mut(datasource)
            .ok_or_else(|| BackendError::Conn(format!("datasource {} 未固定", datasource)))?;
        if !session.in_transaction() {
            session.tx_command("BEGIN").await?;
            session.set_in_transaction(true);
        }
        Ok(())
    }

    /// 确保某数据源连接处于事务中（新固定连接时调用）。
    pub async fn ensure_tx(
        &mut self,
        datasource: &str,
        autocommit: bool,
    ) -> Result<(), BackendError> {
        let need = self.explicit_tx || !autocommit;
        if !need {
            return Ok(());
        }
        if let Some(session) = self.pinned.get_mut(datasource) {
            if !session.in_transaction() {
                session.tx_command("BEGIN").await?;
                session.set_in_transaction(true);
            }
        }
        Ok(())
    }

    /// 提交所有固定连接的事务。对应 `commit`。
    pub async fn commit(&mut self, xa_log: &Arc<XaLog>) -> Result<(), BackendError> {
        let result = if self.xa.is_some() {
            self.xa_commit(xa_log).await
        } else {
            self.broadcast("COMMIT", true).await
        };
        self.explicit_tx = false;
        self.xa = None;
        self.pinned.clear();
        result
    }

    /// XA 两阶段提交：
    /// 1) 各分支 XA END + XA PREPARE（任一失败 → 全部 XA ROLLBACK 并返回错误）；
    /// 2) 先落 WAL 决策（fsync），再逐分支提交已 PREPARE 的事务
    ///    （MySQL XA COMMIT / PG COMMIT PREPARED；单参与者同样如此）；
    ///    全部成功后 mark_done。
    async fn xa_commit(&mut self, xa_log: &Arc<XaLog>) -> Result<(), BackendError> {
        let ctx = self.xa.as_ref().unwrap();
        let gtrid = ctx.gtrid.clone();
        // 1) 各分支 PREPARE（MySQL：XA END + XA PREPARE；PG：PREPARE TRANSACTION）
        let mut first_err: Option<BackendError> = None;
        for (ds, session) in self.pinned.iter_mut() {
            if let Err(e) = session.xa_prepare_branch(&gtrid, ds).await {
                if first_err.is_none() {
                    first_err = Some(e);
                }
            }
        }
        if let Some(e) = first_err {
            // prepare 失败：回滚全部分支（未 prepare 的随连接关闭回滚）
            for (ds, session) in self.pinned.iter_mut() {
                let _ = session.xa_rollback_branch(&gtrid, ds).await;
            }
            return Err(e);
        }
        // 2) COMMIT
        let participants: Vec<String> = self.pinned.keys().cloned().collect();
        if participants.is_empty() {
            return Ok(());
        }
        // 所有分支都已 PREPARE，单参与者也必须 commit prepared：
        // PostgreSQL 的普通 COMMIT 不会结束 prepared transaction。
        // 单参与者同样先落决策 WAL，保证崩溃后可裁决。
        if let Err(e) = xa_log.commit_decision(&gtrid, &participants) {
            // WAL 失败：不能进入提交阶段（崩溃将无法裁决），整体回滚
            for (ds, session) in self.pinned.iter_mut() {
                let _ = session.xa_rollback_branch(&gtrid, ds).await;
            }
            return Err(BackendError::Conn(format!("xa 决策日志写入失败: {}", e)));
        }
        let mut first_err: Option<BackendError> = None;
        for (ds, session) in self.pinned.iter_mut() {
            if let Err(e) = session.xa_commit_branch(&gtrid, ds, false).await {
                session.mark_broken();
                if first_err.is_none() {
                    first_err = Some(e);
                }
                // 其余分支继续提交（恢复流程会补齐失败分支）
            }
        }
        if first_err.is_none() {
            let _ = xa_log.mark_done(&gtrid);
        }
        match first_err {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }

    /// 回滚所有固定连接的事务。对应 `rollback`。
    pub async fn rollback(&mut self) -> Result<(), BackendError> {
        let result = if self.xa.is_some() {
            let ctx = self.xa.as_ref().unwrap();
            let gtrid = ctx.gtrid.clone();
            let mut first_err: Option<BackendError> = None;
            for (ds, session) in self.pinned.iter_mut() {
                if let Err(e) = session.xa_rollback_branch(&gtrid, ds).await {
                    if first_err.is_none() {
                        first_err = Some(e);
                    }
                }
            }
            match first_err {
                Some(e) => Err(e),
                None => Ok(()),
            }
        } else {
            self.broadcast("ROLLBACK", true).await
        };
        self.explicit_tx = false;
        self.xa = None;
        // 实际失败的连接已在路径内标记 broken，健康连接正常回池
        self.pinned.clear();
        result
    }

    /// 向全部固定连接广播会话级语句（SAVEPOINT 等，不改变事务状态）。
    pub async fn broadcast_to_pinned(&mut self, sql: &str) -> Result<(), BackendError> {
        self.broadcast(sql, false).await
    }

    /// 广播语句；`ends_tx=true`（COMMIT/ROLLBACK）时清除连接的事务标志。
    /// 注意：SAVEPOINT 类语句绝不能清标志，否则下一条写语句会重发 BEGIN，
    /// 触发 MySQL 隐式提交（提交旧事务并丢弃全部 savepoint）。
    async fn broadcast(&mut self, sql: &str, ends_tx: bool) -> Result<(), BackendError> {
        let mut first_err: Option<BackendError> = None;
        for session in self.pinned.values_mut() {
            if let Err(e) = session.tx_command(sql).await {
                // 仅连接级错误销毁连接；SQL 级错误（如 COMMIT 时锁超时）连接保持可用
                if matches!(e, BackendError::Conn(_)) {
                    session.mark_broken();
                }
                if first_err.is_none() {
                    first_err = Some(e);
                }
            }
            if ends_tx {
                session.set_in_transaction(false);
            }
        }
        match first_err {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }

    /// 会话结束：回滚未提交事务并释放代理固定连接。
    pub async fn close(&mut self) {
        if let Some(mut conn) = self.proxy_take() {
            if conn.in_transaction() {
                let _ = conn.tx_command("ROLLBACK").await;
                conn.set_in_transaction(false);
            }
            // 代理会话的会话级状态（SET/USE/临时表/@变量）不应泄漏给下一个客户端：
            // 断开连接而非回池（安全优先；代理模式连接churn可接受）
            conn.mark_broken();
        }
        let dirty = self.explicit_tx
            || self.xa.is_some()
            || self.pinned.values().any(|c| c.in_transaction());
        if dirty {
            // 无 WAL 决策（未到提交阶段）→ 回滚安全
            let _ = self.rollback_without_log().await;
        }
        self.xa = None;
        self.pinned.clear();
    }

    /// 无日志回滚（会话关闭路径：XA 未提交事务直接回滚，不写 WAL）。
    async fn rollback_without_log(&mut self) -> Result<(), BackendError> {
        if self.xa.is_some() {
            let ctx = self.xa.as_ref().unwrap();
            let gtrid = ctx.gtrid.clone();
            for (ds, session) in self.pinned.iter_mut() {
                let _ = session.xa_rollback_branch(&gtrid, ds).await;
            }
        } else {
            let _ = self.broadcast("ROLLBACK", true).await;
        }
        Ok(())
    }
}

impl Default for TransactionSession {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_statement_state_release() {
        // autocommit=true 且非显式事务 → close_statement_state 清空
        let mut tx = TransactionSession::new();
        assert_eq!(tx.pinned_count(), 0);
        tx.close_statement_state(true);
        assert_eq!(tx.pinned_count(), 0);
        // 显式事务开启后 close 不释放（无连接可测，仅验证标志语义）
        tx.explicit_tx = true;
        tx.close_statement_state(true);
        assert!(tx.explicit_tx);
    }
}
