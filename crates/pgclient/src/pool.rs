//! PG 后端连接池（与 MySQL client 池同构：信号量上限 + 空闲栈 + 免 ping 窗口）。

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::conn::{PgConnection, PgError, PgResult};

/// 复用空闲连接免 ping 的空闲窗口（与 MySQL 池一致）。
pub const PING_AFTER_IDLE: Duration = Duration::from_secs(1);

/// PG 后端连接端点。
#[derive(Debug, Clone)]
pub struct PgEndpoint {
    pub host: String,
    pub port: u16,
    pub dbname: String,
    pub user: String,
    pub password: String,
    pub connect_timeout: Duration,
}

impl PgEndpoint {
    pub fn pg_config(&self) -> tokio_postgres::Config {
        let mut c = tokio_postgres::Config::new();
        c.host(&self.host)
            .port(self.port)
            .dbname(&self.dbname)
            .user(&self.user)
            .password(&self.password)
            .connect_timeout(self.connect_timeout);
        c
    }
}

/// 解析 JDBC URL：`jdbc:postgresql://host:port/dbname`（port 缺省 5432）。
pub fn parse_pg_jdbc_url(url: &str, user: &str, password: &str) -> PgResult<PgEndpoint> {
    let mut rest = url;
    if let Some(idx) = rest.find("://") {
        rest = &rest[idx + 3..];
    }
    for prefix in [
        "jdbc:postgresql:",
        "jdbc:pgsql:",
        "postgresql://",
        "pgsql://",
    ] {
        if rest.starts_with(prefix) {
            rest = &rest[prefix.len()..];
            break;
        }
    }
    let rest = rest.trim_start_matches('/');
    // host[:port][/dbname]
    let (host_port, dbname) = match rest.split_once('/') {
        Some((hp, db)) => (hp, db.to_string()),
        None => (rest, String::new()),
    };
    let (host, port) = match host_port.rsplit_once(':') {
        Some((h, p)) => (
            h.to_string(),
            p.parse::<u16>()
                .map_err(|_| PgError::Conn(format!("bad port in url: {}", url)))?,
        ),
        None => (host_port.to_string(), 5432u16),
    };
    if host.is_empty() {
        return Err(PgError::Conn(format!("bad postgres url: {}", url)));
    }
    Ok(PgEndpoint {
        host,
        port,
        dbname,
        user: user.to_string(),
        password: password.to_string(),
        connect_timeout: Duration::from_secs(5),
    })
}

/// 池化连接（Drop 归还）。
pub struct CheckedOutPgConn {
    conn: Option<PgConnection>,
    pool: Arc<PgDataSourcePool>,
    _permit: OwnedSemaphorePermit,
    broken: bool,
}

impl CheckedOutPgConn {
    pub fn conn(&mut self) -> &mut PgConnection {
        self.conn.as_mut().expect("pg connection taken")
    }
    pub fn mark_broken(&mut self) {
        self.broken = true;
    }
}

impl Drop for CheckedOutPgConn {
    fn drop(&mut self) {
        if let Some(conn) = self.conn.take() {
            self.pool.return_conn(conn, self.broken);
        }
    }
}

struct IdleConn {
    conn: PgConnection,
    returned_at: Instant,
}

/// 单数据源 PG 连接池。
pub struct PgDataSourcePool {
    pub name: String,
    endpoint: PgEndpoint,
    max_con: usize,
    min_con: usize,
    idle_timeout: Duration,
    init_sqls: Vec<String>,
    idle: Mutex<Vec<IdleConn>>,
    permits: Arc<Semaphore>,
    created: AtomicU64,
}

impl PgDataSourcePool {
    pub fn new(
        name: &str,
        endpoint: PgEndpoint,
        max_con: usize,
        min_con: usize,
        idle_timeout: Duration,
        init_sqls: Vec<String>,
    ) -> Arc<Self> {
        let max_con = max_con.max(1);
        Arc::new(PgDataSourcePool {
            name: name.to_string(),
            endpoint,
            max_con,
            min_con: min_con.min(max_con),
            idle_timeout,
            init_sqls,
            idle: Mutex::new(Vec::new()),
            permits: Arc::new(Semaphore::new(max_con)),
            created: AtomicU64::new(0),
        })
    }

    /// 预热 minCon 条空闲连接（失败不影响按需建连）。
    pub async fn prewarm(self: &Arc<Self>) {
        for _ in 0..self.min_con {
            match self.acquire().await {
                Ok(c) => drop(c),
                Err(e) => {
                    tracing::warn!(pool = %self.name, error = %e, "PostgreSQL pool prewarm failed");
                    break;
                }
            }
        }
    }

    /// 取出一条连接：空闲窗口内免 ping 复用；超窗 ping 校验；否则新建。
    pub async fn acquire(self: &Arc<Self>) -> PgResult<CheckedOutPgConn> {
        let permit = self
            .permits
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| PgError::Conn("pool exhausted".into()))?;
        loop {
            let candidate = {
                let mut idle = self.idle.lock();
                let now = Instant::now();
                let mut candidate = None;
                while let Some(c) = idle.pop() {
                    if now.duration_since(c.returned_at) > self.idle_timeout {
                        continue;
                    }
                    candidate = Some((c.conn, c.returned_at.elapsed() < PING_AFTER_IDLE));
                    break;
                }
                candidate
            };
            let Some((mut conn, fresh)) = candidate else {
                break;
            };
            let alive = fresh || conn.ping().await;
            if alive {
                return Ok(CheckedOutPgConn {
                    conn: Some(conn),
                    pool: self.clone(),
                    _permit: permit,
                    broken: false,
                });
            }
        }
        let endpoint = self.endpoint.clone();
        let mut conn = PgConnection::connect(&endpoint).await?;
        for sql in &self.init_sqls {
            conn.query(sql).await?;
        }
        self.created.fetch_add(1, Ordering::Relaxed);
        Ok(CheckedOutPgConn {
            conn: Some(conn),
            pool: self.clone(),
            _permit: permit,
            broken: false,
        })
    }

    fn return_conn(&self, conn: PgConnection, broken: bool) {
        // 事务未结束的连接不能复用：直接关闭
        if broken || conn.in_transaction {
            return;
        }
        let mut idle = self.idle.lock();
        if idle.len() < self.max_con {
            idle.push(IdleConn {
                conn,
                returned_at: Instant::now(),
            });
        }
    }

    /// 池快照：(空闲, 可用额度, 累计创建)。供 show @@datasource。
    pub fn snapshot(&self) -> (usize, usize, u64) {
        let mut idle = self.idle.lock();
        let now = Instant::now();
        idle.retain(|c| now.duration_since(c.returned_at) <= self.idle_timeout);
        (
            idle.len(),
            self.permits.available_permits(),
            self.created.load(Ordering::Relaxed),
        )
    }
}
