//! SQL Server connection pool (semaphore + idle stack + ping window), mirroring
//! the PostgreSQL pool.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::config::MssqlEndpoint;
use crate::conn::MssqlConnection;
use crate::error::{MssqlError, MssqlResult};

pub const PING_AFTER_IDLE: Duration = Duration::from_secs(1);
const INIT_TIMEOUT: Duration = Duration::from_secs(30);

pub struct CheckedOutMssqlConn {
    conn: Option<MssqlConnection>,
    pool: Arc<MssqlPool>,
    _permit: OwnedSemaphorePermit,
    broken: bool,
}

impl CheckedOutMssqlConn {
    pub fn conn(&mut self) -> &mut MssqlConnection {
        self.conn.as_mut().expect("mssql connection taken")
    }

    pub fn mark_broken(&mut self) {
        self.broken = true;
    }
}

impl Drop for CheckedOutMssqlConn {
    fn drop(&mut self) {
        if let Some(conn) = self.conn.take() {
            self.pool.return_conn(conn, self.broken);
        }
    }
}

struct IdleConn {
    conn: MssqlConnection,
    returned_at: Instant,
}

pub struct MssqlPool {
    pub name: String,
    endpoint: MssqlEndpoint,
    max_con: usize,
    min_con: usize,
    idle_timeout: Duration,
    init_sqls: Vec<String>,
    idle: Mutex<Vec<IdleConn>>,
    permits: Arc<Semaphore>,
    created: AtomicU64,
}

impl MssqlPool {
    pub fn new(
        name: &str,
        endpoint: MssqlEndpoint,
        max_con: usize,
        min_con: usize,
        idle_timeout: Duration,
        init_sqls: Vec<String>,
    ) -> Arc<Self> {
        let max_con = max_con.max(1);
        Arc::new(MssqlPool {
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

    /// Warm `min_con` connections. Failures are logged by the caller/pool and
    /// do not prevent lazy acquisition.
    pub async fn prewarm(self: &Arc<Self>) {
        for _ in 0..self.min_con {
            match self.acquire().await {
                Ok(c) => drop(c),
                Err(e) => {
                    tracing::warn!(pool = %self.name, error = %e, "SQL Server pool prewarm failed");
                    break;
                }
            }
        }
    }

    pub async fn acquire(self: &Arc<Self>) -> MssqlResult<CheckedOutMssqlConn> {
        let permit = self
            .permits
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| MssqlError::Conn("SQL Server pool closed".into()))?;
        loop {
            let candidate = {
                let mut idle = self.idle.lock();
                let now = Instant::now();
                let mut candidate = None;
                while let Some(c) = idle.pop() {
                    if now.duration_since(c.returned_at) > self.idle_timeout {
                        continue; // expired; drop and try the next idle connection
                    }
                    candidate = Some((c.conn, c.returned_at.elapsed() < PING_AFTER_IDLE));
                    break;
                }
                candidate
            };
            let Some((mut conn, fresh)) = candidate else {
                break;
            };
            if fresh || conn.ping().await {
                return Ok(CheckedOutMssqlConn {
                    conn: Some(conn),
                    pool: self.clone(),
                    _permit: permit,
                    broken: false,
                });
            }
        }

        let mut conn = MssqlConnection::connect(&self.endpoint).await?;
        self.init_connection(&mut conn).await?;
        self.created.fetch_add(1, Ordering::Relaxed);
        Ok(CheckedOutMssqlConn {
            conn: Some(conn),
            pool: self.clone(),
            _permit: permit,
            broken: false,
        })
    }

    fn return_conn(&self, conn: MssqlConnection, broken: bool) {
        if broken || conn.in_transaction {
            // The connection is dropped; SQL Server rolls back an open
            // transaction when the session closes.
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

    /// (idle, available permits, created connections)
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

    /// Execute init SQL on a fresh connection. Kept as a helper for tests and
    /// for callers that create a connection outside acquire.
    pub async fn init_connection(&self, conn: &mut MssqlConnection) -> MssqlResult<()> {
        for sql in &self.init_sqls {
            if sql.trim_start().to_ascii_uppercase().starts_with("SET ") {
                conn.set_forward(sql, Some(INIT_TIMEOUT)).await?;
            } else {
                conn.query(sql, Some(INIT_TIMEOUT)).await?;
            }
        }
        Ok(())
    }
}
