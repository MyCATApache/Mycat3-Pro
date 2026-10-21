//! SQL Server connection pool (semaphore + idle stack + ping window), mirroring
//! the PostgreSQL pool.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use tokio::sync::{mpsc, OwnedSemaphorePermit, Semaphore};

use crate::config::MssqlEndpoint;
use crate::conn::{MssqlColumn, MssqlConnection, MssqlStreamEvent};
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

/// A pull-based row stream backed by a bounded channel.
pub struct MssqlRowStream {
    rx: mpsc::Receiver<MssqlStreamEvent>,
    header: Option<Vec<MssqlColumn>>,
    pending_rows: std::collections::VecDeque<Vec<Option<String>>>,
    done: bool,
}

impl MssqlRowStream {
    /// Wait for (and return) the first result set's column metadata.
    pub async fn ready(&mut self) -> MssqlResult<&[MssqlColumn]> {
        if self.header.is_none() {
            loop {
                match self.rx.recv().await {
                    Some(MssqlStreamEvent::Columns(cols)) => {
                        self.header = Some(cols);
                        break;
                    }
                    Some(MssqlStreamEvent::Row(row)) => self.pending_rows.push_back(row),
                    Some(MssqlStreamEvent::Error(e)) => return Err(e),
                    None => {
                        self.done = true;
                        return Err(MssqlError::Conn(
                            "SQL Server stream ended before column metadata".into(),
                        ));
                    }
                }
            }
        }
        Ok(self.header.as_deref().unwrap_or_default())
    }

    /// Fetch the next row (`None` = clean end of stream).
    pub async fn next_row(&mut self) -> MssqlResult<Option<Vec<Option<String>>>> {
        if let Some(row) = self.pending_rows.pop_front() {
            return Ok(Some(row));
        }
        if self.done {
            return Ok(None);
        }
        loop {
            match self.rx.recv().await {
                Some(MssqlStreamEvent::Row(row)) => return Ok(Some(row)),
                Some(MssqlStreamEvent::Columns(_)) => continue,
                Some(MssqlStreamEvent::Error(e)) => return Err(e),
                None => {
                    self.done = true;
                    return Ok(None);
                }
            }
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

    /// Start a streaming SELECT. Rows are decoded by a background task into a
    /// bounded channel, which applies backpressure end-to-end. The checked-out
    /// connection is owned by that task and is returned/discarded when it ends.
    pub async fn stream_query(
        self: &Arc<Self>,
        sql: String,
        timeout: Option<Duration>,
        capacity: usize,
    ) -> MssqlResult<MssqlRowStream> {
        let mut checked = self.acquire().await?;
        let (tx, rx) = mpsc::channel(capacity.max(1));
        tokio::spawn(async move {
            let r = checked.conn().stream_rows_to(&sql, timeout, tx).await;
            match r {
                Ok(true) => {}
                Ok(false) => checked.mark_broken(),
                Err(e) => {
                    if e.is_conn_level() {
                        checked.mark_broken();
                    }
                }
            }
        });
        Ok(MssqlRowStream {
            rx,
            header: None,
            pending_rows: std::collections::VecDeque::new(),
            done: false,
        })
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::conn::MssqlColumn;
    use mycat3_protocol::consts::field_type;

    #[tokio::test]
    async fn stream_ready_buffers_early_row_then_ends() {
        let (tx, rx) = mpsc::channel(4);
        let cols = vec![MssqlColumn {
            name: "id".into(),
            mysql_type: field_type::LONGLONG,
        }];
        tx.send(MssqlStreamEvent::Columns(cols.clone()))
            .await
            .unwrap();
        tx.send(MssqlStreamEvent::Row(vec![Some("1".into())]))
            .await
            .unwrap();
        drop(tx);
        let mut stream = MssqlRowStream {
            rx,
            header: None,
            pending_rows: std::collections::VecDeque::new(),
            done: false,
        };
        assert_eq!(stream.ready().await.unwrap()[0].name, "id");
        assert_eq!(
            stream.next_row().await.unwrap().unwrap()[0].as_deref(),
            Some("1")
        );
        assert!(stream.next_row().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn stream_ready_surfaces_error() {
        let (tx, rx) = mpsc::channel(2);
        tx.send(MssqlStreamEvent::Error(MssqlError::Timeout("x".into())))
            .await
            .unwrap();
        drop(tx);
        let mut stream = MssqlRowStream {
            rx,
            header: None,
            pending_rows: std::collections::VecDeque::new(),
            done: false,
        };
        assert!(stream.ready().await.is_err());
    }
}
