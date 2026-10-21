//! SQL Server connection wrapper and value mapping.
//!
//! `tiberius` owns the TDS protocol; this module owns Mycat semantics:
//! - row-returning vs DML statement dispatch;
//! - transaction state tracking (`BEGIN TRAN` / `COMMIT` / savepoints);
//! - `SET autocommit` translation;
//! - SQL Server `ColumnData` → MySQL text row model.

use std::time::Duration;

use chrono::{FixedOffset, NaiveDate, NaiveDateTime, NaiveTime, TimeZone, Timelike};
use futures_util::TryStreamExt;
use mycat3_protocol::consts::field_type;
use tiberius::{Client, ColumnData, ColumnType, Config, EncryptionLevel, QueryItem, Row};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio_util::compat::{Compat, TokioAsyncWriteCompatExt};
use tracing::debug;

use tiberius::error::Error as TiberiusError;

use crate::config::MssqlEndpoint;
use crate::error::{MssqlError, MssqlResult};

type TcpCompat = Compat<TcpStream>;
type MssqlClient = Client<TcpCompat>;

#[derive(Debug, Clone)]
pub struct MssqlColumn {
    pub name: String,
    pub mysql_type: u8,
}

pub enum MssqlOutcome {
    Ok {
        affected_rows: u64,
        last_insert_id: u64,
    },
    Rows {
        columns: Vec<MssqlColumn>,
        rows: Vec<Vec<Option<String>>>,
    },
}

/// Events emitted by the streaming query task.
pub enum MssqlStreamEvent {
    Columns(Vec<MssqlColumn>),
    Row(Vec<Option<String>>),
    Error(MssqlError),
}

pub struct MssqlConnection {
    client: MssqlClient,
    pub in_transaction: bool,
    database: String,
}

impl MssqlConnection {
    pub async fn connect(ep: &MssqlEndpoint) -> MssqlResult<Self> {
        let mut host = ep.host.clone();
        let mut port = ep.port;
        let mut last_err = None;
        // Azure SQL may return a routing redirect once; follow it like the
        // tiberius README example, but never loop indefinitely.
        for attempt in 0..2 {
            let mut cfg = ep.config()?;
            cfg.host(&host);
            cfg.port(port);
            let tcp = match tokio::time::timeout(
                ep.connect_timeout,
                TcpStream::connect((host.as_str(), port)),
            )
            .await
            {
                Ok(Ok(s)) => s,
                Ok(Err(e)) => {
                    last_err = Some(MssqlError::Conn(format!(
                        "tcp connect {}:{}: {}",
                        host, port, e
                    )));
                    break;
                }
                Err(_) => {
                    last_err = Some(MssqlError::Timeout(format!(
                        "tcp connect {}:{}",
                        host, port
                    )));
                    break;
                }
            };
            let _ = tcp.set_nodelay(true);
            let cfg: Config = cfg;
            match tokio::time::timeout(ep.connect_timeout, Client::connect(cfg, tcp.compat_write()))
                .await
            {
                Ok(Ok(client)) => {
                    debug!(host = %host, port, database = %ep.database, "SQL Server connected");
                    return Ok(MssqlConnection {
                        client,
                        in_transaction: false,
                        database: ep.database.clone(),
                    });
                }
                Ok(Err(TiberiusError::Routing {
                    host: rhost,
                    port: rport,
                })) if attempt == 0 => {
                    debug!(from = %host, to = %rhost, rport, "SQL Server routing redirect");
                    host = rhost;
                    port = rport;
                    continue;
                }
                Ok(Err(e)) => {
                    last_err = Some(tiberius_err(e));
                    break;
                }
                Err(_) => {
                    last_err = Some(MssqlError::Timeout(format!("handshake {}:{}", host, port)));
                    break;
                }
            }
        }
        Err(last_err.unwrap_or_else(|| {
            MssqlError::Conn("SQL Server connection failed without a diagnostic".into())
        }))
    }

    pub fn database(&self) -> &str {
        &self.database
    }

    /// Execute a statement and return either rows or affected rows.
    pub async fn query(
        &mut self,
        sql: &str,
        timeout: Option<Duration>,
    ) -> MssqlResult<MssqlOutcome> {
        let trimmed = sql.trim();
        if trimmed.is_empty() {
            return Ok(MssqlOutcome::Ok {
                affected_rows: 0,
                last_insert_id: 0,
            });
        }
        if first_keyword(trimmed) == "USE" {
            self.execute_batch(trimmed, timeout).await?;
            if let Some(db) = use_database_name(trimmed) {
                self.database = db;
            }
            return Ok(MssqlOutcome::Ok {
                affected_rows: 0,
                last_insert_id: 0,
            });
        }
        if trimmed.to_ascii_uppercase().starts_with("SET AUTOCOMMIT") {
            self.set_forward(trimmed, timeout).await?;
            return Ok(MssqlOutcome::Ok {
                affected_rows: 0,
                last_insert_id: 0,
            });
        }
        if let Some(tx_sql) = self.translate_tx(trimmed)? {
            self.execute_batch(&tx_sql, timeout).await?;
            self.apply_tx_state(trimmed);
            return Ok(MssqlOutcome::Ok {
                affected_rows: 0,
                last_insert_id: 0,
            });
        }

        if is_row_query(trimmed) {
            self.query_rows(trimmed, timeout).await
        } else if is_insert(trimmed) && !contains_output(trimmed) {
            // Append the identity read to the INSERT batch: one round trip
            // instead of INSERT + SELECT SCOPE_IDENTITY(). `@@ROWCOUNT` is
            // evaluated before the SELECT itself changes it.
            let (affected, last_insert_id) =
                self.execute_insert_with_identity(trimmed, timeout).await?;
            Ok(MssqlOutcome::Ok {
                affected_rows: affected,
                last_insert_id,
            })
        } else {
            let affected = self.execute_simple(trimmed, timeout).await?;
            Ok(MssqlOutcome::Ok {
                affected_rows: affected,
                last_insert_id: 0,
            })
        }
    }

    /// Forward a SET statement. MySQL session knobs that do not exist in SQL
    /// Server are translated or explicitly ignored; unknown SET is passed
    /// through so the server can produce its own error.
    pub async fn set_forward(&mut self, sql: &str, timeout: Option<Duration>) -> MssqlResult<()> {
        let trimmed = sql.trim();
        let upper = trimmed.to_ascii_uppercase();
        if upper.starts_with("SET AUTOCOMMIT") {
            let value = upper
                .split('=')
                .nth(1)
                .unwrap_or("")
                .trim()
                .trim_end_matches(';')
                .trim();
            if value == "0" {
                if !self.in_transaction {
                    self.execute_batch("BEGIN TRAN", timeout).await?;
                    self.in_transaction = true;
                }
                return Ok(());
            }
            if value == "1" {
                if self.in_transaction {
                    self.execute_batch("COMMIT", timeout).await?;
                    self.in_transaction = false;
                }
                return Ok(());
            }
            return Err(MssqlError::Server {
                code: 1064,
                state: 1,
                class: 15,
                message: format!("invalid autocommit value in: {}", sql),
            });
        }
        // MySQL-only session settings have no SQL Server equivalent. Ignore
        // them rather than failing every JDBC connection; Unicode is native.
        if upper.starts_with("SET NAMES")
            || upper.starts_with("SET SQL_MODE")
            || upper.starts_with("SET CHARACTER SET")
            || upper.starts_with("SET CHARACTER_SET_")
        {
            return Ok(());
        }
        self.execute_batch(trimmed, timeout).await?;
        Ok(())
    }

    /// Execute a SQL Server batch verbatim (DDL / multi-statement scripts).
    /// Used by DDL automation; unlike `query`, it never applies MySQL→T-SQL
    /// translation (the caller has already produced T-SQL).
    pub async fn execute_script(
        &mut self,
        sql: &str,
        timeout: Option<Duration>,
    ) -> MssqlResult<()> {
        self.execute_batch(sql, timeout).await
    }

    /// Begin/commit/rollback/savepoint translation.
    pub async fn tx_command(&mut self, sql: &str, timeout: Option<Duration>) -> MssqlResult<()> {
        let trimmed = sql.trim();
        if let Some(tx_sql) = self.translate_tx(trimmed)? {
            self.execute_batch(&tx_sql, timeout).await?;
            self.apply_tx_state(trimmed);
            Ok(())
        } else {
            Err(MssqlError::Server {
                code: 1064,
                state: 1,
                class: 15,
                message: format!("unsupported SQL Server transaction command: {}", sql),
            })
        }
    }

    pub async fn ping(&mut self) -> bool {
        matches!(
            self.query_rows("SELECT 1", Some(Duration::from_secs(5)))
                .await,
            Ok(_)
        )
    }

    /// Translate a MySQL transaction statement to T-SQL and update state.
    /// Returns None if the statement is not a transaction command.
    fn translate_tx(&self, sql: &str) -> MssqlResult<Option<String>> {
        let upper = sql.trim().to_ascii_uppercase();
        let compact = upper.split_whitespace().collect::<Vec<_>>();
        if compact.is_empty() {
            return Ok(None);
        }
        let first = compact[0].trim_end_matches(';');
        if first == "BEGIN" || first == "START" {
            if self.in_transaction {
                // MySQL BEGIN inside a transaction implicitly commits; SQL
                // Server nesting would be a different semantic. Stay in the
                // current transaction rather than creating a nested one.
                return Ok(Some("SELECT 1 WHERE 1 = 0".into()));
            }
            return Ok(Some("BEGIN TRAN".into()));
        }
        if first == "COMMIT" {
            if !self.in_transaction {
                return Ok(Some("SELECT 1 WHERE 1 = 0".into()));
            }
            return Ok(Some("COMMIT".into()));
        }
        if first == "ROLLBACK" {
            if compact.iter().any(|x| {
                x.eq_ignore_ascii_case("SAVEPOINT") || x.eq_ignore_ascii_case("TRANSACTION")
            }) {
                let name = savepoint_name(sql);
                return Ok(Some(match name {
                    Some(n) => format!("ROLLBACK TRANSACTION {}", quote_tsql_ident(&n)),
                    None => "ROLLBACK".into(),
                }));
            }
            if !self.in_transaction {
                return Ok(Some("SELECT 1 WHERE 1 = 0".into()));
            }
            return Ok(Some("ROLLBACK".into()));
        }
        if first == "SAVEPOINT" {
            let name = savepoint_name(sql).unwrap_or_else(|| "mycat_sp".into());
            return Ok(Some(format!(
                "SAVE TRANSACTION {}",
                quote_tsql_ident(&name)
            )));
        }
        if first == "RELEASE" {
            return Err(MssqlError::Server {
                code: 1235,
                state: 1,
                class: 16,
                message: "SQL Server does not support RELEASE SAVEPOINT".into(),
            });
        }
        Ok(None)
    }

    fn apply_tx_state(&mut self, sql: &str) {
        let upper = sql.trim().to_ascii_uppercase();
        let compact = upper.split_whitespace().collect::<Vec<_>>();
        let Some(first) = compact.first() else { return };
        let first = first.trim_end_matches(';');
        if first == "BEGIN" || first == "START" {
            self.in_transaction = true;
        } else if first == "COMMIT" {
            self.in_transaction = false;
        } else if first == "ROLLBACK"
            && !compact.iter().any(|x| {
                x.eq_ignore_ascii_case("SAVEPOINT") || x.eq_ignore_ascii_case("TRANSACTION")
            })
        {
            self.in_transaction = false;
        }
    }

    /// Execute a SQL batch (not sp_executesql). Transaction control must use a
    /// batch: BEGIN TRAN inside sp_executesql would end with a mismatched
    /// transaction count (SQL Server error 266).
    async fn execute_batch(&mut self, sql: &str, timeout: Option<Duration>) -> MssqlResult<()> {
        let client = &mut self.client;
        let fut = async {
            let stream = client.simple_query(sql).await.map_err(tiberius_err)?;
            let _ = stream.into_results().await.map_err(tiberius_err)?;
            Ok::<(), MssqlError>(())
        };
        match timeout {
            Some(t) => match tokio::time::timeout(t, fut).await {
                Ok(r) => r,
                Err(_) => Err(MssqlError::Timeout(sql.to_string())),
            },
            None => fut.await,
        }
    }

    async fn execute_simple(&mut self, sql: &str, timeout: Option<Duration>) -> MssqlResult<u64> {
        let client = &mut self.client;
        let fut = async {
            let result = client.execute(sql, &[]).await.map_err(tiberius_err)?;
            Ok::<u64, MssqlError>(result.total())
        };
        match timeout {
            Some(t) => match tokio::time::timeout(t, fut).await {
                Ok(r) => r,
                Err(_) => Err(MssqlError::Timeout(sql.to_string())),
            },
            None => fut.await,
        }
    }

    async fn query_rows(
        &mut self,
        sql: &str,
        timeout: Option<Duration>,
    ) -> MssqlResult<MssqlOutcome> {
        let client = &mut self.client;
        let fut = async {
            let mut stream = client.simple_query(sql).await.map_err(tiberius_err)?;
            // Decode each row as it arrives instead of `into_results()`: that
            // would keep all tiberius `Row` values alive while the text-row
            // copy is built, doubling peak memory for large result sets.
            let mut cols: Vec<MssqlColumn> = Vec::new();
            let mut out = Vec::new();
            let mut result_sets = 0usize;
            while let Some(item) = stream.try_next().await.map_err(tiberius_err)? {
                match item {
                    QueryItem::Metadata(meta) => {
                        result_sets += 1;
                        if result_sets == 1 {
                            cols = meta
                                .columns()
                                .iter()
                                .map(|c| MssqlColumn {
                                    name: c.name().to_string(),
                                    mysql_type: map_column_type(c.column_type()),
                                })
                                .collect();
                        }
                    }
                    // Match the old `into_results().next()` semantics: the
                    // frontends only consume the first result set.
                    QueryItem::Row(row) if result_sets <= 1 => out.push(render_row(&row)?),
                    QueryItem::Row(_) => {}
                }
            }
            if cols.is_empty() {
                return Ok(MssqlOutcome::Ok {
                    affected_rows: 0,
                    last_insert_id: 0,
                });
            }
            Ok(MssqlOutcome::Rows {
                columns: cols,
                rows: out,
            })
        };
        match timeout {
            Some(t) => match tokio::time::timeout(t, fut).await {
                Ok(r) => r,
                Err(_) => Err(MssqlError::Timeout(sql.to_string())),
            },
            None => fut.await,
        }
    }

    /// Execute an INSERT and read `@@ROWCOUNT` + `SCOPE_IDENTITY()` from the
    /// same TDS batch. The trailing SELECT is part of the same round trip, so
    /// the identity lookup adds no extra network latency.
    async fn execute_insert_with_identity(
        &mut self,
        sql: &str,
        timeout: Option<Duration>,
    ) -> MssqlResult<(u64, u64)> {
        let batch = format!(
            "{};\nSELECT CAST(@@ROWCOUNT AS BIGINT) AS __mycat_affected, CAST(SCOPE_IDENTITY() AS BIGINT) AS __mycat_identity",
            sql.trim().trim_end_matches(';')
        );
        let client = &mut self.client;
        let fut = async {
            let stream = client.simple_query(&batch).await.map_err(tiberius_err)?;
            let results = stream.into_results().await.map_err(tiberius_err)?;
            // The last result set is our SELECT, after any trigger results.
            let row = results
                .into_iter()
                .rev()
                .find_map(|rows| rows.into_iter().next());
            let (affected, identity) = match row {
                Some(row) => (
                    row.get::<i64, _>(0).unwrap_or(0).max(0) as u64,
                    row.get::<i64, _>(1)
                        .and_then(|n| u64::try_from(n).ok())
                        .unwrap_or(0),
                ),
                None => (0, 0),
            };
            Ok::<(u64, u64), MssqlError>((affected, identity))
        };
        match timeout {
            Some(t) => match tokio::time::timeout(t, fut).await {
                Ok(r) => r,
                Err(_) => Err(MssqlError::Timeout(sql.to_string())),
            },
            None => fut.await,
        }
    }

    /// Stream the first result set to a bounded channel.
    ///
    /// Returns `Ok(true)` when the stream was fully drained (the connection is
    /// reusable), `Ok(false)` when the consumer disappeared or the timeout hit
    /// (the caller must discard the connection). On protocol/SQL errors an
    /// `Error` event is emitted and `Err` is returned.
    pub(crate) async fn stream_rows_to(
        &mut self,
        sql: &str,
        timeout: Option<Duration>,
        tx: mpsc::Sender<MssqlStreamEvent>,
    ) -> MssqlResult<bool> {
        let client = &mut self.client;
        let fut = async {
            let mut stream = client.simple_query(sql).await.map_err(tiberius_err)?;
            let mut result_sets = 0usize;
            while let Some(item) = stream.try_next().await.map_err(tiberius_err)? {
                match item {
                    QueryItem::Metadata(meta) => {
                        result_sets += 1;
                        if result_sets == 1 {
                            let cols: Vec<MssqlColumn> = meta
                                .columns()
                                .iter()
                                .map(|c| MssqlColumn {
                                    name: c.name().to_string(),
                                    mysql_type: map_column_type(c.column_type()),
                                })
                                .collect();
                            if tx.send(MssqlStreamEvent::Columns(cols)).await.is_err() {
                                return Ok(false);
                            }
                        }
                    }
                    QueryItem::Row(row) if result_sets <= 1 => {
                        let values = render_row(&row)?;
                        if tx.send(MssqlStreamEvent::Row(values)).await.is_err() {
                            return Ok(false);
                        }
                    }
                    QueryItem::Row(_) => {}
                }
            }
            Ok(true)
        };
        let result = match timeout {
            Some(t) => match tokio::time::timeout(t, fut).await {
                Ok(r) => r,
                Err(_) => Err(MssqlError::Timeout(sql.to_string())),
            },
            None => fut.await,
        };
        match result {
            Ok(done) => Ok(done),
            Err(e) => {
                let _ = tx.send(MssqlStreamEvent::Error(e)).await;
                Ok(false)
            }
        }
    }

    /// Secondary redo lag for an Always On readable secondary (seconds).
    ///
    /// Returns `None` when the server is a primary, has no AG database, or the
    /// account lacks `VIEW SERVER STATE`; callers treat that as "unknown" and
    /// keep the replica eligible rather than removing it from the read pool.
    pub async fn secondary_lag_seconds(&mut self, timeout: Duration) -> Option<u64> {
        let sql = "SELECT ISNULL(MAX(secondary_lag_seconds), 0) AS lag_seconds \
                   FROM sys.dm_hadr_database_replica_states \
                   WHERE is_local = 1 AND is_primary_replica = 0";
        let outcome = self.query_rows(sql, Some(timeout)).await.ok()?;
        let MssqlOutcome::Rows { rows, .. } = outcome else {
            return None;
        };
        rows.first()?
            .first()?
            .as_deref()?
            .parse::<i64>()
            .ok()
            .and_then(|n| u64::try_from(n).ok())
    }

    #[allow(dead_code)]
    async fn scope_identity(&mut self, timeout: Option<Duration>) -> Option<u64> {
        let client = &mut self.client;
        let fut = async {
            let stream = client
                .simple_query("SELECT CAST(SCOPE_IDENTITY() AS BIGINT) AS id")
                .await
                .ok()?;
            let results = stream.into_results().await.ok()?;
            let row = results.into_iter().next()?.into_iter().next()?;
            let v: Option<i64> = row.get(0);
            v.and_then(|n| u64::try_from(n).ok())
        };
        match timeout {
            Some(t) => tokio::time::timeout(t, fut).await.ok().flatten(),
            None => fut.await,
        }
    }
}

fn tiberius_err(e: TiberiusError) -> MssqlError {
    match e {
        TiberiusError::Server(inner) => MssqlError::Server {
            code: inner.code(),
            state: inner.state(),
            class: inner.class(),
            message: inner.message().to_string(),
        },
        TiberiusError::Routing { host, port } => {
            MssqlError::Conn(format!("SQL Server requested routing to {}:{}", host, port))
        }
        other => MssqlError::Conn(other.to_string()),
    }
}

pub fn map_column_type(t: ColumnType) -> u8 {
    use ColumnType::*;
    match t {
        Bit | Int1 => field_type::TINY,
        Int2 => field_type::SHORT,
        Int4 => field_type::LONG,
        Int8 => field_type::LONGLONG,
        Float4 => field_type::FLOAT,
        Float8 => field_type::DOUBLE,
        Money | Money4 | Decimaln | Numericn => field_type::NEWDECIMAL,
        Datetime4 | Datetime | Datetimen | Datetime2 => field_type::DATETIME,
        Daten => field_type::DATE,
        Timen => field_type::TIME,
        BigVarBin | BigBinary | Image => field_type::BLOB,
        BigVarChar | BigChar | NVarchar | NChar | Text | NText | Xml | Udt | SSVariant | Null => {
            field_type::VAR_STRING
        }
        DatetimeOffsetn | Guid => field_type::VAR_STRING,
        Intn | Bitn | Floatn => field_type::VAR_STRING,
    }
}

fn render_row(row: &Row) -> MssqlResult<Vec<Option<String>>> {
    let mut out = Vec::with_capacity(row.len());
    for (_, value) in row.cells() {
        out.push(render_value(value)?);
    }
    Ok(out)
}

fn render_value(v: &ColumnData<'_>) -> MssqlResult<Option<String>> {
    Ok(match v {
        ColumnData::U8(v) => v.map(|v| v.to_string()),
        ColumnData::I16(v) => v.map(|v| v.to_string()),
        ColumnData::I32(v) => v.map(|v| v.to_string()),
        ColumnData::I64(v) => v.map(|v| v.to_string()),
        ColumnData::F32(v) => v.map(|v| v.to_string()),
        ColumnData::F64(v) => v.map(|v| v.to_string()),
        ColumnData::Bit(v) => v.map(|v| if v { "1" } else { "0" }.to_string()),
        ColumnData::String(v) => v.as_ref().map(|v| v.to_string()),
        ColumnData::Guid(v) => v.map(|v| v.to_string()),
        ColumnData::Binary(v) => v.as_ref().map(|v| hex_encode(v)),
        ColumnData::Numeric(v) => v.map(|v| v.to_string()),
        ColumnData::Xml(v) => v.as_ref().map(|v| v.to_string()),
        ColumnData::DateTime(v) => v.map(format_datetime),
        ColumnData::SmallDateTime(v) => v.map(format_smalldatetime),
        ColumnData::Time(v) => v.map(format_time),
        ColumnData::Date(v) => v.map(format_date),
        ColumnData::DateTime2(v) => v.map(format_datetime2),
        ColumnData::DateTimeOffset(v) => v.map(format_datetimeoffset),
    })
}

fn format_datetime(v: tiberius::time::DateTime) -> String {
    let base = NaiveDate::from_ymd_opt(1900, 1, 1)
        .unwrap()
        .and_hms_opt(0, 0, 0)
        .unwrap();
    let dt = base
        + chrono::Duration::days(v.days() as i64)
        + chrono::Duration::nanoseconds(v.seconds_fragments() as i64 * 1_000_000_000 / 300);
    fmt_datetime(dt)
}

fn format_smalldatetime(v: tiberius::time::SmallDateTime) -> String {
    let base = NaiveDate::from_ymd_opt(1900, 1, 1)
        .unwrap()
        .and_hms_opt(0, 0, 0)
        .unwrap();
    let dt = base
        + chrono::Duration::days(v.days() as i64)
        + chrono::Duration::seconds(v.seconds_fragments() as i64 * 60);
    fmt_datetime(dt)
}

fn format_date(v: tiberius::time::Date) -> String {
    let base = NaiveDate::from_ymd_opt(1, 1, 1).unwrap();
    let dt = base + chrono::Duration::days(v.days() as i64);
    dt.format("%Y-%m-%d").to_string()
}

fn format_time(v: tiberius::time::Time) -> String {
    let ns = time_nanos(v.increments(), v.scale());
    let t = NaiveTime::from_hms_opt(0, 0, 0).unwrap() + chrono::Duration::nanoseconds(ns);
    fmt_time(t)
}

fn format_datetime2(v: tiberius::time::DateTime2) -> String {
    let date =
        NaiveDate::from_ymd_opt(1, 1, 1).unwrap() + chrono::Duration::days(v.date().days() as i64);
    let t = NaiveTime::from_hms_opt(0, 0, 0).unwrap()
        + chrono::Duration::nanoseconds(time_nanos(v.time().increments(), v.time().scale()));
    fmt_datetime(NaiveDateTime::new(date, t))
}

fn format_datetimeoffset(v: tiberius::time::DateTimeOffset) -> String {
    let dt2 = v.datetime2();
    let date = NaiveDate::from_ymd_opt(1, 1, 1).unwrap()
        + chrono::Duration::days(dt2.date().days() as i64);
    let t = NaiveTime::from_hms_opt(0, 0, 0).unwrap()
        + chrono::Duration::nanoseconds(time_nanos(dt2.time().increments(), dt2.time().scale()));
    let offset = FixedOffset::east_opt(v.offset() as i32 * 60)
        .unwrap_or_else(|| FixedOffset::east_opt(0).unwrap());
    dt2_to_string(NaiveDateTime::new(date, t), offset)
}

fn fmt_datetime(dt: NaiveDateTime) -> String {
    if dt.nanosecond() == 0 {
        dt.format("%Y-%m-%d %H:%M:%S").to_string()
    } else {
        dt.format("%Y-%m-%d %H:%M:%S%.f").to_string()
    }
}

fn fmt_time(t: NaiveTime) -> String {
    if t.nanosecond() == 0 {
        t.format("%H:%M:%S").to_string()
    } else {
        t.format("%H:%M:%S%.f").to_string()
    }
}

fn dt2_to_string(dt: NaiveDateTime, offset: FixedOffset) -> String {
    let local = offset.from_local_datetime(&dt).single().unwrap_or_else(|| {
        // Ambiguous/nonexistent local time: fall back to a fixed offset; this
        // keeps the original wall-clock value instead of silently shifting it.
        offset
            .from_local_datetime(&dt)
            .earliest()
            .unwrap_or_else(|| {
                chrono::DateTime::<FixedOffset>::from_naive_utc_and_offset(dt, offset)
            })
    });
    if dt.nanosecond() == 0 {
        local.format("%Y-%m-%d %H:%M:%S%:z").to_string()
    } else {
        local.format("%Y-%m-%d %H:%M:%S%.f%:z").to_string()
    }
}

fn time_nanos(increments: u64, scale: u8) -> i64 {
    let pow = 9u32.saturating_sub(scale as u32);
    increments
        .saturating_mul(10u64.saturating_pow(pow))
        .min(i64::MAX as u64) as i64
}

fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut s = String::with_capacity(bytes.len() * 2 + 2);
    s.push_str("\\x");
    for b in bytes {
        s.push(HEX[(b >> 4) as usize] as char);
        s.push(HEX[(b & 0x0f) as usize] as char);
    }
    s
}

fn is_row_query(sql: &str) -> bool {
    let upper = sql.to_ascii_uppercase();
    let first = first_keyword(sql);
    if matches!(first.as_str(), "INSERT" | "UPDATE" | "DELETE") && contains_output(sql) {
        return true;
    }
    match first.as_str() {
        "SELECT" | "VALUES" | "EXEC" | "EXECUTE" => true,
        "WITH" => {
            // A CTE can front a DML statement; those use execute() to get
            // affected rows. A token scan is intentionally conservative.
            let words = upper.split(|c: char| !c.is_ascii_alphanumeric() && c != '_');
            !words
                .into_iter()
                .any(|w| matches!(w, "INSERT" | "UPDATE" | "DELETE" | "MERGE"))
        }
        _ => false,
    }
}

fn is_insert(sql: &str) -> bool {
    first_keyword(sql) == "INSERT"
}

fn contains_output(sql: &str) -> bool {
    sql.split(|c: char| !c.is_ascii_alphanumeric() && c != '_')
        .any(|w| w.eq_ignore_ascii_case("OUTPUT"))
}

fn first_keyword(sql: &str) -> String {
    let mut s = sql.trim_start();
    loop {
        if s.starts_with("--") {
            match s.find('\n') {
                Some(i) => s = s[i + 1..].trim_start(),
                None => return String::new(),
            }
        } else if s.starts_with("/*") {
            match s.find("*/") {
                Some(i) => s = s[i + 2..].trim_start(),
                None => return String::new(),
            }
        } else {
            break;
        }
    }
    s.split(|c: char| !c.is_ascii_alphanumeric() && c != '_')
        .next()
        .unwrap_or("")
        .to_ascii_uppercase()
}

fn savepoint_name(sql: &str) -> Option<String> {
    let tokens: Vec<&str> = sql.split_whitespace().collect();
    let idx = tokens
        .iter()
        .rposition(|t| t.eq_ignore_ascii_case("SAVEPOINT") || t.eq_ignore_ascii_case("TRANSACTION"))
        .or_else(|| tokens.iter().position(|t| t.eq_ignore_ascii_case("TO")))?;
    let raw = tokens
        .get(idx + 1)?
        .trim_matches(|c: char| c == ';' || c == '`' || c == '"' || c == '[' || c == ']');
    if raw.is_empty() {
        None
    } else {
        Some(raw.to_string())
    }
}

fn use_database_name(sql: &str) -> Option<String> {
    let rest = sql.trim_start()[3..].trim();
    let name = rest
        .trim_end_matches(';')
        .trim()
        .trim_matches(|c: char| c == '[' || c == ']' || c == '"' || c == '`')
        .to_string();
    if name.is_empty() {
        None
    } else {
        Some(name)
    }
}

fn quote_tsql_ident(name: &str) -> String {
    format!("[{}]", name.replace(']', "]]"))
}

/// SQL Server maps `getdate`-style functions; this helper is public for tests.
pub fn encryption_level(mode: crate::config::EncryptionMode) -> EncryptionLevel {
    match mode {
        crate::config::EncryptionMode::Required => EncryptionLevel::Required,
        crate::config::EncryptionMode::Off => EncryptionLevel::Off,
        crate::config::EncryptionMode::NotSupported => EncryptionLevel::NotSupported,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keywords_and_classification() {
        assert!(is_row_query(" /*x*/ SELECT 1"));
        assert!(is_row_query("WITH x AS (SELECT 1) SELECT * FROM x"));
        assert!(!is_row_query("INSERT INTO t VALUES (1)"));
        assert!(!is_row_query("WITH x AS (SELECT 1) UPDATE t SET a=1"));
        assert_eq!(first_keyword("-- c\nselect 1"), "SELECT");
    }

    #[test]
    fn type_mapping() {
        assert_eq!(
            map_column_type(ColumnType::Int4),
            mycat3_protocol::consts::field_type::LONG
        );
        assert_eq!(
            map_column_type(ColumnType::Guid),
            mycat3_protocol::consts::field_type::VAR_STRING
        );
        assert_eq!(
            map_column_type(ColumnType::Daten),
            mycat3_protocol::consts::field_type::DATE
        );
        assert_eq!(
            map_column_type(ColumnType::Datetime2),
            mycat3_protocol::consts::field_type::DATETIME
        );
        assert_eq!(
            map_column_type(ColumnType::BigVarBin),
            mycat3_protocol::consts::field_type::BLOB
        );
    }

    #[test]
    fn date_formatting() {
        // 2026-10-16 12:34:56
        let dt = NaiveDate::from_ymd_opt(2026, 10, 16)
            .unwrap()
            .and_hms_opt(12, 34, 56)
            .unwrap();
        assert_eq!(fmt_datetime(dt), "2026-10-16 12:34:56");
        assert_eq!(
            fmt_time(NaiveTime::from_hms_opt(1, 2, 3).unwrap()),
            "01:02:03"
        );
    }

    #[test]
    fn savepoint_parsing() {
        assert_eq!(savepoint_name("SAVEPOINT sp1").as_deref(), Some("sp1"));
        assert_eq!(
            savepoint_name("ROLLBACK TO SAVEPOINT sp2").as_deref(),
            Some("sp2")
        );
    }
}
