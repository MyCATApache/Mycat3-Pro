//! SQL Server backend for Mycat3 Pro.
//!
//! The protocol/authentication/TLS/type codecs are provided by `tiberius`;
//! this crate only adds the pieces Mycat needs:
//! - JDBC-style URL parsing and connection configuration;
//! - a Mycat-shaped connection pool (max/min/idle/ping, transaction pinning);
//! - conversion of SQL Server values to the text-row model used by the other
//!   backends;
//! - SQL-level/connection-level error classification and MySQL errno mapping.
//!
//! Deliberately **not** implemented: XA/MSDTC distributed transactions. SQL
//! Server does not speak XA; cross-datasource writes are not atomic and are
//! rejected for `transactionType=xa` at the backend layer.

pub mod config;
pub mod conn;
pub mod error;
pub mod pool;

pub use config::{parse_mssql_jdbc_url, EncryptionMode, MssqlEndpoint};
pub use conn::{MssqlColumn, MssqlConnection, MssqlOutcome, MssqlStreamEvent};
pub use error::{MssqlError, MssqlResult};
pub use pool::{CheckedOutMssqlConn, MssqlPool, MssqlRowStream};
