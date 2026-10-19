//! SQL Server errors: server (SQL-level) vs connection-level, and MySQL errno mapping.

use thiserror::Error;

#[derive(Debug, Error)]
pub enum MssqlError {
    #[error("sqlserver error {code} (state {state}, class {class}): {message}")]
    Server {
        code: u32,
        state: u8,
        class: u8,
        message: String,
    },
    #[error("connection error: {0}")]
    Conn(String),
    #[error("statement timeout: {0}")]
    Timeout(String),
    #[error("mssql config error: {0}")]
    Config(String),
}

pub type MssqlResult<T> = Result<T, MssqlError>;

impl MssqlError {
    pub fn is_conn_level(&self) -> bool {
        match self {
            MssqlError::Conn(_) | MssqlError::Timeout(_) => true,
            MssqlError::Config(_) => true,
            MssqlError::Server { code, .. } => is_connection_code(*code),
        }
    }

    /// Map common SQL Server errors to the MySQL error surface used by all
    /// Mycat frontends. Unmapped SQL-level errors become 1105/HY000.
    pub fn to_mysql_parts(&self) -> (u16, String, String) {
        match self {
            MssqlError::Conn(m) | MssqlError::Timeout(m) => (1105, "HY000".into(), m.clone()),
            MssqlError::Config(m) => (1105, "HY000".into(), m.clone()),
            MssqlError::Server {
                code,
                state,
                class,
                message,
            } => {
                let (errno, sqlstate) = match *code {
                    2627 | 2601 => (1062, "23000"),     // unique/primary key
                    1205 => (1213, "40001"),            // deadlock victim
                    547 => (1452, "23000"),             // FK violation
                    208 => (1146, "42S02"),             // invalid object
                    229 | 230 => (1142, "42000"),       // permission denied
                    18456 => (1045, "28000"),           // login failed
                    4060 => (1049, "42000"),            // cannot open database
                    8152 | 2628 => (1406, "22001"),     // string/binary truncation
                    245 | 8114 => (1292, "22007"),      // conversion/date
                    207 => (1054, "42S22"),             // invalid column
                    102 | 105 | 156 => (1064, "42000"), // syntax
                    _ => (1105, "HY000"),
                };
                let detail = format!(
                    "SQL Server error {} (state {}, class {}): {}",
                    code, state, class, message
                );
                (errno, sqlstate.to_string(), detail)
            }
        }
    }
}

/// SQL Server error numbers that mean the connection/session is no longer usable.
fn is_connection_code(code: u32) -> bool {
    matches!(
        code,
        233      // connection init / named pipes
            | 4060 // cannot open database at login
            | 18456 // login failed
            | 10053 | 10054 | 10060 // network
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maps_common_errors() {
        let e = MssqlError::Server {
            code: 2627,
            state: 1,
            class: 14,
            message: "duplicate".into(),
        };
        assert_eq!(e.to_mysql_parts().0, 1062);
        let e = MssqlError::Server {
            code: 1205,
            state: 1,
            class: 13,
            message: "deadlock".into(),
        };
        assert_eq!(e.to_mysql_parts().0, 1213);
        assert!(MssqlError::Timeout("x".into()).is_conn_level());
        assert!(!MssqlError::Server {
            code: 2627,
            state: 1,
            class: 14,
            message: "dup".into()
        }
        .is_conn_level());
    }
}
