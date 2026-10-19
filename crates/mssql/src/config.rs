//! SQL Server connection configuration and JDBC URL parsing.

use std::time::Duration;

use tiberius::{AuthMethod, Config, EncryptionLevel};

use crate::error::{MssqlError, MssqlResult};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EncryptionMode {
    /// Encrypt all traffic (SQL Server default; required by Azure SQL).
    Required,
    /// Encrypt the login only (JDBC `encrypt=false`).
    Off,
    /// No encryption at all.
    NotSupported,
}

#[derive(Debug, Clone)]
pub struct MssqlEndpoint {
    pub host: String,
    pub port: u16,
    pub database: String,
    pub user: String,
    pub password: String,
    pub encryption: EncryptionMode,
    pub trust_server_certificate: bool,
    pub hostname_in_certificate: Option<String>,
    pub connect_timeout: Duration,
    pub application_name: String,
}

impl MssqlEndpoint {
    pub fn config(&self) -> MssqlResult<Config> {
        let mut cfg = Config::new();
        cfg.host(&self.host);
        cfg.port(self.port);
        if !self.database.is_empty() {
            cfg.database(&self.database);
        }
        cfg.authentication(AuthMethod::sql_server(&self.user, &self.password));
        cfg.encryption(match self.encryption {
            EncryptionMode::Required => EncryptionLevel::Required,
            EncryptionMode::Off => EncryptionLevel::Off,
            EncryptionMode::NotSupported => EncryptionLevel::NotSupported,
        });
        if self.trust_server_certificate {
            cfg.trust_cert();
        }
        if let Some(hostname) = &self.hostname_in_certificate {
            cfg.hostname_in_certificate(hostname);
        }
        cfg.application_name(&self.application_name);
        // Mycat enforces its own per-statement timeout; don't let the driver
        // silently impose a shorter/longer one. The handshake still has a bound.
        cfg.command_timeout(None);
        cfg.handshake_timeout(Some(self.connect_timeout));
        Ok(cfg)
    }
}

/// Parse `jdbc:sqlserver://host:1433;databaseName=db;encrypt=true;...`.
///
/// Named instances (`host\\instance`) are rejected for now: they need SQL
/// Browser (UDP 1434) support; use an explicit port instead.
pub fn parse_mssql_jdbc_url(url: &str, user: &str, password: &str) -> MssqlResult<MssqlEndpoint> {
    let mut rest = url.trim();
    for prefix in [
        "jdbc:sqlserver://",
        "jdbc:mssql://",
        "sqlserver://",
        "mssql://",
    ] {
        if rest.len() >= prefix.len() && rest[..prefix.len()].eq_ignore_ascii_case(prefix) {
            rest = &rest[prefix.len()..];
            break;
        }
    }
    if rest.is_empty() {
        return Err(MssqlError::Config(format!("bad SQL Server URL: {}", url)));
    }

    let mut parts = rest.split(';');
    let host_part = parts.next().unwrap_or_default();
    if host_part.contains('\\') {
        return Err(MssqlError::Config(
            "named SQL Server instances are not supported; use host:port and enable TCP".into(),
        ));
    }
    let (host, port) = if let Some((h, p)) = host_part.rsplit_once(':') {
        let port = p
            .parse::<u16>()
            .map_err(|_| MssqlError::Config(format!("bad port in SQL Server URL: {}", url)))?;
        (h.to_string(), port)
    } else {
        (host_part.to_string(), 1433u16)
    };
    if host.is_empty() {
        return Err(MssqlError::Config(format!("bad SQL Server URL: {}", url)));
    }

    let mut database = String::new();
    let mut user_from_url = None;
    let mut password_from_url = None;
    let mut encryption = EncryptionMode::Required;
    let mut trust_cert = false;
    let mut hostname_in_certificate = None;
    let mut connect_timeout = Duration::from_secs(15);
    let mut application_name = "mycat3-pro".to_string();

    for raw in parts {
        let raw = raw.trim();
        if raw.is_empty() {
            continue;
        }
        let (key, value) = match raw.split_once('=') {
            Some((k, v)) => (k.trim().to_ascii_lowercase(), v.trim().to_string()),
            None => (raw.to_ascii_lowercase(), String::new()),
        };
        match key.as_str() {
            "databasename" | "database" | "initialcatalog" => database = value,
            "user" | "username" | "userid" => user_from_url = Some(value),
            "password" => password_from_url = Some(value),
            "encrypt" => {
                encryption = match value.to_ascii_lowercase().as_str() {
                    "true" | "yes" | "1" | "strict" => EncryptionMode::Required,
                    "false" | "no" | "0" => EncryptionMode::Off,
                    "notsupported" => EncryptionMode::NotSupported,
                    other => {
                        return Err(MssqlError::Config(format!(
                            "unsupported encrypt value '{}'",
                            other
                        )))
                    }
                }
            }
            "trustservercertificate" => {
                trust_cert = matches!(value.to_ascii_lowercase().as_str(), "true" | "yes" | "1")
            }
            "hostnameincertificate" | "servercertificate" => {
                if !value.is_empty() {
                    hostname_in_certificate = Some(value)
                }
            }
            "logintimeout" => {
                if let Ok(secs) = value.parse::<u64>() {
                    connect_timeout = Duration::from_secs(secs.max(1).min(300));
                }
            }
            "applicationname" | "application" => {
                if !value.is_empty() {
                    application_name = value
                }
            }
            // Ignore benign JDBC options that do not change wire semantics.
            "sendstringparametersasunicode"
            | "responsebuffering"
            | "selectmethod"
            | "packetsize"
            | "multisubnetfailover" => {}
            _ => {}
        }
    }

    Ok(MssqlEndpoint {
        host,
        port,
        database,
        user: if user.is_empty() {
            user_from_url.unwrap_or_default()
        } else {
            user.to_string()
        },
        password: if password.is_empty() {
            password_from_url.unwrap_or_default()
        } else {
            password.to_string()
        },
        encryption,
        trust_server_certificate: trust_cert,
        hostname_in_certificate,
        connect_timeout,
        application_name,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_basic_url() {
        let e = parse_mssql_jdbc_url(
            "jdbc:sqlserver://sql:1433;databaseName=db1;encrypt=false;trustServerCertificate=true",
            "sa",
            "pw",
        )
        .unwrap();
        assert_eq!(e.host, "sql");
        assert_eq!(e.port, 1433);
        assert_eq!(e.database, "db1");
        assert_eq!(e.encryption, EncryptionMode::Off);
        assert!(e.trust_server_certificate);
        assert_eq!(e.user, "sa");
    }

    #[test]
    fn parses_database_alias_and_timeout() {
        let e = parse_mssql_jdbc_url(
            "jdbc:sqlserver://127.0.0.1;database=dbx;loginTimeout=7",
            "u",
            "p",
        )
        .unwrap();
        assert_eq!(e.database, "dbx");
        assert_eq!(e.connect_timeout, Duration::from_secs(7));
    }

    #[test]
    fn rejects_named_instance() {
        assert!(
            parse_mssql_jdbc_url("jdbc:sqlserver://host\\SQLEXPRESS;databaseName=x", "u", "p")
                .is_err()
        );
    }
}
