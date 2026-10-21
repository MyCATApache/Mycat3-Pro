//! PostgreSQL 连接与查询（tokio-postgres 薄封装）。

use std::sync::Arc;

use mycat3_protocol::consts::field_type;
use tokio_postgres::types::Type;
use tokio_postgres::{Client, SimpleQueryMessage};

use crate::pool::PgEndpoint;

/// 连接/查询错误。`Db` 为 SQL 级错误（连接可复用）；其余为连接级错误。
#[derive(Debug, thiserror::Error)]
pub enum PgError {
    #[error("postgres error {code}: {message}")]
    Db { code: String, message: String },
    #[error("connection error: {0}")]
    Conn(String),
}

impl PgError {
    /// 是否为连接级错误（连接需重建）。
    pub fn is_conn_level(&self) -> bool {
        matches!(self, PgError::Conn(_))
    }
}

/// tokio-postgres 错误 → PgError（DbError → SQL 级；其余 → 连接级）。
fn classify(e: tokio_postgres::Error) -> PgError {
    if let Some(d) = e.as_db_error() {
        PgError::Db {
            code: d.code().code().to_string(),
            message: d.message().to_string(),
        }
    } else {
        PgError::Conn(e.to_string())
    }
}

pub type PgResult<T> = Result<T, PgError>;

/// 结果列（PG 名称 + MySQL field_type 映射）。
#[derive(Debug, Clone)]
pub struct PgColumn {
    pub name: String,
    pub mysql_type: u8,
}

/// 查询结果。
pub enum PgOutcome {
    Ok {
        affected_rows: u64,
    },
    Rows {
        columns: Vec<PgColumn>,
        rows: Vec<Vec<Option<String>>>,
    },
}

/// PG 类型 OID/枚举 → MySQL field_type。
pub fn map_pg_oid(ty: &Type) -> u8 {
    if *ty == Type::INT2 {
        field_type::SHORT
    } else if *ty == Type::INT4 || *ty == Type::OID {
        field_type::LONG
    } else if *ty == Type::INT8 {
        field_type::LONGLONG
    } else if *ty == Type::FLOAT4 {
        field_type::FLOAT
    } else if *ty == Type::FLOAT8 {
        field_type::DOUBLE
    } else if *ty == Type::NUMERIC {
        field_type::NEWDECIMAL
    } else if *ty == Type::BOOL {
        field_type::TINY
    } else if *ty == Type::BYTEA {
        field_type::BLOB
    } else if *ty == Type::DATE {
        field_type::DATE
    } else if *ty == Type::TIMESTAMP || *ty == Type::TIMESTAMPTZ {
        field_type::DATETIME
    } else if *ty == Type::TIME {
        field_type::TIME
    } else {
        // TEXT/VARCHAR/BPCHAR/NAME/UNKNOWN/JSON/JSONB/UUID/其它
        field_type::VAR_STRING
    }
}

/// PG 连接（含 tokio-postgres Client；连接驱动任务后台运行）。
pub struct PgConnection {
    client: Client,
    /// 列类型缓存：SQL 文本 → 列类型（simple_query 不携带类型，用 prepare 描述一次）
    type_cache: std::collections::HashMap<String, Arc<Vec<Type>>>,
    /// 客户端视角事务状态（BEGIN/PREPARE TRANSACTION → true；COMMIT/ROLLBACK → false）。
    /// 池复用判定用（事务中的连接不可复用）。
    pub in_transaction: bool,
}

/// 列类型缓存上限（超出清空重建；物理 SQL 形状有限，命中率高）。
const TYPE_CACHE_CAP: usize = 1024;

impl PgConnection {
    pub async fn connect(ep: &PgEndpoint) -> PgResult<Self> {
        let cfg = ep.pg_config();
        let (client, connection) = cfg.connect(tokio_postgres::NoTls).await.map_err(classify)?;
        // 连接驱动任务（读写消息循环）
        tokio::spawn(async move {
            let _ = connection.await;
        });
        Ok(PgConnection {
            client,
            type_cache: std::collections::HashMap::new(),
            in_transaction: false,
        })
    }

    /// 执行一条 SQL：SELECT/WITH/SHOW/VALUES → 文本行模型；其余 → batch_execute。
    pub async fn query(&mut self, sql: &str) -> PgResult<PgOutcome> {
        self.track_tx(sql);
        // 全部走 simple_query（文本协议：行值原生 Option<&str> 含 BYTEA 的 \x 文本；
        // CommandComplete 携带影响行数——batch_execute 只回 ()，无法取行数）
        let types = self.column_types(sql).await;
        let msgs = self.client.simple_query(sql).await.map_err(classify)?;
        let mut columns: Vec<PgColumn> = Vec::new();
        let mut rows: Vec<Vec<Option<String>>> = Vec::new();
        let mut affected: u64 = 0;
        for m in msgs {
            match m {
                SimpleQueryMessage::RowDescription(rd) => {
                    columns = rd
                        .iter()
                        .enumerate()
                        .map(|(i, c)| PgColumn {
                            name: c.name().to_string(),
                            mysql_type: types
                                .get(i)
                                .map(map_pg_oid)
                                .unwrap_or(field_type::VAR_STRING),
                        })
                        .collect();
                }
                SimpleQueryMessage::Row(row) => {
                    let mut r = Vec::with_capacity(columns.len());
                    for i in 0..columns.len() {
                        let mut value = row.get(i).map(|s| s.to_string());
                        if types.get(i) == Some(&Type::BOOL) {
                            value = value.map(|v| match v.as_str() {
                                "t" => "1".to_string(),
                                "f" => "0".to_string(),
                                _ => v,
                            });
                        }
                        r.push(value);
                    }
                    rows.push(r);
                }
                SimpleQueryMessage::CommandComplete(n) => {
                    affected = n;
                }
                _ => {}
            }
        }
        if columns.is_empty() {
            Ok(PgOutcome::Ok {
                affected_rows: affected,
            })
        } else {
            Ok(PgOutcome::Rows { columns, rows })
        }
    }

    /// 列类型（缓存）：首次命中用 prepare 描述一次（多一次往返），此后零开销。
    async fn column_types(&mut self, sql: &str) -> Arc<Vec<Type>> {
        if let Some(t) = self.type_cache.get(sql) {
            return t.clone();
        }
        let types = match self.client.prepare(sql).await {
            Ok(stmt) => Arc::new(stmt.columns().iter().map(|c| c.type_().clone()).collect()),
            Err(_) => Arc::new(Vec::new()), // 描述失败（如未知列）：回退全 VAR_STRING
        };
        if self.type_cache.len() >= TYPE_CACHE_CAP {
            self.type_cache.clear();
        }
        self.type_cache.insert(sql.to_string(), types.clone());
        types
    }

    /// 探活。
    pub async fn ping(&mut self) -> bool {
        self.client.simple_query("select 1").await.is_ok()
    }

    /// 客户端侧事务状态跟踪（池复用判定）。
    fn track_tx(&mut self, sql: &str) {
        let head = sql.trim_start();
        let upper = head.get(..16).unwrap_or(head).to_ascii_uppercase();
        if upper.starts_with("BEGIN") {
            self.in_transaction = true;
        } else if upper.starts_with("COMMIT")
            || upper.starts_with("ROLLBACK")
            || upper.starts_with("PREPARE TRANSACTION")
        {
            // PREPARE TRANSACTION 结束当前事务（进入 prepared 状态）
            self.in_transaction = false;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_map_oid() {
        assert_eq!(map_pg_oid(&Type::INT4), field_type::LONG);
        assert_eq!(map_pg_oid(&Type::INT8), field_type::LONGLONG);
        assert_eq!(map_pg_oid(&Type::NUMERIC), field_type::NEWDECIMAL);
        assert_eq!(map_pg_oid(&Type::TEXT), field_type::VAR_STRING);
        assert_eq!(map_pg_oid(&Type::BYTEA), field_type::BLOB);
        assert_eq!(map_pg_oid(&Type::TIMESTAMP), field_type::DATETIME);
    }
}
