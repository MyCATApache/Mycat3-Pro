//! Mycat3 Pro PostgreSQL 后端客户端（基于官方生态 tokio-postgres）。
//!
//! 设计：
//! - 连接/认证/协议编解码完全交给 tokio-postgres（协议层零风险，"100% 兼容"的
//!   基础），本 crate 只做薄封装：连接管理、阻塞式连接池（与 MySQL client 池同构）、
//!   结果模型化（文本行模型 → MySQL field_type 映射，供归并/三前端复用）；
//! - 结果解码：PG 简单查询走文本协议，任意类型的 raw 字节即其文本表示
//!   （BYTEA 为 `\x..` ASCII），故用自定义 `PgText` 全类型解码，规避
//!   postgres-types 对 String 的类型白名单限制；
//! - 事务：原始 SQL `BEGIN/COMMIT/ROLLBACK/SAVEPOINT` 下推（与 MySQL 后端语义一致）；
//!   2PC：`PREPARE TRANSACTION 'gid'` / `COMMIT PREPARED` / `ROLLBACK PREPARED`。

pub mod conn;
pub mod pool;

pub use conn::{map_pg_oid, PgColumn, PgConnection, PgError, PgOutcome, PgResult};
pub use pool::{parse_pg_jdbc_url, CheckedOutPgConn, PgDataSourcePool, PgEndpoint};
