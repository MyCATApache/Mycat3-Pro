//! Mycat3 Pro 后端数据源层：原生异步 MySQL 客户端 + 连接池。
//!
//! 对应 Mycat3 `datasource` 模块（`JdbcConnectionManager` / `JdbcDataSource` /
//! Druid 池）与 `mycat3` 模块的 vert.x 原生客户端（`NewMycatConnection`）。
//! Pro 版统一使用原生 MySQL 协议（而非 JDBC），减少一层驱动开销：
//! - [`conn`]：单连接（握手认证、COM_QUERY/PING/INIT_DB、文本结果集解析与原始载荷保留）
//! - [`pool`]：每数据源连接池（maxCon 信号量、空闲复用、PING 校验、事务连接剔除）

pub mod conn;
pub mod pool;

pub use conn::{
    decode_text_row, BackendEndpoint, ClientError, ClientResult, MySQLConnection, QueryOutcome,
    ResultSet,
};
pub use pool::{CheckedOutConnection, ConnectionManager, DataSourcePool};
