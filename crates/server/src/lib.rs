//! Mycat3 Pro 服务端：前端 MySQL 协议服务、执行引擎、管理命令与监控。
//!
//! 模块对应关系（Java → Rust）：
//! - `MycatCore` / `MetaClusterCurrent` → [`state`]（ServerState 全局服务注册表）
//! - `VertxMycatServer` / `MycatSessionManager` → [`bootstrap`]（TCP 监听与连接分发）
//! - `VertxMySQLAuthHandler` / `MycatVertxMySQLHandler` → [`frontend`]（握手认证 + 命令循环）
//! - `ReceiverImpl` / `VertxExecuter` → [`executor`]（后端执行 + 结果归并）
//! - `MycatdbCommand` 的本地 SQLHandler → [`commands`]（SET/USE/SHOW/事务/EXPLAIN/@@变量）
//! - `ProxyTransactionSession` / `LocalTransactionSession` → [`tx`]
//! - `PrometheusExporter` / `HTTPServer` → [`monitor`]

pub mod backend;
pub mod bootstrap;
pub mod commands;
pub mod ddl;
pub mod engine;
pub mod executor;
pub mod frontend;
pub mod infoschema;
pub mod intercept;
pub mod loaddata;
pub mod monitor;
pub mod pg;
pub mod prepared_text;
pub mod sequence;
pub mod session;
pub mod sniff;
pub mod sqlcache;
pub mod state;
pub mod tds;
pub mod tx;
pub mod xa;

pub use state::{ServerState, ServerStats};
