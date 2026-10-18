//! Mycat3 兼容的 JSON 配置模型与文件存储。
//!
//! 移植自 Mycat3 `config` 模块（`io.mycat.config.*`）与 `mycat3` 模块的
//! `FileStorageManagerImpl` / `FileKV` / `ConfigUpdater` / `MycatRouterConfigOps`。
//!
//! 磁盘布局（与 Java 版一致，`MYCAT_HOME` 目录下）：
//! ```text
//! server.json
//! users/<name>.user.json
//! datasources/<name>.datasource.json
//! clusters/<name>.cluster.json
//! schemas/<name>.schema.json
//! sequences/<name>.sequence.json
//! ```

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("json error at {path}: {source}")]
    Json {
        path: PathBuf,
        source: serde_json::Error,
    },
    #[error("invalid config: {0}")]
    Invalid(String),
}

pub type ConfigResult<T> = Result<T, ConfigError>;

fn json_err(path: &Path) -> impl Fn(serde_json::Error) -> ConfigError + '_ {
    move |e| ConfigError::Json {
        path: path.to_path_buf(),
        source: e,
    }
}

// ---------------------------------------------------------------------------
// 枚举（与 Java 枚举名保持一致，序列化兼容）
// ---------------------------------------------------------------------------

/// 集群类型。对应 `io.mycat.replica.ReplicaType`。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum ReplicaType {
    #[serde(rename = "SINGLE_NODE")]
    Single,
    #[default]
    #[serde(rename = "MASTER_SLAVE")]
    MasterSlave,
    #[serde(rename = "GARELA_CLUSTER", alias = "GALERA_CLUSTER")]
    Galera,
    #[serde(rename = "MGR")]
    Mgr,
    #[serde(rename = "MHA")]
    Mha,
    #[serde(rename = "NONE")]
    None,
}

/// 主从切换类型。对应 `io.mycat.replica.ReplicaSwitchType`（本分支只有两种）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum ReplicaSwitchType {
    #[serde(rename = "NOT_SWITCH")]
    NotSwitch,
    #[default]
    #[serde(rename = "SWITCH")]
    Switch,
}

/// 读负载均衡类型。对应 `io.mycat.replica.balance.BalanceType`。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum BalanceType {
    /// 读写都参与负载均衡
    #[default]
    #[serde(rename = "BALANCE_ALL")]
    All,
    /// 不负载均衡：只走主库
    #[serde(rename = "BALANCE_NONE")]
    None,
    /// 读全部从库，写主库
    #[serde(rename = "BALANCE_ALL_READ")]
    AllRead,
    /// 读走"可读"实例（含未延迟从库），写主库
    #[serde(rename = "BALANCE_READ_WRITE")]
    ReadWrite,
}

/// 数据源实例类型。对应 `io.mycat.datasource.InstanceType`。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum InstanceType {
    #[serde(rename = "READ")]
    Read,
    #[serde(rename = "WRITE")]
    Write,
    #[default]
    #[serde(rename = "READ_WRITE")]
    ReadWrite,
}

// ---------------------------------------------------------------------------
// server.json
// ---------------------------------------------------------------------------

fn default_port() -> u16 {
    8066
}
fn default_ip() -> String {
    "0.0.0.0".to_string()
}
fn default_server_version() -> String {
    "5.7.33-mycat-3.0-pro".to_string()
}
fn default_reactor_number() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(8)
}
fn default_mycat_id() -> u32 {
    1
}
fn default_true() -> bool {
    true
}
fn default_execute_timeout() -> u64 {
    60_000
}

/// `server.json` 中的 `server` 节。对应 `ServerConfig`。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ServerConfig {
    #[serde(default = "default_mycat_id")]
    pub mycat_id: u32,
    #[serde(default = "default_ip")]
    pub ip: String,
    #[serde(default = "default_port")]
    pub port: u16,
    /// IO 线程（reactor）数量
    #[serde(default = "default_reactor_number")]
    pub reactor_number: usize,
    #[serde(default = "default_server_version")]
    pub server_version: String,
    /// 是否在握手能力中启用 CLIENT_DEPRECATE_EOF
    #[serde(default = "default_true")]
    pub deprecate_eof: bool,
    /// 多协议同端口嗅探（MySQL/PostgreSQL/TDS，对应 Java ProtocolDetector）。
    /// 关闭后 8066 仅接受 MySQL 协议，连接无 300ms 嗅探延迟。
    #[serde(default = "default_true")]
    pub protocol_sniff: bool,
    /// 单条 SQL 执行超时（毫秒），0 表示不限制
    #[serde(default = "default_execute_timeout")]
    pub execute_timeout: u64,
    /// 前端 TLS 证书路径（PEM；与 ssl_key 同时配置则启用 MySQL/PG 前端 TLS）
    #[serde(default)]
    pub ssl_cert: Option<String>,
    #[serde(default)]
    pub ssl_key: Option<String>,
}

impl Default for ServerConfig {
    fn default() -> Self {
        ServerConfig {
            mycat_id: default_mycat_id(),
            ip: default_ip(),
            port: default_port(),
            reactor_number: default_reactor_number(),
            server_version: default_server_version(),
            deprecate_eof: true,
            protocol_sniff: true,
            execute_timeout: default_execute_timeout(),
            ssl_cert: None,
            ssl_key: None,
        }
    }
}

/// 监控节。对应 `MonitorConfig`（HTTP 管理端口 9066，等价 Java 版 ManagerConfig）。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MonitorConfig {
    #[serde(default = "default_monitor_ip")]
    pub ip: String,
    #[serde(default = "default_monitor_port")]
    pub port: u16,
    #[serde(default = "default_true")]
    pub open: bool,
    #[serde(default)]
    pub sql_log: bool,
}

fn default_monitor_ip() -> String {
    "0.0.0.0".to_string()
}
fn default_monitor_port() -> u16 {
    9066
}

impl Default for MonitorConfig {
    fn default() -> Self {
        MonitorConfig {
            ip: default_monitor_ip(),
            port: default_monitor_port(),
            open: true,
            sql_log: false,
        }
    }
}

fn default_mode() -> String {
    "local".to_string()
}
fn default_load_balance() -> LoadBalanceConfig {
    LoadBalanceConfig {
        default_load_balance: "BalanceRandom".to_string(),
        load_balances: Vec::new(),
    }
}
fn default_balance_name() -> String {
    "BalanceRandom".to_string()
}

/// 代理模式配置（`server.mode = "proxy"`）。
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct ProxyConfig {
    /// 透传目标（集群名或数据源名）；缺省 prototype
    #[serde(default)]
    pub target: Option<String>,
    /// SQL 拦截规则（命中返回权限错误；执行顺序在缓存之前）
    #[serde(default)]
    pub intercepts: Vec<InterceptRule>,
}

/// SQL 拦截规则。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InterceptRule {
    /// 正则；对 SQL 文本大小写不敏感匹配
    pub pattern: String,
    /// ALL（默认）| SELECT | DML | DDL | OTHER
    #[serde(default = "default_rule_stmt_type")]
    pub stmt_type: String,
    /// 错误文案（缺省 "Access denied: statement intercepted by policy"）
    #[serde(default)]
    pub message: Option<String>,
    /// MySQL 错误码（缺省 1142 ER_TABLEACCESS_DENIED_ERROR）
    #[serde(default)]
    pub errno: Option<u16>,
    /// SQLSTATE（缺省 42000）
    #[serde(default)]
    pub sqlstate: Option<String>,
}

fn default_rule_stmt_type() -> String {
    "ALL".to_string()
}

/// 运行模式："local"（= 分片路由，历史默认）、"cluster"（同上）、"proxy"（纯代理）。
pub fn is_proxy_mode(mode: &str) -> bool {
    mode.eq_ignore_ascii_case("proxy")
}

/// `server.json` 根对象。对应 `MycatServerConfig`。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MycatServerConfig {
    /// "local"（分片路由，默认）/ "cluster"（同 local）/ "proxy"（纯代理）
    #[serde(default = "default_mode")]
    pub mode: String,
    #[serde(default)]
    pub server: ServerConfig,
    #[serde(default)]
    pub monitor: MonitorConfig,
    #[serde(default = "default_load_balance")]
    pub load_balance: LoadBalanceConfig,
    /// 代理模式配置（mode=proxy 时生效）
    #[serde(default)]
    pub proxy: ProxyConfig,
}

impl Default for MycatServerConfig {
    fn default() -> Self {
        MycatServerConfig {
            mode: default_mode(),
            server: ServerConfig::default(),
            monitor: MonitorConfig::default(),
            load_balance: default_load_balance(),
            proxy: ProxyConfig::default(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LoadBalanceConfig {
    #[serde(default = "default_balance_name")]
    pub default_load_balance: String,
    #[serde(default)]
    pub load_balances: Vec<NamedClass>,
}

impl Default for LoadBalanceConfig {
    fn default() -> Self {
        default_load_balance()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NamedClass {
    pub name: String,
    pub clazz: String,
}

// ---------------------------------------------------------------------------
// datasources/*.datasource.json
// ---------------------------------------------------------------------------

fn default_max_con() -> u32 {
    1000
}
fn default_min_con() -> u32 {
    1
}
fn default_max_retry() -> u32 {
    5
}
fn default_connect_timeout() -> u64 {
    30_000
}
fn default_idle_timeout() -> u64 {
    60_000
}
fn default_db_type() -> String {
    "mysql".to_string()
}
fn default_instance_type() -> InstanceType {
    InstanceType::ReadWrite
}
fn default_ds_type() -> String {
    "NATIVE".to_string()
}

/// 对应 `DatasourceConfig`。url 为 JDBC 风格：`jdbc:mysql://host:port/db?params`。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DatasourceConfig {
    pub name: String,
    pub url: String,
    pub user: String,
    #[serde(default)]
    pub password: String,
    /// "mysql"（默认按 url 推断）
    #[serde(default = "default_db_type")]
    pub db_type: String,
    #[serde(default)]
    pub weight: i32,
    #[serde(default = "default_max_con")]
    pub max_con: u32,
    #[serde(default = "default_min_con")]
    pub min_con: u32,
    #[serde(default = "default_max_retry")]
    pub max_retry_count: u32,
    #[serde(default = "default_connect_timeout")]
    pub max_connect_timeout: u64,
    #[serde(default)]
    pub init_sqls: Vec<String>,
    #[serde(default = "default_instance_type")]
    pub instance_type: InstanceType,
    #[serde(default = "default_idle_timeout")]
    pub idle_timeout: u64,
    /// JDBC / NATIVE / NATIVE_JDBC —— Pro 版一律使用原生协议
    #[serde(default = "default_ds_type")]
    pub r#type: String,
}

/// 从 JDBC url 解析出的连接地址。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JdbcUrl {
    pub host: String,
    pub port: u16,
    pub database: String,
    pub params: BTreeMap<String, String>,
}

impl DatasourceConfig {
    /// 解析 `jdbc:mysql://host:port/db?params`（也容忍 `mysql://` 与 `host:port/db` 形式）。
    pub fn parse_url(&self) -> ConfigResult<JdbcUrl> {
        parse_jdbc_url(&self.url)
    }
}

pub fn parse_jdbc_url(url: &str) -> ConfigResult<JdbcUrl> {
    let mut rest = url;
    if let Some(idx) = rest.find("://") {
        rest = &rest[idx + 3..];
    }
    // 去掉查询参数
    let mut params = BTreeMap::new();
    if let Some(q) = rest.find('?') {
        let qs = &rest[q + 1..];
        rest = &rest[..q];
        for kv in qs.split('&') {
            if kv.is_empty() {
                continue;
            }
            let mut it = kv.splitn(2, '=');
            let k = it.next().unwrap_or("");
            let v = it.next().unwrap_or("");
            params.insert(k.to_string(), v.to_string());
        }
    }
    // host:port/database
    let (hostport, database) = match rest.find('/') {
        Some(i) => (&rest[..i], rest[i + 1..].to_string()),
        None => (rest, String::new()),
    };
    let (host, port) = match hostport.rfind(':') {
        Some(i) => {
            let p: u16 = hostport[i + 1..]
                .parse()
                .map_err(|_| ConfigError::Invalid(format!("bad port in url: {}", url)))?;
            (hostport[..i].to_string(), p)
        }
        None => (hostport.to_string(), 3306),
    };
    if host.is_empty() {
        return Err(ConfigError::Invalid(format!("empty host in url: {}", url)));
    }
    Ok(JdbcUrl {
        host,
        port,
        database,
        params,
    })
}

// ---------------------------------------------------------------------------
// clusters/*.cluster.json
// ---------------------------------------------------------------------------

fn default_hb_max_retry() -> u32 {
    3
}
fn default_hb_switch_interval() -> u64 {
    300
}
fn default_hb_timeout() -> u64 {
    1000
}

/// 心跳配置。对应 `HeartbeatConfig`。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HeartbeatConfig {
    /// 连续错误次数达到该值才判定故障（默认 3）
    #[serde(default = "default_hb_max_retry", alias = "maxRetryCount")]
    pub max_retry: u32,
    /// 两次切换的最小时间间隔（秒，默认 300）
    #[serde(default = "default_hb_switch_interval")]
    pub min_switch_time_interval: u64,
    /// 心跳超时（毫秒，默认 1000）
    #[serde(default = "default_hb_timeout")]
    pub heartbeat_timeout: u64,
    /// 从库延迟阈值（秒），超过则不参与读（默认 0：任何延迟都剔除）
    #[serde(default)]
    pub slave_threshold: u64,
    #[serde(default)]
    pub show_log: bool,
}

impl Default for HeartbeatConfig {
    fn default() -> Self {
        HeartbeatConfig {
            max_retry: default_hb_max_retry(),
            min_switch_time_interval: default_hb_switch_interval(),
            heartbeat_timeout: default_hb_timeout(),
            slave_threshold: 0,
            show_log: false,
        }
    }
}

/// 对应 `ClusterConfig`。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ClusterConfig {
    pub name: String,
    #[serde(default)]
    pub cluster_type: ReplicaType,
    #[serde(default)]
    pub switch_type: ReplicaSwitchType,
    #[serde(default)]
    pub read_balance_type: BalanceType,
    /// 读负载均衡策略名（BalanceRandom/BalanceRoundRobin/BalanceLeastActive/...）
    #[serde(default)]
    pub read_balance_name: Option<String>,
    #[serde(default)]
    pub write_balance_name: Option<String>,
    /// 主库数据源名列表（写走 masters[0]，故障时切换）
    #[serde(default)]
    pub masters: Vec<String>,
    /// 从库数据源名列表
    #[serde(default)]
    pub replicas: Vec<String>,
    #[serde(default)]
    pub heartbeat: HeartbeatConfig,
    #[serde(default = "default_cluster_max_con")]
    pub max_con: u32,
}

fn default_cluster_max_con() -> u32 {
    2000
}

// ---------------------------------------------------------------------------
// users/*.user.json
// ---------------------------------------------------------------------------

fn default_tx_type() -> String {
    "proxy".to_string()
}
fn default_dialect() -> String {
    "mysql".to_string()
}
fn default_isolation() -> u8 {
    3
}
fn default_login_limit() -> i32 {
    -1
}

/// 对应 `UserConfig`。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UserConfig {
    pub username: String,
    #[serde(default)]
    pub password: String,
    /// 允许的客户端 IP 正则（空表示不限制）
    #[serde(default)]
    pub ip: Option<String>,
    /// "proxy"（本地事务）或 "xa"
    #[serde(default = "default_tx_type")]
    pub transaction_type: String,
    #[serde(default = "default_dialect")]
    pub dialect: String,
    /// 默认 schema
    #[serde(default)]
    pub schema: Option<String>,
    /// 可访问的 schema 列表（Java 版 root.user.json 使用 "schemas"）
    #[serde(default)]
    pub schemas: Vec<String>,
    /// 隔离级别序号（3 = REPEATABLE-READ）
    #[serde(default = "default_isolation")]
    pub isolation: u8,
    #[serde(default)]
    pub read_only: bool,
    #[serde(default = "default_login_limit")]
    pub login_limit: i32,
    #[serde(default)]
    pub role: Option<UserRole>,
    /// 密码加密类型（Java 兼容）："rsa"（password 为 base64(RSA-PKCS1v15(明文))，
    /// encodeKey 为 base64(PKCS#8 私钥)）；空/缺省 = 明文
    #[serde(default)]
    pub encrypt_type: Option<String>,
    #[serde(default)]
    pub encode_key: Option<String>,
    /// 解密缓存（登录热路径只解密一次）
    #[serde(skip)]
    pub decrypted: std::sync::Arc<parking_lot::Mutex<Option<String>>>,
}

impl UserConfig {
    /// 密码明文：rsa 加密则解密（缓存），否则原样。
    pub fn decrypted_password(&self) -> String {
        if !self
            .encrypt_type
            .as_deref()
            .unwrap_or("")
            .eq_ignore_ascii_case("rsa")
        {
            return self.password.clone();
        }
        if let Some(p) = self.decrypted.lock().clone() {
            return p;
        }
        let plain = rsa_decrypt_password(&self.password, self.encode_key.as_deref().unwrap_or(""))
            .unwrap_or_else(|| self.password.clone());
        *self.decrypted.lock() = Some(plain.clone());
        plain
    }
}

/// Java RSAUtils.decrypt 等价实现：
/// password = base64(RSA/ECB/PKCS1Padding(明文))，encodeKey = base64(PKCS#8 私钥 DER)。
pub fn rsa_decrypt_password(encoded_password: &str, private_key_b64: &str) -> Option<String> {
    use base64::Engine;
    use rsa::pkcs8::DecodePrivateKey;
    let ct = base64::engine::general_purpose::STANDARD
        .decode(encoded_password.trim())
        .ok()?;
    // 容忍 PEM 装甲（-----BEGIN ... KEY----- 头尾/换行）：剥去后 base64 解码 DER
    let body: String = private_key_b64
        .lines()
        .filter(|l| !l.starts_with("-----"))
        .collect();
    let der = base64::engine::general_purpose::STANDARD
        .decode(body.trim())
        .ok()?;
    // PKCS#8 优先；失败回退 PKCS#1（"BEGIN RSA PRIVATE KEY"）
    let key = match rsa::RsaPrivateKey::from_pkcs8_der(&der) {
        Ok(k) => k,
        Err(_) => {
            use rsa::pkcs1::DecodeRsaPrivateKey;
            rsa::RsaPrivateKey::from_pkcs1_der(&der).ok()?
        }
    };
    let pt = key.decrypt(rsa::Pkcs1v15Encrypt, &ct).ok()?;
    String::from_utf8(pt).ok()
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct UserRole {
    #[serde(default)]
    pub disallow_sql_types: Vec<String>,
}

impl UserConfig {
    /// 校验客户端 IP 是否被允许（对应 AuthenticatorImpl 的正则匹配）。
    pub fn ip_allowed(&self, client_ip: &str) -> bool {
        match &self.ip {
            None => true,
            Some(pattern) if pattern.is_empty() => true,
            Some(pattern) => regex::Regex::new(pattern)
                .map(|re| re.is_match(client_ip))
                .unwrap_or(false),
        }
    }

    /// 用户可见 schema 集合（schemas 非空用之，否则用 schema）。
    pub fn visible_schemas(&self) -> Vec<String> {
        if !self.schemas.is_empty() {
            self.schemas.clone()
        } else {
            self.schema.clone().into_iter().collect()
        }
    }
}

// ---------------------------------------------------------------------------
// schemas/*.schema.json
// ---------------------------------------------------------------------------

/// 表在物理端的落点。对应 `BackendTableInfoConfig` / `Locality`。
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct LocalityConfig {
    pub target_name: String,
    #[serde(default)]
    pub schema_name: Option<String>,
    #[serde(default)]
    pub table_name: Option<String>,
}

/// 单表（NORMAL）。对应 `NormalTableConfig`。
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct NormalTableConfig {
    /// Java 版字段名为 createTableSQL（camelCase 序列化名为 createTableSql，别名兼容）
    #[serde(default, alias = "createTableSQL")]
    pub create_table_sql: Option<String>,
    #[serde(default)]
    pub locality: Option<LocalityConfig>,
}

/// 广播表（GLOBAL）。对应 `GlobalTableConfig`。
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct GlobalTableConfig {
    /// Java 版字段名为 createTableSQL（camelCase 序列化名为 createTableSql，别名兼容）
    #[serde(default, alias = "createTableSQL")]
    pub create_table_sql: Option<String>,
    #[serde(default)]
    pub broadcast: Vec<LocalityConfig>,
    /// NO_SEQUENCE / GLOBAL_SEQUENCE / FIRST_SEQUENCE
    #[serde(default)]
    pub sequence_type: Option<String>,
}

/// 分片表后端信息。对应 `ShardingBackEndTableInfoConfig`。
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct ShardingBackendConfig {
    /// 支持 `db$0-3` 形式展开（SplitUtil）
    #[serde(default)]
    pub target_names: Vec<String>,
    #[serde(default)]
    pub schema_names: Vec<String>,
    #[serde(default)]
    pub table_names: Vec<String>,
    /// 显式节点列表：每行 [target, schema, table] 或 [target, schema, table, dbIndex, tableIndex, index]
    #[serde(default)]
    pub data: Vec<Vec<serde_json::Value>>,
}

/// 分片函数定义。对应 `ShardingFunction`：
/// `name` 为空且 `clazz` 为空时使用 AutoFunction（dbMethod/tableMethod）。
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct ShardingFunctionConfig {
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub clazz: Option<String>,
    /// 属性表，如 {"count": "4"} 或 AutoFunction 的
    /// {"dbNum":"2","tableNum":"4","dbMethod":"hash(id)","tableMethod":"hash(id)",
    ///  "mappingFormat":"c${targetIndex}/db1_${dbIndex}/sharding_${tableIndex}"}
    #[serde(default = "default_props")]
    pub properties: BTreeMap<String, String>,
    /// 范围表，如 {"0-99": "0", "100-199": "1"}
    #[serde(default = "default_props")]
    pub ranges: BTreeMap<String, String>,
}

fn default_props() -> BTreeMap<String, String> {
    BTreeMap::new()
}

/// 分片表。对应 `ShardingTableConfig`。
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct ShardingTableConfig {
    #[serde(default)]
    pub partition: ShardingBackendConfig,
    #[serde(default)]
    pub function: ShardingFunctionConfig,
    /// Java 版字段名为 createTableSQL（camelCase 序列化名为 createTableSql，别名兼容）
    #[serde(default, alias = "createTableSQL")]
    pub create_table_sql: Option<String>,
    #[serde(default)]
    pub auto_increment: Option<serde_json::Value>,
}

/// 视图。对应 `ViewConfig`。
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct ViewConfig {
    #[serde(default)]
    pub create_view_sql: Option<String>,
}

fn default_target() -> String {
    "prototype".to_string()
}
fn default_tables<T>() -> BTreeMap<String, T> {
    BTreeMap::new()
}

/// 逻辑库。对应 `LogicSchemaConfig`。
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct LogicSchemaConfig {
    pub schema_name: String,
    /// 该库默认的 target（集群名或数据源名）
    #[serde(default = "default_target")]
    pub target_name: String,
    #[serde(default = "default_tables")]
    pub sharding_tables: BTreeMap<String, ShardingTableConfig>,
    #[serde(default = "default_tables")]
    pub global_tables: BTreeMap<String, GlobalTableConfig>,
    #[serde(default = "default_tables")]
    pub normal_tables: BTreeMap<String, NormalTableConfig>,
    #[serde(default = "default_tables")]
    pub views: BTreeMap<String, ViewConfig>,
}

// ---------------------------------------------------------------------------
// sequences/*.sequence.json
// ---------------------------------------------------------------------------

fn default_seq_class() -> String {
    "io.mycat.router.sequence.SnowflakeSequence".to_string()
}
fn default_seq_step() -> u64 {
    1
}

/// 对应 `SequenceConfig`。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SequenceConfig {
    pub name: String,
    #[serde(default = "default_seq_class")]
    pub clazz: String,
    #[serde(default = "default_target")]
    pub target_name: String,
    #[serde(default)]
    pub schema_name: Option<String>,
    #[serde(default = "default_seq_step")]
    pub step: u64,
    #[serde(default)]
    pub start_with: u64,
    #[serde(default)]
    pub increment_by: u64,
}

/// SQL 结果缓存配置。对应 Java `SqlCacheConfig`（字段兼容）+ Pro 扩展：
/// - `refreshInterval`（秒，Java 字段名）/`refreshIntervalMs`（毫秒，Pro 精度）
/// - `matchMode`：PARAMETERIZED（默认，字面量→? 归一化）| EXACT（SQL 文本精确）
/// - `maxRows`/`maxBytes`：结果超限不缓存（防内存/文件膨胀）
/// - `schemaName`：执行缓存 SQL 时的默认逻辑库（未库限定的 SQL 需要）
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SqlCacheConfig {
    pub name: String,
    pub sql: String,
    /// 刷新间隔（秒，Java 兼容字段）
    #[serde(default = "default_cache_refresh_seconds")]
    pub refresh_interval: u64,
    /// 初始延迟（秒，Java 兼容字段）
    #[serde(default)]
    pub initial_delay: u64,
    /// 匹配模式：PARAMETERIZED（默认）| EXACT
    #[serde(default = "default_cache_match_mode")]
    pub match_mode: String,
    /// 结果超过该行数不缓存（0 = 不限）
    #[serde(default = "default_cache_max_rows")]
    pub max_rows: u64,
    /// 结果超过该字节数不缓存（0 = 不限）
    #[serde(default)]
    pub max_bytes: u64,
    /// 执行缓存 SQL 的默认逻辑库
    #[serde(default)]
    pub schema_name: Option<String>,
}

fn default_cache_refresh_seconds() -> u64 {
    60
}
fn default_cache_match_mode() -> String {
    "PARAMETERIZED".to_string()
}
fn default_cache_max_rows() -> u64 {
    100_000
}

// ---------------------------------------------------------------------------
// 运行时配置快照（对应 MycatRouterConfig）
// ---------------------------------------------------------------------------

/// 全部配置的运行时快照。对应 `io.mycat.config.MycatRouterConfig`。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MycatRouterConfig {
    pub server: MycatServerConfig,
    pub datasources: BTreeMap<String, DatasourceConfig>,
    pub clusters: BTreeMap<String, ClusterConfig>,
    pub users: BTreeMap<String, UserConfig>,
    pub schemas: BTreeMap<String, LogicSchemaConfig>,
    pub sequences: BTreeMap<String, SequenceConfig>,
    /// SQL 结果缓存（对应 Java sqlcaches/ 目录）
    #[serde(default = "default_tables")]
    pub sql_caches: BTreeMap<String, SqlCacheConfig>,
    /// 元数据原型集群名，固定 "prototype"（对应 MetadataManager.getPrototype()）
    pub prototype: String,
}

impl Default for MycatRouterConfig {
    fn default() -> Self {
        MycatRouterConfig {
            server: MycatServerConfig::default(),
            datasources: BTreeMap::new(),
            clusters: BTreeMap::new(),
            users: BTreeMap::new(),
            schemas: BTreeMap::new(),
            sequences: BTreeMap::new(),
            sql_caches: BTreeMap::new(),
            prototype: "prototype".to_string(),
        }
    }
}

impl MycatRouterConfig {
    pub fn new() -> Self {
        Self::default()
    }

    /// 是否纯代理模式（server.mode = "proxy"）。
    pub fn is_proxy_mode(&self) -> bool {
        is_proxy_mode(&self.server.mode)
    }

    /// 代理模式透传目标（缺省 prototype）。
    pub fn proxy_target(&self) -> String {
        self.server
            .proxy
            .target
            .clone()
            .unwrap_or_else(|| self.prototype.clone())
    }
}

// ---------------------------------------------------------------------------
// 文件存储（对应 FileStorageManagerImpl + FileKV）
// ---------------------------------------------------------------------------

/// 带名字的配置对象（用于落盘 key）。
pub trait Named {
    fn name(&self) -> &str;
}
impl Named for DatasourceConfig {
    fn name(&self) -> &str {
        &self.name
    }
}
impl Named for ClusterConfig {
    fn name(&self) -> &str {
        &self.name
    }
}
impl Named for UserConfig {
    fn name(&self) -> &str {
        &self.username
    }
}
impl Named for LogicSchemaConfig {
    fn name(&self) -> &str {
        &self.schema_name
    }
}
impl Named for SequenceConfig {
    fn name(&self) -> &str {
        &self.name
    }
}
impl Named for SqlCacheConfig {
    fn name(&self) -> &str {
        &self.name
    }
}

/// 基于文件系统的配置存储。
pub struct FileConfigStorage {
    home: PathBuf,
}

impl FileConfigStorage {
    pub fn new(home: impl Into<PathBuf>) -> Self {
        FileConfigStorage { home: home.into() }
    }

    pub fn home(&self) -> &Path {
        &self.home
    }

    fn read_json<T: serde::de::DeserializeOwned>(&self, path: &Path) -> ConfigResult<T> {
        let content = std::fs::read(path)?;
        serde_json::from_slice(&content).map_err(json_err(path))
    }

    fn write_json<T: Serialize>(&self, path: &Path, value: &T) -> ConfigResult<()> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let content = serde_json::to_string_pretty(value).map_err(json_err(path))?;
        std::fs::write(path, content)?;
        Ok(())
    }

    /// 加载全部配置。目录不存在或为空时返回带默认值的配置。
    pub fn load_all(&self) -> ConfigResult<MycatRouterConfig> {
        let mut cfg = MycatRouterConfig::new();
        let server_path = self.home.join("server.json");
        if server_path.exists() {
            cfg.server = self.read_json(&server_path)?;
        }
        cfg.datasources = self.load_dir("datasources", "datasource")?;
        cfg.clusters = self.load_dir("clusters", "cluster")?;
        cfg.users = self.load_dir("users", "user")?;
        cfg.schemas = self.load_dir("schemas", "schema")?;
        cfg.sequences = self.load_dir("sequences", "sequence")?;
        cfg.sql_caches = self.load_dir("sqlcaches", "sqlcache")?;
        Ok(cfg)
    }

    /// 加载 `<dir>/<name>.<template>.json` 形式的多文件配置（对应 FileKV）。
    fn load_dir<T: serde::de::DeserializeOwned>(
        &self,
        dir: &str,
        template: &str,
    ) -> ConfigResult<BTreeMap<String, T>> {
        let mut map = BTreeMap::new();
        let path = self.home.join(dir);
        if !path.exists() {
            return Ok(map);
        }
        for entry in std::fs::read_dir(&path)? {
            let entry = entry?;
            let p = entry.path();
            if !p.is_file() {
                continue;
            }
            let name = match p.file_name().and_then(|n| n.to_str()) {
                Some(n) => n,
                None => continue,
            };
            let suffix = format!(".{}.json", template);
            if !name.ends_with(&suffix) {
                continue;
            }
            let key = name[..name.len() - suffix.len()].to_string();
            let value: T = self.read_json(&p)?;
            map.insert(key, value);
        }
        Ok(map)
    }

    /// 全量同步落盘：写出 cfg 并删除目录中已不存在于 cfg 的条目文件
    /// （对应 FileKV removeKey + put 的组合语义，DDL DROP 后配置目录保持一致）。
    pub fn save_all_sync(&self, cfg: &MycatRouterConfig) -> ConfigResult<()> {
        self.save_all(cfg)?;
        Self::purge_dir(
            &self.home.join("datasources"),
            "datasource",
            cfg.datasources.keys(),
        )?;
        Self::purge_dir(&self.home.join("clusters"), "cluster", cfg.clusters.keys())?;
        Self::purge_dir(&self.home.join("users"), "user", cfg.users.keys())?;
        Self::purge_dir(&self.home.join("schemas"), "schema", cfg.schemas.keys())?;
        Self::purge_dir(
            &self.home.join("sequences"),
            "sequence",
            cfg.sequences.keys(),
        )?;
        Self::purge_dir(
            &self.home.join("sqlcaches"),
            "sqlcache",
            cfg.sql_caches.keys(),
        )?;
        Ok(())
    }

    fn purge_dir<'a, I: Iterator<Item = &'a String>>(
        dir: &Path,
        template: &str,
        keep: I,
    ) -> ConfigResult<()> {
        if !dir.exists() {
            return Ok(());
        }
        let keep: Vec<&str> = keep.map(|s| s.as_str()).collect();
        let suffix = format!(".{}.json", template);
        for entry in std::fs::read_dir(dir)? {
            let p = entry?.path();
            if !p.is_file() {
                continue;
            }
            if let Some(name) = p.file_name().and_then(|n| n.to_str()) {
                if let Some(key) = name.strip_suffix(&suffix) {
                    if !keep.contains(&key) {
                        std::fs::remove_file(&p)?;
                    }
                }
            }
        }
        Ok(())
    }

    /// 写出全部配置（对应 ConfigUpdater.writeDefaultConfigToFile / put 系列）。
    pub fn save_all(&self, cfg: &MycatRouterConfig) -> ConfigResult<()> {
        std::fs::create_dir_all(&self.home)?;
        self.write_json(&self.home.join("server.json"), &cfg.server)?;
        for (name, ds) in &cfg.datasources {
            self.write_json(
                &self
                    .home
                    .join("datasources")
                    .join(format!("{}.datasource.json", name)),
                ds,
            )?;
        }
        for (name, c) in &cfg.clusters {
            self.write_json(
                &self
                    .home
                    .join("clusters")
                    .join(format!("{}.cluster.json", name)),
                c,
            )?;
        }
        for (name, u) in &cfg.users {
            self.write_json(
                &self.home.join("users").join(format!("{}.user.json", name)),
                u,
            )?;
        }
        for (name, s) in &cfg.schemas {
            self.write_json(
                &self
                    .home
                    .join("schemas")
                    .join(format!("{}.schema.json", name)),
                s,
            )?;
        }
        for (name, s) in &cfg.sequences {
            self.write_json(
                &self
                    .home
                    .join("sequences")
                    .join(format!("{}.sequence.json", name)),
                s,
            )?;
        }
        for (name, c) in &cfg.sql_caches {
            self.write_json(
                &self
                    .home
                    .join("sqlcaches")
                    .join(format!("{}.sqlcache.json", name)),
                c,
            )?;
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// 默认配置（对应 MycatRouterConfigOps.defaultConfig()）
// ---------------------------------------------------------------------------

/// 生成首次启动的默认配置：root 用户 + prototypeDs 数据源 + prototype/c0 集群。
pub fn default_config() -> MycatRouterConfig {
    let mut cfg = MycatRouterConfig::new();
    cfg.users.insert(
        "root".into(),
        UserConfig {
            username: "root".into(),
            password: String::new(),
            ip: None,
            transaction_type: default_tx_type(),
            dialect: default_dialect(),
            schema: None,
            schemas: vec!["db1".into()],
            isolation: 3,
            read_only: false,
            login_limit: -1,
            role: None,
            encrypt_type: None,
            encode_key: None,
            decrypted: std::sync::Arc::new(parking_lot::Mutex::new(None)),
        },
    );
    cfg.datasources.insert(
        "prototypeDs".into(),
        DatasourceConfig {
            name: "prototypeDs".into(),
            url: "jdbc:mysql://127.0.0.1:3306/mysql".into(),
            user: "root".into(),
            password: String::new(),
            db_type: default_db_type(),
            weight: 0,
            max_con: default_max_con(),
            min_con: default_min_con(),
            max_retry_count: default_max_retry(),
            max_connect_timeout: default_connect_timeout(),
            init_sqls: Vec::new(),
            instance_type: InstanceType::ReadWrite,
            idle_timeout: default_idle_timeout(),
            r#type: default_ds_type(),
        },
    );
    let mk_cluster = |name: &str| ClusterConfig {
        name: name.into(),
        cluster_type: ReplicaType::MasterSlave,
        switch_type: ReplicaSwitchType::Switch,
        read_balance_type: BalanceType::All,
        read_balance_name: None,
        write_balance_name: None,
        masters: vec!["prototypeDs".into()],
        replicas: Vec::new(),
        heartbeat: HeartbeatConfig::default(),
        max_con: 200,
    };
    cfg.clusters
        .insert("prototype".into(), mk_cluster("prototype"));
    cfg.clusters.insert("c0".into(), mk_cluster("c0"));

    // 默认逻辑库 db1（单表模式，落在 c0）
    let mut schema = LogicSchemaConfig {
        schema_name: "db1".into(),
        target_name: "c0".into(),
        ..Default::default()
    };
    schema.normal_tables.insert(
        "travelrecord".into(),
        NormalTableConfig {
            create_table_sql: Some(
                "CREATE TABLE travelrecord(id BIGINT PRIMARY KEY, name VARCHAR(64))".into(),
            ),
            locality: Some(LocalityConfig {
                target_name: "c0".into(),
                schema_name: Some("db1".into()),
                table_name: Some("travelrecord".into()),
            }),
        },
    );
    cfg.schemas.insert("db1".into(), schema);
    cfg
}

/// `SplitUtil.split` 的移植：展开 `db$0-3` → [db0, db1, db2, db3]，以及逗号列表。
/// 对应 `io.mycat.util.SplitUtil`（分隔符 ','，前缀标记 '$'，范围 '-'）。
pub fn split_expand(s: &str) -> Vec<String> {
    let mut out = Vec::new();
    for part in s.split(',') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        if let Some(dollar) = part.find('$') {
            let prefix = &part[..dollar];
            let range = &part[dollar + 1..];
            let (start, end) = match range.split_once('-') {
                Some((a, b)) => (a, b),
                None => (range, range),
            };
            if let (Ok(a), Ok(b)) = (start.parse::<i64>(), end.parse::<i64>()) {
                let pad = start.len() > 1 && start.starts_with('0');
                for i in a..=b {
                    if pad {
                        out.push(format!("{}{:0>width$}", prefix, i, width = start.len()));
                    } else {
                        out.push(format!("{}{}", prefix, i));
                    }
                }
                continue;
            }
        }
        out.push(part.to_string());
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_jdbc_url() {
        let u = parse_jdbc_url(
            "jdbc:mysql://127.0.0.1:3306/db1?useUnicode=true&serverTimezone=Asia/Shanghai",
        )
        .unwrap();
        assert_eq!(u.host, "127.0.0.1");
        assert_eq!(u.port, 3306);
        assert_eq!(u.database, "db1");
        assert_eq!(u.params.get("useUnicode").unwrap(), "true");

        let u = parse_jdbc_url("mysql://db.example.com/mydb").unwrap();
        assert_eq!(u.host, "db.example.com");
        assert_eq!(u.port, 3306);
        assert_eq!(u.database, "mydb");
    }

    #[test]
    fn test_split_expand() {
        assert_eq!(split_expand("db$0-3"), vec!["db0", "db1", "db2", "db3"]);
        assert_eq!(split_expand("db1,db2"), vec!["db1", "db2"]);
        assert_eq!(split_expand("t$00-02"), vec!["t00", "t01", "t02"]);
        assert_eq!(split_expand("single"), vec!["single"]);
    }

    #[test]
    fn test_serde_roundtrip_datasource() {
        // 与 Java 版 prototypeDs.datasource.json 一致的字段 + 未知字段容忍
        let json = r#"{
          "dbType":"mysql","idleTimeout":60000,"initSqls":[],"instanceType":"READ_WRITE",
          "maxCon":1000,"maxConnectTimeout":30000,"maxRetryCount":5,"minCon":1,
          "name":"prototypeDs","password":"","type":"JDBC",
          "url":"jdbc:mysql://localhost:3306/db1",
          "user":"root","weight":0,
          "unknownFutureField": 42
        }"#;
        let ds: DatasourceConfig = serde_json::from_str(json).unwrap();
        assert_eq!(ds.name, "prototypeDs");
        assert_eq!(ds.instance_type, InstanceType::ReadWrite);
        assert_eq!(ds.max_con, 1000);
        assert_eq!(ds.r#type, "JDBC");
        let s = serde_json::to_string(&ds).unwrap();
        assert!(s.contains("\"instanceType\":\"READ_WRITE\""));
    }

    #[test]
    fn test_serde_cluster_defaults() {
        let json = r#"{"name":"c0","masters":["ds0"],"replicas":["ds1"]}"#;
        let c: ClusterConfig = serde_json::from_str(json).unwrap();
        assert_eq!(c.cluster_type, ReplicaType::MasterSlave);
        assert_eq!(c.switch_type, ReplicaSwitchType::Switch);
        assert_eq!(c.read_balance_type, BalanceType::All);
        assert_eq!(c.heartbeat.max_retry, 3);
        assert_eq!(c.heartbeat.heartbeat_timeout, 1000);
        assert_eq!(c.heartbeat.min_switch_time_interval, 300);
    }

    #[test]
    fn test_serde_schema_with_sharding() {
        let json = r#"{
          "schemaName": "db1",
          "targetName": "c0",
          "shardingTables": {
            "orders": {
              "partition": {
                "targetNames": ["c0"],
                "schemaNames": ["db1$0-1"],
                "tableNames": ["orders_$0-3"]
              },
              "function": {
                "clazz": "io.mycat.router.mycat1xfunction.PartitionByMod",
                "properties": {"columnName": "id", "count": "8"}
              }
            }
          }
        }"#;
        let s: LogicSchemaConfig = serde_json::from_str(json).unwrap();
        let t = s.sharding_tables.get("orders").unwrap();
        assert_eq!(t.partition.table_names, vec!["orders_$0-3"]);
        assert_eq!(t.function.properties.get("count").unwrap(), "8");
        assert_eq!(split_expand("db1$0-1"), vec!["db10", "db11"]);
    }

    #[test]
    fn test_user_ip_allowed() {
        let u = UserConfig {
            username: "root".into(),
            password: "".into(),
            ip: Some("127\\.0\\.0\\..*".into()),
            transaction_type: "proxy".into(),
            dialect: "mysql".into(),
            schema: None,
            schemas: vec![],
            isolation: 3,
            read_only: false,
            login_limit: -1,
            role: None,
            encrypt_type: None,
            encode_key: None,
            decrypted: std::sync::Arc::new(parking_lot::Mutex::new(None)),
        };
        assert!(u.ip_allowed("127.0.0.1"));
        assert!(!u.ip_allowed("192.168.1.5"));
    }

    #[test]
    fn test_file_storage_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let storage = FileConfigStorage::new(dir.path());
        let cfg = default_config();
        storage.save_all(&cfg).unwrap();
        let loaded = storage.load_all().unwrap();
        assert!(loaded.users.contains_key("root"));
        assert!(loaded.datasources.contains_key("prototypeDs"));
        assert!(loaded.clusters.contains_key("prototype"));
        assert!(loaded.schemas.contains_key("db1"));
        assert_eq!(loaded.server.server.port, 8066);
        assert!(dir.path().join("users/root.user.json").exists());
        assert!(dir.path().join("clusters/prototype.cluster.json").exists());
    }

    #[test]
    fn test_load_mycat3_java_style_files() {
        // 验证能直接加载 Java Mycat3 的真实配置文件格式
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("datasources")).unwrap();
        std::fs::write(
            dir.path().join("server.json"),
            r#"{"mode":"local","server":{"ip":"0.0.0.0","port":8066,"reactorNumber":8,"serverVersion":"5.7.33-mycat-2.0"}}"#,
        )
        .unwrap();
        std::fs::write(
            dir.path().join("datasources/ds0.datasource.json"),
            r#"{"name":"ds0","url":"jdbc:mysql://127.0.0.1:3306/db1","user":"root","password":"","maxCon":1000,"instanceType":"READ_WRITE"}"#,
        )
        .unwrap();
        let storage = FileConfigStorage::new(dir.path());
        let cfg = storage.load_all().unwrap();
        assert_eq!(cfg.server.server.port, 8066);
        assert_eq!(cfg.server.server.server_version, "5.7.33-mycat-2.0");
        assert_eq!(
            cfg.datasources.get("ds0").unwrap().url,
            "jdbc:mysql://127.0.0.1:3306/db1"
        );
    }
}
