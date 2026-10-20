# 配置参考

Mycat3 Pro 使用与 Mycat3（Java）完全兼容的 JSON 多文件配置。配置目录（`MYCAT_HOME`）由
`--home` 参数或环境变量 `MYCAT3_PRO_HOME` / `MYCAT_HOME` 指定，默认 `./mycat-home`。
首次启动若目录为空，自动生成默认配置（root/空密码 + prototypeDs + prototype/c0 集群 + db1 示例库）。

```
<home>/
├── server.json
├── users/<name>.user.json
├── datasources/<name>.datasource.json
├── clusters/<name>.cluster.json
├── schemas/<name>.schema.json
└── sequences/<name>.sequence.json     # 可选
```

未识别的 JSON 字段会被忽略（与 Java 版 fastjson 行为一致），因此 Java 版配置文件可直接使用。

---

## server.json

```json
{
  "mode": "local",
  "server": {
    "mycatId": 1,
    "ip": "0.0.0.0",
    "port": 8066,
    "reactorNumber": 8,
    "serverVersion": "5.7.33-mycat-3.0-pro",
    "deprecateEof": true,
    "protocolSniff": true,
    "executeTimeout": 60000
  },
  "monitor": { "ip": "0.0.0.0", "port": 9066, "open": true, "sqlLog": false },
  "loadBalance": { "defaultLoadBalance": "BalanceRandom", "loadBalances": [] }
}
```

| 字段 | 默认 | 说明 |
|---|---|---|
| `server.ip` / `server.port` | `0.0.0.0` / `8066` | MySQL 协议监听地址（Java 版默认同为 8066） |
| `server.reactorNumber` | CPU 核数 | tokio 工作线程数（对应 Vert.x eventLoopPoolSize） |
| `server.serverVersion` | `5.7.33-mycat-3.0-pro` | 握手返回的版本串 |
| `server.deprecateEof` | `true` | 允许协商 CLIENT_DEPRECATE_EOF |
| `server.protocolSniff` | `true` | 多协议同端口嗅探（MySQL/PG/TDS）。关闭后仅接受 MySQL 协议，连接免除 300ms 嗅探等待 |
| `server.executeTimeout` | `60000` | 单条后端 SQL 超时（毫秒，0=不限） |
| `monitor.port` | `9066` | HTTP 监控端口（对应 Java ManagerConfig） |
| `monitor.sqlLog` | `false` | 记录每条 SQL 到日志 |

## users/*.user.json

```json
{
  "username": "root",
  "password": "123456",
  "ip": "127\\.0\\.0\\..*|192\\.168\\..*",
  "transactionType": "proxy",
  "dialect": "mysql",
  "schema": "db1",
  "schemas": ["db1"],
  "isolation": 3,
  "readOnly": false,
  "loginLimit": -1
}
```

- `password`：明文（认证时做 scramble 比对，与 Java 版一致）
- `ip`：客户端 IP 正则白名单，空/缺省不限制
- `schemas`（或 `schema`）：登录后可见的逻辑库；认证包携带的 database 必须已存在于 schemas 配置
- `isolation`：0=READ-UNCOMMITTED 1=READ-COMMITTED 2=SERIALIZABLE 3=REPEATABLE-READ（默认）
- `transactionType`：`proxy`（本地事务，默认）或 `xa`（MySQL/PostgreSQL 2PC）

## datasources/*.datasource.json

```json
{
  "name": "ds0",
  "url": "jdbc:mysql://127.0.0.1:3306/db1_0?useUnicode=true&characterEncoding=UTF-8",
  "user": "root",
  "password": "pw",
  "dbType": "mysql",
  "weight": 1,
  "maxCon": 1000,
  "minCon": 1,
  "maxRetryCount": 5,
  "maxConnectTimeout": 30000,
  "initSqls": [],
  "instanceType": "READ_WRITE",
  "idleTimeout": 60000,
  "type": "NATIVE"
}
```

- `url`：JDBC 格式（保持与 Java 版兼容），Pro 解析出 host/port/database 后走原生协议；
  **Oracle 后端**：`dbType: "oracle"` + `jdbc:oracle:thin:@//host:port/service_name`，
  走官方 rust-oracledb thin 驱动（纯 Rust，免 Oracle 客户端库；不支持 ANO 原生加密，
  TLS 可用）。Oracle 后端的物理分片表需手工创建（DDL 自动化为 MySQL 专用，
  见 [oracle-backend-feasibility.md](oracle-backend-feasibility.md)）
- `weight`：BalanceRoundRobin 权重
- `maxCon`：连接池上限（信号量）
- `initSqls`：新建连接后执行的初始化 SQL
- `dbType`：`mysql`（默认）、`postgresql`、`oracle` 或 `mssql`/`sqlserver`；决定后端驱动与 SQL 方言翻译
- `server.sslCert`/`server.sslKey`（可选）：前端 TLS 证书/私钥 PEM 路径（相对 home），
  同时配置启用 MySQL/PG 前端 TLS（TDS 未实现）
- 用户 `readOnly`/`loginLimit`：只读强制 / 活跃连接上限（见 commands.md）
- 用户 `encryptType:"rsa"`+`encodeKey`：RSA 加密密码（Java 兼容）
- Java server.json 的其余调优键（`ignoreCast`/`rewriteInsertBatchedStatementBatch`/
  `stickySession`/`sendBufferSize`/`asyncDDL` 等约 20 项）**忽略且无效果**（文档化边界）
- 用户 `transactionType`：`proxy`（默认，本地代理事务）或 `xa`（**MySQL/PostgreSQL XA 2PC**，
  含决策 WAL `<home>/xa/xa-wal.log`、启动 `XA RECOVER`/`pg_prepared_xacts` 崩溃恢复；
  Oracle/SQL Server 后端分支显式拒绝）
- `instanceType`：`READ` / `WRITE` / `READ_WRITE`
- `idleTimeout`：空闲连接回收阈值（毫秒）

## clusters/*.cluster.json

```json
{
  "name": "c0",
  "clusterType": "MASTER_SLAVE",
  "switchType": "SWITCH",
  "readBalanceType": "BALANCE_ALL",
  "readBalanceName": "BalanceRandom",
  "masters": ["ds0"],
  "replicas": ["ds1"],
  "heartbeat": {
    "maxRetry": 3,
    "minSwitchTimeInterval": 300,
    "heartbeatTimeout": 1000,
    "slaveThreshold": 0
  },
  "maxCon": 2000
}
```

- `clusterType`：`SINGLE_NODE` / `MASTER_SLAVE` / `GARELA_CLUSTER` / `MGR` / `MHA` / `NONE`
  （决定心跳 SQL：`select 1` / `show slave status` / `show status like 'wsrep%'` /
  performance_schema MGR 查询）
- `switchType`：`SWITCH`（主库连续失败 ≥ maxRetry 次后，把存活主库顶到写列表头部）/ `NOT_SWITCH`
- `readBalanceType`：
  - `BALANCE_ALL` 主从都参与读
  - `BALANCE_NONE` 只读主库
  - `BALANCE_ALL_READ` 只读从库（无可用从库回退主库）
  - `BALANCE_READ_WRITE` 可读实例（未延迟从库+主库）
- `readBalanceName`：`BalanceRandom`（默认）/ `BalanceRoundRobin`（权重）/ `BalanceLeastActive`
- `heartbeat.slaveThreshold`：从库 `Seconds_Behind_Master` 超过该值则剔除出读（0 = 任何延迟都剔除）
- `heartbeat.minSwitchTimeInterval`：两次切换最小间隔（秒，防抖）

**写永远路由到 `masters` 中第一个存活实例；读按上述策略负载均衡。**

## schemas/*.schema.json

```json
{
  "schemaName": "db1",
  "targetName": "c0",
  "shardingTables": { "orders": { ... } },
  "globalTables":   { "dict":   { ... } },
  "normalTables":   { "user_info": { ... } },
  "views":          { }
}
```

- `targetName`：库级默认 target（未注册表/无表语句的路由目标），可以是集群名或数据源名

### 分片表 shardingTables

```json
"orders": {
  "createTableSQL": "CREATE TABLE orders(id BIGINT PRIMARY KEY, name VARCHAR(64))",
  "partition": {
    "targetNames": ["ds0", "ds1"],
    "schemaNames": ["db1_0", "db1_1"],
    "tableNames": ["orders_$0-1"],
    "data": [["ds0","db1_0","orders_0"], ["ds1","db1_1","orders_3"]]
  },
  "function": {
    "clazz": "io.mycat.router.mycat1xfunction.PartitionByMod",
    "properties": { "columnName": "id", "count": "4" },
    "ranges": {}
  }
}
```

- `partition.data` 存在时逐行作为节点（`[target, schema, table]`，可带 `dbIndex, tableIndex, index`）；
  否则三个列表做笛卡尔积。名字支持 `db$0-3` 范围展开（含 `t$00-09` 补零形式）
- `function`：
  - `clazz`：Java 类全名（按类名后缀识别）或简名；也支持 Mycat2 风格别名（`mod`、`murmur`、`rangemod-hash`、`date`、`month`、`crc32slot`、`jump-consistent-hash` 等）
  - `name` 与 `clazz` **都为空** → AutoFunction，使用 `properties` 中的
    `dbNum` / `tableNum` / `storeNum` / `mappingFormat` / `dbMethod` / `tableMethod`
  - `properties.columnName`：分片列（legacy 函数必填）
  - `ranges`：范围类函数的区间表，如 `{"0-99": "0", "100-199": "1"}`（值可为节点号或组大小）

支持的算法与属性详见 [commands.md](commands.md#分片算法清单)。

AutoFunction 示例（`dbpartition by hash(id) tbpartition by hash(id) tbpartitions 4` 等价）：

```json
"function": {
  "properties": {
    "dbNum": "2", "tableNum": "4", "storeNum": "1",
    "mappingFormat": "c${targetIndex}/db1_${dbIndex}/sharding_${tableIndex}",
    "dbMethod": "hash(id)", "tableMethod": "hash(id)"
  }
}
```

方法族：`hash`/`mod_hash`/`uni_hash(col)`、`right_shift(col,n)`、
`str_hash(col,start,end,valType,randSeed)`、`yyyymm/yyyydd/yyyyweek/mm/dd/mmdd/week(col)`。
`mappingFormat` 含 `${index}` 且方法属于扁平家族时启用扁平索引空间（与 Java FLATTEN_MAPPING 一致）。

### 广播表 globalTables

```json
"dict": {
  "createTableSQL": "...",
  "broadcast": [
    { "targetName": "ds0", "schemaName": "db1_0" },
    { "targetName": "ds1", "schemaName": "db1_1" }
  ]
}
```

读走第一个节点；INSERT/UPDATE/DELETE/DDL 扇出到全部节点。

### 单表 normalTables

```json
"user_info": { "locality": { "targetName": "ds0", "schemaName": "db1_0", "tableName": "user_info" } }
```

## SQL Server 后端说明（dbType=mssql / sqlserver）

- 使用 `tiberius` TDS 客户端（SQL 认证；TLS 可配 `encrypt=true` /
  `trustServerCertificate=true`）；不支持 Windows/Kerberos/Entra 集成认证；
- URL：`jdbc:sqlserver://host:1433;databaseName=db1;encrypt=true;trustServerCertificate=true`；
  一个数据源固定一个 database，`schemaName` 表示 SQL Server schema；
- 物理表自动创建：逻辑 `CREATE TABLE` 会为每个物理节点生成 T-SQL
  （`schema` 自动创建，`AUTO_INCREMENT`→`IDENTITY(1,1)`，`TEXT/BLOB`→
  `NVARCHAR(MAX)/VARBINARY(MAX)`，`KEY`→`CREATE INDEX`，幂等
  `IF OBJECT_ID(...) IS NULL`）；`repairPhysicalTable` 同样生效。`ENUM/SET`、
  `ON UPDATE`、`FULLTEXT/SPATIAL` 等无法安全映射的构造显式报错；
- 方言：`[bracket]` 标识符、`LIMIT`→`TOP`/`OFFSET … FETCH NEXT`、
  常用函数/布尔映射；upsert/REPLACE/`DELETE LIMIT`/`FOR UPDATE` 等显式报错；
- 事务：`SET autocommit=0`→`BEGIN TRAN`、autocommit=1→`COMMIT`；
  `SAVEPOINT x`→`SAVE TRANSACTION [x]`；跨分片仍非原子；
- **XA 不支持**（SQL Server 无 XA；MSDTC 未实现），`transactionType=xa` 分支显式拒绝；
- `last_insert_id`：INSERT 批次内追加 `@@ROWCOUNT + SCOPE_IDENTITY()`，
  不增加网络往返；非自增表返回 0；多行 INSERT 返回最后生成的 ID
  （`SCOPE_IDENTITY()` 语义，MySQL 原生返回首个 ID）；
- **Always On 读路由**：数据源 URL 支持 `applicationIntent=ReadOnly`
  （TDS Login7 只读意图）与 `multiSubnetFailover=true`。把 AG listener 配成
  master 数据源、把读意图 listener/只读实例配成同一集群的 `replicas`，并设置
  `readBalanceType: BALANCE_ALL_READ`；`slaveThreshold` 会用
  `sys.dm_hadr_database_replica_states.secondary_lag_seconds` 剔除高延迟副本
  （无 `VIEW SERVER STATE` 权限或无 AG 时视为未知，不剔除）。
- Always On 配置示例（同一个 AG listener、两个登录意图）：

`datasources/ds_primary.datasource.json`：

```json
{ "name": "ds_primary", "dbType": "mssql",
  "url": "jdbc:sqlserver://ag-listener:1433;databaseName=db1;encrypt=true;trustServerCertificate=true",
  "user": "mycat", "password": "***" }
```

`datasources/ds_readonly.datasource.json`：

```json
{ "name": "ds_readonly", "dbType": "mssql",
  "url": "jdbc:sqlserver://ag-listener:1433;databaseName=db1;encrypt=true;trustServerCertificate=true;applicationIntent=ReadOnly;multiSubnetFailover=true",
  "user": "mycat", "password": "***" }
```

`clusters/c0.cluster.json`：

```json
{ "name": "c0", "clusterType": "SINGLE_NODE",
  "masters": ["ds_primary"], "replicas": ["ds_readonly"],
  "readBalanceType": "BALANCE_ALL_READ",
  "heartbeat": { "maxRetry": 3, "heartbeatTimeout": 1000, "slaveThreshold": 5 } }
```

写路由固定 `masters[0]`；读路由走只读副本，副本不可用/延迟超阈值时回退主库。


## PostgreSQL 后端说明（dbType=postgresql）

- 官方生态 tokio-postgres 客户端（协议/认证 SCRAM-SHA-256/MD5 零风险封装），
  自研连接池与 MySQL 池同构（免 ping 窗口/上限信号量/事务连接不回池）；
- 物理 SQL 经 `translate_pg_sql` 翻译：MySQL 反引号标识符 → PG 双引号（保大小写，
  与反引号语义一致），其余语法同源直传；MySQL 专有语法由 PG 自身报错（不静默改写）；
- 结果列类型经 prepare 描述缓存映射为 MySQL field_type（文本协议行值原生 &str，
  数值/时间/BYTEA 等全类型无损文本化）；
- 心跳 `select 1`；物理库表由 DBA 手工创建（DDL 自动化 MySQL 专用，与 Oracle 相同）；
- 事务：BEGIN/COMMIT/ROLLBACK/SAVEPOINT 原生下推；
- **XA（2PC）**：`transactionType=xa` 下分支 = 普通 BEGIN，
  `PREPARE TRANSACTION 'gtrid_分支'` / `COMMIT PREPARED` / `ROLLBACK PREPARED`
  （GID 实例级唯一，分支后缀 = 数据源名）；**前置条件：`max_prepared_transactions > 0`**
  （默认 0，PREPARE 会报错）；支持 **MySQL+PG 混合 XA**（两边原生 2PC，恢复按后端分派）。

## Oracle 后端说明

- 路由/分片/归并语义与 MySQL 后端完全一致（逻辑层无方言概念）；
- 物理 SQL 按数据源 dbType 翻译：标识符 `"..."` 引用、`LIMIT`→ROWNUM 包装、
  多行 INSERT→`INSERT ALL`、函数映射（IFNULL→NVL、NOW→CURRENT_TIMESTAMP 裸标识符、
  DATE_FORMAT→TO_CHAR、CONCAT→||、MD5→STANDARD_HASH、GROUP_CONCAT→LISTAGG 等，
  完整清单见 commands.md）；
- 翻译不了的结构显式报错（`INSERT ... ON DUPLICATE KEY UPDATE`、`DROP ... IF EXISTS`、
  `DELETE ... LIMIT` 等）；
- 心跳自动切换为 `select 1 from dual`（SINGLE_NODE 语义，无主从复制探测）；
- `initSqls` 对 Oracle 后端同样生效（连接建立后执行会话初始化 SQL），常用场景：
  设置日期字面量格式以兼容 MySQL 的 `'YYYY-MM-DD HH:MM:SS'` 字面量——
  `"initSqls": ["ALTER SESSION SET NLS_DATE_FORMAT='YYYY-MM-DD HH24:MI:SS'"]`；
- 物理建库建表（DDL 自动化）仅对 MySQL 后端生效，Oracle 分片表由 DBA 手工创建
  （标识符需与配置一致：路由重写使用 `"schema"."table"` 双引号保大小写）；
- 事务语义：驱动默认 autocommit=false，事务外语句由代理层**即时 COMMIT**
  （对应 JDBC autocommit=true）；事务内连接固定，COMMIT/ROLLBACK 走驱动接口；
  连接归还池时回滚净化残留事务。

## 运行模式（server.json `mode`）

- `local`（默认）/ `cluster`：分片路由模式（现有全部能力：分片/归并/DDL 自动化/负载均衡）；
- `proxy`：**纯代理模式**——SQL 不分片、不改写、不归并，原样透传到 `proxy.target`
  （缺省 prototype），仅保留三项增强：**慢 SQL 记录、SQL 结果缓存、SQL 拦截**，
  以及本地管理命令（`show @@*`、KILL、事务/SET/USE、变量探针、缓存/配置 hint）。
  三前端协议（MySQL/PG/TDS）行为一致；`readOnly`/`disallowSqlTypes` 同样生效。

```json
{
  "mode": "proxy",
  "proxy": {
    "target": "ds0",
    "intercepts": [
      { "pattern": "(?i)drop\\s+table", "stmtType": "DDL",
        "errno": 1142, "message": "DDL is blocked by proxy policy" },
      { "pattern": "secret_table", "stmtType": "ALL" }
    ]
  }
}
```

- `proxy.intercepts`：按配置顺序匹配（正则大小写不敏感），首个命中返回权限错误
  （缺省 1142/42000，可自定义 errno/sqlstate/message）；**判定先于 SQL 缓存**，
  缓存命中不能绕过拦截；
- `stmtType`：`ALL`（默认）/`SELECT`/`DML`/`DDL`/`OTHER`；
- 代理模式不做 DDL 配置自动化（CREATE/DROP 等原样透传），不需要 schemas 配置；
- **会话级固定连接**：每个前端会话固定一条后端连接，`-D`/`USE`（含 COM_INIT_DB）、
  `SET`（autocommit/names/sql_mode/用户变量）、`BEGIN/COMMIT/ROLLBACK/SAVEPOINT`
  全部原样下推后端（会话语义与直连一致，临时表可用）；会话结束时连接回滚并**丢弃**
  （会话状态不跨客户端复用）；
- 模式切换（local↔proxy）需重启（热重载仅同步同模式内的配置变更）；
  `check` 命令校验 mode 合法性、proxy.target 存在性、拦截正则可编译。

## sqlcaches/*.sqlcache.json（可选）

SQL 结果缓存配置（Java 目录/字段兼容 + Pro 扩展）：

```json
{ "name": "cnt1",
  "sql": "select count(*) from db1.orders",
  "refreshInterval": 60, "initialDelay": 0,
  "matchMode": "PARAMETERIZED",
  "maxRows": 100000, "maxBytes": 0,
  "schemaName": "db1" }
```

- `refreshInterval`/`initialDelay`：秒（Java 字段名）；`matchMode`：PARAMETERIZED（默认，字面量→?）
  或 EXACT；`schemaName`：缓存 SQL 未库限定时执行所用的默认库（否则以首次命中会话的库为准）；
- `maxRows`/`maxBytes`（0=不限）：结果超限不缓存；缓存二进制文件位于
  `<home>/cache/sqlcache/<name>.bin`（mmap 恢复，重启不失效）。语义详见 commands.md。

## sequences/*.sequence.json（可选）

```json
{ "name": "seq1", "clazz": "...", "targetName": "c0", "schemaName": "db1", "step": 1, "startWith": 1, "incrementBy": 1 }
```

Pro 版支持 Snowflake/计数序列，并在分片/广播表 INSERT 省略自增列时自动填充
（先填充后路由；Oracle 物理表需自行处理 identity/序列）。

---

## 运维命令

```bash
mycat3-pro init  --home ./conf    # 生成默认配置
mycat3-pro check --home ./conf    # 校验配置（引用完整性/URL/分片函数可编译）
mycat3-pro start --home ./conf    # 启动（前台；Ctrl-C 优雅停机）
mycat3-pro version
scripts/mycat3-pro.sh start|stop|restart|status [home]
```
