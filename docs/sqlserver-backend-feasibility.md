# SQL Server 后端接入评估（TDS 客户端 + T-SQL 方言层）

> **实施状态（2026-10）**：已按本文方案落地 `crates/mssql`（`tiberius` 0.13 +
> 自研连接池）、`BackendPool::Mssql`、T-SQL 方言层和 `sharding-sqlserver` Compose 示例。
> 已支持 SQL 认证/TLS、schema 分片、广播/单表、常用类型、`TOP`/`OFFSET FETCH`、本地事务/
> `SET autocommit`/保存点；**DDL 自动化、Always On 只读意图/延迟剔除、端到端流式已实现**；
> **XA/MSDTC、Windows 集成认证仍未实现**。下文保留评估时的判断与估算，作为后续扩展依据。
>
> 已用 SQL Server 2022 容器验证：分片/广播/单表、MySQL/PostgreSQL/TDS 三种前端、
> DECIMAL/DATETIME2/NVARCHAR 类型、重复键 1062 映射、事务/autocommit/保存点、XA 显式拒绝。
>
> 结论先行：**可行，但不是“加一个 dbType 字符串”就能完成**。
> 建议用成熟 TDS 客户端（首选 `tiberius`，PoC 验证 tokio/TLS/类型覆盖）实现
> “SQL Server 分片后端”的 MVP；XA、DDL 自动化、Always On 读副本路由应分阶段，
> 其中 **XA/MSDTC 不建议作为一期目标**。

评估时（v0.3.0）的能力边界：

- 后端：MySQL、PostgreSQL、Oracle；
- SQL Server：仅**前端 TDS 协议**（`tds-frontend` 示例），没有 `dbType=mssql`；
- 因此“SQL Server 分片后端”是新增后端工程，不是配置开关。

当前源码已按第 12 节顺序完成 MVP 后端；限制见顶部“实施状态”。

---

## 1. 现有架构中的接入点

当前后端抽象已经很适合扩展：

| 位置 | 现状 | 接入 SQL Server 需要做什么 |
|---|---|---|
| `crates/server/src/backend.rs` | `BackendPool::{MySql, Pg, Oracle}` + `BackendSession` | 增加 `Mssql` 分支、查询/事务/XA 方法 |
| `crates/pgclient`、`crates/oracle` | 独立后端客户端 crate | 新增 `crates/mssql`（连接、池、类型/错误映射） |
| `crates/sql/src/{pg,oracle}.rs` | MySQL AST → 目标方言 | 新增 `mssql.rs`（T-SQL 翻译） |
| `crates/server/src/state.rs` | `pg_pools` / `oracle_pools` | 增加 `mssql_pools` |
| `crates/server/src/commands.rs` | `show @@datasource` 合并池快照 | 合并 Mssql 池 |
| `crates/server/src/main.rs` | `check` 按 dbType 校验 URL | 增加 SQL Server URL 解析/校验 |
| `crates/server/src/executor.rs` | 结果统一为 `ResultSet` | 复用文本行模型，无需改归并 |
| `crates/server/src/ddl.rs` | 物理 DDL MySQL/SQL Server/PostgreSQL/Oracle 方言分支 | 已接入 |

现有 PG/Oracle 后端已经把“结果字符串行 → MySQL 文本载荷”的路径打通，
归并、广播、三前端回写可以零改动复用；主要成本在 **TDS 客户端** 和 **T-SQL 方言**。

---

## 2. 客户端方案选择

### 2.1 推荐：使用成熟 TDS 客户端（tiberius）

候选：`tiberius`（Rust 社区最成熟的 SQL Server TDS 客户端，支持 SQL 认证、
TLS、TDS 7.x、流式结果；具体 API/Tokio 兼容性在 PoC 锁定版本验证）。

需要包的薄封装（与 `pgclient` 同构）：

- `crates/mssql/src/lib.rs`：配置、URL 解析、类型/错误映射；
- `conn.rs`：`Client` 建连、简单查询/执行、事务状态、ping；
- `pool.rs`：信号量 + 空闲栈 + 免 ping 窗口 + `minCon` 预热；
- `tiberius` 不提供 Mycat 需要的“事务固定连接 + 归并结果模型”，这层必须自研，
  但比自研 TDS 协议小一个数量级。

### 2.2 不建议自研 TDS 客户端

自研需要：Prelogin/TLS 协商、Login7、TDS 包分帧、Token 流（COLMETADATA/ROW/DONE/
ENVCHANGE/ERROR/INFO）、SQL 认证、类型系统、RPC（sp_executesql）、事务、连接池。
现有 `crates/server/src/tds.rs` 是**前端服务端**（解析 Login7、生成 token），
不能直接变成客户端；可复用的只有包格式/常量级知识。

自研估计新增 5,000–8,000 行且高风险，不建议。

### 2.3 SQLx / ODBC 备选

- `sqlx` 的 MSSQL 支持可作为备选，但会引入第二套数据库抽象、类型/池语义，
  与当前“原始文本行 + 自研池”的模型不完全贴合；
- ODBC/FreeTDS 需要额外原生库和部署依赖，不适合当前单二进制 Docker 镜像的定位。
- 结论：**优先 tiberius；PoC 不通过再评估 SQLx，最后才是自研。**

---

## 3. 目标能力与验收矩阵

### 一期 MVP（建议）

- SQL 认证（SQL Server 2019/2022；Azure SQL 可选）；
- 单实例/单个数据库，物理分片用 SQL Server **schema**（与 PG/Oracle 模型一致）；
- 分片表、广播表、单表的路由/读写/归并（count/sum/order/limit）；
- MySQL/PG/TDS 三种前端都能访问 SQL Server 分片；
- 本地事务（单数据源固定连接）、`COMMIT/ROLLBACK/SAVEPOINT`；
- 物理 DDL：init SQL 预建，或逻辑 `CREATE TABLE` 自动翻译为 T-SQL，`dbType: "mssql"` / `"sqlserver"`；
- 基础类型映射、常见错误映射、连接池；
- TLS 可选（SQL Server / Azure SQL 的加密要求）；
- `check` 校验、`show @@datasource`、`/metrics`、文档与 Docker 示例。

### 明确不在一期

- XA / MSDTC 分布式事务（SQL Server 没有 XA；见 §7）；
- ~~DDL 自动化（T-SQL 建表/分片物理表）~~（已实现）；
- ~~Always On 可读副本/延迟感知路由~~（已实现，见 §8）；
- Windows/Kerberos 集成认证、Entra ID 认证；
- `geography/geometry/hierarchyid/sql_variant` 的完整语义；
- `MERGE` 自动改写、复杂 T-SQL 长尾。

### 验收标准

1. `check` 通过，`select 1` 心跳；
2. 两种 schema 分片（至少 2 个物理 schema）下，MySQL/PG/TDS 前端完成
   INSERT/点查/跨分片 count/order/limit/广播写；
3. 唯一键/死锁/对象不存在错误能返回合理的 MySQL errno/SQLSTATE；
4. 事务提交/回滚/保存点在单数据源内语义正确；
5. 连接池上限、空闲回收、故障重连可观测；
6. 同一套 demo 在 Docker Compose 中可复现。

---

## 4. 配置与物理模型

### 4.1 `dbType`

建议同时接受：

```json
{ "dbType": "mssql" }
```

并兼容 `sqlserver` / `mssqlserver` 别名；在 `check` 中统一归一化。

JDBC URL 解析：

```text
jdbc:sqlserver://sqlserver:1433;databaseName=db1;encrypt=true;trustServerCertificate=true
jdbc:sqlserver://sqlserver:1433;database=db1;encrypt=false
```

支持 `databaseName`/`database`、`encrypt`、`trustServerCertificate`、
`hostNameInCertificate`、`loginTimeout`；用户/密码继续用 datasource 字段。

### 4.2 database 与 schema

Mycat 当前的 `partition.data = [target, schemaName, tableName]` 只有三级。
推荐一期：

- **一个 datasource 连接一个 SQL Server database**；
- `schemaName` 表示该 database 内的 **SQL Server schema**（如 `db1_0`/`db1_1`）；
- 物理 SQL 为 `[schema].[table]`（database 由连接固定）。

这与 PostgreSQL 的模型一致，避免三部分名/跨库事务的复杂度。
如果业务要求“每个分片一个 database”，一期可用多个 datasource（每个 database 一条 URL），
`schemaName` 仍填 schema；不要混用两种模型。

### 4.3 物理表命名

SQL Server 默认大小写不敏感，但 Mycat 会做标识符引用转换。建议：

- 物理表名、schema 名在配置和 DBA 建表中保持一致；
- 方言层把 MySQL 反引号转换为 `[...]`，并转义 `]` → `]]`；
- 物理表可自动生成 DDL（`translate_ddl`），init SQL 可选。

---

## 5. T-SQL 方言层设计

新增 `crates/sql/src/mssql.rs`，接口与 `translate_pg_sql` / `translate_oracle_sql` 一致：

```rust
pub fn translate_mssql_sql(sql: &str) -> Result<String, DialectError>
```

策略：**只改语法形状，不改语义；不能翻译的结构显式报错**（沿用 Oracle 翻译器原则）。

### 5.1 标识符

- 反引号 `` `x` `` → `[x]`；`]` 转义为 `]]`；
- 未引用标识符保持原样（SQL Server 默认 collation 通常大小写不敏感）；
- 保留 schema/table 的两段名；`dbo` 默认 schema 不作为隐式要求。

### 5.2 分页（最高优先级）

MySQL 逻辑 SQL 可能来自 MySQL/PG/TDS 前端（TDS 前端的 `TOP n` 已被改写为 `LIMIT n`）。

| MySQL | T-SQL |
|---|---|
| `LIMIT n` | 有 `ORDER BY`：`OFFSET 0 ROWS FETCH NEXT n ROWS ONLY`；无 `ORDER BY`：`SELECT TOP (n) ...` |
| `LIMIT m, n` / `LIMIT n OFFSET m` | 有 `ORDER BY`：`OFFSET m ROWS FETCH NEXT n ROWS ONLY`；无 `ORDER BY`：`ROW_NUMBER() OVER (ORDER BY (SELECT NULL))` 包装 |
| `LIMIT` 在子查询 | 同样包装，需保留外层语义；不能安全改写则显式报错 |

`OFFSET/FETCH` 需要 `ORDER BY`；无排序的分页本身语义不确定，文档必须说明
“结果顺序由 ROW_NUMBER 的占位排序决定”。不要为追求“能跑”而静默改变排序语义。

### 5.3 表达式/函数

建议首批覆盖：

- `IFNULL`→`ISNULL`/`COALESCE`；`IF`→`CASE`；
- `NOW()`/`CURRENT_TIMESTAMP`→`SYSDATETIME()`/`GETDATE()`；
  `CURDATE()`→`CAST(GETDATE() AS date)`；
- `DATE_FORMAT`→`FORMAT`（SQL Server 2012+）或 `CONVERT`；
- `GROUP_CONCAT`→`STRING_AGG`（SQL Server 2017+；需 `WITHIN GROUP` 语义）；
- `CONCAT`、`LEFT/RIGHT`、`SUBSTRING`、`LEN`、`CHAR_LENGTH`；
- `UUID`→`NEWID`、`MD5`→`HASHBYTES('MD5', ...)`；
- 布尔字面量 `TRUE/FALSE`→`1/0`；
- `LIMIT` 不在表达式中出现时，避免误改。

明确报错/不支持（一期）：

- `INSERT ... ON DUPLICATE KEY UPDATE`（可二期改 `MERGE`）；
- `REPLACE INTO`、`INSERT IGNORE`；
- `UPDATE/DELETE ... LIMIT`（可二期用 CTE + TOP）；
- 未知函数、MySQL 特有 `STATUS`/`SHOW`（这些通常已被本地层拦截）；
- `SELECT ... FOR UPDATE`、`LOCK IN SHARE MODE`（SQL Server 用锁提示/隔离级别，语义不同）；
- MySQL 特有的 `ENUM`/`SET`/`UNSIGNED` DDL（二期映射或显式拒绝）。

### 5.4 DDL（已实现）

`crates/sql/src/mssql.rs::translate_ddl` 把 Mycat 规范化的 MySQL
`CREATE TABLE __TABLE__ (...)` 翻译为幂等 T-SQL：

- 表名用 `[schema].[table]` 方括号引用，schema 不存在时先 `CREATE SCHEMA`；
- `AUTO_INCREMENT`→`IDENTITY(1,1)`；
- 整数按有/无符号升位（`TINYINT(1)`→`BIT`，`TINYINT`→`SMALLINT`，
  `INT UNSIGNED`→`BIGINT`，`BIGINT UNSIGNED`→`DECIMAL(20)`）；
- `DATETIME` 保留，`TEXT`→`NVARCHAR(MAX)`，`BLOB`→`VARBINARY(MAX)`，
  `VARCHAR`→`NVARCHAR`，`UUID`→`UNIQUEIDENTIFIER`；
- MySQL `KEY/INDEX`→独立的 `CREATE INDEX`；`UNIQUE KEY`→`UNIQUE` 约束；
- 剥离 `ENGINE`/`CHARSET`/`COLLATE`/表注释；整批包在
  `IF OBJECT_ID(N'…', N'U') IS NULL BEGIN … END` 中，可重复执行；
- `ENUM/SET`、`ON UPDATE`、生成列、`FULLTEXT/SPATIAL` 显式报错；
- `create_physical_node` 与 `repairPhysicalTable` 都走该路径（`dbType=mssql`）。
- 已知边界：`CREATE TABLE ... LIKE`、`ALTER TABLE` 的 T-SQL 方言改写未覆盖；
  IDENTITY 列显式插入仍需 `SET IDENTITY_INSERT`（由 DBA/业务处理）。

---

## 6. 结果模型与类型映射

SQL Server 后端应走“类型化行 → 字符串行 → MySQL 文本载荷”路径（与 PG/Oracle 相同），
而不是 MySQL 的原始载荷快路径；这会多一层 CPU，但归并/三前端零改动。

建议映射（MySQL `field_type`）：

| SQL Server | MySQL field_type | 说明 |
|---|---|---|
| BIT | TINY | 0/1 |
| TINYINT/SMALLINT/INT/BIGINT | TINY/SHORT/LONG/LONGLONG | 注意 SQL Server TINYINT 无符号 |
| REAL/FLOAT | FLOAT/DOUBLE | 精度 |
| DECIMAL/NUMERIC/MONEY/SMALLMONEY | NEWDECIMAL | 字符串化，保留精度 |
| CHAR/VARCHAR/NCHAR/NVARCHAR/TEXT/NTEXT/XML | VAR_STRING | Unicode → UTF-8 |
| BINARY/VARBINARY/IMAGE/ROWVERSION | BLOB | `\x` 十六进制 |
| DATE | DATE | |
| TIME | TIME | |
| DATETIME/DATETIME2/SMALLDATETIME | DATETIME | 统一文本格式 |
| DATETIMEOFFSET | DATETIME | 保留偏移或拒绝（需明确） |
| UNIQUEIDENTIFIER | VAR_STRING | GUID 文本 |
| GEOGRAPHY/GEOMETRY/HIERARCHYID/SQL_VARIANT | VAR_STRING 或显式不支持 | 首期建议显式报错/文本化 |

必须处理：

- NULL 保真；
- `DECIMAL` 精度/尾零；
- `DATETIME2` 纳秒精度与 MySQL 客户端显示的差异；
- `VARCHAR` 的 code page 与 `NVARCHAR` 的 UTF-16 转换（tiberius 输出的 String 要确认编码）；
- `MONEY` 4 位小数；
- `BIT` 的 `0/1` 与 TRUE/FALSE；
- `last_insert_id`：SQL Server 的 TDS 结果没有 MySQL OK 包的 insert id；
  Mycat 在 INSERT 语句后追加 `SELECT CAST(@@ROWCOUNT...)`/`SCOPE_IDENTITY()`
  同批次返回（单次往返），非自增表返回 0；多行 INSERT 返回首个生成的 ID
  （由 `SCOPE_IDENTITY()` 与 `@@ROWCOUNT` 推导；假定 identity 步长 1）。

---

### 6.1 大结果集与端到端流式（已实现）

`MssqlConnection::query_rows` 仍保留增量解码（元数据/行一到即转文本行，
避免 `into_results()` 的双份峰值内存）。

SQL Server **端到端协议级流式**已落地：

- `MssqlPool::stream_query` 在后台任务中持有连接，按 `QueryItem` 逐行读取，
  通过容量 128 的有界 channel 施加背压；消费者消失/超时会丢弃连接（事务/残留
  数据不回到池）。
- `executor::execute_route_stream` 仅对“单任务、SELECT、无 merge、无 hint、
  autocommit、非显式事务、SQL Server 数据源”返回 `StreamingResult`；其他情况
  回退物化路径。
- 三种前端均已接入：
  - MySQL 文本协议：列定义后逐行写包，64KB flush；
  - PostgreSQL 简单查询：RowDescription 后逐行 DataRow，64KB 批次 flush；
  - TDS SQL Batch：COLMETADATA 后逐行 ROW token，EOM 只在最终 DONE 包，
    64KB 批次 flush。
- SQL 缓存命中在进入流式前完成；流式结果不写入缓存。
- 事务内固定连接、多分片归并、MySQL 二进制协议/PG 扩展协议仍走物化路径，
  语义不变（只影响性能，不影响结果）。

实测：SQL Server 2022 上 5000 行结果经 MySQL CLI / psql / sqlcmd 三种前端
分别接收完整 5000 行，`DEBUG` 日志均确认 `SQL Server streaming result started`。

---

## 7. 事务、隔离级别与 XA

### 7.1 本地事务

SQL Server 默认 autocommit；Mycat 的 `SET autocommit=0` 语义需要翻译：

- `SET autocommit=0` → 在固定连接上 `BEGIN TRAN`（首条语句前）；
- `SET autocommit=1` → 提交当前事务；
- `BEGIN/START TRANSACTION` → `BEGIN TRAN`；
- `COMMIT`→`COMMIT`，`ROLLBACK`→`ROLLBACK`；
- `SAVEPOINT x`→`SAVE TRANSACTION x`；
- `ROLLBACK TO SAVEPOINT x`→`ROLLBACK TRANSACTION x`；
- `RELEASE SAVEPOINT` 不支持。

这些需要扩展 `BackendSession::tx_command`/`set_forward`，并保持“一个事务固定一个连接”。
跨 SQL Server database/实例的分片写仍各用独立连接，**一期不承诺跨分片原子性**。

### 7.2 隔离级别

Mycat 用户配置的 `isolation` 是 MySQL 语义（3=REPEATABLE READ）。
SQL Server 默认 READ COMMITTED；需要把支持的级别映射到
`SET TRANSACTION ISOLATION LEVEL ...`（READ COMMITTED/REPEATABLE READ/SERIALIZABLE；
SNAPSHOT 需数据库开启）。不能映射的显式报错，不能静默降级。

### 7.3 XA / MSDTC

**SQL Server 没有 XA 协议。** 分布式事务依赖 MSDTC（OLE-TX/DTM、RPC、Windows 配置），
`tiberius` 不提供 XA/MSDTC 接口；在 Linux/容器中部署 MSDTC 还涉及额外组件和网络/安全配置。

建议：

- 一期 `transactionType=xa` 对 SQL Server 分支**显式拒绝**（与 Oracle 相同）；
- 文档明确“跨 SQL Server 分片写不承诺原子性，需业务补偿”；
- 如果业务强制要求分布式事务，单独立项做 MSDTC 网关/外部事务协调器，
  不要把它伪装成普通 XA；预计额外 4–8 周且运维风险高。

---

## 8. 连接池、TLS 与高可用

- 连接池：仿 `pgclient` 的信号量 + 空闲栈 + `PING_AFTER_IDLE` + `minCon` 预热；
  `maxCon` 必须按 SQL Server `max worker threads`/内存/其它客户端预算。
- TLS：`encrypt=false|true|strict`、`trustServerCertificate`、
  `hostNameInCertificate`；Azure SQL 通常要求加密。
- 认证：一期 SQL 登录；Windows/Kerberos/Entra ID 不做（tiberius 的集成认证特性
  需要额外依赖/平台，PoC 单独评估）。
- 命名实例：一期支持 `host:port`；`host\instance` 的 SQL Browser(UDP 1434)
  可放到二期。
- Always On：AG listener 可作为一个 datasource；URL 增加
  `applicationIntent=ReadOnly` 后 TDS Login7 设置只读意图，SQL Server 将登录
  路由到可读副本；`multiSubnetFailover=true` 支持多子网 listener。
  Mycat 侧把读意图数据源放入集群 `replicas`，用 `BALANCE_ALL_READ` 只读副本、
  无副本时回退 master。`slaveThreshold` 由心跳查询
  `sys.dm_hadr_database_replica_states.secondary_lag_seconds` 实现延迟剔除；
  账号缺少 `VIEW SERVER STATE` 或非 AG 环境时视为未知（不剔除）。
  listener 故障仍由 AG 自身切换。
- SQL Server 连接数/内存：比 MySQL 更重，`maxCon` 从较小值（8–32/分片）起步。

---

## 9. 错误映射

建议首批映射（保留原始错误文本）：

| SQL Server | 场景 | MySQL 错误 |
|---|---|---|
| 2627 / 2601 | 唯一键/主键冲突 | 1062 / 23000 |
| 1205 | 死锁牺牲者 | 1213 / 40001 |
| 547 | 外键约束 | 1451/1452 / 23000 |
| 208 | 对象不存在 | 1146 / 42S02 |
| 229 / 230 | 权限拒绝 | 1142 / 42000 |
| 18456 | 登录失败 | 1045 / 28000 |
| 4060 | 数据库不可用 | 1049 / 42000 |
| 8152 / 2628 | 字符串截断 | 1406 / 22001 |
| 245 / 8114 | 类型转换失败 | 1292 / 22007 |
| 超时/网络 | 连接级 | 1105 / HY000，销毁连接 |

实现上需要区分“SQL 级错误（连接可复用）”与“连接级错误（销毁连接）”，
与当前 `BackendError::Server` / `Conn` 的语义一致。tiberius 的错误分类在 PoC 中确认。

---

## 10. 工作量与代价估算

以下以“使用 tiberius、1 名熟悉 Rust/数据库的资深工程师”为基准；实际受 SQL Server
版本目标、TLS/Azure、类型长尾和测试环境影响。

| 阶段 | 内容 | 新增/改动代码（估算） | 工期（估算） |
|---|---|---|---|
| PoC | tiberius 建连、SQL 认证、TLS、简单查询/类型/错误；Docker SQL Server | 500–1,000 行 | 3–5 天 |
| MVP | `crates/mssql`（连接/池/类型/错误）、BackendPool/State/commands/check、基础 T-SQL、本地事务、手工 DDL | 2,500–3,500 行 | 3–4 周 |
| Production hardening | 分页/函数/DDL 边界、TLS/Azure、错误矩阵、池/故障测试、文档 | 2,000–3,000 行 | 3–4 周 |
| DDL 自动化（可选） | IDENTITY/类型/索引/分片物理 DDL、repair | 500–1,000 行 | 1–2 周 |
| Always On/只读副本（可选） | 读路由/延迟/故障边界 | 500–1,000 行 | 1–2 周 |
| XA / MSDTC（不推荐） | DTC/事务协调、部署/故障恢复 | 3,000–5,000 行 | 4–8 周，高风险 |
| 自研 TDS 客户端（不推荐） | 协议/TLS/RPC/类型/测试 | 5,000–8,000 行 | 6–10 周，高风险 |

**合计建议口径：**

- 一期可用的 SQL Server 分片后端（无 XA/无 DDL 自动化）：
  **约 4–6 周（20–30 人日）**；
- 达到“生产可上线”的稳健度（TLS/Azure、类型/错误/分页、完整测试、文档、Docker demo）：
  **约 6–8 周（30–40 人日）**；
- 加上 DDL 自动化/Always On：**8–10 周**；
- XA/MSDTC 或自研 TDS 不应包含在常规排期内。

如果两人并行（客户端/方言/测试分工），可缩短约 20–30%，但集成测试是串行瓶颈。

---

## 11. 主要风险与缓解

| 风险 | 影响 | 缓解 |
|---|---|---|
| tiberius 与当前 Tokio/加密栈兼容性 | PoC 可能卡住 | 先做 1 天连接/查询/TLS PoC；锁定版本；备选 SQLx |
| T-SQL 分页/函数长尾 | 语义错误 | 只翻译明确规则；不支持即报错；测试驱动 |
| 类型/编码（NVARCHAR/VARCHAR、DATETIME2、DECIMAL、GUID） | 结果错/乱码 | 类型矩阵测试；Unicode 优先；二进制显式十六进制 |
| `SET autocommit`/隔离级别差异 | 事务语义错 | 事务层单测 + SQL Server 集成测试；显式拒绝无法映射项 |
| last_insert_id 语义 | 客户端拿错 ID | INSERT 同批次 `SCOPE_IDENTITY()`；非自增表 0（已实现） |
| XA 被误认为可用 | 数据不一致 | 明确拒绝 SQL Server XA；文档/错误信息 |
| Always On/只读副本 | 读到旧数据 | `applicationIntent=ReadOnly` + `slaveThreshold` 延迟剔除（已实现）；权限/非 AG 环境不剔除 |
| SQL Server  Licensing/资源 | 成本/部署 | 使用 Developer/Express 做开发；生产按实际版本；CI 资源预算 |
| 测试环境（SQL Server 容器 2GB+、启动慢） | CI 成本 | 复用容器、测试分组、不把 SQL Server 放进默认全量测试 |

---

## 12. 推荐实施顺序

1. **PoC（3–5 天）**：Docker SQL Server 2022 + tiberius；验证 SQL 认证、TLS、
   `SELECT 1`、基础类型、错误分类、Tokio 兼容。
2. **MVP 后端（3–4 周）**：
   - 新 `crates/mssql`（URL/配置/连接/池/类型/错误）；
   - `BackendPool::Mssql`、State 池、`check`、`show @@datasource`；
   - 单数据库 + schema 分片、广播/单表、归并；
   - 手工 DDL；本地事务；显式拒绝 XA；
   - Compose demo（`sharding-sqlserver`）+ 三前端验收。
3. **T-SQL 方言完善（2–3 周）**：分页、函数、布尔、INSERT/UPDATE/DELETE 边界、
   DDL 预检；错误信息明确。
4. **生产硬化（2–3 周）**：TLS/Azure、错误矩阵、池/故障、性能、文档、CI。
5. **可选**：DDL 自动化、Always On、MSDTC（单独立项）。

---

## 13. 最终建议

- **如果 SQL Server 是核心分片后端**：按上述 MVP 立项，先不要承诺 XA/DDL 自动化；
  用 tiberius + 手工 DDL + 明确能力矩阵交付，再按业务迭代。
- **如果只是偶尔读取 SQL Server**：优先 ETL/CDC 到 MySQL/PG，或用应用层直连，
  比新增一个完整后端更便宜。
- **不要自研 TDS 客户端**，也不要用“本地 2PC”伪装 XA；SQL Server 的分布式事务
  是 MSDTC 领域，成本和运维风险应单独立项评估。

---

## 14. 代码参考

- `crates/server/src/backend.rs`：后端池/会话/方言分派
- `crates/pgclient/src/{conn,pool}.rs`：最接近的薄客户端/池模板
- `crates/oracle/src/lib.rs`：类型映射/错误分类/事务固定连接
- `crates/sql/src/{pg,oracle}.rs`：方言翻译器结构
- `crates/server/src/tds.rs`：TDS **前端**实现（不可直接当客户端）
- `crates/server/src/ddl.rs`：物理 DDL 分支（MySQL + SQL Server）
- `examples/docker/tds-frontend/`：SQL Server 作为前端的现有示例

---

## 15. 集成验证记录（2026-10-21 增补）

本轮修复的回归测试（PostgreSQL 16 + Oracle Free 23ai + SQL Server 2022）：

- PG `SET autocommit=0 → INSERT → SET autocommit=1`：新连接可见已提交行；不再向 PG
  透传 MySQL 的 `SET autocommit`，也不会出现 `25P02 transaction is aborted`。
- PG 单参与者 XA：`BEGIN/INSERT/COMMIT` 后 `pg_prepared_xacts` 为 0，行已提交。
- PG `repairPhysicalTable`：DRDS 创建的表删除物理表后可通过 repair 重建。
- PG 查询超时（`executeTimeout`）：`pg_sleep(3)` 约 1s 返回 1105 并销毁连接。
- PG 唯一键错误映射为 MySQL 1062/23000；`BOOLEAN` 结果输出 1/0。
- Oracle `SET autocommit=1` 提交隐式事务；唯一键错误映射 1062/23000。
- SQL Server：分片 `AUTO_INCREMENT` 表物理列为普通 BIGINT（`is_identity=0`），
  由 Mycat 序列生成 id 后可多行插入；普通表仍为 IDENTITY；`BIT NULL` 可读写。
- SQL Server `last_insert_id`：单行 3、多行 2 行返回首个 id 4（此前返回末个 5）。
- SQL Server 索引 repair：删除物理索引后 `repairPhysicalTable` 会补建。
- 新增 e2e：mock MySQL 后端验证 `SET autocommit=1` 发送 COMMIT 且不透传 SET。

## 15. 集成验证记录（2026-10-20）

环境：SQL Server 2022 Developer 容器（`mcr.microsoft.com/mssql/server:2022-latest`），
仓库源码构建的 Mycat 镜像；`examples/docker/sharding-sqlserver` Compose 编排。

覆盖：

- `orders` 取模 4 分片、点查裁剪、跨分片 `count`/`sum`/`order by + limit`；
- `tenant_order` 枚举分片、`dict` 广播、`user_info` 单表；
- MySQL / PostgreSQL / TDS(sqlcmd) 三种前端访问同一 SQL Server 后端；
- `DECIMAL/DATETIME2/NVARCHAR/BIT` 等类型映射；
- 普通事务提交/回滚、`SET autocommit=0/1`、`SAVEPOINT`；
- 重复键错误映射（SQL Server 2627 → MySQL 1062/23000）；
- XA 请求显式拒绝；
- 自签 TLS + `trustServerCertificate=true` 连接；
- SQL Server 初始化脚本重复执行（Compose `up` 多次）。

2026-10-20 增补（物理 DDL / last_insert_id / 延迟心跳）：

- 逻辑 `CREATE TABLE ddl_demo (...)`：自动建 `db1` schema、表、`idx_ddl_name` 索引；
- 逻辑分片 `CREATE TABLE ... DBPARTITION BY hash(id) DBPARTITIONS 2
  TBPARTITION BY hash(id) TBPARTITIONS 2`：4 个物理表落在 `db1_0/db1_1`，
  插入 4 行后 Mycat 查询与物理核对均为 4；
- `AUTO_INCREMENT` → `IDENTITY(1,1)`，单行/多行 INSERT 均成功；
- `DROP TABLE` 物理删除 SQL Server 表；
- 删除物理表后 `/*!mycat:repairPhysicalTable*/` 能按配置重建；
- 通过 Mycat 客户端探针验证 INSERT OK 包：单行 `affected=1, last_insert_id=1`，
  多行 `affected=2, last_insert_id=3`（同批次读取，无额外往返）；
- 表不存在/列不存在等 T-SQL 错误映射保持；
- Always On：URL 解析 `/config`、Login7 只读 flags 已实现；本环境无 AG
  listener/可读副本，未做真实 AG 故障切换测试（需 Windows 集群或外部 AG 环境）。

验证中发现并修复：

1. **物理初始化非幂等**：第二次 `docker compose up` 时 `CREATE TABLE` 报 2714；
   改为 `IF OBJECT_ID(...) IS NULL` 包裹每个物理表。
2. **事务控制经 sp_executesql 导致 SQL Server 266**：
   `BEGIN TRAN` 放在 `execute` RPC 中会与 RPC 事务计数不一致；
   事务控制/`USE`/`SET` 改走 SQL batch（`simple_query`）。
3. **旧 Docker Hub `latest` 不含 `dbType=mssql`**：示例 Compose 增加源码 `build`
   上下文，并在文档注明重建镜像。
4. **心跳每 5 秒新建 TLS/Login7 连接**：按数据源缓存 SQL Server 心跳连接，
   失败时移除重连。
5. **连接池取连接未执行 idleTimeout**：取空闲连接时淘汰超时项。
6. **示例脚本可执行位/重复初始化**：`demo.sh` 增加可执行位，初始化幂等。
7. **MySQL `KEY idx(col)` 在 SQL Server 被丢弃**：翻译层改为生成独立
   `CREATE INDEX`；`UNIQUE KEY` 转为 `UNIQUE` 约束（测试覆盖）。
8. **`DROP TABLE` 对 SQL Server 静默跳过**：增加 mssql 物理删除分支并验证。
9. **`LAST_INSERT_ID` 额外往返**：INSERT 批次内追加
   `@@ROWCOUNT + SCOPE_IDENTITY()`，单次往返返回；后续修复为多行返回首个 ID。

结果：上述集成用例全部通过；`cargo test --workspace` 共 **379** 个测试通过。

2026-10-21 增补（跨后端物理 DDL + 端到端流式）：

- PostgreSQL 16：`repairPhysicalTable` 自动创建 `db1_0/db1_1` schema、4 个分片物理表、
  `dict` 广播表、普通表与索引；逻辑 `CREATE TABLE pg_demo` 自动建表/索引；
  `DROP TABLE` 物理删除；`RENAME TABLE` 按 PG 方言重命名；`ALTER ... ADD COLUMN`、
  `CREATE/DROP INDEX ... ON` 均通过。
- Oracle Free 23ai：自动物理建表（`NUMBER/VARCHAR2/TIMESTAMP`、identity、索引、
  幂等 PL/SQL）、`DROP TABLE`、`RENAME TABLE`、`repairPhysicalTable` 通过；
  默认 `schemaName` 使用数据源连接用户。
- SQL Server 2022：逻辑 `ALTER TABLE ADD COLUMN`、`CREATE/DROP INDEX ... ON`、
  `RENAME TABLE`（`sp_rename`）通过。
- 流式：5000 行结果经 MySQL CLI / psql / sqlcmd 三种前端完整接收，
  DEBUG 日志均确认进入 `SQL Server streaming result started` 路径。
- 2026-10-21 修复回归：PG/Oracle `SET autocommit=1` 提交、PG 单参与者 XA
  `COMMIT PREPARED`、DRDS 表 repair、物理 DDL 失败不再假成功、PG 查询超时、
  Oracle 超时连接销毁、分片自增表 SQL Server 普通列生成、SQL Server identity
  首 ID、可空 BIT 类型、PG/Oracle SQLSTATE/ORA 错误码映射、PG/Oracle 池
  `minCon`/`idleTimeout`/`initSqls`/连接超时、索引 repair。

CI / 镜像验收（2026-10-21，v0.3.2）：

- GitHub Actions `CI` run `39606911776`：379 个单测 + SQL Server 2022 Compose 集成
  （含逻辑 `CREATE TABLE ddl_demo`、索引校验、事务/XA 拒绝、三种前端）全部通过；
- 镜像 `mycat/mycat3-pro:v0.3.2`、`0.3.2`、`latest` 已发布，多架构
  manifest digest `sha256:066af41088348b218f23de47b6280a98e1208f0a1fe1d3b6ece4aa584f0c07ad`
  （`linux/amd64` + `linux/arm64`）；Docker Hub Verify 通过并同步简介。
