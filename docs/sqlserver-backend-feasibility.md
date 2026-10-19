# SQL Server 后端接入评估（TDS 客户端 + T-SQL 方言层）

> **实施状态（2026-10）**：已按本文方案落地 `crates/mssql`（`tiberius` 0.13 +
> 自研连接池）、`BackendPool::Mssql`、T-SQL 方言层和 `sharding-sqlserver` Compose 示例。
> 已支持 SQL 认证/TLS、schema 分片、广播/单表、常用类型、`TOP`/`OFFSET FETCH`、本地事务/
> `SET autocommit`/保存点；**XA/MSDTC、Windows 认证、Always On 读路由、DDL 自动化仍未实现**。
> 下文保留评估时的判断与估算，作为后续扩展依据。
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
| `crates/server/src/ddl.rs` | 物理 DDL 仅 MySQL 自动生成 | 一期跳过（手工 DDL），二期加 T-SQL 生成 |

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
- 手工物理 DDL（init SQL），`dbType: "mssql"` / `"sqlserver"`；
- 基础类型映射、常见错误映射、连接池；
- TLS 可选（SQL Server / Azure SQL 的加密要求）；
- `check` 校验、`show @@datasource`、`/metrics`、文档与 Docker 示例。

### 明确不在一期

- XA / MSDTC 分布式事务（SQL Server 没有 XA；见 §7）；
- DDL 自动化（T-SQL 建表/分片物理表）——二期；
- Always On 可读副本/延迟感知路由——二期或不做；
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
- 物理表由 DBA/init SQL 创建（一期不自动生成 DDL）。

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

### 5.4 DDL（二期）

物理建表一期手工；如果二期做自动化：

- `AUTO_INCREMENT`→`IDENTITY(1,1)`；
- `DATETIME`→`DATETIME2`，`TEXT`→`NVARCHAR(MAX)`，`BLOB`→`VARBINARY(MAX)`；
- `TINYINT(1)`→`BIT` 的语义权衡；
- `UNSIGNED` 无法直接表达：升位（INT UNSIGNED→BIGINT）或拒绝；
- `CREATE TABLE IF NOT EXISTS`→SQL Server 2016+ 的 `DROP ... IF EXISTS`/条件判断；
- `ENGINE=InnoDB`/`CHARSET` 等 MySQL 表选项要剥离；
- IDENTITY 列显式插入需要 `SET IDENTITY_INSERT`，与 Mycat 的序列填充/DDL 自动化交互要单独设计。

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
  一期应返回 0，或单独 `SELECT SCOPE_IDENTITY()`/`OUTPUT INSERTED.id`；
  不能伪造。

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
- Always On：把 AG listener 当作一个 datasource 即可；Mycat 的
  `readBalanceType`/延迟感知不适用于 AG 副本，一期不要承诺读写分离；
  listener 故障由 AG 自身切换，Mycat 表现为连接失败→重连。
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
| last_insert_id 语义 | 客户端拿错 ID | 一期返回 0/文档；二期 `OUTPUT`/`SCOPE_IDENTITY` |
| XA 被误认为可用 | 数据不一致 | 明确拒绝 SQL Server XA；文档/错误信息 |
| Always On/只读副本 | 读到旧数据 | 一期无读写分离；AG listener 单写；二期单独设计 |
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
- `crates/server/src/ddl.rs`：物理 DDL 的 MySQL-only 分支
- `examples/docker/tds-frontend/`：SQL Server 作为前端的现有示例
