# 命令参考

## 运行模式

- **分片模式**（`server.mode=local|cluster`，默认）：本文其余章节描述的全部能力；
- **纯代理模式**（`server.mode=proxy`）：SQL 原样透传到单一 target（`proxy.target`），
  不做分片/改写/归并；仅保留 **慢 SQL 记录**（`showSlowSql`/`setSqlTimeFilter`）、
  **SQL 结果缓存**（`createSqlCache` 等，语义不变）、**SQL 拦截**
  （`server.proxy.intercepts`，判定先于缓存）与本地管理命令
  （`show @@*`/KILL/事务/SET/USE/变量探针/hint）；三前端协议一致；
  `readOnly`/`disallowSqlTypes` 生效；DDL 配置自动化停用（原样透传）。
  会话级固定后端连接：`-D`/`USE`/`SET`/`BEGIN…COMMIT/ROLLBACK/SAVEPOINT` 原样下推
  （会话语义与直连一致）；配置与边界详见 configuration.md。

## 多协议连接（同一端口 8066）

服务端对每个新连接做字节级协议嗅探（对应 Java `ProtocolDetector`），三种客户端可混用：

```bash
# MySQL
mysql -h127.0.0.1 -P8066 -uroot -p123456 -Ddb1

# PostgreSQL（psql / libpq / pgjdbc；MD5 或空密码 trust 认证；无 TLS）
psql "postgresql://root:123456@127.0.0.1:8066/db1?sslmode=disable"

# SQL Server（sqlcmd / mssql-jdbc / jTDS；TDS 7.2+；必须 encrypt=false）
sqlcmd -S 127.0.0.1,8066 -U root -P 123456
# JDBC: jdbc:sqlserver://127.0.0.1:8066;encrypt=false;databaseName=db1
#       （预处理语句建议 prepareMethod=executesql，即 sp_executesql 路径）
```

三种协议共享同一执行引擎（`engine::execute_sql` → analyse → route → executor），
因此 SQL 能力、分片路由、归并语义、管理命令完全一致。差异仅在协议层：

| 能力 | MySQL | PostgreSQL | SQL Server (TDS) |
|---|---|---|---|
| 简单查询/DML/DDL | ✓ | ✓ | ✓ |
| 预处理语句 | COM_STMT_*（单节点） | ✓ 扩展协议 Parse/Bind/Execute（$n 参数） | ✓ RPC sp_executesql（? 参数） |
| 事务 | ✓ | ✓（ReadyForQuery 携带 I/T 状态） | ✓（SQL batch） |
| 多语句批 | ✓（MORE_RESULTS） | ✓（'Q' 多结果块） | ✓ |
| 结果类型 | 原生透传 | MySQL 类型 → PG OID 映射（int2/4/8、float4/8、numeric、date/time/timestamp、bool、bytea、text） | 统一 NVARCHAR 文本（NULL 保真） |
| TLS | ✗（拒绝） | ✗（SSLRequest → 'N'） | ✗（ENCRYPT_NOT_SUP） |
| 管理命令 show @@* | ✓ | ✓ | ✓ |
| 方言改写 | – | – | `SELECT [ALL\|DISTINCT] TOP (n)` → `... LIMIT n`（多语句批中 LIMIT 归属首语句；`TOP n PERCENT`/`WITH TIES` 不重写并报错） |

TNS/Oracle 流量会被嗅探识别并**明确拒绝**（Java 版 Mycat3 的 TNS 同样只是无认证、
无法对接真实客户端的 PoC）。协议层可行性已单独评估（2022–2026 年 Oracle 官方开源
四种语言 thin 驱动、O5L/O7L 认证与 ANO 加密算法均已公开），结论为**有条件可行**，
详见 [oracle-tns-feasibility.md](oracle-tns-feasibility.md)。

MySQL 协议连接方式与原生完全相同：

```bash
mysql -h127.0.0.1 -P8066 -uroot -p123456 -Ddb1
```

## SQL 支持矩阵（三协议一致）

| 语句 | 行为 |
|---|---|
| `SELECT ... WHERE 分片键 =/IN/BETWEEN/范围` | 裁剪到命中分片，物理 SQL 下推；单分片结果原始载荷透传 |
| `SELECT`（无分片键 / OR 条件） | 全分片并行扫描 + 行拼接归并 |
| `SELECT ... ORDER BY 列/序号 [DESC] LIMIT n [OFFSET m]` | 各分片下推 `LIMIT offset+count`，全局归并排序后取 offset..count |
| `SELECT COUNT(*) / SUM / MAX / MIN`（无 GROUP BY） | 跨分片聚合归并（COUNT/SUM 求和，MAX/MIN 取极值） |
| `INSERT`（分片表） | 按行分片键值拆分到各节点（多行 VALUES 自动按节点分组重写） |
| `REPLACE INTO`（MySQL 后端） | 与 INSERT 同路由（分片/广播/单表全支持），物理语句保持 REPLACE 语义（先删后插由后端保证）；无列名形式要求显式分片列值（与 INSERT 一致）。Oracle 后端明确报错（无 REPLACE 方言翻译） |
| `INSERT/UPDATE/DELETE`（广播表） | 扇出到全部节点；affectedRows 求和 |
| `UPDATE/DELETE ... WHERE 分片键...` | 裁剪或全分片扇出 |
| `CREATE/DROP/ALTER TABLE`、`TRUNCATE` | 分片表：每节点重写物理表名执行；广播表：全节点；单表：所属节点 |
| `CREATE/DROP DATABASE` | 透传到 prototype target（逻辑库配置需另行管理） |
| `BEGIN / COMMIT / ROLLBACK` | 本地代理事务：按数据源固定连接，提交/回滚全部参与连接 |
| `SET autocommit / NAMES / TRANSACTION ISOLATION LEVEL` | 会话本地生效，并同步到事务中已固定的后端连接 |
| `USE db` | 切换默认逻辑库 |
| `SHOW DATABASES / TABLES` | 由元数据本地生成 |
| `SHOW 其他`（STATUS/VARIABLES/COLUMNS/CREATE TABLE...） | 透传到 prototype 后端 |
| `EXPLAIN <sql>` | 显示路由结果（DATA_NODE + 物理 SQL），不执行 |
| `SELECT @@version / database() / user() / connection_id() / @@tx_isolation ...` | 本地探针应答 |
| 多语句（`;` 分隔） | 逐句执行，MORE_RESULTS 语义回写 |
| `PREPARE / EXECUTE / DEALLOCATE`（COM_STMT_* 二进制协议） | 预处理语句：参数按类型替换执行（多节点路由放开：读归并/写顺序执行），二进制协议回写 |
| MySQL 文本 PREPARE（`PREPARE n FROM '...'` / `EXECUTE n USING @a` / `DEALLOCATE PREPARE n`） | 会话本地语句表 + `SET @x = 字面量` 用户变量跟踪；参数替换后走正常路由/方言翻译（**MySQL/PG/Oracle 后端全支持**）；参数不足 1210 / 未知变量 1331 / 未知语句 1064 |
| PG 文本 PREPARE（`PREPARE n [(types)] AS stmt` / `EXECUTE n(args)` / `DEALLOCATE n|ALL`） | 同上（参数为内联表达式，替换 `$1..$n`；多语句批支持）；错误语义对齐 PG（26000/08P01） |

## DDL 配置自动化（对应 Java MycatRouterConfigOps / CreateTableSQLHandler）

DDL 不仅执行物理语句，还会**自动维护逻辑库表配置**（写盘 + 元数据热更新 + 物理建库建表）：

```sql
-- 逻辑库（写 schemas/<name>.schema.json，并在 prototype 上建物理库）
CREATE DATABASE db2;
CREATE DATABASE db2 /*!mycat:{"targetName":"c0"}*/;   -- 指定默认 target
DROP DATABASE db2;                                    -- 删配置 + prototype 物理库

-- DRDS 风格分片表：自动推导 AutoFunction（storeNum=c* 集群数）
-- 下例在 c0/c1 两集群生成 2 库×4 表共 8 个物理分片，并自动建物理库表
CREATE TABLE orders (id BIGINT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(64))
    DBPARTITION BY hash(id) DBPARTITIONS 2
    TBPARTITION BY hash(id) TBPARTITIONS 4;

-- 广播表：广播到所有 c* 集群（各节点物理库=逻辑库名）
CREATE TABLE dict (id INT PRIMARY KEY, v VARCHAR(20)) BROADCAST;

-- 普通表：locality = 库默认 target
CREATE TABLE cfg (id INT PRIMARY KEY);

-- hint JSON 直配（对应 Java mycatDmlHandler createTable）
/*!+mycat:createTable{"type":"hash","dbNum":"2","tableNum":"4","storeNum":"2",
  "dbMethod":"hash(id)","tableMethod":"hash(id)",
  "mappingFormat":"c${targetIndex}/db2_${dbIndex}/orders_${index}"}*/
CREATE TABLE orders (id BIGINT PRIMARY KEY, name VARCHAR(64));

DROP TABLE orders;          -- 全物理节点 DROP + 配置移除
DROP TABLE IF EXISTS db2.orders;

-- 索引 DDL：分片表扇出全部物理节点（索引名保持逻辑名；GLOBAL 隐藏索引表不做）。
-- 注意 MySQL 不支持 CREATE INDEX IF NOT EXISTS / DROP INDEX IF EXISTS（MariaDB 扩展），
-- 与原生 MySQL 语义一致：重复建索引报 1061
CREATE INDEX idx_name ON orders (name);
DROP INDEX idx_name ON orders;

-- 重命名：逐物理节点 RENAME + 配置热更新（物理名嵌入逻辑名时同步替换；
-- 跨库 RENAME 不支持）。sqlparser 无该语法，由代理文本级解析
RENAME TABLE cfg TO cfg2;

-- ALTER：物理扇出 + 自动从首个物理节点回读 SHOW CREATE TABLE 刷新配置
-- create_sql（SHOW COLUMNS/information_schema.COLUMNS 立即反映新列）
ALTER TABLE cfg2 ADD COLUMN extra INT;

-- 用户与序列（Java 的 CREATE USER SQL 是 no-op，Pro 实现真实配置写入）
CREATE USER bob IDENTIFIED BY 'secret';
DROP USER bob;               -- root 受保护不可删
SHOW USERS;
CREATE SEQUENCE db2.order_seq START WITH 1000 INCREMENT BY 10 [SIMPLE|TIME];
DROP SEQUENCE db2.order_seq;

-- 用户角色 SQL 类型黑名单（对应 Java RoleConfig.disallowSqlTypes，配置字段同名）：
-- 命中返回 1235 "statement is sqlType:XXX, for the user xxx, it is forbidden."
-- 配置重热载（Java 兼容 hint；HTTP 侧为 POST /api/reload）
/*!mycat:loadConfigFromFile*/
```

行为细节（与 Java 一致）：
- 物理建表流程：每个节点先 `CREATE DATABASE IF NOT EXISTS <物理库>`，再执行去除
  DBPARTITION/TBPARTITION/BROADCAST 子句、表名重写为 `` `物理库`.`物理表` `` 的 DDL；
  单节点失败仅告警不中断（log-and-continue）
- 分片表 INSERT 省略 AUTO_INCREMENT 列（或值为 NULL/0）时自动填充 Snowflake 序列
  （workerId=`server.json.mycatId`），填充先于分片路由（序列值可作分片键）
- 配置先验证（元数据可编译）再落盘，失败保持旧配置

## Oracle 后端与方言翻译（dbType=oracle）

逻辑 SQL 在路由重写后、下发后端前按数据源方言翻译。支持清单（超出清单显式报错）：

| 类别 | MySQL | Oracle 翻译 |
|---|---|---|
| 标识符 | `` `db`.\`t\` `` | `"db"."t"`（双引号保大小写；ROWNUM/SYSDATE/NVL 等内建名不引用） |
| LIMIT/OFFSET | `LIMIT n [OFFSET m]` | ROWNUM top-N 包装（含 ORDER BY 语义；offset>0 双层包装） |
| 多行 INSERT | `VALUES (...),(...)` | `INSERT ALL ... SELECT 1 FROM "DUAL"` |
| 函数 | IFNULL/ISNULL、IF、CONCAT、LEFT/RIGHT/SUBSTRING、LENGTH、CEILING | NVL、CASE WHEN、\|\|、SUBSTR、LENGTHB、CEIL |
| 函数 | NOW/CURDATE/CURTIME/UTC_TIMESTAMP/UNIX_TIMESTAMP | CURRENT_TIMESTAMP（裸标识符，空括号在 Oracle 26ai 报 ORA-30088）、TRUNC(SYSDATE)、TO_CHAR(SYSDATE,'HH24:MI:SS')、SYS_EXTRACT_UTC(SYSTIMESTAMP)、日期差×86400 |
| 函数 | DATE_FORMAT/STR_TO_DATE | TO_CHAR/TO_DATE（%-格式自动转换 %Y→YYYY、%i→MI、%T→HH24:MI:SS…） |
| 函数 | DATE_ADD/SUB、DATEDIFF、TIMESTAMPDIFF | INTERVAL 'n' UNIT 运算、TRUNC(a)-TRUNC(b)、日期差×系数 |
| 函数 | RAND/UUID/MD5/SHA1/SHA2 | DBMS_RANDOM.VALUE、SYS_GUID、STANDARD_HASH(v,'MD5'/'SHA1'/'SHA256'…) |
| 函数 | GROUP_CONCAT、REGEXP | LISTAGG ... WITHIN GROUP、REGEXP_LIKE |
| 布尔 | TRUE/FALSE | 1/0 |
| 聚合 | COUNT/SUM/AVG/MAX/MIN（含 DISTINCT） | 同名直传 |
| 明确报错 | ON DUPLICATE KEY UPDATE / INSERT IGNORE / REPLACE INTO / DELETE ... LIMIT / DROP ... IF EXISTS / LAST_INSERT_ID() / 未知函数 | — |

**Oracle 后端事务语义**（对应 JDBC autocommit=true 行为）：
- 事务外的每条 DML/DDL 执行成功后**立即 COMMIT**（驱动默认 autocommit=false，需显式收口）；
- `BEGIN`/`SET autocommit=0` 进入事务：连接按数据源固定（Oracle 隐式开事务，无 BEGIN 语句），
  `COMMIT`/`ROLLBACK` 走驱动原生接口广播；`SAVEPOINT`/`ROLLBACK TO` 直传执行；
- 连接归还池时自动回滚残留的未提交事务（净化），杜绝脏连接复用。

## information_schema 与元数据命令（本地生成，无需 prototype 后端）

```sql
SELECT * FROM information_schema.SCHEMATA;
SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA = 'db2';
SELECT COLUMN_NAME, COLUMN_TYPE, COLUMN_KEY, EXTRA
  FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'orders';
SHOW CREATE TABLE db2.orders;    -- 返回含 dbpartition 子句的逻辑 DDL，支持库限定
SHOW FULL TABLES;                -- (表名, BASE TABLE/VIEW)
SHOW COLUMNS FROM orders;  DESC orders;
SHOW VARIABLES [LIKE 'max%'];    -- 内置变量表（对应 Java MysqlVariableServiceImpl fallback）
SHOW [FULL] PROCESSLIST;         -- 连接注册表（Time 单位为秒；Java 版为毫秒 quirk，已修复）
KILL <id>;                       -- 断开指定前端连接（三协议均生效）
SELECT @@version, @@sql_mode, @@max_allowed_packet, ...;  -- 多变量本地应答
```

information_schema 查询支持等值 / `DATABASE()` / LIKE 谓词过滤与投影列裁剪，
覆盖 JDBC DatabaseMetaData 与常见 BI 工具的元数据查询。

### 明确不支持（返回 `ER_NOT_SUPPORTED_YET 1235`，附中文说明）

- 多分片表 JOIN / 跨库子查询（Java 版由 Calcite 引擎处理）
- 跨分片 `GROUP BY`、`AVG`、`COUNT(DISTINCT)`、混合聚合投影
- 分片表上的多节点 PREPARE（单节点路由可用）
- `INSERT ... SELECT` 跨分片写入
- INSERT 缺分片列值（无序列自动填充）
- 前端 SSL/TLS、压缩协议（连接会被明确拒绝）
- LOAD DATA LOCAL

## Hint（与 Mycat3/Mycat2 兼容）

```sql
/*!mycat:target=ds1*/ select * from orders;         -- 强制路由到指定 target（原样透传，不重写）
/*!mycat:datanode=ds1*/ select * from orders;        -- 同上（Mycat2 拼写）
/*!mycat:master*/ select * from orders;              -- 强制读主库
/*!mycat:slave*/ select * from orders;               -- 强制读从库
/*!mycat:schema=db2*/ show tables;                   -- 指定逻辑库（对 SHOW/元数据命令生效）
/*!mycat:sql=select * from db1_0.orders_0*/;         -- 内嵌 SQL 直接执行
/*!mycat:showTopology{orders}*/;                     -- 输出表物理分片布局（TARGET_NAME/SCHEMA_NAME/TABLE_NAME/DB_INDEX/TABLE_INDEX/INDEX）
/*!mycat:executeTimeout=1000*/ select ...;           -- 语句级超时（毫秒；覆盖数据源 executeTimeout，MySQL/Oracle 后端均生效）
/*!mycat:showConfigText*/;                           -- 配置全文 JSON（单列 CONFIG_TEXT）
/*!mycat:showSlowSql*/;                              -- 慢 SQL 记录（trace_id/sql/sql_rows/start_time/end_time/execute_time/target_name，按耗时降序）
/*!mycat:showSlowSql{"trace_id":16}*/;               -- 按 trace_id 过滤（Java 版此处是对象恒等比较 bug，Pro 用值比较）
/*!mycat:setSqlTimeFilter{"value":"500"}*/;          -- 运行时调整慢 SQL 阈值（毫秒，默认 1000）
/*!mycat:getSqlTimeFilter*/;                         -- 查询当前阈值（列名 value）

/*!mycat:loaddata{"schemaName":"db1","tableName":"orders",
                  "fileName":"/data/orders.csv","skipHeaderRecord":true}*/
-- 服务器端 CSV 批量导入（对应 Java HintHandler.loaddata）：读服务器本地文件，
-- 按表列构造多行 INSERT（1000 行/批）走正常路由/分片（跨 MySQL+Oracle 分片同样按行落库）。
-- 参数：delimiter(默认,)、quoteChar(默认")、escape(默认\)、nullString(默认\N)、
--       ignoreEmptyLines(默认 true)、skipHeaderRecord(默认 false)、header(列名数组，缺省从配置 create_sql 推导)。
-- CSV 语法：引号包裹、引号内 "" 与 \" 转义、引号内换行、# 注释行。
-- 注意：列数与表列数不符报 1136；多后端部分失败时已导入的行不自动回滚（无 XA，与 Java 一致）。

/*!mycat:createDataSource{"name":"ds9","url":"jdbc:mysql://...","user":"u","password":"p",
                          "dbType":"mysql","maxCon":8,"minCon":1,"instanceType":"READ_WRITE"}*/
-- 配置管理 hint 组（对应 Java mycatDmlHandler：JSON 配置对象直接写盘 + 热生效）：
--   createDataSource/dropDataSource（drop 传 {"name":"..."}）
--   createCluster/dropCluster（ClusterConfig 对象；drop 传 {"name":"..."}）
--   createUser/dropUser、setSequence、createSchema/dropSchema（同名配置对象）
/*!mycat:dropDataSource={"name":"ds9"}*/
/*!mycat:repairPhysicalTable*/;                     -- 按元数据为全部逻辑表补建缺失物理库表（幂等；
                                                   --   已删除的物理表会重建为空表；Oracle 后端跳过）

/*!mycat:setAcceptConnect=0*/;                      -- 停止接受新连接（=1 恢复；Java 同语义）
/*!mycat:setReadyToCloseSQL{"sql":"select 'ready'"}*/  -- 发布窗口探针：命中该 SQL 即回 OK
/*!mycat:pauseServer{"connectionIds":[3,5]}*/;      -- 挂起指定连接+自身会话（命令排队）；resumeServer 恢复
/*!mycat:resumeServer*/

-- 未识别的 mycat hint 指令：显式报 1235（Java 版静默忽略——脚本会"成功但没结果"，
-- Pro 拒绝静默给错结果）。hint 后若带 SQL（客户端未用分号分隔），剩余 SQL 照常执行。

/*!mycat:createSqlCache{"name":"cnt1","sql":"select count(*) from orders",
                        "refreshInterval":60,"initialDelay":0,"matchMode":"PARAMETERIZED",
                        "maxRows":100000,"schemaName":"db1"}*/
-- SQL 结果缓存（对应 Java SqlResultSetService，Pro 语义增强）：
--   - 模板中的 **? 是自由参数槽位**：形状匹配的查询都进缓存，但**每个参数值组合
--     有自己独立的结果**（`where id=1` 与 `id=2` 各自缓存、绝不共享）——来查询原文
--     按需执行回填；
--   - **含 ? 的模板无法预读**（参数不可预测）：按需回填 + 变体级 TTL
--     （refreshInterval，0=不过期）惰性刷新；变体上限 4096（逐出最旧）；
--   - 无 ? 的具体 SQL：注册后立即预执行 + 周期刷新（先成功后原子换入，失败保留
--     旧值）；模板中的固定字面量**逐位匹配**（`where id=1` 模板不命中 `where id=2`）；
--   - matchMode=EXACT：SQL 文本精确匹配（Java 语义，单结果可预读）；
--   - **LIMIT/OFFSET 字面量不参数化**：分页是结果形状的一部分，每页独立缓存键，
--     未配置的页正常执行不缓存，**绝不会串页**；
--   - 结果持久化本地二进制文件：具体模板 <name>.bin，变体 <name>_<hash>.bin
--     （temp+rename 原子写，mmap 恢复，损坏忽略；dropSqlCache 时清理）；
--   - 冷缓存每键（预读/每变体）并发共享一次在途执行（等待语义）；
--   - 命中判定：SELECT ∧ 非事务 ∧ autocommit=1 ∧ 无 hint；键含 schema（修掉 Java 跨库撞键）；
--   - 命中路径性能：原始文本 O(1) 快路径（可预读条目）；无缓存项零开销短路；
--   - 写入后失效：Pro 在 DML/DDL 成功后按表名 token 保守清理相关缓存（内存结果 + .bin 文件），
  宁可多失效也不返回旧数据（Java 版不失效）；跨库同名表可能被一并清理；
  maxRows/maxBytes 超限不缓存（回落正常执行）。
/*!mycat:showSqlCaches*/;  -- NAME/SQL/REFRESH_INTERVAL_S/MATCH_MODE/HAS_CACHE/HITS/MISSES/FILE_BYTES
/*!mycat:dropSqlCache={"name":"cnt1"}*/
```

## 权限与连接限制

- 用户 `readOnly: true`：写语句/DML/DDL 显式拒绝（1290，Java 有配置但未强制，Pro 落地）；
- 用户 `loginLimit: n`：同用户活跃连接数上限（超出认证拒绝）；`role.disallowSqlTypes` 同前；
- 密码 `encryptType: "rsa"` + `encodeKey`（base64 DER 私钥或 PEM）：登录时 PKCS1v15 解密比对
  （Java UserConfig 兼容；密码字段存 base64(密文)）。

## 本地 SHOW mock（不依赖 prototype 后端，对应 Java 各 Show*SQLHandler）

`SHOW ENGINES`（8 行固定引擎表）/ `SHOW PLUGINS` / `SHOW ERRORS` / `SHOW WARNINGS` /
`SHOW PROCEDURE STATUS` / `SHOW STATUS` 均为本地结果集——无 prototype 的精简部署可用。

## CHECK TABLE / 视图

- `CHECK TABLE 表`：分片/广播/单表扇出到全部物理节点执行物理 CHECK TABLE，
  结果归并拼接（TABLE/Op/Msg_type/Msg_text）；
- `CREATE VIEW`（仅单表视图）：物理执行（USE 目标库）+ 视图元数据注册；
  引用分片/广播表 → 显式报错（透传模式无法落到单一物理库）；
  `DROP VIEW`：物理删除 + 元数据移除；视图查询经库限定重写路由。

## 前端 TLS（server.json sslCert/sslKey）

- 配置 `sslCert`/`sslKey`（PEM 路径，相对 home）后：MySQL 握手通告 CLIENT_SSL
  （ssl-mode=PREFERRED/REQUIRED 可用，DISABLED 明文兼容）；PG SSLRequest 回 'S'
  （sslmode=require 可用）；TDS 前端 TLS 未实现（保持 ENCRYPT_NOT_SUP）；
- 未配置时保持显式拒绝（与 Java 一致）。

## HTTP 管理 API（监控端口）

`GET /-/ready`（存活就绪）/ `GET /api/config`（配置 JSON）/
`PUT|DELETE /api/config/{datasources|clusters|users|schemas|sequences|sqlcaches}/{name}`
（JSON 配置对象热生效）/ `GET /api/sqlcaches` / `POST /api/reload` /
`GET /api/backups`（配置快照列表）/ `POST /api/config/restore/{ts}`（恢复）。
每次配置变更自动备份到 `<home>/backup/`（保留 10 份）。

## 管理命令（Pro 增强，Mycat2 风格）

```sql
show @@version;      -- 服务端版本
show @@cluster;      -- 集群/实例：角色、存活、可读、活跃连接
show @@datasource;   -- 连接池：空闲/可用额度/累计创建
show @@connection;   -- 前端连接与查询统计
show @@heartbeat;    -- 各数据源心跳连续错误计数
show @@users;        -- 用户清单（USERNAME/IP/DIALECT/SCHEMAS）
show @@schemas;      -- 逻辑库清单（SCHEMA_NAME/TARGET_NAME）
show @@help;         -- 管理命令列表

SHOW STATISTIC;      -- Mycat 专有性能指标表（QPS/SLOW_QPS/ERROR_PER_SECOND/
                     --   ACTIVE_CONNECTIONS/NET_IN/NET_OUT...，按两次采样间隔计算速率；
                     --   Java 版只回列头，Pro 返回一行真实值）
SHOW FULL STATISTIC; -- 同上，26 列全量指标
```

## 监控 HTTP（默认 :9066）

| 端点 | 内容 |
|---|---|
| `/-/healthy` | 健康检查（`ok`） |
| `/metrics` | Prometheus 文本格式：uptime、connections、queries、errors、slow、tx、bytes、每数据源存活/池状态 |
| `/api/status` | JSON 全量状态快照 |

## 分片算法清单

`function.clazz` 可写 Java 全限定名（按后缀识别）或简名/别名：

| 算法（Java 类） | 别名 | properties | ranges | 说明 |
|---|---|---|---|---|
| `PartitionByMod` | `mod` | `columnName`,`count` | – | BigInteger 绝对值取模 |
| `PartitionByLong` | – | `columnName`,`partitionCount`,`partitionLength`（csv，点积=1024） | – | 1024 槽段表，hash=`(v>>>32)^v` |
| `SimplePartitionByLong` | – | `columnName`,`partitionCount` | – | 等分 1024 槽 |
| `PartitionByHashMod` | `hash-int` | `columnName`,`count` | – | Wang hash(String.hashCode) 取模/按位与 |
| `PartitionByMurmurHash` | `murmur` | `columnName`,`seed`,`count`,`virtualBucketTimes` | 桶→权重 | Guava murmur3_32 一致性哈希环 |
| `PartitionByRangeMod` | `rangemod-hash` | `columnName`,`defaultNode` | `"start-end"→组大小` | 区间定位 + 组内取模 |
| `AutoPartitionByLong` | `auto-sharding-long` | `columnName`,`defaultNode` | `"start-end"→节点` | 值区间映射 |
| `PartitionByPattern` | `pattern` | `columnName`,`patternValue`(1024),`defaultNode` | `"start-end"→节点` | `value%patternValue` 查区间 |
| `PartitionByPrefixPattern` | `prefix-pattern` | `columnName`,`patternValue`,`prefixLength`,`defaultNode` | 同上 | 前缀 ASCII 和取模查区间 |
| `PartitionByFileMap` | `filemap`,`enum` | `columnName`,`defaultNode` | `值→节点` | 枚举映射 |
| `PartitionDirectBySubString` | `substring` | `columnName`,`startIndex`,`size`,`partitionCount`,`defaultNode` | – | 截子串直接作节点号 |
| `PartitionByString` | `string` | `columnName`,`partitionCount`,`partitionLength`,`hashSlice`("start:end") | – | 子串 31 进制哈希 → 1024 槽 |
| `PartitionByDate` | `date` | `columnName`,`beginDate`,`endDate`,`partionDay`,`dateFormat` | – | 按天分区（可循环） |
| `PartitionByMonth` | `month` | `columnName`,`beginDate`,`endDate`,`dateFormat` | – | 按月分区（无 begin/end 时返回 0 基月份） |
| `PartitionByHotDate` | `hotdate` | `columnName`,`sLastDay`,`sPartionDay`,`dateFormat` | – | 最近 N 天热数据在 0 号节点 |
| `PartitionByCRC32PreSlot` | `crc32slot` | `columnName`,`count` | `"槽start-槽end"→节点` | CRC32 → 102400 预分槽 |
| `PartitionByJumpConsistentHash` | `jump-consistent-hash` | `columnName`,`totalBuckets` | – | Google Jump 一致性哈希 |
| AutoFunction | –（name/clazz 留空） | `dbNum`,`tableNum`,`storeNum`,`mappingFormat`,`dbMethod`,`tableMethod` | – | 二维库表分片，见 configuration.md |

所有算法与 Java 版逐位一致（相同输入 → 相同分片下标），配置可在两版之间直接迁移。

## 事务语义

- `transactionType=proxy`（本地事务，默认）与 `transactionType=xa`（**XA 2PC：MySQL + PostgreSQL 后端**）：
  - XA 模式（用户配置 transactionType=xa，或会话 `SET transaction_policy='xa'`）：
    BEGIN 后按数据源固定连接，首条写语句前对各分支 `XA START`（分支号=数据源名、
    gtrid=毫秒时间戳+全局序号、formatID=0x4D335850）；COMMIT 时各分支 `XA END` +
    `XA PREPARE`，全部成功且多参与者时**先落决策 WAL（fsync）再逐分支 `XA COMMIT`**
    （单参与者走 `XA COMMIT ... ONE PHASE`）；ROLLBACK 各分支 `XA ROLLBACK`；
  - 崩溃恢复：启动时对每个 MySQL 数据源 `XA RECOVER`，孤儿 xid 有决策 → 补提交、
    无决策（prepare 阶段崩溃）→ 回滚（WAL 位于 `<home>/xa/xa-wal.log`，恢复后压缩）；
  - PG 分支：普通 BEGIN → `PREPARE TRANSACTION 'gtrid_分支'` → `COMMIT PREPARED`/
    `ROLLBACK PREPARED`（GID 实例级唯一加分支后缀；要求 `max_prepared_transactions>0`）；
    支持 MySQL+PG 混合事务；恢复扫描 pg_prepared_xacts 按 WAL 裁决；
  - 边界：Oracle 分支显式报错（thin 驱动无 XA）；XA 事务不支持
    SAVEPOINT；prepare 与 WAL 之间的崩溃 → 孤儿回滚（一致性保持、锁为代价）；
    **故障时刻不存在 100% 原子**（2PC 固有阻塞窗口）；
- `transactionType=proxy`（本地事务，与 Java 版默认一致）：
  - `BEGIN`（或 `SET autocommit=0` 后首条写）开启事务；
  - 事务中每个数据源固定一条后端连接（MySQL 下推 `BEGIN`；Oracle 隐式开事务、BEGIN 为本地无操作），语句间复用；
  - **SQL 级错误不终止事务**（与 MySQL 标准语义一致）：唯一键冲突/语法错误等只返回错误，
    连接保持固定、事务继续；仅连接级错误（IO/协议/超时）销毁连接并剔除固定关系；
  - `COMMIT`/`ROLLBACK` 广播到全部固定连接后释放（回滚失败仅标记实际失败的连接）；
  - 会话断开自动回滚未提交事务；
  - **多数据源写入不具备原子性**（无 XA），部分失败返回错误并回滚已固定连接；
  - Oracle 后端：事务外语句即时提交（见上文"Oracle 后端事务语义"）。
