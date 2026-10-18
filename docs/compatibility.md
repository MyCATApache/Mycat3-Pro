# 与 Mycat3 (Java) 的兼容性与差异

## 1. 完全兼容

| 项目 | 说明 |
|---|---|
| 配置格式 | JSON 多文件布局与字段名逐一兼容（server.json / users / datasources / clusters / schemas / sequences），未知字段忽略；Java 版配置目录可直接给 Pro 使用 |
| 分片算法 | 17 种 legacy 算法 + AutoFunction 的哈希/取模/一致性环实现与 Java 逐位一致（含 `String.hashCode`、Guava murmur3_32 的 UTF-16LE `hashUnencodedChars` 语义、Jump Consistent Hash 常量、1024 槽段表、CRC32）；同一配置下数据落点相同 |
| MySQL 协议 | HandshakeV10 / mysql_native_password / AuthSwitchRequest / caching_sha2 快速认证 / OK-ERR-EOF / DEPRECATE_EOF / 16MB 分包 / MORE_RESULTS / 文本与二进制行协议 |
| 端口约定 | 服务 8066、管理/监控 9066 |
| Hint | `/*!mycat:target=…|master|slave|schema=…|sql=…*/` |
| 路由语义 | 写→主库、读→负载均衡、事务中→主库固定连接；分片键裁剪、OR 退化全扫、广播表读单写全、unknown SHOW→prototype |
| 心跳语义 | SQL 常量（`select 1`/`show slave status`/`show status like 'wsrep%'`/MGR performance_schema）、maxRetry 连续错误阈值、slaveThreshold 延迟剔除、minSwitchTimeInterval 切换节流、SWITCH 存活者置顶 |
| DDL 自动化 | CREATE/DROP DATABASE/TABLE/USER/SEQUENCE + **CREATE/DROP INDEX 分片扇出、RENAME TABLE（物理执行+配置热更新）、ALTER 后回读 SHOW CREATE TABLE 刷新 create_sql**（对应 Java CreateIndex/DropIndex/RenameTable/AlterTable SQLHandler；Oracle 后端物理 DDL 仍由 DBA 手工执行） |
| Hint 扩展 | `showTopology{表}`（物理分片布局视图）、`executeTimeout=n`（语句级超时，双后端生效）、`schema=xx` 对 SHOW/元数据命令生效（对应 Java HintHandler/MycatdbCommand；`showConfigText` 未实现——配置模型无 Serialize，排障直读文件） |
| 用户角色 | `role.disallowSqlTypes` SQL 类型黑名单（返回 1235 与 Java 同文案，对应 Java RoleConfig） |
| loaddata 服务器端 CSV 导入 | `/*!mycat:loaddata{...}*/`：读服务器本地 CSV，按表列构造多行 INSERT（1000 行/批）走正常路由/分片（跨 MySQL+Oracle 混合分片按行落库），参数与 Java 对齐（delimiter/quoteChar/nullString/skipHeaderRecord/header 等） |
| 慢 SQL 观测 | `showSlowSql`（trace_id/sql/sql_rows/start_time/end_time/execute_time/target_name，按耗时降序）+ `setSqlTimeFilter{"value":n}`/`getSqlTimeFilter` 运行时调整阈值；Java 的 trace_id 过滤是对象恒等比较 bug，Pro 按值过滤 |
| showConfigText | `/*!mycat:showConfigText*/` 返回配置全文 JSON（Java 兼容；Pro 配置模型因此补充 Serialize） |
| 配置管理 hint 组 | `createDataSource/dropDataSource/createCluster/dropCluster/createUser/dropUser/setSequence/createSchema/dropSchema` + `repairPhysicalTable`（对应 Java mycatDmlHandler；JSON 配置对象直接写盘+热刷新，URL 与启动校验一致） |
| 运维 hint 组 | `setAcceptConnect=0/1`（停/恢复接入）、`setReadyToCloseSQL`（发布窗口探针）、`pauseServer{"connectionIds":[...]}`/`resumeServer`（连接挂起/恢复，三前端命令循环排队语义与 Java 一致） |
| SHOW STATISTIC / SHOW FULL STATISTIC | Mycat 专有指标表（列与 Java 一致；Java 只回列头，Pro 返回一行按采样间隔计算的真实速率） |
| 未知 mycat hint | Java 静默忽略（脚本"成功但没结果"）→ Pro 显式报 1235；hint 后带剩余 SQL 时照常执行（Java 前缀语义） |
| SQL 结果缓存（createSqlCache/showSqlCaches/dropSqlCache） | Java：内存 + SQL 文本精确键 + 定时预热（先失效后加载）；Pro 语义增强：模板 **? 为自由参数槽位**（形状匹配 + 固定字面量逐位约束），**不同参数值组合各自独立缓存结果**（绝不像 Java 精确键那样跨参数串结果）、含 ? 模板不预读（按需回填 + 变体 TTL）、无 ? 模板预执行 + 周期刷新（先成功后换入）、**二进制文件持久化**（每变体一文件，mmap 恢复）、**冷缓存每键共享一次在途执行**、键含 schema（修掉 Java 跨库撞键）、预热与线上走同一条执行链路 |

## 2. 有意修复的 Java 版缺陷（Pro 行为更正确）

| 位置 | Java 版问题 | Pro 行为 |
|---|---|---|
| `VertxMySQLPacketResolver` | >16MB 多分包重组时覆盖中间分包（丢失数据） | `FrameDecoder` 正确累积拼接（有单测覆盖 0xFFFFFF+100 与恰好 16MB 场景） |
| `ErrorPacketImpl.writePayload` | ERR 包省略 `'#'+sqlstate`（源码中被注释） | 恢复标准格式，严格客户端兼容性更好 |
| `MysqlNativePasswordPluginUtil.nextSeedBuild` | 认证种子为原始随机字节，可能含 NUL/0xFF 导致握手包截断 | 种子限定可打印 ASCII（与官方 MySQL 一致） |
| `BalanceRoundRobin` | 内层循环 `for (int index = 0; i < length; i++)` 下标变量错误 | 平滑加权轮询（nginx 算法），权重比例精确（单测验证 5:1 → 500:100） |
| `TDSProtocolHandler` Login7 解析 | TDSVersion 按大端读取；实测 ODBC/JDBC 驱动按小端写入 0x74000004（字节 `04 00 00 74`），BE 读成 0x04000074 → 误判"TDS < 7.2"拒绝所有现代驱动 | 按小端解析（MS-TDS 2.2.6.3） |
| `TDSProtocolHandler` collation ENVCHANGE | OldValue 重复写 5 字节 collation；实测 ODBC Driver 13 报 "Protocol error in TDS stream" 并断开 | OldValue 长度为 0（真实 SQL Server 登录应答行为，经 fake-server 二分实验定位） |
| `TDSResponse` 行编码 | NULL 值编码为空字符串，丢失 NULL 语义 | 0xFFFF 长度标记，NULL 保真（sqlcmd 实测显示 NULL） |
| `PGProtocolHandler` 扩展协议 | Parse/Bind/Describe/Execute 为假 stub（回 CommandComplete 占位），JDBC/pgAdmin 参数化查询全部失效 | 真实实现：命名语句/portal、$n 参数字面量替换、参数格式码（文本/二进制）、Execute 阶段下发 RowDescription |
| `PGResponse` 类型映射 | 所有列一律 TEXT(OID 25)，数值列在客户端丢失类型 | MySQL 类型 → PG OID 映射（int2/int4/int8/float4/float8/numeric/date/time/timestamp/bool/bytea/text），二进制列 bytea hex 编码 |
| `PGResponse` ReadyForQuery | 事务状态恒为 'I' | 按会话事务状态返回 'I'/'T' |
| `ErrorResponse` | —（Java 版 E 消息缺字段列表终止 NUL 的情况未在真实 psql 下验证） | 以终止 NUL 结束 + 长度自洽（psql 16 实测通过，字节级校验） |
| `MysqlMetadataManager` COLUMN_DEFAULT | 无默认值列返回字符串 `"NULL"`（污染 JDBC 元数据语义） | 返回真 SQL NULL |
| `BalanceRoundRobin` | `randomIndex` 计算后未使用（加权分支整体不可达的佐证） | 平滑加权轮询 |

### 2.5 2026-10 全量代码审计修复（Pro 自身缺陷，非 Java 对比项）

| 缺陷 | 影响 | 修复 |
|---|---|---|
| Oracle LIMIT n OFFSET m 翻译中间层只投影 `rn` | OFFSET 查询**丢光业务数据列** | 中间投影补 `t.*`（单测断言列保留） |
| Oracle 结果渲染按列名线性查找元数据 | O(R·C²) 扫描 + **重名列取错值**（JOIN 场景静默数据错乱） | 按索引取值（db_type 预取） |
| 跨分片归并比较器按值猜类型（两侧可解析才数值比） | VARCHAR 字典序被破坏 + 混合数据非传递性（debug panic / release 乱序） | 按列 field_type 定策略、列内统一全序（数值列数值比、字符串列字节比），排序键一次计算 |
| 后端 SQL 级错误一律销毁连接 | 事务中唯一键冲突 → 连接被杀 → **整个事务被服务端回滚**；非事务场景错误后重建连接风暴 | 仅连接级错误（IO/协议/超时/连接类 ORA 码）销毁；SQL 级错误连接复用、事务继续（与 MySQL 语义一致） |
| ROLLBACK 失败标记全部固定连接 broken | 单后端回滚失败 → 其余健康连接全被销毁重建 | 仅标记实际失败的连接 |
| 并行执行 active 计数未 RAII 化 | acquire 失败路径计数泄漏，BalanceLeastActive/`show @@cluster` 失真 | RAII 守卫对齐 incr/decr |
| COM_STMT_EXECUTE 参数丢类型、按内容猜字符串 | VARCHAR "123" 裸写 → 隐式转换/索引失效；BLOB 二进制经 UTF-8 lossy 损坏 | 参数携带字段类型：字符串加引号、BLOB `X'hex'`、数值裸写 |
| TDS RPC 参数解析无边界检查 | 截断载荷 → 连接任务 panic | 全部读取带边界检查返回明确错误 |
| 配置热重载四把锁逐个换入 | 查询可观察到新元数据+旧副本运行的混合状态 | `RuntimeSnapshot` 单锁原子换入 |
| hex 编码逐字节 `format!` | 大 BLOB 每字节一次堆分配 | 查表编码（Oracle/PG/TDS 三处） |
| 连接池每次取用必 ping（MySQL）+ ping+rollback（Oracle） | 每条语句多 1–2 次固定网络往返 | 空闲窗口免 ping；Oracle dirty 标记按需回滚（事务外 DML 即时 COMMIT 不变） |
| Snowflake 全局 Mutex + 回拨 `thread::sleep` | INSERT 序列填充串行化 + 阻塞 tokio worker | 无锁 CAS（AtomicI64），无 sleep |
| 归并无排序也全量解码重编码 + 首集整表 clone | 跨分片查询 3–4 倍峰值内存、百万级分配 | 无 ORDER BY 直接拼接/截断原始行载荷；聚合只解码首行；排序键缓存 |
| 配置字段 `createTableSQL` 从未被读取 | Java 版配置文件的建表 SQL 键为 `createTableSQL`，Pro serde camelCase 生成 `createTableSql`——读取 Java 配置时建表 SQL 静默丢失（loaddata 列推导/SHOW CREATE TABLE 失效） | serde alias 兼容 `createTableSQL`（2026-10 由 loaddata 集成测试暴露） |
| Oracle 数据源忽略 `initSqls` | MySQL 池执行 initSqls，Oracle 池不执行——会话级初始化（如 NLS_DATE_FORMAT）无法配置，`'YYYY-MM-DD HH:MM:SS'` 字面量插入 DATE 列报 ORA-01861 | Oracle 连接建立后执行 initSqls（与 MySQL 池语义对齐） |
| `CreateUserHandler` / DROP USER | SQL 层为 no-op（仅 hint 可管理用户） | `CREATE/DROP USER` SQL 真实写配置并热生效（root 保护） |
| `ShowProcessListSQLHandler` | Time 列输出毫秒（quirk） | Time 列为秒（MySQL 标准语义） |
| `InformationSchema.COLUMNS` | ORDINAL_POSITION 0 基、CHARACTER_MAXIMUM_LENGTH 恒为 4 | 1 基序号、按 DDL 真实长度 |
| 配置 reload 时连接管理器 | add-only（已存在数据源的 URL/密码修改不生效） | 检测数据源配置变更并驱逐旧连接池，reload 后即刻生效 |
| `TransactionSession` 广播 | — | SAVEPOINT 类语句不重置事务标志（否则下一条写语句重发 BEGIN 触发 MySQL 隐式提交，丢弃 savepoint —— Pro 实测发现并规避的陷阱） |
| 广播表物理 SQL | — | 每广播节点重写物理 schema（各节点库名可不同） |
| `PartitionByMonth.reCalculatePartition` | 负偏移环绕存在 off-by-one 边界（`nPartition - (-t) % n` 在整除时返回 nPartition 越界） | 相同公式移植 + 结果界内断言由上层 `node_at` 兜底（越界返回明确错误而非 panic） |
| 手写 NIO 栈与 Vert.x 栈并存（`NativeMycatServer` vs `VertxMycatServer`） | 双实现漂移 | 单一实现 |

## 3. 未移植的能力（明确报错或忽略，绝不静默给错结果）

| Java 版能力 | Pro 行为 | 建议 |
|---|---|---|
| Calcite 分布式执行（跨分片 JOIN/GROUP BY/AVG/子查询/派生表） | `ER_NOT_SUPPORTED_YET` + 中文说明 | 使用单分片路由、广播表，或复杂分析走 Java 版 Mycat3 |
| 纯代理运行模式（server.mode=proxy） | Java 版无对应模式 | **Pro 扩展**：SQL 原样透传单 target，仅保留慢 SQL/结果缓存/SQL 拦截（拦截先于缓存）与本地管理命令；三前端一致；配置见 configuration.md |
| 用户 readOnly/loginLimit 强制 | Java 仅配置存在、认证层不强制 | **Pro 强制**：只读写语句 1290 拒绝、活跃连接超上限认证拒绝 |
| 密码 RSA 加密（encryptType/encodeKey） | UserConfig.java:86-125（PKCS1v15 + PKCS#8） | 同算法兼容（容忍 PEM 装甲与 DER base64 两种 encodeKey） |
| SHOW ENGINES/PLUGINS/ERRORS/WARNINGS/PROCEDURE STATUS/STATUS 本地 mock | 各 Show*SQLHandler 本地结果集 | 同行为（无 prototype 部署可用） |
| CHECK TABLE | MySQLCheckHandler 扇出+四列归并 | 同语义（物理 CHECK TABLE 扇出归并） |
| CREATE/DROP VIEW 元数据注册 | CreateView/DropViewSQLHandler | 已实现（仅单表视图；分片表视图显式报错） |
| 前端 TLS | Java 拒绝（CLIENT_SSL 未通告 / SSLRequest 回 'N'） | **Pro 支持** MySQL+PG 前端 TLS（sslCert/sslKey 配置；TDS 未实现） |
| 连接池 minCon 预热 / 心跳复用池连接 | Java Druid 池语义 | Pro 补齐（get_pool 后台预热 min_con；心跳优先复用空闲连接） |
| XA 分布式事务（`va` 模块，2PC + 崩溃恢复日志） | **MySQL+PG 已实现**（Java 仅 MySQL）：`transactionType=xa` / `SET transaction_policy='xa'`，分支 `XA START/END/PREPARE`、单参与者 ONE PHASE、多参与者决策 WAL（fsync）→ 广播 COMMIT、启动 `XA RECOVER` 自动裁决（补提交/回滚）；Oracle 后端分支显式拒绝（thin 驱动无 XA）；XA 不支持 SAVEPOINT；故障时刻无 100% 原子（2PC 固有） | 混合 Oracle 事务不承诺原子性（本地代理事务 + 应用层补偿） |
| TNS(Oracle) 前端 | 嗅探识别后明确拒绝并记录日志（Java 版为无认证 PoC：固定 root、把 DATA 包当 UTF-8 文本 SQL，无法对接真实客户端）。**可行性已评估：有条件可行**（官方 rust-oracledb 等源码公开了 TNS/TTC/O5L/O7L 全部细节），Phase 1+2 约 6–10K 行，见 [oracle-tns-feasibility.md](oracle-tns-feasibility.md) | 如需 Oracle 工具链接入，按报告路线图立项 |
| PostgreSQL 后端（dbType=postgresql） | Java 版经 JDBC+jOOQ 支持非 MySQL 后端 | **已实现**：官方生态 tokio-postgres 客户端（SCRAM-SHA-256/MD5 认证零风险）+ 自研池 + 反引号→双引号方言翻译 + 类型映射（prepare 描述缓存）+ `select 1` 心跳；**XA 2PC 原生支持**（PREPARE TRANSACTION/COMMIT PREPARED，要求 max_prepared_transactions>0），**MySQL+PG 混合 XA 实测通过**；物理建表 DBA 手工（DDL 自动化 MySQL 专用） | 三前端协议均可查询 PG 分片表；跨 MySQL+PG+Oracle 混合分片可组合 |
| Oracle 后端（dbType=oracle） | Java 版经 JDBC+jOOQ 方言翻译支持非 MySQL 后端 | **已实现**：官方 rust-oracledb thin 驱动 + 自研池（spawn_blocking 隔离阻塞调用）+ MySQL→Oracle 方言翻译器（标识符/LIMIT/INSERT ALL/函数/日期格式，见 commands.md）+ `select 1 from dual` 心跳；事务外语句即时 COMMIT（autocommit=true 语义）、事务固定连接 + 驱动 commit/rollback、连接归还回滚净化；Oracle 物理建表手工执行（DDL 自动化 MySQL 专用）；TDS 前端 `SELECT TOP n`→`LIMIT n` 改写 | 三前端协议均可查询 Oracle 分片表；跨 MySQL+Oracle 混合分片已实测（写入物理可见/事务/多行 INSERT/update/delete） |
| 前端 SSL/TLS、压缩协议 | 握手期明确拒绝（MySQL ERR / PG 回 'N' / TDS ENCRYPT_NOT_SUP） | 内网部署或前置 TLS 代理 |
| migrate 在线分片迁移 / binlog 增量同步 | 未实现 | 使用 Java 版 migrate |
| BASELINE/SPM 执行计划管理 | `ER_NOT_SUPPORTED_YET` | – |
| 组序列/表序列（group sequence、MySQL 表序列） | Snowflake（默认）+ 简单计数序列（START WITH/INCREMENT BY，内存态非持久） | 需要持久化组序列时用 Java 版 |
| information_schema 原型服务（内嵌 MySQL8 全量视图定义，Calcite 引擎查询） | SCHEMATA/TABLES/COLUMNS 三表本地生成（等值/LIKE/DATABASE() 过滤 + 投影裁剪），其余 SHOW 透传 prototype | 覆盖 JDBC/BI 元数据主路径；其余复杂元数据查询走 prototype |
| 统计信息 CBO（analyze_table 行数表） | 未实现（透传模式无需代价模型） | – |
| 自定义插件加载（MycatCommandLoader/HintLoader 反射注册） | 编译期内置 | 需要扩展时修改源码重编译 |
| Zookeeper/DB 配置中心（ZookeeperKVImpl/DbStorageManagerImpl） | 仅文件存储 | – |
| Galera/MGR/MHA 的动态成员增减（心跳发现新主自动加入写列表） | 心跳判活 + SWITCH 重排既有 masters 列表 | 拓扑变更走配置 + reload |

## 4. 行为差异备忘

1. **后端连接**：Java 版热路径混用 JDBC(Druid) 与 vert.x 原生客户端；Pro 一律原生协议。
   `datasource.type=JDBC` 配置仍被接受（忽略），JDBC 专有参数（如 `queryTimeout` 的驱动语义）
   映射为 `executeTimeout`。
2. **caching_sha2_password**：仅支持快速认证路径（服务端已有密码缓存）；首次连接触发
   full auth（需 TLS/RSA 公钥交换）时返回明确错误，建议后端用户使用
   `mysql_native_password`。
3. **PREPARE 列元数据**：Java 版通过原型服务预计算结果列；Pro 返回 0 列元数据 +
   参数定义，EXECUTE 结果以 VAR_STRING 类型二进制行回写（主流驱动兼容）。
4. **`select @@xxx` 变量**：常用探针（version/database()/user()/connection_id()/
   tx_isolation/autocommit 等）本地应答；其余变量查询透传 prototype。
5. **多语句事务中的并行读**：事务内多分片读退化为串行（固定连接不可跨任务共享），
   非事务读并行执行。
6. **广播表写 affectedRows**：与 Java 版一致为各节点求和（2 节点各插 1 行返回 2）。
7. **栈大小**：debug 构建异步链栈帧较大，runtime 工作线程栈设 8MB（release 下绰绰有余）。

## 5. 测试与验证覆盖

- 单元/集成测试 287 个（`cargo test --workspace`），含 DDL 配置自动化端到端
  （CREATE DATABASE → DRDS 分片建表 → 物理库表创建 → 序列填充 → 路由 →
  BROADCAST 扇出 → information_schema → PROCESSLIST/KILL → 热重载 → DROP 清除）
- 端到端测试（`crates/server/tests/e2e.rs`）：mock 双后端 + 真实前端协议栈，
  覆盖认证失败、分片裁剪、全扫归并、COUNT 归并、ORDER BY+LIMIT 归并、IN 路由、
  INSERT 拆行、UPDATE 扇出求和、广播双写、DDL 扇出、事务 BEGIN/COMMIT 下推、
  PREPARE/EXECUTE 二进制回写、本地命令、管理命令
- 真实 MySQL 8.0 冒烟（docker）：分片写入物理落点核对（db1_0.orders_0/1、
  db1_1.orders_2/3 各 1 行）、跨分片 ORDER BY DESC LIMIT、COUNT=4 归并、广播表
  双库落行、AutoFunction events 路由、事务提交、错误密码 1045、200 并发查询 0 错误、
  心跳判活、/metrics 与 /api/status
- 真实多协议客户端冒烟（同一 8066 端口，同一分片集群）：
  - **psql 16**：MD5/trust 认证、简单查询、跨分片 ORDER BY+LIMIT 归并、COUNT 归并、
    事务 begin/commit、`\bind` 扩展协议参数绑定（PQexecParams → Parse/Bind/Execute）、
    语法错误 ErrorResponse 字节级校验
  - **sqlcmd (ODBC Driver 13)**：PreLogin/Login7 登录、跨分片 select、count 归并、
    分片 insert、NULL 保真显示；登录应答 token 流经代理抓包逐 token 比对规范
  - **mysql CLI**：回归验证嗅探不影响 MySQL 路径；错误密码标准 1045
  - 手写 Python TDS 客户端：独立实现协议解析交叉验证（token 流、NULL 0xFFFF 标记）
  - TNS CONNECT 包 → 明确拒绝响应验证（可行性评估见 oracle-tns-feasibility.md）
- 真实双 MySQL 实例冒烟（DDL 自动化）：空实例上 `create database` + `create table
  ... dbpartition/tbpartition` 自动创建 db2_0/db2_1 物理库与 4 分片表；INSERT 省略
  自增列自动填充 Snowflake ID 且按填充值正确路由；广播表 insert/update/delete
  双实例物理同步一致（rows=1 v1=z）；`drop table` 双侧物理清除 + 配置移除；
  `show create table db2.orders` 返回含分片子句的逻辑 DDL；事务 + SAVEPOINT +
  ROLLBACK TO SAVEPOINT 语义正确（savepoint 前数据保留、其后回滚）；
  HTTP POST /api/reload 热切换数据源生效；psql / sqlcmd 同样可执行 DDL 自动化全流程
