# 架构设计

本文描述 Mycat3 Pro（Rust）的架构、请求生命周期，以及与 Mycat3（Java）的模块映射关系。

## 1. 总体结构

Mycat3 Pro 是一个 Cargo workspace，8 个 crate 按依赖方向分层：

```
┌─────────────────────────────────────────────────────────┐
│ server (bin: mycat3-pro)                                 │
│   bootstrap  TCP 监听/连接分发      state   全局服务注册表   │
│   frontend   握手认证/命令循环      commands 本地 SQL 命令   │
│   executor   后端执行/结果归并      tx      事务会话        │
│   backend    BackendPool 双后端分派（MySql/Oracle）        │
│   monitor    HTTP 监控                                     │
├───────────────┬───────────────────────────────────────────┤
│ sql           │ replica                 │ oracle          │
│  meta   元数据 │  balance   6 种负载均衡策略│  rust-oracledb │
│  route  路由   │  heartbeat 心跳检测/故障切换│  thin 驱动封装  │
│  hint   提示   │  runtime   集群→数据源解析  │  自研阻塞连接池  │
│  oracle 方言翻译│                            │  结果字符串模型化│
├───────────────┼─────────────────────────────┴────────────────┤
│ router        │ client                                      │
│  17 种分片算法 │  conn  原生 MySQL 协议连接                   │
│  Java 兼容哈希 │  pool  每数据源连接池                        │
├───────────────┴─────────────────────────────────────────────┤
│ config    Mycat3 兼容 JSON 配置模型/文件存储                   │
│ protocol  MySQL 协议编解码（无业务依赖）                       │
└──────────────────────────────────────────────────────────────┘
```

## 2. Java ↔ Rust 模块映射

| Mycat3 (Java) | Mycat3 Pro (Rust) | 说明 |
|---|---|---|
| `MycatCore.main` / `MetaClusterCurrent` | `server/src/main.rs` + `state::ServerState` | 启动引导与全局服务注册 |
| `VertxMycatServer` / `MycatSessionManager`（SO_REUSEPORT × reactorNumber） | `server/src/bootstrap.rs`（tokio 多线程 runtime，worker 数 = reactorNumber） | 监听与连接分发 |
| `io.mycat.protocol.ProtocolDetector` + 嗅探时序（300ms/2s/2KB） | `server/src/sniff.rs` | 多协议同端口嗅探 |
| `protocol/pg/PGProtocolHandler` + `PGResponse` | `server/src/pg.rs` | PG v3 前端（Pro 真实实现扩展协议 + OID 映射） |
| `protocol/tds/TDSProtocolHandler` + `TDSResponse` | `server/src/tds.rs` | TDS 7.x 前端（Pro 实现 sp_executesql RPC + NULL 保真） |
| `protocol/tns/*`（无认证 PoC） | 嗅探识别后明确拒绝 | 真实 Oracle 客户端需 TTC/O5LOGON，不做假实现 |
| `protocol/api/MycatProtocolBackend`（多协议共享执行门面） | `server/src/engine.rs` | 协议无关执行层 |
| `VertxMySQLAuthHandler` + `MySQLClientAuthHandler.createHandshakePayload` | `frontend.rs::handshake` + `protocol::packets::build_handshake_v10` | 握手与认证 |
| `VertxMySQLPacketResolver`（HEAD/PAYLOAD 状态机） | `protocol::frame::FrameDecoder` | 流式分包解码（Pro 修复了 >16MB 重组缺陷） |
| `MycatVertxMySQLHandler.handle0`（命令分派，future 链串行化） | `frontend.rs::command_loop`（每连接单任务，天然串行） | 命令循环 |
| `MycatdbCommand.executeQuery`（Druid parse + sqlHandlerMap） | `sql::route::analyse`（sqlparser）+ `commands.rs`（本地 handler） | 语句分派 |
| `HackRouter`（单目标快速下推） | `sql::route`（全部语句走同一分析路径） | 路由 |
| `ValueIndexCondition`（分片条件提取） | `sql::route::extract_conditions`（=/IN/BETWEEN/范围，OR 退化全扫） | 条件裁剪 |
| `MysqlTableReplacer` / `CalciteUtls.getBackendTaskSQL` | `sql::route::TableRewriter`（VisitorMut pre_visit_relation） | 物理 SQL 重写 |
| `io.mycat.QueryBackendTask` | `sql::BackendTask { target_name, sql }` | 路由单元 |
| `MetadataManager` | `sql::meta::Metadata` | 逻辑库表元数据 |
| `router/mycat1xfunction/*`（PartitionByMod 等） | `router/src/mycat1x.rs` | 分片算法逐一移植 |
| `router/function/AutoFunction(+Factory)` | `router/src/auto.rs` | dbMethod/tableMethod/mappingFormat |
| `router/util/PartitionUtil`（1024 槽段表） | `router::function::PartitionUtil` | 逐位一致 |
| `SecurityUtil.scramble411` / `CachingSha2PasswordPlugin` | `protocol::auth` | 认证算法 |
| `MySQLPacketUtil` / `ColumnDefPacketImpl` / `ErrorPacketImpl` | `protocol::packets` | 包构造 |
| `JdbcConnectionManager` + Druid 池 / vert.x `NewMycatConnection` | `client::pool::ConnectionManager` + `client::conn::MySQLConnection` | Pro 统一为原生协议 |
| `datasource` 的 jOOQ parsingDataSource（非 MySQL 后端方言翻译） | `server::backend::BackendPool`/`BackendSession` + `sql::oracle::translate_sql` | Oracle 后端：官方 rust-oracledb thin 驱动（`crates/oracle`，spawn_blocking 阻塞隔离 + 自研池）+ MySQL→Oracle 方言翻译，结果合成 MySQL 文本行载荷 |
| `ReplicaSelectorRuntime` / `ReplicaDataSourceSelector` | `replica::ReplicaSelectorRuntime` / `ReplicaSelector` | 集群解析与读写分离 |
| `plug.loadBalance.*`（BalanceRandom 等 6 种） | `replica::balance` | 负载均衡策略 |
| `replica.heartbeat.*`（HeartbeatFlow + 4 种策略） | `replica::heartbeat` | 心跳/切换 |
| `TransactionSessionTemplate` / `LocalTransactionSession` | `server::tx::TransactionSession` | 本地事务 |
| `ReceiverImpl` / `VertxExecuter`（CompositeFuture.join + sum/max） | `server::executor` | 执行与归并 |
| `MysqlPayloadObjectObserver`（原始载荷透传写回） | `frontend::write_result_set`（raw payload 重分帧） | 结果回写 |
| `PrometheusExporter` / `HTTPServer`（/-/healthy） | `server::monitor`（/metrics /api/status /-/healthy） | 监控 |
| `FileStorageManagerImpl` / `FileKV` / `ConfigUpdater` | `config::FileConfigStorage` | 配置存储 |
| Calcite 引擎（`hbt` DrdsRunner、跨分片 JOIN/GROUP BY）、XA（`va`）、migrate、BASELINE/SPM | **未移植** —— 返回明确的 `ER_NOT_SUPPORTED_YET` 错误 | 见 compatibility.md |

## 3. 请求生命周期（COM_QUERY）

```
客户端字节流
  → FrameDecoder（3 字节长度 + 序号；0xFFFFFF 分包重组）
  → command_loop 按命令字节分派（序号通过 setPacketId 语义延续到响应）
  → COM_QUERY:
     1. probe_local_select       # select @@version 等探针本地应答
     2. sql::analyse(sql, schema, metadata)
        a. hint 解析（/*!mycat:target=…*/ → 直接透传）
        b. sqlparser AST 解析（多语句 → 多条 RouteResult）
        c. 表引用收集 → 元数据判定 NORMAL/GLOBAL/SHARDING
        d. SHARDING: WHERE 提取分片列条件 → RuleFunction.calculate → 命中节点
        e. AST 重写逻辑表名 → 每节点 QueryBackendTask
        f. 多任务 SELECT 生成 MergePlan（ORDER BY/LIMIT 改写、聚合检测）
     3. executor::execute_route
        a. target → ReplicaSelectorRuntime.resolve（写/事务→主库；读→负载均衡）
        b. 事务中：TransactionSession 按数据源固定连接；否则连接池取用
        c. 单任务读：原始载荷透传；多任务：按数据源分组并行（spawn+join）
        d. 写：顺序执行，affectedRows 求和、lastInsertId 取最大
        e. MergePlan：行拼接/归并排序/OFFSET+LIMIT、COUNT/SUM/MAX/MIN
     4. SessionWriter 回写（column_count → 列定义 → [EOF] → 行 → EOF/OK，
        遵循 DEPRECATE_EOF 与 MORE_RESULTS 语义）
     5. closeStatementState：非事务时归还连接
```

## 4. 关键设计决策

### 4.1 原生协议替代 JDBC
Java 版热路径同时存在 JDBC（Druid）与 vert.x 原生客户端两套后端栈。Pro 版统一为自研原生
异步客户端：少一层驱动、少一份缓冲拷贝，透传路径下后端行载荷可以原样重新分帧发给前端。

### 4.2 sqlparser 替代 Druid/Calcite
Java 版用 Druid 做语法树、Calcite 做分布式优化执行。Pro 版聚焦透传路由模式：
sqlparser（MySQL 方言）完成解析与 AST 级表名重写；跨分片复杂查询（JOIN/GROUP BY）
返回明确错误而不是给出错误结果。这覆盖了 Mycat3 生产中占比最高的
`HackRouter` 单表下推路径。

### 4.3 分片算法逐位一致
所有哈希（`String.hashCode`、Thomas Wang、Guava murmur3_32 + `hashUnencodedChars` 的
UTF-16LE 语义、Google Jump Consistent Hash、CRC-32、1024 槽段表）均按 Java 源码逐位移植，
并有已知向量单测（如 `murmur3_32(0,"test")=0xba6bd213`、`"root".hashCode()=3506402`）。
同一份配置在 Java 版与 Pro 版之间迁移，数据落点完全一致。

### 4.4 并发模型
- 每前端连接一个 tokio 任务，命令按到达顺序串行处理（等价 Java 版 sequenceFuture 链）。
- 多分片并行读：按数据源分组，`tokio::spawn` 并发执行后归并；事务中退化为串行（固定连接不可跨任务共享）。
- 连接池：`Arc<Semaphore>`（maxCon 上限）+ 空闲栈（取用时空闲窗口内免 ping 直用、超窗 COM_PING 校验、空闲超时剔除、事务连接不回池、broken 标记丢弃）；Oracle 池按 dirty 标记按需回滚净化。
- 心跳：每 5s 对全部数据源并发探测（对应 Java `TimerConfig` period=5s）。
- Snowflake：无锁 CAS（(last_millis<<12|seq) 单原子量），无互斥无 sleep，时钟回拨沿用上一毫秒保证单调。
- 跨分片归并：无 ORDER BY 直接拼接/截断原始行载荷（零解码零重编码）；聚合归并只解码每分片首行；有排序时排序键一次计算（按列类型统一策略，保证全序）。

### 4.5 序号（packet seq）语义
与 Java 版一致：响应包序号从命令包序号延续（`setPacketId(cmdSeq)`，每写一包 `++`），
16MB 分包自动递增；前端透传后端载荷时按新序号重新分帧。

### 4.6 Oracle 后端（dbType=oracle）
- 分派：`BackendPool::get` 按数据源 `dbType` 返回 MySql 或 Oracle 池；物理 SQL 在路由重写后
  经 `translate_for_backend` 做方言翻译（MySQL 直传）。翻译层只负责"语法形状"，
  不可翻译的结构显式报错（绝不静默产生错误语义）。
- 执行：驱动为同步阻塞 API，全部调用在 `spawn_blocking` 中执行；结果按 `DB_TYPE_*` 渲染为
  字符串行模型并**合成 MySQL 文本行载荷**（ColumnDef + text row），使归并/透传/三前端回写
  零改动复用。
- 事务：驱动默认 autocommit=false——事务外语句由 `BackendSession` 在成功后即时 COMMIT
  （对应 JDBC autocommit=true）；事务内连接按数据源固定，`BEGIN` 为本地无操作
  （Oracle 隐式开事务），COMMIT/ROLLBACK 走驱动接口，SAVEPOINT 直传；连接归还池时
  回滚残留事务（净化）。
- 心跳：`select 1 from dual`（SINGLE_NODE 语义）；DDL 自动化仅对 MySQL 后端生成物理 DDL，
  Oracle 分片表由 DBA 手工创建。
- TDS 前端适配：`SELECT [ALL|DISTINCT] TOP (n)` 在 TDS 会话层改写为 `LIMIT n`
  （多语句批中 LIMIT 归属首语句；PERCENT/WITH TIES 不支持）。
- 边界：不支持 ANO 原生加密（TLS 可用）；跨 MySQL+Oracle 混合事务不保证原子性（无 XA）。

## 5. 线程/任务栈说明

debug 构建下 `前端任务 → 执行器 → 后端客户端` 的异步 poll 链叠加 sqlparser 递归下降解析
的栈帧较大，生产 runtime 设置了 8MB 工作线程栈（`main.rs`），集成测试使用 16MB。
release 构建栈帧显著缩小，该配置仅是保险。
