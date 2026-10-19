# 性能参数与容量规划

这份文档只讨论 **Mycat3 Pro 当前实际生效**的配置，并给出起点区间。任何固定数值都不能替代
对 p95/p99、连接池等待、后端 `max_connections`、CPU 和网络的测量。

示例工程：[`examples/docker/performance`](../examples/docker/performance/README.md)

## 1. 请求路径与瓶颈

```
客户端
  │ 协议握手/编解码
  ▼
Mycat3 Pro
  ├─ SQL 解析/路由（CPU）
  ├─ SQL 结果缓存命中（内存，快路径）
  ├─ 连接池取连接（可能排队）
  ├─ 后端执行（网络 + 数据库）
  └─ 多分片归并/排序（CPU + 内存）
```

常见瓶颈按出现频率排序：

1. 后端数据库慢查询/锁等待；
2. 连接池 `maxCon` 太小导致排队；
3. 跨分片查询扇出（`IN`、范围、全表、广播写）；
4. Mycat CPU 被 SQL 解析/大结果归并占满；
5. 慢 SQL 日志、全量 SQL 日志、TLS 握手等附加开销；
6. 客户端连接数/文件描述符/端口耗尽。

## 2. 参数总表（生效范围）

| 参数 | 默认 | 作用 | 调大/调小的影响 |
|---|---|---|---|
| `server.reactorNumber` | CPU 核数 | Tokio 工作线程数 | 调大可能提高解析/归并吞吐；超过 vCPU 会争抢，增加尾延迟 |
| `server.executeTimeout` | 60000 ms | 单条后端 SQL 超时 | 太小误杀慢查询；太大让慢查询长期占用连接 |
| `server.protocolSniff` | true | 同端口 MySQL/PG/TDS 嗅探 | false 消除 MySQL 连接 300ms 首字节等待，但 PG/TDS 客户端不可用 |
| `server.deprecateEof` | true | MySQL `CLIENT_DEPRECATE_EOF` | 现代客户端下减少包往返；一般保持 true |
| `monitor.open` | true | HTTP 监控 | 开销很小，建议保留 |
| `monitor.sqlLog` | false | 每条 SQL 落日志 | true 会显著增加 I/O；仅排障短时开启 |
| `datasource.maxCon` | 1000 | 每个数据源连接池上限 | 调到后端承受范围；过小排队，过大打爆数据库 |
| `datasource.minCon` | 1 | 启动预热连接数 | 预热降低冷启动抖动；过高浪费后端槽位 |
| `datasource.idleTimeout` | 60000 ms | 空闲连接回收 | 太短频繁重连；太长占用后端连接（需小于数据库 wait_timeout） |
| `datasource.maxConnectTimeout` | 30000 ms | 建连超时 | 太长故障时等待久；太短瞬时抖动就失败 |
| `cluster.heartbeat.maxRetry` | 3 | 连续失败多少次判定故障 | 太小易误切换；太大故障发现慢 |
| `cluster.heartbeat.heartbeatTimeout` | 1000 ms | 单次心跳超时 | 跨 AZ/RTT 大时适当增大 |
| `cluster.heartbeat.minSwitchTimeInterval` | 300 s | 两次切换最小间隔 | 防止抖动，但延长恢复时间 |
| `cluster.readBalanceType` | BALANCE_ALL | 读实例选择范围 | 决定读流量是否分摊到从库 |
| `cluster.readBalanceName` | BalanceRandom | 读负载均衡策略 | Random 最便宜；LeastActive 更均衡但有状态开销 |
| `sqlcache.refreshInterval` | 60 s | 缓存 TTL/刷新间隔 | 越大命中率越高、数据越旧；写入会触发失效 |
| `sqlcache.maxRows` / `maxBytes` | 100000 / 0 | 单结果缓存上限 | 防止大结果占内存/磁盘；0 表示不限（不建议） |
| `sqlcache.matchMode` | PARAMETERIZED | EXACT 或参数化形状匹配 | 参数化命中面更广，但变体更多 |
| `user.loginLimit` | -1 | 同用户活跃连接上限 | 保护后端/Mycat；不是吞吐参数 |
| `server.sslCert/sslKey` | 未配置 | 前端 TLS | 增加握手 CPU；在代理层终止可减少后端 TLS |

**注意：**

- `cluster.maxCon` 字段存在但当前版本**不参与限流**，不要依赖它。
- `datasource.maxRetryCount` 是 Java 兼容字段，当前也不参与连接池重试策略。
- `executeTimeout` 是后端 SQL 执行超时；连接池排队时间不包含在内。池排队要看
  `available`/`active` 和客户端 p99。
- `reactorNumber` 不是“越大越好”：SQL 解析/归并是 CPU 工作，超过容器 CPU limit 会放大尾延迟。

## 3. 连接池：先算预算，再调 maxCon

### 3.1 一个粗略公式

```
峰值分片并发 ≈ QPS × p95 延迟(秒) × 平均扇出分片数
maxCon ≈ 峰值分片并发 × 1.2~1.5（余量）
```

例如：QPS=500、p95=20ms、平均扇出=2 → 峰值约 20 并发；`maxCon` 从 32 起步足够。

### 3.2 后端连接总预算

每个 Mycat 实例、每个数据源都有自己的池；多个实例指向同一数据库时要相加：

```
Σ(所有 Mycat 实例对该库的 maxCon) ≤ 后端 max_connections × 0.7
```

MySQL 默认 `max_connections=151`，还要给复制、管理、备份、其它应用留余量。
PostgreSQL 默认 `max_connections=100`，池要更保守；必要时用外部连接池。
Oracle 受 `processes/sessions` 限制，`maxCon` 要按实例参数留足。

### 3.3 事务与 XA 会占住连接

- 普通事务：每个参与数据源固定一条连接，事务越长，池占用越久；
- 跨分片 XA：每个分支都要占连接，prepare 到 commit 期间不释放；
- 广播写：一次写可能同时占用所有目标分片连接。

长事务/大批量广播写是池耗尽的最常见原因；先优化事务边界，再考虑加 `maxCon`。

### 3.4 起点建议

| 场景 | `maxCon`/分片 | `minCon` | 说明 |
|---|---|---|---|
| 开发/小流量（<50 QPS） | 8–16 | 2–4 | 单实例、2–4 分片 |
| 中等 OLTP（100–1000 QPS） | 20–64 | 10–20% maxCon | 配合 8–16 vCPU |
| 大流量（>1000 QPS） | 64–128 | 8–16 或 10–20% | 先确认后端连接预算；通常需要多实例/多分片 |

不要直接把 `maxCon=1000` 当成“高性能”；数据库侧连接数、内存和锁竞争通常先到瓶颈。

## 4. 线程与超时

- `reactorNumber`：从 `min(vCPU, 8)` 起步；CPU limit 为 4 的容器不要设 16。
  观察 Mycat 进程 CPU、p99 与 GC/内存（Rust 无 GC，但归并会占内存）。
- `executeTimeout`：
  - OLTP：2–10 秒（大于 p99.9）；
  - 报表/批处理：30–300 秒；
  - `0` = 不限制，不建议作为生产默认；
  - 单条语句可用 `/*!mycat:executeTimeout=1000*/` 覆盖，适合临时保护。
- `protocolSniff`：MySQL-only 且短连接很多时设为 false，可省掉每个新连接的 300ms
  嗅探等待；但这样 PG/TDS 客户端无法连接同一个 8066。
- `monitor.sqlLog`：只在排障时短时打开；全量 SQL 日志的 I/O 会直接吃掉吞吐。

## 5. SQL 结果缓存调优

缓存只对“**重复、昂贵、相对稳定**”的 SELECT 有意义：

- 命中率来自相同 SQL 形状/参数；`LIMIT/OFFSET` 的每一页是独立键；
- 写操作会按表名 token 保守失效相关缓存（清内存 + 文件），写多表缓存会频繁回填；
- 参数化缓存每个参数组合一个变体，单缓存变体上限 4096；变体太多会挤占内存/磁盘；
- `maxRows`/`maxBytes` 是安全阀，超过就不缓存；
- **缓存键包含 schema，不包含用户/角色/会话变量**。如果结果依赖当前用户、租户变量、
  RLS 或 `@变量`，不要配置缓存，否则可能把 A 用户的结果返回给 B 用户；
- 缓存文件异步写入 `<home>/cache/sqlcache`；命中在内存，落盘失败不影响查询但重启后
  可能重新回填。

起点 TTL：

| 数据特征 | 建议 `refreshInterval` |
|---|---|
| 商品目录/字典/静态配置 | 60–600 秒 |
| 首页榜单/聚合统计（可接受延迟） | 10–60 秒 |
| 库存/余额/订单状态（强实时） | 不缓存，或 1–5 秒 |
| 报表/大结果 | 谨慎；优先异步物化，不塞代理缓存 |

## 6. 读写分离与高可用参数

- `readBalanceType`：
  - `BALANCE_ALL_READ`：读只走从库（无从库回退主库），读写分离最彻底；
  - `BALANCE_ALL`：主从都参与读，适合从库与主库能力接近；
  - `BALANCE_NONE`：只读主库，强一致优先；
  - `BALANCE_READ_WRITE`：只读未延迟从库 + 主库。
- `slaveThreshold=0` 表示任何延迟都剔除从库；对跨地域复制可放宽到 1–5 秒，
  但要接受相应陈旧读。
- 心跳周期固定 5 秒；故障发现大致为 `maxRetry × 5s` 加上探测超时。
  LAN 可 `heartbeatTimeout=500–1000ms`、`maxRetry=3`；
  跨 AZ 可 2000–3000ms，避免网络抖动误切换。
- `minSwitchTimeInterval` 防止切换风暴；从 60–300 秒起步。

## 7. XA 的性能代价

- 每个分支多一次 `PREPARE`、一次持久化/fsync、一次 `COMMIT PREPARED`；
- 跨 N 个分片时，故障窗口和连接占用都随 N 增加；
- PostgreSQL 需要 `max_prepared_transactions>0`，该值也要计入容量；
- Oracle 后端不支持 XA；
- 只在“跨分片必须原子”的写路径使用 XA；单分片事务、可补偿业务用普通事务更便宜。

## 8. 不同后端的差异

| 后端 | 连接/并发特点 | 性能注意 |
|---|---|---|
| MySQL | 原生协议、原始载荷快路径；XA 原生 | `max_connections`、锁等待、网络 RTT 是主因 |
| PostgreSQL | 异步 tokio-postgres，prepare 描述缓存；结果需类型映射 | 连接成本较高；默认 100 连接；XA 需 prepared 槽位 |
| Oracle | rust-oracledb thin，阻塞调用放入 blocking 池；结果字符串化 | 单连接较重；`maxCon` 宜小；无 XA；日期/数值字符串化有额外 CPU |
| SQL Server | **仅前端 TDS**，不是后端 | 不能用 `dbType=mssql`；详见 tds-frontend 示例 |

PG/Oracle 的结果要经过“数据库类型 → 字符串行 → MySQL 文本载荷”的转换，
比 MySQL 原始载荷透传多一层 CPU；大结果集/跨分片归并时更明显。

## 9. 分片设计对性能的影响

- 点查按分片键裁剪到 1 个节点，最快；
- `IN`/范围查询扇出到多个分片，延迟由最慢分片决定；
- 跨分片 `ORDER BY + LIMIT` 需要归并排序，内存和 CPU 随结果行数增长；
- 广播表写入扇出到所有节点，写放大 N 倍；
- 透传模式不支持跨分片 JOIN/GROUP BY/复杂聚合（显式报错），不要把它们放进热路径；
- 分片键要选择高基数、稳定、查询必带的列；热点租户/热点 key 会造成单分片过载。

## 10. 监控与测量

- `GET /metrics`：QPS、错误、连接数、每数据源存活/池状态；
- `SHOW @@datasource`：`IDLE`/`AVAILABLE`/`CREATED`；
- `SHOW @@connection`：前端连接与查询统计；
- `SHOW STATISTIC` / `SHOW FULL STATISTIC`：QPS、慢查询速率等；
- `/*!mycat:showSlowSql*/`：最近 128 条慢 SQL（阈值可运行时调整）；
- 客户端侧统计 p50/p95/p99，而不是只看平均值；
- 压测时同时采集 Mycat CPU、后端 CPU、连接池等待、锁等待和网络。

## 11. 按规模的起点配置

以下只是起点，必须在目标硬件/数据量上压测后调整：

### 小型/开发（≤10M 行，<50 QPS，4 vCPU）

```json
{
  "reactorNumber": 2,
  "executeTimeout": 30000,
  "datasource": {"maxCon": 8, "minCon": 2, "idleTimeout": 60000},
  "sqlcache": {"refreshInterval": 120, "maxRows": 1000, "maxBytes": 1048576},
  "heartbeat": {"maxRetry": 3, "heartbeatTimeout": 1000, "minSwitchTimeInterval": 300}
}
```

### 中型 OLTP（100–1000 QPS，8–16 vCPU，2–8 分片）

```json
{
  "reactorNumber": 8,
  "executeTimeout": 5000,
  "datasource": {"maxCon": 32, "minCon": 4, "idleTimeout": 30000},
  "sqlcache": {"refreshInterval": 30, "maxRows": 1000, "maxBytes": 1048576},
  "heartbeat": {"maxRetry": 3, "heartbeatTimeout": 1000, "minSwitchTimeInterval": 120}
}
```

### 大型（>1000 QPS，16–32 vCPU，8+ 分片）

```json
{
  "reactorNumber": 16,
  "executeTimeout": 3000,
  "datasource": {"maxCon": 64, "minCon": 8, "idleTimeout": 30000},
  "sqlcache": {"refreshInterval": 10, "maxRows": 500, "maxBytes": 524288},
  "heartbeat": {"maxRetry": 3, "heartbeatTimeout": 1000, "minSwitchTimeInterval": 60}
}
```

大型场景通常需要多 Mycat 实例 + 每分片独立集群；上面的 `maxCon` 必须按第 3.2 节
的预算重新计算，不能直接复制。

## 12. 常见反模式

| 反模式 | 后果 | 建议 |
|---|---|---|
| `maxCon` 超过后端总连接预算 | 后端拒绝连接/内存耗尽 | 先算预算，多实例相加 |
| `reactorNumber` > 容器 CPU limit | 线程争抢、p99 恶化 | 按 vCPU/CPU limit 设置 |
| 对库存/余额配长 TTL 缓存 | 读到旧值 | 不缓存或 1–5 秒，接受失效回填 |
| 缓存依赖用户变量的查询 | 跨用户数据泄露 | 缓存键不含用户，禁止缓存 |
| 跨分片大结果 `ORDER BY` | 归并内存/CPU 打满 | 缩小结果、带分片键、预聚合 |
| 大表广播写 | 写放大、部分失败 | 只广播小字典表 |
| 为所有事务用 XA | 2PC 开销显著 | 仅跨分片原子写使用 |
| 依赖 `cluster.maxCon` 限流 | 实际不生效 | 用 datasource.maxCon/loginLimit/前置限流 |
| 只看平均延迟 | 掩盖长尾 | 看 p95/p99、池等待 |

## 13. 调优顺序

1. 先定位慢查询/锁等待，优化 SQL 和索引；
2. 再确认分片键和扇出是否合理；
3. 计算连接池预算，调整 `maxCon/minCon`；
4. 根据 CPU 调整 `reactorNumber`，避免超卖；
5. 只对稳定的热查询启用缓存，并设置上限/TTL；
6. 调整超时、心跳、读写分离；
7. 最后才考虑增加实例/分片/硬件。
