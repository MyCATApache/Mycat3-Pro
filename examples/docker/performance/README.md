# 性能参数实验室（MySQL 后端）

这个 compose 把 **一个 MySQL 实例的两个 schema** 当作两个分片，在 Mycat3 Pro 上演示：

- `reactorNumber` / `executeTimeout` / `protocolSniff` / `monitor.sqlLog`
- 每个数据源的 `maxCon` / `minCon` / `idleTimeout` / `maxConnectTimeout`
- 读负载均衡、心跳阈值（配置项）
- SQL 结果缓存的 TTL、`maxRows` / `maxBytes`

它不是生产模板，也不承诺某个 QPS 数字；参数区间与容量公式见
[`docs/performance.md`](../../../docs/performance.md)。

## 快速开始

```bash
cd examples/docker/performance
docker compose up -d --wait
./bench.sh
```

`bench.sh` 会：

1. 预热 `order_catalog`（EXACT）和 `order_by_id`（PARAMETERIZED）缓存；
2. 用 mysql CLI 以 1/8 并发各执行 50 次相同查询，对比“命中缓存”和“不同投影（不命中）”；
3. 打印 `showSqlCaches`、`show @@datasource` 和 `/metrics`。

> 这是宿主机上的快速前后对比，数字受 docker exec、连接建立、机器负载影响。
> 真实容量请用 sysbench/mysqlslap 或生产流量，观察 p95/p99、池等待、后端 CPU。

## 关键配置

`conf/server.json`：

| 字段 | 本例 | 说明 |
|---|---|---|
| `reactorNumber` | 4 | Tokio 工作线程；按 CPU limit 设置，不是越大越好 |
| `executeTimeout` | 5000 | 单条后端 SQL 超时（ms）；池排队不计入 |
| `protocolSniff` | false | 只演示 MySQL，省掉每个新连接 300ms 嗅探等待 |
| `monitor.sqlLog` | false | 全量 SQL 日志会显著增加 I/O |
| `deprecateEof` | true | 现代 MySQL 客户端减少包往返 |

`conf/datasources/ds0|ds1`：

| 字段 | 本例 | 说明 |
|---|---|---|
| `maxCon` | 32 | 每个 schema/池的上限；两个池共 64 条后端连接上限 |
| `minCon` | 4 | 启动预热，降低冷启动抖动 |
| `idleTimeout` | 30000 | 空闲 30s 回收；要小于 MySQL `wait_timeout` |
| `maxConnectTimeout` | 2000 | 建连超时；故障时快速失败 |

`conf/sqlcaches`：

| 缓存 | 匹配 | TTL | 上限 |
|---|---|---|---|
| `order_catalog` | EXACT，20 行结果预读 | 30s | 1000 行 / 1MB |
| `order_by_id` | PARAMETERIZED，每个 id 独立变体 | 15s | 100 行 / 64KB |

## 手动观察

```bash
# 缓存
docker compose exec -T -e MYSQL_PWD=123456 mysql mysql \
  --default-character-set=utf8mb4 -hmycat3-pro -P8066 -uroot -Ddb1 \
  -e '/*!mycat:showSqlCaches*/;'

# 连接池
docker compose exec -T -e MYSQL_PWD=123456 mysql mysql \
  --default-character-set=utf8mb4 -hmycat3-pro -P8066 -uroot -Ddb1 \
  -e 'show @@datasource;'

# HTTP
curl -s http://127.0.0.1:29067/metrics
curl -s http://127.0.0.1:29067/api/status | python3 -m json.tool
```

## 调参实验

1. 把 `conf/datasources/*.json` 的 `maxCon` 从 32 调到 4，重启 Mycat，
   观察 8 并发下的客户端延迟/池 `available`；
2. 把 `reactorNumber` 调到超过容器 CPU limit（例如 16），观察 p99 是否变差；
3. 把 `executeTimeout` 调到 50ms，执行 `select sleep(0.1)` 观察超时；
4. 把缓存 `refreshInterval` 改成 0/300，观察命中率与数据新鲜度；
5. 在 MySQL 上执行写操作后观察缓存失效（`HAS_CACHE=false`）与回填。

每次只改一个变量，记录 p95/p99 与后端连接数，不要只看单次 `bench.sh`。

## 目录

```
performance/
├── docker-compose.yml       # MySQL + Mycat3 Pro
├── conf/
│   ├── server.json
│   ├── datasources/ds0|ds1
│   ├── clusters/c0|prototype
│   ├── schemas/db1.schema.json
│   └── sqlcaches/order_catalog|order_by_id
├── init/01-data.sql         # 20 行订单，按 id % 2 分到两个 schema
└── bench.sh
```

## 清理

```bash
docker compose down -v
rm -rf conf/cache conf/xa conf/backup
```
