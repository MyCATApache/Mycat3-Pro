# 纯代理模式 + MySQL：SQL 缓存 / SQL 拦截 / 慢 SQL 实验室

这个例子演示 Mycat3 Pro 的 **proxy-only 模式**：SQL 不做分片、不改写、不归并，
原样透传到单个 target。代理层只保留四类能力：

1. **SQL 结果缓存**（EXACT 与 PARAMETERIZED 两种匹配模式）
2. **SQL 拦截**（按正则 + 语句类型返回权限错误）
3. **慢 SQL 统计**（环形缓冲 + 运行时阈值 + Prometheus 指标）
4. **会话级固定连接**（`USE` / `SET` / 事务与直连语义一致）以及 HTTP 管理接口

```
mysql / psql / sqlcmd
        │  8066
        ▼
┌─────────────────────────────┐
│ Mycat3 Pro（mode=proxy）    │
│  拦截 → 缓存 → 固定连接透传 │
│  /metrics /api/sqlcaches    │
└──────────────┬──────────────┘
               │ 3306
               ▼
        MySQL 8.0（demo 库）
```

## 快速开始

```bash
cd examples/docker/proxy-mysql
docker compose up -d --wait
./demo.sh
```

`demo.sh` 会依次演示缓存、拦截、慢 SQL、会话和 HTTP 接口。也可以手动执行下面的命令。

连接 Mycat（在 `mysql` 容器内使用 mysql 客户端，通过 compose 网络访问代理）：

```bash
docker compose exec mysql \
  mysql --default-character-set=utf8mb4 -hmycat3-pro -P8066 -uroot -p123456 -Ddemo
```

> 演示密码均为测试用明文；生产不要复用。

## 1. SQL 结果缓存

### 1.1 EXACT：启动时预读的具体 SQL

`conf/sqlcaches/product_catalog.sqlcache.json`：

```json
{
  "name": "product_catalog",
  "sql": "select id, name, price, category from t_product order by id",
  "refreshInterval": 300,
  "matchMode": "EXACT",
  "maxRows": 1000,
  "maxBytes": 1048576,
  "schemaName": "demo"
}
```

这条 SQL **不含 `?`**，属于可确定结果的模板：Mycat 启动后会立即预执行一次并缓存；
之后每 300 秒后台刷新一次（先成功后原子替换，失败保留旧结果）。

```sql
select id, name, price, category from t_product order by id;
select id, name, price, category from t_product order by id;
/*!mycat:showSqlCaches*/;
```

`showSqlCaches` 会给出类似列：

| NAME | SQL | REFRESH_INTERVAL_S | MATCH_MODE | HAS_CACHE | VARIANT_COUNT | HITS | MISSES |
|---|---|---|---|---|---|---|---|
| product_catalog | select ... | 300 | EXACT | true | 0 | 2 | 0 |

### 1.2 PARAMETERIZED：`?` 是自由参数槽

`conf/sqlcaches/product_by_id.sqlcache.json`：

```json
{
  "name": "product_by_id",
  "sql": "select id, name, price, category from t_product where id = ?",
  "refreshInterval": 120,
  "matchMode": "PARAMETERIZED",
  "maxRows": 100,
  "maxBytes": 65536,
  "schemaName": "demo"
}
```

语义要点：

- 模板里的 `?` 是**自由参数**，不是字面量：`id=1` 和 `id=2` 都匹配这个模板；
- 但每个参数组合有**独立结果**：`id=1` 与 `id=2` 的结果绝不会共享；
- 因为参数值不可预测，带 `?` 的模板**不能启动预读**；第一次查询按需执行并回填，
  后续同一参数值命中缓存；
- 每个变体有自己的 TTL（`refreshInterval` 秒），变体上限 4096，超出逐出最旧；
- `LIMIT/OFFSET` 的字面量不参与参数化：分页 SQL 的每一页是独立缓存键，
  不会出现第 2 页返回第 1 页结果。

```sql
select id, name, price, category from t_product where id = 1;
select id, name, price, category from t_product where id = 1;  -- 命中
select id, name, price, category from t_product where id = 2;  -- 另一个变体
/*!mycat:showSqlCaches*/;
```

### 1.3 缓存文件与命中条件

- 文件：`conf/cache/sqlcache/<name>.bin` 和 `<name>_<hash>.bin`（参数变体）；
- 编码：二进制，异步写入；重启时从文件恢复（损坏文件忽略并重建）；
- 写入发生在预读/刷新/首次回填路径，落盘不是命中链路的同步阻塞点；
- 命中条件：`SELECT`、非事务、`autocommit=1`、SQL 无 hint；
- **写后失效**：DML/DDL 成功后按表名 token 保守清理相关缓存（内存 + 二进制文件）；
  写多读少的数据仍不要配置长 TTL（失效会导致频繁回填）；
- `maxRows` / `maxBytes` 超限不缓存，回落到正常后端执行；
- 删除缓存：`/*!mycat:dropSqlCache={"name":"product_by_id"}*/;`
  或直接删除对应 `.bin` 后重启。

### 1.4 写入后的缓存失效

Pro 在 DML/DDL 执行成功后，会按表名 token 保守清理相关缓存（清内存结果并删除 `.bin` 文件）：

```sql
update t_product set price = 409.00 where id = 1;
/*!mycat:showSqlCaches*/;       -- product_catalog / product_by_id 变为 false / 变体清空
select id, name, price, category from t_product order by id;   -- 回填 concrete 结果
select id, name, price, category from t_product where id = 1;  -- 回填参数变体
/*!mycat:showSqlCaches*/;       -- 再次变为 true / 变体数恢复
```

失效是启发式的：只要模板 SQL 的文本包含被写表名（按词边界）就会失效，
可能多失效，但不会返回旧数据；跨库同名表也可能被一并清理。

## 2. SQL 拦截

`conf/server.json` 的 `proxy.intercepts`：

```json
{
  "pattern": "^\\s*drop\\s+table\\b",
  "stmtType": "DDL",
  "errno": 1142,
  "sqlstate": "42000",
  "message": "proxy policy: DROP TABLE is blocked"
}
```

判定规则：

- 按数组顺序匹配，**首个命中**即返回错误；
- 正则默认大小写不敏感（这里仍显式写了小写模式，便于阅读）；
- `stmtType` 可为 `ALL` / `SELECT` / `DML` / `DDL` / `OTHER`；
- 默认错误码 `1142` / SQLSTATE `42000`，可自定义 `errno` / `sqlstate` / `message`；
- **拦截先于缓存**：即使 SQL 已缓存，也不会绕过拦截。

本示例配置了三条策略：

| 规则 | 效果 |
|---|---|
| `drop\s+table`（DDL） | 拒绝 DROP TABLE |
| `truncate\s+table`（DDL） | 拒绝 TRUNCATE |
| `delete\s+from\s+表名` 且没有 WHERE（DML） | 拒绝全表 DELETE |

验证：

```sql
drop table t_product;        -- ERROR 1142: proxy policy: DROP TABLE is blocked
delete from t_product;       -- ERROR 1142: DELETE without WHERE is blocked
delete from t_product where id = 999;  -- 放行（0 行）
```

> 正则只做文本匹配，不理解 SQL 语义。对安全要求高的场景，建议同时使用数据库账号
> 最小权限、只读账号和网络 ACL；拦截规则是应用层护栏，不是唯一防线。

## 3. 慢 SQL 统计

慢 SQL 环形缓冲固定 128 条，默认阈值 1000ms；可通过 hint 在运行时调整：

```sql
/*!mycat:setSqlTimeFilter{"value":10}*/;  -- 10ms（演示用，生产不要这么低）
select sleep(0.2) as slept;
/*!mycat:showSlowSql*/;
/*!mycat:getSqlTimeFilter*/;
```

`showSlowSql` 列：`trace_id`、`sql`、`sql_rows`、`start_time`、`end_time`、
`execute_time`、`target_name`，按耗时降序。`/metrics` 同时提供：

- `mycat3_queries_total`
- `mycat3_query_errors_total`
- 每个数据源的 `mycat3_datasource_alive` / `mycat3_pool_*`

慢 SQL 明细以 `/*!mycat:showSlowSql*/` 的环形记录为准（最近 128 条），
Prometheus 指标用于总览 QPS/错误/连接池，不替代慢 SQL 明细。

注意：

- 慢 SQL 记录发生在代理层，包含后端执行时间；代理排队/网络时间以整体统计为准；
- 环形缓冲只保留最近 128 条，适合在线排障，不做审计归档；
- 生产阈值建议从 p99 的 1.5–2 倍开始，例如 p99=80ms → 120–200ms；
- 记录本身有开销（保存 SQL 文本），高频慢查询时应配合采样/日志系统。

## 4. 会话级固定连接

代理模式下每个前端会话固定一条后端连接：

```sql
set @tenant = 'acme';
select @tenant;             -- 用户变量与直连一致

begin;
insert into t_order (id, product_id, qty) values (90001, 1, 1);
rollback;                   -- 回滚下推到同一条后端连接
select count(*) from t_order;

use demo;                   -- COM_INIT_DB 原样转发
```

会话结束时连接会回滚并**丢弃**，不会把临时表/用户变量泄漏给下一个客户端。
这也意味着代理模式不适合依赖连接池级会话状态的场景（如某些连接级临时表用法）。

## 5. HTTP 管理接口

```bash
curl -s http://127.0.0.1:19066/-/healthy
curl -s http://127.0.0.1:19066/metrics
curl -s http://127.0.0.1:19066/api/status | jq .
curl -s http://127.0.0.1:19066/api/sqlcaches | jq .
curl -s -X POST http://127.0.0.1:19066/api/reload
```

`/api/sqlcaches` 返回每个缓存的命中/未命中、变体数、文件大小等，便于确认
“缓存到底有没有生效”。

## 配置目录

```
conf/
├── server.json                         # mode=proxy、target、拦截规则
├── datasources/ds0.datasource.json     # MySQL 后端
├── users/root.user.json                # 前端账号
├── schemas/demo.schema.json            # 仅用于逻辑库可见性
└── sqlcaches/
    ├── product_catalog.sqlcache.json   # EXACT 预读
    ├── product_by_id.sqlcache.json     # PARAMETERIZED 按需变体
    └── category_stats.sqlcache.json    # EXACT 聚合结果
```

关键字段：

| 字段 | 作用 | 本例取值 |
|---|---|---|
| `mode` | `proxy` 关闭分片/重写/归并 | `proxy` |
| `proxy.target` | 透传目标（数据源或集群） | `ds0` |
| `proxy.intercepts[].pattern` | 正则匹配 SQL 文本 | 见上 |
| `proxy.intercepts[].stmtType` | 限定语句类型 | `DDL` / `DML` |
| `server.executeTimeout` | 单条后端 SQL 超时（毫秒） | 30000 |
| `server.protocolSniff` | 同端口三协议嗅探；MySQL-only 可关 | true |
| `datasource.maxCon/minCon` | 后端连接池上限/预热 | 8 / 1 |
| `sqlcache.refreshInterval` | TTL（秒） | 120–300 |
| `sqlcache.matchMode` | `EXACT` 或 `PARAMETERIZED` | 两种都有 |
| `sqlcache.maxRows/maxBytes` | 结果上限，超限不缓存 | 100/1000 行 |

## 设计取舍

- **为什么代理模式不配分片？** 代理模式的价值是低风险接入：不改变 SQL 语义、
  不引入路由/归并错误；缓存/拦截/慢 SQL 在代理层独立生效。
- **为什么缓存 TTL 是 120/300 秒？** 商品目录是读多写少；TTL 越长命中率越高，
  但数据新鲜度越差。写多表（库存、订单）不应配置长 TTL，或使用短 TTL。
- **为什么 `product_by_id` 用 PARAMETERIZED？** 同一个查询形状承接任意商品 ID，
  避免为每个 ID 维护独立配置；每个变体仍然独立结果。
- **为什么需要 maxRows/maxBytes？** 防止一次大结果把内存/磁盘打满；超限直接不缓存。
- **为什么建议 `monitor.sqlLog=false`？** 每条 SQL 落日志会显著增加 I/O；排障时再开。
- **为什么拦截规则放在缓存之前？** 权限策略必须对“已缓存 SQL”同样生效，
  否则缓存会成为绕过策略的通道。

## 排障

| 现象 | 处理 |
|---|---|
| 容器启动失败，端口被占用 | 修改 compose 中 `18066/19066/13316` 宿主端口 |
| `cache/sqlcache` 写入失败 | 配置目录需对容器 UID 1000 可写；不要用只读挂载 |
| SQL 不命中缓存 | 检查是否有事务、`autocommit=0`、hint、LIMIT 分页差异，或 SQL 文本不同 |
| 拦截规则不生效 | 用 `mycat3-pro check --home ./conf` 校验正则；确认 `stmtType` 匹配 |
| 看不到慢 SQL | 阈值是否设为 10ms；`showSlowSql` 只有最近 128 条 |
| 后端连接失败 | 数据源 host 是 `mysql`（compose 服务名），不是 `127.0.0.1` |

## 清理

```bash
docker compose down -v
rm -rf conf/cache conf/xa conf/backup
```
