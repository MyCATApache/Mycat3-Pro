# 分库分表 + MySQL 后端：四种典型分片策略与 XA

这个例子用两个 MySQL 8 实例（`ds0`/`ds1`）组成一个逻辑库 `db1`，
在同一个 schema 配置里演示五种常见落点：

| 逻辑表 | 物理布局 | 路由方式 | 典型用途 |
|---|---|---|---|
| `orders` | 4 张表分散在 2 个实例 | `PartitionByMod(id, 4)` | 订单/交易类，按主键均匀打散 |
| `tenant_order` | 每租户固定节点 | `PartitionByFileMap(tenant_id)` | B2B 多租户，按租户隔离 |
| `events` | 4 个月度分区 | `PartitionByMonth(created)` | 日志/事件/按月归档 |
| `dict` | 两个实例各一份 | 广播表 | 小字典、配置表 |
| `user_info` | 固定 `ds0.db1_0` | 单表 | 不按业务键分片的小表 |

```
           Mycat3 Pro :28066
        ┌────────┴────────┐
        ▼                 ▼
  mysql-shard-0      mysql-shard-1
  db1_0:             db1_1:
    orders_0/1         orders_2/3
    tenant_order_0     tenant_order_1
    events_0/1         events_2/3
    dict               dict
    user_info
```

## 快速开始

```bash
cd examples/docker/sharding-mysql
docker compose up -d --wait
./demo.sh
```

手动连接：

```bash
docker compose exec mysql-shard-0 \
  mysql --default-character-set=utf8mb4 -hmycat3-pro -P8066 -uroot -p123456 -Ddb1
```

> 演示使用明文测试密码；生产请改用密钥管理。重复运行 `demo.sh` 会先按 id 范围
> 清理演示数据；要彻底重建请 `docker compose down -v`。

## 1. 取模分片：orders

```json
"orders": {
  "partition": {
    "data": [
      ["ds0", "db1_0", "orders_0"],
      ["ds0", "db1_0", "orders_1"],
      ["ds1", "db1_1", "orders_2"],
      ["ds1", "db1_1", "orders_3"]
    ]
  },
  "function": {
    "clazz": "PartitionByMod",
    "properties": {"columnName": "id", "count": "4"}
  }
}
```

演示：

```sql
insert into orders (id,user_id,amount,status,created) values
 (1,101,10.00,'NEW','2026-01-01 10:00:00'),
 (2,102,20.00,'PAID','2026-01-02 10:00:00'),
 (3,103,30.00,'NEW','2026-01-03 10:00:00'),
 (4,104,40.00,'PAID','2026-01-04 10:00:00'),
 (5,105,50.00,'NEW','2026-01-05 10:00:00');

/*!mycat:showTopology{orders}*/;
explain select * from orders where id = 3;
select id,amount,status from orders where id = 3;
select count(*) as order_count from orders;
select sum(amount) as amount_sum from orders;
select id,amount from orders order by amount desc limit 3;
```

`showTopology` 输出四条物理落点；`explain` 可见 `id=3` 只发往
`ds1.db1_1.orders_3`。跨分片 `count(*)`、`sum(amount)`、`order by + limit`
由 Mycat 归并。

**为什么这样设计：**

- 取模让主键分布最均匀，单点查询/按主键更新只打一个分片；
- 分片数 4 与 2 个实例解耦：每实例 2 张物理表，后续可在实例内加表；
- `id` 是不可变、高基数、查询必带的键，适合作为分片键；
- 范围查询/`IN (...)` 会退化为多分片扇出，`GROUP BY`/跨分片 JOIN 不支持；
- 取模的扩容需要重新分布数据（4→8），不能只加实例；大规模时应提前规划或使用一致性哈希/范围分片。

> 当前透传模式不支持“COUNT 与 SUM 混合投影/跨分片 GROUP BY”。
> 所以示例把 `count(*)` 和 `sum(amount)` 分开查询；这是有意的能力边界，
> 不是配置错误。

## 2. 枚举分片：tenant_order

```json
"function": {
  "clazz": "PartitionByFileMap",
  "properties": {"columnName": "tenant_id", "defaultNode": "0"},
  "ranges": {"acme": "0", "globex": "1"}
}
```

```sql
insert into tenant_order (id,tenant_id,note) values
 (101,'acme','tenant-a order'),
 (102,'globex','tenant-b order'),
 (103,'acme','tenant-a order 2');

select * from tenant_order where tenant_id='acme' order by id;
select * from tenant_order where tenant_id='globex' order by id;
```

`acme` 全部落 `ds0`，`globex` 全部落 `ds1`。

**为什么这样设计：**

- 同一租户的数据物理聚集，租户内查询/事务只访问一个节点；
- 便于按租户做备份、迁移、限额和故障隔离；
- 枚举表是显式白名单，新增租户要改配置并迁移数据；
- 大客户会形成热点（单节点容量/连接数），需要把大租户单独放节点或再做子分片；
- `defaultNode=0` 是兜底，未知租户会落到 0 号节点；生产建议显式拒绝未知租户（前置校验）或定期审计。

## 3. 月份分片：events

```json
"function": {
  "clazz": "PartitionByMonth",
  "properties": {
    "columnName": "created",
    "beginDate": "2026-01-01 00:00:00",
    "endDate": "2026-04-01 00:00:00",
    "dateFormat": "yyyy-MM-dd HH:mm:ss"
  }
}
```

4 个物理表对应 2026-01 ~ 2026-04；查询 `created between ...` 只访问覆盖的月份：

```sql
insert into events (id,event_type,created) values
 (1,'login','2026-01-15 10:00:00'),
 (2,'buy','2026-02-10 11:00:00'),
 (3,'login','2026-03-05 12:00:00'),
 (4,'buy','2026-04-01 13:00:00');

select * from events
where created between '2026-01-01 00:00:00' and '2026-02-28 23:59:59'
order by id;
```

**为什么这样设计：**

- 时间范围查询可以裁剪到少数月份，适合日志/事件/账单；
- 过期数据可按月归档/删除，运维边界清晰；
- 分区数由保留周期决定；当前实现超出 `endDate` 后会按 `n_count` 循环回绕（示例 4 个月后回到 0 号分区），
  因此生产要把 `endDate` 与保留策略对齐，或定期扩分区；
- `dateFormat` 必须与实际 SQL 字面量一致；本例用完整时间戳，避免只写 `yyyy-MM-dd` 时解析失败；
- 跨月份 `count`/`order by` 需要归并，按月查询尽量带时间范围。

## 4. 广播表：dict

```sql
insert into dict (id,name) values (1,'CN'),(2,'US');
select * from dict order by id;
```

- 读走第一个节点（`ds0`）；
- INSERT/UPDATE/DELETE/DDL 扇出到所有节点；
- 两个分片的 `dict` 都应有两行（见 `demo.sh` 的物理核对）。

**为什么这样设计：** 字典/币种/配置表很小且读多写少，广播可以避免跨节点 JOIN；
写入放大到 N 个节点，**不是原子操作**，大表或频繁写入不适合广播。
需要强一致时用配置中心/缓存，或接受“写后最终一致”的运维补偿。

## 5. 单表：user_info

```json
"user_info": {
  "locality": {"targetName": "ds0", "schemaName": "db1_0", "tableName": "user_info"}
}
```

单表固定落 `ds0.db1_0`，不参与分片。适合数据量小、没有自然分片键、
需要和本节点其他表做本地 JOIN 的表。容量上限是单节点；增长后要改造成分片表。

## 6. XA 事务

`root` 用户是普通代理事务，`xa` 用户使用 MySQL XA 2PC：

```bash
docker compose exec mysql-shard-0 \
  mysql --default-character-set=utf8mb4 -hmycat3-pro -P8066 -uxa -p123456 -Ddb1
```

```sql
begin;
insert into orders (id,user_id,amount,status,created) values (201,1,1.00,'XA','2026-02-01 00:00:00');
insert into orders (id,user_id,amount,status,created) values (202,2,2.00,'XA','2026-02-02 00:00:00');
commit;
```

id=201 落 `orders_1`（ds0），id=202 落 `orders_2`（ds1）；提交时 Mycat 对两个分支
执行 `XA PREPARE`，写决策 WAL（`conf/xa/xa-wal.log`），再广播 `XA COMMIT`；
回滚同理。`demo.sh` 还演示了 XA 回滚和普通事务回滚。

**代价与边界：**

- 2PC 有 prepare + fsync + 两次网络往返，吞吐低于本地事务；
- 只对 MySQL/PostgreSQL 后端提供；Oracle 后端分支会显式拒绝；
- XA 不解决网络分区下的“不确定事务”，故障瞬间仍可能有 in-doubt 分支；
  启动时按 WAL 做恢复决策，但无法承诺 100% 原子；
- 只有跨多个分片且必须原子时才使用；单分片事务用普通事务更便宜。

## 配置目录

```
conf/
├── server.json
├── datasources/ds0.datasource.json
├── datasources/ds1.datasource.json
├── clusters/c0.cluster.json
├── clusters/prototype.cluster.json
├── users/root.user.json          # 普通事务
├── users/xa.user.json            # XA 2PC
└── schemas/db1.schema.json
init/
├── shard0.sql                    # db1_0 物理库表
└── shard1.sql                    # db1_1 物理库表
```

关键配置：

| 配置 | 本例 | 说明 |
|---|---|---|
| `datasource.maxCon` | 16 | 每个分片的连接池上限；两个分片共 32 条后端连接 |
| `datasource.minCon` | 2 | 启动预热，避免冷启动抖动 |
| `cluster.clusterType` | `SINGLE_NODE` | 两个分片都是可写单节点，不做主从切换 |
| `cluster.masters` | `["ds0","ds1"]` | 未注册表的默认读目标；分片表按 `partition.data` 显式路由 |
| `readBalanceType` | `BALANCE_ALL` | 非分片读可在两个分片间均衡 |
| `schema.targetName` | `c0` | 未注册 SQL 的默认 target |
| `shardingTables.*.partition.data` | 显式三元组 | `[数据源, 物理库, 物理表]`，最直观、可审计 |
| `function.clazz/properties` | 见上 | 分片算法与列；`function.name` 也可用别名 |
| `transactionType` | `proxy` / `xa` | 按用户选择事务策略 |

物理表由 `init/shard0.sql`、`init/shard1.sql` 预先创建。MySQL 后端支持 DDL 自动化
（`CREATE TABLE` 可扇出建表），但本示例用初始化 SQL 保证可重复、可审计；
PG/Oracle/SQL Server 后端同样支持自动物理 DDL，init 脚本是可选的。

## 与其它后端的关键差异

| 点 | MySQL（本例） | PostgreSQL | Oracle |
|---|---|---|---|
| 物理库概念 | database | schema（连接固定 database） | schema/user |
| 建表 | 可 DDL 自动化，也可 init SQL | DBA 手工建 schema/table | DBA 手工建 user/table |
| 标识符 | 反引号/大小写不敏感 | 双引号、大小写敏感 | 双引号、默认大写 |
| XA | 原生 XA 2PC | `max_prepared_transactions>0` | thin 驱动无 XA，显式拒绝 |
| 日期字面量 | 直接 | 直接 | 需 `NLS_DATE_FORMAT`/`TO_DATE` |

## 排障

| 现象 | 处理 |
|---|---|
| `count(*)`/`sum` 混合查询报 1235 | 拆成单聚合分别查询；跨分片复杂聚合不在透传模式支持范围 |
| 某条数据查不到 | 用 `showTopology{表}` + `explain` 确认分片键值；范围查询会扇出 |
| 未知租户落到 0 号节点 | 这是 `defaultNode` 行为；生产应前置校验或调整枚举 |
| `events` 的 5 月数据回到 0 号分区 | 月份分区在超过 `endDate` 后按分区数回绕；调整保留周期 |
| XA 测试报错 | 确认使用 `-uxa`；检查 `conf/xa` 可写；MySQL 用户需有 XA 权限（root 已满足） |
| 广播表两节点不一致 | 广播写不是原子操作；检查部分失败日志并做补偿/重建 |

## 清理

```bash
docker compose down -v
rm -rf conf/xa conf/cache conf/backup
```
