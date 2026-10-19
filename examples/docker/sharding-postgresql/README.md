# 分库分表 + PostgreSQL 后端：schema 分片、双协议接入与 XA

这个例子把两个 PostgreSQL 16 实例作为后端，用 Mycat3 Pro 暴露同一个逻辑库 `db1`。
前端既可以用 **PostgreSQL 协议（psql）**，也可以用 **MySQL 协议**；后端统一通过
`dbType=postgresql` 的 tokio-postgres 连接池访问。

```
psql / mysql client
        │ 8066（同端口协议嗅探）
        ▼
   Mycat3 Pro :38066
        ┌────────┴────────┐
        ▼                 ▼
   pg-shard-0         pg-shard-1
   database db1       database db1
   schema db1_0       schema db1_1
     orders_0/1         orders_2/3
     tenant_order_0     tenant_order_1
     dict               dict
     user_info
```

> 关键差异：PostgreSQL 的一个连接固定连一个 **database**，所以分片落在
> **schema**（`db1_0`/`db1_1`）里，而不是像 MySQL 那样落在 database。

## 快速开始

```bash
cd examples/docker/sharding-postgresql
docker compose up -d --wait
./demo.sh
```

psql 连接（PostgreSQL 前端）：

```bash
docker compose exec pg-shard-0 \
  psql "postgresql://root:123456@mycat3-pro:8066/db1?sslmode=disable"
```

MySQL 客户端连接（在其它容器中）：

```bash
docker run --rm -it --network mycat3-sharding-postgresql_default \
  -e MYSQL_PWD=123456 --entrypoint mysql mysql:8.0 \
  --default-character-set=utf8mb4 -hmycat3-pro -P8066 -uroot -Ddb1
```

## 1. 取模分片 orders

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
  "function": {"clazz": "PartitionByMod", "properties": {"columnName": "id", "count": "4"}}
}
```

```sql
insert into orders (id,user_id,amount,status,created) values
 (1,101,10.00,'NEW','2026-01-01 10:00:00'),
 (2,102,20.00,'PAID','2026-01-02 10:00:00'),
 (3,103,30.00,'NEW','2026-01-03 10:00:00'),
 (4,104,40.00,'PAID','2026-01-04 10:00:00');

/*!mycat:showTopology{orders}*/;
explain select * from orders where id = 3;
select id,amount,status from orders where id = 3;
select count(*) as order_count from orders;
select sum(amount) as amount_sum from orders;
select id,amount from orders order by amount desc limit 3;
```

路由语义与 MySQL 后端完全一致：`id=3` 只到 `ds1.db1_1.orders_3`，
`count`/`sum`/`order by + limit` 在 Mycat 归并。`count` 与 `sum` 请分开查询
（透传模式不支持跨分片混合聚合）。

## 2. 枚举分片、广播表、单表

- `tenant_order`：`PartitionByFileMap`，`acme`→`db1_0`，`globex`→`db1_1`；
- `dict`：广播到两个 schema，读第一个节点、写扇出；
- `user_info`：固定 `db1_0.user_info`。

```sql
insert into tenant_order (id,tenant_id,note) values
 (101,'acme','tenant-a'),(102,'globex','tenant-b');
insert into dict (id,name) values (1,'CN'),(2,'US');
insert into user_info (id,name,profile) values (1,'alice','vip');

select * from tenant_order where tenant_id='acme';
select * from dict order by id;
select * from user_info;
```

物理核对：

```bash
docker compose exec pg-shard-0 psql -U postgres -d db1 \
  -c "select count(*) from db1_0.dict; select count(*) from db1_0.orders_0;"
docker compose exec pg-shard-1 psql -U postgres -d db1 \
  -c "select count(*) from db1_1.dict; select count(*) from db1_1.orders_2;"
```

## 3. PostgreSQL 后端的关键配置差异

| 项 | MySQL 后端 | PostgreSQL 后端（本例） |
|---|---|---|
| 物理库 | database | database 固定，分片用 schema |
| URL | `jdbc:mysql://host:3306/db1_0` | `jdbc:postgresql://host:5432/db1` |
| 逻辑分片落点 | `schemaName` = database | `schemaName` = PostgreSQL schema |
| 标识符 | 反引号、大小写不敏感 | 双引号、大小写敏感；物理表用小写 |
| 建表 | DDL 自动化可用 | DBA/init SQL 手工建 schema+table |
| XA | 原生 `XA START/PREPARE` | `PREPARE TRANSACTION`，需 `max_prepared_transactions>0` |
| 自增 | `AUTO_INCREMENT` | 无；用 sequence/应用生成 |
| 心跳 | `select 1` | `select 1` |
| 类型 | 原始载荷透传 | prepare 描述映射为 MySQL field_type，文本化返回 |

物理表全部由 `init/shard0.sql`、`init/shard1.sql` 创建。PostgreSQL 后端不会执行
Mycat 的 MySQL DDL 自动化（与 Oracle 相同），所以 `createTableSQL` 只作为逻辑元数据参考，
真实 DDL 由 DBA/init 脚本负责。

标识符规则：Mycat 把 MySQL 反引号翻译为 PostgreSQL 双引号并保留大小写。
本例物理 schema/table 都小写，因此用 `"db1_0"."orders_0"` 能精确命中。
如果 DBA 建的是未加引号的大写对象，请保持配置与数据库实际大小写一致。

## 4. XA 2PC（PostgreSQL 分支）

`xa` 用户使用 `transactionType: "xa"`；两个分片都配置了：

```yaml
command: ["postgres", "-c", "max_prepared_transactions=64", "-c", "max_connections=100"]
```

```sql
-- psql "postgresql://xa:123456@mycat3-pro:8066/db1?sslmode=disable"
begin;
insert into orders (id,user_id,amount,status,created) values (201,1,1.00,'XA','2026-02-01 00:00:00');
insert into orders (id,user_id,amount,status,created) values (202,2,2.00,'XA','2026-02-02 00:00:00');
commit;
select id,status from orders where id in (201,202) order by id;
```

Mycat 对每个分支执行 `PREPARE TRANSACTION 'gtrid_分支'`，写决策 WAL
（`conf/xa/xa-wal.log`），再广播 `COMMIT PREPARED`。`pg_prepared_xacts` 在成功后应为空；
回滚会 `ROLLBACK PREPARED`。

注意：

- `max_prepared_transactions=0`（PostgreSQL 默认）时 XA prepare 一定失败；
- GID 是 PostgreSQL 实例级唯一，Mycat 用 `gtrid_数据源名` 作为分支 GID；
- 2PC 有额外 fsync/网络往返；只在跨分片原子性必要时使用；
- Oracle 后端不支持 XA（thin 驱动无接口）。

## 5. 双协议前端

同一个 `8066` 端口同时接受：

- PostgreSQL 客户端：`psql "postgresql://root:123456@mycat3-pro:8066/db1?sslmode=disable"`；
- MySQL 客户端：见上面 `docker run ... mysql:8.0`；
- TDS（SQL Server）客户端也可用（本示例未内置 sqlcmd）。

前端协议只影响握手/结果编码；SQL 路由、分片、归并、XA 走同一执行引擎。
MySQL 客户端的反引号、`LIMIT` 等语法会按 MySQL 方言解析，再翻译到 PostgreSQL。

## 配置目录

```
conf/
├── server.json
├── datasources/ds0.datasource.json      # jdbc:postgresql://.../db1
├── datasources/ds1.datasource.json
├── clusters/c0.cluster.json             # SINGLE_NODE，两个可写分片
├── clusters/prototype.cluster.json
├── users/root.user.json                 # proxy 事务
├── users/xa.user.json                   # XA 2PC
└── schemas/db1.schema.json
init/
├── shard0.sql                           # schema db1_0 + 物理表
└── shard1.sql                           # schema db1_1 + 物理表
```

连接池：每个分片 `maxCon=12`，两个分片共 24 条后端连接；PostgreSQL
`max_connections=100`，为管理/复制/其它应用预留了余量。

## 排障

| 现象 | 处理 |
|---|---|
| `relation "db1_0.orders_0" does not exist` | 检查 init SQL 是否执行、schema/表名大小写是否与配置一致 |
| `prepared transaction ... does not exist` | 确认 `max_prepared_transactions>0` 且已重启 PostgreSQL |
| psql 报认证失败 | 检查 Mycat 用户密码；数据库名必须是 `db1` |
| MySQL 客户端反引号 SQL 报错 | PostgreSQL 不支持的 MySQL 专有语法会原样报错，不会静默改写 |
| 跨分片 `GROUP BY` 报 1235 | 透传模式不支持；走单分片或拆查询 |
| 时间带微秒 | PostgreSQL `CURRENT_TIMESTAMP` 精度高于 MySQL；属正常差异 |

## 清理

```bash
docker compose down -v
rm -rf conf/xa conf/cache conf/backup
```
