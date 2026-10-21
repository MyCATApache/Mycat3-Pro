# 分库分表 + Oracle 后端：双 schema、物理 DDL 与方言差异

这个例子用**一个 Oracle Free 实例的两个 schema**（`DB1_0`/`DB1_1`）模拟两个分片，
由 Mycat3 Pro 通过官方 `rust-oracledb` thin 驱动访问。之所以不启动两个 Oracle 容器，
是因为 Oracle Free 资源和镜像体积较大；两个 schema 已足以验证分片路由、广播和归并。
生产环境可以把两个 datasource 指向不同实例/PDB，配置方式不变。

```
mysql / psql / sqlcmd
        │ 8066
        ▼
   Mycat3 Pro :48066
        ┌────────┴────────┐
        ▼                 ▼
   Oracle FREEPDB1
   schema DB1_0       schema DB1_1
     ORDERS_0/1         ORDERS_2/3
     TENANT_ORDER_0     TENANT_ORDER_1
     "dict"             "dict"
     USER_INFO
```

> Oracle Free 首次启动通常需要 2–5 分钟，建议 4GB+ 内存、10GB+ 磁盘。
> 镜像来自 `container-registry.oracle.com`，首次使用可能需要
> `docker login container-registry.oracle.com` 并接受许可。

## 快速开始

```bash
cd examples/docker/sharding-oracle
docker compose up -d
# 等待 Oracle healthy + Mycat /-/healthy（首次建库较慢）
./demo.sh
```

MySQL 协议连接：

```bash
docker compose run --rm -T mysql-client \
  --default-character-set=utf8mb4 -hmycat3-pro -P8066 -uroot -Ddb1
```

PostgreSQL/TDS 客户端也可以连同一个 8066 端口；本例用 MySQL 客户端演示。

## 1. 取模分片 orders

```json
"orders": {
  "partition": {
    "data": [
      ["ds0", "DB1_0", "ORDERS_0"],
      ["ds0", "DB1_0", "ORDERS_1"],
      ["ds1", "DB1_1", "ORDERS_2"],
      ["ds1", "DB1_1", "ORDERS_3"]
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

`id=3` 只发往 `ds1.DB1_1.ORDERS_3`；`count`/`sum`/`order by + limit`
在 Mycat 归并。`LIMIT` 会被翻译为 Oracle 的 `ROWNUM` 包装。

## 2. 枚举分片、广播表、单表

```sql
insert into tenant_order (id,tenant_id,note,created) values
 (101,'acme','tenant-a','2026-01-01 00:00:00'),
 (102,'globex','tenant-b','2026-01-02 00:00:00');
insert into dict (id,name) values (1,'CN'),(2,'US');
insert into user_info (id,name,profile) values (1,'alice','vip');

select * from tenant_order where tenant_id='acme';
select * from dict order by id;
select * from user_info;
```

- `tenant_order` 用 `PartitionByFileMap`，`acme`→`DB1_0`，`globex`→`DB1_1`；
- `dict` 广播到两个 schema；
- `user_info` 固定 `DB1_0.USER_INFO`。

物理核对：

```bash
docker compose exec oracle sqlplus -s "DB1_0/demo_pw@//localhost:1521/FREEPDB1" <<'SQL'
set pagesize 50 linesize 120 feedback off
select count(*) from "dict";
select count(*) from ORDERS_0;
SQL
```

## 3. Oracle 关键字/大小写（最容易踩的坑）

Mycat 会把 MySQL 反引号/标识符翻译成 Oracle 双引号并**保留大小写**，因此 Oracle 物理对象必须
与配置/逻辑 SQL 的大小写精确一致：

| 对象 | 本例命名 | 原因 |
|---|---|---|
| schema/user | `DB1_0` / `DB1_1`（未加引号→大写） | 配置 `schemaName` 写大写，登录用户也是大写 |
| 分片物理表 | `ORDERS_0` 等（未加引号→大写） | `partition.data` 中写大写 |
| 表列 | `"id"`, `"created"` 等（**加引号小写**） | 逻辑 SQL 的列名是小写，翻译后是 `"created"` |
| 广播表 | `"dict"`（加引号小写） | 广播表物理表名使用逻辑表名 `dict`，不是大写 DICT |

初始化脚本 `init/01-schema.sql` 按此规则预建了演示表；逻辑 `CREATE TABLE` 也会生成
同样大小写规则的物理对象（未指定 `schemaName` 时使用数据源连接用户）。若出现：

- `ORA-00904: "created": invalid identifier` → 列名大小写不匹配；
- `ORA-00942: table or view "DB1_0"."dict" does not exist` → 广播表名应为加引号小写 `"dict"`。

## 4. Oracle 后端配置差异

| 项 | 说明 |
|---|---|
| `dbType` | `oracle` |
| URL | `jdbc:oracle:thin:@//host:1521/FREEPDB1` |
| 驱动 | rust-oracledb thin，纯 Rust，无需 Oracle Client |
| 物理建表 | 逻辑 `CREATE TABLE` 自动生成 Oracle DDL（identity/索引/幂等 PL/SQL）；init SQL 可选 |
| 标识符 | 双引号、大小写敏感；见上一节 |
| LIMIT | 翻译为 `ROWNUM` 包装 |
| 多行 INSERT | 翻译为 `INSERT ALL` |
| 函数 | `IFNULL`→`NVL`、`NOW`→`CURRENT_TIMESTAMP`、`DATE_FORMAT`→`TO_CHAR` 等 |
| 不支持 | `INSERT ... ON DUPLICATE KEY UPDATE`、`DROP ... IF EXISTS`、`DELETE ... LIMIT` 等显式报错 |
| 心跳 | `select 1 from dual` |
| XA | **不支持**（thin 驱动无 XA 接口）；跨 Oracle/MySQL 事务不保证原子性 |
| 日期格式 | 本例 datasource `initSqls` 设置 `NLS_DATE_FORMAT='YYYY-MM-DD HH24:MI:SS'` |
| 数值显示 | `NUMBER(12,2)` 可能返回 `40` 而不是 `40.00`（字符串化差异，不是精度丢失） |

Oracle 的 `DATE` 同时含日期和时间；自动 DDL 将 MySQL `DATETIME` 映射为 `TIMESTAMP`；Mycat 连接初始化 NLS 后，
MySQL 风格的 `'2026-01-01 10:00:00'` 字面量可直接插入。

## 5. 为什么用两个 schema 而不是两个 Oracle 容器

- Oracle Free 镜像约 14GB，启动需要数 GB 内存；两个容器对开发机不友好；
- 分片路由只看 `datasource + schema`，两个 schema 已能完整验证路由/归并/广播；
- 生产可以把 `ds0`/`ds1` 指向不同 Oracle 实例或不同 PDB，只需改 URL/用户；
- 单个 Oracle 实例内的两个 schema 仍然是同一数据库进程，**不提供故障隔离**；
  真正的高可用需要 RAC/Data Guard 或多实例，按业务另行设计。

## 配置目录

```
conf/
├── server.json
├── datasources/ds0|ds1.*.json    # 同一 FREEPDB1，不同用户
├── clusters/c0.cluster.json      # 两个 schema 都是可写单节点
├── clusters/prototype.cluster.json
├── users/root.user.json          # proxy 事务（Oracle 不支持 XA）
└── schemas/db1.schema.json
init/01-schema.sql                # 每次启动以 sysdba 执行（幂等）
```

`init/01-schema.sql` 挂载到 `/opt/oracle/scripts/startup`，**每次 Oracle 启动都会执行**；
脚本按幂等编写（用户/表存在时跳过），修改后 `docker compose restart oracle` 即可生效。
如果要彻底重建数据库，使用 `docker compose down -v`。

## 排障

| 现象 | 处理 |
|---|---|
| `ORA-01017` 登录失败 | 检查 datasource 用户/密码；用户是 `DB1_0`/`DB1_1` |
| `ORA-00904` 无效标识符 | 列名大小写；按初始化脚本加引号小写 |
| `ORA-00942` 表不存在 | 分片表用大写；广播表用 `"dict"` 小写；检查是否首次初始化完成 |
| 启动很慢 | Oracle Free 建库需要时间；看 `docker compose logs -f oracle` |
| `ORA-01555`/空间不足 | 增加磁盘；Oracle 数据文件较大 |
| XA 相关报错 | Oracle 后端不支持 XA；改用普通事务或应用补偿 |
| 数值没有小数位 | Oracle NUMBER 字符串化差异；需要固定格式时用 `TO_CHAR` |

## 清理

```bash
docker compose down -v
rm -rf conf/xa conf/cache conf/backup
```
