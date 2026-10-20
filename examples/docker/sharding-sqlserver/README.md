# 分库分表 + SQL Server 后端：TDS 客户端驱动 + T-SQL 方言

这个例子演示 **SQL Server 作为后端**（不是 TDS 前端）：Mycat3 Pro 通过
`tiberius` TDS 客户端连接 SQL Server 2022，把 SQL Server schema 当作物理分片。

> 相关：SQL Server 作为**前端协议**（sqlcmd → Mycat → MySQL）见
> [`tds-frontend`](../tds-frontend/README.md)。两者是不同方向，可以同时使用。

```
mysql / psql / sqlcmd
        │ 8066（同端口协议嗅探）
        ▼
   Mycat3 Pro（dbType=mssql）
        ┌────────┴────────┐
        ▼                 ▼
   SQL Server db1
   schema db1_0       schema db1_1
     orders_0/1         orders_2/3
     tenant_order_0     tenant_order_1
     dict               dict
     user_info
```

## 快速开始

```bash
cd examples/docker/sharding-sqlserver
docker compose up -d --build --wait
./demo.sh
```

> SQL Server 后端代码随源码提供。若 Docker Hub 上的 `mycat/mycat3-pro:latest`
> 仍是旧版本（不支持 `dbType=mssql`），先在仓库根目录
> `docker build -t mycat/mycat3-pro:latest .` 再启动。

- SQL Server 2022 Developer 镜像约 1.5GB（压缩层约 627MB），首次启动需要 1–3 分钟；
- 密码 `SqlServer_Demo1!` 仅用于演示，必须满足 SQL Server 复杂度策略；
- 默认端口：Mycat `38068/39068`，SQL Server `13321`；
- 清理：`docker compose down -v`。

MySQL 协议连接：

```bash
docker compose run --rm -T mysql-client \
  --default-character-set=utf8mb4 -hmycat3-pro -P8066 -uroot -Ddb1
```

SQL Server 直连（用于物理核对）：

```bash
docker compose run --rm -T sqlcmd-client \
  -S sqlserver -U sa -P 'SqlServer_Demo1!' -C -d db1 \
  -Q "select count(*) from db1_0.orders_0"
```

## 配置与物理模型

```yaml
# datasources/ds0.datasource.json
{
  "url": "jdbc:sqlserver://sqlserver:1433;databaseName=db1;encrypt=true;trustServerCertificate=true",
  "user": "sa",
  "password": "SqlServer_Demo1!",
  "dbType": "mssql",
  "maxCon": 8,
  "minCon": 1
}
```

- **一个 datasource 连接一个 database**（本例 `db1`）；
- `schemaName` 是 SQL Server **schema**（`db1_0`/`db1_1`），不是 database；
- 物理 SQL 由方言层生成 `[db1_0].[orders_0]`；
- 物理表可由 `init/01-db.sql` 预建，也可由 Mycat 的逻辑 `CREATE TABLE`
  自动翻译为 T-SQL（自动建 schema/表/索引，幂等），`demo.sh` 第 5 节会验证；
- 需要 SQL 认证；支持 TLS 配置：`encrypt=true|false`、`trustServerCertificate=true`；
- 命名实例（`host\instance`）一期未支持，请使用显式端口；
- 连接池：每个分片 `maxCon=8`，`minCon=1`，空闲 60s 回收。

## 分片策略

| 表 | 策略 | 说明 |
|---|---|---|
| `orders` | `PartitionByMod(id, 4)` | 4 张物理表分散到两个 schema |
| `tenant_order` | `PartitionByFileMap(tenant_id)` | `acme`→db1_0，`globex`→db1_1 |
| `dict` | 广播表 | 两个 schema 各一份，读第一个、写扇出 |
| `user_info` | 单表 | 固定 `db1_0.user_info` |

```sql
insert into orders (id,user_id,amount,status,created) values
 (1,101,10.00,'NEW','2026-01-01 10:00:00'),
 (2,102,20.00,'PAID','2026-01-02 10:00:00'),
 (3,103,30.00,'NEW','2026-01-03 10:00:00'),
 (4,104,40.00,'PAID','2026-01-04 10:00:00');

/*!mycat:showTopology{orders}*/;
explain select * from orders where id = 3;
select count(*) from orders;
select sum(amount) from orders;
select id,amount from orders order by amount desc limit 3;
```

`id=3` 只访问 `db1_1.orders_3`；`count`/`sum`/`order by + limit` 由 Mycat 归并。
（与其它后端一样，`count` 与 `sum` 请分开查询，透传模式不支持混合聚合。）

## T-SQL 方言支持范围

已实现：

- 反引号/双引号标识符 → `[bracket]`；
- `LIMIT n`（无 ORDER BY）→ `TOP (n)`；
- `LIMIT n OFFSET m` / `LIMIT m,n`（有 ORDER BY）→
  `OFFSET m ROWS FETCH NEXT n ROWS ONLY`；
- 布尔字面量 `TRUE/FALSE` → `1/0`；
- `IFNULL`→`ISNULL`、`IF`→`IIF`、`NOW`→`SYSDATETIME`、`UUID`→`NEWID`、
  `LCASE/UCASE`、`CHAR_LENGTH/LENGTH`→`LEN`、`LOCATE`→`CHARINDEX`、
  `SUBSTR`→`SUBSTRING`、`GROUP_CONCAT`（简单形式）→`STRING_AGG`；
- 基础 `INSERT/UPDATE/DELETE/SELECT`、多行 INSERT、广播/分片重写。

明确报错（不静默改写）：

- `INSERT ... ON DUPLICATE KEY UPDATE`、`REPLACE INTO`、`INSERT IGNORE`；
- `DELETE/UPDATE ... LIMIT`；
- `SELECT ... FOR UPDATE` / `LOCK IN SHARE MODE`；
- `REGEXP/RLIKE`、`DATE_FORMAT`、`UNIX_TIMESTAMP`（SQL Server 语义/格式不同）；
- `LIMIT` 无 ORDER BY 且无法安全使用 TOP 的 UNION 查询；
- `OFFSET` 没有 ORDER BY（SQL Server 要求）；请显式排序。
- `GROUP_CONCAT ... SEPARATOR/ORDER BY` 的复杂形式。

## 类型映射

SQL Server → MySQL field_type（文本行模型）：

| SQL Server | MySQL 类型 | 备注 |
|---|---|---|
| `BIT`/`TINYINT` | TINY | |
| `SMALLINT`/`INT`/`BIGINT` | SHORT/LONG/LONGLONG | |
| `REAL`/`FLOAT` | FLOAT/DOUBLE | |
| `DECIMAL/NUMERIC/MONEY` | NEWDECIMAL | 字符串化保留精度 |
| `CHAR/VARCHAR/NCHAR/NVARCHAR/TEXT/XML` | VAR_STRING | UTF-8 文本 |
| `BINARY/VARBINARY/IMAGE` | BLOB | `\x` 十六进制 |
| `DATE/TIME` | DATE/TIME | |
| `DATETIME/DATETIME2/SMALLDATETIME` | DATETIME | 纳秒按实际精度输出 |
| `DATETIMEOFFSET` | VAR_STRING | 保留偏移，避免静默丢时区 |
| `UNIQUEIDENTIFIER` | VAR_STRING | GUID 文本 |
| `GEOGRAPHY/GEOMETRY/HIERARCHYID` | 未完整支持 | 建议显式拒绝/应用层处理 |

NULL 保真。SQL Server `TINYINT` 是 0–255（MySQL TINY 有符号），查询文本值不受影响，
但如果客户端按 TINYINT 解码大值需注意；核心分片键/金额字段不受影响。

## 事务与 XA

- `SET autocommit=0` → `BEGIN TRAN`；`SET autocommit=1` → `COMMIT`；
- `BEGIN/START TRANSACTION` → `BEGIN TRAN`，`COMMIT/ROLLBACK` 直传；
- `SAVEPOINT x` → `SAVE TRANSACTION [x]`，
  `ROLLBACK TO SAVEPOINT x` → `ROLLBACK TRANSACTION [x]`；
- `RELEASE SAVEPOINT` 不支持；
- 一期只支持**单数据源本地事务**；跨分片写不承诺原子性；
- **SQL Server 没有 XA**；`transactionType=xa` 的 SQL Server 分支会明确报错，
  MSDTC 不在一期范围。需要跨实例原子性时使用业务补偿或独立评估 MSDTC 网关。

## 与其它后端的关键差异

| 项 | MySQL | PostgreSQL | Oracle | SQL Server |
|---|---|---|---|---|
| 物理分片位置 | database | schema | schema | schema |
| 标识符 | 反引号 | 双引号 | 双引号/大写 | `[bracket]` |
| 分页 | LIMIT | LIMIT | ROWNUM | TOP / OFFSET FETCH |
| 自增 | AUTO_INCREMENT | sequence | identity | IDENTITY |
| XA | 原生 | 2PC prepared | 不支持 | **不支持（MSDTC 未实现）** |
| DDL 自动化 | 支持 | 支持 | 手工 | 支持（T-SQL，标识符/类型/索引） |
| 认证 | 密码 | 密码 | 密码 | SQL 登录；Windows/Kerberos 未做 |

## 排障

| 现象 | 处理 |
|---|---|
| SQL Server 镜像拉取失败/超时 | MCR 在部分网络不稳定，换网络/代理，或从可访问的机器 `docker save/load` |
| `Login failed for user 'sa'` | `MSSQL_SA_PASSWORD` 必须满足复杂度；确认容器环境一致 |
| 证书/TLS 错误 | 自签证书用 `trustServerCertificate=true`；生产用受信证书并设 `encrypt=true` |
| `OFFSET` 语法错误 | 检查 SQL 是否带 `ORDER BY`；方言层对无排序 offset 显式报错 |
| `last_insert_id` 为 0 | 非自增表返回 0；自增表在同一条 INSERT 批次内读取 `SCOPE_IDENTITY()`（无额外往返） |
| `XA 事务不支持 SQL Server` | 预期行为；SQL Server 分布式事务需 MSDTC，未实现 |
| 物理表不存在 | 确认 `sqlserver-init` 成功执行；SQL Server 表/schema 手工创建 |
| 命名实例连接失败 | 一期只支持 `host:port`；启用 TCP 或改用端口 |

## 清理

```bash
docker compose down -v
rm -rf conf/xa conf/cache conf/backup
```
