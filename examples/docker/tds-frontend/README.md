# SQL Server 客户端接入（TDS 前端）+ MySQL 后端

## 先说明方向：TDS 是前端，SQL Server 也可以做后端

Mycat3 Pro 支持三种客户端协议（MySQL/PG/TDS）和四种后端：

| 角色 | 支持情况 | 配置字段 |
|---|---|---|
| MySQL 后端 | ✅ 原生协议 | `dbType: "mysql"` |
| PostgreSQL 后端 | ✅ tokio-postgres | `dbType: "postgresql"` |
| Oracle 后端 | ✅ rust-oracledb thin | `dbType: "oracle"` |
| **SQL Server 后端** | ✅ tiberius TDS 客户端 | `dbType: "mssql"` / `"sqlserver"` |
| SQL Server 客户端（TDS 协议） | ✅ 前端支持 | 同端口 8066 协议嗅探 |

因此本目录演示的是 **SQL Server 客户端（sqlcmd / TDS）→ Mycat3 Pro → MySQL 后端**，
不是“SQL Server 作为数据源”。SQL Server 作为**后端**见
[`sharding-sqlserver`](../sharding-sqlserver/README.md)（`dbType: "mssql"`，tiberius 驱动）。

TDS 前端与 MySQL/PG 前端共享同一执行引擎，所以分片、广播、归并、管理命令、
XA（后端支持时）完全一致，区别只在协议编解码和少量 T-SQL 改写。

```
sqlcmd / mssql-jdbc / jTDS
        │ TDS 7.2+，8066
        ▼
   Mycat3 Pro（TDS 前端）
        │ 路由 + 归并
        ┌────────┴────────┐
        ▼                 ▼
   MySQL db1_0        MySQL db1_1
   orders_0           orders_1
   dict               dict
   user_info
```

## 快速开始

```bash
cd examples/docker/tds-frontend
docker compose up -d --wait
./demo.sh
```

一次性 TDS 查询：

```bash
docker compose run --rm -T tds-client \
  -S mycat3-pro,8066 -U root -P 123456 -d db1 \
  -W -s '|' -Q "select top 3 id,amount from orders order by amount desc"
```

连接到代理的 JDBC 参数（mssql-jdbc）：

```text
jdbc:sqlserver://127.0.0.1:8066;encrypt=false;databaseName=db1
```

- TDS 前端**不支持 TLS/加密**，客户端必须关闭加密：
  sqlcmd 17/13 默认不加密；sqlcmd 18+ 使用 `-N o`（可选加密，服务端不支持时回退明文），
  不要使用 `-N m`/`-C` 强制 TLS；mssql-jdbc 用 `encrypt=false`；
- 预处理语句走 `sp_executesql` 路径；mssql-jdbc 建议
  `prepareMethod=executesql`（本项目没有实现 RPC cursor 的长尾路径）；
- 不要使用 SQL Server 专有的 `SELECT ... FOR XML/JSON`、`TOP n PERCENT/WITH TIES` 等；
  基础的 `TOP n` 会被改写为 `LIMIT n`，PERCENT/WITH TIES 显式报错。

## 演示内容

### 1. TDS 写入 + 分片

`demo.sh` 通过 sqlcmd 插入 4 条订单，`orders` 按 `id % 2` 分到：

- `orders_0` → `ds0.db1_0`
- `orders_1` → `ds1.db1_1`

```sql
select top 3 id, amount, status from orders order by amount desc;
/*!mycat:showTopology{orders}*/;
```

`TOP 3` 在 Mycat 的 TDS 前端被改写为 `LIMIT 3`，再走正常的分片归并。
`showTopology` 输出两行物理落点。

### 2. 协议无关

同一份数据用 MySQL 协议读取：

```bash
docker compose exec -T -e MYSQL_PWD=123456 mysql \
  mysql -hmycat3-pro -P8066 -uroot -Ddb1 \
  -e "select id,amount,status from orders order by id"
```

结果与 TDS 完全一致——TDS 只是“入口协议”，路由/存储与 MySQL 前端相同。

### 3. 广播表与单表

```sql
insert into dict (id,name) values (1,'CN');       -- 写入扇出到两个 MySQL schema
insert into user_info (id,name,profile) values (1,'alice','vip');  -- 固定 ds0
select * from dict order by id;
select * from user_info;
```

`demo.sh` 会直连 MySQL 分片核对：两个 `dict` 各 1 行，两个 `orders` 各 2 行。

## 配置

```
conf/
├── server.json                 # protocolSniff=true（TDS 与 MySQL/PG 同端口）
├── datasources/ds0|ds1.*.json  # 两个 MySQL database
├── clusters/c0.cluster.json    # 两个可写分片，SINGLE_NODE
├── users/root.user.json        # 前端账号（TDS/MySQL/PG 共用）
└── schemas/db1.schema.json     # orders 取模 2 + dict 广播 + user_info 单表
init/01-shard.sql               # 两个 MySQL schema 的物理表
```

关键字段：

| 字段 | 说明 |
|---|---|
| `server.protocolSniff` | 必须保持 `true` 才能在同一 8066 端口识别 TDS |
| `datasource.dbType` | `mysql`；SQL Server 客户端不改变后端类型 |
| `schemas.*.partition.data` | 物理落点三元组 `[ds, database, table]` |
| `schemas.*.function` | 与 MySQL 前端完全相同的分片函数 |
| `users.*.schemas` | TDS 登录 `databaseName=db1` 必须在这里 |

TDS 登录使用 Mycat 用户（本例 `root/123456`），不是 MySQL 后端账号；
后端账号在 datasource 中配置。

## 与其他前端协议的差异

| 能力 | MySQL 前端 | PostgreSQL 前端 | TDS 前端（本例） |
|---|---|---|---|
| 简单查询/DML/DDL | ✓ | ✓ | ✓ |
| 预处理 | COM_STMT_* | Parse/Bind/Execute | `sp_executesql`（RPC） |
| 事务 | ✓ | ✓ | ✓（SQL batch） |
| 多语句批 | ✓ | ✓ | ✓ |
| 结果类型 | 原生 | MySQL 类型→PG OID | 统一 NVARCHAR 文本 |
| TLS | 支持（可配） | 支持（可配） | **不支持**，必须 encrypt=false |
| 方言改写 | – | – | `TOP n`→`LIMIT n` |

TDS 结果统一为文本/NVARCHAR，NULL 保真；日期/数值的显示格式按 MySQL 后端字符串化，
与 MySQL 客户端可能略有差异（例如时间精度），这是协议层差异，不是数据错误。

## SQL Server 后端

SQL Server 也可以作为 Mycat 的**后端**（不是本目录的 TDS 前端方向）：

```json
{
  "name": "ds0",
  "url": "jdbc:sqlserver://sqlserver:1433;databaseName=db1;encrypt=true;trustServerCertificate=true",
  "user": "sa",
  "password": "...",
  "dbType": "mssql"
}
```

- 基于 `tiberius` TDS 客户端 + 自研连接池；
- 支持 SQL 认证、TLS、schema 分片、常用 T-SQL 方言、本地事务；
- 后端物理表可由逻辑 `CREATE TABLE` 自动创建；XA/MSDTC 未实现，`transactionType=xa` 显式拒绝；
- 完整示例：[`sharding-sqlserver`](../sharding-sqlserver/README.md)；
- 代价与限制：[SQL Server 后端接入评估](../../../docs/sqlserver-backend-feasibility.md)。

## 排障

| 现象 | 处理 |
|---|---|
| 登录失败/要求加密 | sqlcmd 17/13 默认不加密；sqlcmd 18+ 用 `-N o`，JDBC `encrypt=false`；不要强制 TLS |
| `sqlcmd: not found` | `mssql-tools` 镜像里路径是 `/opt/mssql-tools/bin/sqlcmd`，不是 PATH |
| TDS 连接超时 | 确认 `protocolSniff=true`，且客户端使用 TDS（不是 MySQL 驱动）连 8066 |
| `TOP n PERCENT` 报错 | 该形式未实现；改成 `TOP n` 或 `ORDER BY ... OFFSET/FETCH` |
| 中文/宽字符显示 | TDS 统一 NVARCHAR；用 `-W -s '|'` 去掉 sqlcmd 的填充空格 |
| 查不到数据 | 用 MySQL 前端同查一次；再用 `showTopology` 确认分片键 |

## 清理

```bash
docker compose down -v
rm -rf conf/xa conf/cache conf/backup
```
