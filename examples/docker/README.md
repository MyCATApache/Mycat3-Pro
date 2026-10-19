# Mycat3 Pro Docker 示例库

所有示例都用 **Docker Compose** 编排：一个命令启动后端 + Mycat3 Pro，配置在各自
`conf/` 目录，初始化 SQL 在 `init/`，详细说明在各自 `README.md`。

镜像：`mycat/mycat3-pro:latest`（[Docker Hub](https://hub.docker.com/r/mycat/mycat3-pro)）。
如果当前网络拉不到镜像，在仓库根目录执行：

```bash
docker build -t mycat/mycat3-pro:latest .
```

## 示例矩阵

| 示例 | 运行模式 | 后端 | 前端协议 | 重点 |
|---|---|---|---|---|
| [`proxy-mysql`](proxy-mysql/README.md) | `proxy` | MySQL | MySQL / PG / TDS | SQL 结果缓存、SQL 拦截、慢 SQL、会话固定连接、HTTP API |
| [`sharding-mysql`](sharding-mysql/README.md) | `local` | MySQL × 2 | MySQL / PG / TDS | 取模/枚举/月份/广播/单表、XA 2PC |
| [`sharding-postgresql`](sharding-postgresql/README.md) | `local` | PostgreSQL × 2 | PostgreSQL / MySQL / TDS | PG schema 分片、大小写、手工 DDL、PG XA |
| [`sharding-oracle`](sharding-oracle/README.md) | `local` | Oracle Free（双 schema） | MySQL / PG / TDS | 大写标识符/引号列、NLS 日期、无 XA、手工物理表 |
| [`tds-frontend`](tds-frontend/README.md) | `local` | MySQL | **SQL Server (TDS)** / MySQL | sqlcmd/T-SQL 接入、`TOP n` 改写、协议无关性 |
| [`performance`](performance/README.md) | `local` | MySQL（双 schema） | MySQL | 线程/超时/连接池/缓存参数与轻量对比 |
| [`proxy.compose.yml`](proxy.compose.yml) | `proxy` | MySQL | MySQL | 最小 5 分钟代理示例（早期版本） |

### SQL Server 后端？没有这个示例

v0.3.0 的后端只有 **MySQL / PostgreSQL / Oracle**。SQL Server 是**前端 TDS 协议**，
不是后端 `dbType`；[`tds-frontend`](tds-frontend/README.md) 演示的是
“SQL Server 客户端 → Mycat → MySQL”，不是“SQL Server 作为数据源”。
SQL Server 后端需要新增 TDS 客户端驱动和 T-SQL 方言层，当前版本明确不支持；
代价与实施方案见 [docs/sqlserver-backend-feasibility.md](../../docs/sqlserver-backend-feasibility.md)。

## 通用约定

- 测试密码全部是明文 demo 值，生产必须换用密钥管理；
- Mycat 镜像以 UID 1000 运行；配置目录挂载为可写（缓存/XA 需要写入）；
- `docker compose down -v` 会删除演示数据库数据；
- 运行时生成的 `conf/cache/`、`conf/xa/`、`conf/backup/` 已在 `.gitignore` 中忽略；
- 端口按示例区分，可同时运行多个（Oracle 例外，资源占用较大）。

| 示例 | Mycat 端口 | 监控端口 | 后端端口 |
|---|---|---|---|
| proxy-mysql | 18066 | 19066 | MySQL 13316 |
| sharding-mysql | 28066 | 29066 | MySQL 13317/13318 |
| sharding-postgresql | 38066 | 39066 | PG 15434/15435 |
| sharding-oracle | 48066 | 49066 | Oracle 11521 |
| tds-frontend | 58066 | 59066 | MySQL 13319 |
| performance | 28067 | 29067 | MySQL 13320 |

## 运行方式

每个示例目录都有 `README.md`，一般流程：

```bash
cd examples/docker/<示例>
docker compose up -d --wait
./demo.sh            # 除 tds-frontend 外均有 demo.sh；tds 的 demo 在目录中
docker compose down -v
```

- `proxy-mysql`：`./demo.sh` 演示缓存/拦截/慢 SQL/会话/HTTP；
- `sharding-mysql`：`./demo.sh` 演示五种落点与 XA；
- `sharding-postgresql`：`./demo.sh` 用 psql 走 PG 前端；
- `sharding-oracle`：`./demo.sh` 首次启动较慢；
- `tds-frontend`：`./demo.sh` 通过 sqlcmd 写入/查询；
- `performance`：`./bench.sh` 做轻量前后对比（不是严谨压测）。

## 文档

- [Docker 使用指南](../../docs/docker.md)：镜像、卷、TLS、发布流程；
- [性能参数与容量规划](../../docs/performance.md)：参数影响、场景区间、池预算；
- [配置参考](../../docs/configuration.md)：字段级说明；
- [命令参考](../../docs/commands.md)：SQL/hint/管理命令；
- [兼容性](../../docs/compatibility.md)：与 Java 版的差异。
