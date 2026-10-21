# Mycat3 Pro

**分布式数据库中间件 —— Mycat3 的 Rust 重新实现**

Mycat3 Pro 是对 [Mycat3](https://github.com/mycatparser/Mycat3)（Java 版分布式数据库中间件）的 Rust 语言重新实现。它保留了 Mycat3 的核心架构与配置格式（JSON 多文件配置可直接复用），以 tokio 异步运行时替代 Vert.x，以 sqlparser 替代 Druid/Calcite，提供 MySQL 协议代理、数据分片、读写分离、心跳高可用与可观测能力。

- **379 个单元/集成测试全部通过**（协议编解码、17 种分片算法、SQL 路由、连接池、负载均衡、心跳切换、DDL 自动化、端到端全链路）
- **DDL 配置自动化**：一条 `CREATE TABLE ... DBPARTITION BY hash(id) TBPARTITIONS 2 ...` 自动完成
  分片配置生成、物理建库建表、元数据热更新（与 Java 版 MycatRouterConfigOps 同语义）
- **多协议同端口**（与 Java 版一致的字节级嗅探）：8066 端口同时服务 MySQL、PostgreSQL、SQL Server(TDS) 客户端；**真实客户端验证**：mysql CLI、psql 16（含扩展查询协议参数绑定）、sqlcmd/ODBC 同端口读写同一分片集群
- **真实双 MySQL 8.0 实例冒烟验证**：空实例上 DDL 自动化建库建分片表、Snowflake 自增填充与路由、跨分片 ORDER BY+LIMIT 归并、COUNT 聚合归并、广播表双实例同步（insert/update/delete/drop）、事务+SAVEPOINT、热重载、AutoFunction、错误密码拒绝、并发查询；mysql/psql/sqlcmd 三协议客户端均可驱动全部特性
- 单一静态二进制，无 JVM 依赖，内存占用为 Java 版的零头

## 下载

预编译二进制与源码见 [Releases](https://github.com/MyCATApache/Mycat3-Pro/releases)：

| 平台 | 资产 |
|---|---|
| Linux x86_64 | `mycat3-pro-v0.3.0-x86_64-unknown-linux-gnu.tar.gz` |
| Linux arm64 | `mycat3-pro-v0.3.0-aarch64-unknown-linux-gnu.tar.gz` |
| Windows x64 | `mycat3-pro-v0.3.0-x86_64-pc-windows-msvc.zip` |
| Windows arm64 | `mycat3-pro-v0.3.0-aarch64-pc-windows-msvc.zip` |
| 源码 | `mycat3-pro-v0.3.0-source.tar.gz`（另附 GitHub 自动源码包） |

本地交叉编译：`scripts/build-release.sh [VERSION] [TARGET...]`；
CI（tag 触发）见 `.github/workflows/release.yml`。

---

## 功能总览

| 能力 | 说明 | 对应 Java 版模块 |
|------|------|------------------|
| MySQL 协议前端 | 握手、mysql_native_password / caching_sha2 快速认证、AuthSwitch、COM_QUERY/PING/INIT_DB/QUIT/FIELD_LIST/STMT_* | `proxy` / `mycat3.vertx` |
| 多协议同端口嗅探 | 字节级检测 MySQL/PG/TDS/TNS（300ms server-speaks-first、2s/2KB 上限），可配置关闭 | `ProtocolDetector` |
| PostgreSQL 前端 | v3 协议：MD5 认证、简单查询、**真实扩展查询协议**（Parse/Bind/Describe/Execute，Java 版为 stub）、类型 OID 映射、事务状态字节 | `protocol.pg`（Pro 大幅增强） |
| SQL Server (TDS) 前端 | TDS 7.2–7.4：PreLogin/Login7、SQL Batch、**sp_executesql RPC**（Java 版解析损坏）、NULL 保真、ODBC/JDBC 实测兼容 | `protocol.tds`（Pro 修复增强） |
| 原生 MySQL 后端 | 异步原生协议客户端（非 JDBC），支持 DEPRECATE_EOF、分包重组 | `datasource` / vert.x 原生客户端 |
| Oracle 后端 | 官方 rust-oracledb thin 驱动（协议成本归零）+ MySQL→Oracle 方言翻译（标识符/LIMIT→ROWNUM/多行 INSERT→SELECT UNION ALL/函数/日期格式），支持 MySQL+Oracle 混合分片 | JDBC+jOOQ parsingDataSource |
| PostgreSQL 后端 | tokio-postgres + 自研池；schema 分片；PG XA 2PC；**逻辑 DDL 自动建 schema/表/索引** | JDBC+jOOQ |
| SQL Server 后端 | tiberius TDS 客户端 + 自研池；T-SQL 方言（TOP/OFFSET FETCH/函数）；SQL 认证/TLS；本地事务；**逻辑 DDL 自动建 schema/表/索引**；Always On 只读意图/延迟剔除；**单分片大结果端到端流式**；无 XA/MSDTC | JDBC+jOOQ |
| 数据分片 | 17 种算法：Mod / Long / HashMod / MurmurHash 一致性环 / RangeMod / Pattern / PrefixPattern / FileMap / SubString / String / Date / Month / CRC32 预分槽(102400) / JumpConsistentHash / HotDate / AutoPartitionByLong / AutoFunction(hash/right_shift/yyyymm/mm/dd/...) | `router` |
| 分片键裁剪 | `=`、`IN`、`BETWEEN`、`>`/`<` 范围；OR 自动退化为全表扫描 | `ValueIndexCondition` |
| SQL 重写 | 逻辑表名 → 物理表名（AST 级重写，覆盖 FROM/JOIN/INSERT/UPDATE/DELETE/DDL） | `MysqlTableReplacer` |
| 结果归并 | 多分片行拼接、全局 ORDER BY + OFFSET/LIMIT 归并排序、COUNT/SUM/MAX/MIN 跨分片聚合（无排序时零解码直拼原始载荷、排序键按列类型一次计算） | `MysqlPayloadObjectObserver` / Calcite（简化） |
| DDL 自动化 | CREATE/DROP DATABASE/TABLE/USER/SEQUENCE、**CREATE/DROP INDEX 分片扇出、RENAME TABLE 物理执行+配置热更新、ALTER 后回读 SHOW CREATE TABLE 刷新元数据**；**MySQL/PostgreSQL/Oracle/SQL Server 均自动生成物理表 DDL**（PG/T-SQL/Oracle 方言） | 各 DDL SQLHandler |
| 广播表 | 读单节点、写全节点扇出 | `GlobalTable` |
| 读写分离 | BALANCE_ALL / NONE / ALL_READ / READ_WRITE + 随机/权重轮询/最少活跃策略 | `replica` / `plug.loadBalance` |
| 高可用 | 心跳（select 1 / show slave status / Galera wsrep / MGR），连续错误阈值，主从切换与节流 | `replica.heartbeat` |
| 事务 | 本地代理事务（BEGIN/COMMIT/ROLLBACK，按数据源固定连接，autocommit=0 隐式事务）；可选 MySQL/PostgreSQL XA 2PC | `LocalTransactionSession` / XA |
| 预处理语句 | PREPARE/EXECUTE/CLOSE/SEND_LONG_DATA（单节点路由，参数文本替换 + 二进制协议回写） | `COM_STMT_*` handlers |
| Hint | `/*!mycat:target=xx*/`、`master`/`slave`、`schema=`、`sql=` 透传 | `getHintRoute` |
| 管理命令 | `show @@cluster` / `@@datasource` / `@@connection` / `@@heartbeat` / `@@version`、`explain`、`show databases/tables`、`show [full] processlist`、`kill <id>`、`show users` | Pro 增强（Mycat2 风格） |
| DDL 配置自动化 | `CREATE/DROP DATABASE`、`CREATE TABLE ... DBPARTITION BY hash(id) TBPARTITION BY ...`（DRDS 语法→AutoFunction 配置+物理建库建表）、`BROADCAST`、`/*!+mycat:createTable{...}*/` hint、`CREATE/DROP USER`、`CREATE/DROP SEQUENCE` | `CreateTableSQLHandler` / `MycatRouterConfigOps` / `CreateTableUtils` |
| 序列 | Snowflake（workerId=mycatId）+ 计数序列；分片/广播表 INSERT 省略自增列自动填充（先填充后路由） | `plug.sequence` / `explainGlobalInsertSequence01` |
| information_schema | SCHEMATA / TABLES / COLUMNS 本地生成（谓词过滤 + 投影裁剪，JDBC/BI 兼容）；SHOW CREATE TABLE / SHOW FULL TABLES / SHOW COLUMNS / SHOW VARIABLES 本地应答 | `prototypeserver.InformationSchema` / `PrototypeService` |
| 配置热重载 | SQL hint `/*!mycat:loadConfigFromFile*/`（Java 兼容）+ HTTP `POST /api/reload`（Pro 扩展；数据源变更驱逐旧连接池） | `ConfigUpdater.loadConfigFromFile` |
| 会话增强 | SAVEPOINT / RELEASE / ROLLBACK TO 转发、会话粘性路由（读己所写）、SET 事务隔离级别同步后端 | `SavepointSQLHandler` / `DataSourceNearnessImpl` |
| 监控 | Prometheus `/metrics`、`/api/status` JSON、`/-/healthy`（默认 9066 端口） | `PrometheusExporter` |
| 配置 | 与 Mycat3 完全兼容的 JSON 多文件配置（server/users/datasources/clusters/schemas/sequences） | `config` / `FileKV` |

## 架构

```
┌───────────────────────────────────────────────────────────┐
 mysql  ────────► │ sniff     协议嗅探 (MySQL/PG/TDS/TNS)      │
 psql   ────────► │ frontend  MySQL 握手/认证/命令循环           │
 sqlcmd ────────► │ pg / tds  PostgreSQL v3 / TDS 7.x 前端      │
    :8066         │ engine    协议无关执行层（三协议共享）        │
                  │ commands  SET/USE/SHOW/事务/EXPLAIN 本地应答 │
                  │ sql       解析→路由→物理 SQL 重写            │
                  │ router    17 种分片算法 (与 Java 逐位一致)    │
                  │ executor  并行执行/结果归并/透传              │
                  │ replica   负载均衡/心跳/主从切换              │
                  │ client    原生异步 MySQL 后端连接池           │
                  └──────────────┬─────────────┬───────────────┘
                                 ▼             ▼
                        MySQL ds0 (db1_0)   MySQL ds1 (db1_1)
                        orders_0 orders_1   orders_2 orders_3

 monitor :9066  ──►  /metrics  /api/status  /-/healthy
```

模块依赖（Cargo workspace，7 个 crate）：

```
server ──► sql ──► router ──► config
  │         └─────► client ──► protocol
  └─► replica ──► client
```

详细设计见 [docs/architecture.md](docs/architecture.md)。

## 快速开始

### 环境要求

- Rust stable（Dockerfile 使用 `rust:1-bookworm`）
- 一个或多个后端：MySQL 5.7/8.0、PostgreSQL、Oracle、SQL Server 2019/2022（按 `dbType` 配置）
- MySQL 后端使用 `mysql_native_password` 或 caching_sha2 快速认证路径

### 构建

```bash
cd "Mycat 3 Pro"
cargo build --release          # 产物: target/release/mycat3-pro
cargo test --workspace         # 379 个测试
```

### 启动（本机 MySQL 演示）

```bash
# 1. 初始化默认配置（首次启动也会自动生成）
./target/release/mycat3-pro init --home ./conf

# 2. 按你的 MySQL 修改 conf/datasources/*.datasource.json 的 url/user/password

# 3. 校验配置
./target/release/mycat3-pro check --home ./conf

# 4. 启动
./target/release/mycat3-pro start --home ./conf
#    或 scripts/mycat3-pro.sh start

# 5. 连接（同一端口，三种客户端任选）
mysql  -h127.0.0.1 -P8066 -uroot -p123456 -Ddb1
psql   "postgresql://root:123456@127.0.0.1:8066/db1?sslmode=disable"
sqlcmd -S 127.0.0.1,8066 -U root -P 123456   # 需 encrypt=false（TDS 无 TLS）
```

### Docker

官方镜像：`mycat/mycat3-pro:latest`（Docker Hub；`v0.3.3` / `0.3.3` 版本标签同 digest）。

```bash
# 直接使用发布镜像（分片模式）
docker run -d --name mycat3-pro -p 8066:8066 -p 9066:9066 \
  -v "$PWD/conf:/opt/mycat3-pro/conf:ro" mycat/mycat3-pro:latest

# 源码构建（可选）
docker build -t mycat/mycat3-pro:latest .

# 完整演示环境（MySQL 主从 + Mycat3 Pro，自动建分片库表）
docker compose up -d --build
mysql -h127.0.0.1 -P8066 -uroot -p123456 -Ddb1 -e "select * from orders"

# 纯代理模式示例（MySQL + 代理，拦截 DDL；若未拉取官方镜像先执行上面的 docker build）
docker compose -f examples/docker/proxy.compose.yml up -d
```

完整 Compose 示例库见 [examples/docker/README.md](examples/docker/README.md)：

| 示例 | 重点 |
|---|---|
| [proxy-mysql](examples/docker/proxy-mysql/README.md) | SQL 结果缓存、SQL 拦截、慢 SQL、会话固定连接、HTTP API |
| [sharding-mysql](examples/docker/sharding-mysql/README.md) | 取模/枚举/月份/广播/单表 + MySQL XA |
| [sharding-postgresql](examples/docker/sharding-postgresql/README.md) | PG schema 分片、引号/大小写、PG XA |
| [sharding-oracle](examples/docker/sharding-oracle/README.md) | Oracle 双 schema、自动物理 DDL、NLS、无 XA |
| [sharding-sqlserver](examples/docker/sharding-sqlserver/README.md) | SQL Server 2022 后端、T-SQL 方言、SQL 认证/TLS、无 XA |
| [tds-frontend](examples/docker/tds-frontend/README.md) | SQL Server 客户端（TDS）接入；与 SQL Server 后端方向不同 |
| [performance](examples/docker/performance/README.md) | 线程/超时/连接池/缓存参数与容量区间 |

容器细节（端口/卷/TLS/后端示例/健康检查/多架构/发布流程）见 [docs/docker.md](docs/docker.md)，
性能参数与容量规划见 [docs/performance.md](docs/performance.md)。

### 试用示例

```sql
-- 分片表（id % 4 → 4 个物理表分布在 ds0/ds1）
insert into orders (id, name) values (1,'a'),(2,'b'),(3,'c'),(4,'d');
select * from orders where id = 3;        -- 精确裁剪到单分片
select count(*) from orders;              -- 跨分片 COUNT 归并
select * from orders order by id desc limit 2;  -- 跨分片归并排序

-- 广播表：写入自动扇出到所有节点
insert into dict (id, name) values (1, 'x');

-- 查看路由
explain select * from orders where id = 2;

-- Hint 强制路由
/*!mycat:target=ds1*/ select * from orders;

-- 运维命令
show @@cluster;    show @@datasource;    show @@heartbeat;
```

## 配置

与 Mycat3 Java 版相同的 JSON 多文件布局（可直接拷贝 Java 版的配置目录使用）：

```
conf/
├── server.json                  # 端口/线程/监控
├── users/root.user.json         # 用户认证
├── datasources/ds0.datasource.json  # 后端数据源 (jdbc:mysql://...)
├── clusters/c0.cluster.json     # 集群拓扑/读写分离/心跳
├── schemas/db1.schema.json      # 逻辑库/分片表/广播表/单表
└── sequences/*.sequence.json    # 序列（可选）
```

完整字段说明见 [docs/configuration.md](docs/configuration.md)。

## 文档

- [docs/architecture.md](docs/architecture.md) — 架构设计与 Java↔Rust 模块映射
- [docs/oracle-tns-feasibility.md](docs/oracle-tns-feasibility.md) — Oracle/TNS **前端**可行性评估（协议逆向现状、工程量、路线图）
- [docs/oracle-backend-feasibility.md](docs/oracle-backend-feasibility.md) — Oracle **后端**接入评估（官方 rust-oracledb 驱动 + MySQL→Oracle 方言翻译，~3–5K 行；**方向 B 已实施**，含集成验证结果）
- [docs/configuration.md](docs/configuration.md) — 配置参考
- [docs/commands.md](docs/commands.md) — SQL/管理命令参考与能力边界
- [docs/compatibility.md](docs/compatibility.md) — 与 Mycat3 Java 版的兼容性与差异（含修复清单）
- [docs/docker.md](docs/docker.md) — Docker 镜像使用指南（两种模式、TLS、多后端、多架构）
- [docs/performance.md](docs/performance.md) — 性能参数、容量预算、场景区间与调优顺序
- [examples/docker/README.md](examples/docker/README.md) — Compose 示例库（代理/分片/多后端/性能）
- [docs/sqlserver-backend-feasibility.md](docs/sqlserver-backend-feasibility.md) — SQL Server 后端（TDS 客户端 + T-SQL 方言）代价与实施方案

## 与 Java 版的主要差异

Pro 版聚焦**透传路由模式**（Java 版 `HackRouter` 快速路径的超集）。未移植能力遇到时返回明确错误，绝不静默给出错误结果；已补齐的能力在下面单独列出：

未移植：
- Calcite 分布式执行引擎：跨分片 JOIN、GROUP BY 下推、子查询、AVG/复杂聚合（Pro 支持单分片表全 CRUD + 简单聚合归并）
- TNS/Oracle 前端：Java 版为无认证 PoC（无法对接真实客户端），Pro 版嗅探到 TNS 流量时明确拒绝而非伪装。可行性已单独评估（2022–2026 年 Oracle 官方开源 thin 驱动使协议知识公开），结论为有条件可行，路线图见 [oracle-tns-feasibility.md](docs/oracle-tns-feasibility.md)
- 在线分片迁移（migrate）、SQL 计划基线（BASELINE/SPM）、持久化组序列
- MySQL 压缩协议、TDS 加密协商（MySQL/PostgreSQL 前端 TLS 已支持，见下）

已实现，且与 Java 版存在差异的关键能力：
- XA 分布式事务：**MySQL + PostgreSQL**（2PC、决策 WAL、启动 `XA RECOVER`/`pg_prepared_xacts` 恢复；Oracle thin 驱动无 XA 接口，显式拒绝）
- Oracle 后端（`dbType: "oracle"`）：官方 rust-oracledb thin 驱动 + MySQL→Oracle 方言翻译（标识符/LIMIT→ROWNUM/多行 INSERT→SELECT UNION ALL/函数/日期格式），支持 MySQL+Oracle 混合分片、三前端接入、事务语义对齐；Oracle 物理建表支持自动 DDL（Oracle 方言/identity/索引）。详见 [oracle-backend-feasibility.md](docs/oracle-backend-feasibility.md) §8 与 [commands.md](docs/commands.md)
- PostgreSQL / SQL Server 后端、纯代理模式（`server.mode=proxy`）、MySQL/PostgreSQL 前端 TLS

详细清单与有意修复的 Java 版缺陷见 [docs/compatibility.md](docs/compatibility.md)。

## 开发

```bash
cargo fmt --all          # 格式化
cargo clippy --workspace # lint
cargo test --workspace   # 测试（379 个）
```

目录结构：

```
crates/
├── protocol/   # MySQL 协议编解码（握手/认证/包分帧/OK-ERR-EOF/列定义/行）
├── config/     # Mycat3 兼容 JSON 配置模型与文件存储
├── router/     # 分片算法（Java 逐位一致的哈希实现）与节点模型
├── sql/        # SQL 解析、元数据、路由分析、物理 SQL 重写
├── client/     # 原生异步 MySQL 后端客户端 + 连接池
├── pgclient/   # PostgreSQL 后端（tokio-postgres）客户端/池
├── oracle/     # Oracle 后端（rust-oracledb thin）客户端/池
├── mssql/      # SQL Server 后端（tiberius）客户端/池 + 类型/错误映射
├── replica/    # 负载均衡策略、副本选择、心跳与主从切换
└── server/     # 协议嗅探(sniff)、MySQL 前端(frontend)、PostgreSQL 前端(pg)、
                # SQL Server 前端(tds)、协议无关执行层(engine)、执行引擎(executor)、
                # 本地命令(commands)、事务(tx)、监控(monitor)、CLI (mycat3-pro)
conf/           # 默认配置（2 数据源 + 分片/广播/单表演示）
docker/         # compose 演示环境初始化 SQL 与容器配置
examples/       # Docker Compose 示例库（代理、MySQL/PG/Oracle/SQLServer 分片、性能）
scripts/        # 启动/停止、示例校验/运行、Docker Hub 发布脚本
docs/           # 文档
tests/          # （预留）跨 crate 测试
```

## License

GPL-3.0（与 Mycat3 一致，见 LICENSE）。
