# Mycat3 Pro v0.3.0

Rust 重实现的 Mycat3 分布式数据库中间件：一个进程同时提供 **MySQL / PostgreSQL / SQL Server(TDS)**
三种前端协议（同端口嗅探），支持 **MySQL / PostgreSQL / Oracle** 三种后端，可选分片路由或纯代理模式。

## 主要能力

- **三前端同端口嗅探**：MySQL、PostgreSQL（v3 扩展协议）、TDS 7.2+（sp_executesql RPC），
  TNS 流量明确拒绝
- **分片模式**：17 种与 Java 版逐位一致的算法（Mod/Long/HashMod/MurmurHash/RangeMod/Pattern/
  PrefixPattern/FileMap/SubString/String/Date/Month/HotDate/CRC32 预分槽/JumpConsistentHash/
  AutoPartitionByLong/AutoFunction），广播表、分片键裁剪、跨分片 ORDER BY/LIMIT 归并与
  COUNT/SUM/MAX/MIN 聚合
- **纯代理模式**（`server.mode=proxy`）：SQL 原样透传单目标，仅保留慢 SQL 记录、SQL 结果缓存、
  SQL 拦截（先于缓存判定）与本地管理命令；会话固定后端连接，`USE/SET/事务` 语义与直连一致
- **后端**：MySQL 原生协议、PostgreSQL（tokio-postgres）、Oracle（rust-oracledb thin）
- **XA 2PC**：MySQL + PostgreSQL 分支（决策 WAL + 启动 `XA RECOVER` / `pg_prepared_xacts` 恢复），
  支持混合后端事务
- **SQL 结果缓存**：模板 `?` 参数槽位 + 变体缓存、二进制文件持久化（mmap 恢复）、周期预热、
  请求合并、DML 感知失效
- **运维与安全**：前端 TLS（MySQL/PG）、只读/登录数限制、RSA 密码、SQL 拦截规则、
  慢 SQL 观测、`loaddata` CSV 导入、`show @@*` 管理命令、HTTP 管理 API 与配置快照回滚
- **DDL 自动化**：CREATE/DROP DATABASE/TABLE/USER/SEQUENCE、索引扇出、RENAME、
  ALTER 后元数据回读、CHECK TABLE、单表视图注册

测试：**342 个**单元/集成/e2e 测试全部通过（含真实 MySQL、PostgreSQL、Oracle 容器验证）。

## 发布资产

| 文件 | 平台 |
|---|---|
| `mycat3-pro-v0.3.0-x86_64-unknown-linux-gnu.tar.gz` | Linux x86_64 (glibc) |
| `mycat3-pro-v0.3.0-aarch64-unknown-linux-gnu.tar.gz` | Linux arm64 (glibc) |
| `mycat3-pro-v0.3.0-x86_64-pc-windows-msvc.zip` | Windows x64 |
| `mycat3-pro-v0.3.0-aarch64-pc-windows-msvc.zip` | Windows arm64 |
| `mycat3-pro-v0.3.0-source.tar.gz` | 源码（tag 快照） |
| `SHA256SUMS.txt` | 全部资产的 SHA-256 校验值 |

每个压缩包内含：`mycat3-pro[.exe]`、`README.md`、`LICENSE`、`conf/`（示例配置）。

## 快速开始

Linux：

```bash
tar -xzf mycat3-pro-v0.3.0-x86_64-unknown-linux-gnu.tar.gz -C mycat3-pro
cd mycat3-pro
./mycat3-pro check --home conf
./mycat3-pro start --home conf
```

Windows（PowerShell）：

```powershell
Expand-Archive mycat3-pro-v0.3.0-x86_64-pc-windows-msvc.zip -DestinationPath mycat3-pro
cd mycat3-pro
.\mycat3-pro.exe check --home conf
.\mycat3-pro.exe start --home conf
```

默认端口：服务 `8066`，监控 `9066`（见 `conf/server.json`）。示例用户 `root/123456`，请先修改。

连接：

```bash
mysql -h127.0.0.1 -P8066 -uroot -p123456 -Ddb1
psql "host=127.0.0.1 port=8066 user=root dbname=db1 sslmode=disable"
sqlcmd -S 127.0.0.1,8066 -U root -P 123456
```

## 校验

```bash
sha256sum -c SHA256SUMS.txt
```

## 已知边界

- 跨分片 JOIN / GROUP BY / AVG 等分布式执行由 Java 版 Calcite 引擎提供，本版返回明确错误；
- 无 XA 的跨后端混合事务不保证原子性；Oracle 后端不支持 XA（thin 驱动无接口）；
- 前端 TLS 仅 MySQL/PostgreSQL；TDS 加密协商未实现；
- Oracle 物理建表由 DBA 手工执行（DDL 自动化仅 MySQL 后端）。

详细配置、命令与兼容性见仓库 `docs/`。
