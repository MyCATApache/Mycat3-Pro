# Mycat3 Pro

Mycat3 的 Rust 重新实现：一个进程同时提供 **MySQL / PostgreSQL / SQL Server(TDS)** 三种前端协议
（同端口嗅探），支持 **MySQL / PostgreSQL / Oracle / SQL Server** 四种后端，可选**分片路由**或**纯代理**模式。

- 镜像：`mycat/mycat3-pro`（`latest`、`v0.3.0`）
- 平台：`linux/amd64`、`linux/arm64`（由发布工作流构建多架构 manifest）
- 源码与完整文档：https://github.com/MyCATApache/Mycat3-Pro
- Dockerfile：https://github.com/MyCATApache/Mycat3-Pro/blob/main/Dockerfile
- Docker 指南：https://github.com/MyCATApache/Mycat3-Pro/blob/main/docs/docker.md
- 可运行示例：https://github.com/MyCATApache/Mycat3-Pro/tree/main/examples/docker
- SQL Server 后端说明：https://github.com/MyCATApache/Mycat3-Pro/tree/main/examples/docker/sharding-sqlserver

> SQL Server 后端需使用包含 `dbType=mssql` 的新镜像；旧版 `latest` 可先按源码重建。

## 快速开始

```bash
docker run -d --name mycat3-pro \
  -p 8066:8066 -p 9066:9066 \
  -v "$PWD/conf:/opt/mycat3-pro/conf:ro" \
  mycat/mycat3-pro:latest
```

- `8066`：MySQL / PostgreSQL / TDS 同端口
- `9066`：监控 HTTP（`/-/healthy`、`/metrics`、`/api/status`）
- 配置目录：`/opt/mycat3-pro/conf`（镜像自带示例，建议挂载）
- 非 root 运行（UID 1000），`tini` 转 SIGTERM，支持 `docker stop` 优雅停机
- 只读挂载适合纯读配置；启用 XA 或 SQL 缓存落盘时，配置目录需可写（写入 `xa/`、`cache/sqlcache/`）

连接：

```bash
mysql  -h127.0.0.1 -P8066 -uroot -p123456 -Ddb1
psql   "postgresql://root:123456@127.0.0.1:8066/db1?sslmode=disable"
sqlcmd -S 127.0.0.1,8066 -U root -P 123456   # 需 encrypt=false
```

## 两种运行模式

| 模式 | 说明 |
|---|---|
| 分片模式（默认 `mode: local`） | 17 种分片算法、广播表、跨分片归并、DDL 自动化、读写分离 |
| 纯代理模式（`mode: proxy`） | SQL 原样透传单目标，仅保留慢 SQL / SQL 缓存 / SQL 拦截；会话固定后端连接，`USE/SET/事务` 与直连一致 |

纯代理配置示例（`server.json`）：

```json
{
  "mode": "proxy",
  "proxy": {
    "target": "ds0",
    "intercepts": [
      { "pattern": "(?i)drop\\s+table", "stmtType": "DDL", "errno": 1142 }
    ]
  }
}
```

完整可运行示例：`examples/docker/proxy.compose.yml`（MySQL + 代理，含初始化 SQL）。

## 完整演示环境

仓库内 `docker-compose.yml` 提供 1 主 1 从 MySQL + Mycat3 Pro，
自动建分片库表并演示路由/归并/广播表/事务：

```bash
git clone https://github.com/MyCATApache/Mycat3-Pro
cd Mycat3-Pro
docker compose up -d --build
mysql -h127.0.0.1 -P8066 -uroot -p123456 -Ddb1 -e "select * from orders"
```

## 配置

`/opt/mycat3-pro/conf`：`server.json`、`users/`、`datasources/`、`clusters/`、`schemas/`、`sqlcaches/`。
与 Java 版 Mycat3 配置格式兼容。后端示例（MySQL/PostgreSQL/Oracle/SQL Server）、TLS、健康检查、
多架构构建与故障排查见 [docs/docker.md](https://github.com/MyCATApache/Mycat3-Pro/blob/main/docs/docker.md)。

## 校验

```bash
docker run --rm mycat/mycat3-pro:latest version
docker run --rm -v "$PWD/conf:/opt/mycat3-pro/conf:ro" \
  mycat/mycat3-pro:latest check --home /opt/mycat3-pro/conf
docker inspect --format '{{.State.Health.Status}}' mycat3-pro
```

## License

GPL-3.0（与 Mycat3 一致）。
