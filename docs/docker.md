# Docker 使用指南

官方镜像：**`mycat/mycat3-pro`**（Docker Hub）。同一镜像包含分片模式与纯代理模式，
由 `server.json` 的 `mode` 决定，无需不同镜像。

- 标签：`latest`（最新构建）、`v0.3.0`（版本快照）
- 架构：`linux/amd64`（本地构建）；GitHub Actions 发布工作流同时构建 `linux/arm64`
- 端口：`8066`（MySQL / PostgreSQL / TDS 三协议同端口）、`9066`（监控 HTTP）
- 配置目录：`/opt/mycat3-pro/conf`（镜像内已带示例配置，建议挂载覆盖）
- 运行用户：`mycat`（UID 1000，非 root）
- 入口：`tini -- mycat3-pro`（PID 1 正确转发 SIGTERM，支持 `docker stop` 优雅停机）

## 快速开始（分片模式）

从 Docker Hub 运行官方镜像：

```bash
docker pull mycat/mycat3-pro:latest
docker run -d --name mycat3-pro \
  -p 8066:8066 -p 9066:9066 \
  mycat/mycat3-pro:latest
```

> 如果当前环境还拉不到该镜像，可在仓库根目录先执行
> `docker build -t mycat/mycat3-pro:latest .`，再运行上面的 `docker run`。

仓库根目录还提供完整演示环境（1 主 1 从 MySQL + Mycat3 Pro，自动建分片库表）：

```bash
docker compose up -d --build
mysql -h127.0.0.1 -P8066 -uroot -p123456 -Ddb1 -e "select * from orders"
```

只运行代理（挂载自己的配置）：

```bash
docker run -d --name mycat3-pro \
  -p 8066:8066 -p 9066:9066 \
  -v "$PWD/conf:/opt/mycat3-pro/conf:ro" \
  mycat/mycat3-pro:latest
```

> 容器内配置默认的数据源指向 `127.0.0.1:3306`。若后端在宿主机，使用
> `--network host` 或把 `datasources/*.json` 的 host 改为可达地址（如 compose 服务名）。

查看版本 / 校验配置（覆盖默认命令）：

```bash
docker run --rm mycat/mycat3-pro:latest version
docker run --rm -v "$PWD/conf:/opt/mycat3-pro/conf:ro" mycat/mycat3-pro:latest check --home /opt/mycat3-pro/conf
```

## 纯代理模式（proxy-only）

`examples/docker/proxy.compose.yml` 提供可直接运行的示例：一个 MySQL 后端 +
代理（拦截 DDL、可加 SQL 缓存/慢 SQL）。关键配置：

```json
{
  "mode": "proxy",
  "proxy": {
    "target": "ds0",
    "intercepts": [
      { "pattern": "(?i)drop\\s+table", "stmtType": "DDL",
        "errno": 1142, "message": "DDL is blocked by proxy policy" }
    ]
  }
}
```

```bash
docker compose -f examples/docker/proxy.compose.yml up -d
mysql -h127.0.0.1 -P18066 -uroot -p123456 -Ddemo -e "select * from t_proxy"
mysql -h127.0.0.1 -P18066 -uroot -p123456 -Ddemo -e "drop table t_proxy"   # 被拦截：ERROR 1142
```

> 该示例直接使用 `mycat/mycat3-pro:latest`。若尚未从 Docker Hub 拉取，先在仓库根目录执行
> `docker build -t mycat/mycat3-pro:latest .`。

代理模式的会话语义与直连一致：`-D` / `USE` / `SET` / `BEGIN…COMMIT/ROLLBACK`
原样下推后端（每个前端会话固定一条后端连接）。

## 卷与环境变量

| 项 | 说明 |
|---|---|
| `-v <conf>:/opt/mycat3-pro/conf` | 配置目录（server.json / users / datasources / clusters / schemas / sqlcaches） |
| `MYCAT3_PRO_HOME` | 配置目录环境变量（默认 `/opt/mycat3-pro/conf`，仅作提示；实际以命令行 `--home` 为准） |
| `-p 8066:8066` | 三协议服务端口 |
| `-p 9066:9066` | 监控：`/-/healthy`、`/-/ready`、`/metrics`、`/api/status`、`/api/config` |

监控端点（默认开启）：`GET /-/healthy`、`GET /-/ready`、`GET /metrics`、
`GET /api/status`、`GET /api/config`、`GET /api/sqlcaches`、`POST /api/reload`。

> 容器以 UID 1000 运行。只读挂载（`:ro`）适合纯读配置；启用 XA 或 SQL 缓存落盘时，
> 程序需要写 `<配置目录>/xa/` 与 `<配置目录>/cache/sqlcache/`，请改为可写挂载，
> 或为这两个子目录单独挂载可写卷：
>
> ```bash
> docker run -d --name mycat3-pro -p 8066:8066 -p 9066:9066 \
>   -v "$PWD/conf:/opt/mycat3-pro/conf" \
>   -v mycat3-xa:/opt/mycat3-pro/conf/xa \
>   -v mycat3-cache:/opt/mycat3-pro/conf/cache \
>   mycat/mycat3-pro:latest
> ```

## 后端配置示例

### MySQL

```json
{
  "name": "ds0",
  "url": "jdbc:mysql://mysql:3306/db1_0?useUnicode=true&characterEncoding=UTF-8",
  "user": "root",
  "password": "change_me",
  "dbType": "mysql",
  "maxCon": 50,
  "minCon": 1,
  "instanceType": "READ_WRITE",
  "type": "NATIVE"
}
```

### PostgreSQL

```json
{
  "name": "ds1",
  "url": "jdbc:postgresql://postgres:5432/db1",
  "user": "postgres",
  "password": "change_me",
  "dbType": "postgresql",
  "maxCon": 20,
  "minCon": 1,
  "instanceType": "READ_WRITE",
  "type": "NATIVE"
}
```

### Oracle

```json
{
  "name": "ds2",
  "url": "jdbc:oracle:thin:@//oracle:1521/FREEPDB1",
  "user": "mycat",
  "password": "change_me",
  "dbType": "oracle",
  "maxCon": 20,
  "minCon": 1,
  "instanceType": "READ_WRITE",
  "type": "NATIVE",
  "initSqls": ["ALTER SESSION SET NLS_DATE_FORMAT='YYYY-MM-DD HH24:MI:SS'"]
}
```

Oracle 物理表需由 DBA 手工创建；Oracle 后端不支持 XA（thin 驱动无接口）。

## 前端 TLS

把证书挂载进容器，并在 `server.json` 配置相对路径（相对配置目录）：

```json
{ "server": { "sslCert": "cert.pem", "sslKey": "key.pem" } }
```

```bash
docker run -d -p 8066:8066 -p 9066:9066 \
  -v "$PWD/conf:/opt/mycat3-pro/conf:ro" mycat/mycat3-pro:latest
mysql --ssl-mode=REQUIRED -h127.0.0.1 -P8066 -uroot -p123456 -Ddb1
```

MySQL 与 PostgreSQL 前端支持 TLS；TDS 加密协商未实现。

## 健康检查与优雅停机

镜像内置：

```dockerfile
HEALTHCHECK CMD curl -fsS http://127.0.0.1:9066/-/healthy || exit 1
```

`docker stop` 发送 SIGTERM，进程完成优雅停机；`tini` 作为 PID 1 回收子进程。

## 构建与多架构

```bash
# 本机架构
docker build -t mycat/mycat3-pro:latest -t mycat/mycat3-pro:v0.3.0 .

# 多架构（需 QEMU/binfmt）
docker buildx build --platform linux/amd64,linux/arm64 \
  --build-arg VERSION=0.3.0 \
  -t mycat/mycat3-pro:latest -t mycat/mycat3-pro:v0.3.0 --push .
```

`.dockerignore` 已排除 `target/`（20GB 构建产物）与本地测试环境，构建上下文很小。

## 发布镜像到 Docker Hub（维护者）

镜像名固定为 `mycat/mycat3-pro`，标签为 `latest` 与 `v<version>`。
发布账号必须拥有 Docker Hub `mycat` 命名空间的写权限；其余账号只能推送到自己的命名空间。

### 本地发布

```bash
# 1. 先本地构建（或直接使用已有的 mycat/mycat3-pro:latest）
docker build -t mycat/mycat3-pro:latest -t mycat/mycat3-pro:v0.3.0 .

# 2. 预演：只打印计划，不登录、不推送
./scripts/publish-dockerhub.sh --dry-run

# 3. 推送并更新 Docker Hub 仓库简介
DOCKERHUB_USER=<账号> DOCKERHUB_TOKEN=<PAT> ./scripts/publish-dockerhub.sh
```

- `DOCKERHUB_TOKEN` 使用 Docker Hub **Personal Access Token**（Read & Write），不要用账号密码。
- 脚本先推 `v0.3.0`，再推 `latest`；成功后用 `docker/README.md` 更新 Hub 仓库简介。
- 未设置 `DOCKERHUB_*` 时沿用本机 `docker login` 凭据，但会跳过简介更新；
  也可单独运行 `... --description-only` 更新简介。
- 只推本地单架构镜像会覆盖同名多架构 manifest；需要 arm64 时用
  `PLATFORMS=linux/amd64,linux/arm64 ./scripts/publish-dockerhub.sh`（buildx 重新构建）
  或使用下面的 GitHub Actions。

### GitHub Actions（推荐，多架构）

工作流：`.github/workflows/docker-publish.yml`。在仓库
`Settings → Secrets and variables → Actions` 添加：

| Secret | 内容 |
|---|---|
| `DOCKERHUB_USERNAME` | 有 `mycat` 命名空间写权限的 Docker Hub 账号 |
| `DOCKERHUB_TOKEN` | Docker Hub PAT（Read & Write） |

触发方式：

- 推送 `v*` 标签（自动构建并发布该版本 + `latest`）
- Actions 页面手动运行 **Docker Publish**（可指定版本、是否更新 `latest`、目标平台）

工作流使用 QEMU 构建 `linux/amd64,linux/arm64`，推送 manifest 后调用
`scripts/publish-dockerhub.sh --description-only` 同步 Hub 简介。

> Docker Hub 的 `hub.docker.com` / `registry-1.docker.io` 在部分网络环境不可达
> （典型表现是 DNS 解析异常或连接超时）。本地发布需要能访问 Docker Hub；
> 受限网络下推荐用 GitHub Actions runner 发布，或为 Docker daemon 配置 HTTP 代理。

## 故障排查

| 现象 | 处理 |
|---|---|
| 容器反复 unhealthy | 检查 `9066` 是否被覆盖/关闭（`monitor.open=false` 时健康检查会失败，可覆盖 HEALTHCHECK） |
| 无法连接后端 | 容器内 `127.0.0.1` 是容器自身；改用宿主机 IP、`--network host` 或 compose 服务名 |
| 配置不生效 | 确认挂载路径为 `/opt/mycat3-pro/conf`，且 `docker run` 命令为默认 `start --home ...` |
| `Permission denied` / `Read-only file system` | 启用 XA 或 SQL 缓存时配置目录需可写（UID 1000）；去掉 `:ro` 或为 `xa/`、`cache/` 挂载可写卷 |
| `docker push` 超时 / Docker Hub 解析异常 | 当前网络无法访问 Docker Hub；改用可访问的 runner（见 GitHub Actions 发布）或给 daemon 配代理 |
| 数据源密码含特殊字符 | 使用 JSON 转义；避免在 shell 历史中明文传密码，优先挂载配置文件 |
