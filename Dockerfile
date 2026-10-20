# Mycat3 Pro 官方镜像（Docker Hub: mycat/mycat3-pro）
#
# 构建:
#   docker build -t mycat/mycat3-pro:latest .
# 运行（分片模式，挂载自定义配置）:
#   docker run -d --name mycat3-pro -p 8066:8066 -p 9066:9066 \
#     -v "$PWD/conf:/opt/mycat3-pro/conf" mycat/mycat3-pro:latest
# 纯代理模式：同一镜像，把 server.json 的 mode 设为 "proxy" 即可

# ---------- 构建阶段 ----------
FROM rust:1-bookworm AS builder
WORKDIR /build
# aws-lc-sys（rustls 的加密后端）需要 cmake/perl
RUN apt-get update && apt-get install -y --no-install-recommends cmake perl pkg-config \
    && rm -rf /var/lib/apt/lists/*
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
RUN cargo build --release --locked -p mycat3-server

# ---------- 运行阶段 ----------
FROM debian:bookworm-slim
ARG VERSION=0.3.1
LABEL org.opencontainers.image.title="Mycat3 Pro" \
      org.opencontainers.image.description="MySQL/PostgreSQL/TDS protocol middleware with sharding and proxy modes; MySQL/Oracle/PostgreSQL/SQL Server backends" \
      org.opencontainers.image.url="https://github.com/MyCATApache/Mycat3-Pro" \
      org.opencontainers.image.source="https://github.com/MyCATApache/Mycat3-Pro" \
      org.opencontainers.image.documentation="https://github.com/MyCATApache/Mycat3-Pro/blob/main/docs/docker.md" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.licenses="GPL-3.0"

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl tini \
    && rm -rf /var/lib/apt/lists/* \
    && useradd -m -u 1000 mycat

WORKDIR /opt/mycat3-pro
COPY --from=builder /build/target/release/mycat3-pro /usr/local/bin/mycat3-pro
COPY conf ./conf
COPY scripts/mycat3-pro.sh ./bin/mycat3-pro.sh
COPY README.md LICENSE ./
RUN chmod +x ./bin/mycat3-pro.sh && chown -R mycat:mycat /opt/mycat3-pro

USER mycat
# 8066: MySQL/PG/TDS 同端口   9066: 监控 HTTP（/-/healthy、/metrics、/api/status）
EXPOSE 8066 9066
ENV MYCAT3_PRO_HOME=/opt/mycat3-pro/conf

HEALTHCHECK --interval=15s --timeout=3s --start-period=10s --retries=5 \
    CMD curl -fsS http://127.0.0.1:9066/-/healthy || exit 1

# tini 作为 PID 1 转发 SIGTERM（主程序已处理优雅停机）
ENTRYPOINT ["/usr/bin/tini", "--", "mycat3-pro"]
CMD ["start", "--home", "/opt/mycat3-pro/conf"]
