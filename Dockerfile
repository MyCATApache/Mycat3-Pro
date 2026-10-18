# Mycat3 Pro 多阶段构建镜像
# 构建: docker build -t mycat3-pro:latest .
# 运行: docker run -d -p 8066:8066 -p 9066:9066 -v $PWD/conf:/opt/mycat3-pro/conf mycat3-pro:latest

# ---------- 构建阶段 ----------
FROM rust:1.83-bookworm AS builder
WORKDIR /build
# 先拷贝清单以利用依赖层缓存
COPY Cargo.toml Cargo.lock* ./
COPY crates ./crates
RUN cargo build --release --bin mycat3-pro

# ---------- 运行阶段 ----------
FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/* \
    && useradd -m -u 1000 mycat
WORKDIR /opt/mycat3-pro
COPY --from=builder /build/target/release/mycat3-pro /usr/local/bin/mycat3-pro
COPY conf /opt/mycat3-pro/conf
COPY scripts/mycat3-pro.sh /opt/mycat3-pro/bin/mycat3-pro.sh
RUN chmod +x /opt/mycat3-pro/bin/mycat3-pro.sh && chown -R mycat /opt/mycat3-pro
USER mycat
# 8066: MySQL 协议端口  9066: 监控 HTTP 端口
EXPOSE 8066 9066
ENV MYCAT3_PRO_HOME=/opt/mycat3-pro/conf
HEALTHCHECK --interval=15s --timeout=3s --retries=5 \
    CMD curl -fsS http://127.0.0.1:9066/-/healthy || exit 1
ENTRYPOINT ["mycat3-pro", "start", "--home", "/opt/mycat3-pro/conf"]
