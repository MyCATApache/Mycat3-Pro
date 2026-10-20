#!/usr/bin/env bash
# Mycat3 Pro 示例库快速校验 / 一键运行（傻瓜化入口）
#
# 用法:
#   ./scripts/demo-smoke.sh check           # 校验所有 compose + 配置 + 脚本（默认）
#   ./scripts/demo-smoke.sh list            # 列出示例
#   ./scripts/demo-smoke.sh run proxy-mysql # 启动并运行一个示例，完成后停止
#   ./scripts/demo-smoke.sh run proxy-mysql --keep  # 保留容器
#
# 说明:
# - check 优先使用仓库已构建的 target/release/mycat3-pro；没有则用 Docker 镜像。
# - SQL Server 示例会从源码构建镜像（需要 Docker 网络）。
# - Oracle/SQL Server 示例资源占用较大，不建议同时运行。
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DEMOS=(proxy-mysql sharding-mysql sharding-postgresql tds-frontend sharding-oracle sharding-sqlserver performance)

die() { echo "错误: $*" >&2; exit 1; }
section() { printf '\n== %s ==\n' "$*"; }

list_demos() {
    printf '%s\n' "${DEMOS[@]}"
}

check_one() {
    local d="$1"
    local dir="$ROOT/examples/docker/$d"
    [ -d "$dir" ] || die "示例不存在: $d"
    section "$d"
    (cd "$dir" && docker compose config --quiet) || die "$d: compose 配置无效"
    if [ -x "$ROOT/target/release/mycat3-pro" ]; then
        "$ROOT/target/release/mycat3-pro" check --home "$dir/conf" >/dev/null
    else
        docker run --rm -v "$dir/conf:/conf:ro" mycat/mycat3-pro:latest check --home /conf >/dev/null
    fi
    if [ -f "$dir/demo.sh" ]; then
        bash -n "$dir/demo.sh"
    fi
    echo "OK: $d"
}

cmd_check() {
    command -v docker >/dev/null || die "需要 docker"
    for d in "${DEMOS[@]}"; do
        check_one "$d"
    done
    bash -n "$ROOT/scripts/demo-smoke.sh"
    echo
    echo "全部示例配置/脚本校验通过（7 组）。"
}

cmd_run() {
    local name="${1:-}"
    [ -n "$name" ] || die "用法: $0 run <示例名> [--keep]"
    shift || true
    local keep=0
    for a in "$@"; do
        case "$a" in
            --keep) keep=1 ;;
            *) die "未知参数: $a" ;;
        esac
    done
    local dir="$ROOT/examples/docker/$name"
    [ -d "$dir" ] || die "示例不存在: $name（$0 list 查看）"
    [ -f "$dir/docker-compose.yml" ] || die "$name 缺少 docker-compose.yml"
    section "启动 $name"
    if [ "$name" = "sharding-sqlserver" ]; then
        (cd "$dir" && docker compose up -d --build --wait)
    else
        (cd "$dir" && docker compose up -d --wait)
    fi
    local rc=0
    if [ -f "$dir/demo.sh" ]; then
        (cd "$dir" && ./demo.sh) || rc=$?
    else
        echo "该示例没有 demo.sh，请按 README 手动执行。"
    fi
    if [ "$keep" -eq 0 ]; then
        section "停止 $name"
        (cd "$dir" && docker compose down) || true
    fi
    exit "$rc"
}

case "${1:-check}" in
    check) cmd_check ;;
    list) list_demos ;;
    run) shift; cmd_run "$@" ;;
    -h|--help|help)
        sed -n '2,18p' "$0" | sed 's/^# \{0,1\}//'
        ;;
    *) die "未知命令: $1（check|list|run）" ;;
esac
