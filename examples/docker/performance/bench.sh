#!/usr/bin/env bash
# 性能参数 demo 的轻量基准（不是严谨压测；真实容量请用 sysbench/mysqlslap/生产流量）
# 对比：缓存命中路径 vs 不命中缓存的普通分片查询
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"

COMPOSE=(docker compose)
HTTP="http://127.0.0.1:29067"
MYSQL=(docker compose exec -T -e MYSQL_PWD=123456 mysql mysql --default-character-set=utf8mb4 -hmycat3-pro -P8066 -uroot -Ddb1 --table)
MYSQL_BATCH=(docker compose exec -T -e MYSQL_PWD=123456 mysql mysql --default-character-set=utf8mb4 -hmycat3-pro -P8066 -uroot -Ddb1 -N -B)

section() { printf '\n\033[1;36m===== %s =====\033[0m\n' "$*"; }

make_queries() {
    local query="$1" n="$2" file="$3"
    : >"$file"
    for _ in $(seq 1 "$n"); do
        printf '%s;\n' "$query" >>"$file"
    done
}

now_ms() { date +%s%N | cut -b1-13; }

run_batch() {
    local file="$1" label="$2" conc="${3:-1}"
    local start end elapsed total
    start=$(now_ms)
    if [[ "$conc" -le 1 ]]; then
        "${MYSQL_BATCH[@]}" <"$file" >/dev/null
    else
        local pids=()
        for _ in $(seq 1 "$conc"); do
            "${MYSQL_BATCH[@]}" <"$file" >/dev/null &
            pids+=("$!")
        done
        for p in "${pids[@]}"; do wait "$p"; done
    fi
    end=$(now_ms)
    elapsed=$((end - start))
    total=$(( $(wc -l <"$file") * conc ))
    awk -v l="$label" -v c="$conc" -v n="$total" -v ms="$elapsed" \
        'BEGIN { if (ms < 1) ms = 1; printf "%-28s concurrency=%-2s queries=%-4s elapsed=%6dms  qps≈%.1f\n", l, c, n, ms, n * 1000 / ms }'
}

section "启动 MySQL + Mycat3 Pro"
"${COMPOSE[@]}" up -d
for _ in $(seq 1 60); do
    if curl -fsS "$HTTP/-/healthy" >/dev/null 2>&1; then break; fi
    sleep 1
done
curl -fsS "$HTTP/-/healthy"

section "预热缓存"
"${MYSQL[@]}" <<'SQL'
select id, user_id, amount, status from orders order by id;
select id, user_id, amount, status from orders where id = 1;
select id, user_id, amount, status from orders where id = 1;
select id, user_id, amount, status from orders where id = 1;
SQL

tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
make_queries "select id, user_id, amount, status from orders where id = 1" 50 "$tmp/cached.sql"
make_queries "select id, status, user_id from orders where id = 1" 50 "$tmp/uncached.sql"

section "轻量基准"
run_batch "$tmp/cached.sql"   "cache hit (1 conn)" 1
run_batch "$tmp/uncached.sql" "cache miss (1 conn)" 1
run_batch "$tmp/cached.sql"   "cache hit (8 conn)" 8
run_batch "$tmp/uncached.sql" "cache miss (8 conn)" 8

section "缓存状态"
"${MYSQL[@]}" -e '/*!mycat:showSqlCaches*/;'

section "连接池与指标"
"${MYSQL[@]}" -e 'show @@datasource;'
curl -fsS "$HTTP/metrics" | grep -E 'mycat3_(queries_total|query_errors_total)|mycat3_pool_(idle|available|created)' || true
echo
echo "说明：这个脚本用 mysql CLI 做快速前后对比；连接建立、宿主机调度、缓存预热程度都会影响数字。"
echo "真实容量请用 sysbench/mysqlslap 或生产流量，观察 p95/p99、池等待、后端 max_connections。"
