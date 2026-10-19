#!/usr/bin/env bash
# Mycat3 Pro：SQL Server (TDS) 客户端 -> Mycat -> MySQL 一键演示
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"

COMPOSE=(docker compose)
SQLCMD=(docker compose run --rm -T tds-client -S mycat3-pro,8066 -U root -P 123456 -d db1 -W -s '|' -w 240)
HTTP="http://127.0.0.1:59066"

section() { printf '\n\033[1;36m===== %s =====\033[0m\n' "$*"; }
sqlcmd_run() {
    local out rc
    set +e
    out=$("${SQLCMD[@]}" "$@" 2>&1); rc=$?
    set -e
    printf '%s\n' "$out" | grep -vE '^ ?Container |^$' || true
    return $rc
}
sqlcmd_quiet() { "${SQLCMD[@]}" "$@" >/dev/null 2>&1; }

section "启动 MySQL + Mycat3 Pro（TDS 前端同端口 8066）"
"${COMPOSE[@]}" up -d
for _ in $(seq 1 90); do
    if curl -fsS "$HTTP/-/healthy" >/dev/null 2>&1; then break; fi
    sleep 1
done
curl -fsS "$HTTP/-/healthy"

section "清理旧演示数据（通过 TDS 执行）"
for q in \
  "delete from orders where id between 1 and 9999" \
  "delete from dict where id between 1 and 9999" \
  "delete from user_info where id between 1 and 9999"; do
    sqlcmd_quiet -Q "$q"
done

section "1. TDS 写入：4 条订单按 id % 2 分到两个 MySQL schema"
for q in \
  "insert into orders (id,user_id,amount,status,created) values (1,101,10.00,'NEW','2026-01-01 10:00:00')" \
  "insert into orders (id,user_id,amount,status,created) values (2,102,20.00,'PAID','2026-01-02 10:00:00')" \
  "insert into orders (id,user_id,amount,status,created) values (3,103,30.00,'NEW','2026-01-03 10:00:00')" \
  "insert into orders (id,user_id,amount,status,created) values (4,104,40.00,'PAID','2026-01-04 10:00:00')"; do
    sqlcmd_quiet -Q "$q"
done

echo "-- T-SQL TOP：前端改写为 LIMIT 后走 Mycat 归并"
sqlcmd_run -Q "select top 3 id,amount,status from orders order by amount desc"

echo "-- 分片拓扑"
sqlcmd_run -Q "/*!mycat:showTopology{orders}*/"

echo "-- 广播表 + 单表"
sqlcmd_quiet -Q "insert into dict (id,name) values (1,'CN')"
sqlcmd_quiet -Q "insert into user_info (id,name,profile) values (1,'alice','vip')"
sqlcmd_run -Q "select id,name from dict order by id"
sqlcmd_run -Q "select id,name,profile from user_info"

section "2. 同一份数据用 MySQL 协议读取（协议无关）"
docker compose exec -T -e MYSQL_PWD=123456 mysql \
  mysql --default-character-set=utf8mb4 -hmycat3-pro -P8066 -uroot -Ddb1 --table \
  -e "select id,amount,status from orders order by id; select count(*) as order_count from orders;"

section "3. 物理落点核对（MySQL 后端）"
for s in 0 1; do
    echo "-- db1_$s"
    docker compose exec -T -e MYSQL_PWD=tds_root_pw mysql \
      mysql -uroot --table -e "select count(*) as dict_rows from db1_$s.dict; select count(*) as shard_orders from db1_$s.orders_$s;"
done

section "4. HTTP 状态"
curl -fsS "$HTTP/api/status" | python3 -c 'import json,sys;d=json.load(sys.stdin);print(json.dumps({k:d[k] for k in ("mode","queries","datasources")},ensure_ascii=False,indent=2))'
echo "演示结束；清理: docker compose down -v"
