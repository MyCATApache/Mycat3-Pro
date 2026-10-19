#!/usr/bin/env bash
# Mycat3 Pro 分库分表（Oracle 后端）一键演示
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"

COMPOSE=(docker compose)
MYSQL=(docker compose run --rm -T mysql-client --default-character-set=utf8mb4 -hmycat3-pro -P8066 -uroot -Ddb1 --table)
HTTP="http://127.0.0.1:49066"

section() { printf '\n\033[1;36m===== %s =====\033[0m\n' "$*"; }
mysql_run() {
    local out rc
    set +e
    out=$("${MYSQL[@]}" "$@" 2>&1); rc=$?
    set -e
    printf '%s\n' "$out" | grep -vE '^ ?Container |^$' || true
    return $rc
}
mysql_quiet() { "${MYSQL[@]}" "$@" >/dev/null 2>&1; }

section "启动 Oracle Free + Mycat3 Pro（首次启动 2–5 分钟）"
"${COMPOSE[@]}" up -d
for _ in $(seq 1 120); do
    if curl -fsS "$HTTP/-/healthy" >/dev/null 2>&1; then break; fi
    sleep 5
done
curl -fsS "$HTTP/-/healthy"
echo "等待 Oracle 初始化 / Mycat 数据源可用..."
for _ in $(seq 1 120); do
    if mysql_quiet -e "select count(*) from orders"; then break; fi
    sleep 5
done

section "清理旧演示数据"
mysql_run <<'SQL'
delete from orders where id between 1 and 9999;
delete from tenant_order where id between 1 and 9999;
delete from dict where id between 1 and 9999;
delete from user_info where id between 1 and 9999;
SQL

section "1. 取模分片 orders（Oracle schema/大写表名）"
mysql_run <<'SQL'
insert into orders (id,user_id,amount,status,created) values
 (1,101,10.00,'NEW','2026-01-01 10:00:00'),
 (2,102,20.00,'PAID','2026-01-02 10:00:00'),
 (3,103,30.00,'NEW','2026-01-03 10:00:00'),
 (4,104,40.00,'PAID','2026-01-04 10:00:00');
/*!mycat:showTopology{orders}*/;
explain select * from orders where id = 3;
select id,amount,status from orders where id = 3;
select count(*) as order_count from orders;
select sum(amount) as amount_sum from orders;
select id,amount from orders order by amount desc limit 3;
SQL

section "2. 枚举分片 + 广播表 + 单表"
mysql_run <<'SQL'
insert into tenant_order (id,tenant_id,note,created) values
 (101,'acme','tenant-a','2026-01-01 00:00:00'),
 (102,'globex','tenant-b','2026-01-02 00:00:00');
insert into dict (id,name) values (1,'CN'),(2,'US');
insert into user_info (id,name,profile) values (1,'alice','vip');
select * from tenant_order where tenant_id='acme';
select * from dict order by id;
select * from user_info;
SQL

section "3. Oracle 物理落点核对"
echo "-- DB1_0"
docker compose exec -T oracle sqlplus -s "DB1_0/demo_pw@//localhost:1521/FREEPDB1" <<'SQL'
set pagesize 50 linesize 120 feedback off
select count(*) as dict_rows from "dict";
select count(*) as orders_0 from ORDERS_0;
select count(*) as orders_1 from ORDERS_1;
SQL
echo "-- DB1_1"
docker compose exec -T oracle sqlplus -s "DB1_1/demo_pw@//localhost:1521/FREEPDB1" <<'SQL'
set pagesize 50 linesize 120 feedback off
select count(*) as dict_rows from "dict";
select count(*) as orders_2 from ORDERS_2;
select count(*) as orders_3 from ORDERS_3;
SQL

section "4. HTTP 状态"
curl -fsS "$HTTP/api/status" | python3 -c 'import json,sys;d=json.load(sys.stdin);print(json.dumps({k:d[k] for k in ("mode","queries","datasources")},ensure_ascii=False,indent=2))'
echo "说明：Oracle 分支不支持 XA；跨 Oracle 与其它后端的事务不承诺原子性。"
echo "演示结束；清理: docker compose down -v"
