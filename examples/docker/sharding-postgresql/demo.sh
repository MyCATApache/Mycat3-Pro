#!/usr/bin/env bash
# Mycat3 Pro 分库分表（PostgreSQL 后端 / PostgreSQL 前端）一键演示
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"

COMPOSE=(docker compose)
PSQL_URL="postgresql://root:123456@mycat3-pro:8066/db1?sslmode=disable"
PSQL_XA_URL="postgresql://xa:123456@mycat3-pro:8066/db1?sslmode=disable"
HTTP="http://127.0.0.1:39066"

section() { printf '\n\033[1;36m===== %s =====\033[0m\n' "$*"; }
psql_run() { docker compose exec -T pg-shard-0 psql "$PSQL_URL" -v ON_ERROR_STOP=1 -P pager=off "$@"; }
psql_xa() { docker compose exec -T pg-shard-0 psql "$PSQL_XA_URL" -v ON_ERROR_STOP=1 -P pager=off "$@"; }

section "启动 2 个 PostgreSQL 分片 + Mycat3 Pro"
"${COMPOSE[@]}" up -d
for _ in $(seq 1 90); do
    if curl -fsS "$HTTP/-/healthy" >/dev/null 2>&1; then break; fi
    sleep 1
done
curl -fsS "$HTTP/-/healthy"

section "清理旧演示数据"
psql_run <<'SQL'
delete from orders where id between 1 and 9999;
delete from tenant_order where id between 1 and 9999;
delete from dict where id between 1 and 9999;
delete from user_info where id between 1 and 9999;
SQL

section "1. 取模分片 orders（PostgreSQL schema 落点）"
psql_run <<'SQL'
insert into orders (id,user_id,amount,status,created) values
 (1,101,10.00,'NEW','2026-01-01 10:00:00'),
 (2,102,20.00,'PAID','2026-01-02 10:00:00'),
 (3,103,30.00,'NEW','2026-01-03 10:00:00'),
 (4,104,40.00,'PAID','2026-01-04 10:00:00'),
 (5,105,50.00,'NEW','2026-01-05 10:00:00');
/*!mycat:showTopology{orders}*/;
explain select * from orders where id = 3;
select id,amount,status from orders where id = 3;
select count(*) as order_count from orders;
select sum(amount) as amount_sum from orders;
select id,amount from orders order by amount desc limit 3;
SQL

section "2. 枚举分片 tenant_order + 广播表 dict + 单表 user_info"
psql_run <<'SQL'
insert into tenant_order (id,tenant_id,note) values
 (101,'acme','tenant-a'),(102,'globex','tenant-b');
insert into dict (id,name) values (1,'CN'),(2,'US');
insert into user_info (id,name,profile) values (1,'alice','vip');
select * from tenant_order where tenant_id='acme' order by id;
select * from dict order by id;
select * from user_info;
SQL

section "3. XA 2PC：PostgreSQL PREPARE TRANSACTION / COMMIT PREPARED"
psql_xa <<'SQL'
begin;
insert into orders (id,user_id,amount,status,created) values (201,1,1.00,'XA','2026-02-01 00:00:00');
insert into orders (id,user_id,amount,status,created) values (202,2,2.00,'XA','2026-02-02 00:00:00');
commit;
select id,status from orders where id in (201,202) order by id;
begin;
insert into orders (id,user_id,amount,status,created) values (203,1,3.00,'XA','2026-02-03 00:00:00');
rollback;
select count(*) as xa_rolled_back from orders where id = 203;
SQL

section "4. PG 物理 schema 核对"
for s in 0 1; do
    echo "-- pg-shard-$s"
    docker compose exec -T "pg-shard-$s" psql -U postgres -d db1 -P pager=off \
      -c "select count(*) as dict_rows from db1_$s.dict; select count(*) as shard_orders from db1_$s.orders_$((s*2)); select count(*) as shard_orders_next from db1_$s.orders_$((s*2+1));"
done
echo "-- prepared transactions（应为空）"
for s in 0 1; do
    docker compose exec -T "pg-shard-$s" psql -U postgres -d db1 -P pager=off \
      -c "select gid from pg_prepared_xacts;"
done
docker compose exec -T mycat3-pro ls -la /opt/mycat3-pro/conf/xa

section "5. HTTP 状态"
curl -fsS "$HTTP/api/status" | python3 -c 'import json,sys;d=json.load(sys.stdin);print(json.dumps({k:d[k] for k in ("mode","queries","datasources")},ensure_ascii=False,indent=2))'
echo "演示结束；清理: docker compose down -v"
