#!/usr/bin/env bash
# Mycat3 Pro 分库分表（MySQL 后端）一键演示
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"

COMPOSE=(docker compose)
MYSQL=(docker compose exec -T -e MYSQL_PWD=123456 mysql-shard-0 mysql --default-character-set=utf8mb4 -hmycat3-pro -P8066 -uroot -Ddb1 --table)
XA=(docker compose exec -T -e MYSQL_PWD=123456 mysql-shard-0 mysql --default-character-set=utf8mb4 -hmycat3-pro -P8066 -uxa -Ddb1 --table)
HTTP="http://127.0.0.1:29066"

section() { printf '\n\033[1;36m===== %s =====\033[0m\n' "$*"; }
mysql_run() { "${MYSQL[@]}" "$@"; }
xa_run() { "${XA[@]}" "$@"; }

section "启动 2 个 MySQL 分片 + Mycat3 Pro"
"${COMPOSE[@]}" up -d
for _ in $(seq 1 90); do
    if curl -fsS "$HTTP/-/healthy" >/dev/null 2>&1; then break; fi
    sleep 1
done
curl -fsS "$HTTP/-/healthy"

section "清理旧演示数据（便于重复运行）"
mysql_run <<'SQL'
delete from orders where id between 1 and 9999;
delete from tenant_order where id between 1 and 9999;
delete from events where id between 1 and 9999;
delete from dict where id between 1 and 9999;
delete from user_info where id between 1 and 9999;
SQL

section "1. 取模分片 orders：4 个物理表，按 id % 4 路由"
mysql_run <<'SQL'
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

section "2. 枚举分片 tenant_order：tenant_id → 固定节点（租户隔离）"
mysql_run <<'SQL'
insert into tenant_order (id,tenant_id,note) values
 (101,'acme','tenant-a order'),
 (102,'globex','tenant-b order'),
 (103,'acme','tenant-a order 2');
/*!mycat:showTopology{tenant_order}*/;
select * from tenant_order where tenant_id='acme' order by id;
select * from tenant_order where tenant_id='globex' order by id;
SQL

section "3. 月份分片 events：2026-01 ~ 2026-04 四个月，月份 → 节点"
mysql_run <<'SQL'
insert into events (id,event_type,created) values
 (1,'login','2026-01-15 10:00:00'),
 (2,'buy','2026-02-10 11:00:00'),
 (3,'login','2026-03-05 12:00:00'),
 (4,'buy','2026-04-01 13:00:00');
/*!mycat:showTopology{events}*/;
select * from events where created between '2026-01-01 00:00:00' and '2026-02-28 23:59:59' order by id;
SQL

section "4. 广播表 dict + 单表 user_info"
mysql_run <<'SQL'
insert into dict (id,name) values (1,'CN'),(2,'US');
insert into user_info (id,name,profile) values (1,'alice','vip');
select * from dict order by id;
select * from user_info;
/*!mycat:showTopology{dict}*/;
SQL

section "5. XA：跨 ds0/ds1 的 2PC 提交与回滚"
xa_run <<'SQL'
begin;
insert into orders (id,user_id,amount,status,created) values (201,1,1.00,'XA','2026-02-01 00:00:00');
insert into orders (id,user_id,amount,status,created) values (202,2,2.00,'XA','2026-02-02 00:00:00');
commit;
select id,status from orders where id in (201,202) order by id;
begin;
insert into orders (id,user_id,amount,status,created) values (203,1,3.00,'XA','2026-02-03 00:00:00');
insert into orders (id,user_id,amount,status,created) values (204,2,4.00,'XA','2026-02-04 00:00:00');
rollback;
select count(*) as xa_rolled_back from orders where id in (203,204);
SQL

section "6. 普通本地事务回滚"
mysql_run <<'SQL'
begin;
insert into orders (id,user_id,amount,status,created) values (205,1,5.00,'LOCAL','2026-02-05 00:00:00');
rollback;
select count(*) as local_rolled_back from orders where id = 205;
SQL

section "7. 物理落点核对（直接连两个 MySQL 分片）"
for s in 0 1; do
    echo "-- mysql-shard-$s"
    docker compose exec -T -e MYSQL_PWD=shard_root_pw "mysql-shard-$s" \
      mysql -uroot --table -e "select count(*) as dict_rows from db1_$s.dict; select count(*) as shard_orders from db1_$s.orders_$((s*2)); select count(*) as shard_orders_next from db1_$s.orders_$((s*2+1));"
done
docker compose exec -T mycat3-pro ls -la /opt/mycat3-pro/conf/xa

section "8. HTTP 状态"
curl -fsS "$HTTP/api/status" | python3 -c 'import json,sys;d=json.load(sys.stdin);print(json.dumps({k:d[k] for k in ("mode","queries","datasources")},ensure_ascii=False,indent=2))'
echo "演示结束；清理: docker compose down -v"
