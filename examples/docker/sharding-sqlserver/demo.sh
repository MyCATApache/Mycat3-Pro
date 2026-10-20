#!/usr/bin/env bash
# Mycat3 Pro 分库分表（SQL Server 2022 后端）一键演示
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"

# 让容器内非 root 用户（UID 1000）能写配置目录（DDL 自动化会热更新配置）。
# GitHub Actions runner 等宿主 UID 不是 1000 时，绑定挂载默认不可写。
chmod -R a+w conf

COMPOSE=(docker compose)
MYSQL=(docker compose run --rm -T mysql-client --default-character-set=utf8mb4 -hmycat3-pro -P8066 -uroot -Ddb1 --table)
XA=(docker compose run --rm -T mysql-client --default-character-set=utf8mb4 -hmycat3-pro -P8066 -uxa -Ddb1 --table)
SQLCMD=(docker compose run --rm -T sqlcmd-client -S sqlserver -U sa -P 'SqlServer_Demo1!' -C -W -s '|' -w 240)
HTTP="http://127.0.0.1:39068"

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

section "启动 SQL Server 2022 + 初始化 db1/schema + Mycat3 Pro"
"${COMPOSE[@]}" up -d
for _ in $(seq 1 180); do
    if curl -fsS "$HTTP/-/healthy" >/dev/null 2>&1; then break; fi
    sleep 2
done
curl -fsS "$HTTP/-/healthy"
echo "等待 SQL Server 建库/物理表..."
for _ in $(seq 1 60); do
    if mysql_quiet -e "select count(*) from orders"; then break; fi
    sleep 2
done

section "清理旧演示数据"
mysql_run <<'SQL'
delete from orders where id between 1 and 9999;
delete from tenant_order where id between 1 and 9999;
delete from dict where id between 1 and 9999;
delete from user_info where id between 1 and 9999;
SQL

section "1. 取模分片 orders（T-SQL / SQL Server schema）"
mysql_run <<'SQL'
insert into orders (id,user_id,amount,status,created) values
 (1,101,10.00,'NEW', '2026-01-01 10:00:00'),
 (2,102,20.00,'PAID','2026-01-02 10:00:00'),
 (3,103,30.00,'NEW', '2026-01-03 10:00:00'),
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

section "3. 事务：普通回滚 + SQL Server XA 明确拒绝"
mysql_run <<'SQL'
begin;
insert into orders (id,user_id,amount,status,created) values (201,1,1.00,'TX','2026-02-01 00:00:00');
rollback;
select count(*) as rollback_rows from orders where id = 201;
SQL

set +e
out=$("${XA[@]}" -e "begin; insert into orders (id,user_id,amount,status,created) values (202,1,2.00,'XA','2026-02-02 00:00:00'); commit;" 2>&1)
rc=$?
set -e
printf 'XA exit=%s\n%s\n' "$rc" "$out" | grep -vE '^ ?Container |^$' | head -8
echo "（SQL Server 无 XA；跨分片原子性需 MSDTC/业务补偿，详见 README）"

section "4. SQL Server 物理落点核对"
"${SQLCMD[@]}" -d db1 -Q "select count(*) as dict_rows from db1_0.dict; select count(*) as orders_0 from db1_0.orders_0; select count(*) as orders_1 from db1_0.orders_1; select count(*) as orders_2 from db1_1.orders_2; select count(*) as orders_3 from db1_1.orders_3;"

section "5. 物理 DDL 自动化（逻辑 CREATE TABLE → T-SQL 建 schema/表/索引）"
mysql_run <<'SQL'
drop table if exists ddl_demo;
create table ddl_demo (
  id bigint primary key,
  name varchar(64) not null,
  created datetime,
  key idx_ddl_name (name)
);
insert into ddl_demo (id,name,created) values (1,'auto-ddl','2026-03-01 00:00:00');
select * from ddl_demo;
SQL
"${SQLCMD[@]}" -d db1 -Q "select count(*) as ddl_table from sys.objects where object_id = OBJECT_ID(N'db1.ddl_demo') and type = 'U'; select count(*) as ddl_index from sys.indexes where object_id = OBJECT_ID(N'db1.ddl_demo') and name = 'idx_ddl_name';"
mysql_run -e "drop table ddl_demo;"

section "6. HTTP 状态"
curl -fsS "$HTTP/api/status" | python3 -c 'import json,sys;d=json.load(sys.stdin);print(json.dumps({k:d[k] for k in ("mode","queries","datasources")},ensure_ascii=False,indent=2))'
echo "演示结束；清理: docker compose down -v"
