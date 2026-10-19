#!/usr/bin/env bash
# Mycat3 Pro 纯代理模式一键演示
# 用法: ./demo.sh
# 可选: RESET_CACHE=1 ./demo.sh  # 先删除两个演示缓存，观察冷启动效果
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")"
COMPOSE=(docker compose)
MYSQL=(docker compose exec -T -e MYSQL_PWD=123456 mysql mysql --default-character-set=utf8mb4 -hmycat3-pro -P8066 -uroot -Ddemo --table)
HTTP="http://127.0.0.1:19066"

section() { printf '\n\033[1;36m===== %s =====\033[0m\n' "$*"; }
mysql_run() { "${MYSQL[@]}" "$@"; }

section "启动 MySQL 与 Mycat3 Pro"
"${COMPOSE[@]}" up -d
for _ in $(seq 1 60); do
    if curl -fsS "$HTTP/-/healthy" >/dev/null 2>&1; then break; fi
    sleep 1
done
curl -fsS "$HTTP/-/healthy"
"${COMPOSE[@]}" ps

if [[ "${RESET_CACHE:-0}" == "1" ]]; then
    section "重置演示缓存"
    mysql_run <<'SQL'
/*!mycat:dropSqlCache={"name":"product_by_id"}*/;
/*!mycat:dropSqlCache={"name":"product_catalog"}*/;
/*!mycat:dropSqlCache={"name":"category_stats"}*/;
SQL
fi

section "1. 代理模式基本查询（SQL 原样透传到 ds0）"
mysql_run <<'SQL'
select id, name, price, category from t_product order by id;
show @@datasource;
SQL

section "2. EXACT 缓存：product_catalog 启动时已预读"
mysql_run <<'SQL'
select id, name, price, category from t_product order by id;
select id, name, price, category from t_product order by id;
select category, count(*) as cnt from t_product group by category order by category;
select category, count(*) as cnt from t_product group by category order by category;
/*!mycat:showSqlCaches*/;
SQL

section "3. PARAMETERIZED 缓存：每个参数组合独立结果"
mysql_run <<'SQL'
select id, name, price, category from t_product where id = 1;
select id, name, price, category from t_product where id = 1;
select id, name, price, category from t_product where id = 2;
/*!mycat:showSqlCaches*/;
SQL

section "4. 写后失效：UPDATE 清理相关缓存，下一次查询回填"
mysql_run <<'SQL'
update t_product set price = 409.00 where id = 1;
/*!mycat:showSqlCaches*/;
select id, name, price, category from t_product order by id;
select id, name, price, category from t_product where id = 1;
/*!mycat:showSqlCaches*/;
SQL

section "5. SQL 拦截：DROP TABLE / 无 WHERE 的 DELETE"
set +e
out=$(mysql_run -e "drop table t_product" 2>&1); rc=$?
set -e
printf 'DROP exit=%s\n%s\n' "$rc" "$out"
[[ $rc -ne 0 ]] || { echo "ERROR: DROP TABLE 未被拦截" >&2; exit 1; }

set +e
out=$(mysql_run -e "delete from t_product" 2>&1); rc=$?
set -e
printf '\nDELETE(no WHERE) exit=%s\n%s\n' "$rc" "$out"
[[ $rc -ne 0 ]] || { echo "ERROR: 无 WHERE DELETE 未被拦截" >&2; exit 1; }

mysql_run -e "delete from t_product where id = 999" >/dev/null
echo "带 WHERE 的 DELETE 正常放行（影响 0 行）"
mysql_run -e "select count(*) as product_rows from t_product"

section "6. 慢 SQL：运行时把阈值调到 10ms，执行 SLEEP(0.2)"
mysql_run <<'SQL'
/*!mycat:setSqlTimeFilter{"value":10}*/;
select sleep(0.2) as slept;
/*!mycat:showSlowSql*/;
/*!mycat:getSqlTimeFilter*/;
SQL
mysql_run -e '/*!mycat:setSqlTimeFilter{"value":1000}*/;'

section "7. 会话级固定连接：用户变量、显式事务"
mysql_run <<'SQL'
set @tenant = 'acme';
select @tenant as tenant_var;
begin;
insert into t_order (id, product_id, qty) values (90001, 1, 1);
rollback;
select count(*) as order_rows_after_rollback from t_order;
SQL

section "8. 重新预热缓存（便于观察 HTTP 状态）"
mysql_run -e "select id, name, price, category from t_product order by id; select category, count(*) as cnt from t_product group by category order by category; select id, name, price, category from t_product where id = 1;" >/dev/null

section "9. HTTP 监控与缓存状态"
echo "-- /metrics（节选）"
curl -fsS "$HTTP/metrics" | grep -E 'mycat3_(queries_total|query_errors_total)|mycat3_pool_(idle|available)' | head -20 || true
echo "-- /api/status（节选）"
curl -fsS "$HTTP/api/status" | python3 -c 'import json,sys; d=json.load(sys.stdin); print(json.dumps({k:d[k] for k in ("version","mode","queries","datasources")}, ensure_ascii=False, indent=2))'
echo "-- /api/sqlcaches（节选）"
curl -fsS "$HTTP/api/sqlcaches" | python3 -c 'import json,sys; print(json.dumps(json.load(sys.stdin), ensure_ascii=False, indent=2)[:2000])'

section "演示结束"
echo "清理: docker compose down -v"
echo "缓存文件: conf/cache/sqlcache/（删除后重建可观察冷缓存）"
