-- Mycat3 Pro 演示环境：物理库表初始化（从库 mysql-replica（与主库结构一致））
-- 逻辑库 db1 的物理分布：
--   db1_0: orders_0, orders_1, events_0, events_1, dict, user_info
--   db1_1: orders_2, orders_3, events_0, events_1, dict
CREATE DATABASE IF NOT EXISTS db1_0;
CREATE DATABASE IF NOT EXISTS db1_1;

USE db1_0;
CREATE TABLE IF NOT EXISTS orders_0 (id BIGINT PRIMARY KEY, name VARCHAR(64), created DATETIME);
CREATE TABLE IF NOT EXISTS orders_1 (id BIGINT PRIMARY KEY, name VARCHAR(64), created DATETIME);
CREATE TABLE IF NOT EXISTS events_0  (id BIGINT PRIMARY KEY, created DATETIME);
CREATE TABLE IF NOT EXISTS events_1  (id BIGINT PRIMARY KEY, created DATETIME);
CREATE TABLE IF NOT EXISTS dict      (id BIGINT PRIMARY KEY, name VARCHAR(64));
CREATE TABLE IF NOT EXISTS user_info (id BIGINT PRIMARY KEY, name VARCHAR(64));

USE db1_1;
CREATE TABLE IF NOT EXISTS orders_2 (id BIGINT PRIMARY KEY, name VARCHAR(64), created DATETIME);
CREATE TABLE IF NOT EXISTS orders_3 (id BIGINT PRIMARY KEY, name VARCHAR(64), created DATETIME);
CREATE TABLE IF NOT EXISTS events_0 (id BIGINT PRIMARY KEY, created DATETIME);
CREATE TABLE IF NOT EXISTS events_1 (id BIGINT PRIMARY KEY, created DATETIME);
CREATE TABLE IF NOT EXISTS dict     (id BIGINT PRIMARY KEY, name VARCHAR(64));

-- Pro 后端客户端使用 mysql_native_password
ALTER USER 'root'@'%' IDENTIFIED WITH mysql_native_password BY 'mysql_root_pw';
CREATE USER IF NOT EXISTS 'mycat'@'%' IDENTIFIED WITH mysql_native_password BY 'mysql_root_pw';
GRANT ALL PRIVILEGES ON *.* TO 'mycat'@'%' WITH GRANT OPTION;
FLUSH PRIVILEGES;
