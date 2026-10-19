CREATE DATABASE IF NOT EXISTS demo;
USE demo;
CREATE TABLE IF NOT EXISTS t_proxy (id BIGINT PRIMARY KEY, name VARCHAR(64), created DATETIME);
INSERT INTO t_proxy (id, name, created) VALUES (1, 'hello-proxy', NOW()) ON DUPLICATE KEY UPDATE name = VALUES(name);
