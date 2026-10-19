SET NAMES utf8mb4;
USE demo;

CREATE TABLE t_product (
    id INT PRIMARY KEY,
    name VARCHAR(64) NOT NULL,
    price DECIMAL(10,2) NOT NULL,
    category VARCHAR(32) NOT NULL,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

INSERT INTO t_product (id, name, price, category) VALUES
  (1, '机械键盘', 399.00, 'keyboard'),
  (2, '无线鼠标', 129.00, 'mouse'),
  (3, '27寸显示器', 1899.00, 'monitor'),
  (4, 'USB-C 扩展坞', 499.00, 'dock'),
  (5, '人体工学椅', 2599.00, 'chair'),
  (6, '降噪耳机', 999.00, 'audio');

CREATE TABLE t_order (
    id BIGINT PRIMARY KEY,
    product_id INT NOT NULL,
    qty INT NOT NULL,
    created DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE t_audit (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    action VARCHAR(32) NOT NULL,
    detail VARCHAR(255) NOT NULL,
    created DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
