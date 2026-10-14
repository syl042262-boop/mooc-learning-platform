-- 只在 tj_trade 执行。先执行 mq-outbox.sql，再启动已迁移的交易服务。
CREATE TABLE IF NOT EXISTS pay_query_task (
    order_id BIGINT NOT NULL,
    generation BIGINT NOT NULL,
    round INT NOT NULL,
    status VARCHAR(16) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    updated_at DATETIME(3) NOT NULL,
    PRIMARY KEY (order_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
