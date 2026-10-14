-- 手工选择 tj_course、tj_trade、tj_remark，分别执行本脚本；应用不会自动建表。
-- CREATE IF NOT EXISTS 不迁移旧表；已有同名表时需核对字段和唯一索引。
CREATE TABLE IF NOT EXISTS mq_outbox (
    id BIGINT NOT NULL AUTO_INCREMENT,
    event_id VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    event_type VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    biz_key VARCHAR(192) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    exchange_name VARCHAR(128) NOT NULL,
    routing_key VARCHAR(128) NOT NULL,
    payload LONGBLOB NOT NULL,
    headers LONGTEXT NOT NULL,
    content_type VARCHAR(128) NOT NULL,
    content_encoding VARCHAR(32) NULL,
    status VARCHAR(16) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    available_at DATETIME(3) NOT NULL,
    next_retry_at DATETIME(3) NOT NULL,
    attempts INT NOT NULL DEFAULT 0,
    lock_token VARCHAR(64) NULL,
    locked_until DATETIME(3) NULL,
    last_error VARCHAR(1024) NULL,
    created_at DATETIME(3) NOT NULL,
    updated_at DATETIME(3) NOT NULL,
    published_at DATETIME(3) NULL,
    PRIMARY KEY (id),
    UNIQUE KEY uk_outbox_event (event_id),
    UNIQUE KEY uk_outbox_business (event_type, biz_key),
    KEY idx_outbox_ready (status, next_retry_at, id),
    KEY idx_outbox_lease (status, locked_until, id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 课程重复上下架等操作使用；删除业务对象后仍保留版本身份。
CREATE TABLE IF NOT EXISTS mq_outbox_sequence (
    aggregate_key VARCHAR(128) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    version BIGINT NOT NULL,
    PRIMARY KEY (aggregate_key)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
