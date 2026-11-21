-- 在部署新版消费者之前执行。只涉及当前存在数据库去重消费者的两个服务。
-- 不自动执行此脚本，避免启动时隐式修改业务库。
CREATE TABLE IF NOT EXISTS learning.mq_consume_log (
    consumer_id VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL COMMENT '稳定的逻辑消费者名称',
    event_key VARCHAR(192) CHARACTER SET ascii COLLATE ascii_bin NOT NULL COMMENT '业务事件键或旧消息ID',
    message_id VARCHAR(255) NULL COMMENT '原始AMQP消息ID',
    payload LONGBLOB NOT NULL COMMENT '原始消息体',
    created_at DATETIME(3) NOT NULL COMMENT '消费事务开始时间，仅随业务成功一起提交',
    PRIMARY KEY (consumer_id, event_key),
    KEY idx_created_at (created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='MQ成功消费记录';

CREATE TABLE IF NOT EXISTS promotion.mq_consume_log (
    consumer_id VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL COMMENT '稳定的逻辑消费者名称',
    event_key VARCHAR(192) CHARACTER SET ascii COLLATE ascii_bin NOT NULL COMMENT '业务事件键或旧消息ID',
    message_id VARCHAR(255) NULL COMMENT '原始AMQP消息ID',
    payload LONGBLOB NOT NULL COMMENT '原始消息体',
    created_at DATETIME(3) NOT NULL COMMENT '消费事务开始时间，仅随业务成功一起提交',
    PRIMARY KEY (consumer_id, event_key),
    KEY idx_created_at (created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='MQ成功消费记录';
