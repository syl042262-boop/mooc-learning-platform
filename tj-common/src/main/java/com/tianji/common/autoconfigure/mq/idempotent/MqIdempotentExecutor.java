package com.tianji.common.autoconfigure.mq.idempotent;

import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.Message;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.util.Assert;

/**
 * 消费记录和业务数据库写入共用一笔本地事务。只有已提交的记录才代表消费成功。
 * 业务回调必须使用相同数据源和 REQUIRED 事务，不能依靠此组件保护 Redis/MQ 等外部写入。
 */
@Slf4j
public class MqIdempotentExecutor {

    private final JdbcTemplate jdbcTemplate;
    private final TransactionTemplate transactionTemplate;

    public MqIdempotentExecutor(JdbcTemplate jdbcTemplate, PlatformTransactionManager transactionManager) {
        this.jdbcTemplate = jdbcTemplate;
        this.transactionTemplate = new TransactionTemplate(transactionManager);
    }

    /**
     * @return true 表示本次完成处理，false 表示此前已经成功处理。
     */
    public boolean execute(String consumerId, String eventKey, Message message, Runnable business) {
        validateKey(consumerId, 64, "consumerId");
        validateKey(eventKey, 192, "eventKey");
        Assert.notNull(message, "原始 MQ 消息不能为空");
        Assert.notNull(business, "业务回调不能为空");
        // 入口必须在监听器的事务外，避免捕获重复异常后使调用者的事务处于 rollback-only。
        Assert.state(!TransactionSynchronizationManager.isActualTransactionActive(),
                "幂等执行器必须作为消费事务入口，不能在已有事务中调用");
        try {
            transactionTemplate.executeWithoutResult(status -> {
                try {
                    jdbcTemplate.update("INSERT INTO mq_consume_log " +
                                    "(consumer_id, event_key, message_id, payload, created_at) " +
                                    "VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)",
                            consumerId, eventKey, message.getMessageProperties().getMessageId(), message.getBody());
                } catch (DuplicateKeyException e) {
                    // 仅识别消费记录 INSERT 的唯一键冲突，不能吞掉业务表的约束错误。
                    throw new AlreadyConsumedException(e);
                }
                business.run();
            });
            return true;
        } catch (AlreadyConsumedException e) {
            // TransactionTemplate 已结束当前事务；并发 INSERT 会等待另一个事务提交或回滚。
            log.debug("跳过已完成的 MQ 消费，consumerId: {}, eventKey: {}", consumerId, eventKey);
            return false;
        }
    }

    /**
     * 适用于暂时没有业务事件身份的旧消息，只保证同一 messageId 的重投幂等。
     */
    public static String messageKey(Message message) {
        Assert.notNull(message, "原始 MQ 消息不能为空");
        String messageId = message.getMessageProperties().getMessageId();
        Assert.hasText(messageId, "MQ 消息缺少 messageId，不能安全生成消费唯一键");
        return "message:" + messageId;
    }

    private static void validateKey(String key, int maxLength, String name) {
        Assert.hasText(key, name + "不能为空");
        Assert.isTrue(key.length() <= maxLength && key.chars().allMatch(c -> c < 128),
                name + "必须为不超过" + maxLength + "字符的 ASCII 标识");
    }

    private static class AlreadyConsumedException extends RuntimeException {
        private AlreadyConsumedException(DuplicateKeyException cause) {
            super(cause);
        }
    }
}
