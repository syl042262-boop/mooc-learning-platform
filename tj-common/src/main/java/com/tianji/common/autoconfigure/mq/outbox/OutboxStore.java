package com.tianji.common.autoconfigure.mq.outbox;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.LocalDateTime;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

/** 发布器的短事务，与业务事务分离；所有回写都校验本次租约令牌。 */
public class OutboxStore {
    private final JdbcTemplate jdbc;
    private final TransactionTemplate transaction;

    public OutboxStore(JdbcTemplate jdbc, PlatformTransactionManager manager) {
        this.jdbc = jdbc;
        transaction = new TransactionTemplate(manager);
        transaction.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
    }

    private LocalDateTime now() {
        return jdbc.queryForObject("SELECT CURRENT_TIMESTAMP", LocalDateTime.class);
    }

    public List<Long> readyIds(int batchSize) {
        return transaction.execute(status -> {
            LocalDateTime time = now();
            jdbc.update("UPDATE mq_outbox SET status='PENDING',lock_token=NULL,locked_until=NULL," +
                    "updated_at=? WHERE status='PUBLISHING' AND locked_until<=?", time, time);
            return jdbc.query("SELECT id FROM mq_outbox WHERE status='PENDING' AND available_at<=? " +
                            "AND next_retry_at<=? ORDER BY next_retry_at,id LIMIT ?",
                    (rs, row) -> rs.getLong(1), time, time, batchSize);
        });
    }

    public OutboxMessage claim(long id, String token, long leaseSeconds) {
        return transaction.execute(status -> {
            LocalDateTime time = now();
            int claimed = jdbc.update("UPDATE mq_outbox SET status='PUBLISHING',lock_token=?,locked_until=?," +
                            "attempts=attempts+1,updated_at=? WHERE id=? AND status='PENDING' " +
                            "AND available_at<=? AND next_retry_at<=?",
                    token, time.plusSeconds(leaseSeconds), time, id, time, time);
            if (claimed != 1) {
                return null;
            }
            return jdbc.queryForObject("SELECT * FROM mq_outbox WHERE id=?", (rs, row) -> {
                OutboxMessage message = new OutboxMessage();
                message.setId(rs.getLong("id"));
                message.setEventId(rs.getString("event_id"));
                message.setExchange(rs.getString("exchange_name"));
                message.setRoutingKey(rs.getString("routing_key"));
                message.setPayload(rs.getBytes("payload"));
                message.setHeaders(rs.getString("headers"));
                message.setContentType(rs.getString("content_type"));
                message.setContentEncoding(rs.getString("content_encoding"));
                message.setLockToken(rs.getString("lock_token"));
                message.setAttempts(rs.getInt("attempts"));
                return message;
            }, id);
        });
    }

    public boolean published(OutboxMessage message) {
        return Boolean.TRUE.equals(transaction.execute(status -> {
            LocalDateTime time = now();
            return jdbc.update("UPDATE mq_outbox SET status='PUBLISHED',published_at=?,updated_at=?," +
                            "lock_token=NULL,locked_until=NULL,last_error=NULL " +
                            "WHERE id=? AND status='PUBLISHING' AND lock_token=?",
                    time, time, message.getId(), message.getLockToken()) == 1;
        }));
    }

    public void retry(OutboxMessage message, Exception error, OutboxProperties properties) {
        transaction.executeWithoutResult(status -> {
            long multiplier = 1L << Math.min(Math.max(0, message.getAttempts() - 1), 16);
            long base = properties.getRetryBaseSeconds();
            long delay = base > properties.getRetryMaxSeconds() / multiplier
                    ? properties.getRetryMaxSeconds() : base * multiplier;
            delay = Math.min(properties.getRetryMaxSeconds(), delay);
            long jitter = ThreadLocalRandom.current().nextLong(Math.max(1, delay / 5 + 1));
            LocalDateTime time = now();
            String reason = error.getClass().getSimpleName() + ": " + String.valueOf(error.getMessage());
            if (reason.length() > 1000) {
                reason = reason.substring(0, 1000);
            }
            jdbc.update("UPDATE mq_outbox SET status='PENDING',next_retry_at=?,last_error=?,updated_at=?," +
                            "lock_token=NULL,locked_until=NULL WHERE id=? AND status='PUBLISHING' AND lock_token=?",
                    time.plusSeconds(delay + jitter), reason, time, message.getId(), message.getLockToken());
        });
    }
}
