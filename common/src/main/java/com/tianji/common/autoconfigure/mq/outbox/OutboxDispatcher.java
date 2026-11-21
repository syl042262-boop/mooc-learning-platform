package com.tianji.common.autoconfigure.mq.outbox;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.xxl.job.core.handler.annotation.XxlJob;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.util.UUID;

@Slf4j
public class OutboxDispatcher {
    private final OutboxStore store;
    private final ConfirmedMessageSender sender;
    private final ObjectMapper mapper;
    private final OutboxProperties properties;

    public OutboxDispatcher(OutboxStore store, ConfirmedMessageSender sender,
                            ObjectMapper mapper, OutboxProperties properties) {
        properties.validate();
        this.store = store;
        this.sender = sender;
        this.mapper = mapper;
        this.properties = properties;
    }

    @Scheduled(fixedDelayString = "${app.mq.outbox.poll-interval-ms:1000}",
            initialDelayString = "${app.mq.outbox.initial-delay-ms:10000}")
    public void poll() {
        if (properties.isPollEnabled()) {
            publishBatch();
        }
    }

    @XxlJob("publishOutbox")
    public void publishBatch() {
        if (TransactionSynchronizationManager.isActualTransactionActive()) {
            throw new IllegalStateException("Outbox 网络发布不能运行在业务事务中");
        }
        long started = System.nanoTime();
        for (Long id : store.readyIds(properties.getBatchSize())) {
            if ((System.nanoTime() - started) / 1_000_000 >= properties.getMaxBatchMillis()) {
                break;
            }
            OutboxMessage message = store.claim(id, UUID.randomUUID().toString(), properties.getLeaseSeconds());
            if (message == null) {
                continue;
            }
            try {
                sender.send(message.getExchange(), message.getRoutingKey(), message.toMessage(mapper),
                        message.getEventId() + ":" + message.getLockToken());
                if (!store.published(message)) {
                    log.warn("Outbox 发送确认后租约已变更，eventId={}", message.getEventId());
                }
            } catch (Exception e) {
                log.error("Outbox 发布失败，保留补发，eventId={}, attempts={}", message.getEventId(), message.getAttempts(), e);
                try {
                    store.retry(message, e, properties);
                } catch (Exception persistenceFailure) {
                    // 回写不可用时仍保留 PUBLISHING，由过期租约恢复。
                    log.error("Outbox 重试状态回写失败，等待租约恢复，eventId={}", message.getEventId(), persistenceFailure);
                }
                if (e instanceof InterruptedException) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }
    }
}
