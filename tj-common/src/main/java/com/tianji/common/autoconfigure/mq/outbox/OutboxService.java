package com.tianji.common.autoconfigure.mq.outbox;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.MDC;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.dao.ConcurrencyFailureException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;

import static com.tianji.common.constants.Constant.REQUEST_ID_HEADER;

/** 只保存事件；必须参与业务使用的同一数据源事务，不在这里发送网络消息。 */
public class OutboxService {
    private final JdbcTemplate jdbc;
    private final MessageConverter converter;
    private final ObjectMapper mapper;

    public OutboxService(JdbcTemplate jdbc, MessageConverter converter, ObjectMapper mapper) {
        this.jdbc = jdbc;
        this.converter = converter;
        this.mapper = mapper;
    }

    public String record(String eventType, String businessKey, String exchange, String routingKey, Object body) {
        return recordAt(eventType, businessKey, exchange, routingKey, body, null);
    }

    public String recordAt(String eventType, String businessKey, String exchange, String routingKey,
                           Object body, LocalDateTime availableAt) {
        requireBusinessTransaction();
        checkKey(eventType, 64);
        checkKey(businessKey, 192);
        checkKey(exchange, 128);
        checkKey(routingKey, 128);
        if (body == null) {
            throw new IllegalArgumentException("Outbox 消息正文不能为空");
        }
        String eventId = UUID.randomUUID().toString();
        MessageProperties properties = new MessageProperties();
        properties.setMessageId(eventId);
        String requestId = MDC.get(REQUEST_ID_HEADER);
        properties.setHeader(REQUEST_ID_HEADER, requestId == null ? eventId : requestId);
        properties.setHeader("eventId", eventId);
        Message message = converter.toMessage(body, properties);
        LocalDateTime now = jdbc.queryForObject("SELECT CURRENT_TIMESTAMP", LocalDateTime.class);
        String headers;
        try {
            headers = mapper.writeValueAsString(message.getMessageProperties().getHeaders());
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Outbox 消息头不能序列化", e);
        }
        // 不吞唯一键异常：业务自身的状态/请求唯一性负责识别重放，身份冲突必须回滚。
        jdbc.update("INSERT INTO mq_outbox (event_id,event_type,biz_key,exchange_name,routing_key," +
                        "payload,headers,content_type,content_encoding,status,available_at,next_retry_at," +
                        "attempts,created_at,updated_at) VALUES (?,?,?,?,?,?,?,?,?,'PENDING',?,?,0,?,?)",
                eventId, eventType, businessKey, exchange, routingKey, message.getBody(), headers,
                message.getMessageProperties().getContentType(), message.getMessageProperties().getContentEncoding(),
                availableAt == null ? now : availableAt, now, now, now);
        return eventId;
    }

    /** 业务发生多次有效迁移时，为每次迁移生成版本；序列修改也随业务回滚。 */
    public long nextVersion(String aggregateKey) {
        requireBusinessTransaction();
        checkKey(aggregateKey, 128);
        List<Long> versions = jdbc.query("SELECT version FROM mq_outbox_sequence WHERE aggregate_key = ?",
                (rs, row) -> rs.getLong(1), aggregateKey);
        if (versions.isEmpty()) {
            jdbc.update("INSERT INTO mq_outbox_sequence (aggregate_key,version) VALUES (?,1)", aggregateKey);
            return 1;
        }
        long previous = versions.get(0);
        if (previous == Long.MAX_VALUE || jdbc.update("UPDATE mq_outbox_sequence SET version = ? " +
                "WHERE aggregate_key = ? AND version = ?", previous + 1, aggregateKey, previous) != 1) {
            throw new ConcurrencyFailureException("业务事件版本并发冲突，请在新事务重试");
        }
        return previous + 1;
    }

    private void requireBusinessTransaction() {
        if (!TransactionSynchronizationManager.isActualTransactionActive()
                || !TransactionSynchronizationManager.hasResource(jdbc.getDataSource())) {
            throw new IllegalStateException("Outbox 必须在同一数据源的业务事务中写入");
        }
    }

    private static void checkKey(String value, int maximum) {
        if (value == null || value.isEmpty() || value.length() > maximum
                || value.chars().anyMatch(c -> c < 33 || c > 126)) {
            throw new IllegalArgumentException("Outbox 键必须是有长度限制的非空 ASCII 字符串");
        }
    }
}
