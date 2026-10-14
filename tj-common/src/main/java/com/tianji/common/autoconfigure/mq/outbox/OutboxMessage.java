package com.tianji.common.autoconfigure.mq.outbox;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Data;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageDeliveryMode;
import org.springframework.amqp.core.MessageProperties;

import java.io.IOException;
import java.util.Map;

@Data
public class OutboxMessage {
    private long id;
    private String eventId;
    private String exchange;
    private String routingKey;
    private byte[] payload;
    private String headers;
    private String contentType;
    private String contentEncoding;
    private String lockToken;
    private int attempts;

    public Message toMessage(ObjectMapper mapper) throws IOException {
        MessageProperties properties = new MessageProperties();
        Map<String, Object> restored = mapper.readValue(headers, new TypeReference<Map<String, Object>>() { });
        properties.getHeaders().putAll(restored);
        properties.setMessageId(eventId);
        properties.setContentType(contentType);
        properties.setContentEncoding(contentEncoding);
        properties.setDeliveryMode(MessageDeliveryMode.PERSISTENT);
        return new Message(payload.clone(), properties);
    }
}
