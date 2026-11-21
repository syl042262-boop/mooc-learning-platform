package com.tianji.common.autoconfigure.mq.outbox;

import org.springframework.amqp.core.Message;

@FunctionalInterface
public interface ConfirmedMessageSender {
    /** 仅在 broker ACK 且没有路由退回时正常返回；结果未知必须抛异常。 */
    void send(String exchange, String routingKey, Message message, String attemptId) throws Exception;
}
