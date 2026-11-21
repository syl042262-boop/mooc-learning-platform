package com.tianji.common.autoconfigure.mq.outbox;

import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;

import java.util.concurrent.TimeUnit;

public class ConfirmedRabbitSender implements ConfirmedMessageSender, AutoCloseable {
    private final RabbitTemplate template;
    private final long timeoutMillis;

    public ConfirmedRabbitSender(ConnectionFactory factory, long timeoutMillis) {
        if (!factory.isPublisherConfirms() || factory.isSimplePublisherConfirms() || !factory.isPublisherReturns()) {
            throw new IllegalStateException("Outbox 需要 correlated publisher confirms 和 publisher returns");
        }
        // 不声明第二个 RabbitTemplate Bean，避免改变 Boot 的默认模板与现有 helper。
        template = new RabbitTemplate(factory);
        template.setMandatory(true);
        this.timeoutMillis = timeoutMillis;
    }

    // 独立于真实 broker 测试确认/退回/超时的判定。
    ConfirmedRabbitSender(RabbitTemplate template, long timeoutMillis) {
        this.template = template;
        this.timeoutMillis = timeoutMillis;
    }

    @Override
    public void send(String exchange, String routingKey, Message message, String attemptId) throws Exception {
        CorrelationData correlation = new CorrelationData(attemptId);
        template.send(exchange, routingKey, message, correlation);
        CorrelationData.Confirm confirm = correlation.getFuture().get(timeoutMillis, TimeUnit.MILLISECONDS);
        if (!confirm.isAck() || correlation.getReturned() != null) {
            throw new IllegalStateException(correlation.getReturned() != null
                    ? "Outbox 消息无法路由: " + correlation.getReturned().getReplyText()
                    : "Outbox 发布 NACK: " + confirm.getReason());
        }
    }

    @Override
    public void close() {
        template.destroy();
    }
}
