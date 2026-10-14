package com.tianji.common.autoconfigure.mq.outbox;

import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.core.ReturnedMessage;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;

import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class ConfirmedRabbitSenderTest {
    private final Message message = new Message(new byte[]{1}, new MessageProperties());

    @Test
    void brokerAckWithoutReturnIsAccepted() throws Exception {
        RabbitTemplate template = mock(RabbitTemplate.class);
        doAnswer(call -> {
            CorrelationData correlation = call.getArgument(3);
            assertEquals("event:attempt", correlation.getId());
            correlation.getFuture().set(new CorrelationData.Confirm(true, null));
            return null;
        }).when(template).send(anyString(), anyString(), any(Message.class), any(CorrelationData.class));
        new ConfirmedRabbitSender(template, 100).send("exchange", "key", message, "event:attempt");
    }

    @Test
    void unroutableMessageIsRejectedEvenWhenItGetsAnAck() {
        RabbitTemplate template = mock(RabbitTemplate.class);
        doAnswer(call -> {
            CorrelationData correlation = call.getArgument(3);
            correlation.setReturned(new ReturnedMessage(message, 312, "NO_ROUTE", "exchange", "key"));
            correlation.getFuture().set(new CorrelationData.Confirm(true, null));
            return null;
        }).when(template).send(anyString(), anyString(), any(Message.class), any(CorrelationData.class));
        assertThrows(IllegalStateException.class,
                () -> new ConfirmedRabbitSender(template, 100).send("exchange", "key", message, "attempt"));
    }

    @Test
    void nackIsRejected() {
        RabbitTemplate template = mock(RabbitTemplate.class);
        doAnswer(call -> {
            CorrelationData correlation = call.getArgument(3);
            correlation.getFuture().set(new CorrelationData.Confirm(false, "失败"));
            return null;
        }).when(template).send(anyString(), anyString(), any(Message.class), any(CorrelationData.class));
        assertThrows(IllegalStateException.class,
                () -> new ConfirmedRabbitSender(template, 100).send("exchange", "key", message, "attempt"));
    }

    @Test
    void missingConfirmationTimesOutInsteadOfBeingMarkedSuccessful() {
        assertThrows(TimeoutException.class, () -> new ConfirmedRabbitSender(mock(RabbitTemplate.class), 1)
                .send("exchange", "key", message, "attempt"));
    }

    @Test
    void invalidConnectionConfigurationFailsBeforeAnyBusinessCanPublish() {
        CachingConnectionFactory factory = new CachingConnectionFactory();
        try {
            assertThrows(IllegalStateException.class, () -> new ConfirmedRabbitSender(factory, 100));
            factory.setPublisherConfirmType(CachingConnectionFactory.ConfirmType.CORRELATED);
            assertThrows(IllegalStateException.class, () -> new ConfirmedRabbitSender(factory, 100));
            factory.setPublisherReturns(true);
            assertDoesNotThrow(() -> new ConfirmedRabbitSender(factory, 100).close());
        } finally { factory.destroy(); }
    }
}
