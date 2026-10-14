package com.tianji.common.autoconfigure.mq.outbox;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.transaction.PlatformTransactionManager;

/** 仅由已接入的服务显式 Import，不加入公共 spring.factories。 */
@Configuration(proxyBeanMethods = false)
@EnableScheduling
@EnableConfigurationProperties(OutboxProperties.class)
public class MqOutboxConfiguration {
    @Bean
    public OutboxService outboxService(JdbcTemplate jdbc, MessageConverter converter, ObjectMapper mapper) {
        return new OutboxService(jdbc, converter, mapper);
    }

    @Bean
    public OutboxStore outboxStore(JdbcTemplate jdbc, PlatformTransactionManager manager) {
        return new OutboxStore(jdbc, manager);
    }

    @Bean(destroyMethod = "close")
    public ConfirmedRabbitSender confirmedRabbitSender(ConnectionFactory factory, OutboxProperties properties) {
        properties.validate();
        return new ConfirmedRabbitSender(factory, properties.getConfirmTimeoutMillis());
    }

    @Bean
    public OutboxDispatcher outboxDispatcher(OutboxStore store, ConfirmedRabbitSender sender,
                                              ObjectMapper mapper, OutboxProperties properties) {
        return new OutboxDispatcher(store, sender, mapper, properties);
    }
}
