package com.tianji.trade.config;

import com.tianji.common.autoconfigure.mq.outbox.MqOutboxConfiguration;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@Configuration(proxyBeanMethods = false)
@Import(MqOutboxConfiguration.class)
public class OutboxConfig {
}
