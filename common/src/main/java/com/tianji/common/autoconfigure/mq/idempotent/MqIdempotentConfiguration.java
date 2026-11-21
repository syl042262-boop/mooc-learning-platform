package com.tianji.common.autoconfigure.mq.idempotent;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;

/**
 * 由需要数据库消费去重的监听器显式 Import，不影响其他服务的 MQ 消费配置。
 */
@Configuration(proxyBeanMethods = false)
public class MqIdempotentConfiguration {

    @Bean
    public MqIdempotentExecutor mqIdempotentExecutor(
            JdbcTemplate jdbcTemplate, PlatformTransactionManager transactionManager) {
        return new MqIdempotentExecutor(jdbcTemplate, transactionManager);
    }
}
