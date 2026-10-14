package com.tianji.remark.service.impl;

import com.baomidou.mybatisplus.core.MybatisConfiguration;
import com.baomidou.mybatisplus.extension.spring.MybatisSqlSessionFactoryBean;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.tianji.api.dto.remark.LikedTimesDTO;
import com.tianji.common.autoconfigure.mq.outbox.OutboxService;
import com.tianji.remark.mapper.LikedStatMapper;
import org.apache.ibatis.session.SqlSessionFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mybatis.spring.SqlSessionTemplate;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.transaction.annotation.EnableTransactionManagement;

import javax.sql.DataSource;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class LikedStatOutboxTest {
    private AnnotationConfigApplicationContext context;
    private JdbcTemplate jdbc;
    private LikedStatServiceImpl service;

    @BeforeEach
    void setUp() throws Exception {
        context = new AnnotationConfigApplicationContext(Config.class);
        jdbc = context.getBean(JdbcTemplate.class);
        service = context.getBean(LikedStatServiceImpl.class);
        String ddl = Files.readString(Path.of("..", "docs", "sql", "mq-outbox.sql"))
                .replace("CHARACTER SET ascii COLLATE ascii_bin", "")
                .replace("ENGINE=InnoDB DEFAULT CHARSET=utf8mb4", "");
        for (String sql : ddl.split(";")) if (!sql.trim().isEmpty()) jdbc.execute(sql);
        jdbc.execute("CREATE TABLE liked_stat (id BIGINT PRIMARY KEY,liked_times INT,biz_id BIGINT," +
                "user_id BIGINT,biz_type VARCHAR(50),create_time TIMESTAMP,update_time TIMESTAMP)");
    }

    @AfterEach
    void close() { context.close(); }

    @Test
    void statisticsAndTheCompleteSnapshotCommitTogether() {
        service.updateLikedTimes("QA", Arrays.asList(LikedTimesDTO.of(1L, 10), LikedTimesDTO.of(2L, 20)));
        assertEquals(2, jdbc.queryForObject("SELECT COUNT(*) FROM liked_stat", Integer.class));
        assertEquals(1, jdbc.queryForObject("SELECT COUNT(*) FROM mq_outbox", Integer.class));
        assertEquals("PENDING", jdbc.queryForObject("SELECT status FROM mq_outbox", String.class));
        assertEquals("QA.times.changed", jdbc.queryForObject("SELECT routing_key FROM mq_outbox", String.class));
    }

    @Test
    void failedMessagePersistenceRollsBackInsertedStatistics() {
        jdbc.execute("DROP TABLE mq_outbox");
        assertThrows(org.springframework.dao.DataAccessException.class,
                () -> service.updateLikedTimes("QA", Collections.singletonList(LikedTimesDTO.of(1L, 10))));
        assertEquals(0, jdbc.queryForObject("SELECT COUNT(*) FROM liked_stat", Integer.class));
    }

    @Test
    void failedMessagePersistenceRollsBackUpdatedStatisticsIncludingCancellation() {
        service.updateLikedTimes("QA", Collections.singletonList(LikedTimesDTO.of(1L, 10)));
        jdbc.execute("DROP TABLE mq_outbox");
        assertThrows(org.springframework.dao.DataAccessException.class,
                () -> service.updateLikedTimes("QA", Collections.singletonList(LikedTimesDTO.of(1L, 9))));
        assertEquals(10, jdbc.queryForObject("SELECT liked_times FROM liked_stat", Integer.class));
    }

    @Configuration(proxyBeanMethods = false)
    @EnableTransactionManagement(proxyTargetClass = true)
    static class Config {
        @Bean DataSource dataSource() { return new DriverManagerDataSource("jdbc:h2:mem:" + UUID.randomUUID()
                + ";MODE=MySQL;DB_CLOSE_DELAY=-1", "sa", ""); }
        @Bean JdbcTemplate jdbc(DataSource ds) { return new JdbcTemplate(ds); }
        @Bean DataSourceTransactionManager transactionManager(DataSource ds) { return new DataSourceTransactionManager(ds); }
        @Bean SqlSessionFactory factory(DataSource ds) throws Exception {
            MybatisConfiguration config = new MybatisConfiguration();
            config.addMapper(LikedStatMapper.class);
            MybatisSqlSessionFactoryBean factory = new MybatisSqlSessionFactoryBean();
            factory.setDataSource(ds);
            factory.setConfiguration(config);
            return factory.getObject();
        }
        @Bean LikedStatMapper mapper(SqlSessionFactory factory) { return new SqlSessionTemplate(factory).getMapper(LikedStatMapper.class); }
        @Bean OutboxService outbox(JdbcTemplate jdbc) {
            ObjectMapper mapper = new ObjectMapper().findAndRegisterModules();
            return new OutboxService(jdbc, new Jackson2JsonMessageConverter(mapper), mapper);
        }
        @Bean LikedStatServiceImpl service(OutboxService outbox) { return new LikedStatServiceImpl(outbox); }
    }
}
