package com.tianji.trade.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.tianji.common.autoconfigure.mq.outbox.OutboxService;
import com.tianji.trade.config.TradeProperties;
import com.tianji.trade.domain.dto.OrderDelayQueryDTO;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class PayQuerySchedulerTest {
    private JdbcTemplate jdbc;
    private PayQueryScheduler scheduler;
    private final ObjectMapper mapper = new ObjectMapper();

    @BeforeEach
    void setUp() throws Exception {
        DriverManagerDataSource ds = new DriverManagerDataSource("jdbc:h2:mem:" + UUID.randomUUID()
                + ";MODE=MySQL;DB_CLOSE_DELAY=-1;LOCK_TIMEOUT=5000", "sa", "");
        jdbc = new JdbcTemplate(ds);
        for (String file : new String[]{"mq-outbox.sql", "pay-query-task.sql"}) {
            String ddl = Files.readString(Path.of("..", "docs", "sql", file))
                    .replace("CHARACTER SET ascii COLLATE ascii_bin", "")
                    .replace("ENGINE=InnoDB DEFAULT CHARSET=utf8mb4", "");
            for (String sql : ddl.split(";")) if (!sql.trim().isEmpty()) jdbc.execute(sql);
        }
        jdbc.execute("CREATE TABLE `order` (id BIGINT PRIMARY KEY,status INT,deleted INT,create_time TIMESTAMP)");
        scheduler = new PayQueryScheduler(jdbc,
                new OutboxService(jdbc, new Jackson2JsonMessageConverter(mapper), mapper),
                new DataSourceTransactionManager(ds), new TradeProperties());
    }

    @Test
    void startingAnActiveChainTwiceCreatesOneInitialMessageWithAPersistedDeadline() throws Exception {
        scheduler.start(1L);
        scheduler.start(1L);
        assertEquals(1, count());
        OrderDelayQueryDTO saved = mapper.readValue(jdbc.queryForObject("SELECT payload FROM mq_outbox", byte[].class),
                OrderDelayQueryDTO.class);
        assertEquals(1L, saved.getQueryGeneration());
        assertEquals(0, saved.getQueryRound());
        assertEquals(5, saved.getDelayMillis().size());
        assertEquals("trade.query.topic", jdbc.queryForObject("SELECT exchange_name FROM mq_outbox", String.class));
        assertTrue(jdbc.queryForObject("SELECT available_at FROM mq_outbox", LocalDateTime.class)
                .isAfter(jdbc.queryForObject("SELECT created_at FROM mq_outbox", LocalDateTime.class)));
    }

    @Test
    void duplicateRoundOnlyAdvancesAndCreatesTheNextMessageOnce() {
        scheduler.start(1L);
        OrderDelayQueryDTO round = message(1, 0);
        assertTrue(scheduler.isCurrent(round));
        scheduler.advance(round);
        scheduler.advance(round);
        assertEquals(2, count());
        assertEquals(1, jdbc.queryForObject("SELECT round FROM pay_query_task", Integer.class));
        assertFalse(scheduler.isCurrent(round));
        assertEquals(0, round.getQueryRound());
    }

    @Test
    void nextMessagePersistenceFailureRollsBackRoundAdvancement() {
        scheduler.start(1L);
        jdbc.execute("DROP TABLE mq_outbox");
        assertThrows(org.springframework.dao.DataAccessException.class, () -> scheduler.advance(message(1, 0)));
        assertEquals(0, jdbc.queryForObject("SELECT round FROM pay_query_task", Integer.class));
        assertTrue(scheduler.isCurrent(message(1, 0)));
    }

    @Test
    void concurrentCopiesOfARoundCreateOneNextEvent() throws Exception {
        scheduler.start(1L);
        ExecutorService pool = Executors.newFixedThreadPool(2);
        CountDownLatch start = new CountDownLatch(1);
        try {
            Future<?> first = pool.submit(() -> { await(start); scheduler.advance(message(1, 0)); });
            Future<?> second = pool.submit(() -> { await(start); scheduler.advance(message(1, 0)); });
            start.countDown();
            first.get(10, TimeUnit.SECONDS);
            second.get(10, TimeUnit.SECONDS);
            assertEquals(2, count());
        } finally { pool.shutdownNow(); }
    }

    @Test
    void exhaustedChainStopsAndANewRequestUsesANewGeneration() {
        scheduler.start(1L);
        for (int round = 0; round < 6; round++) scheduler.advance(message(1, round));
        assertEquals(6, count());
        assertEquals("FINISHED", jdbc.queryForObject("SELECT status FROM pay_query_task", String.class));
        scheduler.start(1L);
        assertEquals(7, count());
        assertTrue(scheduler.isCurrent(message(2, 0)));
        assertFalse(scheduler.isCurrent(message(1, 0)));
        scheduler.finish(message(1, 0));
        assertTrue(scheduler.isCurrent(message(2, 0)));
    }

    @Test
    void missingTaskRecoverySkipsPaidExpiredAndExistingOrders() {
        LocalDateTime now = LocalDateTime.now();
        jdbc.update("INSERT INTO `order` VALUES (1,1,0,?)", now);
        jdbc.update("INSERT INTO `order` VALUES (2,2,0,?)", now);
        jdbc.update("INSERT INTO `order` VALUES (3,1,0,?)", now.minusDays(1));
        scheduler.recoverMissingTasks();
        scheduler.recoverMissingTasks();
        assertEquals(1, count());
        assertEquals(1L, jdbc.queryForObject("SELECT order_id FROM pay_query_task", Long.class));
    }

    private OrderDelayQueryDTO message(long generation, int round) {
        OrderDelayQueryDTO message = OrderDelayQueryDTO.init(1L);
        message.setQueryGeneration(generation);
        message.setQueryRound(round);
        return message;
    }

    private int count() { return jdbc.queryForObject("SELECT COUNT(*) FROM mq_outbox", Integer.class); }
    private void await(CountDownLatch start) {
        try { start.await(); }
        catch (InterruptedException e) { Thread.currentThread().interrupt(); throw new IllegalStateException(e); }
    }
}
