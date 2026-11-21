package com.tianji.common.autoconfigure.mq.idempotent;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.transaction.support.TransactionTemplate;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class MqIdempotentExecutorTest {

    private JdbcTemplate jdbc;
    private DataSourceTransactionManager transactionManager;
    private MqIdempotentExecutor executor;

    @BeforeEach
    void setUp() {
        DriverManagerDataSource dataSource = new DriverManagerDataSource(
                "jdbc:h2:mem:" + UUID.randomUUID() + ";MODE=MySQL;DB_CLOSE_DELAY=-1;LOCK_TIMEOUT=5000", "sa", "");
        jdbc = new JdbcTemplate(dataSource);
        transactionManager = new DataSourceTransactionManager(dataSource);
        executor = new MqIdempotentExecutor(jdbc, transactionManager);
        jdbc.execute("CREATE TABLE mq_consume_log (consumer_id VARCHAR(64), event_key VARCHAR(192), " +
                "message_id VARCHAR(255), payload BLOB NOT NULL, created_at TIMESTAMP NOT NULL, " +
                "PRIMARY KEY (consumer_id, event_key))");
        jdbc.execute("CREATE TABLE business (id BIGINT PRIMARY KEY, amount INT NOT NULL)");
        jdbc.update("INSERT INTO business VALUES (1, 0)");
    }

    @Test
    void sameBusinessEventWithDifferentMessageIdsExecutesOnceAndSavesOriginalBody() {
        Message first = message("first");
        assertTrue(executor.execute("coupon", "receive:123", first, this::increment));
        assertFalse(executor.execute("coupon", "receive:123", message("republished"), this::increment));
        assertEquals(1, amount());
        assertArrayEquals(first.getBody(), jdbc.queryForObject("SELECT payload FROM mq_consume_log", byte[].class));
        assertEquals("first", jdbc.queryForObject("SELECT message_id FROM mq_consume_log", String.class));
    }

    @Test
    void failedBusinessRollsBackBothMessageAndBusinessAndCanBeRetried() {
        assertThrows(IllegalStateException.class, () -> executor.execute("lesson", "order:1", message("id"), () -> {
            increment();
            throw new IllegalStateException("模拟中途失败");
        }));
        assertEquals(0, amount());
        assertEquals(0, logCount());
        assertTrue(executor.execute("lesson", "order:1", message("id"), this::increment));
        assertEquals(1, amount());
        assertEquals(1, logCount());
    }

    @Test
    void differentLogicalConsumersProcessTheSameEventIndependently() {
        assertTrue(executor.execute("first", "order:1", message("id"), this::increment));
        assertTrue(executor.execute("second", "order:1", message("id"), this::increment));
        assertEquals(2, amount());
        assertEquals(2, logCount());
    }

    @Test
    void businessUniqueConstraintFailureIsNotMistakenForAnAlreadyConsumedMessage() {
        assertThrows(DuplicateKeyException.class, () -> executor.execute("coupon", "receive:1", message("id"),
                () -> jdbc.update("INSERT INTO business VALUES (1, 10)")));
        assertEquals(0, logCount());
    }

    @Test
    void concurrentConsumersCommitOneBusinessChange() throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(8);
        CountDownLatch start = new CountDownLatch(1);
        try {
            List<Future<Boolean>> attempts = new ArrayList<>();
            for (int i = 0; i < 8; i++) {
                attempts.add(pool.submit(() -> {
                    assertTrue(start.await(5, TimeUnit.SECONDS));
                    return executor.execute("coupon", "receive:1", message(UUID.randomUUID().toString()), this::increment);
                }));
            }
            start.countDown();
            int processed = 0;
            for (Future<Boolean> attempt : attempts) {
                if (attempt.get(10, TimeUnit.SECONDS)) {
                    processed++;
                }
            }
            assertEquals(1, processed);
            assertEquals(1, amount());
            assertEquals(1, logCount());
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    void anotherConsumerCanCompleteAfterTheFirstTransactionRollsBack() throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(2);
        CountDownLatch firstInserted = new CountDownLatch(1);
        CountDownLatch secondStarted = new CountDownLatch(1);
        CountDownLatch failFirst = new CountDownLatch(1);
        try {
            Future<?> first = pool.submit(() -> assertThrows(IllegalStateException.class,
                    () -> executor.execute("coupon", "receive:1", message("first"), () -> {
                        increment();
                        firstInserted.countDown();
                        await(failFirst);
                        throw new IllegalStateException("模拟第一个消费者失败");
                    })));
            assertTrue(firstInserted.await(5, TimeUnit.SECONDS));
            Future<Boolean> second = pool.submit(() -> {
                secondStarted.countDown();
                return executor.execute("coupon", "receive:1", message("second"), this::increment);
            });
            assertTrue(secondStarted.await(5, TimeUnit.SECONDS));
            failFirst.countDown();
            first.get(10, TimeUnit.SECONDS);
            assertTrue(second.get(10, TimeUnit.SECONDS));
            assertEquals(1, amount());
            assertEquals("second", jdbc.queryForObject("SELECT message_id FROM mq_consume_log", String.class));
        } finally {
            failFirst.countDown();
            pool.shutdownNow();
        }
    }

    @Test
    void nestedTransactionIsRejectedRatherThanAcknowledgingARollbackOnlyTransaction() {
        new TransactionTemplate(transactionManager).executeWithoutResult(status ->
                assertThrows(IllegalStateException.class,
                        () -> executor.execute("coupon", "receive:1", message("id"), this::increment)));
        assertEquals(0, logCount());
    }

    @Test
    void missingMessageIdCannotBecomeARandomOrEmptyDeduplicationKey() {
        assertThrows(IllegalArgumentException.class,
                () -> MqIdempotentExecutor.messageKey(new Message(new byte[0], new MessageProperties())));
    }

    private Message message(String id) {
        MessageProperties properties = new MessageProperties();
        properties.setMessageId(id);
        return new Message("{\"userId\":1}".getBytes(StandardCharsets.UTF_8), properties);
    }

    private void increment() {
        jdbc.update("UPDATE business SET amount = amount + 1 WHERE id = 1");
    }

    private int amount() {
        return jdbc.queryForObject("SELECT amount FROM business WHERE id = 1", Integer.class);
    }

    private int logCount() {
        return jdbc.queryForObject("SELECT COUNT(*) FROM mq_consume_log", Integer.class);
    }

    private static void await(CountDownLatch latch) {
        try {
            assertTrue(latch.await(5, TimeUnit.SECONDS));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }
}
