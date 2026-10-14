package com.tianji.common.autoconfigure.mq.outbox;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class OutboxReliabilityTest {
    private JdbcTemplate jdbc;
    private TransactionTemplate transaction;
    private OutboxService outbox;
    private OutboxStore store;
    private final ObjectMapper mapper = new ObjectMapper();
    private final OutboxProperties properties = new OutboxProperties();

    @BeforeEach
    void setUp() throws Exception {
        DriverManagerDataSource ds = new DriverManagerDataSource("jdbc:h2:mem:" + UUID.randomUUID()
                + ";MODE=MySQL;DB_CLOSE_DELAY=-1;LOCK_TIMEOUT=5000", "sa", "");
        jdbc = new JdbcTemplate(ds);
        DataSourceTransactionManager manager = new DataSourceTransactionManager(ds);
        transaction = new TransactionTemplate(manager);
        outbox = new OutboxService(jdbc, new Jackson2JsonMessageConverter(mapper), mapper);
        store = new OutboxStore(jdbc, manager);
        // 与部署脚本使用同一结构，只移除 H2 不支持的 MySQL 存储/字符集语法。
        String ddl = Files.readString(Path.of("..", "docs", "sql", "mq-outbox.sql"))
                .replace("CHARACTER SET ascii COLLATE ascii_bin", "")
                .replace("ENGINE=InnoDB DEFAULT CHARSET=utf8mb4", "");
        for (String sql : ddl.split(";")) {
            if (!sql.trim().isEmpty()) jdbc.execute(sql);
        }
        jdbc.execute("CREATE TABLE business (id INT PRIMARY KEY, amount INT NOT NULL)");
        jdbc.update("INSERT INTO business VALUES (1,0)");
    }

    @Test
    void refusesAutocommitAndAnUnrelatedDataSourceTransaction() {
        assertThrows(IllegalStateException.class, () -> record("one"));
        DriverManagerDataSource other = new DriverManagerDataSource("jdbc:h2:mem:" + UUID.randomUUID(), "sa", "");
        TransactionTemplate wrong = new TransactionTemplate(new DataSourceTransactionManager(other));
        wrong.executeWithoutResult(status -> assertThrows(IllegalStateException.class, () -> record("one")));
        assertEquals(0, count());
    }

    @Test
    void businessFailureRollsBackTheOutboxAndTheBusinessWrite() {
        assertThrows(IllegalStateException.class, () -> transaction.executeWithoutResult(status -> {
            jdbc.update("UPDATE business SET amount=10");
            record("one");
            throw new IllegalStateException("业务失败");
        }));
        assertEquals(0, count());
        assertEquals(0, jdbc.queryForObject("SELECT amount FROM business", Integer.class));
    }

    @Test
    void outboxIdentityConflictRollsBackTheBusinessAndKeepsTheOriginalPayload() {
        transaction.executeWithoutResult(status -> record("one"));
        byte[] original = jdbc.queryForObject("SELECT payload FROM mq_outbox", byte[].class);
        assertThrows(DuplicateKeyException.class, () -> transaction.executeWithoutResult(status -> {
            jdbc.update("UPDATE business SET amount=10");
            outbox.record("TEST", "one", "test.topic", "test.key", 999L);
        }));
        assertEquals(1, count());
        assertEquals(0, jdbc.queryForObject("SELECT amount FROM business", Integer.class));
        assertArrayEquals(original, jdbc.queryForObject("SELECT payload FROM mq_outbox", byte[].class));
    }

    @Test
    void aPublishingFailureDoesNotBlockAnotherEventAndUsesTheSameIdentityOnRetry() throws Exception {
        transaction.executeWithoutResult(status -> { record("one"); record("two"); });
        List<Message> sent = new ArrayList<>();
        List<String> attemptIds = new ArrayList<>();
        OutboxDispatcher dispatcher = dispatcher((exchange, key, message, attempt) -> {
            assertFalse(TransactionSynchronizationManager.isActualTransactionActive());
            sent.add(message);
            attemptIds.add(attempt);
            if (sent.size() == 1) throw new IllegalStateException("模拟确认丢失");
        });
        dispatcher.publishBatch();
        assertEquals(1, jdbc.queryForObject("SELECT COUNT(*) FROM mq_outbox WHERE status='PUBLISHED'", Integer.class));
        jdbc.update("UPDATE mq_outbox SET available_at=DATEADD('SECOND',-1,CURRENT_TIMESTAMP)," +
                "next_retry_at=DATEADD('SECOND',-1,CURRENT_TIMESTAMP) WHERE status='PENDING'");
        dispatcher.publishBatch();
        assertEquals(3, sent.size());
        assertEquals(sent.get(0).getMessageProperties().getMessageId(), sent.get(2).getMessageProperties().getMessageId());
        assertArrayEquals(sent.get(0).getBody(), sent.get(2).getBody());
        assertEquals(sent.get(0).getMessageProperties().getHeaders(), sent.get(2).getMessageProperties().getHeaders());
        assertNotEquals(attemptIds.get(0), attemptIds.get(2));
        assertEquals(2, jdbc.queryForObject("SELECT COUNT(*) FROM mq_outbox WHERE status='PUBLISHED'", Integer.class));
        assertEquals("java.lang.Long", sent.get(0).getMessageProperties().getHeader("__TypeId__"));
    }

    @Test
    void onlyOneConcurrentWorkerCanOwnAnEvent() throws Exception {
        transaction.executeWithoutResult(status -> record("one"));
        long id = store.readyIds(10).get(0);
        CountDownLatch start = new CountDownLatch(1);
        ExecutorService pool = Executors.newFixedThreadPool(2);
        try {
            List<Future<OutboxMessage>> claims = new ArrayList<>();
            for (int i = 0; i < 2; i++) claims.add(pool.submit(() -> {
                start.await();
                return store.claim(id, UUID.randomUUID().toString(), 30);
            }));
            start.countDown();
            int winners = 0;
            for (Future<OutboxMessage> result : claims) if (result.get(10, TimeUnit.SECONDS) != null) winners++;
            assertEquals(1, winners);
        } finally { pool.shutdownNow(); }
    }

    @Test
    void expiredLeaseCanBeRecoveredAndItsOldCallbackCannotOverwriteTheNewOwner() {
        transaction.executeWithoutResult(status -> record("one"));
        long id = store.readyIds(10).get(0);
        OutboxMessage old = store.claim(id, "old", 30);
        jdbc.update("UPDATE mq_outbox SET locked_until=DATEADD('SECOND',-60,CURRENT_TIMESTAMP)," +
                "available_at=DATEADD('SECOND',-1,CURRENT_TIMESTAMP),next_retry_at=DATEADD('SECOND',-1,CURRENT_TIMESTAMP) WHERE id=?", id);
        assertEquals(Collections.singletonList(id), store.readyIds(10),
                jdbc.queryForObject("SELECT status || ':' || CAST(available_at AS VARCHAR) || ':' || " +
                        "CAST(next_retry_at AS VARCHAR) || ':' || COALESCE(CAST(locked_until AS VARCHAR),'null') FROM mq_outbox", String.class));
        OutboxMessage current = store.claim(id, "new", 30);
        assertFalse(store.published(old));
        store.retry(old, new IllegalStateException("迟到失败"), properties);
        assertEquals("new", jdbc.queryForObject("SELECT lock_token FROM mq_outbox", String.class));
        assertTrue(store.published(current));
    }

    @Test
    void acknowledgmentFollowedByDatabaseFailureIsSafelyRepublished() {
        transaction.executeWithoutResult(status -> record("one"));
        OutboxStore failing = spy(store);
        doThrow(new IllegalStateException("确认后数据库不可用")).doCallRealMethod().when(failing).published(any());
        List<String> ids = new ArrayList<>();
        OutboxDispatcher dispatcher = new OutboxDispatcher(failing,
                (exchange, key, message, attempt) -> ids.add(message.getMessageProperties().getMessageId()), mapper, properties);
        dispatcher.publishBatch();
        jdbc.update("UPDATE mq_outbox SET available_at=DATEADD('SECOND',-1,CURRENT_TIMESTAMP)," +
                "next_retry_at=DATEADD('SECOND',-1,CURRENT_TIMESTAMP)");
        dispatcher.publishBatch();
        assertEquals(2, ids.size());
        assertEquals(ids.get(0), ids.get(1));
        assertEquals("PUBLISHED", jdbc.queryForObject("SELECT status FROM mq_outbox", String.class));
    }

    @Test
    void futureMessagesAreNotSentAndCannotBeClaimedEarly() {
        transaction.executeWithoutResult(status -> outbox.recordAt("TEST", "later", "test.topic", "test.key", 1L,
                LocalDateTime.now().plusHours(1)));
        assertTrue(store.readyIds(10).isEmpty());
        long id = jdbc.queryForObject("SELECT id FROM mq_outbox", Long.class);
        assertNull(store.claim(id, "early", 30));
    }

    @Test
    void versionsRollBackWithBusinessAndAllowMultipleRealTransitions() {
        assertThrows(IllegalStateException.class, () -> transaction.executeWithoutResult(status -> {
            assertEquals(1, outbox.nextVersion("course:1"));
            throw new IllegalStateException();
        }));
        transaction.executeWithoutResult(status -> {
            assertEquals(1, outbox.nextVersion("course:1"));
            assertEquals(2, outbox.nextVersion("course:1"));
        });
        assertEquals(2, jdbc.queryForObject("SELECT version FROM mq_outbox_sequence", Long.class));
    }

    @Test
    void publishingInsideABusinessTransactionIsRejected() {
        transaction.executeWithoutResult(status -> assertThrows(IllegalStateException.class,
                () -> dispatcher((exchange, key, message, attempt) -> fail("不应发送")).publishBatch()));
    }

    @Test
    void uncommittedEventIsInvisibleToAnotherPublishingConnection() throws Exception {
        ExecutorService pool = Executors.newSingleThreadExecutor();
        try {
            transaction.executeWithoutResult(status -> {
                record("one");
                try { assertTrue(pool.submit(() -> store.readyIds(10)).get(5, TimeUnit.SECONDS).isEmpty()); }
                catch (Exception e) { throw new IllegalStateException(e); }
            });
            assertEquals(1, store.readyIds(10).size());
        } finally { pool.shutdownNow(); }
    }

    @Test
    void persistentFailuresRemainRetryableAfterManyAttempts() {
        transaction.executeWithoutResult(status -> record("one"));
        jdbc.update("UPDATE mq_outbox SET attempts=200");
        dispatcher((exchange, key, message, attempt) -> { throw new IllegalStateException("路由配置错误"); }).publishBatch();
        assertEquals(1, count());
        assertEquals("PENDING", jdbc.queryForObject("SELECT status FROM mq_outbox", String.class));
        assertEquals(201, jdbc.queryForObject("SELECT attempts FROM mq_outbox", Integer.class));
        assertNotNull(jdbc.queryForObject("SELECT last_error FROM mq_outbox", String.class));
    }

    private void record(String key) { outbox.record("TEST", key, "test.topic", "test.key", 1L); }
    private int count() { return jdbc.queryForObject("SELECT COUNT(*) FROM mq_outbox", Integer.class); }
    private OutboxDispatcher dispatcher(ConfirmedMessageSender sender) {
        return new OutboxDispatcher(store, sender, mapper, properties);
    }
}
