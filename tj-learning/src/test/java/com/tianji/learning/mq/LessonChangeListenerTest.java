package com.tianji.learning.mq;

import com.tianji.api.dto.trade.OrderBasicDTO;
import com.tianji.common.autoconfigure.mq.idempotent.MqIdempotentExecutor;
import com.tianji.common.exceptions.BadRequestException;
import com.tianji.learning.service.ILearningLessonService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import java.util.Arrays;
import java.util.Collections;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class LessonChangeListenerTest {

    private JdbcTemplate jdbc;
    private ILearningLessonService service;
    private LessonChangeListener listener;

    @BeforeEach
    void setUp() {
        DriverManagerDataSource dataSource = new DriverManagerDataSource(
                "jdbc:h2:mem:" + UUID.randomUUID() + ";MODE=MySQL;DB_CLOSE_DELAY=-1", "sa", "");
        jdbc = new JdbcTemplate(dataSource);
        jdbc.execute("CREATE TABLE mq_consume_log (consumer_id VARCHAR(64), event_key VARCHAR(192), " +
                "message_id VARCHAR(255), payload BLOB, created_at TIMESTAMP, PRIMARY KEY (consumer_id, event_key))");
        jdbc.execute("CREATE TABLE lesson (user_id BIGINT, course_id BIGINT)");
        service = mock(ILearningLessonService.class);
        listener = new LessonChangeListener(service,
                new MqIdempotentExecutor(jdbc, new DataSourceTransactionManager(dataSource)));
        doAnswer(call -> {
            for (Long courseId : call.<java.util.List<Long>>getArgument(1)) {
                jdbc.update("INSERT INTO lesson VALUES (?, ?)", call.getArgument(0), courseId);
            }
            return null;
        }).when(service).addUserLessons(anyLong(), anyList());
        doAnswer(call -> {
            for (Long courseId : call.<java.util.List<Long>>getArgument(1)) {
                jdbc.update("DELETE FROM lesson WHERE user_id = ? AND course_id = ?", call.getArgument(0), courseId);
            }
            return null;
        }).when(service).deleteUserLessons(anyLong(), anyList());
    }

    @Test
    void repeatedOrderWithDifferentMessageIdsAddsLessonsOnlyOnce() {
        OrderBasicDTO order = order(10L, 101L, 102L);
        listener.listenLessonPay(order, message("first"));
        listener.listenLessonPay(order, message("republished"));
        assertEquals(2, jdbc.queryForObject("SELECT COUNT(*) FROM lesson", Integer.class));
        verify(service, times(1)).addUserLessons(eq(1L), anyList());
    }

    @Test
    void partialFailureRollsBackTheEntireOrderAndAllowsRetry() {
        doAnswer(call -> {
            jdbc.update("INSERT INTO lesson VALUES (1, 101)");
            throw new IllegalStateException("模拟第二门课程保存失败");
        }).when(service).addUserLessons(anyLong(), anyList());
        assertThrows(IllegalStateException.class,
                () -> listener.listenLessonPay(order(10L, 101L, 102L), message("first")));
        assertEquals(0, jdbc.queryForObject("SELECT COUNT(*) FROM lesson", Integer.class));
        assertEquals(0, jdbc.queryForObject("SELECT COUNT(*) FROM mq_consume_log", Integer.class));
    }

    @Test
    void differentCoursesInTheSameOrderCanBeRefundedSeparately() {
        listener.listenLessonPay(order(10L, 101L, 102L), message("pay"));
        listener.listenLessonRefund(order(10L, 101L), message("refund-one"));
        listener.listenLessonRefund(order(10L, 102L), message("refund-two"));
        assertEquals(0, jdbc.queryForObject("SELECT COUNT(*) FROM lesson", Integer.class));
        assertEquals(3, jdbc.queryForObject("SELECT COUNT(*) FROM mq_consume_log", Integer.class));
    }

    @Test
    void replayingAnAlreadyConsumedRefundDoesNotDeleteARepurchasedLesson() {
        listener.listenLessonPay(order(10L, 101L), message("pay"));
        listener.listenLessonRefund(order(10L, 101L), message("refund"));
        listener.listenLessonPay(order(20L, 101L), message("repurchase"));
        listener.listenLessonRefund(order(10L, 101L), message("refund-republished"));
        assertEquals(1, jdbc.queryForObject("SELECT COUNT(*) FROM lesson", Integer.class));
    }

    @Test
    void invalidOrderIsRejectedWithoutSavingAConsumeRecord() {
        OrderBasicDTO invalid = OrderBasicDTO.builder().userId(1L).courseIds(Collections.singletonList(101L)).build();
        assertThrows(BadRequestException.class, () -> listener.listenLessonPay(invalid, message("id")));
        assertEquals(0, jdbc.queryForObject("SELECT COUNT(*) FROM mq_consume_log", Integer.class));
    }

    private OrderBasicDTO order(Long orderId, Long... courseIds) {
        return OrderBasicDTO.builder().orderId(orderId).userId(1L).courseIds(Arrays.asList(courseIds)).build();
    }

    private Message message(String id) {
        MessageProperties properties = new MessageProperties();
        properties.setMessageId(id);
        return new Message(new byte[0], properties);
    }
}
