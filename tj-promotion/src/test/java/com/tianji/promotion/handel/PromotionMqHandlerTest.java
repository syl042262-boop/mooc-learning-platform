package com.tianji.promotion.handel;

import com.tianji.common.autoconfigure.mq.idempotent.MqIdempotentExecutor;
import com.tianji.promotion.domain.dto.UserCouponDTO;
import com.tianji.promotion.service.IUserCouponService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class PromotionMqHandlerTest {

    private JdbcTemplate jdbc;
    private PromotionMqHandler handler;
    private IUserCouponService service;

    @BeforeEach
    void setUp() {
        DriverManagerDataSource dataSource = new DriverManagerDataSource(
                "jdbc:h2:mem:" + UUID.randomUUID() + ";MODE=MySQL;DB_CLOSE_DELAY=-1", "sa", "");
        jdbc = new JdbcTemplate(dataSource);
        jdbc.execute("CREATE TABLE mq_consume_log (consumer_id VARCHAR(64), event_key VARCHAR(192), " +
                "message_id VARCHAR(255), payload BLOB, created_at TIMESTAMP, PRIMARY KEY (consumer_id, event_key))");
        jdbc.execute("CREATE TABLE coupon (id BIGINT PRIMARY KEY, issue_num INT)");
        jdbc.update("INSERT INTO coupon VALUES (1, 0)");
        service = mock(IUserCouponService.class);
        doAnswer(call -> {
            jdbc.update("UPDATE coupon SET issue_num = issue_num + 1 WHERE id = 1");
            return null;
        }).when(service).checkAndCreateUserCouponByMq(any());
        handler = new PromotionMqHandler(service,
                new MqIdempotentExecutor(jdbc, new DataSourceTransactionManager(dataSource)));
    }

    @Test
    void sameRequestRepublishedWithANewMessageIdDoesNotIssueAnotherCoupon() {
        handler.listenCouponReceiveMessage(coupon("request-one", null), message("first"));
        handler.listenCouponReceiveMessage(coupon("request-one", null), message("republished"));
        assertEquals(1, issued());
    }

    @Test
    void differentLegalRequestsCanReceiveTheSameCouponTemplate() {
        handler.listenCouponReceiveMessage(coupon("request-one", null), message("first"));
        handler.listenCouponReceiveMessage(coupon("request-two", null), message("second"));
        assertEquals(2, issued());
    }

    @Test
    void exchangeUsesSerialNumberRegardlessOfTransportMessageId() {
        handler.listenCouponReceiveMessage(coupon(null, 100L), message("first"));
        handler.listenCouponReceiveMessage(coupon(null, 100L), message("republished"));
        assertEquals(1, issued());
    }

    @Test
    void failedIssuanceDoesNotLeaveAConsumeRecordOrAnInventoryChange() {
        doAnswer(call -> {
            jdbc.update("UPDATE coupon SET issue_num = issue_num + 1 WHERE id = 1");
            throw new IllegalStateException("模拟用户券保存失败");
        }).when(service).checkAndCreateUserCouponByMq(any());
        assertThrows(IllegalStateException.class,
                () -> handler.listenCouponReceiveMessage(coupon("request", null), message("id")));
        assertEquals(0, issued());
        assertEquals(0, jdbc.queryForObject("SELECT COUNT(*) FROM mq_consume_log", Integer.class));
    }

    @Test
    void legacyMessageIsDeduplicatedByItsOriginalMessageId() {
        handler.listenCouponReceiveMessage(coupon(null, null), message("legacy"));
        handler.listenCouponReceiveMessage(coupon(null, null), message("legacy"));
        assertEquals(1, issued());
        assertThrows(IllegalArgumentException.class,
                () -> handler.listenCouponReceiveMessage(coupon(null, null), message(null)));
    }

    private int issued() {
        return jdbc.queryForObject("SELECT issue_num FROM coupon WHERE id = 1", Integer.class);
    }

    private UserCouponDTO coupon(String requestId, Long serialNum) {
        UserCouponDTO dto = new UserCouponDTO();
        dto.setUserId(1L);
        dto.setCouponId(1L);
        dto.setReceiveRequestId(requestId);
        dto.setSerialNum(serialNum);
        return dto;
    }

    private Message message(String id) {
        MessageProperties properties = new MessageProperties();
        properties.setMessageId(id);
        return new Message(new byte[0], properties);
    }
}
