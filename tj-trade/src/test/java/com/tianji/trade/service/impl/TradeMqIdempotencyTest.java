package com.tianji.trade.service.impl;

import com.baomidou.mybatisplus.core.MybatisConfiguration;
import com.baomidou.mybatisplus.extension.spring.MybatisSqlSessionFactoryBean;
import com.tianji.api.cache.RoleCache;
import com.tianji.api.client.course.CourseClient;
import com.tianji.api.client.promotion.PromotionClient;
import com.tianji.api.client.user.UserClient;
import com.tianji.common.autoconfigure.mq.outbox.OutboxService;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import java.nio.file.Files;
import java.nio.file.Path;
import com.tianji.common.exceptions.BadRequestException;
import com.tianji.common.exceptions.BizIllegalException;
import com.tianji.common.utils.UserContext;
import com.tianji.pay.sdk.client.PayClient;
import com.tianji.pay.sdk.dto.PayResultDTO;
import com.tianji.pay.sdk.dto.RefundResultDTO;
import com.tianji.trade.config.TradeProperties;
import com.tianji.trade.constants.OrderStatus;
import com.tianji.trade.constants.RefundStatus;
import com.tianji.trade.domain.po.OrderDetail;
import com.tianji.trade.mapper.OrderMapper;
import com.tianji.trade.mapper.RefundApplyMapper;
import com.tianji.trade.service.ICartService;
import com.tianji.trade.service.IOrderDetailService;
import com.tianji.trade.service.IRefundApplyService;
import org.apache.ibatis.session.SqlSessionFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mybatis.spring.SqlSessionTemplate;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.transaction.annotation.EnableTransactionManagement;

import javax.sql.DataSource;
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
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/** 使用真实 MyBatis SQL 和 Spring 事务，验证状态竞争和后续失败的回滚。 */
class TradeMqIdempotencyTest {

    private AnnotationConfigApplicationContext context;
    private JdbcTemplate jdbc;
    private OrderServiceImpl orders;
    private RefundApplyServiceImpl refunds;
    private IOrderDetailService details;
    private OutboxService mq;

    @BeforeEach
    void setUp() throws Exception {
        context = new AnnotationConfigApplicationContext(TestConfiguration.class);
        jdbc = context.getBean(JdbcTemplate.class);
        orders = context.getBean(OrderServiceImpl.class);
        refunds = context.getBean(RefundApplyServiceImpl.class);
        details = context.getBean(IOrderDetailService.class);
        mq = context.getBean(OutboxService.class);
        String ddl = Files.readString(Path.of("..", "docs", "sql", "mq-outbox.sql"))
                .replace("CHARACTER SET ascii COLLATE ascii_bin", "")
                .replace("ENGINE=InnoDB DEFAULT CHARSET=utf8mb4", "");
        for (String sql : ddl.split(";")) if (!sql.trim().isEmpty()) jdbc.execute(sql);
        jdbc.execute("CREATE TABLE `order` (id BIGINT PRIMARY KEY, pay_order_no BIGINT, user_id BIGINT, status INT, " +
                "message VARCHAR(255), total_amount INT, real_amount INT, discount_amount INT, pay_channel VARCHAR(50), " +
                "coupon_ids VARCHAR(1024), create_time TIMESTAMP, pay_time TIMESTAMP, close_time TIMESTAMP, " +
                "finish_time TIMESTAMP, refund_time TIMESTAMP, update_time TIMESTAMP, creater BIGINT, updater BIGINT, deleted INT)");
        jdbc.execute("CREATE TABLE refund_apply (id BIGINT PRIMARY KEY, order_detail_id BIGINT, order_id BIGINT, " +
                "refund_order_no BIGINT, user_id BIGINT, refund_amount INT, status INT, message VARCHAR(255), " +
                "refund_reason VARCHAR(255), question_desc VARCHAR(255), approver BIGINT, approve_opinion VARCHAR(255), " +
                "remark VARCHAR(255), refund_channel VARCHAR(50), failed_reason VARCHAR(255), create_time TIMESTAMP, " +
                "approve_time TIMESTAMP, finish_time TIMESTAMP, update_time TIMESTAMP, creater BIGINT, updater BIGINT)");
        jdbc.execute("CREATE TABLE detail_result (id BIGINT PRIMARY KEY, refund_status INT)");
        jdbc.update("INSERT INTO `order` (id, user_id, status, deleted) VALUES (1, 1, 1, 0)");
        jdbc.update("INSERT INTO refund_apply (id, order_detail_id, order_id, user_id, status, approver) VALUES (1, 42, 1, 1, 3, 1)");
        jdbc.update("INSERT INTO detail_result VALUES (42, 3)");
        when(details.queryCourseIdsByOrderId(1L)).thenReturn(Collections.singletonList(101L));
        when(details.getById(42L)).thenReturn(new OrderDetail().setId(42L).setCourseId(101L));
        doAnswer(call -> {
            Integer status = call.getArgument(1);
            Long detailId = call.getArgument(0);
            jdbc.update("UPDATE detail_result SET refund_status = ? WHERE id = ?", status, detailId);
            return null;
        }).when(details).updateRefundStatusById(anyLong(), anyInt());
    }

    @AfterEach
    void tearDown() {
        context.close();
        UserContext.removeUser();
    }

    @Test
    void repeatedPaymentTransitionsOnceAndSendsOneEnrollmentNotification() {
        orders.handlePaySuccess(payment());
        orders.handlePaySuccess(payment());
        assertEquals(OrderStatus.PAYED.getValue(), orderStatus());
        verify(details, times(1)).markDetailSuccessByOrderId(eq(1L), anyString(), any());
        verify(mq, times(1)).record(anyString(), anyString(), anyString(), anyString(), any());
    }

    @Test
    void concurrentPaymentCallbacksHaveOneWinner() throws Exception {
        concurrently(() -> orders.handlePaySuccess(payment()));
        assertEquals(OrderStatus.PAYED.getValue(), orderStatus());
        verify(details, times(1)).markDetailSuccessByOrderId(eq(1L), anyString(), any());
        verify(mq, times(1)).record(anyString(), anyString(), anyString(), anyString(), any());
    }

    @Test
    void failedDetailUpdateRollsBackThePaymentTransitionAndPermitsRetry() {
        doThrow(new IllegalStateException("模拟明细更新失败")).when(details)
                .markDetailSuccessByOrderId(anyLong(), anyString(), any());
        assertThrows(IllegalStateException.class, () -> orders.handlePaySuccess(payment()));
        assertEquals(OrderStatus.NO_PAY.getValue(), orderStatus());
        verifyNoInteractions(mq);
        doNothing().when(details).markDetailSuccessByOrderId(anyLong(), anyString(), any());
        orders.handlePaySuccess(payment());
        assertEquals(OrderStatus.PAYED.getValue(), orderStatus());
        verify(mq).record(anyString(), anyString(), anyString(), anyString(), any());
    }

    @Test
    void nonSuccessPaymentCannotMarkTheOrderPaid() {
        PayResultDTO invalid = PayResultDTO.builder().status(PayResultDTO.PAYING).bizOrderId(1L).build();
        assertThrows(BadRequestException.class, () -> orders.handlePaySuccess(invalid));
        assertEquals(OrderStatus.NO_PAY.getValue(), orderStatus());
        verifyNoInteractions(details, mq);
    }

    @Test
    void closedOrderPaymentIsReportedWithoutReopeningTheOrder() {
        jdbc.update("UPDATE `order` SET status = 3 WHERE id = 1");
        assertThrows(BizIllegalException.class, () -> orders.handlePaySuccess(payment()));
        assertEquals(OrderStatus.CLOSED.getValue(), orderStatus());
        verifyNoInteractions(details, mq);
    }

    @Test
    void paymentReplayDoesNotRegressAFinishedOrder() {
        jdbc.update("UPDATE `order` SET status = 4 WHERE id = 1");
        orders.handlePaySuccess(payment());
        assertEquals(OrderStatus.FINISHED.getValue(), orderStatus());
        verifyNoInteractions(details, mq);
    }

    @Test
    void repeatedAndLateRefundResultsCannotRegressSuccessOrResendTheRemoval() {
        refunds.handleRefundResult(refund(RefundResultDTO.RUNNING));
        refunds.handleRefundResult(refund(RefundResultDTO.SUCCESS));
        LocalDateTime finish = jdbc.queryForObject("SELECT finish_time FROM refund_apply WHERE id = 1", LocalDateTime.class);
        refunds.handleRefundResult(refund(RefundResultDTO.SUCCESS));
        refunds.handleRefundResult(refund(RefundResultDTO.FAILED));
        refunds.handleRefundResult(refund(RefundResultDTO.RUNNING));
        assertEquals(RefundStatus.SUCCESS.getValue(), refundStatus());
        assertEquals(finish, jdbc.queryForObject("SELECT finish_time FROM refund_apply WHERE id = 1", LocalDateTime.class));
        assertEquals(RefundStatus.SUCCESS.getValue(), jdbc.queryForObject("SELECT refund_status FROM detail_result", Integer.class));
        verify(mq, times(1)).record(anyString(), anyString(), anyString(), anyString(), any());
    }

    @Test
    void concurrentRefundCallbacksHaveOneWinner() throws Exception {
        concurrently(() -> refunds.handleRefundResult(refund(RefundResultDTO.SUCCESS)));
        assertEquals(RefundStatus.SUCCESS.getValue(), refundStatus());
        verify(details, times(1)).updateRefundStatusById(42L, RefundStatus.SUCCESS.getValue());
        verify(mq, times(1)).record(anyString(), anyString(), anyString(), anyString(), any());
    }

    @Test
    void refundOutboxExceptionRollsBackBothRefundAndDetailState() {
        doThrow(new IllegalStateException("模拟 Outbox 落库异常")).when(mq).record(anyString(), anyString(), anyString(), anyString(), any());
        assertThrows(IllegalStateException.class, () -> refunds.handleRefundResult(refund(RefundResultDTO.SUCCESS)));
        assertEquals(RefundStatus.AGREE.getValue(), refundStatus());
        assertEquals(RefundStatus.AGREE.getValue(), jdbc.queryForObject("SELECT refund_status FROM detail_result", Integer.class));
    }

    @Test
    void contradictorySuccessAfterFailureIsReportedInsteadOfOverwritingTheTerminalState() {
        refunds.handleRefundResult(refund(RefundResultDTO.FAILED));
        assertThrows(BizIllegalException.class, () -> refunds.handleRefundResult(refund(RefundResultDTO.SUCCESS)));
        assertEquals(RefundStatus.FAILED.getValue(), refundStatus());
        verifyNoInteractions(mq);
    }

    @Test
    void unapprovedRefundIsRejectedAndCanBeProcessedAfterApprovalCommits() {
        jdbc.update("UPDATE refund_apply SET status = 1 WHERE id = 1");
        assertThrows(BizIllegalException.class, () -> refunds.handleRefundResult(refund(RefundResultDTO.SUCCESS)));
        assertEquals(RefundStatus.UN_APPROVE.getValue(), refundStatus());
        verifyNoInteractions(details, mq);
        jdbc.update("UPDATE refund_apply SET status = 3 WHERE id = 1");
        refunds.handleRefundResult(refund(RefundResultDTO.SUCCESS));
        assertEquals(RefundStatus.SUCCESS.getValue(), refundStatus());
        verify(mq).record(anyString(), anyString(), anyString(), anyString(), any());
    }

    private int orderStatus() {
        return jdbc.queryForObject("SELECT status FROM `order` WHERE id = 1", Integer.class);
    }

    private int refundStatus() {
        return jdbc.queryForObject("SELECT status FROM refund_apply WHERE id = 1", Integer.class);
    }

    private PayResultDTO payment() {
        return PayResultDTO.builder().status(PayResultDTO.SUCCESS).bizOrderId(1L).payOrderNo(100L)
                .payChannel("alipay").successTime(LocalDateTime.of(2026, 10, 14, 12, 0)).build();
    }

    private RefundResultDTO refund(int status) {
        return RefundResultDTO.builder().status(status).bizRefundOrderId(1L).refundOrderNo(200L).refundChannel("alipay").build();
    }

    private void concurrently(Runnable action) throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(6);
        CountDownLatch start = new CountDownLatch(1);
        try {
            List<Future<?>> attempts = new ArrayList<>();
            for (int i = 0; i < 6; i++) {
                attempts.add(pool.submit(() -> {
                    try {
                        assertTrue(start.await(5, TimeUnit.SECONDS));
                        try {
                            action.run();
                        } catch (BizIllegalException e) {
                            assertTrue(e.getMessage().contains("并发状态变更"));
                            // 乐观锁冲突退出原事务，模拟监听器在新事务中重试。
                            action.run();
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new IllegalStateException(e);
                    } finally {
                        UserContext.removeUser();
                    }
                }));
            }
            start.countDown();
            for (Future<?> attempt : attempts) {
                attempt.get(15, TimeUnit.SECONDS);
            }
        } finally {
            pool.shutdownNow();
        }
    }

    @Configuration(proxyBeanMethods = false)
    @EnableTransactionManagement(proxyTargetClass = true)
    static class TestConfiguration {
        @Bean
        DataSource dataSource() {
            return new DriverManagerDataSource(
                    "jdbc:h2:mem:" + UUID.randomUUID() + ";MODE=MySQL;DB_CLOSE_DELAY=-1;LOCK_TIMEOUT=10000", "sa", "");
        }

        @Bean
        JdbcTemplate jdbcTemplate(DataSource dataSource) {
            return new JdbcTemplate(dataSource);
        }

        @Bean
        DataSourceTransactionManager transactionManager(DataSource dataSource) {
            return new DataSourceTransactionManager(dataSource);
        }

        @Bean
        SqlSessionFactory sqlSessionFactory(DataSource dataSource) throws Exception {
            MybatisConfiguration configuration = new MybatisConfiguration();
            configuration.addMapper(OrderMapper.class);
            configuration.addMapper(RefundApplyMapper.class);
            MybatisSqlSessionFactoryBean factory = new MybatisSqlSessionFactoryBean();
            factory.setDataSource(dataSource);
            factory.setConfiguration(configuration);
            return factory.getObject();
        }

        @Bean
        OrderMapper orderMapper(SqlSessionFactory factory) {
            return new SqlSessionTemplate(factory).getMapper(OrderMapper.class);
        }

        @Bean
        RefundApplyMapper refundApplyMapper(SqlSessionFactory factory) {
            return new SqlSessionTemplate(factory).getMapper(RefundApplyMapper.class);
        }

        @Bean
        IOrderDetailService detailService() {
            return mock(IOrderDetailService.class);
        }

        @Bean
        OutboxService outboxService(JdbcTemplate jdbc) {
            ObjectMapper mapper = new ObjectMapper().findAndRegisterModules();
            return spy(new OutboxService(jdbc, new Jackson2JsonMessageConverter(mapper), mapper));
        }

        @Bean
        OrderServiceImpl orderService(IOrderDetailService details, OutboxService mq) {
            return new OrderServiceImpl(mock(CourseClient.class), mock(PromotionClient.class), details,
                    mock(ICartService.class), new TradeProperties(), mq);
        }

        @Bean
        RefundApplyServiceImpl refundService(OrderMapper mapper, IOrderDetailService details, OutboxService mq,
                                             ObjectProvider<IRefundApplyService> provider) {
            return new RefundApplyServiceImpl(mapper, details, mock(UserClient.class), mock(PayClient.class),
                    mock(RoleCache.class), mock(ThreadPoolTaskExecutor.class), mq, provider);
        }
    }
}
