package com.tianji.trade.service;

import com.tianji.common.autoconfigure.mq.outbox.OutboxService;
import com.tianji.trade.config.TradeProperties;
import com.tianji.trade.constants.OrderStatus;
import com.tianji.trade.domain.dto.OrderDelayQueryDTO;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.ConcurrencyFailureException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

import static com.tianji.common.constants.MqConstants.Exchange.TRADE_QUERY_EXCHANGE;
import static com.tianji.common.constants.MqConstants.Key.ORDER_DELAY_KEY;

/** 每个订单只保留一个活跃查单链；推进轮次与下轮发送任务一起提交。 */
@Slf4j
@Service
public class PayQueryScheduler {
    private static final List<Long> DELAYS = OrderDelayQueryDTO.init(0L).getDelayMillis();
    private final JdbcTemplate jdbc;
    private final OutboxService outbox;
    private final TransactionTemplate transaction;
    private final TradeProperties tradeProperties;

    public PayQueryScheduler(JdbcTemplate jdbc, OutboxService outbox, PlatformTransactionManager manager,
                             TradeProperties tradeProperties) {
        this.jdbc = jdbc;
        this.outbox = outbox;
        transaction = new TransactionTemplate(manager);
        this.tradeProperties = tradeProperties;
    }

    public void start(Long orderId) {
        transaction.executeWithoutResult(status -> {
            LocalDateTime time = now();
            List<Task> tasks = jdbc.query("SELECT generation,round,status FROM pay_query_task WHERE order_id=?",
                    (rs, row) -> new Task(rs.getLong(1), rs.getInt(2), rs.getString(3)), orderId);
            long generation = 1;
            if (tasks.isEmpty()) {
                jdbc.update("INSERT INTO pay_query_task (order_id,generation,round,status,updated_at) " +
                        "VALUES (?,1,0,'ACTIVE',?)", orderId, time);
            } else {
                Task task = tasks.get(0);
                if ("ACTIVE".equals(task.status)) {
                    return;
                }
                generation = task.generation + 1;
                if (generation < 1 || jdbc.update("UPDATE pay_query_task SET generation=?,round=0,status='ACTIVE'," +
                                "updated_at=? WHERE order_id=? AND generation=? AND status='FINISHED'",
                        generation, time, orderId, task.generation) != 1) {
                    throw new ConcurrencyFailureException("查单任务并发启动冲突，请重试");
                }
            }
            record(orderId, generation, 0, time);
        });
    }

    public boolean isCurrent(OrderDelayQueryDTO message) {
        Integer count = jdbc.queryForObject("SELECT COUNT(*) FROM pay_query_task WHERE order_id=? " +
                        "AND generation=? AND round=? AND status='ACTIVE'", Integer.class,
                message.getOrderId(), message.getQueryGeneration(), message.getQueryRound());
        return count != null && count == 1;
    }

    public void advance(OrderDelayQueryDTO message) {
        transaction.executeWithoutResult(status -> {
            int next = message.getQueryRound() + 1;
            LocalDateTime time = now();
            int changed = jdbc.update("UPDATE pay_query_task SET round=?,status=?,updated_at=? " +
                            "WHERE order_id=? AND generation=? AND round=? AND status='ACTIVE'",
                    next, next < DELAYS.size() ? "ACTIVE" : "FINISHED", time,
                    message.getOrderId(), message.getQueryGeneration(), message.getQueryRound());
            if (changed == 1 && next < DELAYS.size()) {
                record(message.getOrderId(), message.getQueryGeneration(), next, time);
            }
        });
    }

    public void finish(OrderDelayQueryDTO message) {
        transaction.executeWithoutResult(status -> jdbc.update("UPDATE pay_query_task SET status='FINISHED',updated_at=? " +
                        "WHERE order_id=? AND generation=? AND round=? AND status='ACTIVE'",
                now(), message.getOrderId(), message.getQueryGeneration(), message.getQueryRound()));
    }

    private void record(Long orderId, long generation, int round, LocalDateTime time) {
        OrderDelayQueryDTO message = OrderDelayQueryDTO.init(orderId);
        message.setQueryGeneration(generation);
        message.setQueryRound(round);
        message.setDelayMillis(new ArrayList<>(DELAYS.subList(round + 1, DELAYS.size())));
        outbox.recordAt("ORDER_PAY_QUERY", "order:" + orderId + ":generation:" + generation + ":round:" + round,
                TRADE_QUERY_EXCHANGE, ORDER_DELAY_KEY, message, time.plusNanos(DELAYS.get(round) * 1_000_000));
    }

    /** 覆盖远程申请支付成功、本地初始任务尚未创建就宕机的窗口；不重开已耗尽的链路。 */
    @Scheduled(fixedDelayString = "${app.mq.outbox.query-recovery-interval-ms:60000}", initialDelay = 15000)
    public void recoverMissingTasks() {
        LocalDateTime time = now();
        List<Long> orderIds = jdbc.query("SELECT o.id FROM `order` o WHERE o.status=? AND o.deleted=0 " +
                        "AND o.create_time>=? AND NOT EXISTS (SELECT 1 FROM pay_query_task t WHERE t.order_id=o.id) " +
                        "ORDER BY o.id LIMIT 100", (rs, row) -> rs.getLong(1), OrderStatus.NO_PAY.getValue(),
                time.minusMinutes(tradeProperties.getPayOrderTTLMinutes()));
        for (Long orderId : orderIds) {
            try {
                start(orderId);
            } catch (Exception e) {
                log.error("恢复缺失查单任务失败，orderId={}", orderId, e);
            }
        }
    }

    private LocalDateTime now() {
        return jdbc.queryForObject("SELECT CURRENT_TIMESTAMP", LocalDateTime.class);
    }

    private static class Task {
        final long generation;
        final int round;
        final String status;

        Task(long generation, int round, String status) {
            this.generation = generation;
            this.round = round;
            this.status = status;
        }
    }
}
