package com.tianji.learning.mq;

import com.tianji.api.dto.trade.OrderBasicDTO;
import com.tianji.common.constants.MqConstants;
import com.tianji.common.autoconfigure.mq.idempotent.MqIdempotentConfiguration;
import com.tianji.common.autoconfigure.mq.idempotent.MqIdempotentExecutor;
import com.tianji.common.exceptions.BadRequestException;
import com.tianji.learning.service.ILearningLessonService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.ExchangeTypes;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.Exchange;
import org.springframework.amqp.rabbit.annotation.Queue;
import org.springframework.amqp.rabbit.annotation.QueueBinding;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.context.annotation.Import;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;

@Component
@Slf4j
@RequiredArgsConstructor
@Import(MqIdempotentConfiguration.class)
public class LessonChangeListener {

    private final ILearningLessonService lessonService;
    private final MqIdempotentExecutor idempotentExecutor;


    @RabbitListener(bindings = @QueueBinding(
            value = @Queue(name = "learning.lesson.pay.queue", durable = "true"),
            exchange = @Exchange(name = MqConstants.Exchange.ORDER_EXCHANGE, type = ExchangeTypes.TOPIC),
            key = MqConstants.Key.ORDER_PAY_KEY
    ))
    public void listenLessonPay(OrderBasicDTO orderBasicDTO, Message message) {
        validateOrder(orderBasicDTO);

        // 2.添加课程
        log.debug("监听到用户{}的订单{}，需要添加课程{}到课表中", orderBasicDTO.getUserId(), orderBasicDTO.getOrderId(), orderBasicDTO.getCourseIds());
        idempotentExecutor.execute("learning.lesson.pay", "order:" + orderBasicDTO.getOrderId(), message,
                () -> lessonService.addUserLessons(orderBasicDTO.getUserId(), orderBasicDTO.getCourseIds()));
    }

    @RabbitListener(bindings = @QueueBinding(
            value = @Queue(name = "learning.lesson.refund.queue", durable = "true"),
            exchange = @Exchange(name = MqConstants.Exchange.ORDER_EXCHANGE, type = ExchangeTypes.TOPIC),
            key = MqConstants.Key.ORDER_REFUND_KEY
    ))
    public void listenLessonRefund(OrderBasicDTO orderBasicDTO, Message message) {
        validateOrder(orderBasicDTO);

        // 2.删除课程
        log.debug("监听到用户{}的订单{}，需要删除课程{}到课表中", orderBasicDTO.getUserId(), orderBasicDTO.getOrderId(), orderBasicDTO.getCourseIds());
        // 一个订单可能分多次退不同课程，不能仅按 orderId 去重。
        for (Long courseId : orderBasicDTO.getCourseIds()) {
            idempotentExecutor.execute("learning.lesson.refund",
                    "order:" + orderBasicDTO.getOrderId() + ":course:" + courseId, message,
                    () -> lessonService.deleteUserLessons(orderBasicDTO.getUserId(), Collections.singletonList(courseId)));
        }
    }

    private void validateOrder(OrderBasicDTO order) {
        if (order == null || order.getOrderId() == null || order.getUserId() == null
                || order.getCourseIds() == null || order.getCourseIds().isEmpty()
                || order.getCourseIds().stream().anyMatch(Objects::isNull)
                || new HashSet<>(order.getCourseIds()).size() != order.getCourseIds().size()) {
            throw new BadRequestException("课程订单消息缺少业务身份或课程列表无效");
        }
    }
}
