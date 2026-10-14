package com.tianji.promotion.handel;

import com.tianji.promotion.domain.dto.UserCouponDTO;
import com.tianji.common.autoconfigure.mq.idempotent.MqIdempotentConfiguration;
import com.tianji.common.autoconfigure.mq.idempotent.MqIdempotentExecutor;
import com.tianji.common.exceptions.BadRequestException;
import com.tianji.promotion.service.IUserCouponService;
import lombok.RequiredArgsConstructor;
import org.springframework.amqp.core.ExchangeTypes;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.Exchange;
import org.springframework.amqp.rabbit.annotation.Queue;
import org.springframework.amqp.rabbit.annotation.QueueBinding;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;
import org.springframework.context.annotation.Import;
import org.springframework.util.StringUtils;

import static com.tianji.common.constants.MqConstants.Exchange.PROMOTION_EXCHANGE;
import static com.tianji.common.constants.MqConstants.Key.COUPON_RECEIVE_KEY;

@RequiredArgsConstructor
@Component
@Import(MqIdempotentConfiguration.class)
public class PromotionMqHandler {

    private final IUserCouponService userCouponService;
    private final MqIdempotentExecutor idempotentExecutor;

    @RabbitListener(bindings = @QueueBinding(
            value = @Queue(name = "coupon.receive.queue", durable = "true"),
            exchange = @Exchange(name = PROMOTION_EXCHANGE, type = ExchangeTypes.TOPIC),
            key = COUPON_RECEIVE_KEY
    ))
    public void listenCouponReceiveMessage(UserCouponDTO uc, Message message) {
        if (uc == null || uc.getUserId() == null || uc.getCouponId() == null) {
            throw new BadRequestException("领券消息缺少用户或优惠券信息");
        }
        String eventKey;
        if (uc.getSerialNum() != null) {
            eventKey = "exchange:" + uc.getSerialNum();
        } else if (StringUtils.hasText(uc.getReceiveRequestId())) {
            eventKey = "receive:" + uc.getReceiveRequestId();
        } else {
            // 兼容已有队列中的旧消息，仅保证原 messageId 的重复投递不重复发券。
            eventKey = MqIdempotentExecutor.messageKey(message);
        }
        idempotentExecutor.execute("promotion.coupon.receive", eventKey, message,
                () -> userCouponService.checkAndCreateUserCouponByMq(uc));
    }

}
