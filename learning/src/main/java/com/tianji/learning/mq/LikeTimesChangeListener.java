package com.tianji.learning.mq;

import com.tianji.api.dto.remark.LikedTimesDTO;
import com.tianji.common.autoconfigure.mq.idempotent.MqIdempotentConfiguration;
import com.tianji.common.autoconfigure.mq.idempotent.MqIdempotentExecutor;
import com.tianji.common.exceptions.BadRequestException;
import com.tianji.common.exceptions.DbException;
import com.tianji.learning.domain.po.InteractionReply;
import com.tianji.learning.service.IInteractionReplyService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.ExchangeTypes;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.Exchange;
import org.springframework.amqp.rabbit.annotation.Queue;
import org.springframework.amqp.rabbit.annotation.QueueBinding;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;
import org.springframework.context.annotation.Import;

import java.util.ArrayList;
import java.util.List;

import static com.tianji.common.constants.MqConstants.Exchange.LIKE_RECORD_EXCHANGE;
import static com.tianji.common.constants.MqConstants.Key.QA_LIKED_TIMES_KEY;

@Component
@Slf4j
@RequiredArgsConstructor
@Import(MqIdempotentConfiguration.class)
public class LikeTimesChangeListener {

    private final IInteractionReplyService replyService;
    private final MqIdempotentExecutor idempotentExecutor;

    /*    @RabbitListener(bindings = @QueueBinding(
                value = @Queue(name = "qa.liked.times.queue", durable = "true"),
                exchange = @Exchange(name = LIKE_RECORD_EXCHANGE, type = ExchangeTypes.TOPIC),
                key = QA_LIKED_TIMES_KEY
        ))
        public void listenReplyLikedTimesChange(LikedTimesDTO dto) {
            log.debug("监听到回答或评论{}的点赞数变更:{}", dto.getBizId(), dto.getLikedTimes());
            InteractionReply reply = new InteractionReply();
            reply.setId(dto.getBizId());
            reply.setLikedTimes(dto.getLikedTimes());
            replyService.updateById(reply);
        }*/

    @RabbitListener(bindings = @QueueBinding(
            value = @Queue(name = "qa.liked.times.queue", durable = "true"),
            exchange = @Exchange(name = LIKE_RECORD_EXCHANGE, type = ExchangeTypes.TOPIC),
            key = QA_LIKED_TIMES_KEY
    ))
    public void listenReplyLikedTimesChange(List<LikedTimesDTO> likedTimesDTOs, Message message){
        log.debug("监听到回答或评论的点赞数变更");

        if (likedTimesDTOs == null || likedTimesDTOs.isEmpty()) {
            throw new BadRequestException("点赞数消息为空");
        }

        List<InteractionReply> list = new ArrayList<>(likedTimesDTOs.size());
        for (LikedTimesDTO dto : likedTimesDTOs) {
            if (dto == null || dto.getBizId() == null || dto.getLikedTimes() == null || dto.getLikedTimes() < 0) {
                throw new BadRequestException("点赞数消息数据无效");
            }
            InteractionReply r = new InteractionReply();
            r.setId(dto.getBizId());
            r.setLikedTimes(dto.getLikedTimes());
            list.add(r);
        }
        // 当前快照没有业务版本，只能保证同一 messageId 重投不再次覆盖较新的数据。
        idempotentExecutor.execute("learning.qa.liked.times", MqIdempotentExecutor.messageKey(message), message, () -> {
            if (!replyService.updateBatchById(list)) {
                throw new DbException("点赞数批量更新失败");
            }
        });
    }
}
