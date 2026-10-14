package com.tianji.remark.service.impl;

import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.tianji.api.dto.remark.LikedTimesDTO;
import com.tianji.common.autoconfigure.mq.outbox.OutboxService;
import com.tianji.common.exceptions.DbException;
import com.tianji.common.constants.MqConstants;
import com.tianji.common.utils.CollUtils;
import com.tianji.common.utils.StringUtils;
import com.tianji.remark.domain.po.LikedRecord;
import com.tianji.remark.domain.po.LikedStat;
import com.tianji.remark.mapper.LikedRecordMapper;
import com.tianji.remark.mapper.LikedStatMapper;
import com.tianji.remark.service.ILikedStatService;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
public class LikedStatServiceImpl extends ServiceImpl<LikedStatMapper, LikedStat> implements ILikedStatService {

    private final OutboxService outboxService;

    @Override
    @Transactional(rollbackFor = Exception.class)
    public void updateLikedTimes(String bizType, List<LikedTimesDTO> msg) {
        if (CollUtils.isEmpty(msg)) {
            return;
        }
        // 1.更新本地数据库
        List<Long> bizIds = msg.stream().map(LikedTimesDTO::getBizId).collect(Collectors.toList());
        // 1.1.查询旧数据
        List<LikedStat> oldStats = lambdaQuery().in(LikedStat::getBizId, bizIds).list();
        Map<Long, LikedStat> oldStatMap = new HashMap<>(0);
        if(CollUtils.isNotEmpty(oldStats)) {
            oldStatMap = oldStats.stream().collect(Collectors.toMap(LikedStat::getBizId, e -> e));
        }
        // 1.2.处理为PO
        List<LikedStat> list = new ArrayList<>(msg.size());
        for (LikedTimesDTO dto : msg) {
            // 判断bizId是否已经存在
            LikedStat stat = oldStatMap.get(dto.getBizId());
            if(stat == null){
                // 不存在，新增
                stat = new LikedStat();
                stat.setBizId(dto.getBizId());
                stat.setLikedTimes(dto.getLikedTimes());
                stat.setBizType(bizType);
            }else{
                // 已经存在，需要更新
                stat.setLikedTimes(dto.getLikedTimes());
            }
            list.add(stat);
        }
        // 1.3.批处理
        if (!saveOrUpdateBatch(list)) {
            throw new DbException("点赞统计批量保存失败");
        }

        // 2.发送MQ消息
        outboxService.record("LIKED_TIMES_SNAPSHOT", "snapshot:" + snapshotKey(bizType, msg),
                MqConstants.Exchange.LIKE_RECORD_EXCHANGE,
                StringUtils.format(MqConstants.Key.LIKED_TIMES_KEY_TEMPLATE, bizType),
                msg);
    }

    /** 同一批快照重试必须复用业务键；点赞总数下降也要保留在快照指纹中。 */
    private String snapshotKey(String bizType, List<LikedTimesDTO> msg) {
        String canonical = bizType + ":" + msg.stream()
                .sorted((left, right) -> Long.compare(left.getBizId(), right.getBizId()))
                .map(dto -> dto.getBizId() + "=" + dto.getLikedTimes())
                .collect(Collectors.joining(","));
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256")
                    .digest(canonical.getBytes(StandardCharsets.UTF_8));
            StringBuilder key = new StringBuilder(64);
            for (byte value : digest) {
                key.append(String.format("%02x", value));
            }
            return key.toString();
        } catch (Exception e) {
            throw new IllegalStateException("点赞快照指纹计算失败", e);
        }
    }
}
