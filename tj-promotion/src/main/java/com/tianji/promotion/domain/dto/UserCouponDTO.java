package com.tianji.promotion.domain.dto;

import lombok.Data;

@Data
public class UserCouponDTO {
    /**
     * 同一次普通领券请求的唯一标识，发布重试必须沿用此值。
     */
    private String receiveRequestId;
    /**
     * 用户id
     */
    private Long userId;
    /**
     * 优惠券id
     */
    private Long couponId;
    /**
     * 兑换码序列号（id）
     */
    private Long serialNum;
}
