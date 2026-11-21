# 数据表、Redis 与 MQ 说明

本文依据仓库中的实体映射、SQL、Redis 调用及配置整理，不代表线上数据库或 Redis 实际全量清单。除 `docs/sql/` 中的脚本外，仓库未提供完整建表 DDL，字段类型、索引、约束和实际库名应以部署数据库为准；实体字段可在对应模块的 `domain/po` 或 `domain/pojo` 查看。

## 数据表

逐表字段、Java 类型及源码注释见 [数据表字段字典](数据表字段字典.md)。

| 模块 | 表及用途 |
| --- | --- |
| 认证 | `account_role` 账号角色关联；`role` 角色；`role_menu` 角色菜单关联；`role_privilege` 角色权限关联；`privilege` 权限；`menu` 菜单；`login_record` 登录记录 |
| 用户 | `user` 用户；`user_detail` 用户详情 |
| 课程 | `course` 课程；`course_draft` 课程草稿；`course_content`、`course_content_draft` 课程内容及草稿；`course_catalogue`、`course_catalogue_draft` 课程目录及草稿；`course_teacher`、`course_teacher_draft` 课程教师及草稿；`course_cata_subject`、`course_cata_subject_draft` 目录题目及草稿；`course_subject` 课程题目；`subject` 题目；`subject_category` 题目分类；`category` 课程分类 |
| 学习 | `learning_lesson` 用户课表；`learning_record` 学习记录；`interaction_question` 问答问题；`interaction_reply` 问答回复；`points_record` 积分记录；`points_board` 积分榜；`points_board_season` 榜单赛季 |
| 交易 | `cart` 购物车；`order` 订单；`order_detail` 订单明细；`refund_apply` 退款申请 |
| 支付 | `pay_order` 支付单；`refund_order` 退款单；`pay_channel` 支付渠道 |
| 促销 | `promotion` 促销活动；`coupon` 优惠券；`coupon_scope` 优惠券适用范围；`user_coupon` 用户券；`exchange_code` 兑换码 |
| 媒资 | `media` 媒资；`file` 文件 |
| 点赞 | `liked_record` 点赞记录 |

可靠消息、消费幂等和支付查单还涉及公共基础表，具体字段和建表语句见 [mq-outbox.sql](sql/mq-outbox.sql)、[mq-consume-log.sql](sql/mq-consume-log.sql)、[pay-query-task.sql](sql/pay-query-task.sql)。部分业务表可能按运行时规则分表，须结合映射及实际库核对。

## Redis 数据结构

以下为代码中可识别的主要键；`<...>` 表示运行时拼接部分。过期时间仅在明确说明时保证，其他项需查对应实现或运行环境。

| Key / 前缀 | 类型 | 内容和用途 |
| --- | --- | --- |
| `jwt:uid:<userId>` | String | 当前用户 JWT 的 jti，TTL 与令牌有效期关联 |
| `version` | String | 权限缓存版本计数 |
| `sign:uid:<userId>:<yyyyMM>` | Bitmap | 每位表示当月一天的签到状态 |
| `boards:<yyyyMM>` | ZSet | 积分榜，member 为用户 ID，score 为积分 |
| `likes:set:biz:<bizId>` | Set | 对业务对象点赞的用户 ID 集合 |
| `likes:times:type:<type>` | ZSet | 待同步点赞数，member 为业务 ID，score 为点赞数 |
| `prs:coupon:<couponId>` | Hash | 优惠券库存、限领等缓存，设置过期时间 |
| `prs:user:coupon:<couponId>` | Hash | Lua 领券校验中的用户领券数量 |
| `coupon:code:serial` | String | 兑换码序列号计数 |
| `coupon:code:map` | Bitmap | 兑换码使用状态 |
| `coupon:code:range` | ZSet | 券 ID 与兑换码序列号范围映射 |

学习记录延迟处理还使用 Hash 缓存章节记录；Redisson 在促销及公共锁组件中维护分布式锁，其内部键和值由 Redisson 管理。课程模块还定义 `CATEGORY:THIRD_NUMBER`、`COURSE:COURSE_NUM_CATEGORY` 等缓存常量，是否生效及底层类型应以对应缓存注解和配置为准。Redis 连接参数同样可能由外部配置提供。

## MQ 配置与业务

项目使用 RabbitMQ 和 Spring AMQP。多数服务通过 Nacos 引入 `shared-mq.yaml`；其正文不在仓库中，因此 broker 地址、凭据、ACK、重试次数与间隔等实际值无法从仓库确定。`course`、`trade`、`remark` 的本地 bootstrap 配置声明 `publisher-confirm-type: correlated`。公共配置使用 Jackson JSON 消息转换和 messageId；重试耗尽后通过 `error.topic` 转发至 `error.<应用名>.queue`。

| Exchange | Routing Key | 队列 | 用途 |
| --- | --- | --- | --- |
| `pay.topic` | `pay.success`、`refund.status.change` | `trade.pay.success.queue`、`trade.refund.result.queue` | 支付和退款结果 |
| `order.topic` | `order.pay`、`order.refund` | `learning.lesson.pay.queue`、`learning.lesson.refund.queue` | 课表更新 |
| `course.topic` | `course.up`、`course.down`、`course.expire`、`course.delete` | 仓库内无消费者 | 课程状态变化 |
| `learning.topic` | `sign.in`、`reply.new` | `sign.points.queue`、`qa.points.queue` | 签到和问答积分 |
| `like.record.topic` | `QA.times.changed` | `qa.liked.times.queue` | 问答点赞数同步 |
| `promotion.topic` | `coupon.receive` | `coupon.receive.queue` | 异步领券落库 |
| `trade.query.topic`、`trade.delay.topic` | `delay.order.query` | `trade.delay.order.query` | 延迟支付查单；后者为旧延迟插件绑定 |

部分课程、交易及点赞事件采用数据库 Outbox 和 broker confirm 实现补发；部分消费者有数据库幂等保护，覆盖范围并非全部链路。仓库内没有课程状态事件的消费者；`reply.new` 未发现仓库内有效生产端。延迟交换机兼容绑定需要 RabbitMQ delayed-message 插件。详细链路、可靠性边界及源码入口见 [MQ 使用情况报告](mq-usage-report.md)。

### 消息格式

公共消息转换器使用 Jackson JSON。下表描述消息体，而非 AMQP 消息属性：`messageId`、内容类型和可能存在的类型头等属于消息属性，具体值取决于发送路径。示例仅展示结构，时间格式和空字段是否出现以实际序列化配置及发送端赋值为准。

| 路由键 | 消息体 | 字段说明 |
| --- | --- | --- |
| `pay.success` | `PayResultDTO` 对象 | `status` 支付状态（1 支付中、2 失败、3 成功）；`msg` 结果说明；`bizOrderId` 业务订单号；`payOrderNo` 支付单号；`payChannel` 支付渠道；`successTime` 成功时间 |
| `refund.status.change` | `RefundResultDTO` 对象 | `status` 退款状态（1 处理中、2 失败、3 成功）；`msg` 说明；`bizPayOrderId` 业务支付订单号；`bizRefundOrderId` 业务退款单号；`payOrderNo` 支付流水单号；`refundOrderNo` 退款交易单号；`payChannel`、`refundChannel` 分别为支付及退款渠道 |
| `order.pay`、`order.refund` | `OrderBasicDTO` 对象 | `orderId` 订单 ID；`userId` 用户 ID；`courseIds` 课程 ID 数组；`finishTime` 完成时间 |
| `course.up`、`course.down`、`course.expire`、`course.delete` | Long 标量 | 课程 ID，例如 `123`；不是包含课程字段的对象 |
| `sign.in` | `SignInMessage` 对象 | `userId` 用户 ID；`points` 本次签到积分 |
| `reply.new` | Long 标量 | 用户 ID；仓库内未发现有效生产端，格式依据消费者 |
| `QA.times.changed` | `LikedTimesDTO` 数组 | 每项 `bizId` 为业务 ID，`likedTimes` 为当前点赞总数；当前统计链路发送批量快照 |
| `coupon.receive` | `UserCouponDTO` 对象 | `receiveRequestId` 普通领券请求标识；`userId` 用户 ID；`couponId` 券 ID；`serialNum` 兑换码序列号，普通领券可为空 |
| `delay.order.query` | `OrderDelayQueryDTO` 对象 | `orderId` 订单 ID；`queryGeneration` 查询任务代次；`queryRound` 查询轮次；`delayMillis` 旧延迟链路的剩余延迟列表（毫秒）；代次和轮次均为空表示旧消息 |

示例（字段值仅供理解，不代表实际业务数据）：

```json
{"orderId": 1001, "userId": 2001, "courseIds": [301, 302], "finishTime": "2026-11-07T10:00:00"}
```

```json
[{"bizId": 101, "likedTimes": 8}, {"bizId": 102, "likedTimes": 3}]
```

```json
{"templateCode": "login", "phones": ["13800000000"], "templateParams": {"code": "123456"}}
```

字段定义分别位于 `pay/pay-domain/.../dto/`、`api/.../dto/`、`learning/.../mq/msg/SignInMessage.java`、`promotion/.../domain/dto/UserCouponDTO.java` 和 `trade/.../domain/dto/OrderDelayQueryDTO.java`。同一 DTO 在不同发送分支中可只填充部分字段，消费端不应仅凭示例假定所有字段非空。
