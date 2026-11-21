# 项目 MQ 使用情况报告

## 1. 总览

项目使用 RabbitMQ（Spring AMQP），主要用于跨微服务业务事件通知、延迟查单、异步积分处理，以及课程、订单和点赞事件发布。消息交换机、Routing Key 集中定义在 `common/.../MqConstants.java`；公共发送和监听器配置在 `common/.../mq`。

当前代码中有 9 个有效 `@RabbitListener` 入口，其中交易 3 个、学习 5 个、促销 1 个。点赞统计旧监听方法仍留在注释中，不计为有效入口。

## 2. 业务链路

### 2.1 支付成功后开课

1. 支付服务收到支付宝/微信回调后校验支付单号、金额、支付状态和通知数据，再向 `pay.topic` 发布 `pay.success`，消息体为 `PayResultDTO`，包含业务订单号、支付单号、渠道和成功时间。主动查单确认支付成功也会发布同一事件。
2. 交易服务的 `trade.pay.success.queue` 消费支付事件，调用订单支付成功处理：将待支付订单改为已支付、更新订单明细，随后生成 `order.topic` / `order.pay` 事件。零元报名也会直接产生订单报名事件。
3. `learning.lesson.pay.queue` 将订单中的课程加入用户课表。

订单事件载荷为 `OrderBasicDTO`，含订单 ID、用户 ID、课程 ID 列表和完成/支付时间。学习消费者通过消费记录和数据库事务幂等。

### 2.2 退款结果和退课

1. 支付服务处理第三方退款回调或退款主动查询结果，发布 `pay.topic` / `refund.status.change`，载荷 `RefundResultDTO` 表示成功/失败及业务退款单、支付单信息。
2. `trade.refund.result.queue` 更新退款申请和订单明细状态。只有退款成功才发 `order.topic` / `order.refund`，载荷仍为 `OrderBasicDTO`，课程列表通常是本次退款对应课程。
3. `learning.lesson.refund.queue` 从课表移除退款课程。

交易端以订单/退款状态条件更新抵御重复状态迁移；学习端按订单和课程维度记录消费。退款与支付状态冲突会进入异常处理，不应被当成普通重复事件忽略。

### 2.3 发布课程状态变化事件

课程服务在课程上架、下架、购买期完结和删除时，分别发 `course.topic` 上的 `course.up`、`course.down`、`course.expire`、`course.delete`。当前代码通过 Outbox 记录这些事件；消息体是课程 ID。仓库内没有这些事件的消费者。

### 2.4 签到积分和问答积分

- 学习服务完成签到校验并计算连续签到奖励后，向 `learning.topic` / `sign.in` 发布 `SignInMessage(userId, points)`。基本签到 1 分，连续 7/14/28 天额外奖励分别为 10/20/40 分。`sign.points.queue` 消费后写入积分记录。
- `qa.points.queue` 订阅 `learning.topic` / `reply.new`，收到用户 ID 后记 5 分问答积分。当前扫描到常量和消费者，未找到本仓库中对应的有效生产调用点。

签到/问答积分处理涉及数据库积分记录和 Redis 榜单等多种状态，现有消费幂等文档明确指出不能仅靠本地数据库事务保证跨 MySQL/Redis 原子性；消息也缺少足够稳定的业务身份字段来完整防止不同 messageId 的业务重复。

### 2.5 点赞总数同步到学习服务

点赞服务统计回答/笔记点赞数后向 `like.record.topic` 发布点赞快照。回答使用 `QA.times.changed`，载荷为 `LikedTimesDTO` 列表；当前工作区代码的统计链路将快照写入 Outbox。`qa.liked.times.queue` 批量更新学习服务中的问答点赞数，并按 messageId 防止同一传输消息重复覆盖。

旧代码还保留直接发单条 `LikedTimesDTO` 的路径；`LikedRecordServiceRedisImpl` 中该路径当前被注释。问答消费者只订阅 `QA.times.changed`，笔记 Routing Key 虽有模板定义，但没有发现相应消费者。由于消息没有实体版本，第一次迟到的旧快照仍可能覆盖新值；按最大值过滤也不正确，因为取消点赞会降低计数。

### 2.6 异步领券

促销服务用 Redis/Lua 校验并预占优惠券后，向 `promotion.topic` / `coupon.receive` 发布 `UserCouponDTO`。普通领券消息包含用户 ID、券 ID 和 `receiveRequestId`；兑换码领券还带兑换序列号 `serialNum`。`coupon.receive.queue` 消费后，在数据库事务中生成用户券记录并更新兑换码状态。

消费者以 `receiveRequestId` 或兑换序列号建立幂等键，兼容旧消息时退回 messageId。Redis 预占和 RabbitMQ 发布不是同一事务；若发布失败，预占补偿/可靠发送仍需关注。

### 2.7 延迟支付查单

订单创建后，交易服务创建有 generation/round 状态的查单任务，并将每轮任务写入 Outbox。消息使用 `trade.query.topic` / `delay.order.query`，按每轮延迟逐次调度。`trade.delay.order.query` 同时绑定延迟交换机 `trade.delay.topic`（需要 RabbitMQ delayed-message 插件）和普通交换机 `trade.query.topic`，最终由同一个处理器调用支付查询。

若仍未支付，任务推进到下一轮；确认成功则走支付成功处理，达到最后一轮或订单不再有效时结束任务。定时恢复逻辑会扫描近期仍待支付但缺少查单任务的订单。旧延迟交换机路径仍保留为兼容绑定；当前调度代码使用带 Outbox 的普通交换机路径。

## 3. Exchange、Routing Key 和队列

| Exchange | Routing Key | 消费队列 | 主要用途 |
| --- | --- | --- | --- |
| `pay.topic` | `pay.success` | `trade.pay.success.queue` | 支付成功通知交易服务 |
| `pay.topic` | `refund.status.change` | `trade.refund.result.queue` | 退款结果通知交易服务 |
| `order.topic` | `order.pay` | `learning.lesson.pay.queue` | 加入课表 |
| `order.topic` | `order.refund` | `learning.lesson.refund.queue` | 退课 |
| `course.topic` | `course.up`、`course.down`、`course.expire`、`course.delete` | 仓库内无消费者 | 发布课程状态变化 |
| `learning.topic` | `sign.in` | `sign.points.queue` | 签到积分入账 |
| `learning.topic` | `reply.new` | `qa.points.queue` | 问答积分入账，未发现本地生产端 |
| `like.record.topic` | `QA.times.changed` | `qa.liked.times.queue` | 同步问答点赞总数 |
| `promotion.topic` | `coupon.receive` | `coupon.receive.queue` | 普通领券和兑换码领券落库 |
| `trade.query.topic` | `delay.order.query` | `trade.delay.order.query` | Outbox 驱动的延迟查单 |
| `trade.delay.topic` | `delay.order.query` | `trade.delay.order.query` | 旧延迟插件交换机兼容绑定 |

`MqConstants` 还定义了但在当前有效发送/监听代码中未形成业务闭环的键包括 `course.new`、部分学习行为积分键（`section.learned`、`note.new`、`note.gathered`）和笔记点赞键。通用错误交换机由公共 MQ 配置创建，不属于业务事件链路。

## 4. 公共机制和可靠性边界

- 消息体默认使用 Jackson JSON 转换；`MqConfig` 开启自动生成 messageId，并为监听器恢复请求 ID 上下文。
- `RabbitMqHelper.send()` 同步调用 `convertAndSend` 并设置 CorrelationData/messageId；它本身不等待并核实 publisher confirm。`sendAsync()` 捕获异常后只记日志，因此不能视作持久化补发机制。
- 监听容器失败恢复使用 Spring AMQP 的 `RepublishMessageRecoverer`，重试耗尽后投递到 `error.topic`，按应用名路由至 `error.<应用名>.queue`。最终 ACK、重试次数/间隔、死信处理等运行参数由 Nacos `shared-mq.yaml` 等外部配置决定，仓库没有该配置正文，无法据源码确认线上参数。
- 课程、交易和当前点赞统计的部分写库后发消息链路使用 Outbox：业务状态和发送记录先在同一数据库事务提交，由 `OutboxDispatcher` 定时抢占、发送并等待 broker confirm，再标记已发布；失败保留重试。它提供最终补发，不表示下游业务已经成功，也不覆盖所有生产端。
- 学习课表、促销领券、问答点赞消费者使用 `MqIdempotentExecutor`，将消费凭据和数据库业务操作放入同一事务。交易支付/退款则通过状态条件更新保护关键状态迁移。
- Outbox/数据库消费记录无法让 RabbitMQ、MySQL 和 Redis成为一个原子事务。消息可能重复投递；仍需逐链路确认稳定业务事件键、ACK/重试配置、错误队列回放策略，以及涉及 Redis/外部调用时的补偿和对账。

## 5. 建议重点核查

1. 核对 Nacos 中 RabbitMQ listener ACK、retry 和错误消息路由配置，以及 RabbitMQ 是否启用了 delayed-message 插件；尤其确认 `trade.delay.topic` 的旧绑定是否仍需要。
2. 确认 `reply.new`、笔记/学习行为积分和笔记点赞事件的生产者是否在其他服务或部署版本中；当前仓库内没有完整闭环。
3. 对支付/退款、券领取、积分和点赞事件继续核对 Outbox 覆盖范围、消费幂等键、Redis 补偿及错误队列人工回放流程。

## 6. 主要源码入口

- 公共常量和基础设施：`common/src/main/java/com/tianji/common/constants/MqConstants.java`、`common/src/main/java/com/tianji/common/autoconfigure/mq/`
- 支付和交易：`pay/pay-service/src/main/java/com/tianji/pay/service/impl/NotifyServiceImpl.java`、`trade/src/main/java/com/tianji/trade/handler/PayMessageHandler.java`
- 订单和课表：`trade/src/main/java/com/tianji/trade/service/impl/OrderServiceImpl.java`、`learning/src/main/java/com/tianji/learning/mq/LessonChangeListener.java`
- 课程事件：`course/src/main/java/com/tianji/course/service/impl/`
- 积分和点赞：`learning/src/main/java/com/tianji/learning/mq/`、`remark/src/main/java/com/tianji/remark/service/impl/`
- 优惠券：`promotion/src/main/java/com/tianji/promotion/`
