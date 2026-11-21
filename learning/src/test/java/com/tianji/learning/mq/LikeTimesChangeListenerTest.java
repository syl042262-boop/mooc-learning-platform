package com.tianji.learning.mq;

import com.tianji.api.dto.remark.LikedTimesDTO;
import com.tianji.common.autoconfigure.mq.idempotent.MqIdempotentExecutor;
import com.tianji.learning.domain.po.InteractionReply;
import com.tianji.learning.service.IInteractionReplyService;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.Mockito.*;

class LikeTimesChangeListenerTest {

    @Test
    void replayingAnAppliedSnapshotDoesNotOverwriteANewerSnapshot() {
        DriverManagerDataSource dataSource = new DriverManagerDataSource(
                "jdbc:h2:mem:" + UUID.randomUUID() + ";MODE=MySQL;DB_CLOSE_DELAY=-1", "sa", "");
        JdbcTemplate jdbc = new JdbcTemplate(dataSource);
        jdbc.execute("CREATE TABLE mq_consume_log (consumer_id VARCHAR(64), event_key VARCHAR(192), " +
                "message_id VARCHAR(255), payload BLOB, created_at TIMESTAMP, PRIMARY KEY (consumer_id, event_key))");
        jdbc.execute("CREATE TABLE reply (id BIGINT PRIMARY KEY, liked_times INT)");
        jdbc.update("INSERT INTO reply VALUES (1, 0)");
        IInteractionReplyService service = mock(IInteractionReplyService.class);
        when(service.updateBatchById(anyCollection())).thenAnswer(call -> {
            for (InteractionReply reply : call.<List<InteractionReply>>getArgument(0)) {
                jdbc.update("UPDATE reply SET liked_times = ? WHERE id = ?", reply.getLikedTimes(), reply.getId());
            }
            return true;
        });
        LikeTimesChangeListener listener = new LikeTimesChangeListener(service,
                new MqIdempotentExecutor(jdbc, new DataSourceTransactionManager(dataSource)));
        listener.listenReplyLikedTimesChange(Collections.singletonList(LikedTimesDTO.of(1L, 10)), message("old"));
        listener.listenReplyLikedTimesChange(Collections.singletonList(LikedTimesDTO.of(1L, 8)), message("new"));
        listener.listenReplyLikedTimesChange(Collections.singletonList(LikedTimesDTO.of(1L, 10)), message("old"));
        assertEquals(8, jdbc.queryForObject("SELECT liked_times FROM reply WHERE id = 1", Integer.class));
        verify(service, times(2)).updateBatchById(anyCollection());
    }

    private Message message(String id) {
        MessageProperties properties = new MessageProperties();
        properties.setMessageId(id);
        return new Message(new byte[0], properties);
    }
}
