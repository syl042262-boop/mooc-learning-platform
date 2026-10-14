package com.tianji.course.service.impl;

import com.baomidou.mybatisplus.core.MybatisConfiguration;
import com.baomidou.mybatisplus.core.conditions.Wrapper;
import com.baomidou.mybatisplus.core.metadata.TableInfoHelper;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.tianji.common.autoconfigure.mq.outbox.OutboxService;
import com.tianji.course.constants.CourseStatus;
import com.tianji.course.domain.po.Course;
import com.tianji.course.domain.po.CourseDraft;
import com.tianji.course.mapper.CourseMapper;
import com.tianji.course.service.ICourseDraftService;
import org.apache.ibatis.builder.MapperBuilderAssistant;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.aop.framework.ProxyFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.transaction.annotation.AnnotationTransactionAttributeSource;
import org.springframework.transaction.interceptor.TransactionInterceptor;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class CourseOutboxTest {
    private JdbcTemplate jdbc;
    private CourseServiceImpl service;

    @BeforeEach
    void setUp() throws Exception {
        DriverManagerDataSource ds = new DriverManagerDataSource("jdbc:h2:mem:" + UUID.randomUUID()
                + ";MODE=MySQL;DB_CLOSE_DELAY=-1", "sa", "");
        jdbc = new JdbcTemplate(ds);
        String ddl = Files.readString(Path.of("..", "docs", "sql", "mq-outbox.sql"))
                .replace("CHARACTER SET ascii COLLATE ascii_bin", "")
                .replace("ENGINE=InnoDB DEFAULT CHARSET=utf8mb4", "");
        for (String sql : ddl.split(";")) if (!sql.trim().isEmpty()) jdbc.execute(sql);
        jdbc.execute("CREATE TABLE course_test (id BIGINT PRIMARY KEY,status INT,purchase_end_time TIMESTAMP)");
        jdbc.execute("CREATE TABLE draft_test (id BIGINT PRIMARY KEY)");
        jdbc.update("INSERT INTO course_test VALUES (1,?,?)", CourseStatus.SHELF.getStatus(), LocalDateTime.now().minusDays(1));
        jdbc.update("INSERT INTO draft_test VALUES (1)");
        ICourseDraftService drafts = mock(ICourseDraftService.class);
        when(drafts.getById(1L)).thenAnswer(call -> jdbc.queryForObject("SELECT COUNT(*) FROM draft_test", Integer.class) == 0
                ? null : new CourseDraft().setId(1L));
        doAnswer(call -> { jdbc.update("DELETE FROM draft_test WHERE id=?", (Long) call.getArgument(0)); return null; })
                .when(drafts).delete(anyLong());
        CourseMapper courses = mock(CourseMapper.class);
        TableInfoHelper.initTableInfo(new MapperBuilderAssistant(new MybatisConfiguration(), "test"), Course.class);
        when(courses.selectList(any(Wrapper.class))).thenAnswer(call -> jdbc.query(
                "SELECT * FROM course_test WHERE status=?", (rs, row) -> new Course().setId(rs.getLong(1))
                        .setStatus(rs.getInt(2)).setPurchaseEndTime(rs.getTimestamp(3).toLocalDateTime()),
                CourseStatus.SHELF.getStatus()));
        when(courses.update(any(Course.class), any(Wrapper.class))).thenAnswer(call -> {
            Course changed = call.getArgument(0);
            return jdbc.update("UPDATE course_test SET status=? WHERE id=? AND status=?",
                    changed.getStatus(), changed.getId(), CourseStatus.SHELF.getStatus());
        });
        ObjectMapper mapper = new ObjectMapper().findAndRegisterModules();
        CourseServiceImpl target = new CourseServiceImpl();
        ReflectionTestUtils.setField(target, "baseMapper", courses);
        ReflectionTestUtils.setField(target, "courseDraftService", drafts);
        ReflectionTestUtils.setField(target, "outboxService",
                new OutboxService(jdbc, new Jackson2JsonMessageConverter(mapper), mapper));
        ProxyFactory proxy = new ProxyFactory(target);
        proxy.setProxyTargetClass(true);
        proxy.addAdvice(new TransactionInterceptor(new DataSourceTransactionManager(ds), new AnnotationTransactionAttributeSource()));
        service = (CourseServiceImpl) proxy.getProxy();
    }

    @Test
    void draftDeletionAndNotificationCommitTogetherAndAReplayDoesNotCreateAnotherEvent() {
        service.delete(1L);
        service.delete(1L);
        assertEquals(0, jdbc.queryForObject("SELECT COUNT(*) FROM draft_test", Integer.class));
        assertEquals(1, jdbc.queryForObject("SELECT COUNT(*) FROM mq_outbox", Integer.class));
        assertEquals("COURSE_DELETE", jdbc.queryForObject("SELECT event_type FROM mq_outbox", String.class));
    }

    @Test
    void failedOutboxInsertRollsBackDraftDeletionAndEventVersion() {
        jdbc.execute("DROP TABLE mq_outbox");
        assertThrows(org.springframework.dao.DataAccessException.class, () -> service.delete(1L));
        assertEquals(1, jdbc.queryForObject("SELECT COUNT(*) FROM draft_test", Integer.class));
        assertEquals(0, jdbc.queryForObject("SELECT COUNT(*) FROM mq_outbox_sequence", Integer.class));
    }

    @Test
    void finishedStatusCleanupAndNotificationCommitTogether() {
        assertEquals(1, service.courseFinished());
        assertEquals(0, service.courseFinished());
        assertEquals(CourseStatus.FINISHED.getStatus(), jdbc.queryForObject("SELECT status FROM course_test", Integer.class));
        assertEquals(0, jdbc.queryForObject("SELECT COUNT(*) FROM draft_test", Integer.class));
        assertEquals("COURSE_FINISHED", jdbc.queryForObject("SELECT event_type FROM mq_outbox", String.class));
    }

    @Test
    void failedFinishedNotificationRollsBackStatusAndCleanup() {
        jdbc.execute("DROP TABLE mq_outbox");
        assertThrows(org.springframework.dao.DataAccessException.class, () -> service.courseFinished());
        assertEquals(CourseStatus.SHELF.getStatus(), jdbc.queryForObject("SELECT status FROM course_test", Integer.class));
        assertEquals(1, jdbc.queryForObject("SELECT COUNT(*) FROM draft_test", Integer.class));
    }
}
