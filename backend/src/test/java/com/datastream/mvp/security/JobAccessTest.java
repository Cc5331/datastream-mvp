package com.datastream.mvp.security;

import com.datastream.mvp.model.JobDefinition;
import org.junit.jupiter.api.Test;
import org.springframework.web.server.ResponseStatusException;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 作业归属规则：ownerId 为空的作业不再对所有人开放（历史 bug：空归属即放行）。
 */
class JobAccessTest {

    private static final CurrentUser ADMIN = new CurrentUser(1L, "admin", "管理员", "ADMIN");
    private static final CurrentUser OPERATOR = new CurrentUser(7L, "op", "操作员", "OPERATOR");
    private static final CurrentUser VIEWER = new CurrentUser(9L, "viewer", "观察员", "VIEWER");

    private JobDefinition job(Long ownerId) {
        JobDefinition job = new JobDefinition();
        job.setId(100L);
        job.setName("测试作业");
        job.setOwnerId(ownerId);
        return job;
    }

    @Test
    void adminCanAccessAnyJob() {
        assertDoesNotThrow(() -> JobAccess.assertCanAccess(job(7L), ADMIN));
        assertDoesNotThrow(() -> JobAccess.assertCanAccess(job(null), ADMIN));
        assertTrue(JobAccess.canAccess(job(7L), ADMIN));
    }

    @Test
    void ownerCanAccessOwnJob() {
        assertDoesNotThrow(() -> JobAccess.assertCanAccess(job(7L), OPERATOR));
        assertTrue(JobAccess.canAccess(job(7L), OPERATOR));
    }

    @Test
    void otherUsersAreDenied() {
        ResponseStatusException ex = assertThrows(ResponseStatusException.class,
                () -> JobAccess.assertCanAccess(job(7L), VIEWER));
        assertEquals(403, ex.getStatusCode().value());
        assertFalse(JobAccess.canAccess(job(7L), VIEWER));
    }

    @Test
    void nullOwnerJobIsAdminOnly() {
        ResponseStatusException ex = assertThrows(ResponseStatusException.class,
                () -> JobAccess.assertCanAccess(job(null), OPERATOR));
        assertEquals(403, ex.getStatusCode().value());
        assertTrue(ex.getReason() != null && ex.getReason().contains("无归属人"));
        assertFalse(JobAccess.canAccess(job(null), VIEWER), "无归属作业不再对普通用户可见");
    }

    @Test
    void unauthenticatedIsRejectedAsUnauthorized() {
        ResponseStatusException ex = assertThrows(ResponseStatusException.class,
                () -> JobAccess.assertCanAccess(job(7L), null));
        assertEquals(401, ex.getStatusCode().value());
        assertFalse(JobAccess.canAccess(job(7L), null));
    }
}
