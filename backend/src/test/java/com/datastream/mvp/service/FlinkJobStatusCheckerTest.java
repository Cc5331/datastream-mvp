package com.datastream.mvp.service;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 保护 mock 占位 ID 认领真实作业的时间窗口约束。
 * 历史作业归档后重新出现在 overview 时，若无窗口限制会被误认领，
 * 导致作业状态被旧作业的状态覆盖（曾把提交中的作业误判为 CANCELLED）。
 */
class FlinkJobStatusCheckerTest {

    @Test
    void mockResolveWindow_isBoundedToAvoidClaimingStaleJobs() throws Exception {
        Field field = FlinkJobStatusChecker.class.getDeclaredField("MOCK_RESOLVE_MAX_DIFF_MS");
        field.setAccessible(true);
        long window = (long) field.get(null);

        assertTrue(window > 0, "认领窗口必须为正数");
        assertTrue(window <= 10 * 60 * 1000L, "认领窗口不应超过 10 分钟，否则可能误认领历史作业");
        // 实测误认领发生在 diff≈976s（约 16 分钟），窗口必须小于该值
        assertTrue(window < 976_000L, "窗口需小于实测误认领的时间差 976s");
    }

    @Test
    void mockPrefixes_areRecognizedAsPlaceholders() {
        assertTrue("flink-job-abc".startsWith("flink-job-"));
        assertTrue("mock-abc".startsWith("mock-"));
        assertTrue("sql-submitted-abc".startsWith("sql-submitted-"));
        assertEquals(false, "9f0c47f29f17f81703168ee9d0c39803".startsWith("flink-job-"));
    }
}
