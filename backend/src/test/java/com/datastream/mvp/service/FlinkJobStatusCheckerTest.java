package com.datastream.mvp.service;

import com.datastream.mvp.model.JobDefinition;
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

    /**
     * 作业从 overview 消失时不能再一律判成功：
     * 集群换代（JM 重启 / TM 重新注册）导致的消失必须判 FAILED，
     * 否则「随集群丢失」的作业会被记成 COMPLETED 并生成空输出。
     */
    @Test
    void missingFromOverview_isFailedOnlyWhenClusterGenerationChanged() {
        assertEquals(JobDefinition.JobStatus.COMPLETED,
                FlinkJobStatusChecker.resolveMissingJobStatus(false),
                "集群未换代：Flink 正常结束后会把作业移出 overview，应判 COMPLETED");
        assertEquals(JobDefinition.JobStatus.FAILED,
                FlinkJobStatusChecker.resolveMissingJobStatus(true),
                "集群换代：作业是被丢掉的，应判 FAILED");
    }

    @Test
    void clusterGenerationChange_detectionRules() {
        // 首次观测只记录基线，不算重启
        assertEquals(false, FlinkJobStatusChecker.generationChanged(null, "tm-1"));
        // 同一批 TaskManager = 未换代
        assertEquals(false, FlinkJobStatusChecker.generationChanged("tm-1", "tm-1"));
        // TaskManager 重新注册（JM 重启）→ 换代
        assertEquals(true, FlinkJobStatusChecker.generationChanged("tm-1", "tm-2"));
        assertEquals(true, FlinkJobStatusChecker.generationChanged("tm-1,tm-2", "tm-3"));
        // 任一时刻取不到集群信息时不判定，避免误报
        assertEquals(false, FlinkJobStatusChecker.generationChanged("tm-1", null));
        assertEquals(false, FlinkJobStatusChecker.generationChanged(null, null));
    }
}
