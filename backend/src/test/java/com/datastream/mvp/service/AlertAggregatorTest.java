package com.datastream.mvp.service;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 告警聚合降噪回归：签名归一化、窗口内合并、窗口过期、自动诊断配额。
 *
 * <p>背景（真实痛点）：后端重启时几十个历史 FAILED 作业同时告警，根因是同一个
 * （同一个 SQL Gateway 挂了），旧逻辑会刷出几十条记录 + 几十封邮件。</p>
 */
class AlertAggregatorTest {

    private AlertAggregator aggregator;

    @BeforeEach
    void setUp() {
        aggregator = new AlertAggregator();
        ReflectionTestUtils.setField(aggregator, "windowMs", 600_000L);
        ReflectionTestUtils.setField(aggregator, "autoDiagnosisPerWindow", 3);
    }

    @Test
    void signatureIgnoresJobSpecificNoise() {
        String a = aggregator.signatureOf("JOB_FAILED",
                "作业失败，状态=FAILED，提交失败: Flink 提交失败：SQL Gateway 调用失败: Failed to fetchResults.");
        String b = aggregator.signatureOf("JOB_FAILED",
                "作业失败，状态=FAILED，提交失败: Flink 提交失败：SQL Gateway 调用失败: Failed to fetchResults.");
        assertEquals(a, b, "同一根因文本必须得到同一签名");

        // 作业名/#id 由调用方作为噪声传入（AlertService 会传），路径与数字由归一化处理
        String withNoise = aggregator.signatureOf("JOB_FAILED",
                "job=2800 作业 demo1 读取 D:\\code\\比赛\\output\\a.csv 失败，偏移 12345", List.of("demo1"));
        String otherNoise = aggregator.signatureOf("JOB_FAILED",
                "job=1466 作业 diag5 读取 D:\\code\\比赛\\output\\b.csv 失败，偏移 999", List.of("diag5"));
        assertEquals(withNoise, otherNoise, "不同作业/路径/数字的同类错误应聚合到一起");
    }

    @Test
    void differentEventsOrRootCausesStaySeparate() {
        assertNotEquals(
                aggregator.signatureOf("JOB_FAILED", "SQL Gateway 调用失败"),
                aggregator.signatureOf("THROUGHPUT_ZERO", "SQL Gateway 调用失败"),
                "事件类型不同不能聚合");
        assertNotEquals(
                aggregator.signatureOf("JOB_FAILED", "CSV 输入文件不存在"),
                aggregator.signatureOf("JOB_FAILED", "Checkpoint 超时"),
                "根因不同不能聚合");
    }

    @Test
    void mergesRepeatedAlertsWithinWindow() {
        String sig = aggregator.signatureOf("JOB_FAILED", "SQL Gateway 调用失败");
        assertNull(aggregator.findMergeable(sig), "首次告警没有可合并桶");

        AlertAggregator.Bucket bucket = aggregator.register(sig, 100L, 2800L, "demo1");
        assertEquals(1, bucket.occurrences());
        assertEquals(List.of("demo1(#2800)"), bucket.jobLabels());

        AlertAggregator.Bucket same = aggregator.findMergeable(sig);
        assertNotNull(same, "窗口内同签名必须可合并");
        assertEquals(100L, same.alertId(), "合并目标应是首条告警记录");

        aggregator.merge(same, 1466L, "diag5");
        aggregator.merge(same, 2154L, "diag6");
        assertEquals(3, same.occurrences(), "合并后应为 3 次");
        assertEquals(List.of("demo1(#2800)", "diag5(#1466)", "diag6(#2154)"), same.jobLabels());
        assertEquals(1, aggregator.activeBucketCount(), "同签名只应有一个桶");
    }

    @Test
    void expiresAfterWindow() {
        String sig = aggregator.signatureOf("JOB_FAILED", "SQL Gateway 调用失败");
        aggregator.register(sig, 100L, 1L, "a");
        // 窗口缩短到 0ms 模拟过期
        ReflectionTestUtils.setField(aggregator, "windowMs", 0L);
        assertNull(aggregator.findMergeable(sig), "窗口过期后必须开新记录");
        assertEquals(0, aggregator.activeBucketCount());
    }

    @Test
    void limitsAutoDiagnosisPerWindowToAvoidLlmStorm() {
        String sig = aggregator.signatureOf("JOB_FAILED", "SQL Gateway 调用失败");
        aggregator.register(sig, 100L, 1L, "a");
        assertTrue(aggregator.allowAutoDiagnosis(sig));
        assertTrue(aggregator.allowAutoDiagnosis(sig));
        assertTrue(aggregator.allowAutoDiagnosis(sig));
        assertFalse(aggregator.allowAutoDiagnosis(sig), "同一签名窗口内最多 3 次自动诊断");
        assertTrue(aggregator.allowAutoDiagnosis("别的签名"), "不同签名独立计数");
    }
}
