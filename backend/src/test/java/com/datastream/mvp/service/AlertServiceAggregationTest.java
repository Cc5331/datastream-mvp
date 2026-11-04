package com.datastream.mvp.service;

import com.datastream.mvp.model.AlertRecord;
import com.datastream.mvp.model.JobDefinition;
import com.datastream.mvp.repository.AlertRecordRepository;
import com.datastream.mvp.repository.JobLogRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

/**
 * 告警入口的降噪行为回归：窗口内同根因告警不再发邮件/推 webhook，只累加计数；
 * 自动诊断按窗口配额触发（避免一波故障打几十个 LLM 请求）。
 */
class AlertServiceAggregationTest {

    private AlertRecordRepository alertRepo;
    private JobLogRepository logRepo;
    private AlertMailDispatcher mailDispatcher;
    private ApplicationEventPublisher eventPublisher;
    private AlertService alertService;
    private final AtomicLong idSeq = new AtomicLong(1);
    /** 内存版仓储：save 按自增 id 存，findById 读回同一条（合并路径依赖它） */
    private final java.util.Map<Long, AlertRecord> store = new java.util.HashMap<>();

    @BeforeEach
    void setUp() {
        alertRepo = mock(AlertRecordRepository.class);
        logRepo = mock(JobLogRepository.class);
        mailDispatcher = mock(AlertMailDispatcher.class);
        eventPublisher = mock(ApplicationEventPublisher.class);

        when(alertRepo.save(any(AlertRecord.class))).thenAnswer(inv -> {
            AlertRecord rec = inv.getArgument(0);
            if (rec.getId() == null) rec.setId(idSeq.getAndIncrement());
            store.put(rec.getId(), rec);
            return rec;
        });
        when(alertRepo.findById(any())).thenAnswer(inv -> Optional.ofNullable(store.get(inv.getArgument(0))));

        AlertAggregator aggregator = new AlertAggregator();
        ReflectionTestUtils.setField(aggregator, "windowMs", 600_000L);
        ReflectionTestUtils.setField(aggregator, "autoDiagnosisPerWindow", 2);

        alertService = new AlertService(new ObjectMapper(), logRepo, alertRepo, mailDispatcher,
                eventPublisher, aggregator);
    }

    private JobDefinition job(long id, String name) {
        JobDefinition job = new JobDefinition();
        job.setId(id);
        job.setName(name);
        job.setOwnerId(7L);
        return job;
    }

    @Test
    void repeatedRootCauseAlertsAreMergedWithoutExtraMail() {
        // 第一次：正常落库 + 发邮件
        alertService.sendAlert(job(2800L, "demo1"), "CRITICAL", "JOB_FAILED",
                "提交失败: SQL Gateway 调用失败: Failed to fetchResults.");
        AlertRecord firstRecord = captureSaved();
        assertEquals(1, firstRecord.getOccurrences());
        verify(mailDispatcher, times(1)).send(anyString(), anyString());

        // 第二次/第三次：不同作业但同一根因 -> 合并，不发邮件
        alertService.sendAlert(job(1466L, "diag5"), "CRITICAL", "JOB_FAILED",
                "提交失败: SQL Gateway 调用失败: Failed to fetchResults.");
        alertService.sendAlert(job(2154L, "diag6"), "WARN", "JOB_FAILED",
                "提交失败: SQL Gateway 调用失败: Failed to fetchResults.");

        verify(mailDispatcher, times(1)).send(anyString(), anyString());
        assertEquals(3, firstRecord.getOccurrences(), "三次同类告警应合并为一条记录");
        assertTrue(firstRecord.getMessage().contains("同类告警已合并 3 次"), firstRecord.getMessage());
        assertTrue(firstRecord.getAggregatedJobs().contains("demo1(#2800)"), firstRecord.getAggregatedJobs());
        assertTrue(firstRecord.getAggregatedJobs().contains("diag6(#2154)"), firstRecord.getAggregatedJobs());
        assertEquals("CRITICAL", firstRecord.getLevel(), "级别只升不降");
        assertFalse(firstRecord.getReadFlag(), "又发生一次应重新置未读");
    }

    @Test
    void differentRootCauseStillNotifies() {
        alertService.sendAlert(job(1L, "a"), "CRITICAL", "JOB_FAILED", "CSV 输入文件不存在: x.csv");
        alertService.sendAlert(job(2L, "b"), "CRITICAL", "JOB_FAILED", "Checkpoint 超时");
        verify(mailDispatcher, times(2)).send(anyString(), anyString());
    }

    @Test
    void infoAlertsAreNeverAggregated() {
        alertService.sendAlert(job(1L, "a"), "INFO", "ALERT_RESOLVED", "资源已恢复");
        alertService.sendAlert(job(1L, "a"), "INFO", "ALERT_RESOLVED", "资源已恢复");
        verify(alertRepo, times(2)).save(any(AlertRecord.class));
    }

    @Test
    void autoDiagnosisIsCappedPerWindow() {
        for (int i = 0; i < 5; i++) {
            alertService.sendAlert(job(100L + i, "job" + i), "CRITICAL", "JOB_FAILED",
                    "提交失败: SQL Gateway 调用失败: Failed to fetchResults.");
        }
        // 配额 2：只有前两次会发布自动诊断事件
        verify(eventPublisher, times(2)).publishEvent(any(Object.class));
    }

    private AlertRecord captureSaved() {
        var captor = org.mockito.ArgumentCaptor.forClass(AlertRecord.class);
        verify(alertRepo, atLeastOnce()).save(captor.capture());
        return captor.getAllValues().get(0);
    }
}
