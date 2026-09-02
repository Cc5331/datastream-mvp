package com.datastream.mvp.service;

import com.datastream.mvp.model.AlertRecord;
import com.datastream.mvp.model.JobDefinition;
import com.datastream.mvp.model.JobLog;
import com.datastream.mvp.repository.AlertRecordRepository;
import com.datastream.mvp.repository.JobDefinitionRepository;
import com.datastream.mvp.repository.JobLogRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class HealthMonitorTest {

    private JobDefinition failedJob() {
        JobDefinition j = new JobDefinition();
        j.setId(77L);
        j.setStatus(JobDefinition.JobStatus.FAILED);
        j.setCompletedAt(LocalDateTime.now().minusSeconds(1));
        return j;
    }

    @Test
    void notifyJobFailed_createsAlertForFailedJob() {
        AlertRecordRepository alertRepo = mock(AlertRecordRepository.class);
        JobLogRepository logRepo = mock(JobLogRepository.class);
        AlertService alertService = mock(AlertService.class);
        HealthMonitor monitor = new HealthMonitor(mock(JobDefinitionRepository.class), logRepo, alertRepo, alertService, new ObjectMapper());
        when(alertRepo.findFirstByJobIdAndEventOrderByCreatedAtDesc(eq(77L), eq("JOB_FAILED"))).thenReturn(Optional.empty());
        JobLog err = new JobLog();
        err.setMessage("提交失败: 文件不存在");
        when(logRepo.findByJobIdAndLevelOrderByTimestampDesc(eq(77L), eq("ERROR"))).thenReturn(List.of(err));

        monitor.notifyJobFailed(failedJob());

        verify(alertService).sendAlert(any(JobDefinition.class), eq("CRITICAL"), eq("JOB_FAILED"), contains("文件不存在"));
    }

    @Test
    void notifyJobFailed_skipsDuplicateAlertWithinSameOccurrence() {
        AlertRecordRepository alertRepo = mock(AlertRecordRepository.class);
        JobLogRepository logRepo = mock(JobLogRepository.class);
        AlertService alertService = mock(AlertService.class);
        HealthMonitor monitor = new HealthMonitor(mock(JobDefinitionRepository.class), logRepo, alertRepo, alertService, new ObjectMapper());
        // 最近一条告警时间晚于故障发生时间 => 同一故障已告警，应跳过
        AlertRecord last = new AlertRecord();
        last.setCreatedAt(LocalDateTime.now());
        when(alertRepo.findFirstByJobIdAndEventOrderByCreatedAtDesc(eq(77L), eq("JOB_FAILED")))
                .thenReturn(Optional.of(last));

        monitor.notifyJobFailed(failedJob());

        verify(alertService, never()).sendAlert(any(), any(), any(), any());
    }

    @Test
    void notifyJobFailed_ignoresNonFailedJob() {
        AlertRecordRepository alertRepo = mock(AlertRecordRepository.class);
        AlertService alertService = mock(AlertService.class);
        HealthMonitor monitor = new HealthMonitor(mock(JobDefinitionRepository.class), mock(JobLogRepository.class), alertRepo, alertService, new ObjectMapper());
        JobDefinition running = new JobDefinition();
        running.setId(88L);
        running.setStatus(JobDefinition.JobStatus.RUNNING);

        monitor.notifyJobFailed(running);

        verify(alertService, never()).sendAlert(any(), any(), any(), any());
    }
}
