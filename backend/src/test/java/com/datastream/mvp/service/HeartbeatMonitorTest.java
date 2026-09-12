package com.datastream.mvp.service;

import com.datastream.mvp.model.JobDefinition;
import com.datastream.mvp.repository.AlertRecordRepository;
import com.datastream.mvp.repository.JobDefinitionRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.Set;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class HeartbeatMonitorTest {

    private JobDefinitionRepository jobRepo;
    private AlertRecordRepository alertRepo;
    private AlertService alertService;
    private HeartbeatMonitor monitor;

    @BeforeEach
    void setUp() {
        jobRepo = mock(JobDefinitionRepository.class);
        alertRepo = mock(AlertRecordRepository.class);
        alertService = mock(AlertService.class);
        monitor = new HeartbeatMonitor(jobRepo, alertRepo, alertService, new ObjectMapper());
        ReflectionTestUtils.setField(monitor, "jobMisses", 3);
        ReflectionTestUtils.setField(monitor, "clusterMisses", 3);
    }

    private JobDefinition runningJob(Long id, String jid) {
        JobDefinition job = new JobDefinition();
        job.setId(id);
        job.setName("job-" + id);
        job.setStatus(JobDefinition.JobStatus.RUNNING);
        job.setFlinkJobId(jid);
        return job;
    }

    @Test
    void jobHeartbeat_alertsAfterMissingThreshold_thenResolvesOnRecovery() {
        JobDefinition job = runningJob(1L, "real-jid-1");
        when(alertRepo.findFirstByJobIdAndEventOrderByCreatedAtDesc(eq(1L), eq("JOB_HEARTBEAT_LOST")))
                .thenReturn(java.util.Optional.empty());

        // 前两次消失：未达阈值
        monitor.handleJobProbe(job, Set.of());
        monitor.handleJobProbe(job, Set.of());
        verify(alertService, never()).sendAlert(any(JobDefinition.class), eq("CRITICAL"), eq("JOB_HEARTBEAT_LOST"), anyString());

        // 第三次消失：触发告警
        monitor.handleJobProbe(job, Set.of());
        verify(alertService).sendAlert(eq(job), eq("CRITICAL"), eq("JOB_HEARTBEAT_LOST"), contains("已从 Flink 集群注册表消失"));

        // 恢复：发送 RESOLVED
        monitor.handleJobProbe(job, Set.of("real-jid-1"));
        verify(alertService).sendAlert(eq(job), eq("INFO"), eq("ALERT_RESOLVED"), contains("作业心跳已恢复"));
    }

    @Test
    void clusterHeartbeat_alertsAfterThreshold_thenResolves() {
        when(alertRepo.findFirstByEventOrderByCreatedAtDesc("CLUSTER_HEARTBEAT_LOST"))
                .thenReturn(java.util.Optional.empty());

        monitor.handleClusterMissing();
        monitor.handleClusterMissing();
        verify(alertService, never()).sendAlert(isNull(), eq("CRITICAL"), eq("CLUSTER_HEARTBEAT_LOST"), anyString());

        monitor.handleClusterMissing();
        verify(alertService).sendAlert(isNull(), eq("CRITICAL"), eq("CLUSTER_HEARTBEAT_LOST"), contains("集群心跳丢失"));

        monitor.handleClusterAlive();
        verify(alertService).sendAlert(isNull(), eq("INFO"), eq("ALERT_RESOLVED"), contains("集群心跳已恢复"));
    }

    @Test
    void mockFlinkJobId_isIgnored() {
        JobDefinition job = runningJob(2L, "flink-job-abc");
        monitor.handleJobProbe(job, Set.of());
        monitor.handleJobProbe(job, Set.of());
        monitor.handleJobProbe(job, Set.of());
        verify(alertService, never()).sendAlert(any(JobDefinition.class), eq("CRITICAL"), eq("JOB_HEARTBEAT_LOST"), anyString());
    }
}
