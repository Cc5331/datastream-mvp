package com.datastream.mvp.service;

import com.datastream.mvp.ai.AgentService;
import com.datastream.mvp.ai.LlmClient;
import com.datastream.mvp.ai.PromptCatalog;
import com.datastream.mvp.model.DiagnosisReport;
import com.datastream.mvp.model.JobDefinition;
import com.datastream.mvp.repository.DiagnosisReportRepository;
import com.datastream.mvp.repository.JobLogRepository;
import com.datastream.mvp.security.CurrentUser;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class AgentServiceDiagnosisTest {

    @Test
    void diagnose_persistsReport_withRuleHitSource() {
        DiagnosisReportRepository reportRepo = mock(DiagnosisReportRepository.class);
        JobLogRepository logRepo = mock(JobLogRepository.class);
        JobService jobService = mock(JobService.class);
        LlmClient llmClient = mock(LlmClient.class);
        ObjectMapper mapper = new ObjectMapper();

        JobDefinition job = new JobDefinition();
        job.setId(5L);
        job.setName("bad-job");
        job.setOwnerId(9L);
        job.setStatus(JobDefinition.JobStatus.FAILED);
        job.setDagJson("{\"nodes\":[],\"edges\":[]}");
        when(jobService.findByIdForUser(eq(5L), any())).thenReturn(job);
        when(llmClient.isConfigured()).thenReturn(false); // 走 local-fallback，不调用 LLM

        AgentService agent = new AgentService(
                mock(ControlRegistryService.class), jobService, mock(PreviewService.class),
                mock(MonitorService.class), logRepo, reportRepo, mock(PromptCatalog.class), llmClient, mapper);
        when(reportRepo.save(any())).thenAnswer(i -> i.getArgument(0));

        CurrentUser operator = new CurrentUser(9L, "operator", "操作员", "OPERATOR");
        Map<String, Object> result = agent.diagnose(5L, null, operator, "manual");

        assertNotNull(result.get("rootCause"));
        assertEquals("local-fallback", result.get("source"));
        verify(reportRepo).save(argThat(r -> r.getJobId().equals(5L) && r.getTrigger().equals("manual")
                && r.getRootCause() != null && r.getSource().equals("local-fallback")));
    }

    @Test
    void autoDiagnose_persistsReport_withEventTrigger() {
        DiagnosisReportRepository reportRepo = mock(DiagnosisReportRepository.class);
        JobLogRepository logRepo = mock(JobLogRepository.class);
        JobService jobService = mock(JobService.class);
        LlmClient llmClient = mock(LlmClient.class);
        ObjectMapper mapper = new ObjectMapper();

        JobDefinition job = new JobDefinition();
        job.setId(6L);
        job.setName("failed-job");
        job.setOwnerId(9L);
        job.setStatus(JobDefinition.JobStatus.FAILED);
        job.setDagJson("{\"nodes\":[],\"edges\":[]}");
        when(jobService.findById(6L)).thenReturn(job);
        when(llmClient.isConfigured()).thenReturn(false);
        when(reportRepo.save(any())).thenAnswer(i -> i.getArgument(0));

        AgentService agent = new AgentService(
                mock(ControlRegistryService.class), jobService, mock(PreviewService.class),
                mock(MonitorService.class), logRepo, reportRepo, mock(PromptCatalog.class), llmClient, mapper);

        agent.autoDiagnose(6L, "JOB_FAILED");

        verify(reportRepo).save(argThat(r -> r.getJobId().equals(6L) && r.getTrigger().equals("JOB_FAILED")
                && r.getSource().equals("local-fallback")));
    }

    @Test
    void listDiagnosis_returnsHistory() {
        DiagnosisReportRepository reportRepo = mock(DiagnosisReportRepository.class);
        List<DiagnosisReport> reports = List.of();
        when(reportRepo.findByJobIdOrderByCreatedAtDesc(5L)).thenReturn(reports);

        AgentService agent = new AgentService(
                mock(ControlRegistryService.class), mock(JobService.class), mock(PreviewService.class),
                mock(MonitorService.class), mock(JobLogRepository.class), reportRepo, mock(PromptCatalog.class), mock(LlmClient.class), new ObjectMapper());

        assertEquals(reports, agent.listDiagnosis(5L));
        verify(reportRepo).findByJobIdOrderByCreatedAtDesc(5L);
    }
}
