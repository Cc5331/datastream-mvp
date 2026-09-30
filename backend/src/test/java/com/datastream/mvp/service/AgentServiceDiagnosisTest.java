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

    /**
     * 回归：MySQL 输出以空密码连接时（"using password: NO"）必须判为「凭据未注入」，
     * 不能落到 "access denied for user" 兜底分支被误诊成「账号权限 / 库不存在」。
     * 报文取自真实故障（job 2726，2026-09-29 11:27:50）。
     */
    @Test
    void diagnose_emptyMysqlPassword_reportsCredentialNotInjected() {
        DiagnosisReportRepository reportRepo = mock(DiagnosisReportRepository.class);
        JobLogRepository logRepo = mock(JobLogRepository.class);
        JobService jobService = mock(JobService.class);
        LlmClient llmClient = mock(LlmClient.class);

        JobDefinition job = new JobDefinition();
        job.setId(2726L);
        job.setName("demo1");
        job.setOwnerId(1L);
        job.setStatus(JobDefinition.JobStatus.FAILED);
        job.setDagJson("{\"nodes\":[],\"edges\":[]}");
        when(jobService.findByIdForUser(eq(2726L), any())).thenReturn(job);
        when(llmClient.isConfigured()).thenReturn(false);
        when(reportRepo.save(any())).thenAnswer(i -> i.getArgument(0));
        // 真实故障日志（job 2726，2026-09-29 11:27:50）作为本地规则引擎的证据来源
        com.datastream.mvp.model.JobLog errLog = new com.datastream.mvp.model.JobLog();
        errLog.setJobId(2726L);
        errLog.setLevel("ERROR");
        errLog.setMessage("提交失败: MySQL 目标表准备失败: Access denied for user 'root'@'localhost' (using password: NO)");
        when(logRepo.findTop20ByJobIdOrderByTimestampDesc(2726L)).thenReturn(List.of(errLog));

        AgentService agent = new AgentService(
                mock(ControlRegistryService.class), jobService, mock(PreviewService.class),
                mock(MonitorService.class), logRepo, reportRepo, mock(PromptCatalog.class), llmClient, new ObjectMapper());

        CurrentUser admin = new CurrentUser(1L, "admin", "管理员", "ADMIN");
        Map<String, Object> result = agent.diagnose(2726L, null, admin, "manual");

        Object rootCause = result.get("rootCause");
        assertNotNull(rootCause);
        assertTrue(String.valueOf(rootCause).contains("凭据未注入"),
                "空密码必须判为凭据未注入，实际: " + rootCause);
        assertFalse(String.valueOf(rootCause).contains("库不存在"),
                "不得误诊为库不存在/权限问题，实际: " + rootCause);
    }
}
