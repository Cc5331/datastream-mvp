package com.datastream.mvp.service;

import com.datastream.mvp.model.JobDefinition;
import com.datastream.mvp.repository.JobDefinitionRepository;
import com.datastream.mvp.repository.JobDependencyRepository;
import com.datastream.mvp.repository.JobLogRepository;
import com.datastream.mvp.repository.JobVersionRepository;
import com.datastream.mvp.security.CurrentUser;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.web.server.ResponseStatusException;

import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class JobServiceTest {
    private JobDefinitionRepository jobRepo;
    private DagTranslationService translationService;
    private JobService service;

    @BeforeEach
    void setUp() {
        jobRepo = mock(JobDefinitionRepository.class);
        translationService = mock(DagTranslationService.class);
        service = new JobService(jobRepo, mock(JobLogRepository.class), mock(JobVersionRepository.class),
                mock(ControlRegistryService.class), translationService, new ObjectMapper(),
                mock(ExcelPreprocessor.class), mock(FlinkJobStatusChecker.class),
                mock(HealthMonitor.class), mock(MonitorService.class), mock(JobDependencyRepository.class),
                mock(PreviewService.class), mock(DataSourceNodeResolver.class));
    }

    @Test
    void findAllForUser_filtersByOwnerUnlessAdmin() {
        CurrentUser operator = new CurrentUser(7L, "operator", "操作员", "OPERATOR");
        CurrentUser admin = new CurrentUser(1L, "admin", "管理员", "ADMIN");
        when(jobRepo.findByOwnerIdOrderByUpdatedAtDesc(7L)).thenReturn(List.of(job(10L, 7L)));
        when(jobRepo.findAllByOrderByUpdatedAtDesc()).thenReturn(List.of(job(10L, 7L), job(11L, 8L)));

        assertEquals(1, service.findAllForUser(operator).size());
        assertEquals(2, service.findAllForUser(admin).size());
        verify(jobRepo).findByOwnerIdOrderByUpdatedAtDesc(7L);
    }

    @Test
    void findByIdForUser_rejectsDifferentOwner() {
        JobDefinition job = job(10L, 8L);
        when(jobRepo.findById(10L)).thenReturn(Optional.of(job));

        ResponseStatusException ex = assertThrows(ResponseStatusException.class,
                () -> service.findByIdForUser(10L, new CurrentUser(7L, "operator", "操作员", "OPERATOR")));
        assertEquals(403, ex.getStatusCode().value());
    }

    @Test
    void quoteMysqlTable_acceptsIdentifiersAndRejectsInjection() {
        assertEquals("`orders`", com.datastream.mvp.util.MysqlIdentifier.quoteTable("orders"));
        assertEquals("`sales`.`order`", com.datastream.mvp.util.MysqlIdentifier.quoteTable("sales.order"));
        assertThrows(IllegalArgumentException.class,
                () -> com.datastream.mvp.util.MysqlIdentifier.quoteTable("orders; DROP TABLE users"));
        assertThrows(IllegalArgumentException.class,
                () -> com.datastream.mvp.util.MysqlIdentifier.quoteTable("db.schema.table"));
    }

    @Test
    void preview_rejectsUnsafeMysqlTableBeforeConnecting() {
        JobDefinition job = job(10L, 7L);
        job.setDagJson("{\"nodes\":[{\"type\":\"mysql_output\",\"params\":{" +
                "\"url\":\"jdbc:mysql://192.0.2.1:3306/dataflow\"," +
                "\"table\":\"victim`; DROP TABLE users; --\"}}]}");
        when(jobRepo.findById(10L)).thenReturn(Optional.of(job));

        assertThrows(IllegalArgumentException.class, () -> service.preview(10L));
    }

    @Test
    void submitRejectsUnconfirmedAiDraft() {
        JobDefinition job = job(10L, 7L);
        job.setSource(JobDefinition.JobSource.AI);
        job.setConfirmationStatus(JobDefinition.ConfirmationStatus.PENDING);
        when(jobRepo.findById(10L)).thenReturn(Optional.of(job));

        ResponseStatusException ex = assertThrows(ResponseStatusException.class, () -> service.submit(10L));
        assertEquals(409, ex.getStatusCode().value());
        verifyNoInteractions(translationService);
    }

    @Test
    void create_forcesAiConfirmationToPending_ignoringClientValue() {
        when(jobRepo.save(any())).thenAnswer(i -> i.getArgument(0));
        JobDefinition ai = new JobDefinition();
        ai.setName("ai-job");
        ai.setDagJson("{}");
        ai.setSource(JobDefinition.JobSource.AI);
        ai.setConfirmationStatus(JobDefinition.ConfirmationStatus.CONFIRMED); // 客户端伪造
        ai.setConfirmedBy(999L);

        JobDefinition saved = service.create(ai);

        assertEquals(JobDefinition.ConfirmationStatus.PENDING, saved.getConfirmationStatus());
        assertNull(saved.getConfirmedBy());
        assertNull(saved.getConfirmedAt());
    }

    @Test
    void create_forcesManualToNotRequired_ignoringClientValue() {
        when(jobRepo.save(any())).thenAnswer(i -> i.getArgument(0));
        JobDefinition manual = new JobDefinition();
        manual.setName("manual-job");
        manual.setDagJson("{}");
        manual.setSource(JobDefinition.JobSource.MANUAL);
        manual.setConfirmationStatus(JobDefinition.ConfirmationStatus.CONFIRMED);

        JobDefinition saved = service.create(manual);

        assertEquals(JobDefinition.ConfirmationStatus.NOT_REQUIRED, saved.getConfirmationStatus());
    }

    @Test
    void confirmAiDraftRecordsActor() {
        JobDefinition job = job(10L, 7L);
        job.setStatus(JobDefinition.JobStatus.DRAFT);
        job.setSource(JobDefinition.JobSource.AI);
        job.setConfirmationStatus(JobDefinition.ConfirmationStatus.PENDING);
        when(jobRepo.findById(10L)).thenReturn(Optional.of(job));
        when(jobRepo.save(job)).thenReturn(job);
        CurrentUser operator = new CurrentUser(7L, "operator", "操作员", "OPERATOR");

        JobDefinition confirmed = service.confirmAiDraft(10L, operator);

        assertEquals(JobDefinition.ConfirmationStatus.CONFIRMED, confirmed.getConfirmationStatus());
        assertEquals(7L, confirmed.getConfirmedBy());
        assertNotNull(confirmed.getConfirmedAt());
    }

    @Test
    void submit_updatesStatusAndFlinkId() throws Exception {
        JobDefinition job = job(10L, 7L);
        job.setDagJson("{\"jobName\":\"test\",\"parallelism\":1,\"nodes\":[],\"edges\":[]}");
        when(jobRepo.findById(10L)).thenReturn(Optional.of(job));
        when(translationService.translate(any())).thenReturn("SELECT 1;");
        when(translationService.submitToFlink("SELECT 1;", 1)).thenReturn("flink-123");
        when(jobRepo.save(any())).thenAnswer(i -> i.getArgument(0));

        JobDefinition submitted = service.submit(10L);

        assertEquals(JobDefinition.JobStatus.SUBMITTED, submitted.getStatus());
        assertEquals("flink-123", submitted.getFlinkJobId());
        assertNotNull(submitted.getSubmittedAt());
    }

    /**
     * 并发/重复提交防护：已在集群中运行（SUBMITTED/RUNNING）的作业再次 submit 必须被拒，
     * 否则手动提交、cron 调度、依赖触发三路并发会拉起重复的 Flink 作业。
     */
    @Test
    void submitRejectsJobAlreadyRunning() throws Exception {
        for (JobDefinition.JobStatus running : List.of(
                JobDefinition.JobStatus.SUBMITTED, JobDefinition.JobStatus.RUNNING)) {
            JobDefinition job = job(20L, 7L);
            job.setDagJson("{\"jobName\":\"test\",\"parallelism\":1,\"nodes\":[],\"edges\":[]}");
            job.setStatus(running);
            when(jobRepo.findById(20L)).thenReturn(Optional.of(job));

            ResponseStatusException ex = assertThrows(ResponseStatusException.class,
                    () -> service.submit(20L), "状态 " + running + " 时不应允许重复提交");
            assertEquals(409, ex.getStatusCode().value());
        }
        // 关键：一次都不能真的提交到 Flink
        verify(translationService, never()).submitToFlink(any(), anyInt());
    }

    @Test
    void submit_allowsRerunAfterTerminalStatus() throws Exception {
        JobDefinition job = job(21L, 7L);
        job.setDagJson("{\"jobName\":\"test\",\"parallelism\":1,\"nodes\":[],\"edges\":[]}");
        job.setStatus(JobDefinition.JobStatus.COMPLETED);
        when(jobRepo.findById(21L)).thenReturn(Optional.of(job));
        when(translationService.translate(any())).thenReturn("SELECT 1;");
        when(translationService.submitToFlink("SELECT 1;", 1)).thenReturn("flink-999");
        when(jobRepo.save(any())).thenAnswer(i -> i.getArgument(0));

        JobDefinition submitted = service.submit(21L);

        assertEquals(JobDefinition.JobStatus.SUBMITTED, submitted.getStatus());
        assertEquals("flink-999", submitted.getFlinkJobId());
    }

    private JobDefinition job(Long id, Long ownerId) {
        JobDefinition job = new JobDefinition();
        job.setId(id);
        job.setOwnerId(ownerId);
        job.setName("job-" + id);
        job.setDagJson("{}");
        job.setParallelism(1);
        return job;
    }
}
