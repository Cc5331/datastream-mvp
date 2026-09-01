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
                mock(ExcelPreprocessor.class), mock(FlinkJobStatusChecker.class), mock(JobDependencyRepository.class));
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
