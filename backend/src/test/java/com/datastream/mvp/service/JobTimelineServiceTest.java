package com.datastream.mvp.service;

import com.datastream.mvp.model.AlertRecord;
import com.datastream.mvp.model.JobDefinition;
import com.datastream.mvp.model.JobLog;
import com.datastream.mvp.repository.AlertRecordRepository;
import com.datastream.mvp.repository.JobLogRepository;
import com.datastream.mvp.repository.ScheduleHistoryRepository;
import com.datastream.mvp.security.CurrentUser;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class JobTimelineServiceTest {
    @Test
    void latestFiltersBeforeSubmissionAndSortsDescendingWithoutRunId() {
        JobService jobs = mock(JobService.class); JobLogRepository logs = mock(JobLogRepository.class);
        AlertRecordRepository alerts = mock(AlertRecordRepository.class); ScheduleHistoryRepository histories = mock(ScheduleHistoryRepository.class);
        CurrentUser user = new CurrentUser(5L, "u", "U", "VIEWER");
        LocalDateTime submitted = LocalDateTime.of(2026, 1, 2, 10, 0);
        JobDefinition job = new JobDefinition(); job.setStatus(JobDefinition.JobStatus.RUNNING); job.setCreatedAt(submitted.minusDays(1)); job.setUpdatedAt(submitted.plusMinutes(1)); job.setSubmittedAt(submitted);
        JobLog old = new JobLog(); old.setId(1L); old.setTimestamp(submitted.minusSeconds(1)); old.setLevel("INFO"); old.setMessage("old");
        AlertRecord recent = new AlertRecord(); recent.setId(2L); recent.setCreatedAt(submitted.plusMinutes(2)); recent.setEvent("WARN"); recent.setMessage("recent");
        when(jobs.findByIdForUser(9L, user)).thenReturn(job); when(logs.findByJobIdOrderByTimestampDesc(9L)).thenReturn(List.of(old));
        when(alerts.findByJobIdOrderByCreatedAtDesc(9L)).thenReturn(List.of(recent)); when(histories.findByJobIdOrderByTriggerTimeDesc(9L)).thenReturn(List.of());

        List<Map<String, Object>> result = new JobTimelineService(jobs, logs, alerts, histories).timeline(9L, user, JobTimelineService.Scope.LATEST);

        assertEquals("ALERT", result.get(0).get("source"));
        assertTrue(result.stream().noneMatch(e -> "old".equals(e.get("message"))));
        assertTrue(result.stream().noneMatch(e -> e.containsKey("runId")));
        verify(jobs).findByIdForUser(9L, user);
    }
}
