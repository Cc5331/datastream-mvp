package com.datastream.mvp.service;

import com.datastream.mvp.model.AlertRecord;
import com.datastream.mvp.model.JobDefinition;
import com.datastream.mvp.model.JobLog;
import com.datastream.mvp.model.ScheduleHistory;
import com.datastream.mvp.repository.AlertRecordRepository;
import com.datastream.mvp.repository.JobLogRepository;
import com.datastream.mvp.repository.ScheduleHistoryRepository;
import com.datastream.mvp.security.CurrentUser;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Service
@RequiredArgsConstructor
public class JobTimelineService {
    private final JobService jobService;
    private final JobLogRepository logRepo;
    private final AlertRecordRepository alertRepo;
    private final ScheduleHistoryRepository historyRepo;

    public List<Map<String, Object>> timeline(Long id, CurrentUser user, Scope scope) {
        JobDefinition job = jobService.findByIdForUser(id, user);
        LocalDateTime cutoff = scope == Scope.LATEST ? job.getSubmittedAt() : null;
        List<Map<String, Object>> events = new ArrayList<>();
        add(events, "LIFECYCLE", "CREATED", job.getCreatedAt(), "作业已创建", null);
        add(events, "LIFECYCLE", "UPDATED", job.getUpdatedAt(), "作业最近更新", null);
        add(events, "LIFECYCLE", "SUBMITTED", job.getSubmittedAt(), "作业已提交", null);
        add(events, "LIFECYCLE", job.getStatus().name(), job.getCompletedAt(), "作业结束状态：" + job.getStatus(), null);
        for (JobLog log : logRepo.findByJobIdOrderByTimestampDesc(id))
            add(events, "LOG", log.getLevel(), log.getTimestamp(), log.getMessage(), log.getId());
        for (AlertRecord alert : alertRepo.findByJobIdOrderByCreatedAtDesc(id))
            add(events, "ALERT", alert.getEvent(), alert.getCreatedAt(), alert.getMessage(), alert.getId());
        for (ScheduleHistory history : historyRepo.findByJobIdOrderByTriggerTimeDesc(id))
            add(events, "SCHEDULE", history.getStatus(), history.getTriggerTime(), history.getMessage(), history.getId());
        return events.stream().filter(e -> cutoff == null || !((LocalDateTime) e.get("time")).isBefore(cutoff))
                .sorted(Comparator.comparing(e -> (LocalDateTime) e.get("time"), Comparator.reverseOrder()))
                .toList();
    }

    private void add(List<Map<String, Object>> events, String source, String type, LocalDateTime time,
                     String message, Long sourceId) {
        if (time == null) return;
        Map<String, Object> event = new LinkedHashMap<>();
        event.put("source", source);
        event.put("type", type);
        event.put("time", time);
        event.put("message", message);
        if (sourceId != null) event.put("sourceId", sourceId);
        events.add(event);
    }

    public enum Scope { ALL, LATEST }
}
