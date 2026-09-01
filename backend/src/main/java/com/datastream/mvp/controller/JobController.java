package com.datastream.mvp.controller;

import com.datastream.mvp.audit.Audit;
import com.datastream.mvp.model.JobDefinition;
import com.datastream.mvp.model.JobLog;
import com.datastream.mvp.model.JobVersion;
import com.datastream.mvp.model.ScheduleHistory;
import com.datastream.mvp.repository.ScheduleHistoryRepository;
import com.datastream.mvp.security.CurrentUser;
import com.datastream.mvp.security.SecurityUtils;
import com.datastream.mvp.service.JobService;
import com.datastream.mvp.service.DependencyService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 作业管理 REST API（RBAC：读=登录用户，写=ADMIN/OPERATOR，资源按 owner 隔离）
 */
@RestController
@RequestMapping("/api/jobs")
@RequiredArgsConstructor
public class JobController {

    private final JobService jobService;
    private final ScheduleHistoryRepository historyRepo;
    private final DependencyService dependencyService;

    @GetMapping
    public List<JobDefinition> list() {
        return jobService.findAllForUser(currentUser());
    }

    @GetMapping("/{id}")
    public JobDefinition get(@PathVariable Long id) {
        return jobService.findByIdForUser(id, currentUser());
    }

    @PostMapping
    @PreAuthorize("hasAnyRole('ADMIN','OPERATOR')")
    @Audit(action = "JOB_CREATE", targetType = "JOB", targetId = "#result.id")
    public JobDefinition create(@RequestBody JobDefinition job) {
        return jobService.create(job, currentUser());
    }

    @PutMapping("/{id}")
    @PreAuthorize("hasAnyRole('ADMIN','OPERATOR')")
    @Audit(action = "JOB_UPDATE", targetType = "JOB", targetId = "#id")
    public JobDefinition update(@PathVariable Long id, @RequestBody JobDefinition job) {
        jobService.findByIdForUser(id, currentUser());
        return jobService.update(id, job);
    }

    @DeleteMapping("/{id}")
    @PreAuthorize("hasAnyRole('ADMIN','OPERATOR')")
    @Audit(action = "JOB_DELETE", targetType = "JOB", targetId = "#id")
    public ResponseEntity<Void> delete(@PathVariable Long id) {
        jobService.findByIdForUser(id, currentUser());
        jobService.delete(id);
        return ResponseEntity.ok().build();
    }

    @PostMapping("/{id}/submit")
    @PreAuthorize("hasAnyRole('ADMIN','OPERATOR')")
    @Audit(action = "JOB_SUBMIT", targetType = "JOB", targetId = "#id")
    public JobDefinition submit(@PathVariable Long id) {
        jobService.findByIdForUser(id, currentUser());
        return jobService.submit(id);
    }

    /** 作业上线：流式立即后台持续运行；批量按 cron 周期重跑（需先配置 cron） */
    @PostMapping("/{id}/online")
    @PreAuthorize("hasAnyRole('ADMIN','OPERATOR')")
    @Audit(action = "JOB_ONLINE", targetType = "JOB", targetId = "#id")
    public JobDefinition online(@PathVariable Long id) {
        jobService.findByIdForUser(id, currentUser());
        return jobService.online(id);
    }

    /** 作业下线：停止持续处理（运行中作业取消并合并输出，停用 cron 重跑） */
    @PostMapping("/{id}/offline")
    @PreAuthorize("hasAnyRole('ADMIN','OPERATOR')")
    @Audit(action = "JOB_OFFLINE", targetType = "JOB", targetId = "#id")
    public JobDefinition offline(@PathVariable Long id) {
        jobService.findByIdForUser(id, currentUser());
        return jobService.offline(id);
    }

    @PostMapping("/batch-online")
    @PreAuthorize("hasAnyRole('ADMIN','OPERATOR')")
    @Audit(action = "JOB_BATCH_ONLINE", targetType = "JOB", detail = "'批量上线 ' + #request.ids().size() + ' 个作业'")
    public Map<String, Object> batchOnline(@RequestBody BatchJobRequest request) {
        return runBatch(request.ids(), true, request.scheduleEnabled(), request.cronExpression());
    }

    @PostMapping("/batch-offline")
    @PreAuthorize("hasAnyRole('ADMIN','OPERATOR')")
    @Audit(action = "JOB_BATCH_OFFLINE", targetType = "JOB", detail = "'批量下线 ' + #request.ids().size() + ' 个作业'")
    public Map<String, Object> batchOffline(@RequestBody BatchJobRequest request) {
        return runBatch(request.ids(), false, null, null);
    }

    private Map<String, Object> runBatch(List<Long> ids, boolean online,
                                         Boolean scheduleEnabled, String cronExpression) {
        if (ids == null || ids.isEmpty()) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "请选择至少一个作业");
        }
        List<Long> succeeded = new ArrayList<>();
        List<Map<String, Object>> failed = new ArrayList<>();
        for (Long id : ids.stream().distinct().toList()) {
            try {
                jobService.findByIdForUser(id, currentUser());
                if (online) {
                    jobService.updateSchedule(id, scheduleEnabled, cronExpression, null, null);
                    jobService.online(id);
                } else {
                    jobService.offline(id);
                }
                succeeded.add(id);
            } catch (Exception e) {
                Map<String, Object> item = new LinkedHashMap<>();
                item.put("id", id);
                item.put("message", e.getMessage() == null ? "操作失败" : e.getMessage());
                failed.add(item);
            }
        }
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("succeeded", succeeded);
        result.put("failed", failed);
        return result;
    }

    public record BatchJobRequest(List<Long> ids, Boolean scheduleEnabled, String cronExpression) {}

    @PostMapping("/{id}/copy")
    @PreAuthorize("hasAnyRole('ADMIN','OPERATOR')")
    @Audit(action = "JOB_COPY", targetType = "JOB", targetId = "#id")
    public JobDefinition copy(@PathVariable Long id) {
        jobService.findByIdForUser(id, currentUser());
        return jobService.copy(id);
    }

    @PostMapping("/{id}/schedule")
    @PreAuthorize("hasAnyRole('ADMIN','OPERATOR')")
    @Audit(action = "JOB_SCHEDULE_UPDATE", targetType = "JOB", targetId = "#id")
    public JobDefinition schedule(@PathVariable Long id, @RequestBody ScheduleRequest request) {
        jobService.findByIdForUser(id, currentUser());
        return jobService.updateSchedule(id, request.scheduleEnabled(), request.cronExpression(),
                request.scheduleMaxRetries(), request.webhookUrl());
    }

    /**
     * 定时调度配置请求体
     */
    public record ScheduleRequest(Boolean scheduleEnabled, String cronExpression, Integer scheduleMaxRetries, String webhookUrl) {}

    @PostMapping("/{id}/cancel")
    @PreAuthorize("hasAnyRole('ADMIN','OPERATOR')")
    @Audit(action = "JOB_CANCEL", targetType = "JOB", targetId = "#id")
    public ResponseEntity<Void> cancel(@PathVariable Long id) {
        jobService.findByIdForUser(id, currentUser());
        jobService.cancel(id);
        return ResponseEntity.ok().build();
    }

    @GetMapping("/{id}/versions")
    public List<JobVersion> versions(@PathVariable Long id) {
        jobService.findByIdForUser(id, currentUser());
        return jobService.getVersions(id);
    }

    @PostMapping("/{id}/rollback/{versionId}")
    @PreAuthorize("hasAnyRole('ADMIN','OPERATOR')")
    @Audit(action = "JOB_ROLLBACK", targetType = "JOB", targetId = "#id", detail = "'回滚到版本 ' + #versionId")
    public JobDefinition rollback(@PathVariable Long id, @PathVariable Long versionId) {
        jobService.findByIdForUser(id, currentUser());
        return jobService.rollback(id, versionId);
    }

    @GetMapping("/{id}/schedule-history")
    public List<ScheduleHistory> scheduleHistory(@PathVariable Long id) {
        jobService.findByIdForUser(id, currentUser());
        return historyRepo.findByJobIdOrderByTriggerTimeDesc(id);
    }

    @GetMapping("/{id}/logs")
    public List<JobLog> logs(@PathVariable Long id) {
        jobService.findByIdForUser(id, currentUser());
        return jobService.getLogs(id);
    }

    @GetMapping("/{id}/preview")
    public List<String> preview(@PathVariable Long id) {
        jobService.findByIdForUser(id, currentUser());
        return jobService.preview(id);
    }

    /** 全工作流图（作业 + 依赖边） */
    @GetMapping("/workflow")
    public Map<String, Object> workflow() {
        return dependencyService.getWorkflow(currentUser());
    }

    /** 查询上游依赖 */
    @GetMapping("/{id}/dependencies")
    public List<Map<String, Object>> dependencies(@PathVariable Long id) {
        jobService.findByIdForUser(id, currentUser());
        return dependencyService.getDependencies(id);
    }

    /** 保存上游依赖（全量覆盖） */
    @PutMapping("/{id}/dependencies")
    @PreAuthorize("hasAnyRole('ADMIN','OPERATOR')")
    @Audit(action = "JOB_DEPENDENCY_UPDATE", targetType = "JOB", targetId = "#id")
    public List<Map<String, Object>> saveDependencies(@PathVariable Long id, @RequestBody DependencyRequest request) {
        jobService.findByIdForUser(id, currentUser());
        return dependencyService.saveDependencies(id, request.upstreamIds(), currentUser());
    }

    /** 作业依赖配置请求体 */
    public record DependencyRequest(List<Long> upstreamIds) {}

    private CurrentUser currentUser() {
        CurrentUser cu = SecurityUtils.currentUser();
        if (cu == null) throw new ResponseStatusException(HttpStatus.UNAUTHORIZED, "未登录");
        return cu;
    }
}
