package com.datastream.mvp.service;

import com.datastream.mvp.model.JobDefinition;
import com.datastream.mvp.model.JobLog;
import com.datastream.mvp.repository.JobDefinitionRepository;
import com.datastream.mvp.repository.JobLogRepository;
import com.datastream.mvp.model.ScheduleHistory;
import com.datastream.mvp.repository.ScheduleHistoryRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.scheduling.support.CronExpression;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.List;

/**
 * 定时调度器：每 30s 扫描启用调度的作业，cron 到期后自动提交到 Flink
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class JobScheduler {

    private final JobDefinitionRepository jobRepo;
    private final JobLogRepository logRepo;
    private final JobService jobService;
    private final ScheduleHistoryRepository historyRepo;
    private final AlertService alertService;
    private final DependencyService dependencyService;
    private final ObjectMapper objectMapper;

    @Scheduled(fixedRate = 30000)
    public void fireDueJobs() {
        // 依赖编排扫描：上游完成自动触发下游
        try {
            dependencyService.scanAndTrigger();
        } catch (Exception e) {
            log.warn("Dependency scan error: {}", e.getMessage());
        }

        // 上线作业监督：异常自动拉起（10 分钟最多 3 次，超限告警并自动下线）
        try {
            superviseOnlineJobs();
        } catch (Exception e) {
            log.warn("Online job supervision error: {}", e.getMessage());
        }

        List<JobDefinition> scheduledJobs = jobRepo.findByScheduleEnabledTrue();
        if (scheduledJobs.isEmpty()) return;
        LocalDateTime now = LocalDateTime.now();
        for (JobDefinition job : scheduledJobs) {
            try {
                String cron = job.getCronExpression();
                if (cron == null || cron.isBlank()) continue;
                CronExpression expression = CronExpression.parse(cron.trim());
                LocalDateTime next = job.getNextFireTime();
                if (next == null) {
                    // 首次启用或尚未计算过：初始化下次触发时间
                    job.setNextFireTime(expression.next(now));
                    jobRepo.save(job);
                    continue;
                }
                if (next.isAfter(now)) continue; // 未到触发时间

                // 到达触发时间：避免与运行中的作业重叠提交
                LocalDateTime nextFire = expression.next(now);
                JobDefinition.JobStatus st = job.getStatus();
                if (st == JobDefinition.JobStatus.SUBMITTED || st == JobDefinition.JobStatus.RUNNING) {
                    job.setNextFireTime(nextFire);
                    jobRepo.save(job);
                    continue;
                }
                try {
                    JobDefinition submitted = jobService.submit(job.getId());
                    submitted.setNextFireTime(nextFire);
                    submitted.setScheduleRetries(0);
                    jobRepo.save(submitted);
                    addScheduleLog(job.getId(), "INFO", "定时调度触发：已提交作业（cron=" + cron.trim() + "）");
                    saveHistory(job, "SUCCESS", "已提交，Flink Job ID: " + submitted.getFlinkJobId(), submitted.getFlinkJobId());
                    log.info("Scheduled job {} fired, submitted (cron={})", job.getId(), cron);
                } catch (Exception e) {
                    int maxRetries = job.getScheduleMaxRetries() == null ? 0 : job.getScheduleMaxRetries();
                    int retries = job.getScheduleRetries() == null ? 0 : job.getScheduleRetries();
                    if (retries < maxRetries) {
                        job.setScheduleRetries(retries + 1);
                        jobRepo.save(job);
                        addScheduleLog(job.getId(), "WARN", "调度提交失败，即将重试（" + (retries + 1) + "/" + maxRetries + "）：" + e.getMessage());
                        saveHistory(job, "RETRY", "提交失败，等待重试（" + (retries + 1) + "/" + maxRetries + "）", null);
                        log.warn("Scheduled job {} fire failed, retry {}/{}: {}", job.getId(), retries + 1, maxRetries, e.getMessage());
                    } else {
                        job.setNextFireTime(nextFire);
                        job.setScheduleRetries(0);
                        jobRepo.save(job);
                        addScheduleLog(job.getId(), "ERROR", "定时调度触发失败（已重试 " + maxRetries + " 次）：" + e.getMessage());
                        saveHistory(job, "FAILED", "提交失败：" + e.getMessage(), null);
                        alertService.sendWebhook(job, "SCHEDULE_FAILED", "定时调度提交失败：" + e.getMessage());
                        log.warn("Scheduled job {} fire failed after {} retries: {}", job.getId(), maxRetries, e.getMessage());
                    }
                }
            } catch (Exception e) {
                log.warn("Scheduled job {} scan error: {}", job.getId(), e.getMessage());
            }
        }
    }

    /** 上线作业自动重启上限与时间窗口 */
    private static final int ONLINE_RESTART_MAX = 3;
    private static final long ONLINE_RESTART_WINDOW_MINUTES = 10;

    /**
     * 上线作业监督：流式作业异常（FAILED/CANCELLED/COMPLETED）或批量作业失败时自动重新提交。
     * 10 分钟窗口内最多重启 ONLINE_RESTART_MAX 次，超限则告警并自动下线停止处理。
     */
    private void superviseOnlineJobs() {
        List<JobDefinition> onlineJobs = jobRepo.findByOnlineTrue();
        if (onlineJobs.isEmpty()) return;
        LocalDateTime now = LocalDateTime.now();
        for (JobDefinition job : onlineJobs) {
            try {
                JobDefinition.JobStatus st = job.getStatus();
                if (st == JobDefinition.JobStatus.SUBMITTED || st == JobDefinition.JobStatus.RUNNING) continue;
                boolean streaming = jobService.isStreamingJob(job);
                boolean shouldRestart;
                if (streaming) {
                    shouldRestart = st == JobDefinition.JobStatus.FAILED
                            || st == JobDefinition.JobStatus.CANCELLED
                            || st == JobDefinition.JobStatus.COMPLETED;
                } else {
                    shouldRestart = st == JobDefinition.JobStatus.FAILED;
                }
                if (!shouldRestart) continue;

                int count = job.getOnlineRestartCount() == null ? 0 : job.getOnlineRestartCount();
                LocalDateTime last = job.getLastRestartAt();
                if (last == null || last.isBefore(now.minusMinutes(ONLINE_RESTART_WINDOW_MINUTES))) count = 0;

                if (count >= ONLINE_RESTART_MAX) {
                    job.setOnline(false);
                    job.setUpdatedAt(now);
                    jobRepo.save(job);
                    addScheduleLog(job.getId(), "ERROR",
                            "上线作业 " + ONLINE_RESTART_WINDOW_MINUTES + " 分钟内自动重启超过 " + ONLINE_RESTART_MAX + " 次，已自动下线停止处理");
                    alertService.sendWebhook(job, "ONLINE_JOB_STOPPED",
                            "上线作业自动重启超限已停止：作业[" + job.getName() + "] 状态=" + st);
                    continue;
                }
                job.setOnlineRestartCount(count + 1);
                job.setLastRestartAt(now);
                job.setUpdatedAt(now);
                jobRepo.save(job);
                addScheduleLog(job.getId(), "WARN",
                        "上线作业异常（" + st + "），自动重启（" + (count + 1) + "/" + ONLINE_RESTART_MAX + "）");
                try {
                    jobService.submit(job.getId());
                } catch (Exception e) {
                    addScheduleLog(job.getId(), "ERROR", "上线作业自动重启提交失败: " + e.getMessage());
                }
            } catch (Exception e) {
                log.warn("Supervise online job {} failed: {}", job.getId(), e.getMessage());
            }
        }
    }

    private void saveHistory(JobDefinition job, String status, String message, String flinkJobId) {
        ScheduleHistory h = new ScheduleHistory();
        h.setJobId(job.getId());
        h.setJobName(job.getName());
        h.setTriggerTime(LocalDateTime.now());
        h.setStatus(status);
        h.setMessage(message);
        h.setFlinkJobId(flinkJobId);
        historyRepo.save(h);
    }

    private void addScheduleLog(Long jobId, String level, String message) {
        JobLog logEntry = new JobLog();
        logEntry.setJobId(jobId);
        logEntry.setLevel(level);
        logEntry.setMessage(message);
        logEntry.setTimestamp(LocalDateTime.now());
        logRepo.save(logEntry);
    }
}
