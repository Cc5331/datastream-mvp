package com.datastream.mvp.service;

import com.datastream.mvp.model.JobDefinition;
import com.datastream.mvp.model.JobLog;
import com.datastream.mvp.repository.JobDefinitionRepository;
import com.datastream.mvp.repository.JobLogRepository;
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

    @Scheduled(fixedRate = 30000)
    public void fireDueJobs() {
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
                    jobRepo.save(submitted);
                    addScheduleLog(job.getId(), "INFO", "定时调度触发：已提交作业（cron=" + cron.trim() + "）");
                    log.info("Scheduled job {} fired, submitted (cron={})", job.getId(), cron);
                } catch (Exception e) {
                    // 提交失败也推进下次触发，避免每 30s 重试轰炸
                    job.setNextFireTime(nextFire);
                    jobRepo.save(job);
                    addScheduleLog(job.getId(), "ERROR", "定时调度触发失败：" + e.getMessage());
                    log.warn("Scheduled job {} fire failed: {}", job.getId(), e.getMessage());
                }
            } catch (Exception e) {
                log.warn("Scheduled job {} scan error: {}", job.getId(), e.getMessage());
            }
        }
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
