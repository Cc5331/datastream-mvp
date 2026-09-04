package com.datastream.mvp.service;

import com.datastream.mvp.repository.AlertRecordRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.List;

/**
 * 告警保留策略：防止 alert_record 无限膨胀。
 * - 按时间：删除 createdAt 早于 retention-days（默认 30 天）的记录；
 * - 按上限：超过 retention-max-records（默认 5000）时删除最旧记录，保留最新 N 条。
 * 两项均可用 app.alert.* 配置（环境变量覆盖），0 表示关闭该项。
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class AlertRetentionService {

    private final AlertRecordRepository alertRepo;

    @Value("${app.alert.retention-days:30}")
    private int retentionDays;

    @Value("${app.alert.retention-max-records:5000}")
    private int retentionMaxRecords;

    /** 每 6 小时执行一次；应用启动后首次延迟 1 分钟执行 */
    @Scheduled(initialDelay = 60_000, fixedRate = 6 * 60 * 60 * 1000L)
    @Transactional
    public void cleanup() {
        try {
            int byAge = 0;
            int byMax = 0;
            if (retentionDays > 0) {
                LocalDateTime cutoff = LocalDateTime.now().minusDays(retentionDays);
                byAge = alertRepo.deleteByCreatedAtBefore(cutoff);
                if (byAge > 0) log.info("Alert retention: deleted {} records older than {} days", byAge, retentionDays);
            }
            if (retentionMaxRecords > 0) {
                long total = alertRepo.count();
                long excess = total - retentionMaxRecords;
                if (excess > 0) {
                    // 取最旧的 excess 条（按 createdAt 升序前 excess 个 id）删除
                    List<Long> oldestIds = alertRepo.findAllIdsOrderByCreatedAtAsc(
                            PageRequest.of(0, (int) Math.min(excess, 10_000), Sort.by("createdAt").ascending()));
                    int deleted = alertRepo.deleteByIds(oldestIds);
                    byMax = deleted;
                    if (deleted > 0) log.info("Alert retention: deleted {} oldest records (limit={})", deleted, retentionMaxRecords);
                }
            }
            if (byAge > 0 || byMax > 0) {
                log.info("Alert retention done: byAge={}, byMax={}, remaining={}", byAge, byMax, alertRepo.count());
            }
        } catch (Exception e) {
            log.warn("Alert retention cleanup failed: {}", e.getMessage());
        }
    }
}
