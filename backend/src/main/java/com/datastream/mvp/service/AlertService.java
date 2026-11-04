package com.datastream.mvp.service;

import com.datastream.mvp.model.AlertRecord;
import com.datastream.mvp.model.JobDefinition;
import com.datastream.mvp.model.JobLog;
import com.datastream.mvp.repository.AlertRecordRepository;
import com.datastream.mvp.repository.JobLogRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Locale;

/**
 * 告警服务：webhook 通知 + 告警记录落库 + 邮件通知（异步，见 AlertMailDispatcher）。
 * SMTP 未配置时自动降级为日志记录，不影响主流程。
 *
 * <p>降噪：非 INFO 告警会先经 {@link AlertAggregator} 判定「同一根因、多个作业」的重复告警，
 * 窗口内同签名的第 2..N 条只累加计数（occurrences / aggregatedJobs / lastSeenAt），
 * 不再新建记录、不再发邮件与 webhook。INFO（恢复通知）必须逐条闭环，不参与聚合。</p>
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class AlertService {

    /** 聚合后缀的起始标记（重复合并时据此还原「原始告警文本」） */
    private static final String AGG_MARK = " ｜ 同类告警已合并 ";

    private final ObjectMapper objectMapper;
    private final JobLogRepository logRepo;
    private final AlertRecordRepository alertRepo;
    private final AlertMailDispatcher mailDispatcher;
    private final ApplicationEventPublisher eventPublisher;
    private final AlertAggregator alertAggregator;

    private final HttpClient httpClient = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(5))
            .build();

    /**
     * 统一告警入口：记录 + webhook + 邮件（INFO 级别不发邮件，避免打扰）。
     * @param level INFO / WARN / CRITICAL
     * @param event 事件类型（JOB_FAILED / ONLINE_JOB_STOPPED / THROUGHPUT_ZERO / ALERT_RESOLVED / ...）
     */
    public void sendAlert(JobDefinition job, String level, String event, String message) {
        String lvl = level == null ? "WARN" : level;
        boolean aggregatable = !"INFO".equalsIgnoreCase(lvl);
        // 签名时把作业名与 #id 当噪声抹掉：同一个根因在不同作业上必须能聚到一起
        String signature = aggregatable
                ? alertAggregator.signatureOf(event, message, job == null
                        ? java.util.List.of()
                        : java.util.List.of(String.valueOf(job.getId()), job.getName() == null ? "" : job.getName()))
                : null;

        // ① 同签名仍在聚合窗口内 -> 合并进既有记录，不再提醒
        if (aggregatable) {
            AlertAggregator.Bucket bucket = alertAggregator.findMergeable(signature);
            if (bucket != null && mergeIntoExisting(bucket, job, lvl, event, message)) {
                if (alertAggregator.allowAutoDiagnosis(signature)) publishAutoDiagnosis(job, lvl, event, message);
                return;
            }
        }

        // ② 正常路径：落库 + webhook + 邮件 + 自动诊断
        AlertRecord rec = saveNewRecord(job, lvl, event, message, signature);
        if (aggregatable && rec != null) {
            alertAggregator.register(signature, rec.getId(),
                    job == null ? null : job.getId(), job == null ? null : job.getName());
        }
        if (job != null) sendWebhook(job, lvl, event, message);
        // INFO 级（如恢复通知）不发邮件，只落库 + webhook
        if (!"INFO".equalsIgnoreCase(lvl)) {
            // 异步发送，避免 SMTP 超时阻塞定时扫描线程
            mailDispatcher.send(buildAlertMailSubject(event, lvl, job), buildAlertMailBody(job, lvl, event, message));
            // 真实故障告警 -> 异步触发自动智能诊断（仅作业级；同签名窗口内有配额，避免打爆 LLM）
            if (!aggregatable || alertAggregator.allowAutoDiagnosis(signature)) {
                publishAutoDiagnosis(job, lvl, event, message);
            }
        }
    }

    private AlertRecord saveNewRecord(JobDefinition job, String lvl, String event, String message, String signature) {
        try {
            AlertRecord rec = new AlertRecord();
            rec.setLevel(lvl);
            rec.setEvent(event);
            rec.setJobId(job == null ? null : job.getId());
            rec.setJobName(job == null ? null : job.getName());
            rec.setOwnerId(job == null ? null : job.getOwnerId());
            rec.setMessage(message);
            rec.setReadFlag(false);
            rec.setCreatedAt(LocalDateTime.now());
            rec.setSignature(signature);
            rec.setOccurrences(1);
            rec.setLastSeenAt(rec.getCreatedAt());
            rec.setAggregatedJobs(job == null ? null
                    : (job.getName() == null || job.getName().isBlank() ? "未命名作业" : job.getName().trim())
                        + "(#" + job.getId() + ")");
            AlertRecord saved = alertRepo.save(rec);
            log.info("[ALERT][{}] job={} {}: {}", saved.getLevel(), saved.getJobId(), event, message);
            return saved;
        } catch (Exception e) {
            log.warn("Alert record save failed: {}", e.getMessage());
            return null;
        }
    }

    /**
     * 把本次告警合并进窗口内既有记录：次数 +1、刷新最近时间、补记涉及作业、重新置未读。
     * @return false 表示既有记录已被删除（用户清理/保留策略），调用方应改为新建记录
     */
    private boolean mergeIntoExisting(AlertAggregator.Bucket bucket, JobDefinition job, String lvl, String event, String message) {
        try {
            AlertRecord rec = alertRepo.findById(bucket.alertId()).orElse(null);
            if (rec == null) return false;
            alertAggregator.merge(bucket, job == null ? null : job.getId(), job == null ? null : job.getName());
            String base = stripAggregationNote(rec.getMessage(), message);
            rec.setMessage(buildAggregatedMessage(base, bucket));
            rec.setOccurrences(bucket.occurrences());
            rec.setLastSeenAt(LocalDateTime.now());
            rec.setAggregatedJobs(String.join(", ", bucket.jobLabels()));
            rec.setLevel(strongerLevel(rec.getLevel(), lvl));
            rec.setReadFlag(false);          // 又发生了一次 -> 重新提示
            alertRepo.save(rec);
            log.info("[ALERT][AGG] job={} {}: 合并进告警 #{}（第 {} 次，涉及 {} 个作业）",
                    job == null ? null : job.getId(), event, rec.getId(), bucket.occurrences(), bucket.jobLabels().size());
            return true;
        } catch (Exception e) {
            log.warn("Alert aggregation merge failed (fallback to new record): {}", e.getMessage());
            return false;
        }
    }

    /** 去掉上一次合并时追加的后缀，拿到原始告警文本 */
    private String stripAggregationNote(String current, String fallbackMessage) {
        if (current == null) return fallbackMessage;
        int idx = current.indexOf(AGG_MARK);
        return idx < 0 ? current : current.substring(0, idx);
    }

    private String buildAggregatedMessage(String baseMessage, AlertAggregator.Bucket bucket) {
        List<String> labels = bucket.jobLabels();
        String latest = labels.isEmpty() ? "-" : labels.get(labels.size() - 1);
        String jobs = String.join("、", labels);
        if (labels.size() >= 20) jobs = jobs + " …";
        return baseMessage + AGG_MARK + bucket.occurrences() + " 次（涉及 " + Math.max(labels.size(), 1)
                + " 个作业：" + jobs + "；最近一次 " + latest + "）";
    }

    private String strongerLevel(String a, String b) {
        int ra = levelRank(a), rb = levelRank(b);
        return ra >= rb ? a : b;
    }

    private int levelRank(String level) {
        if (level == null) return 0;
        return switch (level.toUpperCase(Locale.ROOT)) {
            case "CRITICAL" -> 3;
            case "WARN" -> 2;
            case "INFO" -> 1;
            default -> 0;
        };
    }

    private void publishAutoDiagnosis(JobDefinition job, String level, String event, String message) {
        if (job == null) return;
        try {
            eventPublisher.publishEvent(AlertTriggeredEvent.from(job, level, event, message));
        } catch (Exception e) {
            log.warn("publish auto diagnosis event failed for job {}: {}", job.getId(), e.getMessage());
        }
    }

    /** 邮件主题：故障类型 + 作业名（系统级告警无作业名） */
    private String buildAlertMailSubject(String event, String level, JobDefinition job) {
        String tag = "CRITICAL".equalsIgnoreCase(level) ? "【严重】" : "【警告】";
        return tag + "数据流平台告警 " + event + (job != null ? " - " + job.getName() : "（系统级）");
    }

    /**
     * 邮件正文：故障类型、发生时间、涉及作业 ID + 诊断引导语（对应企业验收要求）。
     */
    private String buildAlertMailBody(JobDefinition job, String level, String event, String message) {
        LocalDateTime now = LocalDateTime.now();
        StringBuilder sb = new StringBuilder();
        sb.append("<div style=\"font-family:'Microsoft YaHei',Arial,sans-serif;max-width:640px;\">");
        sb.append("<h2 style=\"color:#c0392b;margin:0 0 4px;\">").append(event).append(" 告警</h2>");
        sb.append("<table style=\"border-collapse:collapse;font-size:14px;\">");
        sb.append(row("级别", level));
        sb.append(row("故障类型", event));
        sb.append(row("发生时间", now.toString().replace('T', ' ')));
        if (job != null) {
            sb.append(row("作业 ID", "#" + job.getId()));
            sb.append(row("作业名称", job.getName()));
            if (job.getFlinkJobId() != null) sb.append(row("Flink Job ID", job.getFlinkJobId()));
        } else {
            sb.append(row("范围", "系统级（非具体作业）"));
        }
        sb.append(row("详细信息", escape(message)));
        sb.append("</table>");
        sb.append("<p style=\"margin-top:12px;padding:10px 14px;background:#fef5e7;border-left:4px solid #e67e22;font-size:13px;\">")
          .append("🤖 智能诊断报告生成中——请稍后在平台「告警中心」点击该作业的「诊断」查看根因与修复建议。")
          .append("</p>");
        sb.append("</div>");
        return sb.toString();
    }

    private String row(String k, String v) {
        return "<tr><td style=\"padding:4px 12px 4px 0;color:#7f8c8d;white-space:nowrap;\">" + k
                + "</td><td style=\"padding:4px 0;color:#2c3e50;\"><b>" + escape(v == null ? "-" : v) + "</b></td></tr>";
    }

    private String escape(String s) {
        return s == null ? "" : s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;");
    }

    /** 仅记录告警（无作业上下文，如系统级事件） */
    public void sendAlert(String level, String event, String message) {
        sendAlert(null, level, event, message);
    }

    private void sendWebhook(JobDefinition job, String level, String event, String message) {
        String url = job.getWebhookUrl();
        if (url == null || url.isBlank()) return;
        String target = url.trim();
        try {
            String body = buildWebhookBody(target, job, level, event, message);
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(target))
                    .timeout(Duration.ofSeconds(10))
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(body))
                    .build();
            sendWebhookWithRetry(job, request, event, target, 0);
        } catch (Exception e) {
            saveWebhookLog(job.getId(), "WARN", "Webhook 告警配置无效（event=" + event + "）：" + e.getMessage());
            log.warn("Webhook send failed for job {}: {}", job.getId(), e.getMessage());
        }
    }

    /**
     * 识别 IM 机器人（钉钉/企业微信/飞书）并按其消息格式包装；其余发送原生 JSON。
     */
    private String buildWebhookBody(String target, JobDefinition job, String level, String event, String message) {
        String jobLabel = job.getName() + "（#" + job.getId() + "）";
        String emoji = "RESOLVED".equals(event) ? "✅" : "CRITICAL".equalsIgnoreCase(level) ? "🔴" : "🟡";
        String text = emoji + " [" + event + "] " + jobLabel + "\n" + message;
        if (target.contains("oapi.dingtalk.com")) {
            return objectMapper.createObjectNode()
                    .putObject("msgtype").put("text", "").putObject("text").put("content", text)
                    .toString();
        }
        if (target.contains("qyapi.weixin.qq.com")) {
            return objectMapper.createObjectNode()
                    .put("msgtype", "text")
                    .putObject("text")
                    .put("content", text)
                    .toString();
        }
        if (target.contains("open.feishu.cn")) {
            return objectMapper.createObjectNode()
                    .put("msg_type", "text")
                    .putObject("content")
                    .put("text", text)
                    .toString();
        }
        // 自定义/原生 webhook：保留结构化字段
        return objectMapper.createObjectNode()
                .put("event", event)
                .put("level", level)
                .put("jobId", job.getId())
                .put("jobName", job.getName())
                .put("status", job.getStatus() != null ? job.getStatus().name() : "UNKNOWN")
                .put("flinkJobId", job.getFlinkJobId())
                .put("message", message)
                .toString();
    }

    /** Webhook 失败有限重试（最多 2 次重试，指数退避 2s/4s），不阻塞调用线程 */
    private void sendWebhookWithRetry(JobDefinition job, HttpRequest request, String event, String target, int attempt) {
        final int maxRetries = 2;
        httpClient.sendAsync(request, HttpResponse.BodyHandlers.ofString())
                .whenComplete((response, error) -> {
                    boolean failed = error != null || response.statusCode() < 200 || response.statusCode() >= 300;
                    if (!failed) {
                        saveWebhookLog(job.getId(), "INFO", "Webhook 告警已发送: " + target + "（event=" + event + "）");
                        log.info("Webhook sent for job {}: {}", job.getId(), target);
                        return;
                    }
                    String reason = error != null ? error.getMessage() : "HTTP=" + response.statusCode();
                    if (attempt < maxRetries) {
                        long backoffMs = 2000L * (1L << attempt);
                        log.warn("Webhook attempt {} failed for job {} ({}), retrying in {}ms", attempt + 1, job.getId(), reason, backoffMs);
                        java.util.concurrent.CompletableFuture.delayedExecutor(backoffMs, java.util.concurrent.TimeUnit.MILLISECONDS)
                                .execute(() -> sendWebhookWithRetry(job, request, event, target, attempt + 1));
                    } else {
                        saveWebhookLog(job.getId(), "WARN",
                                "Webhook 告警发送失败（event=" + event + "，已重试 " + maxRetries + " 次）：" + reason);
                        log.warn("Webhook gave up after {} retries for job {}: {}", maxRetries, job.getId(), reason);
                    }
                });
    }

    private void saveWebhookLog(Long jobId, String level, String message) {
        try {
            JobLog entry = new JobLog();
            entry.setJobId(jobId);
            entry.setLevel(level);
            entry.setMessage(message);
            entry.setTimestamp(LocalDateTime.now());
            logRepo.save(entry);
        } catch (Exception e) {
            log.warn("Webhook result log save failed for job {}: {}", jobId, e.getMessage());
        }
    }
}
