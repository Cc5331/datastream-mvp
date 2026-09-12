package com.datastream.mvp.service;

import com.datastream.mvp.model.AlertRecord;
import com.datastream.mvp.model.JobDefinition;
import com.datastream.mvp.model.JobLog;
import com.datastream.mvp.repository.AlertRecordRepository;
import com.datastream.mvp.repository.JobLogRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.mail.internet.MimeMessage;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.mail.javamail.MimeMessageHelper;
import org.springframework.stereotype.Service;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.LocalDateTime;

/**
 * 告警服务：webhook 通知 + 告警记录落库 + 邮件通知（QQ 邮箱 SMTP）。
 * SMTP 未配置时自动降级为日志记录，不影响主流程。
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class AlertService {

    private final ObjectMapper objectMapper;
    private final JobLogRepository logRepo;
    private final AlertRecordRepository alertRepo;
    private final ObjectProvider<JavaMailSender> mailSenderProvider;
    private final ApplicationEventPublisher eventPublisher;

    @Value("${app.alert.email.host:}")
    private String mailHost;

    @Value("${app.alert.email.username:}")
    private String mailUsername;

    @Value("${app.alert.email.to:}")
    private String mailTo;

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
            alertRepo.save(rec);
            log.info("[ALERT][{}] job={} {}: {}", rec.getLevel(), rec.getJobId(), event, message);
        } catch (Exception e) {
            log.warn("Alert record save failed: {}", e.getMessage());
        }
        if (job != null) sendWebhook(job, lvl, event, message);
        // INFO 级（如恢复通知）不发邮件，只落库 + webhook
        if (!"INFO".equalsIgnoreCase(lvl)) {
            sendEmail(buildAlertMailSubject(event, lvl, job), buildAlertMailBody(job, lvl, event, message));
            // 真实故障告警 -> 异步触发自动智能诊断（仅作业级）
            publishAutoDiagnosis(job, lvl, event, message);
        }
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

    /**
     * 邮件通知：SMTP 未配置时记录“邮件日志”兜底，保证演示可观测。
     */
    public void sendEmail(String subject, String body) {
        if (mailHost == null || mailHost.isBlank() || mailTo == null || mailTo.isBlank()) {
            log.info("[MAIL-FALLBACK] 未配置 SMTP（SMTP_HOST/SMTP_TO），跳过发送。主题: {}", subject);
            return;
        }
        try {
            JavaMailSender sender = mailSenderProvider.getIfAvailable();
            if (sender == null) {
                log.info("[MAIL-FALLBACK] JavaMailSender 不可用（spring.mail.host 未配置），主题: {}", subject);
                return;
            }
            MimeMessage msg = sender.createMimeMessage();
            MimeMessageHelper helper = new MimeMessageHelper(msg, true, "UTF-8");
            helper.setFrom(mailUsername == null || mailUsername.isBlank() ? mailTo : mailUsername);
            helper.setTo(mailTo.split("[,;]"));
            helper.setSubject(subject);
            helper.setText(body, true);
            sender.send(msg);
            log.info("[MAIL] 告警邮件已发送至 {}: {}", mailTo, subject);
        } catch (Exception e) {
            log.warn("[MAIL] 邮件发送失败（不影响主流程）: {}", e.getMessage());
        }
    }
}