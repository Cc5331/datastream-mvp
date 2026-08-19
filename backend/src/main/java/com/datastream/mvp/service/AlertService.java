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
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.mail.javamail.MimeMessageHelper;
import org.springframework.stereotype.Service;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
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

    @Value("${app.alert.email.host:}")
    private String mailHost;

    @Value("${app.alert.email.username:}")
    private String mailUsername;

    @Value("${app.alert.email.to:}")
    private String mailTo;

    private final HttpClient httpClient = HttpClient.newHttpClient();

    /**
     * 统一告警入口：记录 + webhook + 邮件。
     * @param level INFO / WARN / CRITICAL
     * @param event 事件类型（JOB_FAILED / ONLINE_JOB_STOPPED / THROUGHPUT_ZERO / ...）
     */
    public void sendAlert(JobDefinition job, String level, String event, String message) {
        try {
            AlertRecord rec = new AlertRecord();
            rec.setLevel(level == null ? "WARN" : level);
            rec.setEvent(event);
            rec.setJobId(job == null ? null : job.getId());
            rec.setJobName(job == null ? null : job.getName());
            rec.setMessage(message);
            rec.setReadFlag(false);
            rec.setCreatedAt(LocalDateTime.now());
            alertRepo.save(rec);
            log.info("[ALERT][{}] job={} {}: {}", rec.getLevel(), rec.getJobId(), event, message);
        } catch (Exception e) {
            log.warn("Alert record save failed: {}", e.getMessage());
        }
        if (job != null) sendWebhook(job, event, message);
        sendEmail("【数据流平台告警】" + event, message);
    }

    /** 仅记录告警（无作业上下文，如系统级事件） */
    public void sendAlert(String level, String event, String message) {
        sendAlert(null, level, event, message);
    }

    public void sendWebhook(JobDefinition job, String event, String message) {
        String url = job.getWebhookUrl();
        if (url == null || url.isBlank()) return;
        try {
            String body = objectMapper.createObjectNode()
                    .put("event", event)
                    .put("jobId", job.getId())
                    .put("jobName", job.getName())
                    .put("status", job.getStatus() != null ? job.getStatus().name() : "UNKNOWN")
                    .put("flinkJobId", job.getFlinkJobId())
                    .put("message", message)
                    .toString();
            httpClient.send(HttpRequest.newBuilder()
                            .uri(URI.create(url.trim()))
                            .header("Content-Type", "application/json")
                            .POST(HttpRequest.BodyPublishers.ofString(body)).build(),
                    HttpResponse.BodyHandlers.ofString());
            JobLog l = new JobLog();
            l.setJobId(job.getId());
            l.setLevel("INFO");
            l.setMessage("Webhook 告警已发送: " + url.trim() + "（event=" + event + "）");
            l.setTimestamp(LocalDateTime.now());
            logRepo.save(l);
            log.info("Webhook sent for job {}: {}", job.getId(), url.trim());
        } catch (Exception e) {
            log.warn("Webhook send failed for job {}: {}", job.getId(), e.getMessage());
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