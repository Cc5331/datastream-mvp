package com.datastream.mvp.service;

import jakarta.mail.internet.MimeMessage;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.mail.javamail.MimeMessageHelper;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

/**
 * 告警邮件异步发送器。
 *
 * 告警由定时扫描线程（健康扫描 / 状态轮询 / 心跳探测）同步触发，
 * 若在扫描线程内直连 SMTP，邮件服务器不可达时每个告警会阻塞最多 10s，
 * 把单条扫描线程拖垮导致后续作业漏扫。故邮件发送独立到异步线程池执行。
 * SMTP 未配置时降级为日志兜底，不影响主流程。
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class AlertMailDispatcher {

    private final ObjectProvider<JavaMailSender> mailSenderProvider;

    @Value("${app.alert.email.host:}")
    private String mailHost;

    @Value("${app.alert.email.username:}")
    private String mailUsername;

    @Value("${app.alert.email.to:}")
    private String mailTo;

    @Async("alertTaskExecutor")
    public void send(String subject, String body) {
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
