package com.datastream.mvp.service;

import com.datastream.mvp.model.AlertRecord;
import com.datastream.mvp.model.JobDefinition;
import com.datastream.mvp.model.JobLog;
import com.datastream.mvp.repository.AlertRecordRepository;
import com.datastream.mvp.repository.JobDefinitionRepository;
import com.datastream.mvp.repository.JobLogRepository;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 故障感知监控：每 30s 扫描一次作业运行态，命中规则后写告警记录 + 邮件/webhook。
 * 规则：作业失败 / 上线重启超限 / 吞吐归零 / 背压长时间高 / checkpoint 连续失败。
 * 同一事件去重基于 DB（重启后不重复告警）：离散事件（失败/重启超限）按故障发生时间去重，
 * 持续状态（吞吐/背压/checkpoint）按 15 分钟冷却去重。
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class HealthMonitor {

    private final JobDefinitionRepository jobRepo;
    private final JobLogRepository logRepo;
    private final AlertRecordRepository alertRepo;
    private final AlertService alertService;
    private final ObjectMapper objectMapper;

    @Value("${flink.cluster.host:localhost}")
    private String flinkHost;

    @Value("${flink.cluster.port:8081}")
    private int flinkPort;

    private final HttpClient httpClient = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(5))
            .build();

    /** 事件去重冷却（jobId:event -> 上次告警时间戳） */
    private final Map<String, Long> alertCooldown = new ConcurrentHashMap<>();
    private final Map<Long, Long> zeroThroughputSince = new ConcurrentHashMap<>();
    private final Map<Long, Long> backpressureSince = new ConcurrentHashMap<>();
    private static final long COOLDOWN_MS = 15 * 60 * 1000L;
    private static final long ZERO_THROUGHPUT_ALERT_MS = 2 * 60 * 1000L;
    private static final long BACKPRESSURE_ALERT_MS = 60 * 1000L;

    @Scheduled(fixedRate = 30000)
    public void supervise() {
        List<JobDefinition> all = jobRepo.findAllByOrderByUpdatedAtDesc();
        if (all.isEmpty()) return;
        for (JobDefinition job : all) {
            try {
                JobDefinition.JobStatus st = job.getStatus();
                if (st == JobDefinition.JobStatus.FAILED) {
                    checkFailed(job);
                }
                if (Boolean.TRUE.equals(job.getOnline())
                        && job.getOnlineRestartCount() != null && job.getOnlineRestartCount() >= 3) {
                    checkRestartOverLimit(job);
                }
                if (st == JobDefinition.JobStatus.RUNNING || st == JobDefinition.JobStatus.SUBMITTED) {
                    String fid = job.getFlinkJobId();
                    if (fid != null && !fid.startsWith("flink-job-") && !fid.startsWith("mock-") && !fid.startsWith("sql-submitted-")) {
                        checkLiveMetrics(job, fid);
                    }
                }
            } catch (Exception e) {
                log.debug("HealthMonitor job {} check failed: {}", job.getId(), e.getMessage());
            }
        }
    }

    /** 规则 1：作业失败（取最近一条 ERROR 日志作为证据） */
    private void checkFailed(JobDefinition job) {
        LocalDateTime occurred = job.getCompletedAt() != null ? job.getCompletedAt() : job.getUpdatedAt();
        if (!shouldAlert(job.getId(), "JOB_FAILED", occurred)) return;
        List<JobLog> errors = logRepo.findByJobIdAndLevelOrderByTimestampDesc(job.getId(), "ERROR");
        String detail = "无错误日志";
        if (!errors.isEmpty() && errors.get(0).getMessage() != null) {
            detail = errors.get(0).getMessage();
            if (detail.length() > 500) detail = detail.substring(0, 500);
        }
        alertService.sendAlert(job, "CRITICAL", "JOB_FAILED", "作业运行失败（状态=FAILED）：" + detail);
        markAlerted(job.getId(), "JOB_FAILED");
    }

    /** 规则 2：上线作业重启超限（JobScheduler 已自动下线） */
    private void checkRestartOverLimit(JobDefinition job) {
        LocalDateTime occurred = job.getLastRestartAt() != null ? job.getLastRestartAt() : job.getUpdatedAt();
        if (!shouldAlert(job.getId(), "ONLINE_JOB_STOPPED", occurred)) return;
        alertService.sendAlert(job, "CRITICAL", "ONLINE_JOB_STOPPED",
                "上线作业 10 分钟内自动重启超过 3 次，已自动下线停止处理（当前状态=" + job.getStatus() + "）");
        markAlerted(job.getId(), "ONLINE_JOB_STOPPED");
    }

    /** 规则 3/4/5：运行中作业的吞吐 / 背压 / checkpoint 指标 */
    private void checkLiveMetrics(JobDefinition job, String jid) {
        double outRate = queryDoubleMetric(jid, "numRecordsOutPerSecond");
        if (outRate <= 0) {
            long since = zeroThroughputSince.computeIfAbsent(job.getId(), k -> System.currentTimeMillis());
            if (System.currentTimeMillis() - since >= ZERO_THROUGHPUT_ALERT_MS && shouldAlert(job.getId(), "THROUGHPUT_ZERO", null)) {
                alertService.sendAlert(job, "WARN", "THROUGHPUT_ZERO",
                        "运行中作业持续 " + (ZERO_THROUGHPUT_ALERT_MS / 60000) + " 分钟无输出（吞吐=0 行/s），请检查数据源或下游阻塞");
                markAlerted(job.getId(), "THROUGHPUT_ZERO");
            }
        } else {
            zeroThroughputSince.remove(job.getId());
        }
        if (isBackpressureHigh(jid)) {
            long since = backpressureSince.computeIfAbsent(job.getId(), k -> System.currentTimeMillis());
            if (System.currentTimeMillis() - since >= BACKPRESSURE_ALERT_MS && shouldAlert(job.getId(), "BACKPRESSURE_HIGH", null)) {
                alertService.sendAlert(job, "WARN", "BACKPRESSURE_HIGH",
                        "作业出现持续高背压（backpressureLevel=HIGH），下游处理能力不足，建议降低吞吐或扩容并行度");
                markAlerted(job.getId(), "BACKPRESSURE_HIGH");
            }
        } else {
            backpressureSince.remove(job.getId());
        }
        if (hasCheckpointFailure(jid) && shouldAlert(job.getId(), "CHECKPOINT_FAILED", null)) {
            alertService.sendAlert(job, "WARN", "CHECKPOINT_FAILED", "作业最近 checkpoint 失败，请检查状态后端与网络稳定性");
            markAlerted(job.getId(), "CHECKPOINT_FAILED");
        }
    }

    private double queryDoubleMetric(String jid, String name) {
        try {
            String url = "http://" + flinkHost + ":" + flinkPort + "/jobs/" + jid + "/metrics?get=" + name;
            HttpResponse<String> resp = httpClient.send(HttpRequest.newBuilder()
                    .uri(URI.create(url)).GET().build(), HttpResponse.BodyHandlers.ofString());
            if (resp.statusCode() != 200) return -1;
            JsonNode arr = objectMapper.readTree(resp.body());
            if (arr.isArray() && arr.size() > 0 && arr.get(0).has("value")) {
                return Double.parseDouble(arr.get(0).get("value").asText("0"));
            }
        } catch (Exception e) {
            log.debug("metric query failed: {}", e.getMessage());
        }
        return -1;
    }

    private boolean isBackpressureHigh(String jid) {
        try {
            String url = "http://" + flinkHost + ":" + flinkPort + "/jobs/" + jid + "/backpressure";
            HttpResponse<String> resp = httpClient.send(HttpRequest.newBuilder()
                    .uri(URI.create(url)).GET().build(), HttpResponse.BodyHandlers.ofString());
            if (resp.statusCode() != 200) return false;
            JsonNode nodes = objectMapper.readTree(resp.body()).path("backpressure");
            if (nodes.isArray()) {
                for (JsonNode n : nodes) {
                    if ("HIGH".equals(n.path("backpressureLevel").asText())) return true;
                }
            }
        } catch (Exception e) {
            log.debug("backpressure query failed: {}", e.getMessage());
        }
        return false;
    }

    private boolean hasCheckpointFailure(String jid) {
        try {
            String url = "http://" + flinkHost + ":" + flinkPort + "/jobs/" + jid + "/checkpoints";
            HttpResponse<String> resp = httpClient.send(HttpRequest.newBuilder()
                    .uri(URI.create(url)).GET().build(), HttpResponse.BodyHandlers.ofString());
            if (resp.statusCode() != 200) return false;
            JsonNode root = objectMapper.readTree(resp.body());
            JsonNode counts = root.path("counts");
            if (counts.path("failed").asInt(0) > 0) return true;
            if (!root.path("latest").path("failed").isMissingNode()) return true;
        } catch (Exception e) {
            log.debug("checkpoint query failed: {}", e.getMessage());
        }
        return false;
    }

    /**
     * 告警去重（DB 持久化，重启后不重复告警）：
     * - occurrenceTime != null（离散事件，如作业失败/重启超限）：若该事件在“本次故障发生时间”之后已告警过，说明是同一故障，跳过；
     * - occurrenceTime == null（持续状态，如吞吐/背压/checkpoint）：15 分钟内已有告警则跳过（重启后同样生效）。
     */
    private boolean shouldAlert(Long jobId, String event, LocalDateTime occurrenceTime) {
        if (!cooledDown(jobId, event)) return false;
        Optional<AlertRecord> last = alertRepo.findFirstByJobIdAndEventOrderByCreatedAtDesc(jobId, event);
        if (last.isEmpty()) return true;
        LocalDateTime lastAt = last.get().getCreatedAt();
        if (occurrenceTime != null) {
            return lastAt.isBefore(occurrenceTime);
        }
        return lastAt.isBefore(LocalDateTime.now().minus(COOLDOWN_MS, ChronoUnit.MILLIS));
    }

    private boolean cooledDown(Long jobId, String event) {
        Long last = alertCooldown.get(jobId + ":" + event);
        return last == null || (System.currentTimeMillis() - last) >= COOLDOWN_MS;
    }

    private void markAlerted(Long jobId, String event) {
        alertCooldown.put(jobId + ":" + event, System.currentTimeMillis());
    }
}
