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
import java.util.OptionalDouble;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 故障感知监控：默认每 10s（可配置 app.monitor.health-scan-ms）扫描一次作业运行态，命中规则后写告警记录 + 邮件/webhook。
 * 规则：作业失败 / 上线重启超限 / 吞吐归零 / 背压长时间高 / checkpoint 连续失败。
 * 同一事件去重基于 DB（重启后不重复告警）：离散事件（失败/重启超限）按故障发生时间去重，
 * 持续状态（吞吐/背压/checkpoint）按 15 分钟冷却去重。
 * 作业失败支持事件驱动：FlinkJobStatusChecker 在置 FAILED 时调用 notifyJobFailed 即时告警。
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

    @Value("${app.monitor.resource-cpu-percent:90}")
    private double resourceCpuThreshold;

    @Value("${app.monitor.resource-memory-percent:90}")
    private double resourceMemThreshold;

    @Value("${app.monitor.resource-disk-percent:90}")
    private double resourceDiskThreshold;

    private final HttpClient httpClient = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(5))
            .build();

    /** 事件去重冷却（jobId:event -> 上次告警时间戳） */
    private final Map<String, Long> alertCooldown = new ConcurrentHashMap<>();
    private final Map<Long, Long> zeroThroughputSince = new ConcurrentHashMap<>();
    private final Map<Long, Long> backpressureSince = new ConcurrentHashMap<>();
    private final Map<Long, Long> lastCheckpointFailure = new ConcurrentHashMap<>();
    /** 记录“曾经告过警”的故障（jobId -> 事件），用于恢复时发送 RESOLVED 通知 */
    private final Map<Long, java.util.Set<String>> activeFailures = new java.util.concurrent.ConcurrentHashMap<>();
    /** 资源超阈值起始时间（system -> 时间戳），持续超阈值才告警 */
    private volatile long resourceHighSince = 0;
    private volatile long resourceNormalSince = 0;
    /** 已扫描过的日志时间戳下限（jobId -> 扫描时间），避免重复告警同一批历史日志 */
    private final Map<Long, LocalDateTime> logScanWatermark = new ConcurrentHashMap<>();
    private static final long COOLDOWN_MS = 15 * 60 * 1000L;
    private static final long ZERO_THROUGHPUT_ALERT_MS = 2 * 60 * 1000L;
    private static final long BACKPRESSURE_ALERT_MS = 60 * 1000L;
    /** 资源持续超阈值告警窗口（默认 2 分钟） */
    private static final long RESOURCE_HIGH_ALERT_MS = 2 * 60 * 1000L;
    /** 资源恢复需连续低于阈值 5 个百分点并保持 2 分钟，避免临界值抖动反复告警。 */
    private static final long RESOURCE_RECOVERY_MS = 2 * 60 * 1000L;
    private static final double RESOURCE_RECOVERY_MARGIN = 5.0;

    @Scheduled(fixedRateString = "${app.monitor.health-scan-ms:10000}")
    public void supervise() {
        // 系统资源感知（全局，独立于作业）
        checkSystemResources();
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
                    // 作业恢复运行：把此前告过警的故障事件以 RESOLVED 闭环
                    checkRecovered(job, st);
                    String fid = job.getFlinkJobId();
                    if (fid != null && !fid.startsWith("flink-job-") && !fid.startsWith("mock-") && !fid.startsWith("sql-submitted-")) {
                        checkLiveMetrics(job, fid);
                    }
                    // 日志关键字感知：扫描运行中作业最近日志的 ERROR/Exception
                    checkLogKeywords(job);
                }
            } catch (Exception e) {
                log.debug("HealthMonitor job {} check failed: {}", job.getId(), e.getMessage());
            }
        }
    }

    /**
     * 事件驱动入口：作业在提交/运行阶段被置为 FAILED 时立即调用，无需等待下一次定时扫描。
     * 复用 checkFailed（含 DB 去重与证据日志），与定时扫描共享同一套逻辑，不会重复告警。
     */
    public void notifyJobFailed(JobDefinition job) {
        if (job == null) return;
        try {
            if (job.getStatus() == JobDefinition.JobStatus.FAILED) {
                checkFailed(job);
            }
        } catch (Exception e) {
            log.debug("HealthMonitor notifyJobFailed error for job {}: {}", job.getId(), e.getMessage());
        }
    }

    /**
     * 感知 1：服务器资源指标（CPU/内存/磁盘）。
     * 任一指标使用率 ≥ 阈值持续 RESOURCE_HIGH_ALERT_MS 后告警一次；恢复正常后 RESOLVED 闭环。
     * 阈值可配 app.monitor.resource-cpu/memory/disk-percent（默认 90/90/90）。
     */
    void checkSystemResources() {
        try {
            oshi.SystemInfo si = new oshi.SystemInfo();
            oshi.hardware.CentralProcessor cpu = si.getHardware().getProcessor();
            double cpuLoad = cpu.getSystemLoadAverage(3)[2] >= 0 ? cpu.getSystemLoadAverage(3)[2] * 100 / cpu.getLogicalProcessorCount()
                    : cpu.getSystemCpuLoadBetweenTicks(cpu.getSystemCpuLoadTicks()) * 100;
            oshi.hardware.GlobalMemory mem = si.getHardware().getMemory();
            double memPct = mem.getTotal() > 0 ? (mem.getTotal() - mem.getAvailable()) * 100.0 / mem.getTotal() : 0;
            oshi.software.os.OperatingSystem os = si.getOperatingSystem();
            double diskPct = 0;
            String diskLabel = "";
            for (oshi.software.os.OSFileStore fs : os.getFileSystem().getFileStores(true)) {
                if (fs.getTotalSpace() <= 0) continue;
                double pct = (fs.getTotalSpace() - fs.getUsableSpace()) * 100.0 / fs.getTotalSpace();
                if (pct > diskPct) { diskPct = pct; diskLabel = fs.getMount(); }
            }
            double cpuPct = Math.max(0, Math.min(100, cpuLoad));
            processResourceSample(cpuPct, memPct, diskPct, diskLabel, System.currentTimeMillis());
        } catch (Exception e) {
            log.debug("system resource check failed: {}", e.getMessage());
        }
    }

    void processResourceSample(double cpuPct, double memPct, double diskPct, String diskLabel, long now) {
        java.util.Set<String> systemFailures = activeFailures.computeIfAbsent(0L,
                k -> java.util.concurrent.ConcurrentHashMap.newKeySet());
        boolean high = cpuPct >= resourceCpuThreshold || memPct >= resourceMemThreshold || diskPct >= resourceDiskThreshold;
        if (high) {
            resourceNormalSince = 0;
            if (resourceHighSince == 0) resourceHighSince = now;
            if (now - resourceHighSince >= RESOURCE_HIGH_ALERT_MS
                    && !systemFailures.contains("RESOURCE_HIGH")
                    && shouldAlert(0L, "RESOURCE_HIGH", null)) {
                String detail = String.format("服务器资源持续 %d 分钟超阈值：CPU=%.0f%%(阈值%.0f%%) 内存=%.0f%%(阈值%.0f%%) 磁盘%s=%.0f%%(阈值%.0f%%)",
                        RESOURCE_HIGH_ALERT_MS / 60000, cpuPct, resourceCpuThreshold, memPct, resourceMemThreshold,
                        diskLabel, diskPct, resourceDiskThreshold);
                alertService.sendAlert(null, "WARN", "RESOURCE_HIGH", detail);
                markAlerted(0L, "RESOURCE_HIGH");
                systemFailures.add("RESOURCE_HIGH");
            }
            return;
        }

        resourceHighSince = 0;
        if (!systemFailures.contains("RESOURCE_HIGH")) {
            resourceNormalSince = 0;
            return;
        }
        boolean clearlyRecovered = cpuPct < resourceCpuThreshold - RESOURCE_RECOVERY_MARGIN
                && memPct < resourceMemThreshold - RESOURCE_RECOVERY_MARGIN
                && diskPct < resourceDiskThreshold - RESOURCE_RECOVERY_MARGIN;
        if (!clearlyRecovered) {
            resourceNormalSince = 0;
            return;
        }
        if (resourceNormalSince == 0) resourceNormalSince = now;
        if (now - resourceNormalSince >= RESOURCE_RECOVERY_MS && systemFailures.remove("RESOURCE_HIGH")) {
            resourceNormalSince = 0;
            alertService.sendAlert((JobDefinition) null, "INFO", "ALERT_RESOLVED",
                    "服务器资源已恢复正常（CPU=" + String.format("%.0f%%", cpuPct) + " 内存="
                            + String.format("%.0f%%", memPct) + " 磁盘=" + String.format("%.0f%%", diskPct) + "）");
        }
    }

    /**
     * 感知 2：日志关键字扫描。运行中作业最近日志出现 ERROR 级别（含 Exception 关键字）时告警。
     * 水位线（jobId -> 上次扫描时间）保证同一条日志只触发一次；单轮最多报 1 条避免刷屏。
     */
    void checkLogKeywords(JobDefinition job) {
        try {
            LocalDateTime watermark = logScanWatermark.get(job.getId());
            List<JobLog> errors = logRepo.findByJobIdAndLevelOrderByTimestampDesc(job.getId(), "ERROR");
            if (errors.isEmpty()) {
                logScanWatermark.put(job.getId(), LocalDateTime.now());
                return;
            }
            LocalDateTime newest = watermark == null ? LocalDateTime.now() : watermark;
            for (JobLog l : errors) {
                if (watermark != null && l.getTimestamp().isAfter(watermark)
                        && shouldAlert(job.getId(), "LOG_ERROR", null)) {
                    String msg = l.getMessage() == null ? "" : l.getMessage();
                    // 查询已按 level=ERROR 过滤，这里只保证告警正文非空（原条件 ... || msg.length() > 0 恒真，属笔误）
                    if (!msg.isBlank()) {
                        if (msg.length() > 300) msg = msg.substring(0, 300);
                        alertService.sendAlert(job, "WARN", "LOG_ERROR", "运行中作业日志出现异常关键字：" + msg);
                        markAlerted(job.getId(), "LOG_ERROR");
                        activeFailures.computeIfAbsent(job.getId(), k -> java.util.concurrent.ConcurrentHashMap.newKeySet()).add("LOG_ERROR");
                    }
                    break; // 单轮最多 1 条
                }
            }
            logScanWatermark.put(job.getId(), LocalDateTime.now());
        } catch (Exception e) {
            log.debug("log keyword check failed for job {}: {}", job.getId(), e.getMessage());
        }
    }

    /** 规则 1：作业失败（取最近一条 ERROR 日志作为证据） */
    private void checkFailed(JobDefinition job) {        LocalDateTime occurred = job.getCompletedAt() != null ? job.getCompletedAt() : job.getUpdatedAt();
        if (!shouldAlert(job.getId(), "JOB_FAILED", occurred)) return;
        List<JobLog> errors = logRepo.findByJobIdAndLevelOrderByTimestampDesc(job.getId(), "ERROR");
        String detail = "无错误日志";
        if (!errors.isEmpty() && errors.get(0).getMessage() != null) {
            detail = errors.get(0).getMessage();
            if (detail.length() > 500) detail = detail.substring(0, 500);
        }
        alertService.sendAlert(job, "CRITICAL", "JOB_FAILED", "作业运行失败（状态=FAILED）：" + detail);
        markAlerted(job.getId(), "JOB_FAILED");
        activeFailures.computeIfAbsent(job.getId(), k -> java.util.concurrent.ConcurrentHashMap.newKeySet()).add("JOB_FAILED");
    }

    /** 规则 2：上线作业重启超限（JobScheduler 已自动下线） */
    private void checkRestartOverLimit(JobDefinition job) {
        LocalDateTime occurred = job.getLastRestartAt() != null ? job.getLastRestartAt() : job.getUpdatedAt();
        if (!shouldAlert(job.getId(), "ONLINE_JOB_STOPPED", occurred)) return;
                alertService.sendAlert(job, "CRITICAL", "ONLINE_JOB_STOPPED",
                        "上线作业 10 分钟内自动重启超过 3 次，已自动下线停止处理（当前状态=" + job.getStatus() + "）");
        markAlerted(job.getId(), "ONLINE_JOB_STOPPED");
        activeFailures.computeIfAbsent(job.getId(), k -> java.util.concurrent.ConcurrentHashMap.newKeySet()).add("ONLINE_JOB_STOPPED");
    }

    /** 规则 3/4/5：运行中作业的吞吐 / 背压 / checkpoint 指标 */
    private void checkLiveMetrics(JobDefinition job, String jid) {
        OptionalDouble outRate = queryJobThroughput(jid);
        if (outRate.isPresent()) {
            if (outRate.getAsDouble() == 0) {
                long since = zeroThroughputSince.computeIfAbsent(job.getId(), k -> System.currentTimeMillis());
                if (System.currentTimeMillis() - since >= ZERO_THROUGHPUT_ALERT_MS && shouldAlert(job.getId(), "THROUGHPUT_ZERO", null)) {
                    alertService.sendAlert(job, "WARN", "THROUGHPUT_ZERO",
                            "运行中作业持续 " + (ZERO_THROUGHPUT_ALERT_MS / 60000) + " 分钟无输出（吞吐=0 行/s），请检查数据源或下游阻塞");
                    markAlerted(job.getId(), "THROUGHPUT_ZERO");
                    activeFailures.computeIfAbsent(job.getId(), k -> java.util.concurrent.ConcurrentHashMap.newKeySet()).add("THROUGHPUT_ZERO");
                }
            } else {
                // 吞吐恢复：发送 RESOLVED 并清零计时
                resolveIfAlerted(job, "THROUGHPUT_ZERO", "吞吐已恢复（" + (int) outRate.getAsDouble() + " 行/s）");
                zeroThroughputSince.remove(job.getId());
            }
        }
        if (isBackpressureHigh(jid)) {
            long since = backpressureSince.computeIfAbsent(job.getId(), k -> System.currentTimeMillis());
            if (System.currentTimeMillis() - since >= BACKPRESSURE_ALERT_MS && shouldAlert(job.getId(), "BACKPRESSURE_HIGH", null)) {
                alertService.sendAlert(job, "WARN", "BACKPRESSURE_HIGH",
                        "作业出现持续高背压（backpressureLevel=HIGH），下游处理能力不足，建议降低吞吐或扩容并行度");
                markAlerted(job.getId(), "BACKPRESSURE_HIGH");
                activeFailures.computeIfAbsent(job.getId(), k -> java.util.concurrent.ConcurrentHashMap.newKeySet()).add("BACKPRESSURE_HIGH");
            }
        } else {
            resolveIfAlerted(job, "BACKPRESSURE_HIGH", "背压已恢复正常水平");
            backpressureSince.remove(job.getId());
        }
        CheckpointFailure failure = latestCheckpointFailure(jid);
        if (failure != null && !Long.valueOf(failure.token()).equals(lastCheckpointFailure.put(job.getId(), failure.token()))
                && shouldAlert(job.getId(), "CHECKPOINT_FAILED", failure.occurredAt())) {
            alertService.sendAlert(job, "WARN", "CHECKPOINT_FAILED", "作业最新 checkpoint 失败，请检查状态后端与网络稳定性");
            markAlerted(job.getId(), "CHECKPOINT_FAILED");
            activeFailures.computeIfAbsent(job.getId(), k -> java.util.concurrent.ConcurrentHashMap.newKeySet()).add("CHECKPOINT_FAILED");
        } else if (failure == null) {
            resolveIfAlerted(job, "CHECKPOINT_FAILED", "checkpoint 已恢复正常（最新无失败）");
        }
    }

    /**
     * 恢复通知：此前告过警的持续状态恢复正常时，发送 INFO 级 RESOLVED 事件闭环。
     * 只在确实发出过对应告警（activeFailures 中登记）时才发送，避免噪声。
     */
    private void resolveIfAlerted(JobDefinition job, String event, String detail) {
        java.util.Set<String> events = activeFailures.get(job.getId());
        if (events == null || !events.remove(event)) return;
        alertService.sendAlert(job, "INFO", "ALERT_RESOLVED",
                "告警已恢复 [" + event + "]：" + detail);
    }

    /**
     * 作业恢复运行（RUNNING/SUBMITTED）时，把此前所有已告警的离散故障（JOB_FAILED / ONLINE_JOB_STOPPED）闭环为 RESOLVED。
     */
    private void checkRecovered(JobDefinition job, JobDefinition.JobStatus current) {
        java.util.Set<String> events = activeFailures.remove(job.getId());
        if (events == null || events.isEmpty()) return;
        for (String event : events) {
            alertService.sendAlert(job, "INFO", "ALERT_RESOLVED",
                    "作业已恢复运行（状态=" + current + "），故障 [" + event + "] 解除");
        }
    }

    /**
     * 查询作业吞吐（行/s）。
     * Flink SQL 的 Source→Sink 融合链上，作业级 numRecordsOutPerSecond 恒为 0/缺失，
     * 必须取算子（vertex）作用域指标（形如 0.numRecordsOutPerSecond）的最大值。
     */
    private OptionalDouble queryJobThroughput(String jid) {
        OptionalDouble result = queryVertexThroughput(jid);
        // 兜底：少数场景（如未融合的批作业）作业级通用指标可用
        return result.isPresent() ? result : queryDoubleMetric(jid, "numRecordsOutPerSecond");
    }

    /** 遍历全部 vertex，取 numRecordsOutPerSecond 的最大值（各算子指标带数字前缀） */
    private OptionalDouble queryVertexThroughput(String jid) {
        try {
            JsonNode detail = getJson("/jobs/" + jid);
            if (detail == null || !detail.has("vertices")) return OptionalDouble.empty();
            double max = -1;
            for (JsonNode v : detail.get("vertices")) {
                String vid = v.has("id") ? v.get("id").asText() : "";
                if (vid.isEmpty()) continue;
                JsonNode arr = getJson("/jobs/" + jid + "/vertices/" + vid + "/metrics");
                if (arr == null || !arr.isArray()) continue;
                for (JsonNode m : arr) {
                    String id = m.has("id") ? m.get("id").asText() : "";
                    if (!id.endsWith("numRecordsOutPerSecond")) continue;
                    // 指标值为 0/NaN 时 Flink 省略 value 字段，此处按 0 计（正是需要感知的零吞吐场景）
                    double val = m.hasNonNull("value") ? Double.parseDouble(m.get("value").asText()) : 0;
                    if (val > max) max = val;
                }
            }
            return max < 0 ? OptionalDouble.empty() : OptionalDouble.of(max);
        } catch (Exception e) {
            log.debug("vertex throughput query failed for {}: {}", jid, e.getMessage());
        }
        return OptionalDouble.empty();
    }

    /** 读取 Flink REST 相对路径，返回 JSON 或 null */
    private JsonNode getJson(String path) {
        try {
            String url = "http://" + flinkHost + ":" + flinkPort + path;
            HttpResponse<String> resp = httpClient.send(HttpRequest.newBuilder()
                    .uri(URI.create(url)).timeout(Duration.ofSeconds(5)).GET().build(), HttpResponse.BodyHandlers.ofString());
            if (resp.statusCode() != 200) return null;
            return objectMapper.readTree(resp.body());
        } catch (Exception e) {
            log.debug("flink REST {} failed: {}", path, e.getMessage());
            return null;
        }
    }

    private OptionalDouble queryDoubleMetric(String jid, String name) {
        try {
            JsonNode arr = getJson("/jobs/" + jid + "/metrics?get=" + name);
            if (arr != null && arr.isArray() && arr.size() > 0 && arr.get(0).hasNonNull("value")) {
                return OptionalDouble.of(Double.parseDouble(arr.get(0).get("value").asText()));
            }
        } catch (Exception e) {
            log.debug("metric query failed: {}", e.getMessage());
        }
        return OptionalDouble.empty();
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

    private CheckpointFailure latestCheckpointFailure(String jid) {
        try {
            String url = "http://" + flinkHost + ":" + flinkPort + "/jobs/" + jid + "/checkpoints";
            HttpResponse<String> resp = httpClient.send(HttpRequest.newBuilder()
                    .uri(URI.create(url)).timeout(Duration.ofSeconds(5)).GET().build(), HttpResponse.BodyHandlers.ofString());
            if (resp.statusCode() != 200) return null;
            JsonNode latest = objectMapper.readTree(resp.body()).path("latest");
            JsonNode failed = latest.path("failed");
            if (!failed.isObject() || failed.isEmpty()) return null;
            long failedId = failed.path("id").asLong(-1);
            long completedId = latest.path("completed").path("id").asLong(-1);
            if (failedId < 0 || failedId <= completedId) return null;
            long timestamp = failed.path("failure_timestamp").asLong(
                    failed.path("trigger_timestamp").asLong(System.currentTimeMillis()));
            LocalDateTime occurredAt = java.time.Instant.ofEpochMilli(timestamp)
                    .atZone(java.time.ZoneId.systemDefault()).toLocalDateTime();
            return new CheckpointFailure(failedId, occurredAt);
        } catch (Exception e) {
            log.debug("checkpoint query failed: {}", e.getMessage());
            return null;
        }
    }

    private record CheckpointFailure(long token, LocalDateTime occurredAt) {}

    /**
     * 告警去重（DB 持久化，重启后不重复告警）：
     * - occurrenceTime != null（离散事件，如作业失败/重启超限）：若该事件在“本次故障发生时间”之后已告警过，说明是同一故障，跳过。
     *   重复手动重跑：每次失败 completedAt 都会刷新 → 新 occurred > 上一条告警 createdAt → 天然放行产生新告警；
     *   同一次故障内轮询器/扫描器重复检测：occurred 未变，被 lastAt >= occurred 拦住，不会刷屏。
     * - occurrenceTime == null（持续状态，如吞吐/背压/checkpoint）：15 分钟内已有告警则跳过（重启后同样生效）。
     */
    private boolean shouldAlert(Long jobId, String event, LocalDateTime occurrenceTime) {
        // 离散事件不受内存冷却限制：事件驱动（notifyJobFailed）需要立刻穿透，DB 时间比较已足够去重
        if (occurrenceTime == null && !cooledDown(jobId, event)) return false;
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
