package com.datastream.mvp.service;

import com.datastream.mvp.model.AlertRecord;
import com.datastream.mvp.model.JobDefinition;
import com.datastream.mvp.repository.AlertRecordRepository;
import com.datastream.mvp.repository.JobDefinitionRepository;
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
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 心跳感知：监测 Flink JobManager / TaskManager 存活，以及 RUNNING 作业是否从集群注册表消失。
 * 与 HealthMonitor 独立，专司“心跳类”故障：
 *  - CLUSTER_HEARTBEAT_LOST（系统级，job=null）：JobManager REST 不可达或 TaskManager 数为 0，连续 CLUSTER_MISSES 次后告警；
 *  - JOB_HEARTBEAT_LOST（作业级）：本地 RUNNING/SUBMITTED 作业对应的 Flink jid 不再出现在 /jobs/overview，连续 JOB_MISSES 次后告警。
 * 告警基于 DB 冷却（HEARTBEAT_COOLDOWN_MS），恢复后发送 ALERT_RESOLVED 闭环。
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class HeartbeatMonitor {

    private final JobDefinitionRepository jobRepo;
    private final AlertRecordRepository alertRepo;
    private final AlertService alertService;
    private final ObjectMapper objectMapper;

    @Value("${flink.cluster.host:localhost}")
    private String flinkHost;

    @Value("${flink.cluster.port:8081}")
    private int flinkPort;

    /** 连续多少次无 TaskManager / 集群不可达才告警 */
    @Value("${app.monitor.heartbeat.cluster-misses:3}")
    private int clusterMisses;

    /** 连续多少次作业从集群消失才告警 */
    @Value("${app.monitor.heartbeat.job-misses:3}")
    private int jobMisses;

    private static final long HEARTBEAT_COOLDOWN_MS = 30 * 60 * 1000L;

    private final HttpClient httpClient = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(5))
            .build();

    /** RUNNING 本地作业 -> 已连续探测到从集群消失的次数 */
    private final ConcurrentHashMap<Long, Integer> jobMissingCount = new ConcurrentHashMap<>();
    private volatile int clusterFailCount = 0;
    private volatile boolean clusterAlertActive = false;

    @Scheduled(fixedRateString = "${app.monitor.heartbeat-scan-ms:10000}")
    public void scanHeartbeats() {
        Set<String> liveFlinkJids;
        try {
            liveFlinkJids = probeClusterJids();
        } catch (Exception e) {
            log.debug("heartbeat probe failed: {}", e.getMessage());
            return;
        }

        if (liveFlinkJids == null) {
            // 集群不可达：累计失败并可能告警集群心跳丢失
            clusterFailCount++;
            if (clusterFailCount >= clusterMisses) emitClusterLost();
            return;
        }

        clusterFailCount = 0;
        resolveCluster();

        for (JobDefinition job : jobRepo.findByStatusIn(List.of(
                JobDefinition.JobStatus.RUNNING, JobDefinition.JobStatus.SUBMITTED))) {
            handleJobProbe(job, liveFlinkJids);
        }
        // 清理已不再活跃作业的计数
        Set<Long> activeIds = jobRepo.findByStatusIn(List.of(
                        JobDefinition.JobStatus.RUNNING, JobDefinition.JobStatus.SUBMITTED))
                .stream().map(JobDefinition::getId).collect(java.util.stream.Collectors.toSet());
        jobMissingCount.keySet().retainAll(activeIds);
    }

    void handleJobProbe(JobDefinition job, Set<String> liveFlinkJids) {
        String fid = job.getFlinkJobId();
        if (fid == null || fid.startsWith("flink-job-") || fid.startsWith("mock-") || fid.startsWith("sql-submitted-")) {
            jobMissingCount.remove(job.getId());
            return;
        }
        if (liveFlinkJids.contains(fid)) {
            if (jobMissingCount.remove(job.getId()) != null) {
                alertService.sendAlert(job, "INFO", "ALERT_RESOLVED", "作业心跳已恢复：" + job.getName());
            }
        } else {
            int misses = jobMissingCount.merge(job.getId(), 1, Integer::sum);
            if (misses >= jobMisses) emitJobLost(job);
        }
    }

    void handleClusterMissing() {
        clusterFailCount++;
        if (clusterFailCount >= clusterMisses) emitClusterLost();
    }

    void handleClusterAlive() {
        clusterFailCount = 0;
        resolveCluster();
    }

    /**
     * 探测集群：返回当前在册的 Flink job id 集合；若 REST 不可达或 TaskManager 数为 0，返回 null。
     */
    private Set<String> probeClusterJids() throws Exception {
        HttpResponse<String> tms = httpClient.send(HttpRequest.newBuilder()
                .uri(URI.create("http://" + flinkHost + ":" + flinkPort + "/taskmanagers"))
                .timeout(Duration.ofSeconds(5)).GET().build(), HttpResponse.BodyHandlers.ofString());
        if (tms.statusCode() != 200) return null;
        JsonNode tmsNode = objectMapper.readTree(tms.body());
        if (!tmsNode.has("taskmanagers") || tmsNode.path("taskmanagers").isEmpty()) return null;

        HttpResponse<String> overview = httpClient.send(HttpRequest.newBuilder()
                .uri(URI.create("http://" + flinkHost + ":" + flinkPort + "/jobs/overview"))
                .timeout(Duration.ofSeconds(5)).GET().build(), HttpResponse.BodyHandlers.ofString());
        if (overview.statusCode() != 200) return null;
        Set<String> alive = new HashSet<>();
        for (JsonNode fj : objectMapper.readTree(overview.body()).path("jobs")) {
            String jid = fj.path("jid").asText("");
            if (!jid.isEmpty()) alive.add(jid);
        }
        return alive;
    }

    private void resolveCluster() {
        if (clusterAlertActive) {
            clusterAlertActive = false;
            alertService.sendAlert(null, "INFO", "ALERT_RESOLVED", "Flink 集群心跳已恢复（JobManager/TaskManager 可达）");
        }
    }

    private void emitClusterLost() {
        if (clusterAlertActive) return;
        if (!cooledDown(null, "CLUSTER_HEARTBEAT_LOST")) return;
        clusterAlertActive = true;
        alertService.sendAlert(null, "CRITICAL", "CLUSTER_HEARTBEAT_LOST",
                "Flink 集群心跳丢失：JobManager REST 不可达或 TaskManager 数为 0，请检查集群进程与网络");
    }

    private void emitJobLost(JobDefinition job) {
        if (!cooledDown(job.getId(), "JOB_HEARTBEAT_LOST")) return;
        alertService.sendAlert(job, "CRITICAL", "JOB_HEARTBEAT_LOST",
                "作业心跳丢失：作业 " + job.getName() + " 已从 Flink 集群注册表消失（可能 TaskManager 失联或作业被意外移除）");
    }

    private boolean cooledDown(Long jobId, String event) {
        Optional<AlertRecord> last = jobId == null
                ? alertRepo.findFirstByEventOrderByCreatedAtDesc(event)
                : alertRepo.findFirstByJobIdAndEventOrderByCreatedAtDesc(jobId, event);
        return last.isEmpty()
                || last.get().getCreatedAt().isBefore(LocalDateTime.now().minusNanos(HEARTBEAT_COOLDOWN_MS * 1_000_000));
    }
}
