package com.datastream.mvp.service;

import com.datastream.mvp.model.JobDefinition;
import com.datastream.mvp.repository.JobDefinitionRepository;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.*;

/**
 * Flink 作业实时指标采集（吞吐 / 背压 / checkpoint），供前端监控面板轮询。
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class FlinkMetricsService {

    private final JobDefinitionRepository jobRepo;
    private final ObjectMapper objectMapper;

    @Value("${flink.cluster.host:localhost}")
    private String flinkHost;
    @Value("${flink.cluster.port:8081}")
    private int flinkPort;

    private final HttpClient httpClient = HttpClient.newHttpClient();

    /** 所有运行中（RUNNING / SUBMITTED）作业的实时指标概览 */
    public List<Map<String, Object>> overview() {
        List<JobDefinition> running = jobRepo.findByStatusIn(
                List.of(JobDefinition.JobStatus.RUNNING, JobDefinition.JobStatus.SUBMITTED));
        List<Map<String, Object>> result = new ArrayList<>();
        for (JobDefinition job : running) {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("id", job.getId());
            m.put("name", job.getName());
            m.put("status", job.getStatus().name());
            m.put("flinkJobId", job.getFlinkJobId());
            m.put("submittedAt", job.getSubmittedAt());
            m.put("metrics", fetchMetrics(job.getFlinkJobId()));
            result.add(m);
        }
        return result;
    }

    private Map<String, Object> fetchMetrics(String flinkJobId) {
        Map<String, Object> out = new LinkedHashMap<>();
        if (flinkJobId == null || flinkJobId.isBlank() || flinkJobId.startsWith("flink-job-")) {
            out.put("available", false);
            out.put("reason", "mock 作业或无 Flink Job ID，无法获取实时指标");
            return out;
        }
        try {
            String base = "http://" + flinkHost + ":" + flinkPort;
            JsonNode job = getJson(base + "/jobs/" + flinkJobId);
            out.put("available", true);
            out.put("state", job.path("state").asText("UNKNOWN"));
            out.put("duration", job.path("duration").asLong(-1));
            JsonNode vertices = job.path("vertices");
            out.put("vertices", vertices.size());
            String firstVid = vertices.size() > 0 ? vertices.get(0).path("id").asText() : "";

            // 吞吐：vertex 级完整指标名（Flink 指标带 0. 前缀）
            Map<String, String> throughput = new LinkedHashMap<>();
            if (!firstVid.isEmpty()) {
                JsonNode m = getJson(base + "/jobs/" + flinkJobId + "/vertices/" + firstVid
                        + "/metrics?get=0.numRecordsInPerSecond,0.numRecordsOutPerSecond,0.busyTimeMsPerSecond,0.backPressuredTimeMsPerSecond");
                if (m.isArray()) {
                    for (JsonNode n : m) {
                        String id = n.path("id").asText();
                        String key = id.contains("numRecordsInPerSecond") ? "numRecordsInPerSecond"
                                : id.contains("numRecordsOutPerSecond") ? "numRecordsOutPerSecond"
                                : id.contains("busyTimeMsPerSecond") ? "busyTimeMsPerSecond"
                                : id.contains("backPressuredTimeMsPerSecond") ? "backPressuredTimeMsPerSecond" : id;
                        throughput.put(key, n.path("value").asText());
                    }
                }
            }
            out.put("throughput", throughput);

            // 背压：vertex 级，汇总所有 vertex 的最差级别
            String worstLevel = "ok";
            int subtaskCount = 0;
            for (JsonNode v : vertices) {
                String vv = v.path("id").asText();
                try {
                    JsonNode bp = getJson(base + "/jobs/" + flinkJobId + "/vertices/" + vv + "/backpressure");
                    String lv = bp.path("backpressureLevel").asText("ok");
                    subtaskCount += bp.path("subtasks").size();
                    if ("high".equals(lv)) worstLevel = "high";
                    else if ("low".equals(lv) && !"high".equals(worstLevel)) worstLevel = "low";
                } catch (Exception ignored) {
                    // 该 vertex 无背压数据时忽略
                }
            }
            out.put("backpressure", Map.of("level", worstLevel, "subtasks", subtaskCount));

            // checkpoint
            try {
                JsonNode cp = getJson(base + "/jobs/" + flinkJobId + "/checkpoints");
                JsonNode latest = cp.path("latest");
                JsonNode completed = latest.path("completed");
                Map<String, Object> cpInfo = new LinkedHashMap<>();
                cpInfo.put("counts", cp.path("counts").toString());
                if (completed.isObject() && !completed.isEmpty()) {
                    cpInfo.put("lastCompletedTs", completed.path("trigger_timestamp").asLong(-1));
                    cpInfo.put("endToEndDuration", completed.path("end_to_end_duration").asLong(-1));
                    cpInfo.put("stateSize", completed.path("state_size").asLong(-1));
                } else {
                    cpInfo.put("lastCompletedTs", null);
                }
                out.put("checkpoint", cpInfo);
            } catch (Exception e) {
                out.put("checkpoint", Map.of("error", e.getMessage()));
            }
        } catch (Exception e) {
            log.warn("Flink metrics fetch failed for {}: {}", flinkJobId, e.getMessage());
            out.put("available", false);
            out.put("reason", "Flink REST 拉取失败: " + e.getMessage());
        }
        return out;
    }

    private JsonNode getJson(String url) throws Exception {
        HttpResponse<String> resp = httpClient.send(
                HttpRequest.newBuilder().uri(URI.create(url)).GET().build(),
                HttpResponse.BodyHandlers.ofString());
        if (resp.statusCode() != 200) throw new RuntimeException("HTTP " + resp.statusCode());
        return objectMapper.readTree(resp.body());
    }
}
