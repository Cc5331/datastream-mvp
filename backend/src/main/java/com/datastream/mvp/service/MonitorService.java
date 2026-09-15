package com.datastream.mvp.service;

import com.datastream.mvp.model.JobDefinition;
import com.datastream.mvp.model.TrendPoint;
import com.datastream.mvp.repository.JobDefinitionRepository;
import com.datastream.mvp.repository.TrendPointRepository;
import com.datastream.mvp.security.CurrentUser;
import jakarta.annotation.PostConstruct;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import com.fasterxml.jackson.databind.node.ArrayNode;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * 实时监控服务：聚合 Flink REST API 的作业指标（吞吐/背压/checkpoint），供前端监控面板展示
 * 运行中作业展示实时吞吐/背压/checkpoint；最近结束作业展示总处理行数与耗时
 * 历史作业不依赖 Flink 归档（JobManager 重启后历史会丢失）：耗时回退 DB completedAt-submittedAt，行数回退输出文件统计
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class MonitorService {

    private final JobDefinitionRepository jobRepo;
    private final TrendPointRepository trendRepo;
    private final ObjectMapper objectMapper;

    @Value("${flink.cluster.host:localhost}")
    private String flinkHost;

    @Value("${flink.cluster.port:8081}")
    private int flinkPort;

    private final HttpClient httpClient = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(3))
            .build();

    /** 最近结束作业最多展示条数 */
    private static final int RECENT_LIMIT = 6;
    private static final int MAX_TREND_POINTS = 300;
    private final Map<Long, Deque<ObjectNode>> trendBuffer = new ConcurrentHashMap<>();
    /** 趋势缓冲对应的运行批次（jobId -> submittedAt），新一次运行重置趋势 */
    private final Map<Long, String> trendRunKeys = new ConcurrentHashMap<>();
    /** 最近一次运行的实时快照（jobId -> item），作业结束后卡片兜底展示 */
    private final Map<Long, ObjectNode> lastLiveSnapshot = new ConcurrentHashMap<>();

    public List<ObjectNode> overview() {
        return overview(null);
    }

    /**
     * 监控总览。cu 为 null 时不做归属过滤（仅供系统级 AI 诊断聚合使用）；
     * 传入当前用户时，非管理员只能看到自己创建的作业（无归属作业仅管理员可见）。
     */
    public List<ObjectNode> overview(CurrentUser cu) {
        List<ObjectNode> result = new ArrayList<>();
        Set<Long> seen = new HashSet<>();
        List<JobDefinition> active = jobRepo.findByStatusIn(List.of(
                JobDefinition.JobStatus.SUBMITTED, JobDefinition.JobStatus.RUNNING));
        for (JobDefinition job : active) {
            if (!canSee(job, cu)) continue;
            String fid = job.getFlinkJobId();
            if (fid == null || isMockFlinkId(fid)) continue;
            ObjectNode item = buildJobMetrics(job, fid, true);
            if (item != null) {
                result.add(item);
                seen.add(job.getId());
                JsonNode liveMetrics = item.get("metrics");
                if (liveMetrics != null && liveMetrics.path("available").asBoolean(false)
                        && "live".equals(liveMetrics.path("mode").asText())) {
                    lastLiveSnapshot.put(job.getId(), item.deepCopy());
                }
            }
        }
        List<JobDefinition> recent = jobRepo.findAllByOrderByUpdatedAtDesc();
        int added = 0;
        for (JobDefinition job : recent) {
            if (added >= RECENT_LIMIT) break;
            if (seen.contains(job.getId())) continue;
            if (!canSee(job, cu)) continue;
            JobDefinition.JobStatus st = job.getStatus();
            if (st != JobDefinition.JobStatus.COMPLETED
                    && st != JobDefinition.JobStatus.FAILED
                    && st != JobDefinition.JobStatus.CANCELLED) continue;
            String fid = job.getFlinkJobId();
            if (fid == null || isMockFlinkId(fid)) continue;
            ObjectNode item = buildJobMetrics(job, fid, false);
            if (item != null) {
                result.add(item);
                added++;
            }
        }
        return result;
    }

    @Scheduled(fixedDelay = 2000)
    public void collectTrendMetrics() {
        List<JobDefinition> active = jobRepo.findByStatusIn(List.of(
                JobDefinition.JobStatus.SUBMITTED, JobDefinition.JobStatus.RUNNING));
        for (JobDefinition job : active) {
            String fid = job.getFlinkJobId();
            if (fid == null || isMockFlinkId(fid)) continue;
            ObjectNode item = buildJobMetrics(job, fid, true);
            if (item == null || !item.path("metrics").path("available").asBoolean(false)) continue;
            recordTrendPoint(job, item);
            lastLiveSnapshot.put(job.getId(), item.deepCopy());
        }

        List<JobDefinition> completed = jobRepo.findByStatusAndCompletedAtAfter(
                JobDefinition.JobStatus.COMPLETED, java.time.LocalDateTime.now().minusMinutes(3));
        for (JobDefinition job : completed) {
            if (hasCompletedTrend(job)) continue;
            String fid = job.getFlinkJobId();
            if (fid == null || isMockFlinkId(fid)) continue;
            ObjectNode history = buildJobMetrics(job, fid, false);
            JsonNode metrics = history == null ? null : history.get("metrics");
            double average = metrics == null ? 0 : metrics.path("avgRowsPerSecond").asDouble(0);
            if (average <= 0) continue;
            recordCompletedTrend(job, average);
        }
    }

    private boolean hasCompletedTrend(JobDefinition job) {
        String runKey = job.getSubmittedAt() == null ? "" : job.getSubmittedAt().toString();
        Deque<ObjectNode> points = trendBuffer.get(job.getId());
        if (points == null || !runKey.equals(trendRunKeys.get(job.getId()))) return false;
        synchronized (points) {
            return points.stream().anyMatch(point -> "COMPLETED".equals(point.path("status").asText()));
        }
    }

    private boolean isMockFlinkId(String fid) {
        return fid.startsWith("flink-job-") || fid.startsWith("mock-") || fid.startsWith("sql-submitted-");
    }

    private ObjectNode buildJobMetrics(JobDefinition job, String jid, boolean live) {
        ObjectNode item = objectMapper.createObjectNode();
        item.put("id", job.getId());
        item.put("name", job.getName());
        item.put("status", job.getStatus().name());
        item.put("flinkJobId", jid);
        item.put("submittedAt", job.getSubmittedAt() == null ? "" : job.getSubmittedAt().toString());
        ObjectNode metrics = objectMapper.createObjectNode();
        metrics.put("mode", live ? "live" : "history");
        try {
            JsonNode detail = getJson("/jobs/" + jid);
            // 历史模式允许 Flink 详情缺失（JobManager 重启后归档丢失），用 DB + 输出文件兜底
            if (detail == null && live) {
                metrics.put("available", false);
                metrics.put("reason", "Flink 作业详情不可用");
                item.set("metrics", metrics);
                return item;
            }
            long startTime = detail != null && detail.has("start-time") ? detail.get("start-time").asLong() : 0L;
            long now = System.currentTimeMillis();
            if (live) {
                String state = detail.has("state") ? detail.get("state").asText() : "";
                if (!"RUNNING".equals(state) && !"INITIALIZING".equals(state) && !"RECONCILING".equals(state)) {
                    metrics.put("available", false);
                    metrics.put("reason", "Flink 状态: " + state);
                    item.set("metrics", metrics);
                    return item;
                }
                metrics.put("duration", startTime > 0 ? now - startTime : -1);
                collectLiveMetrics(detail, jid, metrics);
            } else {
                metrics.put("duration", resolveHistoryDuration(job, detail));
                collectHistoryMetrics(job, detail, metrics);
            }
            metrics.put("available", true);
        } catch (Exception e) {
            log.warn("Monitor metrics failed for job {} ({}): {}", job.getId(), jid, e.getMessage());
            metrics.put("available", false);
            metrics.put("reason", "指标获取失败: " + e.getMessage());
        }
        item.set("metrics", metrics);
        return item;
    }

    /** 历史耗时：优先 Flink end-time - start-time，其次 DB completedAt - submittedAt，取消作业回退 updatedAt - submittedAt */
    private long resolveHistoryDuration(JobDefinition job, JsonNode detail) {
        if (detail != null) {
            long startTime = detail.has("start-time") ? detail.get("start-time").asLong() : 0L;
            long endTime = detail.has("end-time") ? detail.get("end-time").asLong() : 0L;
            if (startTime > 0 && endTime >= startTime) return endTime - startTime;
        }
        if (job.getSubmittedAt() != null) {
            if (job.getCompletedAt() != null) {
                long d = Duration.between(job.getSubmittedAt(), job.getCompletedAt()).toMillis();
                if (d >= 0) return d;
            }
            if (job.getUpdatedAt() != null) {
                long d = Duration.between(job.getSubmittedAt(), job.getUpdatedAt()).toMillis();
                if (d >= 0) return d;
            }
        }
        return -1;
    }

    private void collectLiveMetrics(JsonNode detail, String jid, ObjectNode metrics) {
        double totalIn = 0, totalOut = 0, maxBpRatio = 0;
        JsonNode vertices = detail.get("vertices");
        if (vertices != null && vertices.isArray()) {
            for (JsonNode v : vertices) {
                String vid = v.has("id") ? v.get("id").asText() : "";
                // Flink SQL 的 Source->Sink 融合链上，通用 numRecordsIn/OutPerSecond 恒为 0，
                // 需从算子作用域指标（Source__*.numRecordsOutPerSecond / Sink__*.numRecordsInPerSecond 等）取最大汇总值
                double[] rates = collectVertexRates(jid, vid);
                totalIn += rates[0];
                totalOut += rates[1];
                JsonNode vmeta = v.get("metrics");
                if (vmeta != null) {
                    double bp = vmeta.has("accumulated-backpressured-time") ? vmeta.get("accumulated-backpressured-time").asDouble() : 0;
                    double busy = vmeta.has("accumulated-busy-time") ? vmeta.get("accumulated-busy-time").asDouble() : 0;
                    double idle = vmeta.has("accumulated-idle-time") ? vmeta.get("accumulated-idle-time").asDouble() : 0;
                    double denom = bp + busy + idle;
                    if (denom > 0) {
                        double ratio = bp / denom;
                        if (ratio > maxBpRatio) maxBpRatio = ratio;
                    }
                }
            }
        }
        ObjectNode throughput = objectMapper.createObjectNode();
        throughput.put("numRecordsInPerSecond", totalIn);
        throughput.put("numRecordsOutPerSecond", totalOut);
        metrics.set("throughput", throughput);

        ObjectNode bpNode = objectMapper.createObjectNode();
        bpNode.put("ratio", Math.round(maxBpRatio * 10000) / 100.0);
        bpNode.put("level", maxBpRatio < 0.05 ? "ok" : maxBpRatio < 0.3 ? "low" : "high");
        metrics.set("backpressure", bpNode);

        JsonNode cpResp = getJson("/jobs/" + jid + "/checkpoints");
        ObjectNode cp = objectMapper.createObjectNode();
        if (cpResp != null && cpResp.has("counts")) {
            JsonNode counts = cpResp.get("counts");
            cp.put("completed", counts.has("completed") ? counts.get("completed").asLong() : 0);
            cp.put("failed", counts.has("failed") ? counts.get("failed").asLong() : 0);
            JsonNode latest = cpResp.has("latest") ? cpResp.get("latest") : null;
            if (latest != null && latest.has("completed") && !latest.get("completed").isNull()) {
                JsonNode lc = latest.get("completed");
                if (lc.has("latest_ack_timestamp")) cp.put("lastCompletedTs", lc.get("latest_ack_timestamp").asLong());
                if (lc.has("duration")) cp.put("endToEndDuration", lc.get("duration").asLong());
                if (lc.has("state_size")) cp.put("stateSize", lc.get("state_size").asLong());
            }
        } else {
            cp.put("error", true);
        }
        metrics.set("checkpoint", cp);
    }

    /**
     * 收集单个 vertex 的吞吐速率（行/s）：返回 {in, out}。
     * 先拉取该 vertex 的全部指标名，再分块查询含 numRecordsInPerSecond / numRecordsOutPerSecond 的算子作用域指标，
     * 按算子去数字前缀后聚合求和，取所有算子汇总值的最大值（融合链的通用指标为 0）。
     */
    private double[] collectVertexRates(String jid, String vid) {
        double[] result = new double[]{0, 0};
        JsonNode names = getJson("/jobs/" + jid + "/vertices/" + vid + "/metrics");
        if (names == null || !names.isArray() || names.isEmpty()) return result;
        List<String> inIds = new ArrayList<>();
        List<String> outIds = new ArrayList<>();
        for (JsonNode m : names) {
            String id = m.path("id").asText("");
            if (id == null || id.isBlank()) continue;
            if (id.endsWith("numRecordsInPerSecond")) inIds.add(id);
            else if (id.endsWith("numRecordsOutPerSecond")) outIds.add(id);
        }
        if (inIds.isEmpty() && outIds.isEmpty()) return result;
        List<String> all = new ArrayList<>(inIds);
        all.addAll(outIds);
        Map<String, Double> valueById = new HashMap<>();
        int chunk = 30;
        for (int i = 0; i < all.size(); i += chunk) {
            List<String> part = all.subList(i, Math.min(i + chunk, all.size()));
            JsonNode vals = getJson("/jobs/" + jid + "/vertices/" + vid + "/metrics?get=" + urlEncode(String.join(",", part)));
            if (vals != null && vals.isArray()) {
                for (JsonNode vv : vals) {
                    valueById.put(vv.path("id").asText(), vv.path("value").asDouble(0));
                }
            }
        }
        result[0] = maxRateByScope(inIds, valueById);
        result[1] = maxRateByScope(outIds, valueById);
        return result;
    }

    /** 指标 id 形如 "0.OperatorName.metric" 或 "metric"：去掉最前面的数字前缀后按算子聚合求和，返回最大值 */
    private double maxRateByScope(List<String> ids, Map<String, Double> valueById) {
        Map<String, Double> byScope = new HashMap<>();
        for (String id : ids) {
            double val = valueById.getOrDefault(id, 0.0);
            String key = id;
            int dot = id.indexOf('.');
            if (dot > 0 && Character.isDigit(id.charAt(0))) key = id.substring(dot + 1);
            byScope.merge(key, val, Double::sum);
        }
        double max = 0;
        for (double v : byScope.values()) {
            if (v > max) max = v;
        }
        return max;
    }

    private String urlEncode(String s) {
        try {
            return java.net.URLEncoder.encode(s, java.nio.charset.StandardCharsets.UTF_8);
        } catch (Exception e) {
            return s;
        }
    }

    private void collectHistoryMetrics(JobDefinition job, JsonNode detail, ObjectNode metrics) {
        long totalIn = 0, totalOut = 0;
        if (detail != null) {
            JsonNode vertices = detail.get("vertices");
            if (vertices != null && vertices.isArray()) {
                for (JsonNode v : vertices) {
                    JsonNode vmeta = v.get("metrics");
                    if (vmeta == null) continue;
                    totalIn += vmeta.has("read-records") ? vmeta.get("read-records").asLong() : 0;
                    totalOut += vmeta.has("write-records") ? vmeta.get("write-records").asLong() : 0;
                }
            }
        }
        // Flink 对快速完成的文件作业常不采集记录指标，或 JobManager 重启后归档丢失：用输出文件行数兜底
        if (totalIn == 0 && totalOut == 0) {
            Long rows = countOutputRows(job);
            if (rows != null) {
                totalIn = rows;
                totalOut = rows;
            }
        }
        // 仍拿不到统计时，用最近一次运行的实时快照兜底，保证作业结束后卡片数据不丢
        if (totalIn == 0 && totalOut == 0) {
            ObjectNode snapshot = lastLiveSnapshot.get(job.getId());
            if (snapshot != null) {
                JsonNode snapMetrics = snapshot.path("metrics");
                ObjectNode retained = objectMapper.createObjectNode();
                JsonNode snapThr = snapMetrics.path("throughput");
                if (snapThr.isObject()) {
                    ObjectNode thr = objectMapper.createObjectNode();
                    thr.put("numRecordsInPerSecond", snapThr.path("numRecordsInPerSecond").asDouble(0));
                    thr.put("numRecordsOutPerSecond", snapThr.path("numRecordsOutPerSecond").asDouble(0));
                    retained.set("throughput", thr);
                }
                JsonNode snapBp = snapMetrics.path("backpressure");
                if (snapBp.isObject()) {
                    ObjectNode bp = objectMapper.createObjectNode();
                    bp.put("ratio", snapBp.path("ratio").asDouble(0));
                    bp.put("level", snapBp.path("level").asText("unknown"));
                    retained.set("backpressure", bp);
                }
                if (retained.size() > 0) metrics.set("retained", retained);
            }
        }
        ObjectNode total = objectMapper.createObjectNode();
        total.put("numRecordsIn", totalIn);
        total.put("numRecordsOut", totalOut);
        metrics.set("total", total);

        // 历史作业不保留瞬时速率，用平均值：平均吞吐（行/s）= 输出行数 / 运行时长
        long duration = metrics.has("duration") ? metrics.get("duration").asLong() : -1;
        if (totalOut > 0 && duration > 0) {
            double avg = (double) totalOut / (duration / 1000.0);
            metrics.put("avgRowsPerSecond", Math.round(avg * 10) / 10.0);
        } else {
            metrics.put("avgRowsPerSecond", 0);
        }
    }

    private Long countOutputRows(JobDefinition job) {
        try {
            String dag = job.getDagJson();
            if (dag == null) return null;
            JsonNode root = objectMapper.readTree(dag);
            JsonNode nodes = root.path("nodes");
            if (!nodes.isArray()) return null;
            for (JsonNode n : nodes) {
                String type = n.path("type").asText("");
                String path = n.path("params").path("path").asText("");
                if (path == null || path.isBlank()) continue;
                String trimmed = path.trim();
                if ("csv_output".equals(type)) return countCsvRows(trimmed);
                if ("excel_output".equals(type)) return countExcelRows(trimmed);
            }
        } catch (Exception e) {
            log.debug("countOutputRows failed for job {}: {}", job.getId(), e.getMessage());
        }
        return null;
    }

    private Long countCsvRows(String path) {
        try {
            java.nio.file.Path out = java.nio.file.Paths.get(path);
            if (!java.nio.file.Files.exists(out)) return null;
            long count = 0;
            try (java.io.BufferedReader reader = java.nio.file.Files.newBufferedReader(out, java.nio.charset.StandardCharsets.UTF_8)) {
                String line;
                boolean first = true;
                while ((line = reader.readLine()) != null) {
                    if (first) { first = false; continue; }
                    if (!line.isBlank()) count++;
                }
            }
            return count;
        } catch (Exception e) {
            log.debug("countCsvRows failed for {}: {}", path, e.getMessage());
            return null;
        }
    }

    private Long countExcelRows(String path) {
        java.io.File file = new java.io.File(path);
        if (!file.isFile() || file.length() == 0) return null;
        try (org.apache.poi.openxml4j.opc.OPCPackage pkg = org.apache.poi.openxml4j.opc.OPCPackage.open(
                file, org.apache.poi.openxml4j.opc.PackageAccess.READ)) {
            org.apache.poi.xssf.eventusermodel.XSSFReader reader =
                    new org.apache.poi.xssf.eventusermodel.XSSFReader(pkg);
            java.util.Iterator<java.io.InputStream> sheets = reader.getSheetsData();
            if (!sheets.hasNext()) return 0L;
            try (java.io.InputStream sheet = sheets.next()) {
                javax.xml.stream.XMLInputFactory factory = javax.xml.stream.XMLInputFactory.newFactory();
                factory.setProperty(javax.xml.stream.XMLInputFactory.SUPPORT_DTD, false);
                factory.setProperty("javax.xml.stream.isSupportingExternalEntities", false);
                javax.xml.stream.XMLStreamReader xml = factory.createXMLStreamReader(sheet);
                long rows = 0;
                try {
                    while (xml.hasNext()) {
                        if (xml.next() == javax.xml.stream.XMLStreamConstants.START_ELEMENT
                                && "row".equals(xml.getLocalName())) {
                            rows++;
                        }
                    }
                } finally {
                    xml.close();
                }
                return Math.max(0, rows - 1);
            }
        } catch (Exception e) {
            log.debug("countExcelRows failed for {}: {}", path, e.getMessage());
            return null;
        }
    }

    private JsonNode getJson(String path) {
        try {
            HttpRequest req = HttpRequest.newBuilder()
                    .uri(URI.create("http://" + flinkHost + ":" + flinkPort + path))
                    .timeout(Duration.ofSeconds(4))
                    .GET().build();
            HttpResponse<String> resp = httpClient.send(req, HttpResponse.BodyHandlers.ofString());
            if (resp.statusCode() != 200) return null;
            return objectMapper.readTree(resp.body());
        } catch (Exception e) {
            log.debug("Flink REST {} failed: {}", path, e.getMessage());
            return null;
        }
    }

    /**
     * 运行中作业的吞吐/背压时间序列（内存环形缓冲，约 10 分钟 @5s）
     */
    /**
     * 吞吐/背压时间序列（内存环形缓冲，约 10 分钟 @5s）。
     * 运行中与最近结束的作业都返回：作业结束后保留趋势曲线，直到下一次重新运行才重置。
     */
    public List<ObjectNode> trends() {
        return trends(null);
    }

    /**
     * 吞吐/背压时间序列。cu 为 null 时不过滤归属；否则非管理员只返回自己作业的曲线。
     * 注意：残留缓冲的清理必须基于全量 jobMap，不能按归属过滤，否则会误删他人曲线。
     */
    public List<ObjectNode> trends(CurrentUser cu) {
        List<ObjectNode> result = new ArrayList<>();
        if (trendBuffer.isEmpty()) return result;
        List<JobDefinition> jobs = jobRepo.findAllById(trendBuffer.keySet());
        Map<Long, JobDefinition> jobMap = new HashMap<>();
        for (JobDefinition j : jobs) {
            if (j.getStatus() == null) continue;
            switch (j.getStatus()) {
                case SUBMITTED:
                case RUNNING:
                case COMPLETED:
                case FAILED:
                case CANCELLED:
                    jobMap.put(j.getId(), j);
                    break;
                default:
                    break;
            }
        }
        // 清理已删除或非监控状态作业的残留缓冲
        trendBuffer.keySet().removeIf(id -> !jobMap.containsKey(id));
        trendRunKeys.keySet().removeIf(id -> !jobMap.containsKey(id));
        lastLiveSnapshot.keySet().removeIf(id -> !jobMap.containsKey(id));
        for (Map.Entry<Long, Deque<ObjectNode>> e : trendBuffer.entrySet()) {
            JobDefinition job = jobMap.get(e.getKey());
            if (job == null) continue;
            if (!canSee(job, cu)) continue;
            ObjectNode item = objectMapper.createObjectNode();
            item.put("id", job.getId());
            item.put("name", job.getName());
            item.put("status", job.getStatus() != null ? job.getStatus().name() : "UNKNOWN");
            ArrayNode points = objectMapper.createArrayNode();
            synchronized (e.getValue()) {
                for (ObjectNode point : e.getValue()) points.add(point);
            }
            item.set("points", points);
            result.add(item);
        }
        // 运行中作业排前面，已结束按最近时间倒序
        result.sort((a, b) -> {
            boolean aActive = isActiveStatus(a.path("status").asText());
            boolean bActive = isActiveStatus(b.path("status").asText());
            if (aActive != bActive) return aActive ? -1 : 1;
            long lastA = lastPointTime((ArrayNode) a.get("points"));
            long lastB = lastPointTime((ArrayNode) b.get("points"));
            return Long.compare(lastB, lastA);
        });
        return result;
    }

    private boolean isActiveStatus(String status) {
        return "RUNNING".equals(status) || "SUBMITTED".equals(status);
    }

    /** 手动移除某作业的趋势曲线：清理内存缓冲并删除落库趋势点 */
    public boolean removeTrend(Long jobId) {
        boolean removed = trendBuffer.remove(jobId) != null
                | trendRunKeys.remove(jobId) != null
                | lastLiveSnapshot.remove(jobId) != null;
        try {
            trendRepo.deleteByJobId(jobId);
        } catch (Exception e) {
            log.warn("Failed to drop trend points for job {}: {}", jobId, e.getMessage());
        }
        return removed;
    }

    /** 校验过的趋势删除：先确认作业存在且当前用户有权限，再清理。 */
    public boolean removeTrendForUser(Long jobId, CurrentUser cu) {
        if (cu == null) throw new ResponseStatusException(HttpStatus.UNAUTHORIZED, "未登录");
        JobDefinition job = jobRepo.findById(jobId)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "作业不存在: " + jobId));
        if (!canSee(job, cu)) {
            throw new ResponseStatusException(HttpStatus.FORBIDDEN, "无权操作该作业的趋势数据");
        }
        return removeTrend(jobId);
    }

    /**
     * 归属判定：ADMIN 全量；cu 为 null 表示系统内部聚合调用（overview()/trends() 无参重载，如 AI 诊断），不做过滤；
     * 传入真实用户时统一走 {@link com.datastream.mvp.security.JobAccess#canAccess}——
     * 即非管理员只能看自己创建的作业，**ownerId 为空的历史作业同样不可见**（此前这里放行，与 JobService 规则不一致）。
     */
    private boolean canSee(JobDefinition job, CurrentUser cu) {
        if (cu == null) return true;
        return com.datastream.mvp.security.JobAccess.canAccess(job, cu);
    }

    private long lastPointTime(ArrayNode points) {
        return points != null && points.size() > 0 && points.get(points.size() - 1).has("t")
                ? points.get(points.size() - 1).get("t").asLong() : 0L;
    }

    void recordTrendPoint(JobDefinition job, ObjectNode item) {
        JsonNode metrics = item.get("metrics");
        if (metrics == null) return;
        JsonNode thr = metrics.get("throughput");
        if (thr == null) return;
        Long jobId = job.getId();
        String runKey = job.getSubmittedAt() == null ? "" : job.getSubmittedAt().toString();
        Deque<ObjectNode> q = trendBuffer.computeIfAbsent(jobId, k -> new ArrayDeque<>());
        ObjectNode point = objectMapper.createObjectNode();
        point.put("t", System.currentTimeMillis());
        double inRate = thr.has("numRecordsInPerSecond") ? thr.get("numRecordsInPerSecond").asDouble() : 0;
        double outRate = thr.has("numRecordsOutPerSecond") ? thr.get("numRecordsOutPerSecond").asDouble() : 0;
        point.put("in", inRate);
        point.put("out", outRate);
        JsonNode bp = metrics.get("backpressure");
        double bpRatio = bp != null && bp.has("ratio") ? bp.get("ratio").asDouble() : 0;
        point.put("bp", bpRatio);
        String status = job.getStatus() == null ? "RUNNING" : job.getStatus().name();
        point.put("status", status);
        synchronized (q) {
            // 新一次运行（submittedAt 变化）时重置，只保留当前运行的曲线
            String prevKey = trendRunKeys.get(jobId);
            if (prevKey == null || !prevKey.equals(runKey)) {
                q.clear();
                trendRunKeys.put(jobId, runKey);
                // 清掉该作业上一落盘的旧趋势点，只保留本次运行
                deletePersisted(jobId);
            }
            q.addLast(point);
            while (q.size() > MAX_TREND_POINTS) q.removeFirst();
        }
        persistPoint(jobId, point.path("t").asLong(), inRate, outRate, bpRatio, runKey, status);
    }

    private void recordCompletedTrend(JobDefinition job, double average) {
        Long jobId = job.getId();
        String runKey = job.getSubmittedAt() == null ? "" : job.getSubmittedAt().toString();
        Deque<ObjectNode> points = trendBuffer.computeIfAbsent(jobId, ignored -> new ArrayDeque<>());
        long start = job.getSubmittedAt() == null
                ? System.currentTimeMillis() - 2000
                : job.getSubmittedAt().atZone(java.time.ZoneId.systemDefault()).toInstant().toEpochMilli();
        long end = job.getCompletedAt() == null
                ? System.currentTimeMillis()
                : job.getCompletedAt().atZone(java.time.ZoneId.systemDefault()).toInstant().toEpochMilli();
        if (end <= start + 1) end = start + 2;

        synchronized (points) {
            if (!runKey.equals(trendRunKeys.get(jobId))) {
                points.clear();
                trendRunKeys.put(jobId, runKey);
                deletePersisted(jobId);
            }
            boolean hasVisibleSamples = points.size() >= 3
                    && points.stream().anyMatch(point -> point.path("out").asDouble(0) > 0);
            if (!hasVisibleSamples) {
                points.clear();
                trendRunKeys.put(jobId, runKey);
                deletePersisted(jobId);
                addTrendPoint(points, jobId, start, 0, runKey, "RUNNING");
                addTrendPoint(points, jobId, start + (end - start) / 2, average, runKey, "RUNNING");
            }
            addTrendPoint(points, jobId, end, 0, runKey, "COMPLETED");
        }
    }

    private void addTrendPoint(Deque<ObjectNode> points, Long jobId, long time, double rate,
                               String runKey, String status) {
        ObjectNode point = objectMapper.createObjectNode();
        point.put("t", time);
        point.put("in", rate);
        point.put("out", rate);
        point.put("bp", 0);
        point.put("status", status);
        points.addLast(point);
        while (points.size() > MAX_TREND_POINTS) points.removeFirst();
        persistPoint(jobId, time, rate, rate, 0, runKey, status);
    }

    /** 新一次运行开始时删除数据库旧趋势点（失败仅记录，不影响热路径） */
    private void deletePersisted(Long jobId) {
        try {
            trendRepo.deleteByJobId(jobId);
        } catch (Exception e) {
            log.warn("Failed to drop stale trend points for job {}: {}", jobId, e.getMessage());
        }
    }

    /** 单条持久化趋势点；写库失败不阻断内存热路径 */
    private void persistPoint(Long jobId, long t, double inRate, double outRate, double bpRatio,
                              String runKey, String status) {
        try {
            TrendPoint tp = new TrendPoint(null, jobId, t, inRate, outRate, bpRatio, runKey, status);
            trendRepo.save(tp);
        } catch (Exception e) {
            log.warn("Failed to persist trend point for job {}: {}", jobId, e.getMessage());
        }
    }

    /** 后端启动时从库恢复最近一次运行的趋势曲线，供前端继续展示 */
    @PostConstruct
    void restoreTrends() {
        try {
            List<JobDefinition> jobs = jobRepo.findAllByOrderByUpdatedAtDesc();
            for (JobDefinition job : jobs) {
                if (job.getStatus() == null) continue;
                JobDefinition.JobStatus st = job.getStatus();
                if (st != JobDefinition.JobStatus.SUBMITTED
                        && st != JobDefinition.JobStatus.RUNNING
                        && st != JobDefinition.JobStatus.COMPLETED
                        && st != JobDefinition.JobStatus.FAILED
                        && st != JobDefinition.JobStatus.CANCELLED) continue;
                restoreTrendForJob(job);
            }
        } catch (Exception e) {
            log.warn("Failed to restore trend buffer from DB: {}", e.getMessage());
        }
    }

    void restoreTrendForJob(JobDefinition job) {
        Long jobId = job.getId();
        String runKey = job.getSubmittedAt() == null ? "" : job.getSubmittedAt().toString();
        try {
            List<TrendPoint> rows = trendRepo.findByJobIdAndRunKeyOrderByTAsc(jobId, runKey);
            if (rows.isEmpty()) return;
            Deque<ObjectNode> q = new ArrayDeque<>();
            for (TrendPoint row : rows) {
                ObjectNode point = objectMapper.createObjectNode();
                point.put("t", row.getT());
                point.put("in", row.getIn());
                point.put("out", row.getOut());
                point.put("bp", row.getBp());
                if (row.getStatus() != null) point.put("status", row.getStatus());
                q.addLast(point);
                if (q.size() >= MAX_TREND_POINTS) break;
            }
            trendBuffer.put(jobId, q);
            trendRunKeys.put(jobId, runKey);
            log.info("Restored {} trend point(s) for job {}", q.size(), jobId);
        } catch (Exception e) {
            log.warn("Failed to restore trend for job {}: {}", jobId, e.getMessage());
        }
    }
}
