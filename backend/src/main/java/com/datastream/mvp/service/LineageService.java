package com.datastream.mvp.service;

import com.datastream.mvp.model.JobDefinition;
import com.datastream.mvp.model.JobDependency;
import com.datastream.mvp.repository.JobDefinitionRepository;
import com.datastream.mvp.repository.JobDependencyRepository;
import com.datastream.mvp.security.CurrentUser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;

import java.util.*;

/**
 * 数据血缘：解析作业 DAG 的输入/输出资产（文件 / 库表 / Kafka topic），
 * 并组合跨作业依赖边形成全工作流血缘图。
 */
@Service
@RequiredArgsConstructor
public class LineageService {

    private final JobDefinitionRepository jobRepo;
    private final JobDependencyRepository depRepo;
    private final ObjectMapper objectMapper;

    /** 单个作业的血缘：输入资产 + 输出资产 */
    public Map<String, Object> getJobLineage(Long jobId, CurrentUser cu) {
        JobDefinition job = jobRepo.findById(jobId)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "作业不存在: " + jobId));
        // 统一走 JobAccess：ownerId 为空的作业仅管理员可访问（见 security/JobAccess）
        com.datastream.mvp.security.JobAccess.assertCanAccess(job, cu);
        List<Map<String, Object>> assets = extractAssets(job.getDagJson());
        Map<String, Object> result = new LinkedHashMap<>();
        Map<String, Object> jobInfo = new LinkedHashMap<>();
        jobInfo.put("id", job.getId());
        jobInfo.put("name", job.getName());
        jobInfo.put("status", job.getStatus().name());
        result.put("job", jobInfo);
        result.put("assets", assets);
        return result;
    }

    /** 全工作流血缘：作业节点 + 资产节点 + 作业内边 + 跨作业依赖边 */
    public Map<String, Object> getWorkflowLineage(CurrentUser cu) {
        List<JobDefinition> jobs = cu == null ? List.of()
                : (cu.isAdmin() ? jobRepo.findAllByOrderByUpdatedAtDesc()
                                : jobRepo.findByOwnerIdOrderByUpdatedAtDesc(cu.id()));
        Set<Long> visible = new HashSet<>();
        List<Map<String, Object>> jobNodes = new ArrayList<>();
        List<Map<String, Object>> assetNodes = new ArrayList<>();
        Map<Long, List<Map<String, Object>>> jobAssets = new HashMap<>();
        for (JobDefinition j : jobs) {
            visible.add(j.getId());
            Map<String, Object> jm = new LinkedHashMap<>();
            jm.put("id", "job:" + j.getId());
            jm.put("jobId", j.getId());
            jm.put("name", j.getName());
            jm.put("status", j.getStatus().name());
            jm.put("category", "job");
            jobNodes.add(jm);
            List<Map<String, Object>> assets = extractAssets(j.getDagJson());
            jobAssets.put(j.getId(), assets);
            for (Map<String, Object> a : assets) {
                Map<String, Object> am = new LinkedHashMap<>(a);
                am.put("id", "asset:" + j.getId() + ":" + a.get("nodeId"));
                am.put("jobId", j.getId());
                am.put("category", "asset");
                assetNodes.add(am);
            }
        }
        List<Map<String, Object>> edges = new ArrayList<>();
        for (Map.Entry<Long, List<Map<String, Object>>> e : jobAssets.entrySet()) {
            Long jobId = e.getKey();
            for (Map<String, Object> a : e.getValue()) {
                String assetId = "asset:" + jobId + ":" + a.get("nodeId");
                Map<String, Object> edge = new LinkedHashMap<>();
                if ("input".equals(a.get("direction"))) {
                    edge.put("source", assetId);
                    edge.put("target", "job:" + jobId);
                } else {
                    edge.put("source", "job:" + jobId);
                    edge.put("target", assetId);
                }
                edge.put("type", "intra");
                edges.add(edge);
            }
        }
        for (JobDependency dep : depRepo.findAllByOrderByCreatedAtAsc()) {
            if (visible.contains(dep.getUpstreamJobId()) && visible.contains(dep.getDownstreamJobId())) {
                Map<String, Object> edge = new LinkedHashMap<>();
                edge.put("source", "job:" + dep.getUpstreamJobId());
                edge.put("target", "job:" + dep.getDownstreamJobId());
                edge.put("type", "depends");
                edges.add(edge);
            }
        }
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("jobs", jobNodes);
        result.put("assets", assetNodes);
        result.put("edges", edges);
        return result;
    }

    private List<Map<String, Object>> extractAssets(String dagJson) {
        List<Map<String, Object>> assets = new ArrayList<>();
        if (dagJson == null || dagJson.isBlank()) return assets;
        try {
            JsonNode root = objectMapper.readTree(dagJson);
            JsonNode nodes = root.path("nodes");
            if (nodes == null || !nodes.isArray()) return assets;
            for (JsonNode n : nodes) {
                String type = n.path("type").asText("");
                String nodeId = n.path("id").asText("");
                JsonNode params = n.path("params");
                String direction = null;
                String asset = null;
                String kind = null;
                switch (type) {
                    case "csv_input": case "excel_input": case "json_input": case "xml_input":
                        direction = "input"; kind = "file"; asset = params.path("path").asText(""); break;
                    case "csv_output": case "excel_output": case "json_output": case "xml_output":
                        direction = "output"; kind = "file"; asset = params.path("path").asText(""); break;
                    case "mysql_input":
                        direction = "input"; kind = "mysql"; asset = mysqlAsset(params); break;
                    case "mysql_output":
                        direction = "output"; kind = "mysql"; asset = mysqlAsset(params); break;
                    case "kafka_input":
                        direction = "input"; kind = "kafka"; asset = "topic:" + params.path("topic").asText(""); break;
                    case "kafka_output":
                        direction = "output"; kind = "kafka"; asset = "topic:" + params.path("topic").asText(""); break;
                    case "datagen_input":
                        direction = "input"; kind = "datagen"; asset = "模拟数据（Datagen）"; break;
                    default:
                        continue;
                }
                if (asset == null || asset.isBlank()) asset = "（未配置）";
                Map<String, Object> a = new LinkedHashMap<>();
                a.put("nodeId", nodeId);
                a.put("type", type);
                a.put("asset", asset);
                a.put("direction", direction);
                a.put("kind", kind);
                assets.add(a);
            }
        } catch (Exception e) {
            // 解析失败不阻断
        }
        return assets;
    }

    private String mysqlAsset(JsonNode params) {
        String table = params.path("table").asText("");
        String url = params.path("url").asText("");
        String db = "";
        if (url != null && url.contains("/")) {
            String after = url.substring(url.indexOf("/") + 1);
            int q = after.indexOf("?");
            if (q >= 0) after = after.substring(0, q);
            db = after;
        }
        return (db.isEmpty() ? "" : db + ".") + (table.isEmpty() ? "（表未配置）" : table);
    }
}
