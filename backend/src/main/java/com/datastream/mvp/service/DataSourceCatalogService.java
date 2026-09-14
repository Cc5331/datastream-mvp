package com.datastream.mvp.service;

import com.datastream.mvp.model.ControlRegistry;
import com.datastream.mvp.model.JobDefinition;
import com.datastream.mvp.security.CurrentUser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Service
@RequiredArgsConstructor
public class DataSourceCatalogService {
    private final ControlRegistryService controlService;
    private final JobService jobService;
    private final ObjectMapper objectMapper;

    public Map<String, Object> catalog(CurrentUser user) {
        List<Map<String, Object>> connectors = controlService.findAll().stream()
                .filter(c -> "input".equals(c.getCategory()) || "output".equals(c.getCategory()))
                .map(this::connectorSummary).toList();
        List<Map<String, Object>> assets = new ArrayList<>();
        for (JobDefinition job : jobService.findAllForUser(user)) parseAssets(job, assets);
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("connectors", connectors);
        result.put("assets", assets);
        return result;
    }

    private Map<String, Object> connectorSummary(ControlRegistry control) {
        Map<String, Object> item = new LinkedHashMap<>();
        item.put("type", control.getType());
        item.put("name", control.getName());
        item.put("category", control.getCategory());
        item.put("description", control.getDescription());
        item.put("version", control.getVersion());
        item.put("enabled", Boolean.TRUE.equals(control.getEnabled()));
        return item;
    }

    private void parseAssets(JobDefinition job, List<Map<String, Object>> assets) {
        try {
            JsonNode nodes = objectMapper.readTree(job.getDagJson()).path("nodes");
            if (!nodes.isArray()) return;
            for (JsonNode node : nodes) {
                String type = node.path("type").asText();
                JsonNode params = node.path("params");
                Map<String, Object> locator = locator(type, params);
                if (locator.isEmpty()) continue;
                Map<String, Object> asset = new LinkedHashMap<>();
                asset.put("jobId", job.getId());
                asset.put("jobName", job.getName());
                asset.put("nodeId", node.path("id").asText());
                asset.put("type", type);
                asset.put("direction", type.endsWith("_input") ? "input" : "output");
                asset.putAll(locator);
                assets.add(asset);
            }
        } catch (Exception ignored) {
            // 非法 DAG 不应导致目录接口整体失败，也不回传解析异常细节。
        }
    }

    private Map<String, Object> locator(String type, JsonNode params) {
        Map<String, Object> result = new LinkedHashMap<>();
        if (type.matches("(csv|excel|json|xml|parquet)_((input)|(output))") || type.matches("hdfs_((input)|(output))")) {
            putText(result, "path", params.path("path"));
        } else if (type.matches("(mysql|pg|postgresql|oracle)_((input)|(output))")) {
            putUrl(result, params.path("url"));
            putText(result, "table", params.path("table"));
        } else if (type.matches("kafka_((input)|(output))")) {
            JsonNode servers = params.has("bootstrapServers") ? params.path("bootstrapServers") : params.path("bootstrap.servers");
            putText(result, "bootstrapServers", servers);
            putText(result, "topic", params.path("topic"));
        }
        return result;
    }

    private void putText(Map<String, Object> target, String key, JsonNode value) {
        if (value.isTextual() && !value.asText().isBlank()) target.put(key, value.asText());
    }

    private void putUrl(Map<String, Object> target, JsonNode value) {
        if (!value.isTextual() || value.asText().isBlank()) return;
        String sanitized = value.asText()
                .replaceAll("(?i)([?;&](?:password|token|apiKey)=)[^;&]*", "$1***")
                .replaceAll("(?i)(jdbc:[^:]+://)[^/@:]+:[^/@]+@", "$1***:***@");
        target.put("url", sanitized);
    }
}
