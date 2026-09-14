package com.datastream.mvp.service;

import com.datastream.mvp.model.ControlRegistry;
import com.datastream.mvp.model.JobDefinition;
import com.datastream.mvp.security.CurrentUser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Service
@RequiredArgsConstructor
public class DagPreflightService {
    /** 这些字段属于「连接级」，可由 dataSourceId 引用的数据源提供。 */
    private static final java.util.Set<String> CONNECTION_LEVEL_FIELDS =
            java.util.Set.of("url", "username", "password", "bootstrapServers", "host", "port");
    private final JobService jobService;
    private final ControlRegistryService controlService;
    private final ObjectMapper objectMapper;
    private final DataSourceNodeResolver dataSourceNodeResolver;

    public Map<String, Object> validate(Long id, CurrentUser user) {
        JobDefinition job = jobService.findByIdForUser(id, user);
        List<String> errors = new ArrayList<>();
        List<String> warnings = new ArrayList<>();
        JsonNode root;
        try {
            root = objectMapper.readTree(job.getDagJson());
        } catch (Exception e) {
            errors.add("DAG JSON 格式非法");
            return result(errors, warnings);
        }
        if (root == null || !root.isObject()) {
            errors.add("DAG 必须是 JSON 对象");
            return result(errors, warnings);
        }
        int parallelism = root.has("parallelism") ? root.path("parallelism").asInt(job.getParallelism() == null ? 1 : job.getParallelism())
                : (job.getParallelism() == null ? 1 : job.getParallelism());
        if (parallelism < 1 || parallelism > 128) errors.add("并行度必须在 1..128 之间");
        else if (parallelism > 16) warnings.add("并行度大于 16，请确认集群 Slot 与资源容量足够");
        JsonNode nodes = root.path("nodes");
        JsonNode edges = root.path("edges");
        if (!nodes.isArray() || nodes.isEmpty()) errors.add("nodes 必须是非空数组");
        if (!edges.isArray()) errors.add("edges 必须是数组");
        if (!nodes.isArray() || !edges.isArray()) return result(errors, warnings);

        Map<String, ControlRegistry> controls = new HashMap<>();
        for (ControlRegistry control : controlService.findAll()) controls.put(control.getType(), control);
        Map<String, JsonNode> byId = new LinkedHashMap<>();
        Set<String> inputs = new HashSet<>(), outputs = new HashSet<>();
        for (JsonNode node : nodes) {
            String nodeId = node.path("id").asText("").trim();
            String type = node.path("type").asText("").trim();
            String label = node.path("label").asText("").trim();
            if (nodeId.isEmpty()) errors.add("节点 id 不能为空");
            else if (byId.putIfAbsent(nodeId, node) != null) errors.add("节点 id 重复: " + nodeId);
            if (label.isEmpty()) warnings.add("节点 " + (nodeId.isEmpty() ? "(未知)" : nodeId) + " 未设置显示名称");
            ControlRegistry control = controls.get(type);
            if (control == null || !Boolean.TRUE.equals(control.getEnabled())) errors.add("控件未注册或未启用: " + type);
            else {
                if ("input".equals(control.getCategory())) inputs.add(nodeId);
                if ("output".equals(control.getCategory())) outputs.add(nodeId);
                validateParams(nodeId, node.path("params"), control.getParamSchema(), errors);
            }
        }
        if (inputs.isEmpty()) errors.add("DAG 至少需要一个输入控件");
        if (outputs.isEmpty()) errors.add("DAG 至少需要一个输出控件");

        // 引用数据源的节点：校验引用是否存在、启用且类型匹配（纯读校验，不建立连接）
        dataSourceNodeResolver.validate(
                objectMapper.convertValue(root, com.datastream.mvp.dag.DagDefinition.class), user, errors, null);

        Map<String, Set<String>> graph = new HashMap<>();
        Map<String, Integer> degree = new HashMap<>();
        byId.keySet().forEach(k -> { graph.put(k, new HashSet<>()); degree.put(k, 0); });
        Set<String> edgeKeys = new HashSet<>();
        for (JsonNode edge : edges) {
            String source = edge.path("source").asText("").trim();
            String target = edge.path("target").asText("").trim();
            if (!byId.containsKey(source) || !byId.containsKey(target)) { errors.add("边引用不存在的节点: " + source + " -> " + target); continue; }
            if (source.equals(target)) errors.add("不允许自环: " + source);
            String key = source + "\u0000" + target;
            if (!edgeKeys.add(key)) errors.add("重复边: " + source + " -> " + target);
            else if (!source.equals(target)) {
                graph.get(source).add(target);
                degree.put(source, degree.get(source) + 1);
                degree.put(target, degree.get(target) + 1);
            }
        }
        degree.forEach((nodeId, value) -> { if (value == 0) errors.add("孤立节点: " + nodeId); });
        if (hasCycle(graph)) errors.add("DAG 中存在环");
        Set<String> reachable = reachableFrom(inputs, graph);
        for (String input : inputs) if (!canReachAny(input, outputs, graph)) errors.add("输入节点无法到达输出: " + input);
        for (String output : outputs) if (!reachable.contains(output)) errors.add("输出节点不可从输入到达: " + output);
        return result(errors, warnings);
    }

    private void validateParams(String nodeId, JsonNode params, String schemaText, List<String> errors) {
        if (schemaText == null || schemaText.isBlank()) return;
        // 引用已保存数据源时，连接级参数由后端注入，不再要求节点内联填写
        boolean usesDataSource = params.isObject() && params.hasNonNull("dataSourceId");
        try {
            JsonNode schema = objectMapper.readTree(schemaText);
            JsonNode values = params.isObject() ? params : objectMapper.createObjectNode();
            for (JsonNode required : schema.path("required")) {
                String key = required.asText();
                if (usesDataSource && CONNECTION_LEVEL_FIELDS.contains(key)) continue;
                JsonNode value = values.get(key);
                if (value == null || value.isNull() || (value.isTextual() && value.asText().isBlank())) errors.add("节点 " + nodeId + " 缺少必填参数: " + key);
            }
            schema.path("properties").fields().forEachRemaining(entry -> {
                JsonNode value = values.get(entry.getKey());
                if (value == null || value.isNull()) return;
                // 表单未填写的可选参数回传空串（如被隐藏的数值项），按「未提供」处理而非类型错误
                if (value.isTextual() && value.asText().isBlank()) return;
                String type = entry.getValue().path("type").asText();
                boolean typeOk = switch (type) {
                    case "string" -> value.isTextual(); case "number" -> isNumericLike(value);
                    case "integer" -> isIntegralLike(value); case "boolean" -> isBooleanLike(value);
                    case "array" -> value.isArray(); case "object" -> value.isObject(); default -> true;
                };
                if (!typeOk) errors.add("节点 " + nodeId + " 参数类型错误: " + entry.getKey());
                JsonNode enumValues = entry.getValue().path("enum");
                if (enumValues.isArray() && !contains(enumValues, value)) errors.add("节点 " + nodeId + " 参数不在枚举范围: " + entry.getKey());
            });
        } catch (Exception e) {
            errors.add("控件参数 Schema 非法: " + nodeId);
        }
    }

    /**
     * 布尔值宽松判定：兼容历史数据与旧前端把开关存成字符串的情况（"true"/"false"）。
     * 后端翻译层统一按 toString() 取值，字符串与布尔等价，故此处放行。
     */
    private boolean isBooleanLike(JsonNode value) {
        if (value.isBoolean()) return true;
        if (!value.isTextual()) return false;
        String text = value.asText().trim();
        return "true".equalsIgnoreCase(text) || "false".equalsIgnoreCase(text);
    }

    private boolean isNumericLike(JsonNode value) {
        if (value.isNumber()) return true;
        return value.isTextual() && isParsable(value.asText(), false);
    }

    private boolean isIntegralLike(JsonNode value) {
        if (value.isIntegralNumber()) return true;
        return value.isTextual() && isParsable(value.asText(), true);
    }

    private boolean isParsable(String text, boolean integralOnly) {
        if (text == null || text.isBlank()) return false;
        try {
            if (integralOnly) Long.parseLong(text.trim());
            else new java.math.BigDecimal(text.trim());
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    private boolean contains(JsonNode array, JsonNode value) { for (JsonNode candidate : array) if (candidate.equals(value)) return true; return false; }
    private boolean hasCycle(Map<String, Set<String>> graph) {
        Map<String, Integer> indegree = new HashMap<>(); graph.keySet().forEach(k -> indegree.put(k, 0));
        graph.values().forEach(vs -> vs.forEach(v -> indegree.put(v, indegree.get(v) + 1)));
        ArrayDeque<String> queue = new ArrayDeque<>(); indegree.forEach((k, v) -> { if (v == 0) queue.add(k); });
        int visited = 0; while (!queue.isEmpty()) { String n = queue.remove(); visited++; for (String next : graph.get(n)) if (indegree.compute(next, (k,v) -> v - 1) == 0) queue.add(next); }
        return visited != graph.size();
    }
    private Set<String> reachableFrom(Set<String> starts, Map<String, Set<String>> graph) {
        Set<String> seen = new HashSet<>(starts); ArrayDeque<String> queue = new ArrayDeque<>(starts);
        while (!queue.isEmpty()) for (String next : graph.getOrDefault(queue.remove(), Set.of())) if (seen.add(next)) queue.add(next);
        return seen;
    }
    private boolean canReachAny(String start, Set<String> targets, Map<String, Set<String>> graph) { Set<String> seen = reachableFrom(Set.of(start), graph); return targets.stream().anyMatch(seen::contains); }
    private Map<String, Object> result(List<String> errors, List<String> warnings) { Map<String,Object> r = new LinkedHashMap<>(); r.put("valid", errors.isEmpty()); r.put("errors", errors); r.put("warnings", warnings); return r; }
}
