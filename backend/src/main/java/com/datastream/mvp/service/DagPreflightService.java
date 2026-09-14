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
    private final JobService jobService;
    private final ControlRegistryService controlService;
    private final ObjectMapper objectMapper;

    public Map<String, Object> validate(Long id, CurrentUser user) {
        JobDefinition job = jobService.findByIdForUser(id, user);
        List<String> errors = new ArrayList<>();
        JsonNode root;
        try {
            root = objectMapper.readTree(job.getDagJson());
        } catch (Exception e) {
            errors.add("DAG JSON 格式非法");
            return result(errors);
        }
        if (root == null || !root.isObject()) {
            errors.add("DAG 必须是 JSON 对象");
            return result(errors);
        }
        int parallelism = root.has("parallelism") ? root.path("parallelism").asInt(job.getParallelism() == null ? 1 : job.getParallelism())
                : (job.getParallelism() == null ? 1 : job.getParallelism());
        if (parallelism < 1 || parallelism > 128) errors.add("并行度必须在 1..128 之间");
        JsonNode nodes = root.path("nodes");
        JsonNode edges = root.path("edges");
        if (!nodes.isArray() || nodes.isEmpty()) errors.add("nodes 必须是非空数组");
        if (!edges.isArray()) errors.add("edges 必须是数组");
        if (!nodes.isArray() || !edges.isArray()) return result(errors);

        Map<String, ControlRegistry> controls = new HashMap<>();
        for (ControlRegistry control : controlService.findAll()) controls.put(control.getType(), control);
        Map<String, JsonNode> byId = new LinkedHashMap<>();
        Set<String> inputs = new HashSet<>(), outputs = new HashSet<>();
        for (JsonNode node : nodes) {
            String nodeId = node.path("id").asText("").trim();
            String type = node.path("type").asText("").trim();
            if (nodeId.isEmpty()) errors.add("节点 id 不能为空");
            else if (byId.putIfAbsent(nodeId, node) != null) errors.add("节点 id 重复: " + nodeId);
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
        return result(errors);
    }

    private void validateParams(String nodeId, JsonNode params, String schemaText, List<String> errors) {
        if (schemaText == null || schemaText.isBlank()) return;
        try {
            JsonNode schema = objectMapper.readTree(schemaText);
            JsonNode values = params.isObject() ? params : objectMapper.createObjectNode();
            for (JsonNode required : schema.path("required")) {
                String key = required.asText();
                JsonNode value = values.get(key);
                if (value == null || value.isNull() || (value.isTextual() && value.asText().isBlank())) errors.add("节点 " + nodeId + " 缺少必填参数: " + key);
            }
            schema.path("properties").fields().forEachRemaining(entry -> {
                JsonNode value = values.get(entry.getKey());
                if (value == null || value.isNull()) return;
                String type = entry.getValue().path("type").asText();
                boolean typeOk = switch (type) {
                    case "string" -> value.isTextual(); case "number" -> value.isNumber();
                    case "integer" -> value.isIntegralNumber(); case "boolean" -> value.isBoolean();
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
    private Map<String, Object> result(List<String> errors) { Map<String,Object> r = new LinkedHashMap<>(); r.put("valid", errors.isEmpty()); r.put("errors", errors); return r; }
}
