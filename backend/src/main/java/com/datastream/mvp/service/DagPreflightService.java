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
import java.util.LinkedHashSet;
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
    private final KafkaMonitorService kafkaMonitorService;

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
                if ("kafka_input".equals(type) || "kafka_output".equals(type)) {
                    validateKafkaTopic(node, errors, warnings);
                }
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

        validateFieldReferences(nodes, edges, controls, byId, errors);
        return result(errors, warnings);
    }

    /**
     * 字段引用校验：DAG 里引用的字段必须能在「该节点可达的输入节点」声明的 schema 中找到。
     *
     * 为什么需要：翻译层完全信任节点 params 里的字段名（例如 redis_lookup 的 keyField 直接渲染成
     * redis_lookup(..., CAST(`id` AS STRING))），而输入节点声明什么字段就只有一个表什么列。
     * 字段写错时平台会一路放行到 Flink，在 CREATE VIEW 阶段被 Calcite 拦下，用户只看到
     * "Flink SQL 第 N 条语句执行失败: Failed to fetchResults"（实测 Flink 1.18.1 的 Gateway
     * REST 在语句校验期失败时只回 {"status":"ERROR"}，result 接口直接 HTTP 500 空 body，
     * 真实报文只落在 Gateway 日志里），定位成本极高。这里提前给出可照做的错误。
     *
     * 判定口径：只以「该节点上游可达的输入节点」声明的字段为准 —— 字段若由上游转换节点产生
     * （如字段拼接的新字段），在输入 schema 里同样找不到，因此必须排除可达输入中不存在的情况，
     * 避免把合法 DAG 误判为错误。
     */
    private void validateFieldReferences(JsonNode nodes, JsonNode edges, Map<String, ControlRegistry> controls,
                                         Map<String, JsonNode> byId, List<String> errors) {
        // 输入节点 -> 它声明的字段（只有 input 类控件有确定的来源 schema）
        Map<String, Set<String>> inputFields = new LinkedHashMap<>();
        Map<String, String> nodeType = new LinkedHashMap<>();
        Map<String, String> nodeLabel = new LinkedHashMap<>();
        for (JsonNode n : nodes) {
            String id = n.path("id").asText("");
            String type = n.path("type").asText("");
            if (id.isEmpty()) continue;
            nodeType.put(id, type);
            String label = n.path("label").asText("");
            nodeLabel.put(id, label.isEmpty() ? id : label);
            ControlRegistry c = controls.get(type);
            if (c != null && "input".equals(c.getCategory())) {
                Set<String> f = declaredFields(n.path("params"));
                if (!f.isEmpty()) inputFields.put(id, f);
            }
        }
        if (inputFields.isEmpty()) return;   // 无可校验的声明式 schema（如库表输入）时不妄下结论

        // 反向邻接：target -> 直接上游
        Map<String, Set<String>> parents = new LinkedHashMap<>();
        for (JsonNode e : edges) {
            String s = e.path("source").asText(""), t = e.path("target").asText("");
            if (!s.isEmpty() && !t.isEmpty()) parents.computeIfAbsent(t, k -> new LinkedHashSet<>()).add(s);
        }

        for (JsonNode n : nodes) {
            String id = n.path("id").asText("");
            String type = nodeType.getOrDefault(id, "");
            JsonNode params = n.path("params");
            for (String field : referencedFields(type, params)) {
                Set<String> reachableIn = new LinkedHashSet<>();
                collectReachableInputs(id, parents, inputFields, reachableIn, new HashSet<>());
                if (reachableIn.isEmpty()) continue;                        // 拿不到声明式来源，放行
                boolean anyDeclares = reachableIn.stream().anyMatch(k -> inputFields.get(k).contains(field));
                if (!anyDeclares) {
                    errors.add("节点「" + nodeLabel.getOrDefault(id, id) + "」引用的字段 `" + field
                            + "` 在上游输入声明的字段中不存在（可用字段：" + String.join(", ", inputFields.get(reachableIn.iterator().next()))
                            + "）；请核对输入节点的「字段定义」或该控件里的字段名");
                }
            }
        }
    }

    /**
     * 某节点 params 里引用到的「上游字段名」。
     * 注意只收「输入侧」字段：targetField / newFieldName 之类是转换节点**新产出**的字段，
     * 它们本来就不在上游 schema 里，误当成引用会制造假告警。
     */
    private List<String> referencedFields(String type, JsonNode params) {
        List<String> out = new ArrayList<>();
        if (params == null || params.isMissingNode() || !params.isObject()) return out;
        if ("redis_lookup".equals(type)) {
            addField(out, params, "keyField");
        } else if ("xml_json".equals(type) || "json_parse".equals(type)) {
            addField(out, params, "sourceField");
        } else if ("field_filter".equals(type)) {
            for (String f : splitCsv(params.path("fields").asText(""))) out.add(f);
        } else if ("field_rename".equals(type)) {
            for (String pair : splitCsv(params.path("mappings").asText(""))) {
                int idx = pair.indexOf(':');
                out.add((idx > 0 ? pair.substring(0, idx) : pair).trim());
            }
        }
        return out;
    }

    private void addField(List<String> out, JsonNode params, String key) {
        String v = params.path(key).asText("").trim();
        if (!v.isEmpty()) out.add(v);
    }

    private List<String> splitCsv(String raw) {
        List<String> out = new ArrayList<>();
        if (raw == null) return out;
        for (String part : raw.split(",")) {
            String t = part.trim();
            if (!t.isEmpty()) out.add(t);
        }
        return out;
    }

    /** 输入节点 params 里声明的字段名（fieldsConfig 形如 [{"name":"id","type":"INT"}]） */
    private Set<String> declaredFields(JsonNode params) {
        Set<String> out = new LinkedHashSet<>();
        if (params == null || !params.isObject()) return out;
        String fc = params.path("fieldsConfig").asText("");
        if (fc.isBlank()) return out;
        try {
            JsonNode arr = objectMapper.readTree(fc);
            if (arr.isArray()) {
                for (JsonNode f : arr) {
                    String name = f.path("name").asText("").trim();
                    if (!name.isEmpty()) out.add(name);
                }
            }
        } catch (Exception ignore) {
            // fieldsConfig 非法由参数校验负责报告，这里不重复报错
        }
        return out;
    }

    private void collectReachableInputs(String nodeId, Map<String, Set<String>> parents,
                                        Map<String, Set<String>> inputFields, Set<String> acc, Set<String> seen) {
        if (!seen.add(nodeId)) return;
        if (inputFields.containsKey(nodeId)) { acc.add(nodeId); return; }
        for (String p : parents.getOrDefault(nodeId, Set.of())) {
            collectReachableInputs(p, parents, inputFields, acc, seen);
        }
    }

    /**
     * Kafka 节点校验：topic 必须真实存在。
     *
     * 为什么要挡在提交前：Flink 的 kafka source 在建表阶段不校验 topic，
     * 直到运行期才发现 → 作业提交成功再 FAILED，日志里只有一坨
     * "UnknownTopicOrPartitionException ... Recovery is suppressed by NoRestartBackoffTimeStrategy"，
     * 用户完全看不出是 topic 名字写错了（实测：默认值 test-topic 就会踩）。
     */
    private void validateKafkaTopic(JsonNode node, List<String> errors, List<String> warnings) {
        JsonNode params = node.path("params");
        String label = node.path("label").asText(node.path("id").asText("节点"));
        String topic = params.path("topic").asText("").trim();
        String bs = params.path("bootstrapServers").asText("").trim();
        if (topic.isEmpty()) {
            errors.add(label + "：未填写 Kafka topic");
            return;
        }
        if (bs.isEmpty()) {
            errors.add(label + "：未填写 Bootstrap 地址");
            return;
        }
        List<KafkaMonitorService.TopicInfo> topics;
        try {
            topics = kafkaMonitorService.listTopics();
        } catch (Exception e) {
            warnings.add(label + "：无法连接 Kafka（" + bs + "）以校验 topic 是否存在：" + e.getMessage()
                    + "。broker 不可达时作业会在运行期失败。");
            return;
        }
        boolean exists = topics.stream().anyMatch(t -> topic.equals(t.name()));
        if (!exists) {
            StringBuilder sample = new StringBuilder();
            for (KafkaMonitorService.TopicInfo t : topics) {
                if (sample.length() > 0) sample.append("、");
                if (sample.length() > 90) { sample.append("…"); break; }
                sample.append(t.name());
            }
            errors.add(label + "：Kafka topic「" + topic + "」不存在（broker " + bs + "）。"
                    + (sample.length() == 0 ? "当前 broker 上没有任何 topic。" : "可用 topic：" + sample + "。")
                    + "请改成已存在的 topic（或先在 broker 上创建它）—— 否则作业会提交成功但在运行期失败。");
        }
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
                    // 字段类参数历史上被存成 JSON 数组（如 field_concat.fields），翻译层两种形式都能解析
                    case "string" -> value.isTextual() || value.isArray();
                    case "number" -> isNumericLike(value);
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
