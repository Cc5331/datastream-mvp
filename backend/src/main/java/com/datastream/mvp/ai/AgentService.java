package com.datastream.mvp.ai;

import com.datastream.mvp.model.ControlRegistry;
import com.datastream.mvp.model.JobDefinition;
import com.datastream.mvp.model.JobLog;
import com.datastream.mvp.model.DiagnosisReport;
import com.datastream.mvp.repository.DiagnosisReportRepository;
import com.datastream.mvp.repository.JobLogRepository;
import com.datastream.mvp.security.CurrentUser;
import com.datastream.mvp.service.ControlRegistryService;
import com.datastream.mvp.service.JobService;
import com.datastream.mvp.service.MonitorService;
import com.datastream.mvp.service.PreviewService;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * AI 智能化层核心服务：
 * 1. NL2Pipeline：自然语言 -> 可执行 DAG（仅使用注册表已有控件类型与合法参数），生成结果一律 DRAFT + 人工确认；
 * 2. 智能诊断：组装诊断数据包，先走本地规则引擎（覆盖 80% 高频问题），未命中再调用 DeepSeek 归因。
 * 安全红线：发给 LLM 的数据只含字段结构 / 前 5 行预览，不整表发送；密码字段一律掩码。
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class AgentService {

    private final ControlRegistryService controlService;
    private final JobService jobService;
    private final PreviewService previewService;
    private final MonitorService monitorService;
    private final JobLogRepository logRepo;
    private final DiagnosisReportRepository diagnosisRepo;
    private final PromptCatalog promptCatalog;
    private final LlmClient llmClient;
    private final ObjectMapper objectMapper;

    private static final Pattern WINDOWS_PATH = Pattern.compile("[A-Za-z]:\\\\[^\\s\"']+");
    private static final Pattern FILE_PATH = Pattern.compile("(?:/\\\\|\\\\)?[^\\s\"']+\\.(?:csv|xlsx|xls|json|xml|parquet|txt)");

    // ===================== NL2Pipeline =====================

    public Map<String, Object> nl2Pipeline(String prompt, String model, CurrentUser cu) {
        if (prompt == null || prompt.isBlank()) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "请输入自然语言描述");
        }
        String fileHints = collectFileHints(prompt);
        String controlDigest = buildControlDigest();
        boolean english = com.datastream.mvp.util.LanguageDetector.isEnglish(prompt.trim());
        String system = english ? promptCatalog.nl2PipelineEnglish(controlDigest) : promptCatalog.nl2PipelineChinese(controlDigest);
        String user = english
                ? "User request:\n" + prompt.trim()
                + (fileHints.isEmpty() ? "" : "\n\nDetected files (field structure, first 5 rows):\n" + fileHints)
                + "\n\nReturn the DAG JSON directly."
                : "用户需求：\n" + prompt.trim()
                + (fileHints.isEmpty() ? "" : "\n\n已探测到以下文件（含字段结构，仅前 5 行）：\n" + fileHints)
                + "\n\n请直接返回 DAG JSON。";

        JsonNode raw;
        try {
            raw = llmClient.chatJson(system, user, model);
        } catch (Exception e) {
            throw new ResponseStatusException(HttpStatus.BAD_GATEWAY, e.getMessage());
        }
        JobDefinition job = buildJobFromLlm(raw, prompt, cu);
        return Map.of(
                "job", job,
                "dag", job.getDagJson(),
                "controlTypes", usedControlTypes(job.getDagJson()),
                "source", llmClient.providerName());
    }

    /** 从 LLM 返回的 JSON 构建并保存 DRAFT 作业（校验 + 规范化） */
    private JobDefinition buildJobFromLlm(JsonNode raw, String originalPrompt, CurrentUser cu) {
        if (raw == null || !raw.isObject()) {
            throw new ResponseStatusException(HttpStatus.BAD_GATEWAY, "AI 返回格式错误：缺少 DAG JSON");
        }
        JsonNode nodes = raw.path("nodes");
        if (!nodes.isArray() || nodes.isEmpty()) {
            throw new ResponseStatusException(HttpStatus.BAD_GATEWAY, "AI 未生成任何节点，请补充更具体的描述");
        }
        Map<String, ControlRegistry> registry = new LinkedHashMap<>();
        for (ControlRegistry c : controlService.findAll()) registry.put(c.getType(), c);

        // ---- 节点规范化 ----
        ArrayNode normNodes = objectMapper.createArrayNode();
        Set<String> usedIds = new HashSet<>();
        Map<String, String> idType = new LinkedHashMap<>();
        int inputCount = 0, outputCount = 0;
        for (JsonNode n : nodes) {
            String type = n.path("type").asText("").trim();
            ControlRegistry ctrl = registry.get(type);
            if (ctrl == null) {
                throw new ResponseStatusException(HttpStatus.BAD_GATEWAY, "AI 生成了未注册控件类型: " + type + "，请重试或手动配置");
            }
            String label = n.path("label").asText(ctrl.getName());
            String baseId = sanitizeId(n.path("id").asText(type + "_" + (usedIds.size() + 1)));
            String id = baseId;
            int k = 1;
            while (!usedIds.add(id)) id = baseId + "_" + (++k);

            ObjectNode node = objectMapper.createObjectNode();
            node.put("id", id);
            node.put("type", type);
            node.put("label", label);
            ObjectNode params = objectMapper.createObjectNode();
            if (n.path("params").isObject()) {
                params = (ObjectNode) n.path("params");
            }
            params = normalizeParams(params, ctrl);
            node.set("params", params);
            node.put("x", n.path("x").isNumber() ? n.path("x").asDouble() : 120 + (usedIds.size() % 5) * 220);
            node.put("y", n.path("y").isNumber() ? n.path("y").asDouble() : 120 + (usedIds.size() / 5) * 100);
            normNodes.add(node);
            idType.put(id, type);
            if ("input".equals(ctrl.getCategory())) inputCount++;
            if ("output".equals(ctrl.getCategory())) outputCount++;
        }
        if (inputCount == 0) throw new ResponseStatusException(HttpStatus.BAD_GATEWAY, "AI 生成的 DAG 缺少输入控件");
        if (outputCount == 0) throw new ResponseStatusException(HttpStatus.BAD_GATEWAY, "AI 生成的 DAG 缺少输出控件");

        // ---- 边规范化 ----
        ArrayNode normEdges = objectMapper.createArrayNode();
        Set<String> edgeKeys = new HashSet<>();
        if (raw.path("edges").isArray()) {
            for (JsonNode e : raw.path("edges")) {
                String src = sanitizeId(e.path("source").asText(""));
                String tgt = sanitizeId(e.path("target").asText(""));
                if (!idType.containsKey(src) || !idType.containsKey(tgt)) continue;
                if ("output".equals(categoryOf(idType.get(src), registry)) && "input".equals(categoryOf(idType.get(tgt), registry))) continue;
                String key = src + "->" + tgt;
                if (!edgeKeys.add(key)) continue;
                ObjectNode edge = objectMapper.createObjectNode();
                edge.put("id", "e_" + src + "_" + tgt);
                edge.put("source", src);
                edge.put("target", tgt);
                normEdges.add(edge);
            }
        }

        ObjectNode dag = objectMapper.createObjectNode();
        dag.put("jobName", raw.path("jobName").asText(truncate(originalPrompt.trim(), 24)));
        dag.put("parallelism", raw.path("parallelism").isNumber() ? raw.path("parallelism").asInt(1) : 1);
        dag.set("nodes", normNodes);
        dag.set("edges", normEdges);

        JobDefinition job = new JobDefinition();
        job.setName(dag.path("jobName").asText("AI 生成的作业"));
        try {
            job.setDagJson(objectMapper.writeValueAsString(dag));
        } catch (Exception e) {
            throw new ResponseStatusException(HttpStatus.BAD_GATEWAY, "DAG 序列化失败: " + e.getMessage());
        }
        job.setParallelism(dag.path("parallelism").asInt(1));
        job.setSource(JobDefinition.JobSource.AI);
        job.setConfirmationStatus(JobDefinition.ConfirmationStatus.PENDING);
        job.setDescription("由 AI 助手生成（DRAFT，需人工确认）：" + truncate(originalPrompt.trim(), 200));
        return jobService.create(job, cu);
    }

    private String categoryOf(String type, Map<String, ControlRegistry> registry) {
        ControlRegistry c = registry.get(type);
        return c == null ? "" : c.getCategory();
    }

    /** 参数规范化：按 paramSchema 校验类型、补齐必填默认值、路径参数 trim */
    private ObjectNode normalizeParams(ObjectNode params, ControlRegistry ctrl) {
        ObjectNode out = objectMapper.createObjectNode();
        Set<String> required = new HashSet<>();
        Map<String, JsonNode> propDefaults = new LinkedHashMap<>();
        Map<String, String> propTypes = new LinkedHashMap<>();
        try {
            JsonNode schema = objectMapper.readTree(ctrl.getParamSchema());
            if (schema.has("required") && schema.get("required").isArray()) {
                schema.get("required").forEach(r -> required.add(r.asText()));
            }
            JsonNode props = schema.path("properties");
            if (props.isObject()) {
                props.fields().forEachRemaining(e -> {
                    propTypes.put(e.getKey(), e.getValue().path("type").asText("string"));
                    if (e.getValue().has("default")) propDefaults.put(e.getKey(), e.getValue().get("default"));
                });
            }
        } catch (Exception ignore) {}

        if (params != null) {
            params.fields().forEachRemaining(e -> out.set(e.getKey(), coerce(e.getValue(), propTypes.get(e.getKey()))));
        }
        // 必填但缺失 -> 默认值（若有）
        for (String r : required) {
            if (!out.has(r) && propDefaults.containsKey(r)) {
                out.set(r, coerce(propDefaults.get(r), propTypes.get(r)));
            }
        }
        // 路径类参数统一 trim
        if (out.has("path") && out.get("path").isTextual()) {
            out.put("path", out.get("path").asText().trim());
        }
        return out;
    }

    private JsonNode coerce(JsonNode v, String type) {
        if (v == null || v.isNull()) return objectMapper.getNodeFactory().nullNode();
        if ("boolean".equals(type) && v.isTextual()) {
            return objectMapper.getNodeFactory().booleanNode(Boolean.parseBoolean(v.asText()));
        }
        if (("integer".equals(type) || "number".equals(type)) && v.isTextual()) {
            try { return objectMapper.getNodeFactory().numberNode(Double.parseDouble(v.asText())); } catch (Exception ignore) {}
        }
        return v;
    }

    private String sanitizeId(String id) {
        String s = id == null ? "" : id.trim();
        s = s.replaceAll("[^a-zA-Z0-9_\\-]", "_");
        if (s.isEmpty()) s = "node";
        return s;
    }

    // ===================== 智能诊断 =====================

    public Map<String, Object> diagnose(Long jobId, String model, CurrentUser currentUser) {
        return diagnose(jobId, model, currentUser, "manual");
    }

    public Map<String, Object> diagnose(Long jobId, String model, CurrentUser currentUser, String trigger) {
        JobDefinition job = jobService.findByIdForUser(jobId, currentUser);
        ObjectNode packet = buildDiagnosisPacket(job);
        Map<String, Object> result = doDiagnose(packet, model);
        persistDiagnosis(job, currentUser, trigger, packet, result);
        return result;
    }

    /** 执行诊断：本地规则 -> （本地规则未命中时）LLM 归因；不抛异常时返回可持久化的结果 Map。 */
    public Map<String, Object> doDiagnose(ObjectNode packet, String model) {
        Map<String, Object> ruleHit = localRuleEngine(packet);
        if (ruleHit != null) return ruleHit;

        // 未配置外部 LLM 时仍返回本地诊断结果，避免前端因 502 完全不可用
        if (!llmClient.isConfigured()) {
            Map<String, Object> result = new LinkedHashMap<>();
            result.put("source", "local-fallback");
            result.put("rootCause", "本地规则未识别到明确根因，且 ChatGPT API 尚未配置");
            result.put("evidence", findFirstError(packet));
            result.put("suggestions", List.of(
                    "先查看作业日志中第一条 ERROR 并检查对应节点参数",
                    "确认 Flink、SQL Gateway、数据源和输出目录均可访问",
                    "如需 ChatGPT 深度归因，请在 .env 中设置 OPENAI_API_KEY 后重启 Backend"));
            result.put("paramFixes", objectMapper.createObjectNode());
            return result;
        }

        // 本地规则未命中 -> LLM 归因
        try {
            String system = "你是数据流平台（Spring Boot + Flink）的故障诊断专家。"
                    + "根据给定的作业诊断数据包，输出故障归因 JSON，不要输出额外文字。格式："
                    + "{\"rootCause\":\"一句话根因\",\"evidence\":\"关键证据\",\"suggestions\":[\"建议1\",\"建议2\"],\"paramFixes\":{\"节点id\":{\"参数名\":\"修正值\"}}}"
                    + "注意：paramFixes 只能修正 DAG 节点参数（如 path/url/table/username/password/topic/bootstrapServers），不要改节点类型；无法确定修正值时给空对象。";
            JsonNode resp = llmClient.chatJson(system, "诊断数据包：\n" + packet.toString(), model);
            Map<String, Object> result = new LinkedHashMap<>();
            result.put("source", llmClient.providerName());
            result.put("rootCause", resp.path("rootCause").asText("未知根因"));
            result.put("evidence", resp.path("evidence").asText(""));
            List<String> suggestions = new ArrayList<>();
            resp.path("suggestions").forEach(s -> suggestions.add(s.asText()));
            result.put("suggestions", suggestions);
            result.put("paramFixes", resp.path("paramFixes").isObject() ? resp.path("paramFixes") : objectMapper.createObjectNode());
            return result;
        } catch (Exception e) {
            throw new ResponseStatusException(HttpStatus.BAD_GATEWAY, "诊断服务不可用: " + e.getMessage());
        }
    }

    private void persistDiagnosis(JobDefinition job, CurrentUser currentUser, String trigger,
                                  ObjectNode packet, Map<String, Object> result) {
        try {
            DiagnosisReport report = new DiagnosisReport();
            report.setJobId(job.getId());
            report.setOwnerId(job.getOwnerId());
            report.setJobName(job.getName());
            report.setTrigger(trigger);
            report.setSource(String.valueOf(result.get("source")));
            report.setRootCause(truncate(String.valueOf(result.get("rootCause")), 1000));
            report.setEvidence(truncate(String.valueOf(result.get("evidence")), 4000));
            Object suggestions = result.get("suggestions");
            report.setSuggestions(truncate(suggestions == null ? "[]" : objectMapper.writeValueAsString(suggestions), 4000));
            Object paramFixes = result.get("paramFixes");
            report.setParamFixes(paramFixes == null ? "{}" : objectMapper.writeValueAsString(paramFixes));
            report.setPacket(packet.toString());
            report.setCreatedAt(java.time.LocalDateTime.now());
            diagnosisRepo.save(report);
        } catch (Exception e) {
            log.warn("persist diagnosis failed for job {}: {}", job.getId(), e.getMessage());
        }
    }

    public List<DiagnosisReport> listDiagnosis(Long jobId) {
        return diagnosisRepo.findByJobIdOrderByCreatedAtDesc(jobId);
    }

    /**
     * 故障告警自动触发诊断（系统驱动，无用户上下文）。
     * 拉取作业、组装脱敏数据包、执行诊断并持久化一份 trigger=<告警事件> 的报告。
     * 不参与 owner 校验（消息源自监控层，只为告警对应的作业生成报告）。
     */
    public void autoDiagnose(Long jobId, String event) {
        try {
            JobDefinition job = jobService.findById(jobId);
            ObjectNode packet = buildDiagnosisPacket(job);
            Map<String, Object> result = doDiagnose(packet, null);
            persistDiagnosis(job, null, event, packet, result);
            log.info("auto diagnosis done for job {} trigger={}", jobId, event);
        } catch (Exception e) {
            log.warn("auto diagnosis failed for job {}: {}", jobId, e.getMessage());
        }
    }

    /** 组装诊断数据包（敏感字段掩码，只含结构与前几行预览） */
    private ObjectNode buildDiagnosisPacket(JobDefinition job) {
        ObjectNode p = objectMapper.createObjectNode();
        p.put("jobId", job.getId());
        p.put("jobName", job.getName());
        p.put("status", job.getStatus() == null ? "" : job.getStatus().name());
        p.put("flinkJobId", job.getFlinkJobId());
        p.put("parallelism", job.getParallelism());
        p.put("submittedAt", job.getSubmittedAt() == null ? "" : job.getSubmittedAt().toString());
        p.put("online", Boolean.TRUE.equals(job.getOnline()));
        p.put("restartCount", job.getOnlineRestartCount() == null ? 0 : job.getOnlineRestartCount());

        ObjectNode dag = objectMapper.createObjectNode();
        try {
            JsonNode root = objectMapper.readTree(job.getDagJson());
            dag.set("nodes", maskParams(root.path("nodes")));
            dag.set("edges", root.path("edges"));
        } catch (Exception e) {
            dag.put("parseError", String.valueOf(e.getMessage()));
        }
        p.set("dag", dag);

        ArrayNode logs = objectMapper.createArrayNode();
        List<JobLog> recent = logRepo.findTop20ByJobIdOrderByTimestampDesc(job.getId());
        for (JobLog l : recent) {
            ObjectNode lo = objectMapper.createObjectNode();
            lo.put("time", l.getTimestamp() == null ? "" : l.getTimestamp().toString());
            lo.put("level", l.getLevel());
            lo.put("message", truncate(l.getMessage() == null ? "" : l.getMessage(), 800));
            logs.add(lo);
        }
        p.set("logs", logs);

        // 运行指标（若仍在 Flink 中）
        try {
            for (JsonNode m : monitorService.overview()) {
                if (m.path("id").asLong(-1) == job.getId()) {
                    p.set("metrics", m.path("metrics"));
                    break;
                }
            }
        } catch (Exception ignore) {}
        return p;
    }

    private ArrayNode maskParams(JsonNode nodes) {
        ArrayNode out = objectMapper.createArrayNode();
        if (!nodes.isArray()) return out;
        for (JsonNode n : nodes) {
            ObjectNode copy = n.deepCopy();
            if (copy.has("params") && copy.get("params").isObject()) {
                ObjectNode params = (ObjectNode) copy.get("params");
                if (params.has("password") && params.get("password").isTextual()
                        && !params.get("password").asText().isEmpty()) {
                    params.put("password", "******");
                }
            }
            out.add(copy);
        }
        return out;
    }

    /** 本地规则引擎：覆盖 80% 高频问题 */
    private Map<String, Object> localRuleEngine(ObjectNode packet) {
        String allText = packet.toString();
        String lower = allText.toLowerCase();
        String jobName = packet.path("jobName").asText("");
        String evidence = findFirstError(packet);

        Map<String, Object> hit = null;
        if ("COMPLETED".equals(packet.path("status").asText())) {
            hit = rule("作业当前运行正常，未发现需要修复的故障",
                    "作业状态为 COMPLETED，Flink Job ID: " + packet.path("flinkJobId").asText("-"),
                    List.of("检查输出文件或目标表的数据量是否符合预期", "如需持续运行，请按业务需要配置调度或上线", "可继续观察后续运行日志与告警"),
                    Map.of(), jobName);
        } else if (lower.contains("no such file") || lower.contains("文件不存在") || lower.contains("does not exist") || lower.contains("找不到")) {
            hit = rule("文件路径不存在", evidence,
                    List.of("检查输入/输出节点的 path 参数是否真实存在", "确认文件是否被移动或删除", "Windows 路径注意反斜杠转义"),
                    Map.of(), jobName);
        } else if (lower.contains("communications link failure") || (lower.contains("connection refused") && lower.contains("jdbc"))) {
            hit = rule("数据库连接失败（MySQL 拒绝连接）", evidence,
                    List.of("确认 MySQL 服务已启动（netstat -ano | findstr :3306）", "检查 JDBC URL 主机与端口", "确认用户名/密码正确"),
                    Map.of(), jobName);
        } else if (lower.contains("access denied for user") || lower.contains("unknown database")) {
            hit = rule("数据库账号权限 / 库不存在", evidence,
                    List.of("确认 MySQL 用户名密码正确", "确认数据库名已创建（CREATE DATABASE ...）", "给用户授权访问该库"),
                    Map.of(), jobName);
        } else if (lower.contains("table") && (lower.contains("doesn't exist") || lower.contains("does not exist"))) {
            hit = rule("目标表不存在（MySQL 输出未自动建表或表名错误）", evidence,
                    List.of("确认输出表名与实际需求一致", "检查 URL 指向的数据库", "手动创建表或改用已存在的表名"),
                    Map.of(), jobName);
        } else if (lower.contains("excel") && (lower.contains("conversion failed") || lower.contains("转换失败"))) {
            hit = rule("Excel 转换失败", evidence,
                    List.of("确认 Excel 文件路径存在且为 .xlsx/.xls", "确认文件未被占用（Excel 打开中会锁定）", "确认首行是表头"),
                    Map.of(), jobName);
        } else if (lower.contains("mock") || lower.contains("降级") || lower.contains("flink 集群不可用")) {
            hit = rule("Flink 集群不可用（提交降级为 mock）", evidence,
                    List.of("启动 Flink：D:\\code\\flink-1.18.1\\bin\\start-cluster.bat", "检查 Flink Web UI http://localhost:18081", "上线作业要求真实运行，先恢复集群再上线"),
                    Map.of(), jobName);
        } else if (lower.contains("unknown column") || (lower.contains("field") && lower.contains("not found"))) {
            hit = rule("字段名不匹配（DAG 中引用的字段不存在）", evidence,
                    List.of("用“预览数据”查看输入文件/表的真实字段名", "修正转换控件（field_concat/field_filter/field_rename）里的字段名"),
                    Map.of(), jobName);
        } else if (lower.contains("kafka") && (lower.contains("topic") || lower.contains("no brokers") || lower.contains("connection"))) {
            hit = rule("Kafka 连接 / 主题问题", evidence,
                    List.of("确认 Kafka 已启动（docker compose 或 29092 端口）", "确认 topic 已创建且名称正确", "确认 bootstrapServers 地址可达"),
                    Map.of(), jobName);
        } else if (lower.contains("json") && lower.contains("parse")) {
            hit = rule("JSON 解析失败", evidence,
                    List.of("确认输入 JSON 是 JSON Lines 或合法数组", "检查 json_parse 控件字段路径是否与数据一致"),
                    Map.of(), jobName);
        }
        return hit;
    }

    private Map<String, Object> rule(String rootCause, String evidence, List<String> suggestions, Map<String, Object> fixes, String jobName) {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("source", "rule");
        result.put("rootCause", rootCause);
        result.put("evidence", evidence == null || evidence.isBlank() ? "依据作业日志与 DAG 参数推断" : evidence);
        result.put("suggestions", suggestions);
        ObjectNode fixNode = objectMapper.valueToTree(fixes);
        result.put("paramFixes", fixNode);
        return result;
    }

    private String findFirstError(ObjectNode packet) {
        try {
            for (JsonNode l : packet.path("logs")) {
                if ("ERROR".equals(l.path("level").asText()) && l.path("message").asText().length() > 8) {
                    return l.path("message").asText();
                }
            }
        } catch (Exception ignore) {}
        return "";
    }

    // ===================== 工具 =====================

    /** 控件注册表摘要（type/name/category/params） */
    private String buildControlDigest() {
        StringBuilder sb = new StringBuilder();
        for (ControlRegistry c : controlService.findAll()) {
            sb.append("- type=").append(c.getType())
                    .append(", name=").append(c.getName())
                    .append(", category=").append(c.getCategory())
                    .append(", desc=").append(c.getDescription() == null ? "" : c.getDescription())
                    .append(", params=").append(c.getParamSchema() == null ? "{}" : c.getParamSchema())
                    .append("\n");
        }
        return sb.toString();
    }

    /** 从用户描述中探测文件路径并取前 5 行预览（只发字段结构） */
    private String collectFileHints(String prompt) {
        Set<String> paths = new LinkedHashSet<>();
        Matcher m1 = WINDOWS_PATH.matcher(prompt);
        while (m1.find()) paths.add(m1.group().trim());
        if (paths.isEmpty()) {
            Matcher m2 = FILE_PATH.matcher(prompt);
            while (m2.find()) paths.add(m2.group().trim());
        }
        StringBuilder sb = new StringBuilder();
        int count = 0;
        for (String p : paths) {
            if (count >= 3) break;
            try {
                Map<String, Object> prev = previewService.previewFile(p, 5);
                Object cols = prev.get("columns");
                if (cols instanceof List && !((List<?>) cols).isEmpty()) {
                    sb.append("文件[").append(p).append("] 字段: ").append(cols).append("\n");
                    count++;
                }
            } catch (Exception ignore) {}
        }
        return sb.toString();
    }

    private String usedControlTypes(String dagJson) {
        try {
            Set<String> types = new LinkedHashSet<>();
            JsonNode root = objectMapper.readTree(dagJson);
            root.path("nodes").forEach(n -> types.add(n.path("type").asText()));
            return String.join(",", types);
        } catch (Exception e) {
            return "";
        }
    }

    private String truncate(String s, int max) {
        if (s == null) return "";
        return s.length() > max ? s.substring(0, max) : s;
    }
}
