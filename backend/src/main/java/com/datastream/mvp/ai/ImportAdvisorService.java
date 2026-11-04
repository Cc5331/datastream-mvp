package com.datastream.mvp.ai;

import com.datastream.mvp.service.PreviewService;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * 文件导入建议器：给画布「拖拽文件导入」确认框提供**可解释的建议**（不自动改用户的表单）。
 *
 * <p>两段式，和智能诊断同范式：</p>
 * <ol>
 *   <li><b>本地规则</b>（永远可用、可单测）：前导零 id 别用 INT、日期/度量列类型、全空列、
 *       字段名规范化（可一键应用为 renames）、主键候选、列数过多、缺时间列……</li>
 *   <li><b>模型补充</b>（`LlmClient.isConfigured()` 时才跑）：只发**列名 + 类型 + 前 3 行**，
 *       让模型给「这份数据适合怎么落地」的建议；失败/未配置一律静默降级为纯本地建议。</li>
 * </ol>
 *
 * <p>安全口径与 AgentService 一致：只发结构 + 少量样本行，整表不外发；建议只是建议，
 * 节点参数仍要用户在确认框里点「确认生成控件」。</p>
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class ImportAdvisorService {

    private static final Pattern ID_LIKE = Pattern.compile("(?i).*(id|code|no|uuid|编号|编码|工号|订单号|流水号).*");
    private static final Pattern DATE_LIKE = Pattern.compile("(?i).*(date|time|day|ts|日期|时间|年月).*");
    private static final Pattern MEASURE_LIKE = Pattern.compile("(?i).*(amount|price|qty|quantity|count|total|score|rate|金额|数量|单价|总额|评分).*");
    private static final Pattern BAD_NAME = Pattern.compile(".*[^A-Za-z0-9_\\u4e00-\\u9fa5]+.*");
    private static final int LLM_SAMPLE_ROWS = 3;
    private static final int MANY_COLUMNS = 15;

    private final PreviewService previewService;
    private final LlmClient llmClient;
    private final ObjectMapper objectMapper;

    /**
     * 分析一个已暂存的导入文件，返回建议清单。
     *
     * @param path        服务器路径（必须落在 app.preview.allowed-roots 内，PreviewService 会校验）
     * @param controlType 用户当前选中的控件类型（仅用于文案）
     */
    public Map<String, Object> inspect(String path, String controlType) {
        if (path == null || path.isBlank()) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "缺少文件路径");
        }
        Map<String, Object> preview = previewService.previewFile(path, 20);
        List<String> columns = asStringList(preview.get("columns"));
        List<List<String>> rows = asRows(preview.get("rows"));
        if (columns.isEmpty()) {
            Object message = preview.get("message");
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST,
                    message == null ? "没有从文件里识别到可分析的列" : String.valueOf(message));
        }

        List<Map<String, Object>> suggestions = new ArrayList<>(localSuggestions(columns, rows));
        Map<String, String> renames = new LinkedHashMap<>();
        collectRenames(suggestions, renames);

        String summary = null;
        String source = "local-rules";
        if (llmClient.isConfigured()) {
            try {
                Map<String, Object> advice = askModel(columns, rows);
                if (advice != null) {
                    summary = (String) advice.getOrDefault("summary", null);
                    @SuppressWarnings("unchecked")
                    List<Map<String, Object>> aiSuggestions = (List<Map<String, Object>>) advice.get("suggestions");
                    if (aiSuggestions != null) suggestions.addAll(aiSuggestions);
                    @SuppressWarnings("unchecked")
                    Map<String, String> aiRenames = (Map<String, String>) advice.get("renames");
                    if (aiRenames != null) renames.putAll(aiRenames);
                    source = llmClient.providerName();
                }
            } catch (Exception e) {
                log.warn("Import advisor LLM advice failed (degrade to local rules): {}", e.getMessage());
            }
        }

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("path", path);
        result.put("controlType", controlType == null ? "" : controlType);
        result.put("source", source);
        result.put("summary", summary);
        result.put("columns", columns);
        result.put("suggestions", suggestions);
        result.put("renames", renames);
        return result;
    }

    // ===================== 本地规则 =====================

    private List<Map<String, Object>> localSuggestions(List<String> columns, List<List<String>> rows) {
        List<Map<String, Object>> out = new ArrayList<>();
        Set<String> seenUniqueKey = new LinkedHashSet<>();

        for (int i = 0; i < columns.size(); i++) {
            String name = columns.get(i);
            List<String> values = columnValues(rows, i);
            long nonEmpty = values.stream().filter(v -> v != null && !v.trim().isEmpty()).count();

            // ① 全空列：Flink 会整列读成 null，建议直接从字段定义里去掉
            if (!rows.isEmpty() && nonEmpty == 0) {
                out.add(suggestion("empty-column", "列「" + name + "」在样本里全为空",
                        "建议从字段定义里去掉该列，否则下游聚合/校验会一直拿到 null", null));
                continue;
            }

            // ② 前导零风险：id/编号类列用 INT 会丢前导零（000123 -> 123）
            if (ID_LIKE.matcher(name).matches() && looksInteger(values)) {
                out.add(suggestion("leading-zero", "列「" + name + "」像编号，建议用 STRING",
                        "编号列转成 INT/BIGINT 会丢掉前导零与超长位（如 000123、身份证、订单号），"
                                + "建议类型保持 STRING。样本：" + sample(values, 3), null));
            }

            // ③ 日期列：名字像日期但样本不是标准格式
            if (DATE_LIKE.matcher(name).matches() && !values.isEmpty()
                    && !looksDate(values) && !looksTimestamp(values)) {
                out.add(suggestion("date-format", "列「" + name + "」是日期语义，但样本不是标准格式",
                        "建议在文件里统一成 yyyy-MM-dd 或 yyyy-MM-dd HH:mm:ss 再设 DATE/TIMESTAMP，"
                                + "否则 Flink 只能按 STRING 处理，时间窗口聚合用不了。样本：" + sample(values, 3), null));
            }

            // ④ 度量列：金额/数量类被推断成 STRING 时，多半混了千分位或单位
            if (MEASURE_LIKE.matcher(name).matches() && !values.isEmpty() && !looksNumeric(values)) {
                out.add(suggestion("measure-type", "列「" + name + "」是度量列却含非数字",
                        "检查是否混入千分位、货币符号或单位；清掉后可设 DOUBLE/BIGINT，否则数值计算会退化成字符串比较。"
                                + "样本：" + sample(values, 3), null));
            }

            // ⑤ 字段名规范化：空格/横线/奇怪字符会逼着下游到处加反引号
            if (BAD_NAME.matcher(name).matches()) {
                String normalized = normalizeName(name);
                out.add(suggestion("name-style", "字段名「" + name + "」含特殊字符",
                        "建议改成 " + normalized + "（页面可一键应用），避免下游引用时要反复加反引号", normalized));
            }
        }

        // ⑥ 主键候选：单列唯一且像 id（可用于 dedupe / 幂等写入）
        for (int i = 0; i < columns.size(); i++) {
            String name = columns.get(i);
            List<String> values = columnValues(rows, i);
            if (values.size() < 3) continue;
            if (new LinkedHashSet<>(values).size() == values.size() && ID_LIKE.matcher(name).matches()) {
                seenUniqueKey.add(name);
            }
        }
        if (!seenUniqueKey.isEmpty()) {
            out.add(suggestion("unique-key", "唯一列可用于去重/幂等写入：" + String.join(", ", seenUniqueKey),
                    "若下游要去重或写 MySQL 需要幂等，可把该字段作为「按字段去重」的 keyFields", null));
        }

        // ⑦ 列数过多：大宽表直接落库很难维护
        if (columns.size() > MANY_COLUMNS) {
            out.add(suggestion("too-many-columns", "列数偏多（" + columns.size() + " 列）",
                    "建议在输出前加「字段过滤」节点只保留分析需要的列，减少落库与网络开销", null));
        }

        // ⑧ 没有任何时间语义的列：时间窗口类分析做不了
        boolean hasTimeColumn = columns.stream().anyMatch(c -> DATE_LIKE.matcher(c).matches());
        if (!hasTimeColumn) {
            out.add(suggestion("no-time-column", "未发现时间列",
                    "这份数据做不了按时间窗口的聚合/趋势；如需此类分析，请在数据里补一列时间并设为 DATE/TIMESTAMP", null));
        }
        return out;
    }

    private void collectRenames(List<Map<String, Object>> suggestions, Map<String, String> renames) {
        for (Map<String, Object> s : suggestions) {
            Object rename = s.get("suggestedName");
            if (rename instanceof String target && !target.isBlank()) {
                Object title = s.get("title");
                // title 形如「字段名「原名」含特殊字符」
                String origin = extractQuoted(title == null ? "" : title.toString());
                if (!origin.isBlank() && !origin.equals(target)) renames.putIfAbsent(origin, target);
            }
        }
    }

    private Map<String, Object> suggestion(String kind, String title, String detail, String suggestedName) {
        Map<String, Object> s = new LinkedHashMap<>();
        s.put("kind", kind);
        s.put("origin", "local");
        s.put("title", title);
        s.put("detail", detail);
        if (suggestedName != null) s.put("suggestedName", suggestedName);
        return s;
    }

    // ===================== 模型补充 =====================

    private Map<String, Object> askModel(List<String> columns, List<List<String>> rows) throws Exception {
        String system = String.join("\n",
                "你是数据接入助手。用户要从文件导入数据到 Flink SQL 任务编排平台。",
                "只依据给出的列名、类型与前几行样本给建议；不要编造看不到的字段。",
                "建议最多 4 条，每条一句话，具体可执行（类型调整、字段改名、下游落库方式）。",
                "只输出 JSON：{\"summary\":\"一句话总评\",\"suggestions\":[{\"title\":\"\",\"detail\":\"\"}],",
                "\"renames\":{\"原字段名\":\"新字段名\"}}。没有改名就返回空对象。"
        );
        StringBuilder user = new StringBuilder("列名与类型：\n");
        for (String c : columns) user.append("- ").append(c).append('\n');
        user.append("\n前 ").append(Math.min(LLM_SAMPLE_ROWS, rows.size())).append(" 行样本：\n");
        for (int i = 0; i < Math.min(LLM_SAMPLE_ROWS, rows.size()); i++) {
            user.append(rows.get(i)).append('\n');
        }

        JsonNode resp = llmClient.chatJson(system, user.toString(), null);
        if (resp == null || resp.isMissingNode()) return null;

        Map<String, Object> advice = new LinkedHashMap<>();
        advice.put("summary", resp.path("summary").asText(null));
        List<Map<String, Object>> list = new ArrayList<>();
        for (JsonNode item : resp.path("suggestions")) {
            String title = item.path("title").asText("").trim();
            if (title.isEmpty()) continue;
            Map<String, Object> s = new LinkedHashMap<>();
            s.put("kind", "ai-advice");
            s.put("origin", "ai");
            s.put("title", title);
            s.put("detail", item.path("detail").asText(""));
            list.add(s);
        }
        advice.put("suggestions", list);
        Map<String, String> renames = new LinkedHashMap<>();
        JsonNode renameNode = resp.path("renames");
        if (renameNode.isObject()) {
            renameNode.fields().forEachRemaining(e -> renames.put(e.getKey(), e.getValue().asText()));
        }
        advice.put("renames", renames);
        return advice;
    }

    // ===================== 小工具 =====================

    private List<String> columnValues(List<List<String>> rows, int index) {
        List<String> values = new ArrayList<>();
        for (List<String> row : rows) {
            values.add(index < row.size() && row.get(index) != null ? row.get(index) : "");
        }
        return values;
    }

    private List<String> asStringList(Object raw) {
        List<String> out = new ArrayList<>();
        if (raw instanceof List<?> list) {
            for (Object item : list) if (item != null) out.add(String.valueOf(item));
        }
        return out;
    }

    private List<List<String>> asRows(Object raw) {
        List<List<String>> out = new ArrayList<>();
        if (raw instanceof List<?> list) {
            for (Object item : list) {
                List<String> row = new ArrayList<>();
                if (item instanceof List<?> cells) {
                    for (Object cell : cells) row.add(cell == null ? "" : String.valueOf(cell));
                }
                out.add(row);
            }
        }
        return out;
    }

    private String extractQuoted(String text) {
        int start = text.indexOf('「');
        int end = text.indexOf('」');
        return start >= 0 && end > start ? text.substring(start + 1, end) : "";
    }

    private boolean looksInteger(List<String> values) {
        boolean any = false;
        for (String v : values) {
            String t = v == null ? "" : v.trim();
            if (t.isEmpty()) continue;
            any = true;
            if (!t.matches("[+-]?\\d+")) return false;
        }
        return any;
    }

    private boolean looksNumeric(List<String> values) {
        boolean any = false;
        for (String v : values) {
            String t = v == null ? "" : v.trim();
            if (t.isEmpty()) continue;
            any = true;
            if (!t.matches("[+-]?\\d+(\\.\\d+)?")) return false;
        }
        return any;
    }

    private boolean looksDate(List<String> values) {
        return values.stream().filter(v -> v != null && !v.trim().isEmpty())
                .allMatch(v -> v.trim().matches("\\d{4}[-/]\\d{1,2}[-/]\\d{1,2}"));
    }

    private boolean looksTimestamp(List<String> values) {
        return values.stream().filter(v -> v != null && !v.trim().isEmpty())
                .allMatch(v -> v.trim().matches("\\d{4}-\\d{2}-\\d{2}[ T]\\d{2}:\\d{2}(:\\d{2})?"));
    }

    private String sample(List<String> values, int n) {
        List<String> picked = values.stream().filter(v -> v != null && !v.trim().isEmpty())
                .limit(n).map(String::trim).toList();
        return picked.isEmpty() ? "（无）" : String.join(" / ", picked);
    }

    /** 字段名规范化：非字母数字下划线中文 -> 下划线，首字符是数字则加前缀 */
    private String normalizeName(String name) {
        String s = name.trim().replaceAll("[^A-Za-z0-9_\\u4e00-\\u9fa5]+", "_");
        s = s.replaceAll("_+", "_").replaceAll("^_|_$", "");
        if (s.isEmpty()) s = "col";
        if (Character.isDigit(s.charAt(0))) s = "c_" + s;
        return s.toLowerCase(Locale.ROOT);
    }

    /** 供测试/调试：本地规则的空实现占位（保持 API 稳定） */
    List<Map<String, Object>> localOnly(List<String> columns, List<List<String>> rows) {
        return localSuggestions(columns, rows == null ? Collections.emptyList() : rows);
    }
}
