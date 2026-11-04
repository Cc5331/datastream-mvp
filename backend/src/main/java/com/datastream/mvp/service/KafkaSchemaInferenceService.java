package com.datastream.mvp.service;

import com.datastream.mvp.util.DataTypeInference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Kafka 消息字段推断：抽样 topic 的真实消息，推出可直接粘进 `kafka_input.fieldsConfig` 的字段定义。
 *
 * <p>为什么需要：`kafka_input` 的字段定义必须与消息里的 JSON 键一致，写错一个名字会一路放行到
 * Flink 的 `CREATE TEMPORARY VIEW` 阶段才被 Calcite 拦下（`Column 'id' not found in any table`），
 * 而 SQL Gateway 在这种校验失败时**不回传根因**（见 DEVELOPMENT 红线 35）。与其让用户手抄消息键名，
 * 不如抽 20 条样本自动推断。</p>
 *
 * <p>口径：值统一按 UTF-8 字符串推断类型（复用 {@link DataTypeInference}，空值退化为 STRING）；
 * 嵌套对象/数组按 JSON 文本存 STRING 列；非 JSON 消息退化为单个 `value` 列并给出告警。</p>
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class KafkaSchemaInferenceService {

    private final ObjectMapper objectMapper;
    private final KafkaMonitorService kafkaMonitorService;

    @Value("${app.kafka.schema-sample-size:20}")
    private int sampleSize;

    @Value("${app.kafka.schema-sample-timeout-ms:6000}")
    private long sampleTimeoutMs;

    /**
     * 抽样 topic 并推断字段定义。
     *
     * @param topic      目标 topic
     * @param sampleSize 覆盖默认抽样条数（1..100）
     */
    public Map<String, Object> infer(String topic, Integer sampleSize) {
        String normalizedTopic = topic == null ? "" : topic.trim();
        if (normalizedTopic.isEmpty()) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "topic 不能为空");
        }
        int limit = sampleSize == null || sampleSize <= 0 ? this.sampleSize : Math.min(sampleSize, 100);

        List<String> values = new ArrayList<>();
        List<String> errors = new ArrayList<>();
        try {
            kafkaMonitorService.streamTopic(normalizedTopic, true, limit, sampleTimeoutMs, msg -> {
                if (msg.containsKey("error")) {
                    errors.add(String.valueOf(msg.get("error")));
                    return;
                }
                Object value = msg.get("value");
                if (value != null) values.add(String.valueOf(value));
            });
        } catch (Exception e) {
            log.warn("Kafka schema sampling failed for {}: {}", normalizedTopic, e.getMessage());
            errors.add(e.getMessage());
        }

        if (values.isEmpty()) {
            String detail = errors.isEmpty() ? "没有抽到消息（topic 为空或 broker 不可达）" : String.join("; ", errors);
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST,
                    "无法从 topic「" + normalizedTopic + "」抽到消息：" + detail);
        }
        Map<String, Object> result = buildResult(normalizedTopic, values);
        if (!errors.isEmpty()) {
            @SuppressWarnings("unchecked")
            List<String> warnings = (List<String>) result.get("warnings");
            warnings.add("抽样过程有异常（不影响已抽到的样本）：" + String.join("; ", errors));
        }
        log.info("Kafka 字段推断: topic={} 样本 {} 条 -> {} 个字段", normalizedTopic, values.size(),
                result.get("columns") instanceof List<?> list ? list.size() : 0);
        return result;
    }

    /**
     * 纯逻辑部分（不碰 Kafka，便于单测）：从样本消息文本推出列与 fieldsConfig。
     * 包级可见，供 {@code KafkaSchemaInferenceServiceTest} 直接调用。
     */
    Map<String, Object> buildResult(String topic, List<String> rawValues) {
        Map<String, Object> result = new LinkedHashMap<>();
        List<String> warnings = new ArrayList<>();
        List<String> values = new ArrayList<>();
        for (String raw : rawValues) {
            if (raw != null && !raw.isBlank()) values.add(raw);
        }

        Set<String> keys = new LinkedHashSet<>();
        List<JsonNode> parsed = new ArrayList<>();
        int nonJson = 0;
        for (String value : values) {
            try {
                JsonNode node = objectMapper.readTree(value);
                parsed.add(node);
                if (node != null && node.isObject()) {
                    node.fieldNames().forEachRemaining(keys::add);
                } else {
                    nonJson++;
                }
            } catch (Exception e) {
                nonJson++;
                parsed.add(null);
            }
        }

        boolean jsonMessages = !keys.isEmpty();
        if (!jsonMessages) {
            // 纯文本/非 JSON：只能当单列 value 用（Kafka connector 的默认 value 列）
            keys.add("value");
            warnings.add("抽样消息不是 JSON 对象，已按单列 `value`（STRING）处理；"
                    + "如需拆分字段，请在链路上游先把消息转成 JSON");
        }

        List<Map<String, String>> columns = new ArrayList<>();
        for (String key : keys) {
            List<String> columnValues = new ArrayList<>();
            for (int i = 0; i < parsed.size(); i++) {
                JsonNode node = parsed.get(i);
                if (node == null) {
                    columnValues.add("");
                    continue;
                }
                if (!jsonMessages) {
                    columnValues.add(values.get(i));
                    continue;
                }
                JsonNode value = node.get(key);
                if (value == null || value.isNull()) columnValues.add("");
                else if (value.isValueNode()) columnValues.add(value.asText());
                else columnValues.add(value.toString());
            }
            Map<String, String> column = new LinkedHashMap<>();
            column.put("name", key);
            column.put("type", DataTypeInference.infer(columnValues));
            columns.add(column);
        }
        if (nonJson > 0 && jsonMessages) {
            warnings.add("有 " + nonJson + " 条消息不是 JSON 对象，已跳过（字段按其余消息推断）");
        }
        if (values.size() < 3) {
            warnings.add("有效样本只有 " + values.size() + " 条，字段可能不全（建议 topic 里再多攒些消息）");
        }

        result.put("topic", topic);
        result.put("sampled", values.size());
        result.put("columns", columns);
        result.put("fieldsConfig", toFieldsConfig(columns));
        result.put("warnings", warnings);
        return result;
    }

    private String toFieldsConfig(List<Map<String, String>> columns) {
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < columns.size(); i++) {
            if (i > 0) sb.append(",");
            sb.append("{\"name\":\"").append(escape(columns.get(i).get("name")))
              .append("\",\"type\":\"").append(columns.get(i).get("type")).append("\"}");
        }
        return sb.append("]").toString();
    }

    private String escape(String s) {
        return s == null ? "" : s.replace("\\", "\\\\").replace("\"", "\\\"");
    }
}
