package com.datastream.mvp.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Kafka 字段推断回归：从抽样消息推出 kafka_input 的 fieldsConfig。
 * 只测纯逻辑部分（不连 Kafka）——写错字段名会一路放行到 Calcite 才报错（红线 35）。
 */
class KafkaSchemaInferenceServiceTest {

    private final KafkaSchemaInferenceService service =
            new KafkaSchemaInferenceService(new ObjectMapper(), null);

    @Test
    @SuppressWarnings("unchecked")
    void infersFieldsFromJsonSamples() {
        Map<String, Object> result = service.buildResult("orders", List.of(
                "{\"sale_id\":\"S1000\",\"region\":\"华北\",\"amount\":\"2716.16\",\"quantity\":\"2\",\"channel\":\"线上\"}",
                "{\"sale_id\":\"S1001\",\"region\":\"华南\",\"amount\":\"18.00\",\"quantity\":\"5\",\"channel\":\"线下\"}",
                "{\"sale_id\":\"S1002\",\"region\":\"华东\",\"amount\":\"9.50\",\"quantity\":\"1\",\"channel\":\"线上\"}"
        ));

        List<Map<String, String>> columns = (List<Map<String, String>>) result.get("columns");
        assertEquals(List.of("sale_id", "region", "amount", "quantity", "channel"),
                columns.stream().map(c -> c.get("name")).toList());
        // 金额样本是纯数字字符串 -> DOUBLE；数量 -> INT；其余 STRING
        assertEquals(List.of("STRING", "STRING", "DOUBLE", "INT", "STRING"),
                columns.stream().map(c -> c.get("type")).toList());
        String fieldsConfig = String.valueOf(result.get("fieldsConfig"));
        assertTrue(fieldsConfig.startsWith("[{\"name\":\"sale_id\""), fieldsConfig);
        assertTrue(fieldsConfig.contains("{\"name\":\"amount\",\"type\":\"DOUBLE\"}"), fieldsConfig);
        // 生成的 fieldsConfig 必须是合法 JSON（翻译层直接 JSON.parse 它）
        assertDoesNotThrow(() -> new ObjectMapper().readTree(fieldsConfig));
        assertEquals(3, result.get("sampled"));
        assertTrue(((List<String>) result.get("warnings")).isEmpty(), "样本充足且都是 JSON，不应有告警");
    }

    @Test
    @SuppressWarnings("unchecked")
    void emptyValueDowngradesColumnToString() {
        Map<String, Object> result = service.buildResult("t", List.of(
                "{\"id\":\"0001\",\"amount\":\"12.5\"}",
                "{\"id\":\"0002\",\"amount\":\"\"}",
                "{\"id\":\"0003\",\"amount\":\"7\"}"
        ));
        List<Map<String, String>> columns = (List<Map<String, String>>) result.get("columns");
        // 含空值 -> STRING（避免 Flink 解析失败变 null）
        assertEquals("STRING", columns.get(1).get("type"));
    }

    @Test
    @SuppressWarnings("unchecked")
    void nonJsonMessagesFallBackToSingleValueColumn() {
        Map<String, Object> result = service.buildResult("plain", List.of("hello", "world", "again"));
        List<Map<String, String>> columns = (List<Map<String, String>>) result.get("columns");
        assertEquals(1, columns.size());
        assertEquals("value", columns.get(0).get("name"));
        assertEquals("STRING", columns.get(0).get("type"));
        assertTrue(((List<String>) result.get("warnings")).stream()
                .anyMatch(w -> w.contains("不是 JSON 对象")), result.get("warnings").toString());
    }

    @Test
    @SuppressWarnings("unchecked")
    void nestedObjectsBecomesStringColumnsAndBadLinesAreSkipped() {
        Map<String, Object> result = service.buildResult("t", List.of(
                "{\"id\":\"1\",\"extra\":{\"a\":1}}",
                "not-a-json-line",
                "{\"id\":\"2\",\"extra\":{\"a\":2}}"
        ));
        List<Map<String, String>> columns = (List<Map<String, String>>) result.get("columns");
        assertEquals(List.of("id", "extra"), columns.stream().map(c -> c.get("name")).toList());
        assertEquals("STRING", columns.get(0).get("type"), "有坏行 -> 该列样本含空 -> STRING");
        assertEquals("STRING", columns.get(1).get("type"), "嵌套对象按 JSON 文本存 STRING");
        assertTrue(((List<String>) result.get("warnings")).stream()
                .anyMatch(w -> w.contains("不是 JSON 对象")), result.get("warnings").toString());
    }
}
