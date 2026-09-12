package com.datastream.mvp.ai;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class LlmClientParseJsonTest {

    private final ObjectMapper mapper = new ObjectMapper();
    private final LlmClient client = new LlmClient(mapper);

    @Test
    void createResponsesPayload_serializesInstructionsAsString() {
        JsonNode payload = client.createResponsesPayload(
                "只返回 JSON", "生成数据流", "deepseek-v4-flash", "medium", false);

        assertEquals("只返回 JSON", payload.path("instructions").asText());
        assertEquals(true, payload.path("instructions").isTextual());
        assertEquals("生成数据流", payload.path("input").asText());
        assertEquals("medium", payload.path("reasoning").path("effort").asText());
    }

    @Test
    void parseJson_handlesPureJson() {
        JsonNode result = client.parseJson("{\"jobName\":\"demo\",\"nodes\":[]}");
        assertEquals("demo", result.path("jobName").asText());
    }

    @Test
    void parseJson_extractsJsonFromMixedReasoningText() {
        String mixed = "我先看下仓库里已有的 DAG 结构和相关约定。我并行看几个关键点：DAG 样例、CSV 读取节点。"
                + "{ \"dag_id\": \"sales_150k_csv_to_students1\", \"schedule\": \"@once\" }"
                + " 后面还有一些说明。";
        JsonNode result = client.parseJson(mixed);
        assertNotNull(result);
        assertEquals("sales_150k_csv_to_students1", result.path("dag_id").asText());
    }

    @Test
    void parseJson_extractsFromCodeFence() {
        String fenced = "```json\n{\"nodes\":[{\"id\":\"n1\"}]}\n```";
        JsonNode result = client.parseJson(fenced);
        assertEquals("n1", result.path("nodes").path(0).path("id").asText());
    }
}
