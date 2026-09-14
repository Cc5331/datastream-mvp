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

    @Test
    void parseChatCompletion_readsContentForNormalReply() {
        JsonNode root = readJson("{\"choices\":[{\"finish_reason\":\"stop\",\"message\":"
                + "{\"role\":\"assistant\",\"content\":\"{\\\"rootCause\\\":\\\"x\\\"}\"}}]}");

        JsonNode result = client.parseChatCompletion(root, "deepseek-v4-flash");

        assertEquals("x", result.path("rootCause").asText());
    }

    @Test
    void parseChatCompletion_reportsTruncatedReasoningWhenContentEmpty() {
        // 推理模型：推理吃掉全部 token 预算后 content 为空，finish_reason=length。
        // 此时必须给出可操作的提示，而不是笼统的「LLM 返回为空」
        JsonNode root = readJson("{\"choices\":[{\"finish_reason\":\"length\",\"message\":"
                + "{\"role\":\"assistant\",\"content\":\"\",\"reasoning_content\":\"We need answer only JSON...\"}}],"
                + "\"usage\":{\"completion_tokens\":4000,"
                + "\"completion_tokens_details\":{\"reasoning_tokens\":3900}}}");

        IllegalStateException ex = org.junit.jupiter.api.Assertions.assertThrows(
                IllegalStateException.class, () -> client.parseChatCompletion(root, "deepseek-v4-flash"));

        org.junit.jupiter.api.Assertions.assertTrue(ex.getMessage().contains("token 上限截断"), ex.getMessage());
        org.junit.jupiter.api.Assertions.assertTrue(ex.getMessage().contains("reasoning_tokens=3900"), ex.getMessage());
        org.junit.jupiter.api.Assertions.assertTrue(ex.getMessage().contains("app.ai.max-tokens"), ex.getMessage());
    }

    @Test
    void parseChatCompletion_reportsReasoningOnlyReply() {
        JsonNode root = readJson("{\"choices\":[{\"finish_reason\":\"stop\",\"message\":"
                + "{\"role\":\"assistant\",\"content\":\"   \",\"reasoning_content\":\"thinking...\"}}],"
                + "\"usage\":{\"completion_tokens\":120}}");

        IllegalStateException ex = org.junit.jupiter.api.Assertions.assertThrows(
                IllegalStateException.class, () -> client.parseChatCompletion(root, "deepseek-v4-flash"));

        org.junit.jupiter.api.Assertions.assertTrue(ex.getMessage().contains("推理内容"), ex.getMessage());
    }

    private JsonNode readJson(String json) {
        try { return mapper.readTree(json); }
        catch (Exception e) { throw new IllegalStateException(e); }
    }
}
