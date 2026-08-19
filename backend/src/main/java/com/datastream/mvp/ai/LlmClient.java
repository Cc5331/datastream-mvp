package com.datastream.mvp.ai;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

/**
 * LLM 客户端（DeepSeek，OpenAI 兼容 Chat Completions）。
 * 密钥只从环境变量注入（DEEPSEEK_API_KEY），不落库、不进代码仓库。
 */
@Slf4j
@Component
public class LlmClient {

    private final ObjectMapper objectMapper;
    private final HttpClient httpClient;

    @Value("${app.ai.deepseek.api-key:}")
    private String apiKey;

    @Value("${app.ai.deepseek.base-url:https://api.deepseek.com}")
    private String baseUrl;

    @Value("${app.ai.deepseek.model:deepseek-chat}")
    private String model;

    @Value("${app.ai.deepseek.timeout-ms:60000}")
    private long timeoutMs;

    public LlmClient(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(15))
                .build();
    }

    public boolean isConfigured() {
        return apiKey != null && !apiKey.isBlank();
    }

    /**
     * 调用 LLM 并要求返回 JSON 对象。
     */
    public JsonNode chatJson(String systemPrompt, String userContent) {
        if (!isConfigured()) {
            throw new IllegalStateException("未配置 DEEPSEEK_API_KEY，请先在 .env 中配置后再使用 AI 功能");
        }
        try {
            ObjectNode payload = objectMapper.createObjectNode();
            payload.put("model", model);
            payload.put("temperature", 0.2);
            payload.put("max_tokens", 4000);
            payload.set("response_format", objectMapper.createObjectNode().put("type", "json_object"));
            ArrayNode messages = payload.putArray("messages");
            ObjectNode sys = messages.addObject();
            sys.put("role", "system");
            sys.put("content", systemPrompt);
            ObjectNode user = messages.addObject();
            user.put("role", "user");
            user.put("content", userContent);

            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(baseUrl.trim().replaceAll("/+$", "") + "/chat/completions"))
                    .header("Content-Type", "application/json")
                    .header("Authorization", "Bearer " + apiKey)
                    .timeout(Duration.ofMillis(timeoutMs))
                    .POST(HttpRequest.BodyPublishers.ofString(objectMapper.writeValueAsString(payload)))
                    .build();

            HttpResponse<String> resp = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            if (resp.statusCode() != 200) {
                throw new IllegalStateException("LLM 接口调用失败 (HTTP " + resp.statusCode() + "): "
                        + truncate(resp.body(), 500));
            }
            JsonNode root = objectMapper.readTree(resp.body());
            JsonNode content = root.path("choices").path(0).path("message").path("content");
            if (content.isMissingNode() || content.asText().isBlank()) {
                throw new IllegalStateException("LLM 返回为空: " + truncate(resp.body(), 500));
            }
            String text = content.asText().trim();
            // 兼容返回内容中包裹了代码块的情况
            if (text.startsWith("\u0060\u0060\u0060")) {
                text = text.replaceAll("^\\s*\u0060\u0060\u0060(?:json)?\\s*", "").replaceAll("\\s*\u0060\u0060\u0060\\s*$", "");
            }
            return objectMapper.readTree(text);
        } catch (IllegalStateException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalStateException("LLM 调用失败: " + e.getMessage(), e);
        }
    }

    private String truncate(String s, int max) {
        if (s == null) return "";
        return s.length() > max ? s.substring(0, max) : s;
    }
}