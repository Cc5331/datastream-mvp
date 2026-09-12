package com.datastream.mvp.ai;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.crypto.Cipher;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.SecretKeySpec;
import java.io.File;
import java.security.SecureRandom;
import java.security.spec.KeySpec;
import java.util.Base64;
import java.net.InetSocketAddress;
import java.net.ProxySelector;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * LLM 客户端（OpenAI ChatGPT / DeepSeek，兼容 Chat Completions）。
 * 密钥只从环境变量注入，不落库、不进代码仓库。
 */
@Slf4j
@Component
public class LlmClient {

    private final ObjectMapper objectMapper;
    private final HttpClient httpClient;

    @Value("${app.ai.provider:openai}")
    private String provider;

    @Value("${app.ai.api-key:}")
    private String apiKey;

    @Value("${app.ai.base-url:https://api.openai.com/v1}")
    private String baseUrl;

    @Value("${app.ai.model:gpt-4.1-mini}")
    private String model;

    @Value("${app.ai.allowed-models:gpt-4.1-mini,gpt-4.1,gpt-4o-mini}")
    private String allowedModels;

    @Value("${app.ai.timeout-ms:60000}")
    private long timeoutMs;

    @Value("${app.ai.provider-file:data/ai_providers.json}")
    private String providerFile;

    private static final SecureRandom SECURE_RANDOM = new SecureRandom();
    private static final String CIPHER = "AES/GCM/NoPadding";
    private static final int GCM_TAG_BITS = 128;
    private static final int SALT_LEN_BYTES = 16;
    private static final int IV_LEN_BYTES = 12;

    private final Map<String, ProviderProfile> profiles = new ConcurrentHashMap<>();
    private volatile String activeProfileId;
    private volatile String activeWireApi = "chat_completions";
    private volatile String activeReasoningEffort = "medium";
    private volatile boolean activeStoreResponses = false;

    public record ProviderInfo(String id, String name, String provider, String baseUrl,
                               String defaultModel, List<String> models, String wireApi,
                               String reasoningEffort, boolean storeResponses,
                               boolean apiKeyConfigured, boolean active) {}

    private static class ProviderProfile {
        public String id;
        public String name;
        public String provider;
        public String baseUrl;
        public String apiKey;
        public String defaultModel;
        public List<String> models;
        public String wireApi;
        public String reasoningEffort;
        public boolean storeResponses;
    }

    /** 落盘物：API Key 存加密后的密文 */
    private static class PersistedProfile {
        public String id;
        public String name;
        public String provider;
        public String baseUrl;
        public String encryptedApiKey;
        public String defaultModel;
        public List<String> models;
        public String wireApi;
        public String reasoningEffort;
        public boolean storeResponses;
        public boolean active;
    }

    public LlmClient(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
        HttpClient.Builder builder = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(15));
        String proxy = firstNonBlank(System.getenv("HTTPS_PROXY"), System.getenv("https_proxy"),
                System.getenv("HTTP_PROXY"), System.getenv("http_proxy"));
        if (proxy != null) {
            try {
                URI proxyUri = URI.create(proxy);
                if (proxyUri.getHost() != null && proxyUri.getPort() > 0) {
                    builder.proxy(ProxySelector.of(new InetSocketAddress(proxyUri.getHost(), proxyUri.getPort())));
                    log.info("LLM HTTP client using proxy {}:{}", proxyUri.getHost(), proxyUri.getPort());
                }
            } catch (Exception e) {
                log.warn("Ignoring invalid AI proxy configuration: {}", e.getMessage());
            }
        }
        this.httpClient = builder.build();
    }

    @PostConstruct
    void initializeDefaultProfile() {
        ProviderProfile initial = new ProviderProfile();
        initial.id = "default";
        initial.name = providerName().equals("deepseek") ? "DeepSeek" : "默认服务商";
        initial.provider = providerName();
        initial.baseUrl = baseUrl;
        initial.apiKey = apiKey;
        initial.defaultModel = model;
        initial.models = availableModels();
        initial.wireApi = "chat_completions";
        initial.reasoningEffort = "medium";
        initial.storeResponses = false;
        profiles.put(initial.id, initial);
        activeProfileId = initial.id;

        List<PersistedProfile> saved = loadPersisted();
        if (saved != null && !saved.isEmpty()) {
            log.info("Loading {} persisted AI provider profile(s)", saved.size());
            String activeId = null;
            for (PersistedProfile p : saved) {
                ProviderProfile profile = new ProviderProfile();
                profile.id = p.id;
                profile.name = p.name;
                profile.provider = p.provider;
                profile.baseUrl = p.baseUrl;
                profile.apiKey = p.encryptedApiKey == null ? "" : decrypt(p.encryptedApiKey);
                profile.defaultModel = p.defaultModel;
                profile.models = p.models == null ? List.of() : new ArrayList<>(p.models);
                profile.wireApi = normalizeWireApi(p.wireApi);
                profile.reasoningEffort = p.reasoningEffort == null ? "medium" : p.reasoningEffort;
                profile.storeResponses = p.storeResponses;
                if (profile.id != null && !"default".equals(profile.id)) {
                    profiles.put(profile.id, profile);
                    if (p.active) activeId = profile.id;
                }
            }
            if (activeId != null && profiles.containsKey(activeId)) {
                activateProvider(activeId);
            }
        }
    }

    private File providerFile() {
        return new File(providerFile);
    }

    private synchronized void persistProfiles() {
        try {
            List<PersistedProfile> list = new ArrayList<>();
            for (ProviderProfile p : profiles.values()) {
                if ("default".equals(p.id)) continue; // 默认服务商来自环境变量，不落盘
                PersistedProfile pp = new PersistedProfile();
                pp.id = p.id;
                pp.name = p.name;
                pp.provider = p.provider;
                pp.baseUrl = p.baseUrl;
                pp.encryptedApiKey = p.apiKey == null || p.apiKey.isBlank() ? "" : encrypt(p.apiKey);
                pp.defaultModel = p.defaultModel;
                pp.models = p.models;
                pp.wireApi = p.wireApi;
                pp.reasoningEffort = p.reasoningEffort;
                pp.storeResponses = p.storeResponses;
                pp.active = p.id.equals(activeProfileId);
                list.add(pp);
            }
            File file = providerFile();
            if (file.getParentFile() != null) {
                file.getParentFile().mkdirs();
            }
            objectMapper.writerWithDefaultPrettyPrinter().writeValue(file, list);
        } catch (Exception e) {
            log.warn("Failed to persist AI provider config: {}", e.getMessage());
        }
    }

    private List<PersistedProfile> loadPersisted() {
        try {
            File file = providerFile();
            if (!file.isFile()) return List.of();
            return objectMapper.readValue(file, objectMapper.getTypeFactory()
                    .constructCollectionType(List.class, PersistedProfile.class));
        } catch (Exception e) {
            log.warn("Failed to load persisted AI provider config: {}", e.getMessage());
            return List.of();
        }
    }

    private String encryptionSecret() {
        String secret = System.getenv("AI_CONFIG_ENCRYPTION_KEY");
        if (secret == null || secret.isBlank()) {
            secret = "dataflow-mvp-ai-provider-dev-key-2026";
        }
        return secret;
    }

    private String encrypt(String plain) {
        try {
            byte[] salt = new byte[SALT_LEN_BYTES];
            SECURE_RANDOM.nextBytes(salt);
            byte[] iv = new byte[IV_LEN_BYTES];
            SECURE_RANDOM.nextBytes(iv);
            SecretKeySpec key = deriveKey(encryptionSecret(), salt);
            Cipher cipher = Cipher.getInstance(CIPHER);
            cipher.init(Cipher.ENCRYPT_MODE, key, new GCMParameterSpec(GCM_TAG_BITS, iv));
            byte[] cipherText = cipher.doFinal(plain.getBytes(java.nio.charset.StandardCharsets.UTF_8));
            byte[] out = new byte[salt.length + iv.length + cipherText.length];
            System.arraycopy(salt, 0, out, 0, salt.length);
            System.arraycopy(iv, 0, out, salt.length, iv.length);
            System.arraycopy(cipherText, 0, out, salt.length + iv.length, cipherText.length);
            return Base64.getEncoder().encodeToString(out);
        } catch (Exception e) {
            throw new IllegalStateException("加密 AI API Key 失败: " + e.getMessage(), e);
        }
    }

    private String decrypt(String encoded) {
        try {
            byte[] in = Base64.getDecoder().decode(encoded);
            byte[] salt = new byte[SALT_LEN_BYTES];
            byte[] iv = new byte[IV_LEN_BYTES];
            System.arraycopy(in, 0, salt, 0, salt.length);
            System.arraycopy(in, salt.length, iv, 0, iv.length);
            byte[] cipherText = new byte[in.length - salt.length - iv.length];
            System.arraycopy(in, salt.length + iv.length, cipherText, 0, cipherText.length);
            SecretKeySpec key = deriveKey(encryptionSecret(), salt);
            Cipher cipher = Cipher.getInstance(CIPHER);
            cipher.init(Cipher.DECRYPT_MODE, key, new GCMParameterSpec(GCM_TAG_BITS, iv));
            return new String(cipher.doFinal(cipherText), java.nio.charset.StandardCharsets.UTF_8);
        } catch (Exception e) {
            log.warn("Failed to decrypt AI API Key (config may be stale): {}", e.getMessage());
            return "";
        }
    }

    private SecretKeySpec deriveKey(String secret, byte[] salt) throws Exception {
        PBEKeySpec spec = new PBEKeySpec(secret.toCharArray(), salt, 65536, 256);
        SecretKeyFactory factory = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA256");
        byte[] keyBytes = factory.generateSecret(spec).getEncoded();
        return new SecretKeySpec(keyBytes, "AES");
    }

    private String firstNonBlank(String... values) {
        for (String value : values) {
            if (value != null && !value.isBlank()) {
                return value.trim();
            }
        }
        return null;
    }

    public boolean isConfigured() {
        return apiKey != null && !apiKey.isBlank();
    }

    public String providerName() {
        return provider == null || provider.isBlank() ? "openai" : provider.trim().toLowerCase();
    }

    public List<String> availableModels() {
        List<String> models = new ArrayList<>(Arrays.stream(allowedModels.split(","))
                .map(String::trim)
                .filter(value -> !value.isBlank())
                .distinct()
                .toList());
        if (model != null && !model.isBlank() && !models.contains(model)) {
            models.add(0, model);
        }
        return models;
    }

    public String defaultModel() {
        return model;
    }

    public String baseUrl() {
        return baseUrl;
    }

    public List<ProviderInfo> providerProfiles() {
        return profiles.values().stream()
                .map(profile -> new ProviderInfo(profile.id, profile.name, profile.provider, profile.baseUrl,
                        profile.defaultModel, List.copyOf(profile.models), profile.wireApi,
                        profile.reasoningEffort, profile.storeResponses,
                        profile.apiKey != null && !profile.apiKey.isBlank(), profile.id.equals(activeProfileId)))
                .sorted((left, right) -> Boolean.compare(right.active(), left.active()))
                .toList();
    }

    public synchronized ProviderInfo saveProvider(String id, String name, String provider, String baseUrl,
                                                  String apiKey, String defaultModel, List<String> models,
                                                  String wireApi, String reasoningEffort, boolean storeResponses) {
        String profileId = id == null || id.isBlank() ? UUID.randomUUID().toString() : id.trim();
        ProviderProfile existing = profiles.get(profileId);
        String profileKey = apiKey == null || apiKey.isBlank()
                ? existing == null ? "" : existing.apiKey
                : apiKey.trim();
        updateRuntimeConfig(provider, baseUrl, profileKey, defaultModel, models);
        this.apiKey = profileKey;
        ProviderProfile profile = new ProviderProfile();
        profile.id = profileId;
        profile.name = name == null || name.isBlank() ? providerName() : name.trim();
        profile.provider = providerName();
        profile.baseUrl = this.baseUrl;
        profile.apiKey = this.apiKey;
        profile.defaultModel = this.model;
        profile.models = availableModels();
        profile.wireApi = normalizeWireApi(wireApi);
        profile.reasoningEffort = reasoningEffort == null || reasoningEffort.isBlank() ? "medium" : reasoningEffort.trim();
        profile.storeResponses = storeResponses;
        profiles.put(profileId, profile);
        activeProfileId = profileId;
        persistProfiles();
        return providerProfiles().stream().filter(item -> item.id().equals(profileId)).findFirst().orElseThrow();
    }

    public synchronized ProviderInfo activateProvider(String id) {
        ProviderProfile profile = profiles.get(id);
        if (profile == null) {
            throw new IllegalArgumentException("服务商不存在: " + id);
        }
        this.provider = profile.provider;
        this.baseUrl = profile.baseUrl;
        this.apiKey = profile.apiKey;
        this.model = profile.defaultModel;
        this.allowedModels = String.join(",", profile.models);
        this.activeProfileId = profile.id;
        applyProtocolConfig(profile.wireApi, profile.reasoningEffort, profile.storeResponses);
        return providerProfiles().stream().filter(item -> item.id().equals(id)).findFirst().orElseThrow();
    }

    private String normalizeWireApi(String wireApi) {
        if (wireApi != null && "responses".equalsIgnoreCase(wireApi.trim())) {
            return "responses";
        }
        return "chat_completions";
    }

    public synchronized void deleteProvider(String id) {
        if (id == null || id.equals(activeProfileId)) {
            throw new IllegalArgumentException("不能删除当前正在使用的服务商");
        }
        if (profiles.remove(id) == null) {
            throw new IllegalArgumentException("服务商不存在: " + id);
        }
        persistProfiles();
    }

    public synchronized void updateRuntimeConfig(String provider, String baseUrl, String apiKey,
                                                 String defaultModel, List<String> models) {
        if (baseUrl == null || baseUrl.isBlank()) {
            throw new IllegalArgumentException("Base URL 不能为空");
        }
        URI uri = URI.create(baseUrl.trim());
        if (!"http".equalsIgnoreCase(uri.getScheme()) && !"https".equalsIgnoreCase(uri.getScheme())) {
            throw new IllegalArgumentException("Base URL 仅支持 http 或 https");
        }
        if (uri.getHost() == null) {
            throw new IllegalArgumentException("Base URL 格式不正确");
        }
        List<String> normalized = models == null ? List.of() : models.stream()
                .map(String::trim)
                .filter(value -> !value.isBlank())
                .distinct()
                .toList();
        if (normalized.isEmpty()) {
            throw new IllegalArgumentException("至少配置一个可选模型");
        }
        String selectedDefault = defaultModel == null ? "" : defaultModel.trim();
        if (!normalized.contains(selectedDefault)) {
            throw new IllegalArgumentException("默认模型必须包含在可选模型中");
        }
        this.provider = provider == null || provider.isBlank() ? "openai" : provider.trim();
        this.baseUrl = baseUrl.trim().replaceAll("/+$", "");
        this.model = selectedDefault;
        this.allowedModels = String.join(",", normalized);
        if (apiKey != null && !apiKey.isBlank()) {
            this.apiKey = apiKey.trim();
        }
    }

    public JsonNode testConnection(String requestedModel) {
        return chatJson("你是 API 连通性测试助手，只返回 JSON。", "请返回 {\"ok\":true,\"message\":\"连接成功\"}", requestedModel);
    }

    public JsonNode testProviderConnection(String id, String requestedModel) {
        ProviderProfile profile = profiles.get(id);
        if (profile == null) {
            throw new IllegalArgumentException("服务商不存在: " + id);
        }
        String selectedModel = requestedModel == null || requestedModel.isBlank()
                ? profile.defaultModel : requestedModel.trim();
        return chatJsonWithConfig("你是 API 连通性测试助手，只返回 JSON。",
                "请返回 {\"ok\":true,\"message\":\"连接成功\"}", selectedModel,
                profile.baseUrl, profile.apiKey, profile.models,
                profile.wireApi, profile.reasoningEffort, profile.storeResponses);
    }

    /**
     * 调用 LLM 并要求返回 JSON 对象。
     */
    public JsonNode chatJson(String systemPrompt, String userContent) {
        return chatJson(systemPrompt, userContent, model);
    }

    public JsonNode chatJson(String systemPrompt, String userContent, String requestedModel) {
        String selectedModel = requestedModel == null || requestedModel.isBlank() ? model : requestedModel.trim();
        return chatJsonWithConfig(systemPrompt, userContent, selectedModel, baseUrl, apiKey, availableModels(),
                activeWireApi, activeReasoningEffort, activeStoreResponses);
    }

    private JsonNode chatJsonWithConfig(String systemPrompt, String userContent, String selectedModel,
                                        String requestBaseUrl, String requestApiKey, List<String> requestModels) {
        if (requestApiKey == null || requestApiKey.isBlank()) {
            throw new IllegalStateException("未配置 AI API Key；请先在服务商配置中填写 API Key");
        }
        if (!requestModels.contains(selectedModel)) {
            throw new IllegalArgumentException("不支持的 AI 模型: " + selectedModel);
        }
        try {
            ObjectNode payload = objectMapper.createObjectNode();
            payload.put("model", selectedModel);
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
                    .uri(URI.create(requestBaseUrl.trim().replaceAll("/+$", "") + "/chat/completions"))
                    .header("Content-Type", "application/json")
                    .header("Authorization", "Bearer " + requestApiKey)
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

    private void applyProtocolConfig(String wireApi, String reasoningEffort, boolean storeResponses) {
        this.activeWireApi = normalizeWireApi(wireApi);
        this.activeReasoningEffort = reasoningEffort == null || reasoningEffort.isBlank() ? "medium" : reasoningEffort.trim();
        this.activeStoreResponses = storeResponses;
    }

    private JsonNode chatJsonWithConfig(String systemPrompt, String userContent, String selectedModel,
                                        String requestBaseUrl, String requestApiKey, List<String> requestModels,
                                        String wireApi, String reasoningEffort, boolean storeResponses) {
        if (requestApiKey == null || requestApiKey.isBlank()) {
            throw new IllegalStateException("未配置 AI API Key；请先在服务商配置中填写 API Key");
        }
        if (!requestModels.contains(selectedModel)) {
            throw new IllegalArgumentException("不支持的 AI 模型: " + selectedModel);
        }
        try {
            String base = requestBaseUrl.trim().replaceAll("/+$", "");
            String resBody;
            if ("responses".equalsIgnoreCase(wireApi)) {
                ObjectNode payload = createResponsesPayload(
                        systemPrompt, userContent, selectedModel, reasoningEffort, storeResponses);

                HttpRequest request = HttpRequest.newBuilder()
                        .uri(URI.create(base + "/responses"))
                        .header("Content-Type", "application/json")
                        .header("Authorization", "Bearer " + requestApiKey)
                        .timeout(Duration.ofMillis(timeoutMs))
                        .POST(HttpRequest.BodyPublishers.ofString(objectMapper.writeValueAsString(payload)))
                        .build();

                HttpResponse<String> resp = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
                if (resp.statusCode() != 200) {
                    throw new IllegalStateException("LLM 接口调用失败 (HTTP " + resp.statusCode() + "): "
                            + truncate(resp.body(), 500));
                }
                JsonNode root = objectMapper.readTree(resp.body());
                StringBuilder textBuilder = new StringBuilder();
                JsonNode output = root.path("output");
                if (output.isArray()) {
                    for (JsonNode item : output) {
                        if ("message".equals(item.path("type").asText())) {
                            JsonNode contentArray = item.path("content");
                            if (contentArray.isArray()) {
                                for (JsonNode part : contentArray) {
                                    if ("output_text".equals(part.path("type").asText())) {
                                        textBuilder.append(part.path("text").asText());
                                    }
                                }
                            }
                        }
                    }
                }
                String text = textBuilder.toString().trim();
                if (text.isEmpty()) {
                    throw new IllegalStateException("LLM 返回为空: " + truncate(resp.body(), 500));
                }
                return parseJson(text);
            }

            ObjectNode payload = objectMapper.createObjectNode();
            payload.put("model", selectedModel);
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
                    .uri(URI.create(base + "/chat/completions"))
                    .header("Content-Type", "application/json")
                    .header("Authorization", "Bearer " + requestApiKey)
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
            return parseJson(content.asText().trim());
        } catch (IllegalStateException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalStateException("LLM 调用失败: " + e.getMessage(), e);
        }
    }

    ObjectNode createResponsesPayload(String systemPrompt, String userContent, String selectedModel,
                                      String reasoningEffort, boolean storeResponses) {
        ObjectNode payload = objectMapper.createObjectNode();
        payload.put("model", selectedModel);
        payload.put("store", storeResponses);
        payload.put("input", userContent);
        payload.put("instructions", systemPrompt);
        payload.set("reasoning", objectMapper.createObjectNode().put("effort", reasoningEffort));
        return payload;
    }

    JsonNode parseJson(String text) {
        String raw = text;
        // 兼容返回内容中包裹了代码块的情况
        if (raw.startsWith("\u0060\u0060\u0060")) {
            raw = raw.replaceAll("^\\s*\u0060\u0060\u0060(?:json)?\\s*", "").replaceAll("\\s*\u0060\u0060\u0060\\s*$", "");
        }
        try {
            return objectMapper.readTree(raw);
        } catch (Exception e) {
            // 模型可能在思考文字之外混入目标 JSON，尝试提取第一个完整 JSON 对象
            JsonNode extracted = extractJsonObject(raw);
            if (extracted == null) {
                throw new IllegalStateException("LLM 返回内容不是有效 JSON: " + truncate(text, 200), e);
            }
            return extracted;
        }
    }

    private JsonNode extractJsonObject(String text) {
        int start = 0;
        while ((start = text.indexOf('{', start)) >= 0) {
            int depth = 0;
            boolean inString = false;
            boolean escaped = false;
            for (int i = start; i < text.length(); i++) {
                char c = text.charAt(i);
                if (inString) {
                    if (escaped) {
                        escaped = false;
                    } else if (c == '\\') {
                        escaped = true;
                    } else if (c == '"') {
                        inString = false;
                    }
                    continue;
                }
                if (c == '"') {
                    inString = true;
                } else if (c == '{') {
                    depth++;
                } else if (c == '}') {
                    depth--;
                    if (depth == 0) {
                        String candidate = text.substring(start, i + 1);
                        try {
                            return objectMapper.readTree(candidate);
                        } catch (Exception ignored) {
                            // 该候选不是完整 JSON，继续找下一处 '{'
                        }
                        break;
                    }
                }
            }
            start++;
        }
        return null;
    }

    private String truncate(String s, int max) {
        if (s == null) return "";
        return s.length() > max ? s.substring(0, max) : s;
    }
}