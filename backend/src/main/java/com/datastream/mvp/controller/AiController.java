package com.datastream.mvp.controller;

import com.datastream.mvp.ai.AgentService;
import com.datastream.mvp.ai.LlmClient;
import com.datastream.mvp.audit.Audit;
import com.datastream.mvp.security.CurrentUser;
import com.datastream.mvp.security.SecurityUtils;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

import java.util.List;
import java.util.Map;

/**
 * AI 智能化层 REST API：
 * - POST /api/ai/nl2pipeline  自然语言 -> 可执行 DAG（生成 DRAFT，人工确认）
 * - POST /api/ai/diagnose/{jobId}  智能诊断（本地规则 + DeepSeek 归因）
 */
@RestController
@RequestMapping("/api/ai")
@RequiredArgsConstructor
public class AiController {

    private final AgentService agentService;
    private final LlmClient llmClient;
    private final com.datastream.mvp.service.JobService jobService;

    public record Nl2PipelineRequest(String prompt, String model) {}
    public record AiConfigRequest(String provider, String baseUrl, String apiKey,
                                  String defaultModel, List<String> models) {}
    public record AiTestRequest(String model) {}
    public record ProviderRequest(String id, String name, String provider, String baseUrl, String apiKey,
                                  String defaultModel, List<String> models,
                                  String wireApi, String reasoningEffort, Boolean storeResponses) {}

    @GetMapping("/models")
    public Map<String, Object> models() {
        List<String> available = llmClient.availableModels();
        return Map.of(
                "provider", llmClient.providerName(),
                "defaultModel", llmClient.defaultModel(),
                "models", available,
                "configured", llmClient.isConfigured());
    }

    @GetMapping("/config")
    @PreAuthorize("hasRole('ADMIN')")
    public Map<String, Object> config() {
        return Map.of(
                "provider", llmClient.providerName(),
                "baseUrl", llmClient.baseUrl(),
                "defaultModel", llmClient.defaultModel(),
                "models", llmClient.availableModels(),
                "apiKeyConfigured", llmClient.isConfigured());
    }

    @PutMapping("/config")
    @PreAuthorize("hasRole('ADMIN')")
    @Audit(action = "AI_CONFIG_UPDATE", targetType = "SYSTEM", detail = "'更新 AI API 运行时配置'")
    public Map<String, Object> updateConfig(@RequestBody AiConfigRequest request) {
        try {
            llmClient.updateRuntimeConfig(request.provider(), request.baseUrl(), request.apiKey(),
                    request.defaultModel(), request.models());
            return config();
        } catch (IllegalArgumentException e) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, e.getMessage());
        }
    }

    @PostMapping("/test")
    @PreAuthorize("hasRole('ADMIN')")
    @Audit(action = "AI_CONFIG_TEST", targetType = "SYSTEM", detail = "'测试 AI API 连接'")
    public Map<String, Object> testConnection(@RequestBody AiTestRequest request) {
        try {
            String selectedModel = request.model() == null || request.model().isBlank()
                    ? llmClient.defaultModel() : request.model();
            llmClient.testConnection(selectedModel);
            return Map.of("success", true, "message", "API 连接成功", "model", selectedModel);
        } catch (Exception e) {
            throw new ResponseStatusException(HttpStatus.BAD_GATEWAY, "API 连接失败: " + e.getMessage());
        }
    }

    @GetMapping("/providers")
    @PreAuthorize("hasRole('ADMIN')")
    public List<LlmClient.ProviderInfo> providers() {
        return llmClient.providerProfiles();
    }

    @PostMapping("/providers")
    @PreAuthorize("hasRole('ADMIN')")
    @Audit(action = "AI_PROVIDER_SAVE", targetType = "SYSTEM", detail = "'保存 AI 服务商 ' + #request.name()")
    public LlmClient.ProviderInfo saveProvider(@RequestBody ProviderRequest request) {
        try {
            return llmClient.saveProvider(request.id(), request.name(), request.provider(), request.baseUrl(),
                    request.apiKey(), request.defaultModel(), request.models(),
                    request.wireApi(), request.reasoningEffort(), Boolean.TRUE.equals(request.storeResponses()));
        } catch (IllegalArgumentException e) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, e.getMessage());
        }
    }

    @PostMapping("/providers/{id}/activate")
    @PreAuthorize("hasRole('ADMIN')")
    @Audit(action = "AI_PROVIDER_ACTIVATE", targetType = "SYSTEM", targetId = "#id")
    public LlmClient.ProviderInfo activateProvider(@PathVariable String id) {
        try {
            return llmClient.activateProvider(id);
        } catch (IllegalArgumentException e) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, e.getMessage());
        }
    }

    @PostMapping("/providers/{id}/test")
    @PreAuthorize("hasRole('ADMIN')")
    public Map<String, Object> testProvider(@PathVariable String id, @RequestBody AiTestRequest request) {
        try {
            LlmClient.ProviderInfo profile = llmClient.providerProfiles().stream()
                    .filter(item -> item.id().equals(id))
                    .findFirst()
                    .orElseThrow(() -> new IllegalArgumentException("服务商不存在: " + id));
            String selectedModel = request.model() == null || request.model().isBlank()
                    ? profile.defaultModel() : request.model();
            llmClient.testProviderConnection(id, selectedModel);
            return Map.of("success", true, "message", "API 连接成功", "model", selectedModel);
        } catch (IllegalArgumentException e) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, e.getMessage());
        } catch (Exception e) {
            throw new ResponseStatusException(HttpStatus.BAD_GATEWAY, "API 连接失败: " + e.getMessage());
        }
    }

    @DeleteMapping("/providers/{id}")
    @PreAuthorize("hasRole('ADMIN')")
    @Audit(action = "AI_PROVIDER_DELETE", targetType = "SYSTEM", targetId = "#id")
    public void deleteProvider(@PathVariable String id) {
        try {
            llmClient.deleteProvider(id);
        } catch (IllegalArgumentException e) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, e.getMessage());
        }
    }

    @PostMapping("/nl2pipeline")
    @PreAuthorize("hasAnyRole('ADMIN','OPERATOR')")
    @Audit(action = "AI_NL2PIPELINE", targetType = "JOB", targetId = "#result.job.id")
    public Map<String, Object> nl2Pipeline(@RequestBody Nl2PipelineRequest req) {
        return agentService.nl2Pipeline(req.prompt(), req.model(), currentUser());
    }

    @PostMapping("/diagnose/{jobId}")
    @PreAuthorize("hasAnyRole('ADMIN','OPERATOR')")
    @Audit(action = "AI_DIAGNOSE", targetType = "JOB", targetId = "#jobId")
    public Map<String, Object> diagnose(@PathVariable Long jobId,
                                        @RequestParam(required = false) String model) {
        return agentService.diagnose(jobId, model, currentUser());
    }

    @GetMapping("/diagnose/{jobId}")
    @PreAuthorize("hasAnyRole('ADMIN','OPERATOR','VIEWER')")
    public List<com.datastream.mvp.model.DiagnosisReport> diagnosisHistory(@PathVariable Long jobId) {
        jobService.findByIdForUser(jobId, currentUser());
        return agentService.listDiagnosis(jobId);
    }

    private CurrentUser currentUser() {
        CurrentUser cu = SecurityUtils.currentUser();
        if (cu == null) throw new ResponseStatusException(HttpStatus.UNAUTHORIZED, "未登录");
        return cu;
    }
}
