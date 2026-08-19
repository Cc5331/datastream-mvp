package com.datastream.mvp.controller;

import com.datastream.mvp.ai.AgentService;
import com.datastream.mvp.audit.Audit;
import com.datastream.mvp.security.CurrentUser;
import com.datastream.mvp.security.SecurityUtils;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

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

    public record Nl2PipelineRequest(String prompt) {}

    @PostMapping("/nl2pipeline")
    @PreAuthorize("hasAnyRole('ADMIN','OPERATOR')")
    @Audit(action = "AI_NL2PIPELINE", targetType = "JOB", targetId = "#result.job.id")
    public Map<String, Object> nl2Pipeline(@RequestBody Nl2PipelineRequest req) {
        return agentService.nl2Pipeline(req.prompt(), currentUser());
    }

    @PostMapping("/diagnose/{jobId}")
    @PreAuthorize("hasAnyRole('ADMIN','OPERATOR')")
    @Audit(action = "AI_DIAGNOSE", targetType = "JOB", targetId = "#jobId")
    public Map<String, Object> diagnose(@PathVariable Long jobId) {
        return agentService.diagnose(jobId);
    }

    private CurrentUser currentUser() {
        CurrentUser cu = SecurityUtils.currentUser();
        if (cu == null) throw new ResponseStatusException(HttpStatus.UNAUTHORIZED, "未登录");
        return cu;
    }
}
