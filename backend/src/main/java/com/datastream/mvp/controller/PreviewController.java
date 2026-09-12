package com.datastream.mvp.controller;

import com.datastream.mvp.model.JobDefinition;
import com.datastream.mvp.security.CurrentUser;
import com.datastream.mvp.security.SecurityUtils;
import com.datastream.mvp.service.JobService;
import com.datastream.mvp.service.PreviewService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

import java.util.Map;

/**
 * 数据预览接口：按文件类型读取前 N 行，供前端画布节点“预览数据”。
 */
@RestController
@RequestMapping("/api/preview")
@RequiredArgsConstructor
public class PreviewController {

    private final PreviewService previewService;
    private final JobService jobService;

    public record NodePreviewRequest(Long jobId, String nodeId, Integer limit) {}

    @GetMapping("/file")
    public Map<String, Object> previewFile(@RequestParam String path,
                                           @RequestParam(defaultValue = "20") int limit) {
        return previewService.previewFile(path, limit);
    }

    @PostMapping("/node")
    public Map<String, Object> previewNode(@RequestBody NodePreviewRequest request) {
        CurrentUser currentUser = SecurityUtils.currentUser();
        if (currentUser == null) {
            throw new ResponseStatusException(HttpStatus.UNAUTHORIZED, "未登录");
        }
        if (request == null || request.jobId() == null || request.nodeId() == null || request.nodeId().isBlank()) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "jobId 和 nodeId 必填");
        }
        JobDefinition job = jobService.findByIdForUser(request.jobId(), currentUser);
        return previewService.previewNode(job, request.nodeId(), request.limit() == null ? 20 : request.limit());
    }
}
