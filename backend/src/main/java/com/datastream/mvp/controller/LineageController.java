package com.datastream.mvp.controller;

import com.datastream.mvp.security.CurrentUser;
import com.datastream.mvp.security.SecurityUtils;
import com.datastream.mvp.service.LineageService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

import java.util.Map;

/**
 * 数据血缘查询 API
 */
@RestController
@RequestMapping("/api/lineage")
@RequiredArgsConstructor
public class LineageController {

    private final LineageService lineageService;

    @GetMapping("/job/{id}")
    public Map<String, Object> jobLineage(@PathVariable Long id) {
        return lineageService.getJobLineage(id, currentUser());
    }

    @GetMapping("/workflow")
    public Map<String, Object> workflowLineage() {
        return lineageService.getWorkflowLineage(currentUser());
    }

    private CurrentUser currentUser() {
        CurrentUser cu = SecurityUtils.currentUser();
        if (cu == null) throw new ResponseStatusException(HttpStatus.UNAUTHORIZED, "未登录");
        return cu;
    }
}
