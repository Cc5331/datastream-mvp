package com.datastream.mvp.controller;

import com.datastream.mvp.security.SecurityUtils;
import com.datastream.mvp.service.MonitorService;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * 实时监控 REST API
 */
@RestController
@RequestMapping("/api/monitor")
@RequiredArgsConstructor
public class MonitorController {

    private final MonitorService monitorService;

    @GetMapping("/overview")
    public List<ObjectNode> overview() {
        return monitorService.overview(SecurityUtils.currentUser());
    }

    @GetMapping("/trends")
    public List<ObjectNode> trends() {
        return monitorService.trends(SecurityUtils.currentUser());
    }

    /** 移除某作业的吞吐趋势曲线（内存缓冲 + 落库趋势点），需 OPERATOR/ADMIN 且限本人作业 */
    @DeleteMapping("/trends/{jobId}")
    @PreAuthorize("hasAnyRole('ADMIN','OPERATOR')")
    public ResponseEntity<Void> removeTrend(@PathVariable Long jobId) {
        monitorService.removeTrendForUser(jobId, SecurityUtils.currentUser());
        return ResponseEntity.noContent().build();
    }
}
