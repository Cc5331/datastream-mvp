package com.datastream.mvp.controller;

import com.datastream.mvp.service.MonitorService;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
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
        return monitorService.overview();
    }

    @GetMapping("/trends")
    public List<ObjectNode> trends() {
        return monitorService.trends();
    }

    /** 移除某作业的吞吐趋势曲线（内存缓冲 + 落库趋势点） */
    @DeleteMapping("/trends/{jobId}")
    public ResponseEntity<Void> removeTrend(@PathVariable Long jobId) {
        monitorService.removeTrend(jobId);
        return ResponseEntity.noContent().build();
    }
}
