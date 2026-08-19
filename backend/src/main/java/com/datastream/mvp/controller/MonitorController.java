package com.datastream.mvp.controller;

import com.datastream.mvp.service.MonitorService;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
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
}
