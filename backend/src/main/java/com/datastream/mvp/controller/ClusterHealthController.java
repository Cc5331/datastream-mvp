package com.datastream.mvp.controller;

import com.datastream.mvp.service.ClusterHealthService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
@RequestMapping("/api/cluster")
@RequiredArgsConstructor
public class ClusterHealthController {
    private final ClusterHealthService clusterHealthService;

    @GetMapping("/health")
    public Map<String, Object> health() {
        return clusterHealthService.health();
    }
}
