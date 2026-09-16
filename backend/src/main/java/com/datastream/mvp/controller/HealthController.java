package com.datastream.mvp.controller;

import com.datastream.mvp.repository.JobDefinitionRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.lang.management.ManagementFactory;
import java.time.LocalDateTime;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 健康探针：供看门狗（scripts/watchdog.ps1）、容器 healthcheck 与演示前预检使用。
 *
 * 设计要点：
 * - **免鉴权**（SecurityConfig permitAll）：探针不该依赖账号密码；
 * - **无副作用**：不写审计、不写业务日志（避免探针把审计表刷满）；
 * - **真查一次数据库**：只探 TCP 端口不够——2026-09-16 出现过「端口在监听、连接能建立，
 *   但请求永不响应」的僵死，只有走完整 HTTP + DB 链路才能发现；
 * - 数据库不可用时返回 503，让调用方按「不健康」处理。
 */
@RestController
@RequestMapping("/api/health")
@RequiredArgsConstructor
public class HealthController {

    private final JobDefinitionRepository jobRepo;

    @GetMapping
    public ResponseEntity<Map<String, Object>> health() {
        Map<String, Object> body = new LinkedHashMap<>();
        boolean dbUp;
        try {
            // 最轻量的真实查询：覆盖连接池与数据库可用性
            jobRepo.count();
            dbUp = true;
        } catch (Exception e) {
            dbUp = false;
            body.put("dbError", e.getClass().getSimpleName());
        }
        body.put("status", dbUp ? "UP" : "DOWN");
        body.put("db", dbUp ? "UP" : "DOWN");
        body.put("uptimeSeconds", ManagementFactory.getRuntimeMXBean().getUptime() / 1000);
        body.put("time", LocalDateTime.now().toString());
        return dbUp
                ? ResponseEntity.ok(body)
                : ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body(body);
    }
}
