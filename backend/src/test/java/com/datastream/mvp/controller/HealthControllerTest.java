package com.datastream.mvp.controller;

import com.datastream.mvp.repository.JobDefinitionRepository;
import org.junit.jupiter.api.Test;
import org.springframework.http.ResponseEntity;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * 健康探针：只有「HTTP 通 + DB 可用」才算 UP；DB 异常必须返回 503，
 * 否则看门狗无法发现「端口在听但链路已坏」的僵死。
 */
class HealthControllerTest {

    @Test
    void healthIsUpWhenDatabaseResponds() {
        JobDefinitionRepository repo = mock(JobDefinitionRepository.class);
        when(repo.count()).thenReturn(55L);

        ResponseEntity<Map<String, Object>> resp = new HealthController(repo).health();

        assertEquals(200, resp.getStatusCode().value());
        assertEquals("UP", resp.getBody().get("status"));
        assertEquals("UP", resp.getBody().get("db"));
        assertEquals(true, resp.getBody().containsKey("uptimeSeconds"));
    }

    @Test
    void healthIsDownWhenDatabaseFails() {
        JobDefinitionRepository repo = mock(JobDefinitionRepository.class);
        when(repo.count()).thenThrow(new RuntimeException("db down"));

        ResponseEntity<Map<String, Object>> resp = new HealthController(repo).health();

        assertEquals(503, resp.getStatusCode().value());
        assertEquals("DOWN", resp.getBody().get("status"));
        assertEquals("DOWN", resp.getBody().get("db"));
        assertEquals("RuntimeException", resp.getBody().get("dbError"));
    }
}
