package com.datastream.mvp.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.net.http.HttpClient;
import java.time.Duration;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class ClusterHealthServiceTest {
    @Test
    void networkFailureDegradesToDownResponseWithoutExceptionDetails() {
        ClusterHealthService service = new ClusterHealthService("http://127.0.0.1:1", new ObjectMapper(),
                HttpClient.newBuilder().connectTimeout(Duration.ofMillis(100)).build(), Duration.ofMillis(100));

        Map<String, Object> result = service.health();

        assertEquals("DOWN", result.get("status"));
        assertEquals("Flink REST API 不可用", result.get("message"));
        assertFalse(result.containsKey("exception"));
        assertFalse(result.containsKey("stackTrace"));
    }
}
