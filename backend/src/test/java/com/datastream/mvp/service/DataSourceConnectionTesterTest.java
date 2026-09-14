package com.datastream.mvp.service;

import com.datastream.mvp.dto.DataSourceTestResponse;
import com.datastream.mvp.model.DataSourceType;
import org.junit.jupiter.api.Test;

import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class DataSourceConnectionTesterTest {
    private final DataSourceConnectionTester tester = new DataSourceConnectionTester();

    @Test
    void testsRedisUsingRespWithoutExposingFailureDetails() throws Exception {
        try (ServerSocket server = new ServerSocket(0)) {
            CompletableFuture<Void> peer = CompletableFuture.runAsync(() -> {
                try (var socket = server.accept()) {
                    socket.getInputStream().readNBytes(14);
                    socket.getOutputStream().write("+PONG\r\n".getBytes(StandardCharsets.UTF_8));
                    socket.getOutputStream().flush();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
            DataSourceTestResponse success = tester.test(DataSourceType.REDIS,
                    Map.of("host", "127.0.0.1", "port", server.getLocalPort(), "database", 0, "ssl", false), Map.of());
            assertTrue(success.success());
            assertEquals("连接成功", success.message());
            peer.get(2, TimeUnit.SECONDS);
        }

        DataSourceTestResponse failed = tester.test(DataSourceType.REDIS,
                Map.of("host", "127.0.0.1", "port", 1, "database", 0, "ssl", false),
                Map.of("password", "top-secret"));
        assertFalse(failed.success());
        assertEquals("连接失败，请检查地址、凭据和服务状态", failed.message());
        assertFalse(failed.message().contains("top-secret"));
    }
}
