package com.datastream.mvp.controller;

import com.datastream.mvp.service.KafkaMonitorService;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Kafka 可视化 REST API：topic 列表/统计 + SSE 实时消息流。
 */
@Slf4j
@RestController
@RequestMapping("/api/kafka")
@RequiredArgsConstructor
public class KafkaMonitorController {

    private final KafkaMonitorService kafkaMonitorService;
    private final ObjectMapper objectMapper;

    @GetMapping("/topics")
    public List<KafkaMonitorService.TopicInfo> topics() {
        return kafkaMonitorService.listTopics();
    }

    /**
     * SSE 实时消息流：from=beginning 从最早开始重放，from=latest 只收新消息。
     */
    @GetMapping(value = "/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public SseEmitter stream(@RequestParam String topic,
                             @RequestParam(defaultValue = "beginning") String from,
                             @RequestParam(defaultValue = "0") long maxMessages,
                             @RequestParam(defaultValue = "0") long durationMs) {
        boolean fromBeginning = !"latest".equalsIgnoreCase(from);
        SseEmitter emitter = new SseEmitter(durationMs > 0 ? durationMs : 0L);
        Thread worker = new Thread(() -> {
            try {
                kafkaMonitorService.streamTopic(topic, fromBeginning, maxMessages, durationMs, msg -> {
                    try {
                        emitter.send(SseEmitter.event().name("message").data(objectMapper.writeValueAsString(msg)));
                    } catch (IOException e) {
                        Thread.currentThread().interrupt();
                    }
                });
                emitter.complete();
            } catch (Exception e) {
                log.warn("Kafka stream failed: {}", e.getMessage());
                try {
                    emitter.send(SseEmitter.event().name("error")
                            .data("{\"error\":\"" + escape(e.getMessage()) + "\"}"));
                } catch (IOException ignored) {
                    // client gone
                }
                emitter.completeWithError(e);
            }
        }, "kafka-stream-" + topic);
        worker.setDaemon(true);
        emitter.onCompletion(worker::interrupt);
        emitter.onTimeout(worker::interrupt);
        worker.start();
        return emitter;
    }

    private String escape(String s) {
        if (s == null) {
            return "";
        }
        return s.replace("\\", "\\\\").replace("\"", "\\\"")
                .replace("\n", " ").replace("\r", " ");
    }
}
