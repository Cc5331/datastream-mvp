package com.datastream.mvp.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.LinkedHashMap;
import java.util.Map;

@Service
public class ClusterHealthService {
    private final String baseUrl;
    private final ObjectMapper objectMapper;
    private final HttpClient httpClient;
    private final Duration requestTimeout;

    public ClusterHealthService(@Value("${flink.cluster.host:localhost}") String host,
                                @Value("${flink.cluster.port:8081}") int port,
                                ObjectMapper objectMapper) {
        this("http://" + host + ":" + port, objectMapper,
                HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build(), Duration.ofSeconds(3));
    }

    ClusterHealthService(String baseUrl, ObjectMapper objectMapper, HttpClient httpClient, Duration requestTimeout) {
        this.baseUrl = baseUrl;
        this.objectMapper = objectMapper;
        this.httpClient = httpClient;
        this.requestTimeout = requestTimeout;
    }

    public Map<String, Object> health() {
        try {
            JsonNode overview = get("/overview");
            JsonNode taskmanagers = get("/taskmanagers");
            JsonNode jobs = get("/jobs/overview");
            int tmCount = taskmanagers.path("taskmanagers").size();
            int slotsTotal = overview.path("slots-total").asInt(0);
            int slotsAvailable = overview.path("slots-available").asInt(0);
            String status = tmCount > 0 && slotsTotal > 0 ? "UP" : "DEGRADED";
            Map<String, Object> result = base(status);
            result.put("taskManagers", tmCount);
            result.put("slotsTotal", slotsTotal);
            result.put("slotsAvailable", slotsAvailable);
            result.put("runningJobs", countJobs(jobs, "RUNNING"));
            result.put("finishedJobs", countJobs(jobs, "FINISHED"));
            return result;
        } catch (Exception e) {
            Map<String, Object> result = base("DOWN");
            result.put("message", "Flink REST API 不可用");
            return result;
        }
    }

    private JsonNode get(String path) throws Exception {
        HttpRequest request = HttpRequest.newBuilder(URI.create(baseUrl + path)).timeout(requestTimeout).GET().build();
        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() < 200 || response.statusCode() >= 300) throw new IllegalStateException("Flink HTTP " + response.statusCode());
        return objectMapper.readTree(response.body());
    }

    private long countJobs(JsonNode jobs, String state) {
        long count = 0;
        for (JsonNode job : jobs.path("jobs")) if (state.equals(job.path("state").asText())) count++;
        return count;
    }

    private Map<String, Object> base(String status) {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("status", status);
        result.put("checkedAt", LocalDateTime.now());
        return result;
    }
}
