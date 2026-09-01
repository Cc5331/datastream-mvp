package com.datastream.mvp.service;

import com.datastream.mvp.model.JobDefinition;
import com.datastream.mvp.model.TrendPoint;
import com.datastream.mvp.repository.JobDefinitionRepository;
import com.datastream.mvp.repository.TrendPointRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * 验证趋势点持久化逻辑：写入、runKey 变化时清理旧点、写库异常不阻断内存热路径。
 */
class MonitorServicePersistenceTest {

    private JobDefinitionRepository jobRepo;
    private TrendPointRepository trendRepo;
    private ObjectMapper mapper;
    private MonitorService service;

    @BeforeEach
    void setUp() {
        jobRepo = mock(JobDefinitionRepository.class);
        trendRepo = mock(TrendPointRepository.class);
        mapper = new ObjectMapper();
        service = new MonitorService(jobRepo, trendRepo, mapper);
    }

    private JobDefinition job(Long id, LocalDateTime submittedAt) {
        JobDefinition j = new JobDefinition();
        j.setId(id);
        j.setName("job-" + id);
        j.setStatus(JobDefinition.JobStatus.RUNNING);
        j.setSubmittedAt(submittedAt);
        return j;
    }

    /** 构造带 throughput 的 item 指标 */
    private ObjectNode metricsItem(double inRate, double outRate) {
        ObjectNode item = mapper.createObjectNode();
        item.put("id", 1L);
        ObjectNode metrics = item.putObject("metrics");
        metrics.put("available", true);
        metrics.put("mode", "live");
        ObjectNode thr = metrics.putObject("throughput");
        thr.put("numRecordsInPerSecond", inRate);
        thr.put("numRecordsOutPerSecond", outRate);
        ObjectNode bp = metrics.putObject("backpressure");
        bp.put("ratio", 0.1);
        return item;
    }

    @Test
    void recordTrendPoint_persistsEachPoint() {
        JobDefinition j = job(1L, LocalDateTime.of(2026, 8, 31, 10, 0));
        ObjectNode item = metricsItem(10, 500);
        service.recordTrendPoint(j, item);
        verify(trendRepo, times(1)).save(any());
    }

    @Test
    void recordTrendPoint_runKeyChange_clearsOldRowsThenSaves() {
        JobDefinition j1 = job(1L, LocalDateTime.of(2026, 8, 31, 10, 0));
        service.recordTrendPoint(j1, metricsItem(10, 500));
        // 首次运行也清理（保证无残留），并保存
        verify(trendRepo, times(1)).deleteByJobId(1L);
        verify(trendRepo, times(1)).save(any());

        // 同 job 再次运行，submittedAt 变化 -> runKey 变化 -> 再次清旧 + 插新
        JobDefinition j2 = job(1L, LocalDateTime.of(2026, 8, 31, 11, 0));
        service.recordTrendPoint(j2, metricsItem(20, 800));
        verify(trendRepo, times(2)).deleteByJobId(1L);
        verify(trendRepo, times(2)).save(any());
    }

    @Test
    void recordTrendPoint_dbFailure_doesNotBreakMemoryBuffer() {
        doThrow(new RuntimeException("db down")).when(trendRepo).save(any());
        JobDefinition j = job(1L, LocalDateTime.of(2026, 8, 31, 10, 0));
        ObjectNode item = metricsItem(10, 500);
        // 不应抛异常，内存热路径不受影响
        service.recordTrendPoint(j, item);
    }

    @Test
    void restoreTrendForJob_rehydratesBufferFromDb() {
        JobDefinition j = job(9L, LocalDateTime.of(2026, 8, 31, 10, 0));
        String runKey = j.getSubmittedAt().toString();
        List<TrendPoint> rows = List.of(
                new TrendPoint(1L, 9L, 1000L, 5, 100, 0.1, runKey, "RUNNING"),
                new TrendPoint(2L, 9L, 2000L, 6, 200, 0.2, runKey, "RUNNING"));
        when(trendRepo.findByJobIdAndRunKeyOrderByTAsc(9L, runKey)).thenReturn(rows);
        when(jobRepo.findAllById(java.util.Set.of(9L))).thenReturn(List.of(j));

        service.restoreTrendForJob(j);

        // 通过 trends() 暴露恢复后的点
        List<ObjectNode> trends = service.trends();
        ObjectNode jobTrend = trends.stream().filter(x -> x.path("id").asLong() == 9L).findFirst().orElseThrow();
        ArrayNode points = (ArrayNode) jobTrend.get("points");
        assertEquals(2, points.size());
        assertEquals(200, points.get(1).path("out").asDouble());
        assertEquals(2000L, points.get(1).path("t").asLong());
    }
}
