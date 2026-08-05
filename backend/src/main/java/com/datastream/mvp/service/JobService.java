package com.datastream.mvp.service;

import com.datastream.mvp.dag.DagDefinition;
import com.datastream.mvp.model.ControlRegistry;
import com.datastream.mvp.model.JobDefinition;
import com.datastream.mvp.model.JobLog;
import com.datastream.mvp.repository.JobDefinitionRepository;
import com.datastream.mvp.repository.JobLogRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.List;

/**
 * 作业管理服务
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class JobService {

    private final JobDefinitionRepository jobRepo;
    private final JobLogRepository logRepo;
    private final ControlRegistryService controlService;
    private final DagTranslationService dagTranslationService;
    private final ObjectMapper objectMapper;
    private final ExcelPreprocessor excelPreprocessor;

    public List<JobDefinition> findAll() {
        return jobRepo.findAllByOrderByUpdatedAtDesc();
    }

    public JobDefinition findById(Long id) {
        return jobRepo.findById(id).orElseThrow(() ->
                new RuntimeException("Job not found: id=" + id));
    }

    public JobDefinition create(JobDefinition job) {
        job.setStatus(JobDefinition.JobStatus.DRAFT);
        job.setCreatedAt(LocalDateTime.now());
        job.setUpdatedAt(LocalDateTime.now());
        return jobRepo.save(job);
    }

    public JobDefinition update(Long id, JobDefinition update) {
        JobDefinition existing = findById(id);
        if (update.getName() != null) existing.setName(update.getName());
        if (update.getDescription() != null) existing.setDescription(update.getDescription());
        if (update.getDagJson() != null) existing.setDagJson(update.getDagJson());
        if (update.getParallelism() != null) existing.setParallelism(update.getParallelism());
        if (update.getScheduleEnabled() != null) existing.setScheduleEnabled(update.getScheduleEnabled());
        if (update.getCronExpression() != null && !update.getCronExpression().isBlank()) {
            existing.setCronExpression(update.getCronExpression().trim());
            existing.setNextFireTime(computeNextFireTime(update.getCronExpression()));
        } else if (update.getCronExpression() != null && update.getCronExpression().isBlank()) {
            existing.setCronExpression(null);
            existing.setNextFireTime(null);
        }
        existing.setUpdatedAt(LocalDateTime.now());
        return jobRepo.save(existing);
    }

    public void delete(Long id) {
        if (jobRepo.existsById(id)) {
            jobRepo.deleteById(id);
        } else {
            throw new RuntimeException("Job not found: id=" + id);
        }
    }

    /**
     * 提交作业到 Flink
     */
    public JobDefinition submit(Long id) {
        JobDefinition job = findById(id);
        try {
            // 解析 DAG JSON
            DagDefinition dag = objectMapper.readValue(job.getDagJson(), DagDefinition.class);

            // 翻译 DAG -> Flink 作业
            String flinkSql = dagTranslationService.translate(dag);

            // TODO: 实际提交到 Flink REST API
            String flinkJobId = dagTranslationService.submitToFlink(flinkSql, job.getParallelism());

            job.setStatus(JobDefinition.JobStatus.SUBMITTED);
            job.setFlinkJobId(flinkJobId);
            job.setSubmittedAt(LocalDateTime.now());
            job.setUpdatedAt(LocalDateTime.now());
            jobRepo.save(job);

            addLog(id, "INFO", "作业已提交，Flink Job ID: " + flinkJobId);
        } catch (Exception e) {
            job.setStatus(JobDefinition.JobStatus.FAILED);
            jobRepo.save(job);
            addLog(id, "ERROR", "提交失败: " + e.getMessage());
            throw new RuntimeException("Job submission failed: " + e.getMessage(), e);
        }
        return job;
    }

    /**
     * 取消作业
     */
    public void cancel(Long id) {
        JobDefinition job = findById(id);
        dagTranslationService.cancelFlinkJob(job.getFlinkJobId());
        job.setStatus(JobDefinition.JobStatus.CANCELLED);
        job.setUpdatedAt(LocalDateTime.now());
        jobRepo.save(job);
        addLog(id, "INFO", "作业已取消，Flink Job ID: " + job.getFlinkJobId());
    }
    /**
     * 复制作业（深拷贝 DAG，状态重置为 DRAFT，名称追加（副本），默认不继承调度）
     */
    public JobDefinition copy(Long id) {
        JobDefinition source = findById(id);
        JobDefinition copy = new JobDefinition();
        copy.setName(source.getName() + "（副本）");
        copy.setDescription(source.getDescription());
        copy.setDagJson(source.getDagJson());
        copy.setParallelism(source.getParallelism());
        copy.setCronExpression(source.getCronExpression());
        copy.setScheduleEnabled(Boolean.FALSE);
        copy.setNextFireTime(null);
        copy.setStatus(JobDefinition.JobStatus.DRAFT);
        copy.setCreatedAt(LocalDateTime.now());
        copy.setUpdatedAt(LocalDateTime.now());
        JobDefinition saved = jobRepo.save(copy);
        addLog(saved.getId(), "INFO", "作业已复制，来源作业 ID=" + id);
        return saved;
    }

    /**
     * 更新定时调度配置（启用开关 + cron 表达式），并计算下次触发时间
     */
    public JobDefinition updateSchedule(Long id, Boolean enabled, String cronExpression) {
        JobDefinition job = findById(id);
        boolean enable = enabled != null && enabled;
        if (enable) {
            if (cronExpression == null || cronExpression.isBlank()) {
                throw new RuntimeException("启用定时调度必须填写 cron 表达式");
            }
            job.setCronExpression(cronExpression.trim());
            job.setNextFireTime(computeNextFireTime(cronExpression));
        } else {
            if (cronExpression != null && !cronExpression.isBlank()) {
                job.setCronExpression(cronExpression.trim());
            }
            job.setNextFireTime(null);
        }
        job.setScheduleEnabled(enable);
        job.setUpdatedAt(LocalDateTime.now());
        JobDefinition saved = jobRepo.save(job);
        addLog(id, "INFO", "定时调度已" + (enable ? "启用: " + job.getCronExpression() : "停用"));
        return saved;
    }

    private LocalDateTime computeNextFireTime(String cronExpression) {
        if (cronExpression == null || cronExpression.isBlank()) return null;
        try {
            return org.springframework.scheduling.support.CronExpression.parse(cronExpression.trim())
                    .next(LocalDateTime.now());
        } catch (IllegalArgumentException e) {
            throw new RuntimeException("无效的 cron 表达式: " + cronExpression + "（" + e.getMessage() + "）");
        }
    }

    public List<JobLog> getLogs(Long jobId) {
        return logRepo.findByJobIdOrderByTimestampDesc(jobId);
    }

    private void addLog(Long jobId, String level, String message) {
        JobLog log = new JobLog();
        log.setJobId(jobId);
        log.setLevel(level);
        log.setMessage(message);
        log.setTimestamp(LocalDateTime.now());
        logRepo.save(log);
    }

    /**
     * Preview output data for a submitted job
     */
    public List<String> preview(Long id) {
        JobDefinition job = findById(id);
        java.util.List<String> result = new java.util.ArrayList<>();
        try {
            com.fasterxml.jackson.databind.JsonNode dagNode = objectMapper.readTree(job.getDagJson());
            com.fasterxml.jackson.databind.JsonNode nodes = dagNode.get("nodes");
            if (nodes != null && nodes.isArray()) {
                for (int ni = 0; ni < nodes.size(); ni++) {
                    com.fasterxml.jackson.databind.JsonNode n = nodes.get(ni);
                    String type = n.has("type") ? n.get("type").asText() : "";
                    com.fasterxml.jackson.databind.JsonNode params = n.get("params");

                    // MySQL output preview - query MySQL directly
                    if ("mysql_output".equals(type) && params != null) {
                        String url = params.has("url") ? params.get("url").asText() : "jdbc:mysql://localhost:3306/flink_demo";
                        String table = params.has("table") ? params.get("table").asText() : "user_data";
                        String username = params.has("username") ? params.get("username").asText() : "root";
                        String password = params.has("password") ? params.get("password").asText() : "YOUR_MYSQL_PASSWORD";
                        try (java.sql.Connection conn = java.sql.DriverManager.getConnection(url, username, password);
                             java.sql.Statement stmt = conn.createStatement();
                             java.sql.ResultSet rs = stmt.executeQuery("SELECT * FROM " + table + " LIMIT 20")) {
                            int colCount = rs.getMetaData().getColumnCount();
                            // Header
                            StringBuilder header = new StringBuilder();
                            for (int ci = 1; ci <= colCount; ci++) {
                                if (ci > 1) header.append(",");
                                header.append(rs.getMetaData().getColumnName(ci));
                            }
                            result.add(header.toString());
                            // Data rows
                            int rowCount = 0;
                            while (rs.next()) {
                                StringBuilder row = new StringBuilder();
                                for (int ci = 1; ci <= colCount; ci++) {
                                    if (ci > 1) row.append(",");
                                    String val = rs.getString(ci);
                                    row.append(val != null ? val : "NULL");
                                }
                                result.add(row.toString());
                                rowCount++;
                            }
                            result.add("--- " + rowCount + " rows from MySQL table '" + table + "' ---");
                        } catch (Exception e) {
                            result.add("MySQL preview error: " + e.getMessage());
                        }
                        return result;
                    }

                    // CSV output preview - read file
                    if ("csv_output".equals(type) || (!"mysql_output".equals(type) && type.endsWith("_output"))) {
                        com.fasterxml.jackson.databind.JsonNode csvParams = n.get("params");
                        if (csvParams != null && csvParams.has("path")) {
                            String path = csvParams.get("path").asText();
                            java.nio.file.Path outputPath = java.nio.file.Paths.get(path);
                            if (java.nio.file.Files.exists(outputPath) && java.nio.file.Files.isRegularFile(outputPath)) {
                                java.util.List<String> lines = java.nio.file.Files.readAllLines(outputPath);
                                int limit = Math.min(20, lines.size());
                                for (int i = 0; i < limit; i++) { result.add(lines.get(i)); }
                                if (lines.size() > 20) { result.add("... (" + (lines.size() - 20) + " more lines)"); }
                            } else if (java.nio.file.Files.isDirectory(outputPath)) {
                                // Filesystem connector creates directory+part files, try reading part files
                                java.io.File dir = outputPath.toFile();
                                java.io.File[] partFiles = dir.listFiles((d, name) -> name.startsWith("part-"));
                                if (partFiles != null && partFiles.length > 0) {
                                    java.util.List<String> lines = java.nio.file.Files.readAllLines(partFiles[0].toPath());
                                    int limit = Math.min(20, lines.size());
                                    for (int i = 0; i < limit; i++) { result.add(lines.get(i)); }
                                    if (lines.size() > 20) { result.add("... (" + (lines.size() - 20) + " more lines)"); }
                                    result.add("(read from part file)");
                                } else {
                                    result.add("Output directory exists but no part files found: " + outputPath.toAbsolutePath());
                                }
                            } else {
                                result.add("Output file not found: " + outputPath.toAbsolutePath());
                            }
                            return result;
                        }
                    }
                }
            }
            result.add("No output node found in DAG");
        } catch (Exception e) {
            log.warn("Preview failed: {}", e.getMessage());
            result.add("Preview failed: " + e.getMessage());
        }
        return result;
    }
}
