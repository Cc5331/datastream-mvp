package com.datastream.mvp.service;

import com.datastream.mvp.dag.DagDefinition;
import com.datastream.mvp.model.ControlRegistry;
import com.datastream.mvp.model.JobDefinition;
import com.datastream.mvp.model.JobLog;
import com.datastream.mvp.security.CurrentUser;
import com.datastream.mvp.security.SecurityUtils;
import com.datastream.mvp.model.JobVersion;
import com.datastream.mvp.repository.JobDefinitionRepository;
import com.datastream.mvp.repository.JobLogRepository;
import com.datastream.mvp.repository.JobVersionRepository;
import com.datastream.mvp.repository.JobDependencyRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;
import org.springframework.transaction.annotation.Transactional;

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
    private final JobVersionRepository versionRepo;
    private final ControlRegistryService controlService;
    private final DagTranslationService dagTranslationService;
    private final ObjectMapper objectMapper;
    private final ExcelPreprocessor excelPreprocessor;
    private final FlinkJobStatusChecker flinkJobStatusChecker;
    private final JobDependencyRepository dependencyRepo;

    @Value("${app.mysql.default-username:root}")
    private String defaultMysqlUsername;

    @Value("${app.mysql.default-password:}")
    private String defaultMysqlPassword;

    public List<JobDefinition> findAll() {
        return jobRepo.findAllByOrderByUpdatedAtDesc();
    }

    public JobDefinition findById(Long id) {
        return jobRepo.findById(id).orElseThrow(() ->
                new RuntimeException("Job not found: id=" + id));
    }

    /**
     * RBAC：非管理员只能看到自己的作业
     */
    public List<JobDefinition> findAllForUser(CurrentUser cu) {
        if (cu == null) return List.of();
        if (cu.isAdmin()) return jobRepo.findAllByOrderByUpdatedAtDesc();
        return jobRepo.findByOwnerIdOrderByUpdatedAtDesc(cu.id());
    }

    /**
     * RBAC：按资源归属校验后返回作业
     */
    public JobDefinition findByIdForUser(Long id, CurrentUser cu) {
        JobDefinition job = findById(id);
        assertCanAccess(job, cu);
        return job;
    }

    public void assertCanAccess(JobDefinition job, CurrentUser cu) {
        if (cu == null) throw new ResponseStatusException(HttpStatus.UNAUTHORIZED, "未登录");
        if (cu.isAdmin()) return;
        if (job.getOwnerId() != null && !job.getOwnerId().equals(cu.id())) {
            throw new ResponseStatusException(HttpStatus.FORBIDDEN, "无权访问该作业");
        }
    }

    public JobDefinition create(JobDefinition job, CurrentUser cu) {
        if (cu != null) {
            job.setOwnerId(cu.id());
            job.setOwnerName(cu.displayName() == null ? cu.username() : cu.displayName());
        }
        return create(job);
    }

    public JobDefinition create(JobDefinition job) {
        job.setStatus(JobDefinition.JobStatus.DRAFT);
        job.setCreatedAt(LocalDateTime.now());
        job.setUpdatedAt(LocalDateTime.now());
        JobDefinition saved = jobRepo.save(job);
        snapshotVersion(saved, 1);
        return saved;
    }

    public JobDefinition update(Long id, JobDefinition update) {
        JobDefinition existing = findById(id);
        String prevDag = existing.getDagJson();
        String prevName = existing.getName();
        Integer prevParallelism = existing.getParallelism();
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
        if (update.getWebhookUrl() != null) existing.setWebhookUrl(update.getWebhookUrl());
        if (update.getScheduleMaxRetries() != null) existing.setScheduleMaxRetries(update.getScheduleMaxRetries());
        existing.setUpdatedAt(LocalDateTime.now());
        JobDefinition saved = jobRepo.save(existing);
        boolean changed = !java.util.Objects.equals(prevDag, saved.getDagJson())
                || !java.util.Objects.equals(prevName, saved.getName())
                || !java.util.Objects.equals(prevParallelism, saved.getParallelism());
        if (changed) snapshotVersion(saved, nextVersionNo(id));
        return saved;
    }

    @Transactional
    public void delete(Long id) {
        if (jobRepo.existsById(id)) {
            versionRepo.deleteByJobId(id);
            dependencyRepo.deleteByUpstreamJobIdOrDownstreamJobId(id, id);
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
        flinkJobStatusChecker.finalizeJobOutputs(job);
        addLog(id, "INFO", "作业已取消，Flink Job ID: " + job.getFlinkJobId());
    }
    /**
     * 作业上线：进入受监管的持续处理状态。
     * - 流式作业（kafka_input / datagen_input）：立即提交到 Flink 后台持续运行，禁用 Kafka 体验模式自动停止；
     * - 批量文件作业：上线 = 按 cron 周期重跑（必须先配置 cron），启用调度并立即执行一次；
     * - 上线要求 Flink 集群可用且真实运行（禁止 mock 降级），否则报错并保持未上线。
     */
    public JobDefinition online(Long id) {
        JobDefinition job = findById(id);
        LocalDateTime now = LocalDateTime.now();
        if (Boolean.TRUE.equals(job.getOnline())
                && (job.getStatus() == JobDefinition.JobStatus.SUBMITTED || job.getStatus() == JobDefinition.JobStatus.RUNNING)) {
            addLog(id, "INFO", "作业已处于上线状态且运行中，无需重复上线");
            return job;
        }
        if (!dagTranslationService.isFlinkClusterAvailable()) {
            throw new RuntimeException("Flink 集群不可用，上线失败（上线要求作业真实运行，不允许 mock 降级）");
        }
        boolean streaming = isStreamingJob(job);
        if (!streaming) {
            String cron = job.getCronExpression();
            if (cron == null || cron.isBlank()) {
                throw new RuntimeException("批量文件作业上线前必须先配置 cron 表达式（上线后按周期重跑）");
            }
            job.setScheduleEnabled(true);
            job.setNextFireTime(computeNextFireTime(cron));
        } else {
            job.setScheduleEnabled(false);
            job.setDagJson(forceKafkaNoAutoStop(job.getDagJson()));
        }
        job.setOnline(true);
        job.setOnlineSince(now);
        job.setOnlineRestartCount(0);
        job.setLastRestartAt(null);
        job.setUpdatedAt(now);
        jobRepo.save(job);
        addLog(id, "INFO", "作业已上线" + (streaming ? "（流式，后台持续处理）" : "（批量，按 cron 周期重跑：" + job.getCronExpression() + "）"));
        try {
            JobDefinition submitted = submit(id);
            String fid = submitted.getFlinkJobId();
            if (fid != null && (fid.startsWith("flink-job-") || fid.startsWith("mock-"))) {
                job.setOnline(false);
                job.setStatus(JobDefinition.JobStatus.FAILED);
                job.setUpdatedAt(LocalDateTime.now());
                jobRepo.save(job);
                addLog(id, "ERROR", "上线失败：作业未在 Flink 真实运行（提交降级为 mock）");
                throw new RuntimeException("上线失败：作业未在 Flink 真实运行（提交降级为 mock）");
            }
            return submitted;
        } catch (Exception e) {
            if (e.getMessage() != null && e.getMessage().startsWith("上线失败")) throw e;
            job.setOnline(false);
            job.setUpdatedAt(LocalDateTime.now());
            jobRepo.save(job);
            addLog(id, "ERROR", "上线失败: " + e.getMessage());
            throw new RuntimeException("上线失败: " + e.getMessage(), e);
        }
    }

    /**
     * 作业下线：停止持续处理。
     * 运行中作业先取消 Flink 作业并合并输出；同时停用 cron 周期重跑（cron 表达式保留，重新上线可复用）。
     */
    public JobDefinition offline(Long id) {
        JobDefinition job = findById(id);
        boolean wasOnline = Boolean.TRUE.equals(job.getOnline());
        job.setOnline(false);
        job.setScheduleEnabled(false);
        job.setOnlineRestartCount(0);
        job.setLastRestartAt(null);
        job.setUpdatedAt(LocalDateTime.now());
        JobDefinition.JobStatus st = job.getStatus();
        if (st == JobDefinition.JobStatus.SUBMITTED || st == JobDefinition.JobStatus.RUNNING) {
            dagTranslationService.cancelFlinkJob(job.getFlinkJobId());
            job.setStatus(JobDefinition.JobStatus.CANCELLED);
            jobRepo.save(job);
            flinkJobStatusChecker.finalizeJobOutputs(job);
            addLog(id, "INFO", "作业已下线并停止运行（Flink Job: " + job.getFlinkJobId() + "）");
        } else {
            jobRepo.save(job);
            addLog(id, "INFO", "作业已下线" + (wasOnline ? "" : "（此前未上线）"));
        }
        return job;
    }

    /** 判断作业是否流式：DAG 含 kafka_input 或 datagen_input */
    public boolean isStreamingJob(JobDefinition job) {
        try {
            com.fasterxml.jackson.databind.JsonNode dag = objectMapper.readTree(job.getDagJson());
            com.fasterxml.jackson.databind.JsonNode nodes = dag.path("nodes");
            if (nodes.isArray()) {
                for (com.fasterxml.jackson.databind.JsonNode n : nodes) {
                    String type = n.path("type").asText("");
                    if ("kafka_input".equals(type) || "datagen_input".equals(type)) return true;
                }
            }
        } catch (Exception e) {
            log.warn("isStreamingJob parse failed for job {}: {}", job.getId(), e.getMessage());
        }
        return false;
    }

    /** 上线时强制关闭 Kafka 输入控件的体验模式自动停止，保证后台持续消费 */
    private String forceKafkaNoAutoStop(String dagJson) {
        try {
            com.fasterxml.jackson.databind.JsonNode root = objectMapper.readTree(dagJson);
            if (root.has("nodes") && root.get("nodes").isArray()) {
                boolean changed = false;
                for (com.fasterxml.jackson.databind.JsonNode n : root.get("nodes")) {
                    if ("kafka_input".equals(n.path("type").asText("")) && n.has("params")) {
                        ((com.fasterxml.jackson.databind.node.ObjectNode) n.get("params")).put("autoStop", false);
                        changed = true;
                    }
                }
                if (changed) return objectMapper.writeValueAsString(root);
            }
        } catch (Exception e) {
            log.warn("forceKafkaNoAutoStop failed: {}", e.getMessage());
        }
        return dagJson;
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
        copy.setOwnerId(source.getOwnerId());
        copy.setOwnerName(source.getOwnerName());
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
    public JobDefinition updateSchedule(Long id, Boolean enabled, String cronExpression, Integer scheduleMaxRetries, String webhookUrl) {
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
        if (scheduleMaxRetries != null) job.setScheduleMaxRetries(scheduleMaxRetries);
        if (webhookUrl != null) job.setWebhookUrl(webhookUrl.isBlank() ? null : webhookUrl.trim());
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
                        String username = resolveCredential(params.has("username") ? params.get("username").asText() : null, defaultMysqlUsername == null ? "root" : defaultMysqlUsername);
                        String password = resolveCredential(params.has("password") ? params.get("password").asText() : null, defaultMysqlPassword);
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

    /**
     * 作业版本历史（按版本号倒序）
     */
    public List<JobVersion> getVersions(Long jobId) {
        findById(jobId); // 校验作业存在
        return versionRepo.findByJobIdOrderByVersionNoDesc(jobId);
    }

    /**
     * 回滚到指定版本：恢复 name / dagJson / parallelism，并记录一个新版本
     */
    public JobDefinition rollback(Long jobId, Long versionId) {
        JobDefinition job = findById(jobId);
        JobVersion v = versionRepo.findById(versionId)
                .orElseThrow(() -> new RuntimeException("Job version not found: id=" + versionId));
        if (!java.util.Objects.equals(v.getJobId(), jobId)) {
            throw new RuntimeException("版本不属于该作业");
        }
        if (v.getName() != null) job.setName(v.getName());
        if (v.getDagJson() != null) job.setDagJson(v.getDagJson());
        if (v.getParallelism() != null) job.setParallelism(v.getParallelism());
        job.setUpdatedAt(LocalDateTime.now());
        JobDefinition saved = jobRepo.save(job);
        snapshotVersion(saved, nextVersionNo(jobId));
        addLog(jobId, "INFO", "已回滚到版本 " + v.getVersionNo());
        return saved;
    }

    private int nextVersionNo(Long jobId) {
        List<JobVersion> vs = versionRepo.findByJobIdOrderByVersionNoDesc(jobId);
        return vs.isEmpty() ? 1 : vs.get(0).getVersionNo() + 1;
    }

    private void snapshotVersion(JobDefinition job, int versionNo) {
        JobVersion v = new JobVersion();
        v.setJobId(job.getId());
        v.setVersionNo(versionNo);
        v.setName(job.getName());
        v.setDagJson(job.getDagJson());
        v.setParallelism(job.getParallelism());
        v.setCreatedAt(LocalDateTime.now());
        versionRepo.save(v);
    }

    /**
     * 用户名/密码解析：空值或 ${MYSQL_USERNAME} / ${MYSQL_PASSWORD} 占位符回退到环境变量默认值
     */
    private String resolveCredential(String value, String envDefault) {
        if (value == null || value.trim().isEmpty()) return envDefault == null ? "" : envDefault;
        String v = value.trim();
        if ("${MYSQL_USERNAME}".equals(v) || "${MYSQL_PASSWORD}".equals(v)) {
            return envDefault == null ? "" : envDefault;
        }
        return v;
    }

}