package com.datastream.mvp.service;

import com.datastream.mvp.model.JobDefinition;
import com.datastream.mvp.model.JobDefinition.JobStatus;
import com.datastream.mvp.model.JobLog;
import com.datastream.mvp.repository.JobDefinitionRepository;
import com.datastream.mvp.repository.JobLogRepository;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Set;

@Slf4j
@Service
@RequiredArgsConstructor
public class FlinkJobStatusChecker {

    private final JobDefinitionRepository jobRepo;
    private final JobLogRepository logRepo;
    private final ObjectMapper objectMapper;
    private final ExcelOutputConverter excelOutputConverter;
    private final XmlOutputConverter xmlOutputConverter;
    private final DagTranslationService dagTranslationService;
    private final ParquetOutputConverter parquetOutputConverter;

    @Value("${flink.cluster.host:localhost}")
    private String flinkHost;

    @Value("${flink.cluster.port:8081}")
    private int flinkPort;

    private static final Map<String, JobStatus> FLINK_TO_LOCAL_STATUS = new java.util.HashMap<>() {{
        put("CREATED", JobStatus.SUBMITTED);
        put("INITIALIZING", JobStatus.RUNNING);
        put("RECONCILING", JobStatus.RUNNING);
        put("RUNNING", JobStatus.RUNNING);
        put("RESTARTING", JobStatus.RUNNING);
        put("FINISHED", JobStatus.COMPLETED);
        put("FAILED", JobStatus.FAILED);
        put("FAILING", JobStatus.FAILED);
        put("CANCELLING", JobStatus.CANCELLED);
        put("CANCELED", JobStatus.CANCELLED);
        put("SUSPENDED", JobStatus.RUNNING);
    }};

    @Scheduled(fixedRate = 15000)
    public void syncJobStatuses() {
        List<JobDefinition> activeJobs = jobRepo.findByStatusIn(List.of(
            JobStatus.SUBMITTED, JobStatus.RUNNING
        ));
        if (activeJobs.isEmpty()) return;

        try {
            HttpClient httpClient = HttpClient.newHttpClient();
            String resp = httpClient.send(
                HttpRequest.newBuilder()
                    .uri(URI.create("http://" + flinkHost + ":" + flinkPort + "/jobs/overview"))
                    .GET().build(),
                HttpResponse.BodyHandlers.ofString()
            ).body();
            JsonNode root = objectMapper.readTree(resp);
            JsonNode jobsArray = root.get("jobs");
            if (jobsArray == null || !jobsArray.isArray()) return;

            // ---- Step 1: Collect all known real Flink job IDs (exclude mock/placeholder IDs) ----
            java.util.Set<String> knownRealIds = new java.util.HashSet<>();
            List<JobDefinition> allJobs = jobRepo.findAllByOrderByUpdatedAtDesc();
            for (JobDefinition j : allJobs) {
                String fid = j.getFlinkJobId();
                if (fid != null && !fid.startsWith("flink-job-") && !fid.startsWith("mock-") && !fid.startsWith("sql-submitted-")) {
                    knownRealIds.add(fid);
                }
            }

            // ---- Step 2: Find unclaimed Flink jobs (jid -> start-time) ----
            java.util.Map<String, Long> newJobCandidates = new java.util.LinkedHashMap<>();
            for (JsonNode fj : jobsArray) {
                if (!fj.has("jid")) continue;
                String jid = fj.get("jid").asText();
                if (!knownRealIds.contains(jid)) {
                    long startTime = fj.has("start-time") ? fj.get("start-time").asLong() : 0;
                    newJobCandidates.put(jid, startTime);
                }
            }

            // ---- Step 3: For each active mock/placeholder job, find the best matching real Flink job ----
            for (JobDefinition job : activeJobs) {
                String flinkJobId = job.getFlinkJobId();
                if (flinkJobId == null) continue;
                if (autoStopKafkaJobIfNeeded(job)) continue;

                boolean needsResolution = flinkJobId.startsWith("flink-job-") || flinkJobId.startsWith("mock-") || flinkJobId.startsWith("sql-submitted-");
                if (needsResolution && !newJobCandidates.isEmpty()) {
                    long jobSubmitTime = job.getSubmittedAt() != null ?
                        job.getSubmittedAt().atZone(java.time.ZoneId.systemDefault()).toInstant().toEpochMilli() : 0;
                    String bestJid = null;
                    long bestDiff = Long.MAX_VALUE;
                    for (java.util.Map.Entry<String, Long> entry : newJobCandidates.entrySet()) {
                        long diff = Math.abs(entry.getValue() - jobSubmitTime);
                        if (diff < bestDiff) {
                            bestDiff = diff;
                            bestJid = entry.getKey();
                        }
                    }
                    if (bestJid != null) {
                        log.info("Resolved mock Flink job ID {} -> real Flink job ID {} (diff={}ms)", job.getId(), bestJid, bestDiff);
                        job.setFlinkJobId(bestJid);
                        flinkJobId = bestJid;
                        jobRepo.save(job);
                    }
                }

                boolean jobFoundInFlink = false;
                String foundFlinkState = null;

                for (int t = 0; t < jobsArray.size(); t++) {
                    JsonNode fj = jobsArray.get(t);
                    if (!fj.get("jid").asText().equals(flinkJobId)) continue;

                    jobFoundInFlink = true;
                    String flinkState = fj.has("state") ? fj.get("state").asText() : "";
                    JobStatus localStatus = FLINK_TO_LOCAL_STATUS.get(flinkState);
                    foundFlinkState = flinkState;
                    if (localStatus != null && localStatus != job.getStatus()) {
                        log.info("Job {} status: {} -> {} (Flink: {})", job.getId(), job.getStatus(), localStatus, flinkState);
                        job.setStatus(localStatus);
                        job.setUpdatedAt(LocalDateTime.now());
                        if (localStatus == JobStatus.COMPLETED || localStatus == JobStatus.FAILED) {
                            job.setCompletedAt(LocalDateTime.now());
                        }
                        jobRepo.save(job);
                        JobLog l = new JobLog();
                        l.setJobId(job.getId()); l.setLevel("INFO");
                        l.setMessage("Status: " + localStatus + " (Flink: " + flinkState + ")");
                        l.setTimestamp(LocalDateTime.now());
                        logRepo.save(l);
                        if (localStatus == JobStatus.COMPLETED) {
                            convertExcelOutputsIfNeeded(job);
                            mergeCsvOutputsIfNeeded(job);
                            mergeJsonOutputsIfNeeded(job);
                            convertXmlOutputsIfNeeded(job);
                            convertParquetOutputsIfNeeded(job);
                        }
                        if (localStatus == JobStatus.CANCELLED) {
                            convertExcelOutputsIfNeeded(job);
                            mergeCsvOutputsIfNeeded(job);
                            mergeJsonOutputsIfNeeded(job);
                            convertXmlOutputsIfNeeded(job);
                            convertParquetOutputsIfNeeded(job);
                        }
                        if (localStatus == JobStatus.FAILED) {
                            fetchFlinkJobError(job, flinkJobId);
                        }
                    }
                    break;
                }

                // If job is already FAILED but we haven't fetched errors yet, try fetching now
                if (jobFoundInFlink && "FAILED".equals(foundFlinkState) && job.getStatus() == JobStatus.FAILED) {
                    fetchFlinkJobError(job, flinkJobId);
                }

                // If the job was not found in Flink overview but was in RUNNING/SUBMITTED state,
                // it may have completed and already been removed from the overview
                if (!jobFoundInFlink && !needsResolution) {
                    log.info("Job {} (Flink ID: {}) not found in Flink overview, may have completed", job.getId(), flinkJobId);
                    if (job.getStatus() == JobStatus.RUNNING || job.getStatus() == JobStatus.SUBMITTED) {
                        log.info("Job {} transitioning from {} -> COMPLETED (job not in Flink)", job.getId(), job.getStatus());
                        job.setStatus(JobStatus.COMPLETED);
                        job.setUpdatedAt(LocalDateTime.now());
                        job.setCompletedAt(LocalDateTime.now());
                        jobRepo.save(job);
                        JobLog l = new JobLog();
                        l.setJobId(job.getId()); l.setLevel("INFO");
                        l.setMessage("Status: COMPLETED (job no longer in Flink overview)");
                        l.setTimestamp(LocalDateTime.now());
                        logRepo.save(l);
convertExcelOutputsIfNeeded(job);
                        mergeCsvOutputsIfNeeded(job);
                        mergeJsonOutputsIfNeeded(job);
                        convertXmlOutputsIfNeeded(job);
                        convertParquetOutputsIfNeeded(job);
                    }
                }
            }
            finalizeRecentlyCompletedJobs();
        } catch (Exception e) {
            log.debug("Job status sync: {}", e.getMessage());
        }
    }

    private void convertExcelOutputsIfNeeded(JobDefinition job) {
        try {
            String dagJson = job.getDagJson();
            if (dagJson == null || dagJson.isEmpty()) return;
            JsonNode dagNode = objectMapper.readTree(dagJson);
            JsonNode nodes = dagNode.get("nodes");
            if (nodes == null || !nodes.isArray()) return;

            // 按输出文件分组：path -> LinkedHashMap<sheetName, 合并后CSV>；delimiter 取首个节点配置
            java.util.Map<String, java.util.LinkedHashMap<String, String>> files = new java.util.LinkedHashMap<>();
            java.util.Map<String, String> fileDelimiter = new java.util.HashMap<>();
            java.util.Map<String, java.util.Set<String>> fileTempDirs = new java.util.HashMap<>();
            for (JsonNode node : nodes) {
                String type = node.has("type") ? node.get("type").asText() : "";
                if (!"excel_output".equals(type)) continue;
                JsonNode params = node.get("params");
                if (params == null) continue;
                String actualPath = params.has("path") ? params.get("path").asText().trim() : "";
                actualPath = resolveOutputPath(actualPath);
                if (actualPath.isEmpty()) continue;
                String sheetName = params.has("sheetName") && !params.get("sheetName").asText().trim().isEmpty()
                        ? params.get("sheetName").asText().trim() : "Data";
                String delimiter = params.has("delimiter") ? params.get("delimiter").asText().trim() : ",";
                String tempCsvPath = ExcelOutputConverter.getTempCsvPath(actualPath, sheetName);
                String header = findSourceHeader(dagNode, node.has("id") ? node.get("id").asText() : "", delimiter);
                String mergedCsv = mergeCsvParts(tempCsvPath, header);
                if (mergedCsv == null) {
                    log.warn("Excel output conversion failed, temp CSV at: {}", tempCsvPath);
                    continue;
                }
                files.computeIfAbsent(actualPath, k -> new java.util.LinkedHashMap<>()).put(sheetName, mergedCsv);
                fileTempDirs.computeIfAbsent(actualPath, k -> new java.util.LinkedHashSet<>()).add(tempCsvPath);
                fileDelimiter.putIfAbsent(actualPath, delimiter);
                log.info("Excel output node {} -> file {} sheet {}", node.has("id") ? node.get("id").asText() : "?", actualPath, sheetName);
            }

            for (java.util.Map.Entry<String, java.util.LinkedHashMap<String, String>> entry : files.entrySet()) {
                String actualPath = entry.getKey();
                java.util.LinkedHashMap<String, String> sheets = entry.getValue();
                String delimiter = fileDelimiter.getOrDefault(actualPath, ",");
                boolean success;
                if (sheets.size() == 1) {
                    java.util.Map.Entry<String, String> only = sheets.entrySet().iterator().next();
                    success = excelOutputConverter.convertCsvToExcel(only.getValue(), actualPath, delimiter, true, only.getKey());
                } else {
                    success = excelOutputConverter.convertCsvsToExcel(sheets, actualPath, delimiter, true);
                }
                if (success) {
                    JobLog l = new JobLog();
                    l.setJobId(job.getId()); l.setLevel("INFO");
                    l.setMessage("Excel output converted: " + actualPath + " (" + sheets.size() + " sheets)");
                    l.setTimestamp(LocalDateTime.now());
                    logRepo.save(l);
                } else {
                    JobLog l = new JobLog();
                    l.setJobId(job.getId()); l.setLevel("WARN");
                    l.setMessage("Excel output conversion failed: " + actualPath);
                    l.setTimestamp(LocalDateTime.now());
                    logRepo.save(l);
                }
                if (success) {
                    java.util.Set<String> dirs = fileTempDirs.get(actualPath);
                    if (dirs != null) {
                        for (String d : dirs) {
                            deleteRecursively(new java.io.File(d));
                            deleteRecursively(new java.io.File(d + "_merged.csv"));
                        }
                    }
                }
            }
        } catch (Exception e) {
            log.warn("Failed to convert excel outputs for job {}: {}", job.getId(), e.getMessage());
        }
    }

    /**
     * 合并 temp CSV 目录下所有 part 文件（可选带表头）为单个 CSV 文件
     */
    private String mergeCsvParts(String tempCsvPath, String header) {
        try {
            java.io.File tempDir = new java.io.File(tempCsvPath);
            if (!tempDir.isDirectory()) return null;
            java.io.File[] parts = tempDir.listFiles((d, n) -> n.startsWith("part-") || n.startsWith(".part-"));
            if (parts == null || parts.length == 0) return null;
            java.util.Arrays.sort(parts);
            java.io.File merged = new java.io.File(tempCsvPath + "_merged.csv");
            try (java.io.BufferedOutputStream output = new java.io.BufferedOutputStream(
                    java.nio.file.Files.newOutputStream(merged.toPath()))) {
                if (header != null && !header.trim().isEmpty()) {
                    output.write((header + "\n").getBytes(java.nio.charset.StandardCharsets.UTF_8));
                }
                for (java.io.File part : parts) {
                    java.nio.file.Files.copy(part.toPath(), output);
                }
            }
            return merged.getAbsolutePath();
        } catch (Exception e) {
            log.warn("Failed to merge CSV parts {}: {}", tempCsvPath, e.getMessage());
            return null;
        }
    }

    /**
     * Parquet 输出：作业完成后将临时 CSV 转换为 .parquet
     */
    private void convertParquetOutputsIfNeeded(JobDefinition job) {
        try {
            String dagJson = job.getDagJson();
            if (dagJson == null || dagJson.isEmpty()) return;
            JsonNode dagNode = objectMapper.readTree(dagJson);
            JsonNode nodes = dagNode.get("nodes");
            if (nodes == null || !nodes.isArray()) return;

            for (JsonNode node : nodes) {
                String type = node.has("type") ? node.get("type").asText() : "";
                if (!"parquet_output".equals(type)) continue;
                JsonNode params = node.get("params");
                if (params == null) continue;
                String actualPath = params.has("path") ? params.get("path").asText().trim() : "";
                actualPath = resolveOutputPath(actualPath);
                if (actualPath.isEmpty()) continue;
                String delimiter = params.has("delimiter") ? params.get("delimiter").asText().trim() : ",";
                String tempCsvPath = actualPath.replaceAll("(?i)\\.parquet$", "") + "_temp_csv";
                String header = findSourceHeader(dagNode, node.has("id") ? node.get("id").asText() : "", delimiter);
                String mergedCsv = mergeCsvParts(tempCsvPath, header);
                if (mergedCsv == null) {
                    JobLog w = new JobLog();
                    w.setJobId(job.getId()); w.setLevel("WARN");
                    w.setMessage("Parquet output conversion failed, temp CSV at: " + tempCsvPath);
                    w.setTimestamp(LocalDateTime.now());
                    logRepo.save(w);
                    continue;
                }
                boolean success = parquetOutputConverter.convertCsvToParquet(mergedCsv, actualPath, delimiter);
                if (success) {
                    deleteRecursively(new java.io.File(tempCsvPath));
                    deleteRecursively(new java.io.File(tempCsvPath + "_merged.csv"));
                }
                JobLog l = new JobLog();
                l.setJobId(job.getId()); l.setLevel(success ? "INFO" : "WARN");
                l.setMessage(success ? ("Parquet output converted: " + actualPath) : ("Parquet output conversion failed: " + actualPath));
                l.setTimestamp(LocalDateTime.now());
                logRepo.save(l);
            }
        } catch (Exception e) {
            log.warn("Failed to convert parquet outputs for job {}: {}", job.getId(), e.getMessage());
        }
    }

    private void convertXmlOutputsIfNeeded(JobDefinition job) {
        try {
            String dagJson = job.getDagJson();
            if (dagJson == null || dagJson.isEmpty()) return;
            JsonNode dagNode = objectMapper.readTree(dagJson);
            JsonNode nodes = dagNode.get("nodes");
            if (nodes == null || !nodes.isArray()) return;

            for (JsonNode node : nodes) {
                String type = node.has("type") ? node.get("type").asText() : "";
                if (!"xml_output".equals(type)) continue;

                JsonNode params = node.get("params");
                if (params == null) continue;
                String actualPath = params.has("path") ? params.get("path").asText().trim() : "";
                actualPath = resolveOutputPath(actualPath);
                if (actualPath.isEmpty()) continue;

                String delimiter = params.has("delimiter") ? params.get("delimiter").asText().trim() : ",";
                String rootTag = params.has("rootTag") ? params.get("rootTag").asText().trim() : "root";
                String rowTag = params.has("rowTag") ? params.get("rowTag").asText().trim() : "record";
                String encoding = params.has("encoding") ? params.get("encoding").asText().trim() : "UTF-8";
                String tempCsvPath = XmlOutputConverter.getTempCsvPath(actualPath);

                log.info("Converting CSV to XML for job {}: {} -> {}", job.getId(), tempCsvPath, actualPath);
                String header = findSourceHeader(dagNode, node.has("id") ? node.get("id").asText() : "", delimiter);
                boolean success;
                java.io.File tempDir = new java.io.File(tempCsvPath);
                if (header != null && !header.trim().isEmpty() && tempDir.isDirectory()) {
                    java.io.File[] parts = tempDir.listFiles((d, n) -> n.startsWith("part-") || n.startsWith(".part-"));
                    if (parts != null && parts.length > 0) {
                        java.util.Arrays.sort(parts);
                        StringBuilder hdrSb = new StringBuilder(header).append("\n");
                        for (java.io.File pf : parts) {
                            hdrSb.append(new String(java.nio.file.Files.readAllBytes(pf.toPath()), java.nio.charset.StandardCharsets.UTF_8));
                        }
                        java.io.File hdrFile = new java.io.File(actualPath + "_hdr.csv");
                        java.nio.file.Files.write(hdrFile.toPath(), hdrSb.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8));
                        success = xmlOutputConverter.convertCsvToXml(hdrFile.getAbsolutePath(), actualPath, delimiter, rootTag, rowTag, encoding);
                        hdrFile.delete();
                    } else {
                        success = xmlOutputConverter.convertCsvToXml(tempCsvPath, actualPath, delimiter, rootTag, rowTag, encoding);
                    }
                } else {
                    success = xmlOutputConverter.convertCsvToXml(tempCsvPath, actualPath, delimiter, rootTag, rowTag, encoding);
                }

                if (success) {
                deleteRecursively(new java.io.File(tempCsvPath));
                    JobLog l = new JobLog();
                    l.setJobId(job.getId()); l.setLevel("INFO");
                    l.setMessage("XML output converted: " + actualPath);
                    l.setTimestamp(LocalDateTime.now());
                    logRepo.save(l);
                } else {
                    JobLog l = new JobLog();
                    l.setJobId(job.getId()); l.setLevel("WARN");
                    l.setMessage("XML output conversion failed, temp CSV at: " + tempCsvPath);
                    l.setTimestamp(LocalDateTime.now());
                    logRepo.save(l);
                }
            }
        } catch (Exception e) {
            log.warn("Failed to convert xml outputs for job {}: {}", job.getId(), e.getMessage());
        }
    }

    private String findSourceHeader(JsonNode dagNode, String targetNodeId, String delimiter) {
        try {
            JsonNode edges = dagNode.get("edges");
            JsonNode nodes = dagNode.get("nodes");
            if (edges == null || nodes == null || targetNodeId == null) return null;
            Set<String> transformTypes = Set.of(
                    "field_filter", "field_rename", "row_filter", "json_parse", "xml_json", "field_concat",
                    "dedupe", "validate", "route");
            // 沿边从输出节点向上收集节点链：输入节点在链尾，transform 依次在前
            List<String> chain = new ArrayList<>();
            String cur = targetNodeId;
            boolean isInput = false;
            for (int hop = 0; hop < 10 && !isInput; hop++) {
                String srcId = null;
                for (JsonNode e : edges) {
                    if (e.has("target") && cur.equals(e.get("target").asText())) { srcId = e.get("source").asText(); break; }
                }
                if (srcId == null) return null;
                String type = findNodeType(nodes, srcId);
                if (type == null) return null;
                chain.add(srcId);
                if (transformTypes.contains(type)) {
                    cur = srcId;
                } else {
                    isInput = true;
                }
            }
            if (chain.isEmpty()) return null;
            // 输入节点是链尾，先取它的字段列表
            String inputId = chain.get(chain.size() - 1);
            List<String> fields = sourceHeaderFields(nodes, inputId, delimiter);
            if (fields == null) return null;
            // 再从输入往输出方向应用 transform，得到精确表头
            for (int i = chain.size() - 2; i >= 0; i--) {
                fields = applyTransformHeader(nodes, chain.get(i), fields);
                if (fields == null) return null;
            }
            if (fields.isEmpty()) return null;
            return String.join(delimiter, fields);
        } catch (Exception e) {
            log.warn("findSourceHeader error: {}", e.getMessage());
        }
        return null;
    }

    private String findNodeType(JsonNode nodes, String nodeId) {
        for (JsonNode n : nodes) {
            if (n.has("id") && nodeId.equals(n.get("id").asText())) {
                return n.has("type") ? n.get("type").asText() : "";
            }
        }
        return null;
    }

    private List<String> sourceHeaderFields(JsonNode nodes, String inputId, String delimiter) {
        try {
            for (JsonNode n : nodes) {
                if (!n.has("id") || !inputId.equals(n.get("id").asText())) continue;
                String type = n.has("type") ? n.get("type").asText() : "";
                JsonNode params = n.get("params");
                if (params == null) return null;
                if ("csv_input".equals(type) || "excel_input".equals(type) || "xml_input".equals(type) || "json_input".equals(type)) {
                    if ("json_input".equals(type)) {
                        String mode = params.has("mode") ? params.get("mode").asText() : "auto";
                        boolean arrayMode = "array".equalsIgnoreCase(mode)
                                || ("auto".equalsIgnoreCase(mode) && JsonPreprocessor.isJsonArrayFile(params.has("path") ? params.get("path").asText().trim() : ""));
                        if (!arrayMode) {
                            String fc = params.has("fieldsConfig") ? params.get("fieldsConfig").asText() : "[]";
                            return fieldsConfigNameList(fc);
                        }
                    }
                    String hasHeader = params.has("hasHeader") ? params.get("hasHeader").asText() : "true";
                    if (!"true".equalsIgnoreCase(hasHeader)) return null;
                    String p = params.has("path") ? params.get("path").asText().trim() : "";
                    String filePath = p;
                    if ("excel_input".equals(type)) {
                        filePath = p.replaceAll("(?i)\\.(xlsx|xls)$", "") + "_converted.csv";
                    } else if ("xml_input".equals(type)) {
                        filePath = p.replaceAll("(?i)\\.xml$", "") + "_converted.csv";
                    } else if ("json_input".equals(type)) {
                        filePath = p.replaceAll("(?i)\\.json$", "") + "_converted.csv";
                    }
                    String delim = params.has("delimiter") ? params.get("delimiter").asText().trim() : ",";
                    java.io.File f = new java.io.File(filePath);
                    if (!f.isFile()) return null;
                    try (java.io.BufferedReader r = java.nio.file.Files.newBufferedReader(f.toPath(), java.nio.charset.StandardCharsets.UTF_8)) {
                        String line = r.readLine();
                        if (line == null) return null;
                        return parseHeaderList(line, delim);
                    }
                } else if ("parquet_input".equals(type)) {
                    String pqPath = params.has("path") ? params.get("path").asText().trim() : "";
                    String pqCsv = pqPath.replaceAll("(?i)\\.parquet$", "") + "_converted.csv";
                    String pqDelim = params.has("delimiter") ? params.get("delimiter").asText().trim() : ",";
                    java.io.File pqFile = new java.io.File(pqCsv);
                    if (!pqFile.isFile()) return null;
                    try (java.io.BufferedReader pqReader = java.nio.file.Files.newBufferedReader(pqFile.toPath(), java.nio.charset.StandardCharsets.UTF_8)) {
                        String pqLine = pqReader.readLine();
                        if (pqLine == null) return null;
                        return parseHeaderList(pqLine, pqDelim);
                    }
                } else if ("mysql_input".equals(type)) {
                    String url = params.has("url") ? params.get("url").asText().trim() : "";
                    String table = params.has("table") ? params.get("table").asText().trim() : "";
                    String username = params.has("username") ? params.get("username").asText().trim() : "root";
                    String password = params.has("password") ? params.get("password").asText() : "";
                    if (url.isEmpty() || table.isEmpty()) return null;
                    return mysqlColumnNameList(url, table, username, password);
                } else if ("datagen_input".equals(type) || "kafka_input".equals(type)) {
                    String fc = params.has("fieldsConfig") ? params.get("fieldsConfig").asText() : "[]";
                    return fieldsConfigNameList(fc);
                }
                return null;
            }
        } catch (Exception e) {
            log.warn("sourceHeaderFields error: {}", e.getMessage());
            return null;
        }
        return null;
    }

    private List<String> applyTransformHeader(JsonNode nodes, String transformId, List<String> fields) {
        for (JsonNode n : nodes) {
            if (!n.has("id") || !transformId.equals(n.get("id").asText())) continue;
            String type = n.has("type") ? n.get("type").asText() : "";
            JsonNode params = n.get("params");
            if ("field_filter".equals(type)) {
                String keepRaw = params != null && params.has("fields") ? params.get("fields").asText() : "";
                List<String> keep = parseHeaderList(keepRaw, ",");
                if (keep == null) return fields;
                List<String> out = new ArrayList<>();
                for (String f : fields) {
                    if (keep.contains(f)) out.add(f);
                }
                return out;
            } else if ("field_rename".equals(type)) {
                String mapRaw = params != null && params.has("mappings") ? params.get("mappings").asText() : "";
                java.util.Map<String, String> m = new java.util.LinkedHashMap<>();
                for (String part : mapRaw.split(",")) {
                    String p = part.trim();
                    if (p.isEmpty()) continue;
                    int eq = p.indexOf('=');
                    if (eq > 0 && eq < p.length() - 1) m.put(p.substring(0, eq).trim(), p.substring(eq + 1).trim());
                }
                List<String> out = new ArrayList<>();
                for (String f : fields) out.add(m.getOrDefault(f, f));
                return out;
            } else if ("json_parse".equals(type)) {
                String fc = params != null && params.has("fieldsConfig") ? params.get("fieldsConfig").asText() : "[]";
                List<String> extra = fieldsConfigNameList(fc);
                if (extra == null) return fields;
                List<String> out = new ArrayList<>(fields);
                for (String e : extra) {
                    if (!out.contains(e)) out.add(e);
                }
                return out;
            } else if ("field_concat".equals(type)) {
                String nfn = params != null && params.has("newFieldName") ? params.get("newFieldName").asText().trim() : "concat_field";
                if (nfn.isEmpty()) return fields;
                List<String> out = new ArrayList<>(fields);
                if (!out.contains(nfn)) out.add(nfn);
                return out;
            }
            // row_filter / xml_json：schema 透传
            return fields;
        }
        return fields;
    }

    private List<String> parseHeaderList(String line, String delimiter) {
        if (line == null || line.isEmpty()) return null;
        List<String> out = new ArrayList<>();
        for (String part : line.split(java.util.regex.Pattern.quote(delimiter))) {
            String t = part.trim();
            if (t.length() >= 2 && t.startsWith("\"") && t.endsWith("\"")) t = t.substring(1, t.length() - 1);
            if (!t.isEmpty()) out.add(t);
        }
        return out.isEmpty() ? null : out;
    }

    private List<String> fieldsConfigNameList(String fieldsConfig) {
        try {
            if (fieldsConfig == null || fieldsConfig.trim().isEmpty()) return null;
            JsonNode arr = objectMapper.readTree(fieldsConfig);
            if (arr == null || !arr.isArray() || arr.size() == 0) return null;
            List<String> names = new ArrayList<>();
            for (JsonNode f : arr) {
                if (f.has("name")) names.add(f.get("name").asText());
            }
            return names.isEmpty() ? null : names;
        } catch (Exception e) {
            log.warn("fieldsConfig header parse error: {}", e.getMessage());
            return null;
        }
    }

    private List<String> mysqlColumnNameList(String url, String table, String username, String password) {
        String quoted = "\u0060" + table.replace("\u0060", "\u0060\u0060") + "\u0060";
        try (Connection conn = DriverManager.getConnection(url, username, password);
             Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT * FROM " + quoted + " LIMIT 0")) {
            ResultSetMetaData md = rs.getMetaData();
            List<String> names = new ArrayList<>();
            for (int i = 1; i <= md.getColumnCount(); i++) names.add(md.getColumnLabel(i));
            return names.isEmpty() ? null : names;
        } catch (Exception e) {
            log.warn("mysql header query error: {}", e.getMessage());
            return null;
        }
    }

    private void prependSourceHeaderIfNeeded(JsonNode dagNode, String targetNodeId, String delimiter, java.io.File out) {
        try {
            if (!out.exists() || out.length() == 0) return;
            String header = findSourceHeader(dagNode, targetNodeId, delimiter);
            if (header == null || header.trim().isEmpty()) return;
            String firstLine;
            try (java.io.BufferedReader r = java.nio.file.Files.newBufferedReader(out.toPath(), java.nio.charset.StandardCharsets.UTF_8)) {
                firstLine = r.readLine();
            }
            if (header.equals(firstLine)) return;
            byte[] existing = java.nio.file.Files.readAllBytes(out.toPath());
            String content = new String(existing, java.nio.charset.StandardCharsets.UTF_8);
            java.nio.file.Files.write(out.toPath(), (header + "\n" + content).getBytes(java.nio.charset.StandardCharsets.UTF_8));
            log.info("Prepend source header to CSV output: {}", out.getAbsolutePath());
        } catch (Exception e) {
            log.warn("prependSourceHeaderIfNeeded error: {}", e.getMessage());
        }
    }

    /** Docker 中兼容历史作业保存的 Windows 输出绝对路径。 */
    private String resolveOutputPath(String path) {
        if (path == null || path.isBlank() || java.io.File.separatorChar == '\\'
                || !path.matches("^[A-Za-z]:[\\\\/].*")) {
            return path;
        }
        String normalized = path.replace('\\', '/');
        String mapped = "/output/" + normalized.substring(normalized.lastIndexOf('/') + 1);
        log.info("Mapped output path for container runtime: {} -> {}", path, mapped);
        return mapped;
    }

    /**
     * Merge Flink filesystem sink part-* files into the user-specified single CSV file.
     */
    private void deleteRecursively(java.io.File f) {
        if (f == null || !f.exists()) return;
        if (f.isDirectory()) {
            java.io.File[] children = f.listFiles();
            if (children != null) {
                for (java.io.File c : children) deleteRecursively(c);
            }
        }
        f.delete();
    }

    /**
     * 最近 3 分钟内完成的作业再跑一轮输出合并：处理多输出链路中个别 part 文件延迟落盘的竞态
     */
    /**
     * 判断作业是否仍有待合并的输出临时目录（避免对已完成合并的作业重复扫描）
     */
    private boolean hasPendingOutputs(JobDefinition job) {
        try {
            if (job.getDagJson() == null || job.getDagJson().isEmpty()) return false;
            JsonNode dagNode = objectMapper.readTree(job.getDagJson());
            JsonNode nodes = dagNode.get("nodes");
            if (nodes == null || !nodes.isArray()) return false;
            for (JsonNode node : nodes) {
                String type = node.has("type") ? node.get("type").asText() : "";
                if (!type.endsWith("_output")) continue;
                if (node.get("params") == null || !node.get("params").has("path")) continue;
                String path = node.get("params").get("path").asText().trim();
                path = resolveOutputPath(path);
                if (path.isEmpty()) continue;
                if ("csv_output".equals(type) || "json_output".equals(type)) {
                    if (new java.io.File(path + ".tmp").isDirectory()) return true;
                    continue;
                }
                if ("excel_output".equals(type) || "xml_output".equals(type) || "parquet_output".equals(type)) {
                    String base = path.replaceAll("(?i)\\.(xlsx|xls|xml|parquet)$", "");
                    java.io.File baseTemp = new java.io.File(base + "_temp_csv");
                    if (baseTemp.isDirectory()) return true;
                    java.io.File parent = baseTemp.getParentFile();
                    if (parent != null && parent.isDirectory()) {
                        String prefix = baseTemp.getName();
                        java.io.File[] sib = parent.listFiles((d, n) -> n.equals(prefix) || n.startsWith(prefix + "_"));
                        if (sib != null && sib.length > 0) return true;
                    }
                }
            }
            return false;
        } catch (Exception e) {
            return false;
        }
    }

    private void finalizeRecentlyCompletedJobs() {
        try {
            List<JobDefinition> recent = jobRepo.findByStatusAndCompletedAtAfter(JobStatus.COMPLETED, LocalDateTime.now().minusSeconds(180));
            if (recent.isEmpty()) return;
            for (JobDefinition job : recent) {
                if (!hasPendingOutputs(job)) continue;
                convertExcelOutputsIfNeeded(job);
                mergeCsvOutputsIfNeeded(job);
                mergeJsonOutputsIfNeeded(job);
                convertXmlOutputsIfNeeded(job);
                convertParquetOutputsIfNeeded(job);
            }
        } catch (Exception e) {
            log.debug("Finalize recently completed jobs: {}", e.getMessage());
        }
    }

    private void mergeCsvOutputsIfNeeded(JobDefinition job) {
        try {
            String dagJson = job.getDagJson();
            if (dagJson == null || dagJson.isEmpty()) return;
            JsonNode dagNode = objectMapper.readTree(dagJson);
            JsonNode nodes = dagNode.get("nodes");
            if (nodes == null || !nodes.isArray()) return;

            for (JsonNode node : nodes) {
                String type = node.has("type") ? node.get("type").asText() : "";
                if (!"csv_output".equals(type)) continue;

                JsonNode params = node.get("params");
                if (params == null) continue;
                String actualPath = params.has("path") ? params.get("path").asText().trim() : "";
                actualPath = resolveOutputPath(actualPath);
                if (actualPath.isEmpty()) continue;

                String tempDir = actualPath + ".tmp";
                java.io.File dir = new java.io.File(tempDir);
                if (!dir.isDirectory()) {
                    log.warn("CSV output temp dir not found for job {}: {}", job.getId(), tempDir);
                    continue;
                }
                java.io.File[] parts = dir.listFiles((d, n) -> n.startsWith("part-") || n.startsWith(".part-"));
                if (parts == null || parts.length == 0) {
                    log.warn("No part files in temp dir for job {}: {}", job.getId(), tempDir);
                    continue;
                }
                java.util.Arrays.sort(parts);
                StringBuilder sb = new StringBuilder();
                for (java.io.File pf : parts) {
                    sb.append(new String(java.nio.file.Files.readAllBytes(pf.toPath()), java.nio.charset.StandardCharsets.UTF_8));
                }
                java.io.File out = new java.io.File(actualPath);
                if (out.exists()) out.delete();
                java.nio.file.Files.write(out.toPath(), sb.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8));
                String delimiter = params.has("delimiter") ? params.get("delimiter").asText().trim() : ",";
                prependSourceHeaderIfNeeded(dagNode, node.has("id") ? node.get("id").asText() : "", delimiter, out);
                deleteRecursively(dir);

                JobLog l = new JobLog();
                l.setJobId(job.getId()); l.setLevel("INFO");
                l.setMessage("CSV output merged: " + actualPath + " (" + parts.length + " part file(s))");
                l.setTimestamp(LocalDateTime.now());
                logRepo.save(l);
                log.info("Merged {} part files into {} for job {}", parts.length, actualPath, job.getId());
            }
        } catch (Exception e) {
            log.warn("Failed to merge csv outputs for job {}: {}", job.getId(), e.getMessage());
        }
    }

    /**
     * Finalize outputs for a cancelled job: merge CSV/JSON part files and
     * convert Excel/XML/Parquet, so streaming jobs still produce files on cancel.
     */
    public void finalizeJobOutputs(JobDefinition job) {
        try {
            convertExcelOutputsIfNeeded(job);
            mergeCsvOutputsIfNeeded(job);
            mergeJsonOutputsIfNeeded(job);
            convertXmlOutputsIfNeeded(job);
            convertParquetOutputsIfNeeded(job);
        } catch (Exception e) {
            log.warn("finalizeJobOutputs error for job {}: {}", job.getId(), e.getMessage());
        }
    }

    /**
     * 体验模式：Kafka 输入作业（autoStop=true）运行超过 stopAfterSeconds 后自动停止，
     * 并合并输出文件，方便本地测试查看结果（Kafka 是无界流，不会自然结束）。
     */
    private boolean autoStopKafkaJobIfNeeded(JobDefinition job) {
        try {
            if (job.getStatus() != JobStatus.RUNNING && job.getStatus() != JobStatus.SUBMITTED) return false;
            String flinkJobId = job.getFlinkJobId();
            if (flinkJobId == null || flinkJobId.startsWith("mock-") || flinkJobId.startsWith("flink-job-")) return false;
            String dagJson = job.getDagJson();
            if (dagJson == null || dagJson.isEmpty()) return false;
            JsonNode dagNode = objectMapper.readTree(dagJson);
            JsonNode nodes = dagNode.get("nodes");
            if (nodes == null || !nodes.isArray()) return false;
            boolean kafkaAutoStop = false;
            long stopAfterSeconds = 30;
            for (JsonNode node : nodes) {
                if (!"kafka_input".equals(node.has("type") ? node.get("type").asText() : "")) continue;
                JsonNode params = node.get("params");
                if (params == null) continue;
                JsonNode as = params.get("autoStop");
                if (as != null && as.asBoolean(false)) {
                    kafkaAutoStop = true;
                    JsonNode ss = params.get("stopAfterSeconds");
                    if (ss != null && ss.canConvertToLong()) stopAfterSeconds = Math.max(1, ss.asLong());
                }
            }
            if (!kafkaAutoStop) return false;
            java.time.LocalDateTime submitTime = job.getSubmittedAt() != null ? job.getSubmittedAt() : job.getUpdatedAt();
            if (submitTime == null) return false;
            long elapsed = java.time.Duration.between(submitTime, LocalDateTime.now()).getSeconds();
            if (elapsed < stopAfterSeconds) return false;

            log.info("Kafka auto-stop: job {} ran {}s >= {}s, cancelling Flink job {}", job.getId(), elapsed, stopAfterSeconds, flinkJobId);
            dagTranslationService.cancelFlinkJob(flinkJobId);
            job.setStatus(JobStatus.CANCELLED);
            job.setUpdatedAt(LocalDateTime.now());
            job.setCompletedAt(LocalDateTime.now());
            jobRepo.save(job);
            JobLog l = new JobLog();
            l.setJobId(job.getId()); l.setLevel("INFO");
            l.setMessage("体验模式自动停止：Kafka 输入作业运行 " + elapsed + "s，已取消并合并输出");
            l.setTimestamp(LocalDateTime.now());
            logRepo.save(l);
            convertExcelOutputsIfNeeded(job);
            mergeCsvOutputsIfNeeded(job);
            mergeJsonOutputsIfNeeded(job);
            convertXmlOutputsIfNeeded(job);
            convertParquetOutputsIfNeeded(job);
            return true;
        } catch (Exception e) {
            log.warn("autoStopKafkaJobIfNeeded error for job {}: {}", job.getId(), e.getMessage());
            return false;
        }
    }

    /**
     * Fetch Flink job error details and save as ERROR log
     */
    private void fetchFlinkJobError(JobDefinition job, String flinkJobId) {
        if (flinkJobId == null || flinkJobId.startsWith("flink-job-") || flinkJobId.startsWith("mock-")) {
            return;
        }
        // Check if we already have an ERROR log for this job (avoid duplicate fetches)
        java.util.List<JobLog> existingErrors = logRepo.findByJobIdAndLevelOrderByTimestampDesc(job.getId(), "ERROR");
        for (JobLog el : existingErrors) {
            if (el.getMessage() != null && el.getMessage().startsWith("Flink error:")) {
                log.debug("Job {} already has error info, skipping fetch", job.getId());
                return;
            }
        }
        try {
            var excHc = java.net.http.HttpClient.newHttpClient();
            String excUrl = "http://" + flinkHost + ":" + flinkPort + "/jobs/" + flinkJobId + "/exceptions";
            java.net.http.HttpResponse<String> excResponse = excHc.send(
                java.net.http.HttpRequest.newBuilder()
                    .uri(java.net.URI.create(excUrl))
                    .GET().build(),
                java.net.http.HttpResponse.BodyHandlers.ofString()
            );
            if (excResponse.statusCode() != 200) {
                log.warn("Flink exceptions endpoint returned {} for job {}, may have been cleaned up",
                    excResponse.statusCode(), flinkJobId);
                JobLog el = new JobLog();
                el.setJobId(job.getId());
                el.setLevel("ERROR");
                el.setMessage("Flink error: Job " + flinkJobId + " no longer available in cluster (HTTP " + excResponse.statusCode() + ")");
                el.setTimestamp(java.time.LocalDateTime.now());
                logRepo.save(el);
                return;
            }
            com.fasterxml.jackson.databind.JsonNode excRoot = objectMapper.readTree(excResponse.body());
            String rootExc = null;
            if (excRoot.has("root-exception")) {
                rootExc = excRoot.get("root-exception").asText();
            } else if (excRoot.has("errors") && excRoot.get("errors").isArray() && excRoot.get("errors").size() > 0) {
                rootExc = excRoot.get("errors").get(0).asText();
            }
            if (rootExc != null && !rootExc.isEmpty() && !"null".equals(rootExc)) {
                JobLog el = new JobLog();
                el.setJobId(job.getId());
                el.setLevel("ERROR");
                el.setMessage("Flink error: " + (rootExc.length() > 2000 ? rootExc.substring(0, 2000) : rootExc));
                el.setTimestamp(java.time.LocalDateTime.now());
                logRepo.save(el);
                log.info("Saved Flink error detail for job {}: {}...", job.getId(),
                    rootExc.length() > 100 ? rootExc.substring(0, 100) : rootExc);
            }
        } catch (Exception exc) {
            log.warn("Failed to fetch Flink exceptions for job {}: {}", job.getId(), exc.getMessage());
        }
    }
    private void mergeJsonOutputsIfNeeded(JobDefinition job) {
        try {
            String dagJson = job.getDagJson();
            if (dagJson == null || dagJson.isEmpty()) return;
            JsonNode dagNode = objectMapper.readTree(dagJson);
            JsonNode nodes = dagNode.get("nodes");
            if (nodes == null || !nodes.isArray()) return;

            for (JsonNode node : nodes) {
                String type = node.has("type") ? node.get("type").asText() : "";
                if (!"json_output".equals(type)) continue;

                JsonNode params = node.get("params");
                if (params == null) continue;
                String actualPath = params.has("path") ? params.get("path").asText().trim() : "";
                actualPath = resolveOutputPath(actualPath);
                if (actualPath.isEmpty()) continue;

                String tempDir = actualPath + ".tmp";
                java.io.File dir = new java.io.File(tempDir);
                if (!dir.isDirectory()) {
                    log.warn("JSON output temp dir not found for job {}: {}", job.getId(), tempDir);
                    continue;
                }
                java.io.File[] parts = dir.listFiles((d, n) -> n.startsWith("part-") || n.startsWith(".part-"));
                if (parts == null || parts.length == 0) {
                    log.warn("No part files in temp dir for job {}: {}", job.getId(), tempDir);
                    continue;
                }
                java.util.Arrays.sort(parts);
                StringBuilder sb = new StringBuilder();
                for (java.io.File pf : parts) {
                    sb.append(new String(java.nio.file.Files.readAllBytes(pf.toPath()), java.nio.charset.StandardCharsets.UTF_8));
                }
                String mode = params.has("mode") ? params.get("mode").asText().trim() : "lines";
                String content;
                if ("array".equalsIgnoreCase(mode)) {
                    // JSON Lines -> JSON 数组：[line1, line2, ...]
                    java.util.List<String> jsonLines = new java.util.ArrayList<>();
                    for (String ln : sb.toString().split("\n")) {
                        String t = ln.trim();
                        if (!t.isEmpty()) jsonLines.add(t);
                    }
                    content = "[\n  " + String.join(",\n  ", jsonLines) + "\n]\n";
                } else {
                    content = sb.toString();
                }
                java.io.File out = new java.io.File(actualPath);
                if (out.exists()) out.delete();
                java.nio.file.Files.write(out.toPath(), content.getBytes(java.nio.charset.StandardCharsets.UTF_8));
                deleteRecursively(dir);
                JobLog l = new JobLog();
                l.setJobId(job.getId()); l.setLevel("INFO");
                l.setMessage("JSON output merged: " + actualPath + " (" + parts.length + " part file(s))");
                l.setTimestamp(LocalDateTime.now());
                logRepo.save(l);
                log.info("Merged {} JSON part files into {} for job {}", parts.length, actualPath, job.getId());
            }
        } catch (Exception e) {
            log.warn("Failed to merge json outputs for job {}: {}", job.getId(), e.getMessage());
        }
    }

}
