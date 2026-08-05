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

@Slf4j
@Service
@RequiredArgsConstructor
public class FlinkJobStatusChecker {

    private final JobDefinitionRepository jobRepo;
    private final JobLogRepository logRepo;
    private final ObjectMapper objectMapper;
    private final ExcelOutputConverter excelOutputConverter;

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
                    }
                }
            }
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

            for (JsonNode node : nodes) {
                String type = node.has("type") ? node.get("type").asText() : "";
                if (!"excel_output".equals(type)) continue;

                JsonNode params = node.get("params");
                if (params == null) continue;
                String actualPath = params.has("path") ? params.get("path").asText().trim() : "";
                if (actualPath.isEmpty()) continue;

                String delimiter = params.has("delimiter") ? params.get("delimiter").asText().trim() : ",";
                String tempCsvPath = ExcelOutputConverter.getTempCsvPath(actualPath);

                log.info("Converting CSV to Excel for job {}: {} -> {}", job.getId(), tempCsvPath, actualPath);
                String header = findSourceHeader(dagNode, node.has("id") ? node.get("id").asText() : "");
                boolean success;
                java.io.File tempDir = new java.io.File(tempCsvPath);
                if (header != null && !header.trim().isEmpty() && tempDir.isDirectory()) {
                    java.io.File[] parts = tempDir.listFiles((d, n) -> n.startsWith("part-"));
                    if (parts != null && parts.length > 0) {
                        java.util.Arrays.sort(parts);
                        StringBuilder hdrSb = new StringBuilder(header).append("\n");
                        for (java.io.File pf : parts) {
                            hdrSb.append(new String(java.nio.file.Files.readAllBytes(pf.toPath()), java.nio.charset.StandardCharsets.UTF_8));
                        }
                        java.io.File hdrFile = new java.io.File(actualPath + "_hdr.csv");
                        java.nio.file.Files.write(hdrFile.toPath(), hdrSb.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8));
                        success = excelOutputConverter.convertCsvToExcel(hdrFile.getAbsolutePath(), actualPath, delimiter, true);
                        hdrFile.delete();
                    } else {
                        success = excelOutputConverter.convertCsvToExcel(tempCsvPath, actualPath, delimiter, true);
                    }
                } else {
                    success = excelOutputConverter.convertCsvToExcel(tempCsvPath, actualPath, delimiter, true);
                }

                if (success) {
                    JobLog l = new JobLog();
                    l.setJobId(job.getId()); l.setLevel("INFO");
                    l.setMessage("Excel output converted: " + actualPath);
                    l.setTimestamp(LocalDateTime.now());
                    logRepo.save(l);
                } else {
                    JobLog l = new JobLog();
                    l.setJobId(job.getId()); l.setLevel("WARN");
                    l.setMessage("Excel output conversion failed, temp CSV at: " + tempCsvPath);
                    l.setTimestamp(LocalDateTime.now());
                    logRepo.save(l);
                }
            }
        } catch (Exception e) {
            log.warn("Failed to convert excel outputs for job {}: {}", job.getId(), e.getMessage());
        }
    }

    private String findSourceHeader(JsonNode dagNode, String targetNodeId) {
        try {
            JsonNode edges = dagNode.get("edges");
            JsonNode nodes = dagNode.get("nodes");
            if (edges == null || nodes == null || targetNodeId == null) return null;
            String srcId = null;
            for (JsonNode e : edges) {
                if (e.has("target") && targetNodeId.equals(e.get("target").asText())) { srcId = e.get("source").asText(); break; }
            }
            if (srcId == null) return null;
            for (JsonNode n : nodes) {
                if (!n.has("id") || !srcId.equals(n.get("id").asText())) continue;
                String type = n.has("type") ? n.get("type").asText() : "";
                JsonNode params = n.get("params");
                if (params == null) return null;
                String hasHeader = params.has("hasHeader") ? params.get("hasHeader").asText() : "true";
                if (!"true".equalsIgnoreCase(hasHeader)) return null;
                if ("csv_input".equals(type)) {
                    String p = params.has("path") ? params.get("path").asText().trim() : "";
                    java.io.File f = new java.io.File(p);
                    if (!f.isFile()) return null;
                    try (java.io.BufferedReader r = java.nio.file.Files.newBufferedReader(f.toPath(), java.nio.charset.StandardCharsets.UTF_8)) {
                        return r.readLine();
                    }
                } else if ("excel_input".equals(type)) {
                    String p = params.has("path") ? params.get("path").asText().trim() : "";
                    String csvPath = p.replaceAll("(?i)\\.(xlsx|xls)$", "") + "_converted.csv";
                    java.io.File f = new java.io.File(csvPath);
                    if (!f.isFile()) return null;
                    try (java.io.BufferedReader r = java.nio.file.Files.newBufferedReader(f.toPath(), java.nio.charset.StandardCharsets.UTF_8)) {
                        return r.readLine();
                    }
                }
                return null;
            }
        } catch (Exception e) {
            log.warn("findSourceHeader error: {}", e.getMessage());
        }
        return null;
    }

    private void prependSourceHeaderIfNeeded(JsonNode dagNode, String targetNodeId, java.io.File out) {
        try {
            if (!out.exists() || out.length() == 0) return;
            String header = findSourceHeader(dagNode, targetNodeId);
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

    /**
     * Merge Flink filesystem sink part-* files into the user-specified single CSV file.
     */
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
                if (actualPath.isEmpty()) continue;

                String tempDir = actualPath + ".tmp";
                java.io.File dir = new java.io.File(tempDir);
                if (!dir.isDirectory()) {
                    log.warn("CSV output temp dir not found for job {}: {}", job.getId(), tempDir);
                    continue;
                }
                java.io.File[] parts = dir.listFiles((d, n) -> n.startsWith("part-"));
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
                prependSourceHeaderIfNeeded(dagNode, node.has("id") ? node.get("id").asText() : "", out);

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
                if (actualPath.isEmpty()) continue;

                String tempDir = actualPath + ".tmp";
                java.io.File dir = new java.io.File(tempDir);
                if (!dir.isDirectory()) {
                    log.warn("JSON output temp dir not found for job {}: {}", job.getId(), tempDir);
                    continue;
                }
                java.io.File[] parts = dir.listFiles((d, n) -> n.startsWith("part-"));
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
