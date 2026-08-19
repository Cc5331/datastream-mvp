package com.datastream.mvp.service;

import com.datastream.mvp.dag.DagDefinition;
import com.datastream.mvp.model.ControlRegistry;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.*;
import java.sql.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.TimeUnit;
import com.datastream.mvp.service.ExcelOutputConverter;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

@Slf4j
@Service
@RequiredArgsConstructor
public class DagTranslationService {

    private final ControlRegistryService controlService;
    private final ObjectMapper objectMapper;
    private final ExcelPreprocessor excelPreprocessor;
    private final MysqlTableCreator mysqlTableCreator;
    private final XmlPreprocessor xmlPreprocessor;
    private final JsonPreprocessor jsonPreprocessor;
    private final ParquetPreprocessor parquetPreprocessor;

    @Value("${flink.home:D:\\code\\flink-1.18.1}")
    private String flinkHome;

    @Value("${flink.cluster.host:localhost}")
    private String flinkHost;

    @Value("${flink.cluster.port:8081}")
    private int flinkPort;

    @Value("${flink.sql-gateway.host:}")
    private String sqlGatewayHost;

    @Value("${flink.sql-gateway.port:8083}")
    private int sqlGatewayPort;

    @Value("${app.mysql.default-username:root}")
    private String defaultMysqlUsername;

    @Value("${app.mysql.default-password:}")
    private String defaultMysqlPassword;

    public String translate(DagDefinition dag) {
        cleanOutputTempDirs(dag);
        StringBuilder flinkSql = new StringBuilder();
        flinkSql.append("-- DAG Job: ").append(dag.getJobName()).append("\n");
        flinkSql.append("SET 'parallelism.default' = '").append(dag.getParallelism()).append("';\n\n");
        boolean hasDedupe = dag.getNodes().stream().anyMatch(n -> "dedupe".equals(n.getType()));
        if (hasDedupe) {
            flinkSql.append("SET 'execution.runtime-mode' = 'BATCH';\n\n");
        }
        boolean hasXmlJson = dag.getNodes().stream().anyMatch(n -> "xml_json".equals(n.getType()));
        if (hasXmlJson) {
            flinkSql.append("CREATE FUNCTION IF NOT EXISTS xml2json AS 'com.datastream.udf.XmlToJson' LANGUAGE JAVA;\n");
            flinkSql.append("CREATE FUNCTION IF NOT EXISTS json2xml AS 'com.datastream.udf.JsonToXml' LANGUAGE JAVA;\n\n");
        }

        List<DagDefinition.DagNode> sortedNodes = topologicalSort(dag);
        Map<String, String> tableAlias = new HashMap<>();
        Map<String, String> nodeSchemas = new HashMap<>();

        for (DagDefinition.DagNode node : sortedNodes) {
            ControlRegistry control = controlService.findByType(node.getType());
            String template = control.getFlinkTemplate();

            if ("datagen_input".equals(node.getType())) {
                String rps = node.getParams() != null ? node.getParams().getOrDefault("rowsPerSecond", "10").toString() : "10";
                String fc = node.getParams() != null ? node.getParams().getOrDefault("fieldsConfig", "[]").toString() : "[]";
                String rendered = generateDatagenDDL(sanitize(node.getId()), rps, fc);
                nodeSchemas.put(node.getId(), extractFieldsFromDatagenConfig(fc));
                log.debug("Stored schema for node {}: {}", node.getId(), nodeSchemas.get(node.getId()));
                flinkSql.append("-- Node: ").append(node.getLabel()).append(" (").append(node.getId()).append(")\n");
                flinkSql.append(rendered).append("\n\n");
                tableAlias.put(node.getId(), sanitize(node.getId()));
                continue;
            }

            if ("csv_input".equals(node.getType())) {
                String path = node.getParams() != null ? node.getParams().getOrDefault("path", "/data/input.csv").toString().trim() : "/data/input.csv";
                String delimiter = node.getParams() != null ? node.getParams().getOrDefault("delimiter", ",").toString().trim() : ",";
                String hasHeader = node.getParams() != null ? node.getParams().getOrDefault("hasHeader", "true").toString().trim() : "true";
                String fc = node.getParams() != null ? node.getParams().getOrDefault("fieldsConfig", "[]").toString() : "[]";
                if (path.isEmpty() || !new java.io.File(path).isFile()) {
                    throw new RuntimeException("CSV 输入文件不存在: " + path);
                }
                String effectiveDelimiter = autoDetectDelimiter(path, delimiter);
                List<String> autoCols = null;
                JsonNode fcNode = tryParseJsonArray(fc);
                if (fcNode == null || !fcNode.isArray() || fcNode.size() == 0) {
                    autoCols = detectCsvColumns(path, effectiveDelimiter, hasHeader);
                }
                String effectivePath = stripCsvHeaderIfNeeded(path, hasHeader);
                String rendered = generateCsvInputDDL(sanitize(node.getId()), effectivePath, effectiveDelimiter, hasHeader, fc, autoCols);
                String schema = extractFieldsFromDatagenConfig(fc);
                if (schema != null) {
                    nodeSchemas.put(node.getId(), schema);
                } else {
                    String ddlSchema = extractFieldsFromDdlTemplate(rendered);
                    if (ddlSchema != null) nodeSchemas.put(node.getId(), ddlSchema);
                }
                log.debug("Stored schema for node {}: {}", node.getId(), nodeSchemas.get(node.getId()));
                flinkSql.append("-- Node: ").append(node.getLabel()).append(" (").append(node.getId()).append(")\n");
                flinkSql.append(rendered).append("\n\n");
                tableAlias.put(node.getId(), sanitize(node.getId()));
                continue;
            }

// Excel Input -> 先用 POI 转成临时 CSV，再按 CSV 输入处理（自动探测表头 schema）
            if ("excel_input".equals(node.getType())) {
                String path = node.getParams() != null ? node.getParams().getOrDefault("path", "").toString().trim() : "";
                String delimiter = node.getParams() != null ? node.getParams().getOrDefault("delimiter", ",").toString().trim() : ",";
                String hasHeader = node.getParams() != null ? node.getParams().getOrDefault("hasHeader", "true").toString().trim() : "true";
                String sheetName = node.getParams() != null && node.getParams().get("sheetName") != null ? node.getParams().get("sheetName").toString().trim() : "";
                if (path.isEmpty()) {
                    throw new RuntimeException("Excel 输入缺少 path 参数");
                }
                if (!new java.io.File(path).isFile()) {
                    throw new RuntimeException("Excel 输入文件不存在: " + path);
                }
                String csvPath = excelPreprocessor.convertToCsv(path, delimiter, true, sheetName);
                if (csvPath == null) {
                    throw new RuntimeException("Excel 输入转换失败（文件不是有效的 Excel 格式，请使用 .xlsx/.xls 文件）: " + path);
                }
                String effectiveDelimiter = autoDetectDelimiter(csvPath, delimiter);
                List<String> autoCols = detectCsvColumns(csvPath, effectiveDelimiter, hasHeader);
                String effectivePath = stripCsvHeaderIfNeeded(csvPath, hasHeader);
                String rendered = generateCsvInputDDL(sanitize(node.getId()), effectivePath, effectiveDelimiter, hasHeader, "[]", autoCols);
                String schema = extractFieldsFromDdlTemplate(rendered);
                if (schema != null) nodeSchemas.put(node.getId(), schema);
                log.info("Excel input {}: {} -> {} (schema: {})", node.getId(), path, csvPath, schema);
                flinkSql.append("-- Node: ").append(node.getLabel()).append(" (").append(node.getId()).append(") [Excel Input -> Temp CSV]\n");
                flinkSql.append(rendered).append("\n\n");
                tableAlias.put(node.getId(), sanitize(node.getId()));
                continue;
            }

            // Parquet Input -> 先用 ParquetPreprocessor 转成临时 CSV，再按 CSV 输入处理
            if ("parquet_input".equals(node.getType())) {
                String path = node.getParams() != null ? node.getParams().getOrDefault("path", "").toString().trim() : "";
                String delimiter = node.getParams() != null ? node.getParams().getOrDefault("delimiter", ",").toString().trim() : ",";
                if (path.isEmpty()) {
                    throw new RuntimeException("Parquet 输入缺少 path 参数");
                }
                if (!new java.io.File(path).isFile()) {
                    throw new RuntimeException("Parquet 输入文件不存在: " + path);
                }
                String csvPath = parquetPreprocessor.convertToCsv(path, delimiter);
                if (csvPath == null) {
                    throw new RuntimeException("Parquet 输入转换失败（请确认为有效的 .parquet 文件）: " + path);
                }
                String effectiveDelimiter = autoDetectDelimiter(csvPath, delimiter);
                List<String> autoCols = detectCsvColumns(csvPath, effectiveDelimiter, "true");
                String effectivePath = stripCsvHeaderIfNeeded(csvPath, "true");
                String rendered = generateCsvInputDDL(sanitize(node.getId()), effectivePath, effectiveDelimiter, "true", "[]", autoCols);
                String schema = extractFieldsFromDdlTemplate(rendered);
                if (schema != null) nodeSchemas.put(node.getId(), schema);
                log.info("Parquet input {}: {} -> {} (schema: {})", node.getId(), path, csvPath, schema);
                flinkSql.append("-- Node: ").append(node.getLabel()).append(" (").append(node.getId()).append(") [Parquet Input -> Temp CSV]\n");
                flinkSql.append(rendered).append("\n\n");
                tableAlias.put(node.getId(), sanitize(node.getId()));
                continue;
            }

            // XML Input -> 先解析成临时 CSV（rowTag 行元素，嵌套保留为 JSON 列），再按 CSV 输入处理
            if ("xml_input".equals(node.getType())) {
                String path = node.getParams() != null ? node.getParams().getOrDefault("path", "").toString().trim() : "";
                String rowTag = node.getParams() != null ? node.getParams().getOrDefault("rowTag", "").toString().trim() : "";
                String delimiter = node.getParams() != null ? node.getParams().getOrDefault("delimiter", ",").toString().trim() : ",";
                String encoding = node.getParams() != null ? node.getParams().getOrDefault("encoding", "UTF-8").toString().trim() : "UTF-8";
                if (path.isEmpty()) {
                    throw new RuntimeException("XML 输入缺少 path 参数");
                }
                if (!new java.io.File(path).isFile()) {
                    throw new RuntimeException("XML 输入文件不存在: " + path);
                }
                String csvPath = xmlPreprocessor.convertToCsv(path, rowTag, delimiter, encoding);
                if (csvPath == null) {
                    throw new RuntimeException("XML 输入转换失败（请确认为记录列表结构的 XML）: " + path);
                }
                String effectiveDelimiter = autoDetectDelimiter(csvPath, delimiter);
                List<String> autoCols = detectCsvColumns(csvPath, effectiveDelimiter, "true");
                String effectivePath = stripCsvHeaderIfNeeded(csvPath, "true");
                String rendered = generateCsvInputDDL(sanitize(node.getId()), effectivePath, effectiveDelimiter, "true", "[]", autoCols);
                String schema = extractFieldsFromDdlTemplate(rendered);
                if (schema != null) nodeSchemas.put(node.getId(), schema);
                log.info("XML input {}: {} -> {} (schema: {})", node.getId(), path, csvPath, schema);
                flinkSql.append("-- Node: ").append(node.getLabel()).append(" (").append(node.getId()).append(") [XML Input -> Temp CSV]\n");
                flinkSql.append(rendered).append("\n\n");
                tableAlias.put(node.getId(), sanitize(node.getId()));
                continue;
            }
            // MySQL Input -> JDBC 读表，字段从表结构自动推导
            if ("mysql_input".equals(node.getType())) {
                String url = node.getParams() != null && node.getParams().get("url") != null ? node.getParams().get("url").toString().trim() : "";
                String table = node.getParams() != null && node.getParams().get("table") != null ? node.getParams().get("table").toString().trim() : "";
                String username = resolveCredential(node.getParams() != null ? node.getParams().get("username") : null, defaultMysqlUsername);
                String password = resolveCredential(node.getParams() != null ? node.getParams().get("password") : null, defaultMysqlPassword);
                if (url.isEmpty() || table.isEmpty()) {
                    throw new RuntimeException("MySQL 输入缺少 url / table 参数");
                }
                String fields = inferMysqlFields(url, table, username, password);
                String rendered = generateMysqlInputDDL(sanitize(node.getId()), url, table, username, password, fields);
                nodeSchemas.put(node.getId(), fields);
                log.info("MySQL input {}: {}.{} -> {} fields", node.getId(), url, table, fields.split("\n").length);
                flinkSql.append("-- Node: ").append(node.getLabel()).append(" (").append(node.getId()).append(") [MySQL Input]\n");
                flinkSql.append(rendered).append("\n\n");
                tableAlias.put(node.getId(), sanitize(node.getId()));
                continue;
            }

                        // JSON Input -> 支持 array 数组（转临时 CSV 自动 schema）与 lines（JSON Lines，fieldsConfig 指定 schema）
            if ("json_input".equals(node.getType())) {
                String path = node.getParams() != null && node.getParams().get("path") != null ? node.getParams().get("path").toString().trim() : "";
                if (path.isEmpty() || !new java.io.File(path).isFile()) {
                    throw new RuntimeException("JSON 输入文件不存在: " + path);
                }
                String mode = node.getParams() != null && node.getParams().get("mode") != null ? node.getParams().get("mode").toString().trim() : "auto";
                boolean arrayMode = "array".equalsIgnoreCase(mode)
                        || ("auto".equalsIgnoreCase(mode) && JsonPreprocessor.isJsonArrayFile(path));
                if (arrayMode) {
                    String delimiter = node.getParams() != null && node.getParams().get("delimiter") != null ? node.getParams().get("delimiter").toString().trim() : ",";
                    String encoding = node.getParams() != null && node.getParams().get("encoding") != null ? node.getParams().get("encoding").toString().trim() : "UTF-8";
                    String csvPath = jsonPreprocessor.convertArrayToCsv(path, delimiter, encoding);
                    if (csvPath == null) {
                        throw new RuntimeException("JSON 数组输入转换失败（请确认为 JSON 数组文件）: " + path);
                    }
                    String effectiveDelimiter = autoDetectDelimiter(csvPath, delimiter);
                    List<String> autoCols = detectCsvColumns(csvPath, effectiveDelimiter, "true");
                    String effectivePath = stripCsvHeaderIfNeeded(csvPath, "true");
                    String rendered = generateCsvInputDDL(sanitize(node.getId()), effectivePath, effectiveDelimiter, "true", "[]", autoCols);
                    String schema = extractFieldsFromDdlTemplate(rendered);
                    if (schema != null) nodeSchemas.put(node.getId(), schema);
                    log.info("JSON array input {}: {} -> {} (schema: {})", node.getId(), path, csvPath, schema);
                    flinkSql.append("-- Node: ").append(node.getLabel()).append(" (").append(node.getId()).append(") [JSON Array Input -> Temp CSV]\n");
                    flinkSql.append(rendered).append("\n\n");
                    tableAlias.put(node.getId(), sanitize(node.getId()));
                    continue;
                }
                // lines 模式：保持原有 JSON Lines 逻辑
                String fc = node.getParams() != null && node.getParams().get("fieldsConfig") != null ? node.getParams().get("fieldsConfig").toString() : "[]";
                String rendered = generateJsonInputDDL(sanitize(node.getId()), path, fc);
                String schema = extractFieldsFromDatagenConfig(fc);
                if (schema != null) nodeSchemas.put(node.getId(), schema);
                flinkSql.append("-- Node: ").append(node.getLabel()).append(" (").append(node.getId()).append(") [JSON Input]\n");
                flinkSql.append(rendered).append("\n\n");
                tableAlias.put(node.getId(), sanitize(node.getId()));
                continue;
            }

// Excel Output -> 生成临时 CSV 输出，作业完成后自动转换为 .xlsx
            if ("excel_output".equals(node.getType())) {
                String actualPath = node.getParams() != null ? node.getParams().getOrDefault("path", "D:\\code\\比赛\\2026省服务外包\\output\\output.xlsx").toString().trim() : "/data/output.xlsx";
                String delimiter = node.getParams() != null ? node.getParams().getOrDefault("delimiter", ",").toString().trim() : ",";
                String sourceFields = findIncomingSourceSchema(node.getId(), dag.getEdges(), nodeSchemas);
                String sheetName = node.getParams() != null && node.getParams().get("sheetName") != null ? node.getParams().get("sheetName").toString().trim() : "Data";
                String tempCsvPath = ExcelOutputConverter.getTempCsvPath(actualPath, sheetName);

                log.info("Excel output '{}': actual path={}, temp CSV path={}", node.getId(), actualPath, tempCsvPath);

                StringBuilder excelDdl = new StringBuilder();
                excelDdl.append("CREATE TABLE ").append(sanitize(node.getId())).append(" (\n");
                if (sourceFields != null) {
                    excelDdl.append(sourceFields).append("\n");
                } else {
                    excelDdl.append("  data STRING\n");
                }
                excelDdl.append(") WITH (\n");
                excelDdl.append("  'connector' = 'filesystem',\n");
                excelDdl.append("  'path' = '").append(tempCsvPath).append("',\n");
                excelDdl.append("  'format' = 'csv',\n");
                excelDdl.append("  'csv.delimiter' = '").append(delimiter).append("',\n");
                excelDdl.append("  'sink.parallelism' = '1'\n");
                excelDdl.append(");\n");
                String rendered = excelDdl.toString();

                flinkSql.append("-- Node: ").append(node.getLabel()).append(" (").append(node.getId()).append(") [Excel Output -> Temp CSV]\n");
                flinkSql.append(rendered).append("\n\n");
                tableAlias.put(node.getId(), sanitize(node.getId()));
                continue;
            }
            // Parquet Output -> 生成临时 CSV 输出，作业完成后自动转换为 .parquet
            if ("parquet_output".equals(node.getType())) {
                String actualPath = node.getParams() != null ? node.getParams().getOrDefault("path", "D:\\code\\比赛\\2026省服务外包\\output\\output.parquet").toString().trim() : "/data/output.parquet";
                String delimiter = node.getParams() != null ? node.getParams().getOrDefault("delimiter", ",").toString().trim() : ",";
                String sourceFields = findIncomingSourceSchema(node.getId(), dag.getEdges(), nodeSchemas);
                String tempCsvPath = actualPath.replaceAll("(?i)\\.parquet$", "") + "_temp_csv";

                log.info("Parquet output '{}': actual path={}, temp CSV path={}", node.getId(), actualPath, tempCsvPath);

                StringBuilder parquetDdl = new StringBuilder();
                parquetDdl.append("CREATE TABLE ").append(sanitize(node.getId())).append(" (\n");
                if (sourceFields != null) {
                    parquetDdl.append(sourceFields).append("\n");
                } else {
                    parquetDdl.append("  data STRING\n");
                }
                parquetDdl.append(") WITH (\n");
                parquetDdl.append("  'connector' = 'filesystem',\n");
                parquetDdl.append("  'path' = '").append(tempCsvPath).append("',\n");
                parquetDdl.append("  'format' = 'csv',\n");
                parquetDdl.append("  'csv.delimiter' = '").append(delimiter).append("',\n");
                parquetDdl.append("  'sink.parallelism' = '1'\n");
                parquetDdl.append(");\n");
                String rendered = parquetDdl.toString();

                flinkSql.append("-- Node: ").append(node.getLabel()).append(" (").append(node.getId()).append(") [Parquet Output -> Temp CSV]\n");
                flinkSql.append(rendered).append("\n\n");
                tableAlias.put(node.getId(), sanitize(node.getId()));
                continue;
            }

            // XML Output -> 生成临时 CSV 输出，作业完成后自动转换为 XML
            if ("xml_output".equals(node.getType())) {
                String actualPath = node.getParams() != null ? node.getParams().getOrDefault("path", "D:\\code\\比赛\\2026省服务外包\\output\\output.xml").toString().trim() : "/data/output.xml";
                String delimiter = node.getParams() != null ? node.getParams().getOrDefault("delimiter", ",").toString().trim() : ",";
                String sourceFields = findIncomingSourceSchema(node.getId(), dag.getEdges(), nodeSchemas);
                String tempCsvPath = XmlOutputConverter.getTempCsvPath(actualPath);

                log.info("XML output '{}': actual path={}, temp CSV path={}", node.getId(), actualPath, tempCsvPath);

                StringBuilder xmlDdl = new StringBuilder();
                xmlDdl.append("CREATE TABLE ").append(sanitize(node.getId())).append(" (\n");
                if (sourceFields != null) {
                    xmlDdl.append(sourceFields).append("\n");
                } else {
                    xmlDdl.append("  data STRING\n");
                }
                xmlDdl.append(") WITH (\n");
                xmlDdl.append("  'connector' = 'filesystem',\n");
                xmlDdl.append("  'path' = '").append(tempCsvPath).append("',\n");
                xmlDdl.append("  'format' = 'csv',\n");
                xmlDdl.append("  'csv.delimiter' = '").append(delimiter).append("',\n");
                xmlDdl.append("  'sink.parallelism' = '1'\n");
                xmlDdl.append(");\n");
                String rendered = xmlDdl.toString();

                flinkSql.append("-- Node: ").append(node.getLabel()).append(" (").append(node.getId()).append(") [XML Output -> Temp CSV]\n");
                flinkSql.append(rendered).append("\n\n");
                tableAlias.put(node.getId(), sanitize(node.getId()));
                continue;
            }
            // === Kafka Input (Flink SQL connector) ===
            if ("kafka_input".equals(node.getType())) {
                String topic = node.getParams() != null ? node.getParams().getOrDefault("topic", "test-topic").toString() : "test-topic";
                String bs = node.getParams() != null ? node.getParams().getOrDefault("bootstrapServers", "localhost:9092").toString() : "localhost:9092";
                String fc = node.getParams() != null ? node.getParams().getOrDefault("fieldsConfig", "[]").toString() : "[]";
                String rendered = generateKafkaInputDDL(sanitize(node.getId()), topic, bs, fc);
                String schema = extractFieldsFromDatagenConfig(fc);
                if (schema != null) nodeSchemas.put(node.getId(), schema);
                flinkSql.append("-- Node: ").append(node.getLabel()).append(" (").append(node.getId()).append(")\n");
                flinkSql.append(rendered).append("\n\n");
                tableAlias.put(node.getId(), sanitize(node.getId()));
                continue;
            }

            // === Kafka Output (Flink SQL connector) ===
            if ("kafka_output".equals(node.getType())) {
                String topic = node.getParams() != null ? node.getParams().getOrDefault("topic", "output-topic").toString() : "output-topic";
                String bs = node.getParams() != null ? node.getParams().getOrDefault("bootstrapServers", "localhost:9092").toString() : "localhost:9092";
                String sourceFields = findIncomingSourceSchema(node.getId(), dag.getEdges(), nodeSchemas);
                String rendered = generateKafkaOutputDDL(sanitize(node.getId()), topic, bs, sourceFields);
                flinkSql.append("-- Node: ").append(node.getLabel()).append(" (").append(node.getId()).append(")\n");
                flinkSql.append(rendered).append("\n\n");
                tableAlias.put(node.getId(), sanitize(node.getId()));
                continue;
            }


            if (template == null || template.isBlank()) {
                if (!"transform".equals(control.getCategory())) {
                    log.warn("Control {} has no Flink template, skipping", node.getType());
                    continue;
                }
                // transform 控件的 SQL 由边循环内联生成，模板可以为空
            }

            if ("input".equals(control.getCategory())) {
                String schema = extractFieldsFromDdlTemplate(template);
                if (schema != null) { nodeSchemas.put(node.getId(), schema); }
                flinkSql.append("-- Node: ").append(node.getLabel()).append(" (").append(node.getId()).append(")\n");
                flinkSql.append(renderTemplate(template, node.getParams(), node.getId())).append("\n\n");
            } else if ("output".equals(control.getCategory())) {
                String sourceFields = findIncomingSourceSchema(node.getId(), dag.getEdges(), nodeSchemas);
                String rendered;
                Map<String, Object> renderParams = node.getParams();
                if ("csv_output".equals(node.getType()) || "json_output".equals(node.getType())) {
                    // Flink filesystem sink writes a directory of part-* files; we write to a temp
                    // dir and merge into the user's single file after the job completes.
                    Map<String, Object> tmpParams = new HashMap<>();
                    if (node.getParams() != null) tmpParams.putAll(node.getParams());
                    String origPath = tmpParams.getOrDefault("path", "D:\\code\\比赛\\2026省服务外包\\output\\output.csv").toString().trim();
                    tmpParams.put("path", origPath + ".tmp");
                    renderParams = tmpParams;
                }
                if ("mysql_output".equals(node.getType())) {
                    // 凭据支持占位符/空值：提交时解析为环境变量 MYSQL_USERNAME / MYSQL_PASSWORD，避免明文入库
                    Map<String, Object> resolvedParams = new HashMap<>();
                    if (node.getParams() != null) resolvedParams.putAll(node.getParams());
                    resolvedParams.put("username", resolveCredential(node.getParams() != null ? node.getParams().get("username") : null, defaultMysqlUsername));
                    resolvedParams.put("password", resolveCredential(node.getParams() != null ? node.getParams().get("password") : null, defaultMysqlPassword));
                    renderParams = resolvedParams;
                    if (sourceFields != null) {
                        mysqlTableCreator.ensureTable(resolvedParams, sourceFields);
                    }
                }
                if (sourceFields != null) {
                    log.info("Schema propagation: replacing 'data STRING' with custom fields from source for node {}", node.getId());
                    String modifiedTemplate = replaceDataStringInDDL(template, sourceFields);
                    rendered = renderTemplate(modifiedTemplate, renderParams, node.getId());
                } else {
                    rendered = renderTemplate(template, renderParams, node.getId());
                }
                flinkSql.append(rendered).append("\n\n");

            } else if ("transform".equals(control.getCategory())) {
                log.debug("Processing transform node: {} with type: {}", node.getId(), node.getType());
                String incomingSchema = findIncomingSourceSchema(node.getId(), dag.getEdges(), nodeSchemas);
                if (incomingSchema != null) {
                    if ("field_concat".equals(node.getType())) {
                        String nfn = node.getParams() != null ? node.getParams().getOrDefault("newFieldName", "new_field").toString() : "new_field";
                        nodeSchemas.put(node.getId(), incomingSchema + ",\n  " + nfn + " STRING");
                    } else if ("xml_json".equals(node.getType())) {
                        String tfn = node.getParams() != null ? node.getParams().getOrDefault("targetField", "result").toString() : "result";
                        nodeSchemas.put(node.getId(), incomingSchema + ",\n  `" + tfn + "` STRING");
                    } else if ("field_filter".equals(node.getType())) {
                        nodeSchemas.put(node.getId(), filterSchemaFields(incomingSchema, parseFieldList(node.getParams())));
                    } else if ("field_rename".equals(node.getType())) {
                        nodeSchemas.put(node.getId(), renameSchemaFields(incomingSchema, parseRenameMappings(node.getParams())));
                    } else if ("row_filter".equals(node.getType())) {
                        nodeSchemas.put(node.getId(), incomingSchema);
                    } else if ("dedupe".equals(node.getType())) {
                        nodeSchemas.put(node.getId(), incomingSchema);
                    } else if ("validate".equals(node.getType())) {
                        nodeSchemas.put(node.getId(), incomingSchema);
                    } else if ("route".equals(node.getType())) {
                        nodeSchemas.put(node.getId(), incomingSchema);
                    } else if ("json_parse".equals(node.getType())) {
                        String jfc = node.getParams() != null && node.getParams().get("fieldsConfig") != null ? node.getParams().get("fieldsConfig").toString() : "[]";
                        String extra = extractFieldsFromDatagenConfig(jfc);
                        nodeSchemas.put(node.getId(), extra != null ? incomingSchema + ",\n" + extra : incomingSchema);
                    } else {
                        nodeSchemas.put(node.getId(), incomingSchema);
                    }
                }
                // Transform SQL is embedded inline in the edge loop's INSERT INTO, not standalone
                log.debug("Transform node {} SQL embedded in downstream INSERT for schema propagation", node.getId());
                // (rendered template not output here - handled by edge loop)
            }
            tableAlias.put(node.getId(), sanitize(node.getId()));
        }

        // Edge loop with transform chain support
        Map<String, String> nodeTypeMap = new HashMap<>();
        Map<String, String> nodeCategoryMap = new HashMap<>();
        Map<String, Map<String, Object>> transformNodeParams = new HashMap<>();
        for (DagDefinition.DagNode n : dag.getNodes()) {
            nodeTypeMap.put(n.getId(), n.getType());
            ControlRegistry ctrl = controlService.findByType(n.getType());
            nodeCategoryMap.put(n.getId(), ctrl.getCategory());
            if ("transform".equals(ctrl.getCategory())) {
                transformNodeParams.put(n.getId(), n.getParams());
            }
        }

        for (DagDefinition.DagEdge edge : dag.getEdges()) {
            String sourceTable = sanitize(edge.getSource());
            String targetTable = sanitize(edge.getTarget());

            String targetCategory = nodeCategoryMap.get(edge.getTarget());
            if ("transform".equals(targetCategory)) { continue; }

            String sourceCategory = nodeCategoryMap.get(edge.getSource());
            if ("transform".equals(sourceCategory)) {
                String transformType = nodeTypeMap.get(edge.getSource());
                String upstreamTable = null;
                for (DagDefinition.DagEdge ie : dag.getEdges()) {
                    if (ie.getTarget().equals(edge.getSource())) {
                        upstreamTable = sanitize(ie.getSource());
                        break;
                    }
                }
                if (upstreamTable == null) upstreamTable = sourceTable;
                String ts;
                Map<String, Object> tp = transformNodeParams.get(edge.getSource());
                if ("field_filter".equals(transformType)) {
                    ts = "SELECT " + buildFieldFilterSelect(tp) + " FROM " + upstreamTable;
                } else if ("field_rename".equals(transformType)) {
                    ts = "SELECT " + buildFieldRenameSelect(tp, findIncomingSourceSchema(edge.getSource(), dag.getEdges(), nodeSchemas)) + " FROM " + upstreamTable;
                } else if ("json_parse".equals(transformType)) {
                    ts = "SELECT " + buildJsonParseSelect(tp, findIncomingSourceSchema(edge.getSource(), dag.getEdges(), nodeSchemas)) + " FROM " + upstreamTable;
                } else if ("dedupe".equals(transformType)) {
                    List<String> dedupeFields = parseCsvFields(tp != null ? tp.get("dedupeFields") : null);
                    if (dedupeFields.isEmpty()) {
                        ts = "SELECT DISTINCT * FROM " + upstreamTable;
                    } else {
                        String incomingSchema = findIncomingSourceSchema(edge.getSource(), dag.getEdges(), nodeSchemas);
                        List<String> allCols = parseSchemaFieldNames(incomingSchema);
                        if (allCols.isEmpty()) {
                            ts = "SELECT DISTINCT " + quoteFieldsList(dedupeFields) + " FROM " + upstreamTable;
                        } else {
                            String partition = String.join(", ", dedupeFields.stream().map(this::quoteFlinkField).toList());
                            String orderKey = quoteFlinkField(dedupeFields.get(0));
                            String colList = String.join(", ", allCols.stream().map(this::quoteFlinkField).toList());
                            ts = "SELECT " + colList + " FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY " + partition + " ORDER BY " + orderKey + ") AS __rn FROM " + upstreamTable + ") WHERE __rn = 1";
                        }
                    }
                } else if ("validate".equals(transformType)) {
                    ts = buildValidateSql(tp, upstreamTable);
                } else if ("route".equals(transformType)) {
                    ts = buildRouteSql(edge, dag.getEdges(), tp, upstreamTable);
                } else {
                    ControlRegistry sc = controlService.findByType(transformType);
                    ts = renderTemplate(sc.getFlinkTemplate(), tp, edge.getSource());
                    ts = ts.replace(sanitize(edge.getSource()), upstreamTable);
                }
                flinkSql.append("INSERT INTO ").append(targetTable).append("\n").append(ts).append(";\n\n");
                continue;
            }

            flinkSql.append("INSERT INTO ").append(targetTable)
                    .append(" SELECT * FROM ").append(sourceTable).append(";\n");
        }

        log.info("Generated Flink SQL:\n{}", flinkSql.toString());
        return flinkSql.toString();
    }

    /**
     * 提交前清理输出控件的临时目录，避免上一次运行遗留的 part-* 文件被合并导致数据重复
     */
    private void cleanOutputTempDirs(DagDefinition dag) {
        try {
            for (DagDefinition.DagNode node : dag.getNodes()) {
                String type = node.getType();
                if (type == null || !type.endsWith("_output")) continue;
                if (node.getParams() == null || node.getParams().get("path") == null) continue;
                String path = node.getParams().get("path").toString().trim();
                if (path.isEmpty()) continue;
                if ("csv_output".equals(type) || "json_output".equals(type)) {
                    deleteRecursively(new java.io.File(path + ".tmp"));
                    continue;
                }
                if ("excel_output".equals(type) || "xml_output".equals(type) || "parquet_output".equals(type)) {
                    String base = path.replaceAll("(?i)\\.(xlsx|xls|xml|parquet)$", "");
                    java.io.File baseTemp = new java.io.File(base + "_temp_csv");
                    java.io.File parent = baseTemp.getParentFile();
                    if (parent != null && parent.isDirectory()) {
                        String prefix = baseTemp.getName();
                        java.io.File[] siblings = parent.listFiles((d, n) -> n.equals(prefix) || n.startsWith(prefix + "_"));
                        if (siblings != null) {
                            for (java.io.File f : siblings) deleteRecursively(f);
                        }
                    } else {
                        deleteRecursively(baseTemp);
                    }
                }
            }
        } catch (Exception e) {
            log.warn("Failed to clean output temp dirs: {}", e.getMessage());
        }
    }

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

    public String submitToFlink(String flinkSql, int parallelism) {
        String jobId = null;
        if (checkFlinkCluster()) {
            try {
                log.info("Flink cluster available, submitting SQL via Gateway...");
                jobId = submitViaSqlClient(flinkSql, parallelism);
            } catch (Exception e) {
                log.warn("SQL Gateway submission failed: {}. Will poll for jobs.", e.getMessage());
            }
        } else {
            log.info("Flink cluster not available at {}:{}", flinkHost, flinkPort);
        }
        // Use real job ID if submitViaSqlClient found one; otherwise use fallback mock
        if (jobId != null && !jobId.startsWith("flink-job-")) return jobId;
        String mockJobId = "flink-job-" + UUID.randomUUID().toString();
        log.info("No real Flink job ID captured, using fallback ID: {}", mockJobId);
        return mockJobId;
    }

    private String pollFlinkJob(int maxWaitSec) {
        try {
            java.net.http.HttpClient c = java.net.http.HttpClient.newHttpClient();
            for (int i = 0; i < maxWaitSec; i++) {
                Thread.sleep(1000);
                String r = c.send(java.net.http.HttpRequest.newBuilder()
                    .uri(java.net.URI.create("http://" + flinkHost + ":" + flinkPort + "/jobs/overview"))
                    .GET().build(), java.net.http.HttpResponse.BodyHandlers.ofString()).body();
                JsonNode j = objectMapper.readTree(r).get("jobs");
                if (j != null && j.isArray()) {
                    for (JsonNode jj : j) {
                        String st = jj.has("state") ? jj.get("state").asText() : "";
                        if ("RUNNING".equals(st) || "CREATED".equals(st)) {
                            String jid = jj.get("jid").asText();
                            log.info("Found active Flink job: {} ({})", jj.get("name").asText(), jid);
                            return jid;
                        }
                    }
                }
            }
        } catch (Exception e) { log.warn("pollFlinkJob error: {}", e.getMessage()); }
        return null;
    }


    /**
     * Cancel a Flink job via Flink REST API
     */
    public void cancelFlinkJob(String flinkJobId) {
        if (flinkJobId == null || flinkJobId.isEmpty() || flinkJobId.startsWith("mock-") || flinkJobId.startsWith("flink-job-")) {
            log.info("Flink job ID is mock or empty, skipping Flink REST API call: {}", flinkJobId);
            return;
        }
        try {
            String cancelUrl = "http://" + flinkHost + ":" + flinkPort + "/jobs/" + flinkJobId + "?mode=cancel";
            java.net.http.HttpClient httpClient = java.net.http.HttpClient.newHttpClient();
            int statusCode = httpClient.send(
                java.net.http.HttpRequest.newBuilder()
                    .uri(java.net.URI.create(cancelUrl))
                    .method("PATCH", java.net.http.HttpRequest.BodyPublishers.noBody())
                    .build(),
                java.net.http.HttpResponse.BodyHandlers.discarding()
            ).statusCode();
            log.info("Flink job {} cancel request sent, Flink REST API status: {}", flinkJobId, statusCode);
        } catch (Exception e) {
            log.warn("Failed to cancel Flink job {}: {}", flinkJobId, e.getMessage());
        }
    }

    private boolean checkFlinkCluster() {
        try { java.net.Socket s = new java.net.Socket(flinkHost, flinkPort); s.close(); return true; }
        catch (Exception e) { return false; }
    }

    /** 上线等场景探测 Flink 集群是否可用（探测失败即不允许真实提交） */
    public boolean isFlinkClusterAvailable() {
        return checkFlinkCluster();
    }


        private String submitViaSqlClient(String flinkSql, int parallelism) throws Exception {
        Path tempSqlFile = Files.createTempFile("flink-job-", ".sql");
        Files.writeString(tempSqlFile, flinkSql, StandardCharsets.UTF_8);
        log.info("SQL written to: {}", tempSqlFile.toAbsolutePath());
        // Capture job list before submission for downstream job detection
        var initialHc = java.net.http.HttpClient.newHttpClient();
        java.util.Set<String> initialBeforeIds = getCurrentJobIds(initialHc);

        String jobId = trySqlGateway(flinkSql, parallelism);
        if (jobId != null) { log.info("Job via SQL Gateway: {}", jobId); Files.deleteIfExists(tempSqlFile); return jobId; }

        log.info("SQL Gateway down, trying sql-client.sh...");
        jobId = trySqlClientScript(tempSqlFile);
        if (jobId != null) { log.info("Job via sql-client.sh: {}", jobId); Files.deleteIfExists(tempSqlFile); return jobId; }

        log.info("Polling for new Flink jobs (using pre-submission baseline)...");
        jobId = submitViaFlinkRestApi(flinkSql, initialBeforeIds);
        Files.deleteIfExists(tempSqlFile);
        if (jobId == null) { jobId = "flink-job-" + UUID.randomUUID().toString(); log.info("Using fallback ID: {}", jobId); }
        return jobId;
    }

    private String trySqlGateway(String flinkSql, int parallelism) {
        try {
            String gatewayHost = (sqlGatewayHost == null || sqlGatewayHost.isEmpty()) ? flinkHost : sqlGatewayHost;
            String gatewayUrl = "http://" + gatewayHost + ":" + sqlGatewayPort;
            java.net.http.HttpClient hc = java.net.http.HttpClient.newHttpClient();
            String sr = hc.send(java.net.http.HttpRequest.newBuilder()
                .uri(java.net.URI.create(gatewayUrl + "/v1/sessions"))
                .header("Content-Type", "application/json")
                .POST(java.net.http.HttpRequest.BodyPublishers.ofString("{}")).build(),
                java.net.http.HttpResponse.BodyHandlers.ofString()).body();
            JsonNode sj = objectMapper.readTree(sr);
            if (!sj.has("sessionHandle")) {
                log.warn("Gateway session create failed, response: " + sr);
                return null;
            }
            String sh = sj.get("sessionHandle").asText();
            log.info("Gateway session created: " + sh);

            String[] stmts = flinkSql.split(";");
            var beforeIds = getCurrentJobIds(hc);
            String jid = null;

            for (int i = 0; i < stmts.length; i++) {
                String stmtRaw = stmts[i].replaceAll("(?m)^--.*\n?", "").trim();
                if (stmtRaw.isEmpty()) continue;

                log.info("Gateway stmt " + (i+1) + "/" + stmts.length + ": " + stmtRaw.substring(0, Math.min(80, stmtRaw.length())));
                String body = objectMapper.createObjectNode().put("statement", stmtRaw + ";").toString();
                String resp = hc.send(java.net.http.HttpRequest.newBuilder()
                    .uri(java.net.URI.create(gatewayUrl + "/v1/sessions/" + sh + "/statements"))
                    .header("Content-Type", "application/json")
                    .POST(java.net.http.HttpRequest.BodyPublishers.ofString(body)).build(),
                    java.net.http.HttpResponse.BodyHandlers.ofString()).body();
                JsonNode rj = objectMapper.readTree(resp);
                if (rj.has("errors")) {
                    log.warn("Gateway stmt " + (i+1) + " failed: " + rj.get("errors"));
                    continue;
                }
                if (rj.has("operationHandle")) {
                    String oh = rj.get("operationHandle").asText();
                    log.info("Gateway stmt " + (i+1) + " operation handle: " + oh);

                    boolean isInsert = stmtRaw.toUpperCase().startsWith("INSERT");
                    int maxPolls = isInsert ? 20 : 10;

                    for (int w = 0; w < maxPolls; w++) {
                        Thread.sleep(500);
                        String op = hc.send(java.net.http.HttpRequest.newBuilder()
                            .uri(java.net.URI.create(gatewayUrl + "/v1/sessions/" + sh + "/operations/" + oh + "/status"))
                            .GET().build(),
                            java.net.http.HttpResponse.BodyHandlers.ofString()).body();
                        // Handle empty response - fast operation completed
                        if (op.isEmpty() || op.trim().isEmpty()) {
                            log.debug("Gateway stmt " + (i+1) + " empty response - operation completed");
                            if (isInsert) {
                                for (int j = 0; j < 20; j++) {
                                    String n = findNewFlinkJob(hc, beforeIds);
                                    if (n != null) { jid = n; break; }
                                    Thread.sleep(500);
                                }
                            }
                            break;
                        }
                                                JsonNode oj = objectMapper.readTree(op);

                        // Handle nested status: {"status": {"status": "COMPLETED"}}
                        String os = null;
                        if (oj.has("status")) {
                            JsonNode st = oj.get("status");
                            if (st.isObject() && st.has("status")) {
                                os = st.get("status").asText();
                            } else if (st.isTextual()) {
                                os = st.asText();
                            }
                        }

                        log.debug("Gateway stmt " + (i+1) + " poll " + (w+1) + "/" + maxPolls + " status: " + os);

                        boolean _isDone = "COMPLETED".equals(os) || "FINISHED".equals(os) || "SUCCESS".equals(os) || (isInsert && "RUNNING".equals(os));
                        if (!_isDone && os == null && !op.isEmpty() && op.startsWith("{") && !oj.has("errors")) {
                            _isDone = true;
                            log.debug("Gateway stmt " + (i+1) + " detected completion (empty status, no errors)");
                        }
                        if (_isDone) {
                            log.info("Gateway stmt " + (i+1) + " completed (status=" + os + ")");
                            if (isInsert) {
                                if (oj.has("result") && oj.get("result").has("jobId")) {
                                    jid = oj.get("result").get("jobId").asText();
                                    log.info("Gateway returned jobId from result: " + jid);
                                }
                                if (jid == null && oj.has("info") && oj.get("info").isObject()) {
                                    JsonNode infoNestedStatus = oj.get("info").get("status");
                                    if (infoNestedStatus != null && infoNestedStatus.has("jobIds")) {
                                        JsonNode jids = infoNestedStatus.get("jobIds");
                                        if (jids.isArray() && jids.size() > 0) {
                                            jid = jids.get(0).asText();
                                            log.info("Gateway returned jobId from info.status.jobIds: " + jid);
                                        }
                                    }
                                }
                                if (jid == null) {
                                    for (int j = 0; j < 20; j++) {
                                        String n = findNewFlinkJob(hc, beforeIds);
                                        if (n != null) { jid = n; break; }
                                        Thread.sleep(500);
                                    }
                                }
                            }
                            break;
                        }
                    }
                }
            }
            try { hc.send(java.net.http.HttpRequest.newBuilder()
                .uri(java.net.URI.create(gatewayUrl + "/v1/sessions/" + sh))
                .DELETE().build(), java.net.http.HttpResponse.BodyHandlers.discarding()); } catch (Exception ign) {}
            return jid;
        } catch (Exception e) {
            log.warn("Gateway failed: " + e.getMessage());
            return null;
        }
    }
    private String trySqlClientScript(Path sqlFile) {
        try {
            String b = null;
            for (String bp : new String[]{"C:/Program Files/Git/bin/bash.exe", "C:/Program Files (x86)/Git/bin/bash.exe"}) {
                if (new java.io.File(bp).exists()) { b = bp; break; }
            }
            if (b == null) { log.warn("bash.exe not found"); return null; }

            String sc = flinkHome.replace("\\", "/") + "/bin/sql-client.sh";
            String sf = sqlFile.toAbsolutePath().toString().replace("\\", "/");
            log.info("Running: " + b + " " + sc + " -f " + sf);
            ProcessBuilder pb = new ProcessBuilder(b, sc, "-f", sf);
            pb.environment().put("FLINK_CONF_DIR", flinkHome.replace("\\", "/") + "/conf");
            pb.environment().put("FLINK_HOME", flinkHome.replace("\\", "/"));
            pb.redirectErrorStream(true);
            Process p = pb.start();

            // Read stdout with timeout (async to avoid blocking)
            StringBuilder output = new StringBuilder();
            Thread reader = new Thread(() -> {
                try {
                    java.io.BufferedReader br = new java.io.BufferedReader(
                        new java.io.InputStreamReader(p.getInputStream(), java.nio.charset.StandardCharsets.UTF_8));
                    String line;
                    while ((line = br.readLine()) != null) {
                        output.append(line).append("\n");
                    }
                } catch (Exception e) { /* stream closed */ }
            });
            reader.setDaemon(true);
            reader.start();

            if (!p.waitFor(30, TimeUnit.SECONDS)) {
                p.destroyForcibly();
                log.warn("sql-client.sh timed out after 30s");
                return null;
            }
            reader.join(5000);

            String all = output.toString();
            log.info("sql-client.sh output: " + all.length() + " chars");

            // Try to find Flink Job ID in output
            java.util.regex.Matcher m = java.util.regex.Pattern.compile(
                "Job\\s+ID\\s*:\\s*([a-f0-9-]+)", java.util.regex.Pattern.CASE_INSENSITIVE).matcher(all);
            if (m.find()) {
                String found = m.group(1);
                log.info("Found job ID from sql-client.sh: " + found);
                return found;
            }
            m = java.util.regex.Pattern.compile(
                "([a-f0-9]{32}|[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12})",
                java.util.regex.Pattern.CASE_INSENSITIVE).matcher(all);
            if (m.find()) {
                String found = m.group(1);
                log.info("Found job ID (hex) from sql-client.sh: " + found);
                return found;
            }

            // SQL was submitted even without Job ID
            log.info("sql-client.sh finished but no Job ID found, returning submission marker");
                        return null; // sql-client finished but no Job ID
        } catch (Exception e) {
            log.warn("sql-client.sh failed: " + e.getMessage());
            return null;
        }
    }

    private String submitViaFlinkRestApi(String flinkSql, java.util.Set<String> beforeIds) {
        try {
            var hc = java.net.http.HttpClient.newHttpClient();
            log.info("submitViaFlinkRestApi: polling for new Flink jobs, before count: " + beforeIds.size());
            for (int i = 0; i < 120; i++) {
                Thread.sleep(500);
                String resp = hc.send(java.net.http.HttpRequest.newBuilder()
                    .uri(java.net.URI.create("http://" + flinkHost + ":" + flinkPort + "/jobs/overview"))
                    .GET().build(), java.net.http.HttpResponse.BodyHandlers.ofString()).body();
                JsonNode jobs = objectMapper.readTree(resp).get("jobs");
                if (jobs != null && jobs.isArray()) {
                    for (int j = 0; j < jobs.size(); j++) {
                        JsonNode job = jobs.get(j);
                        String jid = job.get("jid").asText();
                        if (!beforeIds.contains(jid)) {
                            String state = job.has("state") ? job.get("state").asText() : "";
                            log.info("Found new Flink job: " + job.get("name").asText() + " (" + jid + ") state=" + state);
                            return jid;
                        }
                    }
                    if (i % 10 == 0) {
                        log.info("submitViaFlinkRestApi poll " + (i+1) + "/60: " + jobs.size() + " jobs, no new ones");
                    }
                }
            }
            log.warn("submitViaFlinkRestApi: no new jobs found after 30s polling");
        } catch (Exception e) {
            log.warn("REST poll error: " + e.getMessage());
        }
        return null;
    }
private java.util.Set<String> getCurrentJobIds(java.net.http.HttpClient httpClient) throws Exception {
        java.util.HashSet<String> ids = new java.util.HashSet<>();
        try {
            String jobsResp = httpClient.send(
                java.net.http.HttpRequest.newBuilder()
                    .uri(java.net.URI.create("http://" + flinkHost + ":" + flinkPort + "/jobs/overview"))
                    .GET().build(),
                java.net.http.HttpResponse.BodyHandlers.ofString()
            ).body();
            JsonNode jobs = objectMapper.readTree(jobsResp).get("jobs");
            if (jobs != null && jobs.isArray()) {
                for (JsonNode j : jobs) { ids.add(j.get("jid").asText()); }
            }
        } catch (Exception e) { log.debug("getCurrentJobIds: {}", e.getMessage()); }
        return ids;
    }

    private long getFlinkJobCount(java.net.http.HttpClient httpClient) throws Exception {
        String jobsResp = httpClient.send(
            java.net.http.HttpRequest.newBuilder()
                .uri(java.net.URI.create("http://" + flinkHost + ":" + flinkPort + "/jobs/overview"))
                .GET().build(),
            java.net.http.HttpResponse.BodyHandlers.ofString()
        ).body();
        JsonNode jobs = objectMapper.readTree(jobsResp).get("jobs");
        return jobs != null ? jobs.size() : 0;
    }

    private String findNewFlinkJob(java.net.http.HttpClient httpClient, java.util.Set<String> beforeIds) {
        try {
            String jobsResp = httpClient.send(
                java.net.http.HttpRequest.newBuilder()
                    .uri(java.net.URI.create("http://" + flinkHost + ":" + flinkPort + "/jobs/overview"))
                    .GET().build(),
                java.net.http.HttpResponse.BodyHandlers.ofString()
            ).body();
            JsonNode jobs = objectMapper.readTree(jobsResp).get("jobs");
            if (jobs != null && jobs.isArray()) {
                // Pick the most recently started new job (any state, including FINISHED)
                String bestJid = null;
                long bestTime = -1;
                for (int i = 0; i < jobs.size(); i++) {
                    JsonNode j = jobs.get(i);
                    String jid = j.get("jid").asText();
                    if (!beforeIds.contains(jid)) {
                        String state = j.has("state") ? j.get("state").asText() : "";
                          // Accept any state (including FAILED)
                          long startTime = j.has("start-time") ? j.get("start-time").asLong() : 0;
                        if (startTime > bestTime) {
                            bestTime = startTime;
                            bestJid = jid;
                        }
                    }
                }
                if (bestJid != null) {
                    log.info("Found new Flink job (most recent): " + bestJid);
                    return bestJid;
                }
            }
        } catch (Exception e) {
            log.debug("findNewFlinkJob error: " + e.getMessage());
        }
        return null;
    }

    private List<DagDefinition.DagNode> topologicalSort(DagDefinition dag) {
        List<DagDefinition.DagNode> sorted = new ArrayList<>();
        Map<String, Integer> inDegree = new HashMap<>();
        Map<String, List<String>> adj = new HashMap<>();
        for (DagDefinition.DagNode node : dag.getNodes()) {
            inDegree.put(node.getId(), 0);
            adj.put(node.getId(), new ArrayList<>());
        }
        for (DagDefinition.DagEdge edge : dag.getEdges()) {
            if (!adj.containsKey(edge.getSource())) adj.put(edge.getSource(), new ArrayList<>());
            adj.get(edge.getSource()).add(edge.getTarget());
            inDegree.put(edge.getTarget(), inDegree.getOrDefault(edge.getTarget(), 0) + 1);
        }
        java.util.Queue<String> queue = new java.util.LinkedList<>();
        for (Map.Entry<String, Integer> entry : inDegree.entrySet()) {
            if (entry.getValue() == 0) queue.add(entry.getKey());
        }
        Map<String, DagDefinition.DagNode> nodeMap = new HashMap<>();
        for (DagDefinition.DagNode node : dag.getNodes()) {
            nodeMap.put(node.getId(), node);
        }
        while (!queue.isEmpty()) {
            String id = queue.poll();
            sorted.add(nodeMap.get(id));
            if (adj.containsKey(id)) {
                for (String neighbor : adj.get(id)) {
                    inDegree.put(neighbor, inDegree.get(neighbor) - 1);
                    if (inDegree.get(neighbor) == 0) queue.add(neighbor);
                }
            }
        }
        return sorted;
    }

        private String generateDDLFromTemplate(String template, String tableName, Map<String, Object> params, String fieldsConfig) {
        JsonNode fa = tryParseJsonArray(fieldsConfig);
        StringBuilder schemaBuilder = new StringBuilder();
        try {
            if (fa != null && fa.isArray() && fa.size() > 0) {
                for (int i = 0; i < fa.size(); i++) {
                    JsonNode f = fa.get(i);
                    String fn = f.has("name") ? f.get("name").asText() : "field" + i;
                    String ft = f.has("type") ? f.get("type").asText() : "STRING";
                    schemaBuilder.append("  ").append(fn).append(" ").append(ft);
                    if (i < fa.size() - 1) schemaBuilder.append(",\n");
                }
            } else {
                schemaBuilder.append("  data STRING");
            }
        } catch (Exception e) {
            log.warn("Failed to parse fieldsConfig in generateDDLFromTemplate: {}", e.getMessage());
            schemaBuilder.append("  data STRING");
        }
        String modifiedTemplate = replaceDataStringInDDL(template, schemaBuilder.toString());
        return renderTemplate(modifiedTemplate, params, tableName);
    }

    private JsonNode tryParseJsonArray(String json) {
        for (int attempt = 0; attempt < 4; attempt++) {
            if (json == null || json.trim().isEmpty()) return null;
            try {
                JsonNode n = objectMapper.readTree(json.trim());
                if (n.isArray()) return n;
                if (n.isTextual()) { json = n.asText(); continue; }
            } catch (Exception e) {
                try {
                    String cleaned = json.trim();
                    if (cleaned.startsWith("\"") && cleaned.endsWith("\"")) {
                        cleaned = cleaned.substring(1, cleaned.length() - 1);
                    }
                    cleaned = cleaned.replace("\\\"", "\"").replace("\\n", "\n").replace("\\t", "\t").replace("\\\\", "\\");
                    JsonNode n = objectMapper.readTree(cleaned);
                    if (n.isArray()) return n;
                    if (n.isTextual()) { json = n.asText(); continue; }
                } catch (Exception e2) {
                    try {
                        java.util.regex.Matcher m = java.util.regex.Pattern.compile("\\[.*?\\]", java.util.regex.Pattern.DOTALL).matcher(json);
                        if (m.find()) {
                            JsonNode n = objectMapper.readTree(m.group());
                            if (n.isArray()) return n;
                        }
                    } catch (Exception e3) {}
                }
            }
        }
        return null;
    }

    private String findIncomingSourceTable(String nodeId, List<DagDefinition.DagEdge> edges, Map<String, String> tableAlias) {
        for (DagDefinition.DagEdge edge : edges) {
            if (edge.getTarget().equals(nodeId) && tableAlias.containsKey(edge.getSource())) {
                return edge.getSource();
            }
        }
        for (DagDefinition.DagEdge edge : edges) {
            if (edge.getTarget().equals(nodeId)) {
                return edge.getSource();
            }
        }
        return null;
    }

    private String findTransformBetween(String sourceNodeId, String outputNodeId, List<DagDefinition.DagEdge> edges, List<DagDefinition.DagNode> nodes) {
        for (DagDefinition.DagEdge edge : edges) {
            if (edge.getSource().equals(sourceNodeId)) {
                String midNode = edge.getTarget();
                for (DagDefinition.DagEdge edge2 : edges) {
                    if (edge2.getSource().equals(midNode) && edge2.getTarget().equals(outputNodeId)) {
                        for (DagDefinition.DagNode n : nodes) {
                            if (n.getId().equals(midNode)) {
                                String cat = n.getCategory();
                                if (cat != null && cat.equals("transform")) {
                                    return n.getId();
                                }
                            }
                        }
                    }
                }
            }
        }
        return null;
    }

    private String findTransformSql(String transformNodeId, List<DagDefinition.DagEdge> edges, List<DagDefinition.DagNode> nodes) {
        for (DagDefinition.DagNode node : nodes) {
            if (node.getId().equals(transformNodeId)) {
                String template;
                try {
                    template = controlService.findByType(node.getType()).getFlinkTemplate();
                } catch (Exception e) {
                    log.warn("Control not found for transform node {}: {}", node.getId(), e.getMessage());
                    return null;
                }
                if (template == null || template.isBlank()) return null;
                String sourceTable = null;
                for (DagDefinition.DagEdge edge : edges) {
                    if (edge.getTarget().equals(transformNodeId)) {
                        sourceTable = edge.getSource();
                        break;
                    }
                }
                if (sourceTable == null) return null;
                String rendered = renderTemplate(template, node.getParams(), transformNodeId);
                rendered = rendered.replace(sanitize(transformNodeId), sanitize(sourceTable));
                log.debug("Transform SQL for {}: {}", transformNodeId, rendered);
                return rendered;
            }
        }
        return null;
    }

    private String findIncomingSourceSchema(String nodeId, List<DagDefinition.DagEdge> edges, Map<String, String> nodeSchemas) {
        for (DagDefinition.DagEdge edge : edges) {
            if (edge.getTarget().equals(nodeId) && nodeSchemas.containsKey(edge.getSource())) {
                return nodeSchemas.get(edge.getSource());
            }
        }
        return null;
    }

    private String extractFieldsFromDdlTemplate(String template) {
        try {
            int ps = template.indexOf('('); int pe = template.indexOf(')');
            if (ps > 0 && pe > ps) { String cols = template.substring(ps+1, pe).trim(); if (!cols.isEmpty()) return cols; }
        } catch (Exception e) { log.warn("Failed to extract fields from DDL template: {}", e.getMessage()); }
        return null;
    }

        private String replaceDataStringInDDL(String template, String newFields) {
        // Replace "data STRING" with actual source schema
        String result = template.replaceAll("(?m)^[ \\t]*data\\s+STRING[ \\t]*(,?)[ \\t]*$", newFields + "$1");
        if (result.equals(template)) {
            result = template.replaceAll("data\\s+STRING", newFields);
        }
        return result;
    }

    private String stripCsvHeaderIfNeeded(String path, String hasHeader) {
        if (!"true".equalsIgnoreCase(hasHeader) || path == null || path.isEmpty()) return path;
        try {
            java.io.File f = new java.io.File(path);
            if (!f.exists() || !f.isFile()) return path;
            java.util.List<String> lines = java.nio.file.Files.readAllLines(f.toPath(), java.nio.charset.StandardCharsets.UTF_8);
            if (lines.size() <= 1) return path;
            String tmpPath = path + ".nohdr";
            java.nio.file.Files.write(java.nio.file.Paths.get(tmpPath), lines.subList(1, lines.size()), java.nio.charset.StandardCharsets.UTF_8);
            log.info("CSV header stripped: {} -> {} ({} data lines)", path, tmpPath, lines.size() - 1);
            return tmpPath;
        } catch (Exception e) {
            log.warn("Failed to strip CSV header for {}: {}", path, e.getMessage());
            return path;
        }
    }

    private String renderTemplate(String template, Map<String, Object> params, String nodeId) {
        String result = template;
        result = result.replace("${id}", sanitize(nodeId));
        if (params != null) {
            for (Map.Entry<String, Object> entry : params.entrySet()) {
                result = result.replace("${" + entry.getKey() + "}", entry.getValue() != null ? entry.getValue().toString() : "");
            }
        }
        return result;
    }

    /**
     * 用户名/密码解析：空值或 ${MYSQL_USERNAME} / ${MYSQL_PASSWORD} 占位符回退到环境变量默认值
     */
    private String resolveCredential(Object value, String envDefault) {
        if (value == null || value.toString().trim().isEmpty()) return envDefault == null ? "" : envDefault;
        String v = value.toString().trim();
        if ("${MYSQL_USERNAME}".equals(v) || "${MYSQL_PASSWORD}".equals(v)) {
            return envDefault == null ? "" : envDefault;
        }
        return v;
    }

    private String sanitize(String id) { return id.replaceAll("[^a-zA-Z0-9_]", "_"); }

    private List<String> parseCsvFields(Object rawObj) {
        String raw = rawObj != null ? rawObj.toString() : "";
        raw = raw.trim();
        if (raw.startsWith("[") && raw.endsWith("]")) raw = raw.substring(1, raw.length() - 1);
        List<String> out = new ArrayList<>();
        for (String p : raw.split(",")) {
            String t = p.trim();
            if (t.isEmpty()) continue;
            out.add(t);
        }
        return out;
    }

    private String quoteFieldsList(List<String> names) {
        List<String> quoted = new ArrayList<>();
        for (String n : names) quoted.add(quoteFlinkField(n));
        return String.join(", ", quoted);
    }

    private String buildValidateSql(Map<String, Object> params, String upstreamTable) {
        String checkFields = params != null && params.get("checkFields") != null ? params.get("checkFields").toString().trim() : "";
        boolean ignoreEmpty = params == null || params.get("ignoreEmpty") == null || Boolean.parseBoolean(params.get("ignoreEmpty").toString());
        List<String> fields = parseCsvFields(checkFields);
        if (fields.isEmpty()) throw new RuntimeException("空值校验: 请填写必填字段 checkFields（逗号分隔）");
        List<String> conds = new ArrayList<>();
        for (String f : fields) {
            String q = quoteFlinkField(f);
            conds.add(ignoreEmpty ? q + " IS NOT NULL AND " + q + " <> ''" : q + " IS NOT NULL");
        }
        return "SELECT * FROM " + upstreamTable + " WHERE " + String.join(" AND ", conds);
    }

    private String buildValueList(String raw) {
        if (raw == null || raw.trim().isEmpty()) throw new RuntimeException("条件路由: 请填写匹配值 matchValues（逗号分隔）");
        List<String> parts = new ArrayList<>();
        for (String p : raw.split(",")) {
            String t = p.trim();
            if (t.isEmpty()) continue;
            parts.add(t.matches("-?\\d+(\\.\\d+)?") ? t : "'" + t.replace("'", "''") + "'");
        }
        if (parts.isEmpty()) throw new RuntimeException("条件路由: 匹配值不能为空");
        return String.join(", ", parts);
    }

    private String buildRouteSql(DagDefinition.DagEdge edge, List<DagDefinition.DagEdge> edges, Map<String, Object> params, String upstreamTable) {
        String routeField = params != null && params.get("routeField") != null ? params.get("routeField").toString().trim() : "";
        String matchValues = params != null && params.get("matchValues") != null ? params.get("matchValues").toString().trim() : "";
        if (routeField.isEmpty()) throw new RuntimeException("条件路由: 请填写路由字段 routeField");
        String inList = buildValueList(matchValues);
        List<DagDefinition.DagEdge> outs = new ArrayList<>();
        for (DagDefinition.DagEdge e : edges) {
            if (e.getSource().equals(edge.getSource())) outs.add(e);
        }
        outs.sort(java.util.Comparator.comparing(e -> e.getId() == null ? "" : e.getId()));
        int idx = -1;
        for (int i = 0; i < outs.size(); i++) {
            DagDefinition.DagEdge e = outs.get(i);
            boolean same = (e == edge);
            if (!same && e.getId() != null && edge.getId() != null && e.getId().equals(edge.getId()) && e.getTarget().equals(edge.getTarget())) same = true;
            if (same) { idx = i; break; }
        }
        String cond;
        if (idx <= 0) {
            cond = quoteFlinkField(routeField) + " IN (" + inList + ")";
        } else {
            cond = quoteFlinkField(routeField) + " NOT IN (" + inList + ") OR " + quoteFlinkField(routeField) + " IS NULL";
        }
        return "SELECT * FROM " + upstreamTable + " WHERE " + cond;
    }

    private List<String> parseFieldList(Map<String, Object> params) {
        String raw = params != null && params.get("fields") != null ? params.get("fields").toString() : "";
        raw = raw.trim();
        if (raw.startsWith("[") && raw.endsWith("]")) raw = raw.substring(1, raw.length() - 1);
        List<String> out = new ArrayList<>();
        for (String p : raw.split(",")) {
            String t = p.trim();
            if (t.isEmpty()) continue;
            out.add(t);
        }
        if (out.isEmpty()) throw new RuntimeException("字段过滤: 请填写要保留的字段（逗号分隔）");
        return out;
    }

    private Map<String, String> parseRenameMappings(Map<String, Object> params) {
        String raw = params != null && params.get("mappings") != null ? params.get("mappings").toString() : "";
        Map<String, String> m = new LinkedHashMap<>();
        for (String part : raw.split(",")) {
            String p = part.trim();
            if (p.isEmpty()) continue;
            int eq = p.indexOf('=');
            if (eq <= 0 || eq >= p.length() - 1) {
                throw new RuntimeException("字段改名: 映射格式应为 oldName=newName，收到: " + p);
            }
            m.put(p.substring(0, eq).trim(), p.substring(eq + 1).trim());
        }
        if (m.isEmpty()) throw new RuntimeException("字段改名: 请填写改名映射（逗号分隔的 old=new）");
        return m;
    }

    private List<String> parseSchemaFieldNames(String schema) {
        List<String> names = new ArrayList<>();
        if (schema == null) return names;
        for (String line : schema.split("\n")) {
            String t = line.trim();
            if (t.isEmpty()) continue;
            if (t.endsWith(",")) t = t.substring(0, t.length() - 1).trim();
            String name = parseSchemaFieldName(t);
            if (name != null) names.add(name);
        }
        return names;
    }

    private String parseSchemaFieldName(String line) {
        if (line == null || line.isEmpty()) return null;
        int sp = line.indexOf(' ');
        if (sp <= 0) return null;
        String name = line.substring(0, sp).trim();
        if (name.startsWith("`") && name.endsWith("`") && name.length() >= 2) {
            name = name.substring(1, name.length() - 1).replace("``", "`");
        }
        return name;
    }

    private String quoteFlinkField(String raw) {
        String name = raw == null ? "" : raw.trim().replace("`", "");
        if (name.isEmpty()) return "`col`";
        return "`" + name.replace("`", "``") + "`";
    }

    private String filterSchemaFields(String incomingSchema, List<String> keep) {
        if (incomingSchema == null) throw new RuntimeException("字段过滤: 无法获取上游字段，请确认连线");
        List<String> available = parseSchemaFieldNames(incomingSchema);
        List<String> missing = new ArrayList<>();
        for (String k : keep) {
            if (!available.contains(k)) missing.add(k);
        }
        if (!missing.isEmpty()) {
            throw new RuntimeException("字段过滤: 字段不存在: " + String.join(", ", missing) + "（可选字段: " + String.join(", ", available) + "）");
        }
        StringBuilder sb = new StringBuilder();
        for (String line : incomingSchema.split("\n")) {
            String t = line.trim();
            if (t.isEmpty()) continue;
            if (t.endsWith(",")) t = t.substring(0, t.length() - 1).trim();
            String name = parseSchemaFieldName(t);
            if (name == null || !keep.contains(name)) continue;
            if (sb.length() > 0) sb.append(",\n");
            sb.append("  ").append(t);
        }
        return sb.length() == 0 ? incomingSchema : sb.toString();
    }

    private String renameSchemaFields(String incomingSchema, Map<String, String> mappings) {
        if (incomingSchema == null) throw new RuntimeException("字段改名: 无法获取上游字段，请确认连线");
        List<String> available = parseSchemaFieldNames(incomingSchema);
        for (Map.Entry<String, String> e : mappings.entrySet()) {
            if (!available.contains(e.getKey())) {
                throw new RuntimeException("字段改名: 字段不存在: " + e.getKey() + "（可选字段: " + String.join(", ", available) + "）");
            }
            if (available.contains(e.getValue())) {
                throw new RuntimeException("字段改名: 新字段名与已有字段冲突: " + e.getValue());
            }
        }
        StringBuilder sb = new StringBuilder();
        for (String line : incomingSchema.split("\n")) {
            String t = line.trim();
            if (t.isEmpty()) continue;
            if (t.endsWith(",")) t = t.substring(0, t.length() - 1).trim();
            String name = parseSchemaFieldName(t);
            if (name == null) continue;
            String newName = mappings.get(name);
            int sp = t.indexOf(' ');
            String type = sp > 0 ? t.substring(sp).trim() : "STRING";
            if (sb.length() > 0) sb.append(",\n");
            sb.append("  ").append(quoteFlinkField(newName != null ? newName : name)).append(" ").append(type);
        }
        return sb.toString();
    }

    private String buildFieldFilterSelect(Map<String, Object> params) {
        return quoteFieldsList(parseFieldList(params));
    }

    private String buildFieldRenameSelect(Map<String, Object> params, String incomingSchema) {
        Map<String, String> mappings = parseRenameMappings(params);
        List<String> names = parseSchemaFieldNames(incomingSchema);
        List<String> parts = new ArrayList<>();
        for (String n : names) {
            String nn = mappings.get(n);
            parts.add(nn != null ? quoteFlinkField(n) + " AS " + quoteFlinkField(nn) : quoteFlinkField(n));
        }
        if (parts.isEmpty()) throw new RuntimeException("字段改名: 无法获取上游字段，请确认连线");
        return String.join(", ", parts);
    }

    private String buildJsonParseSelect(Map<String, Object> params, String incomingSchema) {
        String srcField = params != null && params.get("sourceField") != null ? params.get("sourceField").toString().trim() : "";
        String fc = params != null && params.get("fieldsConfig") != null ? params.get("fieldsConfig").toString() : "[]";
        if (srcField.isEmpty()) throw new RuntimeException("JSON 解析: 请填写 sourceField 参数");
        JsonNode fa = tryParseJsonArray(fc);
        if (fa == null || !fa.isArray() || fa.size() == 0) {
            throw new RuntimeException("JSON 解析: fieldsConfig 为空，请按 [{\"name\":\"id\",\"type\":\"INT\"}] 填写");
        }
        List<String> incoming = parseSchemaFieldNames(incomingSchema);
        List<String> parts = new ArrayList<>();
        for (String n : incoming) parts.add(quoteFlinkField(n));
        for (JsonNode f : fa) {
            String fn = f.has("name") ? f.get("name").asText().trim() : "";
            String ft = f.has("type") ? f.get("type").asText().trim() : "STRING";
            String path = f.has("path") && !f.get("path").asText().trim().isEmpty() ? f.get("path").asText().trim() : "$." + fn;
            if (fn.isEmpty()) throw new RuntimeException("JSON 解析: 解析字段缺少 name");
            if (incoming.contains(fn)) {
                throw new RuntimeException("JSON 解析: 解析字段与上游字段冲突: " + fn);
            }
            parts.add("CAST(JSON_VALUE(" + quoteFlinkField(srcField) + ", '" + path.replace("'", "''") + "') AS " + ft + ") AS " + quoteFlinkField(fn));
        }
        return String.join(", ", parts);
    }

    private String inferMysqlFields(String url, String table, String username, String password) {
        String quotedTable = "`" + table.replace("`", "``") + "`";
        try (Connection conn = DriverManager.getConnection(url, username, password);
             Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT * FROM " + quotedTable + " LIMIT 0")) {
            ResultSetMetaData md = rs.getMetaData();
            StringBuilder sb = new StringBuilder();
            for (int i = 1; i <= md.getColumnCount(); i++) {
                String name = md.getColumnLabel(i);
                String flinkType = mysqlToFlinkType(md.getColumnType(i), md.getPrecision(i), md.getScale(i));
                if (sb.length() > 0) sb.append(",\n");
                sb.append("  ").append(quoteFlinkField(name)).append(" ").append(flinkType);
            }
            if (sb.length() == 0) throw new RuntimeException("MySQL 输入: 表无字段: " + table);
            return sb.toString();
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException("MySQL 输入读取表结构失败: " + e.getMessage() + "（请检查 url/table/username/password 与网络）", e);
        }
    }

    private String generateMysqlInputDDL(String tableName, String url, String table, String username, String password, String fields) {
        StringBuilder ddl = new StringBuilder();
        ddl.append("CREATE TABLE ").append(tableName).append(" (\n");
        ddl.append(fields).append("\n");
        ddl.append(") WITH (\n");
        ddl.append("  'connector' = 'jdbc',\n");
        ddl.append("  'url' = '").append(url.replace("'", "''")).append("',\n");
        ddl.append("  'table-name' = '").append(table.replace("'", "''")).append("',\n");
        ddl.append("  'username' = '").append(username.replace("'", "''")).append("',\n");
        ddl.append("  'password' = '").append(password.replace("'", "''")).append("',\n");
        ddl.append("  'driver' = 'com.mysql.cj.jdbc.Driver',\n");
        ddl.append("  'scan.fetch-size' = '1000'\n");
        ddl.append(");\n");
        return ddl.toString();
    }

    private String generateJsonInputDDL(String tableName, String path, String fieldsConfig) {
        JsonNode fa = tryParseJsonArray(fieldsConfig);
        StringBuilder ddl = new StringBuilder();
        ddl.append("CREATE TABLE ").append(tableName).append(" (\n");
        try {
            if (fa != null && fa.isArray() && fa.size() > 0) {
                for (int i = 0; i < fa.size(); i++) {
                    JsonNode f = fa.get(i);
                    String fn = f.has("name") ? f.get("name").asText() : "field" + i;
                    String ft = f.has("type") ? f.get("type").asText() : "STRING";
                    ddl.append("  ").append(quoteFlinkField(fn)).append(" ").append(ft);
                    if (i < fa.size() - 1) ddl.append(",");
                    ddl.append("\n");
                }
            } else {
                ddl.append("  data STRING\n");
            }
        } catch (Exception e) {
            log.warn("Failed to parse json fieldsConfig: {}", e.getMessage());
            ddl.append("  data STRING\n");
        }
        ddl.append(") WITH (\n");
        ddl.append("  'connector' = 'filesystem',\n");
        ddl.append("  'path' = '").append(path.replace("'", "''")).append("',\n");
        ddl.append("  'format' = 'json',\n");
        ddl.append("  'json.ignore-parse-errors' = 'true',\n");
        ddl.append("  'json.fail-on-missing-field' = 'false'\n");
        ddl.append(");\n");
        return ddl.toString();
    }

    private String mysqlToFlinkType(int jdbcType, int precision, int scale) {
        switch (jdbcType) {
            case java.sql.Types.INTEGER: return "INT";
            case java.sql.Types.BIGINT: return "BIGINT";
            case java.sql.Types.SMALLINT: return "SMALLINT";
            case java.sql.Types.TINYINT: return "TINYINT";
            case java.sql.Types.FLOAT:
            case java.sql.Types.REAL: return "FLOAT";
            case java.sql.Types.DOUBLE: return "DOUBLE";
            case java.sql.Types.DECIMAL:
            case java.sql.Types.NUMERIC: return "DECIMAL(" + Math.max(precision, 1) + "," + Math.max(scale, 0) + ")";
            case java.sql.Types.BIT:
            case java.sql.Types.BOOLEAN: return "BOOLEAN";
            case java.sql.Types.CHAR:
            case java.sql.Types.VARCHAR:
            case java.sql.Types.LONGVARCHAR:
            case java.sql.Types.NCHAR:
            case java.sql.Types.NVARCHAR:
            case java.sql.Types.LONGNVARCHAR: return "STRING";
            case java.sql.Types.DATE: return "DATE";
            case java.sql.Types.TIME: return "TIME";
            case java.sql.Types.TIMESTAMP: return "TIMESTAMP(3)";
            case java.sql.Types.BINARY:
            case java.sql.Types.VARBINARY:
            case java.sql.Types.LONGVARBINARY:
            case java.sql.Types.BLOB: return "BYTES";
            default: return "STRING";
        }
    }


    private String extractFieldsFromDatagenConfig(String fieldsConfig) {
        JsonNode fa = tryParseJsonArray(fieldsConfig);
        if (fa != null && fa.isArray() && fa.size() > 0) {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < fa.size(); i++) {
                JsonNode f = fa.get(i);
                String fn = f.has("name") ? f.get("name").asText() : "field" + i;
                String ft = f.has("type") ? f.get("type").asText() : "STRING";
                sb.append("  ").append(fn).append(" ").append(ft);
                if (i < fa.size()-1) sb.append(",\n");
            }
            return sb.toString();
        }
        return null;
    }


    private List<String> detectCsvColumns(String path, String delimiter, String hasHeader) {
        try {
            java.io.File f = new java.io.File(path);
            if (!f.exists() || !f.isFile()) return null;
            String effDelim = autoDetectDelimiter(path, delimiter);
            try (BufferedReader reader = Files.newBufferedReader(f.toPath(), StandardCharsets.UTF_8)) {
                String firstLine = reader.readLine();
                if (firstLine == null || firstLine.trim().isEmpty()) return null;
                CSVFormat format = CSVFormat.DEFAULT.builder()
                        .setDelimiter(effDelim.charAt(0))
                        .setIgnoreEmptyLines(true)
                        .build();
                List<CSVRecord> records = CSVParser.parse(firstLine, format).getRecords();
                if (records.isEmpty()) return null;
                List<String> names = new ArrayList<>();
                Set<String> used = new HashSet<>();
                if ("true".equalsIgnoreCase(hasHeader)) {
                    for (int i = 0; i < records.get(0).size(); i++) {
                        String name = records.get(0).get(i).trim();
                        String clean = sanitizeColumn(name);
                        if (clean.isEmpty()) clean = "col" + (i + 1);
                        String base = clean;
                        int k = 2;
                        while (used.contains(clean)) { clean = base + "_" + k; k++; }
                        used.add(clean);
                        names.add(clean);
                    }
                } else {
                    int n = records.get(0).size();
                    for (int i = 1; i <= n; i++) names.add("col" + i);
                }
                return names;
            }
        } catch (Exception e) {
            log.warn("Failed to detect CSV columns for {}: {}", path, e.getMessage());
            return null;
        }
    }

    private String autoDetectDelimiter(String path, String fallback) {
        try {
            java.io.File f = new java.io.File(path);
            if (!f.exists() || !f.isFile()) return fallback;
            String firstLine;
            try (BufferedReader reader = Files.newBufferedReader(f.toPath(), StandardCharsets.UTF_8)) {
                firstLine = reader.readLine();
            }
            if (firstLine == null) return fallback;
            char[] candidates = {',', '\t', ';', '|'};
            char best = (fallback != null && !fallback.isEmpty()) ? fallback.charAt(0) : ',';
            int bestCount = countOccurrences(firstLine, best);
            for (char c : candidates) {
                int cnt = countOccurrences(firstLine, c);
                if (cnt > bestCount) { best = c; bestCount = cnt; }
            }
            return String.valueOf(best);
        } catch (Exception e) {
            return fallback;
        }
    }

    private int countOccurrences(String s, char c) {
        int n = 0;
        for (int i = 0; i < s.length(); i++) if (s.charAt(i) == c) n++;
        return n;
    }

    private String sanitizeColumn(String name) {
        String s = name.trim().replaceAll("[^a-zA-Z0-9_\\u4e00-\\u9fa5]", "_");
        if (s.isEmpty()) s = "col";
        if (Character.isDigit(s.charAt(0))) s = "_" + s;
        return s;
    }

    private String generateCsvInputDDL(String tableName, String path, String delimiter, String hasHeader, String fieldsConfig, List<String> autoCols) {
        JsonNode fa = tryParseJsonArray(fieldsConfig);
        StringBuilder ddl = new StringBuilder();
        ddl.append("CREATE TABLE ").append(tableName).append(" (\n");
        try {
            if (fa != null && fa.isArray() && fa.size() > 0) {
                for (int i = 0; i < fa.size(); i++) {
                    JsonNode f = fa.get(i);
                    String fn = f.has("name") ? f.get("name").asText() : "field" + i;
                    String ft = f.has("type") ? f.get("type").asText() : "STRING";
                    ddl.append("  `").append(fn).append("` ").append(ft);
                    if (i < fa.size()-1) ddl.append(",");
                    ddl.append("\n");
                }
            } else if (autoCols != null && !autoCols.isEmpty()) {
                for (int i = 0; i < autoCols.size(); i++) {
                    ddl.append("  `").append(autoCols.get(i)).append("` STRING");
                    if (i < autoCols.size()-1) ddl.append(",");
                    ddl.append("\n");
                }
            } else {
                ddl.append("  data STRING\n");
            }
        } catch (Exception e) {
            log.warn("Failed to parse csv fieldsConfig: {}", e.getMessage());
            ddl.append("  data STRING\n");
        }
        ddl.append(") WITH (\n");
        ddl.append("  'connector' = 'filesystem',\n");
        ddl.append("  'path' = '").append(path).append("',\n");
        ddl.append("  'format' = 'csv',\n");
        ddl.append("  'csv.delimiter' = '").append(delimiter).append("',\n");
        ddl.append("  'csv.ignore-parse-errors' = 'true',\n");
        ddl.append("  'csv.allow-comments' = 'true'\n");
        ddl.append(");\n");
        return ddl.toString();
    }

    private String generateDatagenDDL(String tableName, String rowsPerSecond, String fieldsConfig) {
        if (rowsPerSecond == null || rowsPerSecond.isEmpty()) rowsPerSecond = "10";
        if (fieldsConfig == null || fieldsConfig.isEmpty()) fieldsConfig = "[]";
        // Handle double-escaped JSON from UI (string inside string)
        JsonNode fa = tryParseJsonArray(fieldsConfig);
        StringBuilder ddl = new StringBuilder();
        ddl.append("CREATE TABLE ").append(tableName).append(" (\n");
        try {
            if (fa != null && fa.isArray() && fa.size() > 0) {
                for (int i = 0; i < fa.size(); i++) {
                    JsonNode f = fa.get(i);
                    String fn = f.has("name") ? f.get("name").asText() : "field" + i;
                    String ft = f.has("type") ? f.get("type").asText() : "STRING";
                    ddl.append("  ").append(fn).append(" ").append(ft);
                    if (i < fa.size()-1) ddl.append(",");
                    ddl.append("\n");
                }
            } else {
                ddl.append("  data STRING\n");
            }
        } catch (Exception e) {
            log.warn("Failed to parse fieldsConfig: {}", e.getMessage());
            ddl.append("  data STRING\n");
        }
        ddl.append(") WITH (\n");
        ddl.append("  'connector' = 'datagen',\n");
        ddl.append("  'rows-per-second' = '").append(rowsPerSecond).append("'");
        if (fa != null && fa.isArray()) {
            for (int i = 0; i < fa.size(); i++) {
                JsonNode f = fa.get(i);
                String fn = f.has("name") ? f.get("name").asText() : "field" + i;
                if (f.has("kind") && "sequence".equals(f.get("kind").asText())) {
                    ddl.append(",\n  'fields.").append(fn).append(".kind' = 'sequence'");
                    if (f.has("start")) ddl.append(",\n  'fields.").append(fn).append(".start' = '").append(f.get("start").asText()).append("'");
                    if (f.has("end")) ddl.append(",\n  'fields.").append(fn).append(".end' = '").append(f.get("end").asText()).append("'");
                } else {
                    if (f.has("min")) ddl.append(",\n  'fields.").append(fn).append(".min' = '").append(f.get("min").asText()).append("'");
                    if (f.has("max")) ddl.append(",\n  'fields.").append(fn).append(".max' = '").append(f.get("max").asText()).append("'");
                    if (f.has("length")) ddl.append(",\n  'fields.").append(fn).append(".length' = '").append(f.get("length").asText()).append("'");
                }
            }
        }
        ddl.append("\n);\n");
        return ddl.toString();
    }

    /**
     * Generate Kafka input DDL (source table reading from Kafka topic)
     */
    private String generateKafkaInputDDL(String tableName, String topic, String bootstrapServers, String fieldsConfig) {
        JsonNode fa = tryParseJsonArray(fieldsConfig);
        StringBuilder ddl = new StringBuilder();
        ddl.append("CREATE TABLE ").append(tableName).append(" (\n");
        try {
            if (fa != null && fa.isArray() && fa.size() > 0) {
                for (int i = 0; i < fa.size(); i++) {
                    JsonNode f = fa.get(i);
                    String fn = f.has("name") ? f.get("name").asText() : "field" + i;
                    String ft = f.has("type") ? f.get("type").asText() : "STRING";
                    ddl.append("  ").append(fn).append(" ").append(ft);
                    if (i < fa.size() - 1) ddl.append(",");
                    ddl.append("\n");
                }
            } else {
                ddl.append("  data STRING\n");
            }
        } catch (Exception e) {
            log.warn("Failed to parse Kafka fieldsConfig: {}", e.getMessage());
            ddl.append("  data STRING\n");
        }
        ddl.append(") WITH (\n");
        ddl.append("  'connector' = 'kafka',\n");
        ddl.append("  'topic' = '").append(topic).append("',\n");
        ddl.append("  'properties.bootstrap.servers' = '").append(bootstrapServers).append("',\n");
        ddl.append("  'properties.group.id' = 'flink-group-").append(tableName).append("',\n");
        ddl.append("  'scan.startup.mode' = 'earliest-offset',\n");
        ddl.append("  'format' = 'json',\n");
        ddl.append("  'json.fail-on-missing-field' = 'false',\n");
        ddl.append("  'json.ignore-parse-errors' = 'true'\n");
        ddl.append(");\n");
        return ddl.toString();
    }

    /**
     * Generate Kafka output DDL (sink table writing to Kafka topic)
     */
    private String generateKafkaOutputDDL(String tableName, String topic, String bootstrapServers, String sourceFields) {
        StringBuilder ddl = new StringBuilder();
        ddl.append("CREATE TABLE ").append(tableName).append(" (\n");
        if (sourceFields != null && !sourceFields.isEmpty()) {
            ddl.append(sourceFields).append("\n");
        } else {
            ddl.append("  data STRING\n");
        }
        ddl.append(") WITH (\n");
        ddl.append("  'connector' = 'kafka',\n");
        ddl.append("  'topic' = '").append(topic).append("',\n");
        ddl.append("  'properties.bootstrap.servers' = '").append(bootstrapServers).append("',\n");
        ddl.append("  'format' = 'json'\n");
        ddl.append(");\n");

        return ddl.toString();
    }
}






