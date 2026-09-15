package com.datastream.mvp.service;

import com.datastream.mvp.util.JdbcUrlUtil;

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
    private final PostgresqlTableCreator postgresqlTableCreator;
    private final OracleTableCreator oracleTableCreator;
    private final XmlPreprocessor xmlPreprocessor;
    private final JsonPreprocessor jsonPreprocessor;
    private final ParquetPreprocessor parquetPreprocessor;

    @Value("${flink.home:D:\\code\\flink-1.18.1}")
    private String flinkHome;

    @Value("${flink.cluster.host:localhost}")
    private String flinkHost;

    /** 本次提交时刻（毫秒），用于认领新作业时排除归档后重现的旧作业 */
    private volatile long baselineTime = 0;

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

    @Value("${app.postgres.default-username:postgres}")
    private String defaultPostgresUsername;

    @Value("${app.postgres.default-password:}")
    private String defaultPostgresPassword;

    @Value("${app.oracle.default-username:system}")
    private String defaultOracleUsername;

    @Value("${app.oracle.default-password:}")
    private String defaultOraclePassword;

    @Value("${app.storage.output-root:../output}")
    private String outputRoot;

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
        boolean hasRedisLookup = dag.getNodes().stream().anyMatch(n -> "redis_lookup".equals(n.getType()));
        if (hasRedisLookup) {
            flinkSql.append("CREATE FUNCTION IF NOT EXISTS redis_lookup AS 'com.datastream.udf.RedisLookupUdf' LANGUAGE JAVA;\n\n");
        }
        List<DagDefinition.DagNode> sortedNodes = topologicalSort(dag);
        Map<String, String> tableAlias = new HashMap<>();
        Map<String, String> nodeSchemas = new HashMap<>();
        // HDFS 输入的表头过滤条件（nodeId -> WHERE 条件），在边循环生成 SELECT 时应用
        Map<String, String> hdfsHeaderFilters = new HashMap<>();

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
                path = resolveRuntimePath(path, "/data");
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
                // 兼容历史作业保存的宿主绝对路径（容器内映射到 /data）
                path = resolveRuntimePath(path, "/data");
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
                path = resolveRuntimePath(path, "/data");
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
                path = resolveRuntimePath(path, "/data");
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
                String url = JdbcUrlUtil.normalize(node.getParams() != null && node.getParams().get("url") != null ? node.getParams().get("url").toString() : "");
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
            // PostgreSQL Input -> JDBC 读表（复用 JDBC connector，PG 方言），字段自动推导
            if ("pg_input".equals(node.getType())) {
                String url = node.getParams() != null && node.getParams().get("url") != null ? node.getParams().get("url").toString().trim() : "";
                String table = node.getParams() != null && node.getParams().get("table") != null ? node.getParams().get("table").toString().trim() : "";
                String username = resolveCredential(node.getParams() != null ? node.getParams().get("username") : null, defaultPostgresUsername);
                String password = resolveCredential(node.getParams() != null ? node.getParams().get("password") : null, defaultPostgresPassword);
                if (url.isEmpty() || table.isEmpty()) {
                    throw new RuntimeException("PostgreSQL 输入缺少 url / table 参数");
                }
                if (!url.startsWith("jdbc:postgresql://")) {
                    throw new RuntimeException("PostgreSQL 输入 url 必须以 jdbc:postgresql:// 开头: " + url);
                }
                String fields = inferJdbcFields(url, table, username, password, "PostgreSQL");
                String rendered = generateJdbcInputDDL(sanitize(node.getId()), url, table, username, password, fields, "org.postgresql.Driver");
                nodeSchemas.put(node.getId(), fields);
                log.info("PostgreSQL input {}: {}.{} -> {} fields", node.getId(), url, table, fields.split("\n").length);
                flinkSql.append("-- Node: ").append(node.getLabel()).append(" (").append(node.getId()).append(") [PostgreSQL Input]\n");
                flinkSql.append(rendered).append("\n\n");
                tableAlias.put(node.getId(), sanitize(node.getId()));
                continue;
            }
            // Oracle Input -> JDBC 读表（Oracle 方言），字段自动推导
            if ("oracle_input".equals(node.getType())) {
                String url = node.getParams() != null && node.getParams().get("url") != null ? node.getParams().get("url").toString().trim() : "";
                String table = node.getParams() != null && node.getParams().get("table") != null ? node.getParams().get("table").toString().trim().toUpperCase() : "";
                String username = resolveCredential(node.getParams() != null ? node.getParams().get("username") : null, defaultOracleUsername).toUpperCase();
                String password = resolveCredential(node.getParams() != null ? node.getParams().get("password") : null, defaultOraclePassword);
                if (url.isEmpty() || table.isEmpty()) {
                    throw new RuntimeException("Oracle 输入缺少 url / table 参数");
                }
                if (!url.startsWith("jdbc:oracle:")) {
                    throw new RuntimeException("Oracle 输入 url 必须以 jdbc:oracle: 开头（如 jdbc:oracle:thin:@host:1521/FREE）: " + url);
                }
                Map<String, Object> oracleParams = new HashMap<>();
                if (node.getParams() != null) oracleParams.putAll(node.getParams());
                oracleParams.put("username", username);
                oracleParams.put("table", table);
                String qualifiedTable = oracleTableCreator.qualifiedTable(oracleParams);
                String fields = inferJdbcFields(url, qualifiedTable, username, password, "Oracle");
                String rendered = generateJdbcInputDDL(sanitize(node.getId()), url, qualifiedTable, username, password, fields, "oracle.jdbc.OracleDriver");
                nodeSchemas.put(node.getId(), fields);
                log.info("Oracle input {}: {}.{} -> {} fields", node.getId(), url, table, fields.split("\n").length);
                flinkSql.append("-- Node: ").append(node.getLabel()).append(" (").append(node.getId()).append(") [Oracle Input]\n");
                flinkSql.append(rendered).append("\n\n");
                tableAlias.put(node.getId(), sanitize(node.getId()));
                continue;
            }

            // HDFS Input -> 内置 filesystem connector 读 hdfs:// 路径，fieldsConfig 指定 schema
            if ("hdfs_input".equals(node.getType())) {
                String path = node.getParams() != null && node.getParams().get("path") != null ? node.getParams().get("path").toString().trim() : "";
                String fc = node.getParams() != null && node.getParams().get("fieldsConfig") != null ? node.getParams().get("fieldsConfig").toString() : "[]";
                String delimiter = node.getParams() != null && node.getParams().get("delimiter") != null ? node.getParams().get("delimiter").toString().trim() : ",";
                if (path.isEmpty()) {
                    throw new RuntimeException("HDFS 输入缺少 path 参数（如 hdfs://localhost:9000/data/students.csv）");
                }
                if (!path.startsWith("hdfs://")) {
                    throw new RuntimeException("HDFS 输入 path 必须以 hdfs:// 开头: " + path);
                }
                if (delimiter.length() != 1) {
                    throw new RuntimeException("HDFS 输入 delimiter 必须是单个字符");
                }
                String hasHeader = node.getParams() != null && node.getParams().get("hasHeader") != null
                        ? node.getParams().get("hasHeader").toString().trim() : "true";
                boolean skipHeader = !"false".equalsIgnoreCase(hasHeader);
                String rendered = generateHdfsInputDDL(sanitize(node.getId()), path, fc, delimiter, skipHeader);
                String schema = extractFieldsFromDatagenConfig(fc);
                // 跳表头时全列按 STRING 读，下游 schema 必须同步为 STRING，否则 sink 类型校验失败
                if (skipHeader) schema = forceStringSchema(fc);
                if (schema != null) nodeSchemas.put(node.getId(), schema);
                if (skipHeader) {
                    String filter = hdfsHeaderFilter(fc);
                    if (filter != null) hdfsHeaderFilters.put(node.getId(), filter);
                }
                log.info("HDFS input {}: {} (schema: {})", node.getId(), path, schema);
                flinkSql.append("-- Node: ").append(node.getLabel()).append(" (").append(node.getId()).append(") [HDFS Input]\n");
                flinkSql.append(rendered).append("\n\n");
                tableAlias.put(node.getId(), sanitize(node.getId()));
                continue;
            }

            // HDFS Output -> 内置 filesystem connector 写 hdfs:// 目录并生成 part 文件
            if ("hdfs_output".equals(node.getType())) {
                String path = node.getParams() != null && node.getParams().get("path") != null ? node.getParams().get("path").toString().trim() : "";
                String delimiter = node.getParams() != null && node.getParams().get("delimiter") != null ? node.getParams().get("delimiter").toString().trim() : ",";
                if (path.isEmpty()) {
                    throw new RuntimeException("HDFS 输出缺少 path 参数（如 hdfs://localhost:9000/output/result）");
                }
                if (!path.startsWith("hdfs://")) {
                    throw new RuntimeException("HDFS 输出 path 必须以 hdfs:// 开头: " + path);
                }
                String upstream = findIncomingSourceTable(node.getId(), dag.getEdges(), tableAlias);
                if (upstream == null) {
                    throw new RuntimeException("HDFS 输出缺少上游输入节点连线");
                }
                String fields = findIncomingSourceSchema(node.getId(), dag.getEdges(), nodeSchemas);
                if (fields == null || fields.isBlank()) {
                    throw new RuntimeException("HDFS 输出无法获取上游字段，请确认输入与转换节点 Schema 完整");
                }
                if (delimiter.length() != 1) {
                    throw new RuntimeException("HDFS 输出 delimiter 必须是单个字符");
                }
                String rendered = generateHdfsOutputDDL(sanitize(node.getId()), path, delimiter, fields);
                log.info("HDFS output {}: {}", node.getId(), path);
                flinkSql.append("-- Node: ").append(node.getLabel()).append(" (").append(node.getId()).append(") [HDFS Output]\n");
                flinkSql.append(rendered).append("\n\n");
                tableAlias.put(node.getId(), sanitize(node.getId()));
                continue;
            }

                        // JSON Input -> 支持 array 数组（转临时 CSV 自动 schema）与 lines（JSON Lines，fieldsConfig 指定 schema）
            if ("json_input".equals(node.getType())) {
                String path = node.getParams() != null && node.getParams().get("path") != null ? node.getParams().get("path").toString().trim() : "";
                if (path.isEmpty()) {
                    throw new RuntimeException("JSON 输入缺少 path 参数（auto/lines/array 模式均需要本地文件）");
                }
                path = resolveRuntimePath(path, "/data");
                if (!new java.io.File(path).isFile()) {
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
                String actualPath = node.getParams() != null ? node.getParams().getOrDefault("path", "D:\\code\\比赛\\2026省服务外包\\output\\output.xlsx").toString().trim() : "/output/output.xlsx";
                actualPath = resolveRuntimePath(actualPath, "/output");
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
                excelDdl.append("  'path' = '").append(escapeSqlLiteral(tempCsvPath)).append("',\n");
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
                String actualPath = node.getParams() != null ? node.getParams().getOrDefault("path", "D:\\code\\比赛\\2026省服务外包\\output\\output.parquet").toString().trim() : "/output/output.parquet";
                actualPath = resolveRuntimePath(actualPath, "/output");
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
                parquetDdl.append("  'path' = '").append(escapeSqlLiteral(tempCsvPath)).append("',\n");
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
                String actualPath = node.getParams() != null ? node.getParams().getOrDefault("path", "D:\\code\\比赛\\2026省服务外包\\output\\output.xml").toString().trim() : "/output/output.xml";
                actualPath = resolveRuntimePath(actualPath, "/output");
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
                xmlDdl.append("  'path' = '").append(escapeSqlLiteral(tempCsvPath)).append("',\n");
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


            // PostgreSQL Output -> JDBC Sink；按安全策略校验/创建目标表
            if ("pg_output".equals(node.getType())) {
                String fields = findIncomingSourceSchema(node.getId(), dag.getEdges(), nodeSchemas);
                if (fields == null || fields.isBlank()) {
                    throw new RuntimeException("PostgreSQL 输出无法获取上游字段，请确认节点已正确连线");
                }
                Map<String, Object> params = new HashMap<>();
                if (node.getParams() != null) params.putAll(node.getParams());
                String url = String.valueOf(params.getOrDefault("url", "")).trim();
                if (!url.startsWith("jdbc:postgresql://")) {
                    throw new RuntimeException("PostgreSQL 输出 url 必须以 jdbc:postgresql:// 开头");
                }
                String username = resolveCredential(params.get("username"), defaultPostgresUsername);
                String password = resolveCredential(params.get("password"), defaultPostgresPassword);
                params.put("username", username);
                params.put("password", password);
                int batchSize = parseBoundedInt(params.get("batchSize"), 1000, 1, 10000, "PostgreSQL 输出 batchSize");
                String qualifiedTable = postgresqlTableCreator.qualifiedTable(params);
                postgresqlTableCreator.ensureTable(params, fields);
                String rendered = generateJdbcOutputDDL(sanitize(node.getId()), url, qualifiedTable,
                        username, password, fields, "org.postgresql.Driver", batchSize);
                flinkSql.append("-- Node: ").append(node.getLabel()).append(" (").append(node.getId()).append(") [PostgreSQL Output]\n");
                flinkSql.append(rendered).append("\n\n");
                tableAlias.put(node.getId(), sanitize(node.getId()));
                continue;
            }

            // Oracle Output -> JDBC Sink；按安全策略校验/创建目标表
            if ("oracle_output".equals(node.getType())) {
                String fields = findIncomingSourceSchema(node.getId(), dag.getEdges(), nodeSchemas);
                if (fields == null || fields.isBlank()) {
                    throw new RuntimeException("Oracle 输出无法获取上游字段，请确认节点已正确连线");
                }
                Map<String, Object> params = new HashMap<>();
                if (node.getParams() != null) params.putAll(node.getParams());
                String url = String.valueOf(params.getOrDefault("url", "")).trim();
                if (!url.startsWith("jdbc:oracle:")) {
                    throw new RuntimeException("Oracle 输出 url 必须以 jdbc:oracle: 开头");
                }
                String username = resolveCredential(params.get("username"), defaultOracleUsername).toUpperCase();
                String password = resolveCredential(params.get("password"), defaultOraclePassword);
                params.put("username", username);
                params.put("password", password);
                int batchSize = parseBoundedInt(params.get("batchSize"), 500, 1, 10000, "Oracle 输出 batchSize");
                String qualifiedTable = oracleTableCreator.qualifiedTable(params);
                oracleTableCreator.ensureTable(params, fields);
                String rendered = generateJdbcOutputDDL(sanitize(node.getId()), url, qualifiedTable,
                        username, password, fields, "oracle.jdbc.OracleDriver", batchSize);
                flinkSql.append("-- Node: ").append(node.getLabel()).append(" (").append(node.getId()).append(") [Oracle Output]\n");
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
                flinkSql.append(renderTemplate(template, withSchemaDefaults(mapLocalInputPath(node.getParams()), control.getParamSchema()), node.getId())).append("\n\n");
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
                    origPath = resolveRuntimePath(origPath, "/output");
                    tmpParams.put("path", origPath + ".tmp");
                    renderParams = tmpParams;
                }
                if ("mysql_output".equals(node.getType())) {
                    // 凭据支持占位符/空值：提交时解析为环境变量 MYSQL_USERNAME / MYSQL_PASSWORD，避免明文入库
                    Map<String, Object> resolvedParams = new HashMap<>();
                    if (node.getParams() != null) resolvedParams.putAll(node.getParams());
                    if (resolvedParams.containsKey("url")) {
                        resolvedParams.put("url", JdbcUrlUtil.normalize(String.valueOf(resolvedParams.get("url"))));
                    }
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
                    rendered = renderTemplate(modifiedTemplate, withSchemaDefaults(renderParams, control.getParamSchema()), node.getId());
                } else {
                    rendered = renderTemplate(template, withSchemaDefaults(renderParams, control.getParamSchema()), node.getId());
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
                    } else if ("redis_lookup".equals(node.getType())) {
                        String tfn = node.getParams() != null ? node.getParams().getOrDefault("targetField", "extra_info").toString() : "extra_info";
                        nodeSchemas.put(node.getId(), incomingSchema + ",\n  `" + tfn + "` STRING");
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

        // transform→transform 的中间节点物化为 TEMPORARY VIEW，供下游 SELECT 引用；
        // 末尾 transform（直接连输出）仍内联进 INSERT，保持原有单 transform 作业的 SQL 形态不变。
        Set<String> materializedViews = new HashSet<>();

        for (DagDefinition.DagEdge edge : dag.getEdges()) {
            String sourceTable = sanitize(edge.getSource());
            String targetTable = sanitize(edge.getTarget());

            String targetCategory = nodeCategoryMap.get(edge.getTarget());
            if ("transform".equals(targetCategory)) {
                // 上游是 transform 时，当前边是链中间环节，需要把上游物化为视图
                if ("transform".equals(nodeCategoryMap.get(edge.getSource()))) {
                    String viewName = sanitize(edge.getSource());
                    if (materializedViews.add(viewName)) {
                        String viewSql = buildTransformSql(edge, dag.getEdges(), nodeTypeMap,
                                transformNodeParams, hdfsHeaderFilters, nodeSchemas);
                        flinkSql.append("CREATE TEMPORARY VIEW ").append(viewName).append(" AS\n")
                                .append(viewSql).append(";\n\n");
                    }
                }
                continue;
            }

            String sourceCategory = nodeCategoryMap.get(edge.getSource());
            if ("transform".equals(sourceCategory)) {
                // 上游已在链中间物化为视图，直接引用视图名
                String ts = buildTransformSql(edge, dag.getEdges(), nodeTypeMap,
                        transformNodeParams, hdfsHeaderFilters, nodeSchemas);
                flinkSql.append("INSERT INTO ").append(targetTable).append("\n").append(ts).append(";\n\n");
                continue;
            }

            // HDFS 输入若含表头，读入后在 SELECT 层过滤掉表头行
            String headerFilter = hdfsHeaderFilters.get(edge.getSource());
            if (headerFilter != null) {
                flinkSql.append("INSERT INTO ").append(targetTable)
                        .append(" SELECT * FROM ").append(sourceTable)
                        .append(" WHERE ").append(headerFilter).append(";\n");
            } else {
                flinkSql.append("INSERT INTO ").append(targetTable)
                        .append(" SELECT * FROM ").append(sourceTable).append(";\n");
            }
        }

        String translatedSql = flinkSql.toString();
        log.info("Generated Flink SQL:\n{}", redactSqlSecrets(translatedSql));
        return translatedSql;
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
        if (f == null) return;
        try {
            if (!com.datastream.mvp.util.ManagedFiles.deleteRecursively(f.toPath(), outputRoot)) {
                log.warn("Refusing to delete path outside managed output root: {}", f);
            }
        } catch (java.io.IOException e) {
            log.warn("Failed to delete managed output path {}: {}", f, e.getMessage());
        }
    }

    /**
     * 按分号切分多条 SQL 语句，忽略单引号字符串内部的 ';'。
     * 路径/密码/topic 里出现分号时，朴素 split(";") 会把一条语句从中间切断。
     */
    private java.util.List<String> splitSqlStatements(String sql) {
        java.util.List<String> out = new java.util.ArrayList<>();
        StringBuilder cur = new StringBuilder();
        boolean inQuote = false;
        for (int i = 0; i < sql.length(); i++) {
            char c = sql.charAt(i);
            if (c == '\'') {
                // SQL 中的 '' 表示转义后的单引号，不改变引用状态
                if (inQuote && i + 1 < sql.length() && sql.charAt(i + 1) == '\'') {
                    cur.append("''");
                    i++;
                    continue;
                }
                inQuote = !inQuote;
                cur.append(c);
                continue;
            }
            if (c == ';' && !inQuote) {
                out.add(cur.toString());
                cur.setLength(0);
                continue;
            }
            cur.append(c);
        }
        if (cur.length() > 0) out.add(cur.toString());
        return out;
    }

    /** 提交结果：真实 Job ID / 集群是否已接受语句 / 失败原因，三者互斥用于区分「成功」「待认领」「失败」。 */
    private static final class SubmitResult {
        final String jobId;
        final boolean accepted;
        final String error;

        private SubmitResult(String jobId, boolean accepted, String error) {
            this.jobId = jobId;
            this.accepted = accepted;
            this.error = error;
        }

        static SubmitResult ok(String jobId) { return new SubmitResult(jobId, false, null); }
        static SubmitResult acceptedNoId() { return new SubmitResult(null, true, null); }
        static SubmitResult failed(String error) { return new SubmitResult(null, false, error); }
    }

    public String submitToFlink(String flinkSql, int parallelism) {
        if (checkFlinkCluster()) {
            SubmitResult result;
            try {
                log.info("Flink cluster available, submitting SQL via Gateway...");
                result = submitViaSqlClient(flinkSql, parallelism);
            } catch (Exception e) {
                // 集群在线时提交失败要显式失败：否则作业会被标成 SUBMITTED，前端显示“提交成功”
                log.warn("Flink submission failed: {}", e.getMessage());
                throw new RuntimeException("Flink 提交失败：" + e.getMessage(), e);
            }
            if (result.jobId != null) return result.jobId;
            if (result.accepted) {
                // 语句已被集群接受（作业很可能正在运行），只是本次没取到 Job ID：
                // 保留占位 ID，交给 FlinkJobStatusChecker 按提交时间窗口认领，避免误判 FAILED
                String placeholder = "flink-job-" + UUID.randomUUID().toString();
                log.warn("Flink 已接受 SQL 但未取到 Job ID，使用占位 ID 待状态轮询认领: {}", placeholder);
                return placeholder;
            }
            throw new RuntimeException("Flink 提交失败：" + (result.error == null ? "未产生 Flink 作业" : result.error));
        }
        // 离线演示模式：集群不可达时用占位 ID，前端仍可演示编辑/保存，上线会被 isFlinkClusterAvailable 拦截
        String mockJobId = "flink-job-" + UUID.randomUUID().toString();
        log.warn("Flink cluster not available at {}:{}, using offline placeholder ID: {}", flinkHost, flinkPort, mockJobId);
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


        private SubmitResult submitViaSqlClient(String flinkSql, int parallelism) throws Exception {
        Path tempSqlFile = Files.createTempFile("flink-job-", ".sql");
        Files.writeString(tempSqlFile, flinkSql, StandardCharsets.UTF_8);
        log.info("SQL written to: {}", tempSqlFile.toAbsolutePath());
        // Capture job list before submission for downstream job detection
        var initialHc = java.net.http.HttpClient.newHttpClient();
        java.util.Set<String> initialBeforeIds = getCurrentJobIds(initialHc);
        // 提交时刻下界：只认领此时间之后启动的作业，避免误认领归档后重现的旧作业
        this.baselineTime = System.currentTimeMillis();

        SubmitResult gatewayResult = trySqlGateway(flinkSql, parallelism);
        if (gatewayResult.jobId != null || gatewayResult.accepted) {
            if (gatewayResult.jobId != null) log.info("Job via SQL Gateway: {}", gatewayResult.jobId);
            Files.deleteIfExists(tempSqlFile);
            return gatewayResult;
        }

        log.info("SQL Gateway down, trying sql-client script...");
        String jobId = trySqlClientScript(tempSqlFile);
        if (jobId != null) { log.info("Job via sql-client.sh: {}", jobId); Files.deleteIfExists(tempSqlFile); return SubmitResult.ok(jobId); }

        log.info("Polling for new Flink jobs (using pre-submission baseline)...");
        jobId = submitViaFlinkRestApi(flinkSql, initialBeforeIds);
        Files.deleteIfExists(tempSqlFile);
        if (jobId != null) return SubmitResult.ok(jobId);
        log.warn("No new Flink job detected after submission");
        return SubmitResult.failed(gatewayResult.error != null
                ? gatewayResult.error
                : "三条提交路径（SQL Gateway / sql-client / REST 轮询）均未产生 Flink 作业");
    }

    private SubmitResult trySqlGateway(String flinkSql, int parallelism) {
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
                return SubmitResult.failed("SQL Gateway 会话创建失败: " + sr);
            }
            String sh = sj.get("sessionHandle").asText();
            log.info("Gateway session created: " + sh);

            java.util.List<String> stmts = splitSqlStatements(flinkSql);
            var beforeIds = getCurrentJobIds(hc);
            String jid = null;
            // 是否已有 INSERT 语句被集群接受（用于区分「待认领」与「彻底失败」）
            boolean insertAccepted = false;

            for (int i = 0; i < stmts.size(); i++) {
                String stmtRaw = stmts.get(i).replaceAll("(?m)^--.*\n?", "").trim();
                if (stmtRaw.isEmpty()) continue;

                log.info("Gateway stmt " + (i+1) + "/" + stmts.size() + ": " + stmtRaw.substring(0, Math.min(80, stmtRaw.length())));
                String body = objectMapper.createObjectNode().put("statement", stmtRaw + ";").toString();
                String resp = hc.send(java.net.http.HttpRequest.newBuilder()
                    .uri(java.net.URI.create(gatewayUrl + "/v1/sessions/" + sh + "/statements"))
                    .header("Content-Type", "application/json")
                    .POST(java.net.http.HttpRequest.BodyPublishers.ofString(body)).build(),
                    java.net.http.HttpResponse.BodyHandlers.ofString()).body();
                JsonNode rj = objectMapper.readTree(resp);
                if (rj.has("errors")) {
                    // 生成 DDL/DML 报错必须立即失败：继续往下跑只会让 INSERT 因缺表而失败，
                    // 却把作业标成“已提交”，错误被推迟到状态轮询才暴露
                    String err = "Flink SQL 第 " + (i+1) + " 条语句执行失败: " + rj.get("errors");
                    log.warn(err);
                    throw new RuntimeException(err);
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
                                insertAccepted = true;
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

                        // 失败状态必须立即暴露：否则会一直轮询到耗尽，最终报出误导性的
                        // “未接受任何 INSERT 语句”，把真实错误（如表不存在、SQL 非法）吞掉
                        if ("ERROR".equals(os) || "FAILED".equals(os) || "CANCELED".equals(os)) {
                            String detail = oj.has("errors") ? oj.get("errors").toString() : op;
                            String err = "Flink SQL 第 " + (i+1) + " 条语句执行失败(" + os + "): "
                                    + detail.substring(0, Math.min(300, detail.length()));
                            log.warn(err);
                            throw new RuntimeException(err);
                        }
                        // INSERT 已进入 RUNNING 即视为集群已接受，作业 ID 由后续轮询认领
                        if (isInsert && "RUNNING".equals(os)) {
                            insertAccepted = true;
                            if (jid == null) for (int j = 0; j < 20; j++) {
                                String n = findNewFlinkJob(hc, beforeIds);
                                if (n != null) { jid = n; break; }
                                Thread.sleep(500);
                            }
                            break;
                        }

                        boolean _isDone = "COMPLETED".equals(os) || "FINISHED".equals(os) || "SUCCESS".equals(os);
                        if (!_isDone && os == null && !op.isEmpty() && op.startsWith("{") && !oj.has("errors")) {
                            _isDone = true;
                            log.debug("Gateway stmt " + (i+1) + " detected completion (empty status, no errors)");
                        }
                        if (_isDone) {
                            log.info("Gateway stmt " + (i+1) + " completed (status=" + os + ")");
                            if (isInsert) {
                                insertAccepted = true;
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
                    // 轮询耗尽仍无终态：INSERT 未报错说明集群可能仍在启动作业，
                    // 按「已接受待认领」处理，交给状态轮询器取回真实 Job ID；
                    // 直接判失败会把慢启动作业误标为 FAILED。
                    if (isInsert && !insertAccepted) {
                        insertAccepted = true;
                        log.info("Gateway stmt " + (i+1) + " 轮询超时未达终态，按已接受处理待状态轮询认领");
                    }
                }
            }
            try { hc.send(java.net.http.HttpRequest.newBuilder()
                .uri(java.net.URI.create(gatewayUrl + "/v1/sessions/" + sh))
                .DELETE().build(), java.net.http.HttpResponse.BodyHandlers.discarding()); } catch (Exception ign) {}
            if (jid != null) return SubmitResult.ok(jid);
            if (insertAccepted) return SubmitResult.acceptedNoId();
            return SubmitResult.failed("SQL Gateway 未接受任何 INSERT 语句（DDL 可能未生成 INSERT）");
        } catch (Exception e) {
            log.warn("Gateway failed: " + e.getMessage());
            return SubmitResult.failed("SQL Gateway 调用失败: " + e.getMessage());
        }
    }
    private String trySqlClientScript(Path sqlFile) {
        try {
            // Flink 官方发行版在 Windows 上不含 sql-client.sh（只有 .bat）。
            // 先探测脚本是否存在，避免对必然失败的路径做 30s 空等与误导性的“尝试过”日志。
            String scriptName;
            String bash = null;
            String sh = "sql-client.sh";
            if (new java.io.File(flinkHome.replace("\\", "/") + "/bin/" + sh).exists()) {
                for (String bp : new String[]{"C:/Program Files/Git/bin/bash.exe", "C:/Program Files (x86)/Git/bin/bash.exe"}) {
                    if (new java.io.File(bp).exists()) { bash = bp; break; }
                }
                if (bash == null) { log.warn("sql-client.sh 存在但未找到 bash.exe，跳过该降级路径"); return null; }
                scriptName = sh;
            } else if (new java.io.File(flinkHome.replace("\\", "/") + "/bin/sql-client.bat").exists()) {
                String comspec = System.getenv("COMSPEC");
                bash = (comspec == null || comspec.isBlank()) ? "cmd.exe" : comspec;
                scriptName = "sql-client.bat";
            } else {
                log.warn("{} 下未找到 sql-client 脚本，跳过该降级路径（Windows 发行版通常只有 sql-client.bat）", flinkHome);
                return null;
            }
            String b = bash;

            String sc = flinkHome.replace("\\", "/") + "/bin/" + scriptName;
            String sf = sqlFile.toAbsolutePath().toString().replace("\\", "/");
            log.info("Running: " + b + " " + sc + " -f " + sf);
            // .bat 不能直接作为 Executable 启动，必须经 cmd /c 调用
            ProcessBuilder pb = "sql-client.bat".equals(scriptName)
                    ? new ProcessBuilder(b, "/c", sc, "-f", sf)
                    : new ProcessBuilder(b, sc, "-f", sf);
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
                log.warn("{} timed out after 30s", scriptName);
                return null;
            }
            reader.join(5000);

            String all = output.toString();
            log.info("{} output: {} chars", scriptName, all.length());

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
        return findNewFlinkJob(httpClient, beforeIds, baselineTime);
    }

    /**
     * 认领本次提交产生的新 Flink 作业。
     * 除「jid 不在提交前基线中」外，还必须满足 start-time 不早于提交时刻：
     * 历史作业归档后重新出现在 overview 时会带旧 jid，仅靠差集会误认领，导致作业状态被旧 jid 的错误状态覆盖。
     */
    private String findNewFlinkJob(java.net.http.HttpClient httpClient, java.util.Set<String> beforeIds, long notBefore) {
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
                    if (beforeIds.contains(jid)) continue;
                    long startTime = j.has("start-time") ? j.get("start-time").asLong() : 0;
                    // 容差 3s：作业可能在基线采集前的一瞬间已启动
                    if (notBefore > 0 && startTime < notBefore - 3000) {
                        log.debug("Skip stale Flink job {} (startTime={} < notBefore={})", jid, startTime, notBefore);
                        continue;
                    }
                    if (startTime > bestTime) {
                        bestTime = startTime;
                        bestJid = jid;
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

    /**
     * 输入节点的本地文件路径映射：历史作业里保存的宿主绝对路径在容器内映射到 /data。
     * 非本地路径（hdfs:// 等）与不存在的映射保持原值。
     */
    private Map<String, Object> mapLocalInputPath(Map<String, Object> params) {
        if (params == null) return null;
        Object raw = params.get("path");
        if (raw == null) return params;
        String path = raw.toString().trim();
        String mapped = resolveRuntimePath(path, "/data");
        if (mapped.equals(path)) return params;
        Map<String, Object> copy = new HashMap<>(params);
        copy.put("path", mapped);
        return copy;
    }

    /**
     * Docker 中兼容历史作业保存的 Windows 绝对路径。
     * 输入文件映射到 /data，输出文件映射到 /output；本机存在的路径保持不变。
     */
    private String resolveRuntimePath(String path, String mountedDir) {
        java.io.File original = new java.io.File(path);
        if (original.exists() || java.io.File.separatorChar == '\\') {
            return path;
        }
        if (!path.matches("^[A-Za-z]:[\\\\/].*")) {
            return path;
        }
        String fileName = path.replace('\\', '/');
        fileName = fileName.substring(fileName.lastIndexOf('/') + 1);
        String mapped = mountedDir + "/" + fileName;
        if ("/output".equals(mountedDir) || new java.io.File(mapped).exists()) {
            log.info("Mapped host path for container runtime: {} -> {}", path, mapped);
            return mapped;
        }
        return path;
    }

    /**
     * 生成 HDFS 输入 DDL：filesystem connector + CSV format。
     * fieldsConfig 指定 schema；hasHeader=true 时按字符串全列读取，由下游过滤表头行
     * （Flink CSV connector 无 skip-header 选项，只能读入后过滤）。
     */
    private String generateHdfsInputDDL(String tableName, String path, String fieldsConfig, String delimiter,
                                        boolean skipHeader) {
        JsonNode fa = tryParseJsonArray(fieldsConfig);
        StringBuilder ddl = new StringBuilder();
        ddl.append("CREATE TABLE ").append(tableName).append(" (\n");
        if (fa != null && fa.isArray() && fa.size() > 0) {
            for (int i = 0; i < fa.size(); i++) {
                JsonNode f = fa.get(i);
                String fn = f.has("name") ? f.get("name").asText() : "field" + i;
                String ft = f.has("type") ? f.get("type").asText() : "STRING";
                // 需要跳过表头时全列按 STRING 读，避免表头文本触发类型解析失败被整行丢弃
                if (skipHeader) ft = "STRING";
                ddl.append("  ").append(quoteFlinkField(fn)).append(" ").append(ft);
                if (i < fa.size() - 1) ddl.append(",");
                ddl.append("\n");
            }
        } else {
            throw new RuntimeException("HDFS 输入需要 fieldsConfig 声明字段（HDFS 文件无法自动推导表头），如 [{\"name\":\"id\",\"type\":\"INT\"}]");
        }
        ddl.append(") WITH (\n");
        ddl.append("  'connector' = 'filesystem',\n");
        ddl.append("  'path' = '").append(path.replace("'", "''")).append("',\n");
        ddl.append("  'format' = 'csv',\n");
        ddl.append("  'csv.delimiter' = '").append(delimiter.replace("'", "''")).append("',\n");
        ddl.append("  'csv.ignore-parse-errors' = 'true',\n");
        ddl.append("  'csv.allow-comments' = 'false'\n");
        ddl.append(");\n");
        return ddl.toString();
    }

    /** HDFS 输入的表头过滤条件：首列等于该列字段名即判定为表头行。 */
    private String hdfsHeaderFilter(String fieldsConfig) {
        JsonNode fa = tryParseJsonArray(fieldsConfig);
        if (fa == null || !fa.isArray() || fa.size() == 0) return null;
        JsonNode first = fa.get(0);
        String firstField = first.has("name") ? first.get("name").asText().trim() : "";
        if (firstField.isEmpty()) return null;
        return quoteFlinkField(firstField) + " <> '" + firstField.replace("'", "''") + "'";
    }

    /** 按 fieldsConfig 的字段名生成全 STRING 的 schema（跳表头时使用，保证与读取类型一致）。 */
    private String forceStringSchema(String fieldsConfig) {
        JsonNode fa = tryParseJsonArray(fieldsConfig);
        if (fa == null || !fa.isArray() || fa.size() == 0) return null;
        StringBuilder sb = new StringBuilder();
        for (JsonNode f : fa) {
            String fn = f.has("name") ? f.get("name").asText().trim() : "";
            if (fn.isEmpty()) continue;
            if (sb.length() > 0) sb.append(",\n");
            sb.append("  ").append(quoteFlinkField(fn)).append(" STRING");
        }
        return sb.length() == 0 ? null : sb.toString();
    }

    /** 生成 HDFS 输出 DDL：filesystem connector，非分区流式写（source 下游为有界时写完即 FINISHED） */
    private String generateHdfsOutputDDL(String tableName, String path, String delimiter, String fields) {
        StringBuilder ddl = new StringBuilder();
        ddl.append("CREATE TABLE ").append(tableName).append(" (\n");
        ddl.append(fields).append("\n");
        ddl.append(") WITH (\n");
        ddl.append("  'connector' = 'filesystem',\n");
        ddl.append("  'path' = '").append(path.replace("'", "''")).append("',\n");
        ddl.append("  'format' = 'csv',\n");
        ddl.append("  'csv.delimiter' = '").append(delimiter.replace("'", "''")).append("',\n");
        ddl.append("  'sink.rolling-policy.rollover-interval' = '15 min',\n");
        ddl.append("  'sink.rolling-policy.check-interval' = '1 min',\n");
        ddl.append("  'sink.parallelism' = '1'\n");
        ddl.append(");\n");
        return ddl.toString();
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

    /**
     * 构建某个 transform 节点的 SELECT 片段。
     * 抽成方法是因为 transform→transform 链上的中间节点要物化为 VIEW，
     * 与「末尾 transform 内联进下游 INSERT」两处都需要同一段逻辑。
     */
    private String buildTransformSql(DagDefinition.DagEdge edge,
                                     List<DagDefinition.DagEdge> edges,
                                     Map<String, String> nodeTypeMap,
                                     Map<String, Map<String, Object>> transformNodeParams,
                                     Map<String, String> hdfsHeaderFilters,
                                     Map<String, String> nodeSchemas) {
        String transformNodeId = edge.getSource();
        String transformType = nodeTypeMap.get(transformNodeId);
        String upstreamTable = null;
        for (DagDefinition.DagEdge ie : edges) {
            if (ie.getTarget().equals(transformNodeId)) {
                upstreamTable = sanitize(ie.getSource());
                break;
            }
        }
        if (upstreamTable == null) upstreamTable = sanitize(transformNodeId);
        // HDFS 输入含表头时，转换节点的上游同样要过滤
        String upstreamHeaderFilter = hdfsHeaderFilters.get(transformNodeId) != null
                ? hdfsHeaderFilters.get(transformNodeId) : hdfsHeaderFilters.get(upstreamTable);
        String upstreamSource = upstreamHeaderFilter != null
                ? "(SELECT * FROM " + upstreamTable + " WHERE " + upstreamHeaderFilter + ")"
                : upstreamTable;
        Map<String, Object> tp = transformNodeParams.get(transformNodeId);
        String ts;
        if ("field_filter".equals(transformType)) {
            ts = "SELECT " + buildFieldFilterSelect(tp) + " FROM " + upstreamSource;
        } else if ("field_rename".equals(transformType)) {
            ts = "SELECT " + buildFieldRenameSelect(tp, findIncomingSourceSchema(transformNodeId, edges, nodeSchemas)) + " FROM " + upstreamSource;
        } else if ("json_parse".equals(transformType)) {
            ts = "SELECT " + buildJsonParseSelect(tp, findIncomingSourceSchema(transformNodeId, edges, nodeSchemas)) + " FROM " + upstreamSource;
        } else if ("dedupe".equals(transformType)) {
            List<String> dedupeFields = parseCsvFields(tp != null ? tp.get("dedupeFields") : null);
            if (dedupeFields.isEmpty()) {
                ts = "SELECT DISTINCT * FROM " + upstreamTable;
            } else {
                String incomingSchema = findIncomingSourceSchema(transformNodeId, edges, nodeSchemas);
                List<String> allCols = parseSchemaFieldNames(incomingSchema);
                if (allCols.isEmpty()) {
                    ts = "SELECT DISTINCT " + quoteFieldsList(dedupeFields) + " FROM " + upstreamSource;
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
            ts = buildRouteSql(edge, edges, tp, upstreamTable);
        } else if ("redis_lookup".equals(transformType)) {
            ts = buildRedisLookupSelect(tp, upstreamTable);
        } else if ("field_concat".equals(transformType)) {
            ts = "SELECT *, " + buildFieldConcatSelect(tp) + " FROM " + upstreamSource;
        } else {
            ControlRegistry sc = controlService.findByType(transformType);
            ts = renderTemplate(sc.getFlinkTemplate(), tp, transformNodeId);
            ts = ts.replace(sanitize(transformNodeId), upstreamTable);
        }
        if (ts == null || ts.isBlank()) {
            throw new IllegalArgumentException("转换节点未生成可执行 SQL: " + transformNodeId);
        }
        return ts;
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
            int ps = template.indexOf('(');
            if (ps < 0) return null;
            // 不能直接取第一个 ')'：DECIMAL(10,2) 这类带括号的类型会被截断成非法 schema
            int depth = 0;
            for (int i = ps; i < template.length(); i++) {
                char c = template.charAt(i);
                if (c == '(') depth++;
                else if (c == ')') {
                    depth--;
                    if (depth == 0) {
                        String cols = template.substring(ps + 1, i).trim();
                        return cols.isEmpty() ? null : cols;
                    }
                }
            }
        } catch (Exception e) { log.warn("Failed to extract fields from DDL template: {}", e.getMessage()); }
        return null;
    }

        private String replaceDataStringInDDL(String template, String newFields) {
        // Replace "data STRING" with actual source schema
        // quoteReplacement：字段名含 $ 或 \ 时不会触发非法分组引用
        String replacement = java.util.regex.Matcher.quoteReplacement(newFields);
        String result = template.replaceAll("(?m)^[ \\t]*data\\s+STRING[ \\t]*(,?)[ \\t]*$", replacement + "$1");
        if (result.equals(template)) {
            result = template.replaceAll("data\\s+STRING", replacement);
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
                // 参数会落进 CREATE TABLE 的字符串字面量（path/url/topic/password 等），
                // 必须转义单引号，否则密码或路径含 ' 会闭合字面量导致语法错误/注入
                String value = entry.getValue() != null ? escapeSqlLiteral(entry.getValue().toString()) : "";
                result = result.replace("${" + entry.getKey() + "}", value);
                // ${params.x} 表示该占位符必须由节点参数提供（无 schema 默认值兜底），
                // 参数缺失时整体替换为空串，避免把占位符原样拼进 Flink SQL
                result = result.replace("${params." + entry.getKey() + "}", entry.getValue() != null ? value : "");
            }
        }
        return result;
    }

    /**
     * 用控件 paramSchema 的默认值补齐缺失参数。
     * 节点 params 只保存用户显式填写的值，带默认值的可选项（如 csv_output 的 delimiter）
     * 不会落库；若模板里引用了这些占位符，不补齐就会把 ${delimiter} 原样拼进 Flink SQL 导致建表失败。
     */
    private Map<String, Object> withSchemaDefaults(Map<String, Object> params, String paramSchema) {
        Map<String, Object> merged = new HashMap<>();
        if (params != null) merged.putAll(params);
        if (paramSchema == null || paramSchema.isBlank()) return merged;
        try {
            JsonNode properties = objectMapper.readTree(paramSchema).path("properties");
            properties.fields().forEachRemaining(entry -> {
                if (merged.containsKey(entry.getKey())) return;
                JsonNode def = entry.getValue().path("default");
                if (def.isMissingNode() || def.isNull()) return;
                merged.put(entry.getKey(), def.isTextual() ? def.asText() : def);
            });
        } catch (Exception e) {
            log.warn("控件 paramSchema 默认值解析失败，跳过补齐: {}", e.getMessage());
        }
        return merged;
    }

    /**
     * 用户名/密码解析：空值或 ${MYSQL_USERNAME} / ${MYSQL_PASSWORD} 占位符回退到环境变量默认值
     */
    private String resolveCredential(Object value, String envDefault) {
        if (value == null || value.toString().trim().isEmpty()) return envDefault == null ? "" : envDefault;
        String v = value.toString().trim();
        if (v.startsWith("${") && v.endsWith("}")) {
            return envDefault == null ? "" : envDefault;
        }
        return v;
    }

    private String sanitize(String id) { return id.replaceAll("[^a-zA-Z0-9_]", "_"); }

    private String escapeSqlLiteral(String value) {
        return value == null ? "" : value.replace("'", "''");
    }

    private String redactSqlSecrets(String sql) {
        return sql.replaceAll("(?im)^([ \\t]*'password'[ \\t]*=[ \\t]*').*(',?[ \\t]*)$", "$1******$2");
    }

    private int parseBoundedInt(Object value, int fallback, int min, int max, String label) {
        if (value == null || value.toString().isBlank()) return fallback;
        try {
            int parsed = Integer.parseInt(value.toString().trim());
            if (parsed < min || parsed > max) throw new NumberFormatException();
            return parsed;
        } catch (NumberFormatException e) {
            throw new RuntimeException(label + " 必须是 " + min + "-" + max + " 的整数");
        }
    }

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

    /**
     * 字段拼接：CONCAT(`a`, `b`) AS `新字段`。
     * fields 在 DAG 里可能是 JSON 数组或逗号分隔字符串，必须解析成字段列表再逐个加引号，
     * 直接把 List.toString() 拼进模板会生成 CONCAT([a, b]) 这种方括号非法语法。
     */
    private String buildFieldConcatSelect(Map<String, Object> params) {
        List<String> fields = parseCsvFields(params != null ? params.get("fields") : null);
        if (fields.isEmpty()) {
            throw new RuntimeException("字段拼接: 请填写要拼接的字段（逗号分隔或 JSON 数组）");
        }
        String newField = params != null && params.get("newFieldName") != null
                ? params.get("newFieldName").toString().trim() : "";
        if (newField.isEmpty()) throw new RuntimeException("字段拼接: 请填写新字段名");
        String separator = params.get("separator") == null ? "," : params.get("separator").toString();
        List<String> parts = new ArrayList<>();
        for (int i = 0; i < fields.size(); i++) {
            if (i > 0 && !separator.isEmpty()) parts.add("'" + escapeSqlLiteral(separator) + "'");
            // CONCAT 要求参数为字符串，非字符串列需显式 CAST，否则 Flink 会因类型推断失败
            parts.add("CAST(" + quoteFlinkField(fields.get(i)) + " AS STRING)");
        }
        return "CONCAT(" + String.join(", ", parts) + ") AS " + quoteFlinkField(newField);
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

    /**
     * Redis 富化 SELECT：保留上游全部字段 + redis_lookup(字段A) 扩充 targetField。
     * UDF 内部按 keyPrefix 拼 key：GET keyPrefix + 字段A值，取不到时返回 NULL。
     */
    private String buildRedisLookupSelect(Map<String, Object> params, String upstreamTable) {
        String keyField = params != null && params.get("keyField") != null ? params.get("keyField").toString().trim() : "";
        String targetField = params != null && params.get("targetField") != null ? params.get("targetField").toString().trim() : "extra_info";
        String keyPrefix = params != null && params.get("keyPrefix") != null ? params.get("keyPrefix").toString() : "";
        String host = params != null && params.get("host") != null ? params.get("host").toString().trim() : "localhost";
        String port = params != null && params.get("port") != null ? params.get("port").toString().trim() : "6379";
        String password = params != null && params.get("password") != null ? params.get("password").toString() : "";
        if (keyField.isEmpty()) {
            throw new RuntimeException("Redis 富化: 请填写 keyField（用作 Redis key 的字段名）");
        }
        if (targetField.isEmpty()) {
            throw new RuntimeException("Redis 富化: 请填写 targetField（扩充字段名）");
        }
        if (host.isEmpty()) {
            throw new RuntimeException("Redis 富化: 请填写 Redis 地址");
        }
        int portNumber;
        try {
            portNumber = Integer.parseInt(port);
        } catch (NumberFormatException e) {
            throw new RuntimeException("Redis 富化: 端口必须是 1-65535 的整数");
        }
        if (portNumber < 1 || portNumber > 65535) {
            throw new RuntimeException("Redis 富化: 端口必须是 1-65535 的整数");
        }
        String keyExpression = keyPrefix.isEmpty()
                ? "CAST(" + quoteFlinkField(keyField) + " AS STRING)"
                : "CONCAT('" + escapeSqlLiteral(keyPrefix) + "', CAST(" + quoteFlinkField(keyField) + " AS STRING))";
        return "SELECT *, redis_lookup('" + escapeSqlLiteral(host) + "', '" + portNumber + "', '"
                + escapeSqlLiteral(password) + "', " + keyExpression + ") AS " + quoteFlinkField(targetField)
                + " FROM " + upstreamTable;
    }

    private String buildJsonParseSelect(Map<String, Object> params, String incomingSchema) {        String srcField = params != null && params.get("sourceField") != null ? params.get("sourceField").toString().trim() : "";
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

    /** 通用 JDBC 输入 DDL（PostgreSQL / Oracle 等方言，driver 由调用方传入） */
    private String generateJdbcInputDDL(String tableName, String url, String table, String username, String password, String fields, String driver) {
        StringBuilder ddl = new StringBuilder();
        ddl.append("CREATE TABLE ").append(tableName).append(" (\n");
        ddl.append(fields).append("\n");
        ddl.append(") WITH (\n");
        ddl.append("  'connector' = 'jdbc',\n");
        ddl.append("  'url' = '").append(url.replace("'", "''")).append("',\n");
        ddl.append("  'table-name' = '").append(table.replace("'", "''")).append("',\n");
        if (username != null && !username.isEmpty()) {
            ddl.append("  'username' = '").append(username.replace("'", "''")).append("',\n");
        }
        ddl.append("  'password' = '").append(password == null ? "" : password.replace("'", "''")).append("',\n");
        ddl.append("  'driver' = '").append(driver).append("',\n");
        ddl.append("  'scan.fetch-size' = '1000'\n");
        ddl.append(");\n");
        return ddl.toString();
    }

    private String generateJdbcOutputDDL(String tableName, String url, String targetTable,
                                         String username, String password, String fields,
                                         String driver, int batchSize) {
        return "CREATE TABLE " + tableName + " (\n" + fields + "\n) WITH (\n"
                + "  'connector' = 'jdbc',\n"
                + "  'url' = '" + escapeSqlLiteral(url) + "',\n"
                + "  'table-name' = '" + escapeSqlLiteral(targetTable) + "',\n"
                + "  'username' = '" + escapeSqlLiteral(username) + "',\n"
                + "  'password' = '" + escapeSqlLiteral(password) + "',\n"
                + "  'driver' = '" + driver + "',\n"
                + "  'sink.buffer-flush.max-rows' = '" + batchSize + "',\n"
                + "  'sink.buffer-flush.interval' = '1s'\n"
                + ");\n";
    }

    /** 通用 JDBC 字段推导（复用 MySQL 推导逻辑 + 类型映射，限 SELECT * LIMIT 0 元数据） */
    private String inferJdbcFields(String url, String table, String username, String password, String label) {
        String quotedTable = label.equals("Oracle") ? table : "\"" + table.replace("\"", "\"\"") + "\"";
        try (Connection conn = DriverManager.getConnection(url, username, password);
             Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT * FROM " + quotedTable + " WHERE 1=0")) {
            ResultSetMetaData md = rs.getMetaData();
            StringBuilder sb = new StringBuilder();
            for (int i = 1; i <= md.getColumnCount(); i++) {
                String name = md.getColumnLabel(i);
                String flinkType = mysqlToFlinkType(md.getColumnType(i), md.getPrecision(i), md.getScale(i));
                if (sb.length() > 0) sb.append(",\n");
                sb.append("  ").append(quoteFlinkField(name)).append(" ").append(flinkType);
            }
            if (sb.length() == 0) throw new RuntimeException(label + " 输入: 表无字段: " + table);
            return sb.toString();
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(label + " 输入读取表结构失败: " + e.getMessage() + "（请检查 url/table/username/password 与网络）", e);
        }
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
            case java.sql.Types.NUMERIC:
                // Oracle 无精度 NUMBER / PG 无约束 numeric 的 precision 常为 0，
                // 直接用 max(precision,1) 会生成 DECIMAL(1,0) 导致数值被截断
                if (precision <= 0) return "DECIMAL(38,18)";
                return "DECIMAL(" + precision + "," + Math.max(scale, 0) + ")";
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
        ddl.append("  'path' = '").append(escapeSqlLiteral(path)).append("',\n");
        ddl.append("  'format' = 'csv',\n");
        ddl.append("  'csv.delimiter' = '").append(escapeSqlLiteral(delimiter)).append("',\n");
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
        ddl.append("  'rows-per-second' = '").append(escapeSqlLiteral(rowsPerSecond)).append("'");
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
        ddl.append("  'topic' = '").append(escapeSqlLiteral(topic)).append("',\n");
        ddl.append("  'properties.bootstrap.servers' = '").append(escapeSqlLiteral(bootstrapServers)).append("',\n");
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
        ddl.append("  'topic' = '").append(escapeSqlLiteral(topic)).append("',\n");
        ddl.append("  'properties.bootstrap.servers' = '").append(escapeSqlLiteral(bootstrapServers)).append("',\n");
        ddl.append("  'format' = 'json'\n");
        ddl.append(");\n");

        return ddl.toString();
    }
}






