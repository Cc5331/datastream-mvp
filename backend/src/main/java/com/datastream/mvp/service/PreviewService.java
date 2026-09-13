package com.datastream.mvp.service;

import com.datastream.mvp.model.JobDefinition;
import com.datastream.mvp.util.JdbcUrlUtil;
import com.datastream.mvp.util.MysqlIdentifier;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.DataFormatter;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 通用数据预览服务：按文件类型读取前 N 行，返回结构化表格数据（列名 + 行）。
 * 支持 CSV / Excel / JSON / XML / 普通文本；用于前端画布节点“预览数据”。
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class PreviewService {

    private final ObjectMapper objectMapper;
    private final XmlPreprocessor xmlPreprocessor;

    @Value("${app.preview.allowed-roots:../data,../test-resources,../output}")
    private String allowedRoots;

    @Value("${app.preview.max-file-bytes:67108864}")
    private long maxFileBytes;

    @Value("${app.preview.allowed-jdbc-hosts:localhost,127.0.0.1,postgres,oracle,mysql}")
    private String allowedJdbcHosts;

    @Value("${app.preview.allowed-hdfs-authorities:localhost:9000,namenode:9000}")
    private String allowedHdfsAuthorities;

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

    public Map<String, Object> previewFile(String path, int limit) {
        Map<String, Object> result = new LinkedHashMap<>();
        java.io.File f = resolveAllowedFile(path).toFile();
        result.put("path", f.getAbsolutePath());
        if (!f.exists() || !f.isFile()) {
            result.put("message", "文件不存在: " + path);
            return result;
        }
        if (f.length() > maxFileBytes) {
            throw new IllegalArgumentException("文件过大，无法预览（最大 " + maxFileBytes + " 字节）");
        }
        int n = (limit <= 0) ? 20 : Math.min(limit, 100);
        String lower = f.getName().toLowerCase();
        try {
            if (lower.endsWith(".csv") || lower.endsWith(".txt")) return previewCsv(f, n);
            if (lower.endsWith(".xlsx") || lower.endsWith(".xls")) return previewExcel(f, n);
            if (lower.endsWith(".json")) return previewJson(f, n);
            if (lower.endsWith(".xml")) return previewXml(f, n);
            return previewText(f, n);
        } catch (Exception e) {
            log.warn("Preview failed for {}: {}", path, e.getMessage());
            result.put("message", "预览失败: " + e.getMessage());
        }
        return result;
    }

    /** 从已授权作业的 DAG 中读取节点配置，避免客户端伪造远程连接参数。 */
    public Map<String, Object> previewNode(JobDefinition job, String nodeId, int limit) {
        int normalizedLimit = limit <= 0 ? 20 : Math.min(limit, 100);
        try {
            JsonNode root = objectMapper.readTree(job.getDagJson());
            JsonNode target = null;
            for (JsonNode node : root.path("nodes")) {
                if (nodeId.equals(node.path("id").asText())) {
                    target = node;
                    break;
                }
            }
            if (target == null) throw new IllegalArgumentException("作业中不存在节点: " + nodeId);
            String type = target.path("type").asText();
            JsonNode params = target.path("params");
            return switch (type) {
                case "mysql_input", "mysql_output" -> previewJdbc(type, params, normalizedLimit);
                case "pg_input", "pg_output" -> previewJdbc(type, params, normalizedLimit);
                case "oracle_input", "oracle_output" -> previewJdbc(type, params, normalizedLimit);
                case "hdfs_input" -> previewHdfs(params, normalizedLimit);
                default -> throw new IllegalArgumentException("该节点类型不支持远程预览: " + type);
            };
        } catch (IllegalArgumentException e) {
            throw e;
        } catch (Exception e) {
            log.warn("Remote preview failed for job {} node {}: {}", job.getId(), nodeId, e.getMessage());
            throw new IllegalArgumentException("远程数据预览失败，请检查连接、权限和节点参数");
        }
    }

    private Map<String, Object> previewJdbc(String type, JsonNode params, int limit) throws Exception {
        String url = text(params, "url");
        String schema = text(params, "schema");
        String table = text(params, "table");
        String username = text(params, "username");
        String password = text(params, "password");
        String qualified;
        String query;

        if (type.startsWith("mysql_")) {
            url = JdbcUrlUtil.normalize(url);
            requireJdbcUrl(url, "jdbc:mysql://");
            username = resolveCredential(username, defaultMysqlUsername);
            password = resolveCredential(password, defaultMysqlPassword);
            qualified = MysqlIdentifier.quoteTable(table);
            query = "SELECT * FROM " + qualified + " LIMIT " + (limit + 1);
        } else if (type.startsWith("pg_")) {
            requireJdbcUrl(url, "jdbc:postgresql://");
            username = resolveCredential(username, defaultPostgresUsername);
            password = resolveCredential(password, defaultPostgresPassword);
            schema = schema.isBlank() ? "public" : schema;
            qualified = quoteQualified(schema, table, "PostgreSQL");
            query = "SELECT * FROM " + qualified + " LIMIT " + (limit + 1);
        } else {
            requireJdbcUrl(url, "jdbc:oracle:");
            username = resolveCredential(username, defaultOracleUsername);
            password = resolveCredential(password, defaultOraclePassword);
            schema = schema.isBlank() ? username : schema;
            qualified = quoteQualified(schema.toUpperCase(), table.toUpperCase(), "Oracle");
            query = "SELECT * FROM " + qualified + " FETCH FIRST " + (limit + 1) + " ROWS ONLY";
        }

        List<String> columns = new ArrayList<>();
        List<List<String>> rows = new ArrayList<>();
        try (Connection connection = DriverManager.getConnection(url, username, password);
             Statement statement = connection.createStatement()) {
            statement.setQueryTimeout(8);
            statement.setFetchSize(limit + 1);
            try (ResultSet resultSet = statement.executeQuery(query)) {
                ResultSetMetaData metadata = resultSet.getMetaData();
                for (int i = 1; i <= metadata.getColumnCount(); i++) columns.add(metadata.getColumnLabel(i));
                while (resultSet.next() && rows.size() <= limit) {
                    List<String> row = new ArrayList<>();
                    for (int i = 1; i <= metadata.getColumnCount(); i++) {
                        Object value = resultSet.getObject(i);
                        row.add(value == null ? "" : String.valueOf(value));
                    }
                    rows.add(row);
                }
            }
        }
        boolean truncated = rows.size() > limit;
        if (truncated) rows.remove(rows.size() - 1);
        return remoteResult(type, columns, rows, truncated);
    }

    private Map<String, Object> previewHdfs(JsonNode params, int limit) throws Exception {
        String path = text(params, "path").trim();
        String delimiter = text(params, "delimiter");
        if (delimiter.isEmpty()) delimiter = ",";
        if (delimiter.length() != 1) throw new IllegalArgumentException("HDFS delimiter 必须是单个字符");
        URI uri = URI.create(path);
        if (!"hdfs".equalsIgnoreCase(uri.getScheme())) throw new IllegalArgumentException("HDFS 路径必须使用 hdfs://");
        if (!allowedValues(allowedHdfsAuthorities).contains(uri.getAuthority())) {
            throw new IllegalArgumentException("HDFS 地址不在预览白名单中");
        }
        List<String> columns = fieldNames(text(params, "fieldsConfig"));
        if (columns.isEmpty()) throw new IllegalArgumentException("HDFS 预览缺少 fieldsConfig");
        List<List<String>> rows = new ArrayList<>();
        org.apache.hadoop.conf.Configuration configuration = new org.apache.hadoop.conf.Configuration();
        try (org.apache.hadoop.fs.FileSystem fileSystem = org.apache.hadoop.fs.FileSystem.get(uri, configuration);
             Reader reader = new InputStreamReader(fileSystem.open(new org.apache.hadoop.fs.Path(uri)), StandardCharsets.UTF_8);
             CSVParser parser = CSVFormat.DEFAULT.builder().setDelimiter(delimiter.charAt(0)).build().parse(reader)) {
            for (CSVRecord record : parser) {
                if (rows.size() > limit) break;
                List<String> row = new ArrayList<>();
                for (int i = 0; i < columns.size(); i++) row.add(i < record.size() ? record.get(i) : "");
                rows.add(row);
            }
        }
        boolean truncated = rows.size() > limit;
        if (truncated) rows.remove(rows.size() - 1);
        return remoteResult("hdfs_input", columns, rows, truncated);
    }

    private Map<String, Object> remoteResult(String source, List<String> columns, List<List<String>> rows, boolean truncated) {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("source", source);
        result.put("columns", columns);
        result.put("rows", rows);
        result.put("truncated", truncated);
        return result;
    }

    private void requireJdbcUrl(String url, String prefix) {
        if (url == null || !url.startsWith(prefix)) throw new IllegalArgumentException("JDBC URL 类型与节点不匹配");
        String host;
        try {
            if (prefix.equals("jdbc:oracle:")) {
                Matcher matcher = Pattern.compile("@(?:\\/\\/)?([^:/]+)").matcher(url);
                if (!matcher.find()) throw new IllegalArgumentException("Oracle JDBC URL 无法解析主机");
                host = matcher.group(1);
            } else {
                host = URI.create(url.substring(5)).getHost();
            }
        } catch (Exception e) {
            throw new IllegalArgumentException("JDBC URL 无法解析");
        }
        if (host == null || !allowedValues(allowedJdbcHosts).contains(host)) {
            throw new IllegalArgumentException("JDBC 主机不在预览白名单中");
        }
    }

    private String quoteQualified(String schema, String table, String label) {
        Pattern identifier = Pattern.compile("[A-Za-z_][A-Za-z0-9_$]*");
        if (!identifier.matcher(schema).matches() || !identifier.matcher(table).matches()) {
            throw new IllegalArgumentException(label + " Schema 或表名不合法");
        }
        return "\"" + schema + "\".\"" + table + "\"";
    }

    private List<String> fieldNames(String fieldsConfig) throws Exception {
        List<String> names = new ArrayList<>();
        JsonNode fields = objectMapper.readTree(fieldsConfig == null || fieldsConfig.isBlank() ? "[]" : fieldsConfig);
        if (!fields.isArray()) return names;
        for (JsonNode field : fields) {
            String name = field.path("name").asText().trim();
            if (!name.isEmpty()) names.add(name);
        }
        return names;
    }

    private String resolveCredential(String value, String fallback) {
        String trimmed = value == null ? "" : value.trim();
        return trimmed.isEmpty() || (trimmed.startsWith("${") && trimmed.endsWith("}"))
                ? (fallback == null ? "" : fallback) : trimmed;
    }

    private Set<String> allowedValues(String csv) {
        Set<String> result = new LinkedHashSet<>();
        if (csv != null) for (String value : csv.split(",")) result.add(value.trim());
        return result;
    }

    private String text(JsonNode node, String field) {
        return node != null && node.hasNonNull(field) ? node.get(field).asText() : "";
    }

    private Map<String, Object> previewCsv(java.io.File f, int n) {
        Map<String, Object> result = baseResult(f);
        List<String> columns = new ArrayList<>();
        List<List<String>> rows = new ArrayList<>();
        try (Reader reader = Files.newBufferedReader(f.toPath(), StandardCharsets.UTF_8);
             CSVParser parser = CSVFormat.DEFAULT.parse(reader)) {
            Iterator<CSVRecord> it = parser.iterator();
            boolean first = true;
            int rowCount = 0;
            while (it.hasNext() && rowCount < n) {
                CSVRecord rec = it.next();
                if (rec.size() == 0 && rec.get(0).isEmpty()) continue;
                if (first) {
                    for (int i = 0; i < rec.size(); i++) columns.add(rec.get(i).isEmpty() ? "col" + (i + 1) : rec.get(i));
                    first = false;
                    continue;
                }
                List<String> row = new ArrayList<>();
                for (int i = 0; i < rec.size(); i++) row.add(rec.get(i));
                rows.add(row);
                rowCount++;
            }
            if (first) { // 空文件或只有空行
                columns.add("col1");
            }
            if (it.hasNext()) result.put("truncated", true);
        } catch (Exception e) {
            log.warn("CSV preview fallback to text: {}", e.getMessage());
            return previewText(f, n);
        }
        result.put("columns", columns);
        result.put("rows", rows);
        return result;
    }

    private Map<String, Object> previewExcel(java.io.File f, int n) {
        Map<String, Object> result = baseResult(f);
        List<String> columns = new ArrayList<>();
        List<List<String>> rows = new ArrayList<>();
        DataFormatter fmt = new DataFormatter();
        try (Workbook wb = WorkbookFactory.create(f)) {
            Sheet sheet = wb.getSheetAt(0);
            if (sheet == null) {
                result.put("message", "Excel 文件没有工作表");
                return result;
            }
            int first = sheet.getFirstRowNum();
            int last = Math.min(first + n + 1, sheet.getLastRowNum() + 1);
            boolean header = true;
            for (int r = first; r < last; r++) {
                Row row = sheet.getRow(r);
                int cols = (row == null) ? 0 : row.getLastCellNum();
                List<String> vals = new ArrayList<>();
                for (int c = 0; c < cols; c++) {
                    Cell cell = row.getCell(c);
                    vals.add(cell == null ? "" : fmt.formatCellValue(cell));
                }
                if (header) {
                    for (int i = 0; i < vals.size(); i++) columns.add(vals.get(i).isEmpty() ? "col" + (i + 1) : vals.get(i));
                    header = false;
                } else {
                    rows.add(vals);
                }
            }
            if (columns.isEmpty()) columns.add("col1");
            if (sheet.getLastRowNum() + 1 > last) result.put("truncated", true);
        } catch (Exception e) {
            log.warn("Excel preview failed: {}", e.getMessage());
            result.put("message", "Excel 预览失败: " + e.getMessage());
            return result;
        }
        result.put("columns", columns);
        result.put("rows", rows);
        return result;
    }

    private Map<String, Object> previewJson(java.io.File f, int n) {
        Map<String, Object> result = baseResult(f);
        List<JsonNode> items = new ArrayList<>();
        try {
            String text = new String(Files.readAllBytes(f.toPath()), StandardCharsets.UTF_8);
            if (text.startsWith("﻿")) text = text.substring(1);
            JsonNode root = objectMapper.readTree(text);
            if (root.isArray()) {
                root.forEach(items::add);
            } else if (root.isObject()) {
                items.add(root);
            }
            if (items.isEmpty()) {
                // JSON Lines
                for (String line : text.split("\\n")) {
                    String t = line.trim();
                    if (t.isEmpty()) continue;
                    try { items.add(objectMapper.readTree(t)); } catch (Exception ignore) {}
                }
            }
        } catch (Exception e) {
            log.warn("JSON preview failed: {}", e.getMessage());
            result.put("message", "JSON 预览失败: " + e.getMessage());
            return result;
        }
        LinkedHashSet<String> colSet = new LinkedHashSet<>();
        for (JsonNode it : items) {
            if (it != null && it.isObject()) it.fieldNames().forEachRemaining(colSet::add);
        }
        if (colSet.isEmpty()) colSet.add("value");
        List<String> columns = new ArrayList<>(colSet);
        List<List<String>> rows = new ArrayList<>();
        int rowCount = 0;
        for (JsonNode it : items) {
            if (rowCount >= n) { result.put("truncated", true); break; }
            if (it == null || !it.isObject()) continue;
            List<String> row = new ArrayList<>();
            for (String c : columns) {
                JsonNode v = it.get(c);
                row.add((v == null || v.isNull()) ? "" : (v.isValueNode() ? v.asText() : v.toString()));
            }
            rows.add(row);
            rowCount++;
        }
        result.put("columns", columns);
        result.put("rows", rows);
        return result;
    }

    private Map<String, Object> previewXml(java.io.File f, int n) {
        Map<String, Object> result = baseResult(f);
        try {
            String csvPath = xmlPreprocessor.convertToCsv(f.getAbsolutePath(), "", ",", "UTF-8");
            if (csvPath == null) {
                result.put("message", "XML 无法解析为记录列表结构");
                return result;
            }
            Map<String, Object> csv = previewCsv(new java.io.File(csvPath), n);
            csv.put("path", f.getAbsolutePath());
            csv.put("note", "XML 已按记录列表解析（嵌套子结构保留为 JSON 列）");
            return csv;
        } catch (Exception e) {
            log.warn("XML preview failed: {}", e.getMessage());
            result.put("message", "XML 预览失败: " + e.getMessage());
            return result;
        }
    }

    private Map<String, Object> previewText(java.io.File f, int n) {
        Map<String, Object> result = baseResult(f);
        List<String> rows = new ArrayList<>();
        try (BufferedReader reader = Files.newBufferedReader(f.toPath(), StandardCharsets.UTF_8)) {
            String line;
            while (rows.size() < n && (line = reader.readLine()) != null) {
                rows.add(line);
            }
            if (reader.readLine() != null) result.put("truncated", true);
        } catch (Exception e) {
            result.put("message", "读取失败: " + e.getMessage());
            return result;
        }
        List<String> columns = new ArrayList<>();
        columns.add("content");
        List<List<String>> table = new ArrayList<>();
        for (String r : rows) { List<String> row = new ArrayList<>(); row.add(r); table.add(row); }
        result.put("columns", columns);
        result.put("rows", table);
        return result;
    }

    /**
     * 校验并规范化文件路径，仅允许 app.preview.allowed-roots 内的文件。
     * 供其它预览入口（如作业输出预览）复用，避免出现绕过白名单的读文件路径。
     */
    public Path assertAllowedRead(String rawPath) {
        return resolveAllowedFile(rawPath);
    }

    private Path resolveAllowedFile(String rawPath) {
        if (rawPath == null || rawPath.isBlank()) {
            throw new IllegalArgumentException("文件路径不能为空");
        }
        Path candidate = Path.of(rawPath.trim()).toAbsolutePath().normalize();
        try {
            Path realCandidate = candidate.toRealPath();
            boolean allowed = Arrays.stream(allowedRoots.split(","))
                    .map(String::trim)
                    .filter(root -> !root.isEmpty())
                    .map(root -> Path.of(root).toAbsolutePath().normalize())
                    .filter(Files::exists)
                    .map(root -> {
                        try {
                            return root.toRealPath();
                        } catch (java.io.IOException e) {
                            return null;
                        }
                    })
                    .filter(java.util.Objects::nonNull)
                    .anyMatch(realCandidate::startsWith);
            if (!allowed) {
                throw new IllegalArgumentException("文件路径不在允许的预览目录中");
            }
            return realCandidate;
        } catch (java.nio.file.NoSuchFileException e) {
            return candidate;
        } catch (java.io.IOException e) {
            throw new IllegalArgumentException("无法解析文件路径", e);
        }
    }

    private Map<String, Object> baseResult(java.io.File f) {
        Map<String, Object> r = new LinkedHashMap<>();
        r.put("path", f.getAbsolutePath());
        return r;
    }
}
