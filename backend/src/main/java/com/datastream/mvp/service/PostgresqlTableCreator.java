package com.datastream.mvp.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** PostgreSQL 输出表校验与按需创建。 */
@Slf4j
@Service
public class PostgresqlTableCreator {

    private static final Pattern COLUMN_PATTERN =
            Pattern.compile("^`?([^`\\s]+)`?\\s+([A-Za-z0-9_(), ]+)$");
    private static final Pattern IDENTIFIER_PATTERN = Pattern.compile("[\\p{L}_][\\p{L}\\p{N}_$]*");

    @Value("${app.postgres.default-username:postgres}")
    private String defaultUsername;

    @Value("${app.postgres.default-password:}")
    private String defaultPassword;

    public void ensureTable(Map<String, Object> params, String sourceFields) {
        if (params == null) throw new RuntimeException("PostgreSQL 输出缺少连接参数");
        String url = str(params.get("url")).trim();
        String schema = defaultValue(str(params.get("schema")).trim(), "public");
        String table = str(params.get("table")).trim();
        String username = resolveCredential(str(params.get("username")), defaultUsername, "${POSTGRES_USERNAME}");
        String password = resolveCredential(str(params.get("password")), defaultPassword, "${POSTGRES_PASSWORD}");
        String policy = defaultValue(str(params.get("createTablePolicy")).trim(), "FAIL_IF_MISSING").toUpperCase();

        if (!url.startsWith("jdbc:postgresql://")) {
            throw new RuntimeException("PostgreSQL 输出 url 必须以 jdbc:postgresql:// 开头");
        }
        validateIdentifier(schema, "Schema");
        validateIdentifier(table, "表名");
        if (!List.of("FAIL_IF_MISSING", "CREATE_IF_MISSING", "VALIDATE_EXISTING").contains(policy)) {
            throw new RuntimeException("PostgreSQL 输出 createTablePolicy 不支持: " + policy);
        }
        List<String> columns = parseColumns(sourceFields);
        if (columns.isEmpty()) throw new RuntimeException("PostgreSQL 输出无法获取上游字段定义");

        try (Connection connection = DriverManager.getConnection(url, username, password)) {
            if (tableExists(connection, schema, table)) {
                if ("VALIDATE_EXISTING".equals(policy)) validateColumns(connection, schema, table, sourceFields);
                return;
            }
            if (!"CREATE_IF_MISSING".equals(policy)) {
                throw new RuntimeException("PostgreSQL 目标表不存在: " + schema + "." + table
                        + "；如需自动创建请选择 CREATE_IF_MISSING");
            }
            String ddl = "CREATE TABLE " + quote(schema) + "." + quote(table)
                    + " (\n  " + String.join(",\n  ", columns) + "\n)";
            try (Statement statement = connection.createStatement()) {
                statement.execute(ddl);
            }
            log.info("PostgreSQL table created: {}.{} ({} columns)", schema, table, columns.size());
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException("PostgreSQL 目标表准备失败: " + e.getMessage(), e);
        }
    }

    public String qualifiedTable(Map<String, Object> params) {
        String schema = defaultValue(str(params == null ? null : params.get("schema")).trim(), "public");
        String table = str(params == null ? null : params.get("table")).trim();
        validateIdentifier(schema, "Schema");
        validateIdentifier(table, "表名");
        return schema + "." + table;
    }

    private boolean tableExists(Connection connection, String schema, String table) throws Exception {
        DatabaseMetaData metadata = connection.getMetaData();
        try (ResultSet rows = metadata.getTables(null, schema, table, new String[]{"TABLE"})) {
            return rows.next();
        }
    }

    private void validateColumns(Connection connection, String schema, String table, String sourceFields) throws Exception {
        List<String> actual = new ArrayList<>();
        try (ResultSet rows = connection.getMetaData().getColumns(null, schema, table, null)) {
            while (rows.next()) actual.add(rows.getString("COLUMN_NAME").toLowerCase());
        }
        for (String line : sourceFields.split("\\n")) {
            Matcher matcher = COLUMN_PATTERN.matcher(line.trim().replaceFirst(",$", ""));
            if (matcher.matches() && !actual.contains(matcher.group(1).toLowerCase())) {
                throw new RuntimeException("PostgreSQL 目标表缺少字段: " + matcher.group(1));
            }
        }
    }

    private List<String> parseColumns(String sourceFields) {
        List<String> columns = new ArrayList<>();
        if (sourceFields == null || sourceFields.isBlank()) return columns;
        for (String part : sourceFields.split("\\n")) {
            String line = part.trim();
            if (line.endsWith(",")) line = line.substring(0, line.length() - 1).trim();
            Matcher matcher = COLUMN_PATTERN.matcher(line);
            if (!matcher.matches()) continue;
            String name = matcher.group(1).trim();
            validateIdentifier(name, "字段名");
            columns.add(quote(name) + " " + mapType(matcher.group(2).replaceAll("\\s+", "")));
        }
        return columns;
    }

    private String mapType(String flinkType) {
        String type = flinkType.toUpperCase();
        if (type.startsWith("DECIMAL")) return "NUMERIC" + type.substring("DECIMAL".length());
        if (type.startsWith("TIMESTAMP")) return type;
        if (type.startsWith("BINARY") || type.startsWith("VARBINARY")) return "BYTEA";
        return switch (type) {
            case "BOOLEAN" -> "BOOLEAN";
            case "TINYINT", "SMALLINT" -> "SMALLINT";
            case "INT" -> "INTEGER";
            case "BIGINT" -> "BIGINT";
            case "FLOAT" -> "REAL";
            case "DOUBLE" -> "DOUBLE PRECISION";
            case "DATE" -> "DATE";
            case "TIME" -> "TIME";
            case "STRING" -> "TEXT";
            default -> "TEXT";
        };
    }

    private void validateIdentifier(String value, String label) {
        if (value == null || !IDENTIFIER_PATTERN.matcher(value).matches()) {
            throw new IllegalArgumentException("PostgreSQL " + label + "不合法: " + value);
        }
    }

    private String quote(String value) {
        return "\"" + value.replace("\"", "\"\"") + "\"";
    }

    private String resolveCredential(String value, String fallback, String placeholder) {
        String trimmed = value == null ? "" : value.trim();
        return trimmed.isEmpty() || placeholder.equals(trimmed) ? defaultValue(fallback, "") : trimmed;
    }

    private String defaultValue(String value, String fallback) {
        return value == null || value.isEmpty() ? fallback : value;
    }

    private String str(Object value) {
        return value == null ? "" : value.toString();
    }
}
