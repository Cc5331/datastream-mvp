package com.datastream.mvp.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * MySQL 输出自动建表：提交作业前根据上游 schema 在目标库中创建表（IF NOT EXISTS）。
 */
@Slf4j
@Service
public class MysqlTableCreator {

    private static final Pattern DB_PATTERN =
            Pattern.compile("jdbc:mysql://[^/]+/([^?;]+)");
    private static final Pattern COLUMN_PATTERN =
            Pattern.compile("^`?([^`\\s]+)`?\\s+([A-Za-z0-9_() ]+)$");

    @Value("${app.mysql.default-username:root}")
    private String defaultMysqlUsername;

    @Value("${app.mysql.default-password:}")
    private String defaultMysqlPassword;

    private static final Set<String> RESERVED = new HashSet<>(Arrays.asList(
            "order", "group", "select", "from", "where", "insert", "update", "delete", "drop",
            "create", "table", "index", "key", "primary", "foreign", "constraint", "unique",
            "default", "desc", "asc", "limit", "offset", "having", "join", "left", "right",
            "inner", "outer", "union", "all", "and", "or", "not", "null", "like", "in", "is",
            "as", "on", "using", "into", "values", "set", "alter", "rename", "check", "column",
            "columns", "database", "schema", "type", "user", "status", "range", "rank", "case",
            "when", "then", "else", "end", "between", "exists", "distinct", "cast", "convert",
            "date", "time", "timestamp", "year", "binary", "text", "enum", "key", "interval"
    ));

    /**
     * 根据上游字段定义自动创建 MySQL 表。
     *
     * @param params       MySQL 输出节点参数（url/table/username/password）
     * @param sourceFields 上游 schema，形如 "`col` STRING,\n  `col2` STRING"
     */
    public void ensureTable(Map<String, Object> params, String sourceFields) {
        if (params == null) {
            throw new RuntimeException("MySQL 输出缺少连接参数");
        }
        String url = str(params.get("url")).trim();
        String table = str(params.get("table")).trim();
        String username = resolveCredential(str(params.get("username")), defaultMysqlUsername);
        String password = resolveCredential(str(params.get("password")), defaultMysqlPassword);

        if (url.isEmpty()) {
            throw new RuntimeException("MySQL 输出缺少 JDBC URL");
        }
        if (table.isEmpty()) {
            throw new RuntimeException("MySQL 输出缺少表名");
        }

        String db = parseDatabase(url);
        if (db == null || db.isEmpty()) {
            throw new RuntimeException("无法从 JDBC URL 解析数据库名，请在 URL 中指定，如 jdbc:mysql://localhost:3306/dataflow?useSSL=false");
        }

        List<String> columns = parseColumns(sourceFields);
        if (columns.isEmpty()) {
            throw new RuntimeException("MySQL 输出无法获取字段定义，请确认输入/转换节点已正确连线");
        }

        String ddl = "CREATE TABLE IF NOT EXISTS `" + table + "` (\n  "
                + String.join(",\n  ", columns)
                + "\n) DEFAULT CHARSET=utf8mb4";

        log.info("MySQL auto-create table: {}", ddl);
        try (Connection conn = DriverManager.getConnection(url, username, password);
             Statement st = conn.createStatement()) {
            st.execute(ddl);
            log.info("MySQL table ensured: `{}`.`{}` ({} columns)", db, table, columns.size());
        } catch (Exception e) {
            throw new RuntimeException("MySQL 自动建表失败: " + e.getMessage() + " | SQL: " + ddl, e);
        }
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

    private String parseDatabase(String url) {
        Matcher m = DB_PATTERN.matcher(url);
        return m.find() ? m.group(1).trim() : null;
    }

    private List<String> parseColumns(String sourceFields) {
        List<String> cols = new ArrayList<>();
        if (sourceFields == null || sourceFields.isBlank()) return cols;
        String[] parts = sourceFields.split("\n");
        for (String part : parts) {
            String line = part.trim();
            if (line.isEmpty()) continue;
            if (line.endsWith(",")) line = line.substring(0, line.length() - 1).trim();
            Matcher m = COLUMN_PATTERN.matcher(line);
            if (!m.find()) continue;
            String name = m.group(1).trim();
            String type = m.group(2).trim().replaceAll("\\s+", "");
            cols.add(quoteName(name) + " " + mapType(type));
        }
        return cols;
    }

    private String mapType(String flinkType) {
        String t = flinkType.toUpperCase();
        if (t.startsWith("DECIMAL")) return "DECIMAL" + t.substring("DECIMAL".length());
        if (t.startsWith("TIMESTAMP")) return "DATETIME(3)";
        if (t.startsWith("BINARY") || t.startsWith("VARBINARY")) return "VARBINARY(255)";
        switch (t) {
            case "INT": return "INT";
            case "BIGINT": return "BIGINT";
            case "SMALLINT": return "SMALLINT";
            case "TINYINT": return "TINYINT";
            case "FLOAT": return "FLOAT";
            case "DOUBLE": return "DOUBLE";
            case "BOOLEAN": return "TINYINT(1)";
            case "STRING": return "VARCHAR(255)";
            case "DATE": return "DATE";
            case "TIME": return "TIME";
            default: return "TEXT";
        }
    }

    private String quoteName(String raw) {
        String name = raw.replace("`", "").trim();
        if (name.isEmpty()) return "col";
        if (Character.isDigit(name.charAt(0))) name = "_" + name;
        boolean plain = name.matches("[A-Za-z_][A-Za-z0-9_]*") && !RESERVED.contains(name.toLowerCase());
        return plain ? name : "`" + name.replace("`", "``") + "`";
    }

    private String str(Object o) {
        return o == null ? "" : o.toString();
    }
}
