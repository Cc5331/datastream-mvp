package com.datastream.mvp.service;

import com.datastream.mvp.util.JdbcUrlUtil;

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

    private static final String CREATE_IF_MISSING = "CREATE_IF_MISSING";
    private static final String FAIL_IF_MISSING = "FAIL_IF_MISSING";
    private static final String VALIDATE_EXISTING = "VALIDATE_EXISTING";

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
        String url = JdbcUrlUtil.normalize(str(params.get("url")));
        String table = str(params.get("table")).trim();
        String username = resolveCredential(str(params.get("username")), defaultMysqlUsername);
        String password = resolveCredential(str(params.get("password")), defaultMysqlPassword);

        if (url.isEmpty()) {
            throw new RuntimeException("MySQL 输出缺少 JDBC URL");
        }
        if (table.isEmpty()) {
            throw new RuntimeException("MySQL 输出缺少表名");
        }
        String quotedTable = com.datastream.mvp.util.MysqlIdentifier.quoteTable(table);

        String db = parseDatabase(url);
        if (db == null || db.isEmpty()) {
            throw new RuntimeException("无法从 JDBC URL 解析数据库名，请在 URL 中指定，如 jdbc:mysql://localhost:3306/dataflow?useSSL=false");
        }

        String policy = (params.get("createTablePolicy") == null ? "" : params.get("createTablePolicy").toString())
                .trim().toUpperCase();
        // 未显式设置时按自动建表处理，历史 DAG 与 AI 生成的作业无需预先建表
        if (policy.isEmpty()) policy = CREATE_IF_MISSING;
        if (!List.of(CREATE_IF_MISSING, FAIL_IF_MISSING, VALIDATE_EXISTING).contains(policy)) {
            throw new RuntimeException("MySQL 输出 createTablePolicy 不支持: " + policy);
        }

        List<String> columns = parseColumns(sourceFields);
        if (columns.isEmpty()) {
            throw new RuntimeException("MySQL 输出无法获取字段定义，请确认输入/转换节点已正确连线");
        }

        try (Connection conn = DriverManager.getConnection(url, username, password)) {
            boolean exists = tableExists(conn, db, table);
            if (exists) {
                if (VALIDATE_EXISTING.equals(policy)) validateColumns(conn, db, table, sourceFields);
                else log.info("MySQL table already exists, skip create: `{}`.`{}`", db, table);
                return;
            }
            if (!CREATE_IF_MISSING.equals(policy)) {
                throw new RuntimeException("MySQL 目标表不存在: " + db + "." + table
                        + "；如需自动创建请将建表策略设为 CREATE_IF_MISSING");
            }
            String ddl = "CREATE TABLE " + quotedTable + " (\n  "
                    + String.join(",\n  ", columns)
                    + "\n) DEFAULT CHARSET=utf8mb4";
            try (Statement st = conn.createStatement()) {
                st.execute(ddl);
            }
            log.info("MySQL table created: `{}`.`{}` ({} columns)", db, table, columns.size());
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException("MySQL 目标表准备失败: " + e.getMessage(), e);
        }
    }

    private boolean tableExists(Connection conn, String db, String table) throws Exception {
        try (java.sql.PreparedStatement ps = conn.prepareStatement(
                "SELECT 1 FROM information_schema.tables WHERE table_schema = ? AND table_name = ? LIMIT 1")) {
            ps.setString(1, db);
            ps.setString(2, table);
            try (java.sql.ResultSet rs = ps.executeQuery()) {
                return rs.next();
            }
        }
    }

    private void validateColumns(Connection conn, String db, String table, String sourceFields) throws Exception {
        Set<String> actual = new HashSet<>();
        try (java.sql.ResultSet rs = conn.getMetaData().getColumns(db, null, table, null)) {
            while (rs.next()) actual.add(rs.getString("COLUMN_NAME").toLowerCase());
        }
        for (String line : sourceFields.split("\\n")) {
            Matcher m = COLUMN_PATTERN.matcher(line.trim().replaceFirst(",$", ""));
            if (m.matches() && !actual.contains(m.group(1).replace("`", "").toLowerCase())) {
                throw new RuntimeException("MySQL 目标表缺少字段: " + m.group(1));
            }
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
        if (t.startsWith("BINARY") || t.startsWith("VARBINARY") || t.startsWith("BYTES")) return "VARBINARY(4096)";
        switch (t) {
            case "INT": return "INT";
            case "BIGINT": return "BIGINT";
            case "SMALLINT": return "SMALLINT";
            case "TINYINT": return "TINYINT";
            case "FLOAT": return "FLOAT";
            case "DOUBLE": return "DOUBLE";
            case "BOOLEAN": return "TINYINT(1)";
            // 上游 STRING 长度不可知，用 TEXT 避免长文本/JSON/中文被静默截断
            case "STRING": return "TEXT";
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
