package com.datastream.mvp.util;

public final class MysqlIdentifier {
    private MysqlIdentifier() {}

    public static String quoteTable(String table) {
        String value = table == null ? "" : table.trim();
        String[] parts = value.split("\\.", -1);
        if (parts.length < 1 || parts.length > 2) {
            throw new IllegalArgumentException("MySQL 表名格式无效");
        }
        StringBuilder quoted = new StringBuilder();
        for (String part : parts) {
            if (!part.matches("[A-Za-z0-9_$]+")) {
                throw new IllegalArgumentException("MySQL 表名包含非法字符");
            }
            if (!quoted.isEmpty()) quoted.append('.');
            quoted.append('`').append(part).append('`');
        }
        return quoted.toString();
    }
}
