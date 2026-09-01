package com.datastream.mvp.util;

/**
 * MySQL JDBC URL 归一化：
 * MySQL 8 + caching_sha2_password 在 useSSL=false 时要求 allowPublicKeyRetrieval=true，
 * 否则报 "Public Key Retrieval is not allowed"。统一在此补全，避免各连接点重复处理。
 */
public final class JdbcUrlUtil {

    private JdbcUrlUtil() {
    }

    /**
     * 对 jdbc:mysql 开头的 URL 追加 allowPublicKeyRetrieval=true（若尚未包含）。
     */
    public static String normalize(String url) {
        if (url == null) {
            return null;
        }
        String u = url.trim();
        if (!u.startsWith("jdbc:mysql")) {
            return u;
        }
        if (u.contains("allowPublicKeyRetrieval")) {
            return u;
        }
        String sep = u.contains("?") ? "&" : "?";
        return u + sep + "allowPublicKeyRetrieval=true";
    }
}
