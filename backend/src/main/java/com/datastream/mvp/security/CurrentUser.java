package com.datastream.mvp.security;

/**
 * 当前登录用户（JWT principal）
 */
public record CurrentUser(Long id, String username, String displayName, String role) {
    public boolean isAdmin() { return "ADMIN".equals(role); }
}
