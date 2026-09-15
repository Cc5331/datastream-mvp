package com.datastream.mvp.security;

import io.jsonwebtoken.Claims;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 令牌版本：改密码 / 重置密码 / 登出后旧 token 必须失效（无状态 JWT 的"全端下线"实现）。
 */
class JwtUtilTest {

    private JwtUtil jwtUtil;

    @BeforeEach
    void setUp() throws Exception {
        jwtUtil = new JwtUtil();
        set(jwtUtil, "secret", "unit-test-secret-key-at-least-32-bytes-long!!");
        set(jwtUtil, "expirationMs", 60_000L);
    }

    @Test
    void tokenCarriesTokenVersionAndUserId() {
        String token = jwtUtil.generateToken(11L, "alice", "爱丽丝", "ADMIN", 4);
        Claims claims = jwtUtil.parse(token);

        assertEquals("11", claims.getSubject());
        assertEquals("alice", claims.get("username"));
        assertEquals("爱丽丝", claims.get("displayName"));
        assertEquals("ADMIN", claims.get("role"));
        assertEquals(4, JwtUtil.tokenVersionOf(claims));
    }

    @Test
    void legacyTokenWithoutVersionClaim_readsAsZero() {
        // 旧版本（无令牌版本 claim）签发的 token 兼容为 0，改密后即因版本不匹配失效
        String legacy = jwtUtil.generateToken(12L, "bob", null, "VIEWER");
        assertEquals(0, JwtUtil.tokenVersionOf(jwtUtil.parse(legacy)));
    }

    @Test
    void tokenVersionMatches_isNullSafe() {
        assertTrue(JwtAuthFilter.tokenVersionMatches(0, 0));
        assertTrue(JwtAuthFilter.tokenVersionMatches(null, null), "claim 与库中均缺失按 0 处理");
        assertTrue(JwtAuthFilter.tokenVersionMatches(null, 0));
        assertTrue(JwtAuthFilter.tokenVersionMatches(0, null));
        assertFalse(JwtAuthFilter.tokenVersionMatches(0, 1), "登出/改密后自增，旧 token 必须不匹配");
        assertFalse(JwtAuthFilter.tokenVersionMatches(3, 4));
    }

    private void set(Object target, String field, Object value) throws Exception {
        Field f = JwtUtil.class.getDeclaredField(field);
        f.setAccessible(true);
        f.set(target, value);
    }
}
