package com.datastream.mvp.security;

import io.jsonwebtoken.Claims;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.security.Keys;
import jakarta.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.crypto.SecretKey;
import java.nio.charset.StandardCharsets;
import java.util.Date;

/**
 * JWT 生成与解析（HS256）
 */
@Component
public class JwtUtil {

    /** 令牌版本 claim：改密码 / 登出后自增，用于失效此前签发的 token */
    public static final String TOKEN_VERSION_CLAIM = "tv";

    @Value("${app.jwt.secret}")
    private String secret;

    @Value("${app.jwt.expiration-ms:86400000}")
    private long expirationMs;

    @PostConstruct
    void validateConfiguration() {
        if (secret == null || secret.getBytes(StandardCharsets.UTF_8).length < 32) {
            throw new IllegalStateException("JWT_SECRET 必须配置且至少为 32 字节");
        }
    }

    private SecretKey key() {
        return Keys.hmacShaKeyFor(secret.getBytes(StandardCharsets.UTF_8));
    }

    public String generateToken(Long userId, String username, String displayName, String role) {
        return generateToken(userId, username, displayName, role, 0);
    }

    /**
     * 生成 token；tokenVersion 参与签发，用户改密/登出后旧 token 因版本不匹配而失效。
     */
    public String generateToken(Long userId, String username, String displayName, String role, Integer tokenVersion) {
        Date now = new Date();
        return Jwts.builder()
                .subject(String.valueOf(userId))
                .claim("username", username)
                .claim("displayName", displayName == null ? username : displayName)
                .claim("role", role)
                .claim(TOKEN_VERSION_CLAIM, tokenVersion == null ? 0 : tokenVersion)
                .issuedAt(now)
                .expiration(new Date(now.getTime() + expirationMs))
                .signWith(key())
                .compact();
    }

    /** token 中的令牌版本；旧版本签发的 token 没有该 claim，按 0 处理 */
    public static Integer tokenVersionOf(Claims claims) {
        Object raw = claims == null ? null : claims.get(TOKEN_VERSION_CLAIM);
        if (raw instanceof Number n) return n.intValue();
        if (raw == null) return 0;
        try {
            return Integer.parseInt(String.valueOf(raw));
        } catch (NumberFormatException e) {
            return 0;
        }
    }

    public Claims parse(String token) {
        return Jwts.parser().verifyWith(key()).build().parseSignedClaims(token).getPayload();
    }
}
