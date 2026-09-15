package com.datastream.mvp.model;

import jakarta.persistence.*;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.AllArgsConstructor;

import java.time.LocalDateTime;

/**
 * 系统用户（RBAC：ADMIN / OPERATOR / VIEWER）
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Entity
@Table(name = "app_user")
public class AppUser {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false, unique = true)
    private String username;

    @Column(nullable = false)
    private String passwordHash;

    private String displayName;

    @Column(nullable = false)
    @Enumerated(EnumType.STRING)
    private UserRole role = UserRole.VIEWER;

    private boolean enabled = true;

    @Column(length = 255)
    private String avatarKey;

    @Column(length = 200)
    private String signature;

    @Enumerated(EnumType.STRING)
    @Column(length = 16)
    private UserStatus statusPreference = UserStatus.ONLINE;

    private LocalDateTime lastLoginAt;

    private LocalDateTime lastSeenAt;

    /**
     * 令牌版本：签发 JWT 时写入 claim，每次请求与库中值比对。
     * 改密码 / 管理员重置密码 / 主动登出时 +1，使此前签发的所有 token 立即失效
     * （无状态 JWT 无法逐个吊销，用版本号实现"全端下线"）。
     */
    private Integer tokenVersion = 0;

    private LocalDateTime updatedAt;

    private LocalDateTime createdAt = LocalDateTime.now();

    public enum UserRole { ADMIN, OPERATOR, VIEWER }
    public enum UserStatus { ONLINE, AWAY, BUSY, INVISIBLE, OFFLINE }
}
