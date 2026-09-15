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

    private LocalDateTime updatedAt;

    private LocalDateTime createdAt = LocalDateTime.now();

    public enum UserRole { ADMIN, OPERATOR, VIEWER }
    public enum UserStatus { ONLINE, AWAY, BUSY, INVISIBLE, OFFLINE }
}
