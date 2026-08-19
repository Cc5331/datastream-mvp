package com.datastream.mvp.model;

import jakarta.persistence.*;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.AllArgsConstructor;

import java.time.LocalDateTime;

/**
 * 操作审计日志
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Entity
@Table(name = "audit_log")
public class AuditLog {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    private Long userId;
    private String username;

    @Column(nullable = false)
    private String action;

    private String targetType;
    private String targetId;

    @Column(columnDefinition = "TEXT")
    private String detail;

    private String ip;

    private LocalDateTime createdAt = LocalDateTime.now();
}
