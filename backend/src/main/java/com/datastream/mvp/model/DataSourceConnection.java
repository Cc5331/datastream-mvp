package com.datastream.mvp.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import jakarta.persistence.*;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.time.LocalDateTime;

@Getter
@Setter
@NoArgsConstructor
@Entity
@Table(name = "data_source_connection",
        uniqueConstraints = @UniqueConstraint(name = "uk_data_source_owner_name", columnNames = {"owner_id", "name"}),
        indexes = {
                @Index(name = "idx_data_source_owner", columnList = "owner_id"),
                @Index(name = "idx_data_source_updated", columnList = "updated_at")
        })
public class DataSourceConnection {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false, length = 128)
    private String name;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 32)
    private DataSourceType type;

    @Column(columnDefinition = "TEXT")
    private String description;

    @Column(name = "config_json", columnDefinition = "TEXT", nullable = false)
    private String configJson;

    @JsonIgnore
    @Column(name = "encrypted_credentials", columnDefinition = "TEXT")
    private String encryptedCredentials;

    @Column(name = "credential_configured", nullable = false)
    private boolean credentialConfigured;

    @Column(nullable = false)
    private boolean enabled = true;

    @Column(name = "owner_id", nullable = false)
    private Long ownerId;

    @Column(name = "owner_name", nullable = false, length = 128)
    private String ownerName;

    @Column(name = "created_at", nullable = false)
    private LocalDateTime createdAt;

    @Column(name = "updated_at", nullable = false)
    private LocalDateTime updatedAt;

    @Version
    private Long version;

    @PrePersist
    void prePersist() {
        LocalDateTime now = LocalDateTime.now();
        if (createdAt == null) createdAt = now;
        updatedAt = now;
    }

    @PreUpdate
    void preUpdate() {
        updatedAt = LocalDateTime.now();
    }
}
