package com.datastream.mvp.model;

import jakarta.persistence.*;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.AllArgsConstructor;

import java.time.LocalDateTime;

/**
 * 作业版本快照：保存/回滚历史（每次保存生成一个新版本）
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Entity
@Table(name = "job_version")
public class JobVersion {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false)
    private Long jobId;

    private Integer versionNo;

    @Column(columnDefinition = "TEXT")
    private String name;

    @Column(columnDefinition = "TEXT", nullable = false)
    private String dagJson;

    private Integer parallelism;

    private LocalDateTime createdAt = LocalDateTime.now();
}
