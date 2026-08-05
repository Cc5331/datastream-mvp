package com.datastream.mvp.model;

import jakarta.persistence.*;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.AllArgsConstructor;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.time.LocalDateTime;

/**
 * 作业定义实体
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Entity
@Table(name = "job_definition")
@JsonIgnoreProperties(ignoreUnknown = true)
public class JobDefinition {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false)
    private String name;

    @Column(columnDefinition = "TEXT")
    private String description;

    @Column(columnDefinition = "TEXT", nullable = false)
    private String dagJson;         // DAG 定义 JSON

    @Column(nullable = false)
    @Enumerated(EnumType.STRING)
    private JobStatus status = JobStatus.DRAFT;  // DRAFT / SUBMITTED / RUNNING / COMPLETED / FAILED / CANCELLED

    @Column(columnDefinition = "TEXT")
    private String flinkJobId;      // Flink 作业 ID

    private Integer parallelism = 1; // 并行度

    private LocalDateTime createdAt = LocalDateTime.now();
    private LocalDateTime updatedAt = LocalDateTime.now();
    private LocalDateTime submittedAt;
    private LocalDateTime completedAt;

    private String cronExpression;              // 定时调度 cron 表达式（如 0 */5 * * * ?）
    private Boolean scheduleEnabled = false;    // 是否启用定时调度
    private LocalDateTime nextFireTime;         // 下次调度触发时间

    public enum JobStatus {
        DRAFT, SUBMITTED, RUNNING, COMPLETED, FAILED, CANCELLED
    }
}
