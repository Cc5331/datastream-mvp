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
    private JobStatus status = JobStatus.DRAFT;  // DRAFT / SUBMITTED / RUNNING / COMPLETED / FAILED / CANCELLED / WAITING / BLOCKED

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
    private Integer scheduleMaxRetries = 0;     // 调度提交失败最大重试次数
    private Integer scheduleRetries = 0;        // 当前已重试次数
    private String webhookUrl;                  // 告警 webhook 地址（可选）

    private Boolean online = false;             // 上线状态（生命周期）：上线=受监管后台持续处理
    private LocalDateTime onlineSince;          // 最近一次上线时间
    private Integer onlineRestartCount = 0;     // 10 分钟窗口内自动重启次数
    private LocalDateTime lastRestartAt;        // 最近一次自动重启时间

    private Long ownerId;                       // 创建人 ID（RBAC 资源归属）
    private String ownerName;                   // 创建人名称

    public enum JobStatus {
        DRAFT, SUBMITTED, RUNNING, COMPLETED, FAILED, CANCELLED, WAITING, BLOCKED
    }
}
