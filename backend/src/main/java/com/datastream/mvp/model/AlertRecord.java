package com.datastream.mvp.model;

import jakarta.persistence.*;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.AllArgsConstructor;

import java.time.LocalDateTime;

/**
 * 告警记录实体：告警中心数据源（作业失败 / 重启超限 / 吞吐归零 / 背压 / checkpoint / 调度失败）
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Entity
@Table(name = "alert_record", indexes = {
        @Index(name = "idx_alert_owner_created", columnList = "ownerId,createdAt"),
        @Index(name = "idx_alert_read_created", columnList = "readFlag,createdAt")
})
public class AlertRecord {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false)
    private String level;           // INFO / WARN / CRITICAL

    @Column(nullable = false)
    private String event;           // JOB_FAILED / ONLINE_JOB_STOPPED / THROUGHPUT_ZERO / BACKPRESSURE_HIGH / CHECKPOINT_FAILED / SCHEDULE_FAILED / AI_ACTION

    private Long jobId;
    private String jobName;
    private Long ownerId;

    @Column(columnDefinition = "TEXT", nullable = false)
    private String message;

    private Boolean readFlag = false;   // 是否已读

    private LocalDateTime createdAt = LocalDateTime.now();

    /**
     * 聚合签名（event + 归一化后的根因），用于把「同一根因、多个作业」的告警合并成一条。
     * 见 AlertAggregator.signatureOf。旧数据为 null，表示未参与聚合。
     */
    @Column(length = 512)
    private String signature;

    /** 合并次数（含首条）；旧数据为 null 按 1 处理 */
    private Integer occurrences = 1;

    /** 最近一次同类告警的时间（告警中心展示「最近发生」） */
    private LocalDateTime lastSeenAt;

    /** 已合并涉及的作业清单（"作业名(#id)" 逗号分隔，最多保留 20 个） */
    @Column(columnDefinition = "TEXT")
    private String aggregatedJobs;
}
