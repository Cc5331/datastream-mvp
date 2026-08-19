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
@Table(name = "alert_record")
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

    @Column(columnDefinition = "TEXT", nullable = false)
    private String message;

    private Boolean readFlag = false;   // 是否已读

    private LocalDateTime createdAt = LocalDateTime.now();
}
