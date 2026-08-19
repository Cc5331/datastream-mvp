package com.datastream.mvp.model;

import jakarta.persistence.*;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

/**
 * 定时调度触发历史
 */
@Data
@NoArgsConstructor
@Entity
@Table(name = "schedule_history")
public class ScheduleHistory {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    private Long jobId;
    private String jobName;
    private LocalDateTime triggerTime;
    private String status;      // SUCCESS / FAILED / RETRY
    @Column(columnDefinition = "TEXT")
    private String message;
    private String flinkJobId;
}
