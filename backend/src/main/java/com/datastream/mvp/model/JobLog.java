package com.datastream.mvp.model;

import jakarta.persistence.*;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.AllArgsConstructor;

import java.time.LocalDateTime;

/**
 * 作业运行日志实体
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Entity
@Table(name = "job_log")
public class JobLog {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false)
    private Long jobId;

    @Column(nullable = false)
    private String level;       // INFO / WARN / ERROR

    @Column(columnDefinition = "TEXT", nullable = false)
    private String message;

    private LocalDateTime timestamp = LocalDateTime.now();
}
