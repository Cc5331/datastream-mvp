package com.datastream.mvp.model;

import jakarta.persistence.*;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.AllArgsConstructor;

/**
 * 监控吞吐趋势点（持久化真实，内存 trendBuffer 为热路径缓存）。
 * 每个作业只保留最近一次运行（runKey）的点，重新运行时旧行被删除。
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Entity
@Table(name = "trend_point")
public class TrendPoint {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false)
    private Long jobId;

    /** 采样时间（epoch ms） */
    @Column(nullable = false)
    private long t;

    /** 入速率 records/s；列名避开 H2/MySQL 保留字 IN */
    @Column(name = "input_rate", nullable = false)
    private double in;

    /** 出速率 records/s（吞吐曲线主值）；列名避开 SQL 关键字 */
    @Column(name = "output_rate", nullable = false)
    private double out;

    /** 背压比例 0-1 */
    private double bp;

    /** 运行批次标识（job.submittedAt.toString()），新运行会切换 runKey 并清旧 */
    @Column(length = 64)
    private String runKey;

    /** 记录点时的作业状态快照（RUNNING/COMPLETED/FAILED/CANCELLED） */
    @Column(length = 16)
    private String status;
}
