package com.datastream.mvp.model;

import jakarta.persistence.*;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.AllArgsConstructor;

import java.time.LocalDateTime;

/**
 * 作业诊断报告：智能诊断 Agent 的结果快照。
 * source 标明报告来源（rule=本地规则 / openai-deepseek 等=LLM / local-fallback）。
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Entity
@Table(name = "diagnosis_report")
public class DiagnosisReport {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    private Long jobId;

    private Long ownerId;

    @Column(length = 64)
    private String jobName;

    @Column(length = 64)
    private String trigger;      // manual=用户手动诊断 / 或告警事件代码（JOB_FAILED 等=故障自动触发）

    @Column(length = 64)
    private String source;       // rule / provider 名 / local-fallback

    @Column(length = 1000)
    private String rootCause;

    @Column(length = 4000)
    private String evidence;

    @Column(length = 4000)
    private String suggestions;  // JSON 数组字符串

    @Column(columnDefinition = "TEXT")
    private String paramFixes;   // JSON 对象字符串（节点id -> {参数:修正值}）

    @Column(columnDefinition = "TEXT")
    private String packet;       // 诊断时使用的脱敏数据包快照

    private LocalDateTime createdAt = LocalDateTime.now();
}
