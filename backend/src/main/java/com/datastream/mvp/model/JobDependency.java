package com.datastream.mvp.model;

import jakarta.persistence.*;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.AllArgsConstructor;

import java.time.LocalDateTime;

/**
 * 作业依赖边：upstream 完成后自动触发 downstream（跨作业编排）
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Entity
@Table(name = "job_dependency",
        uniqueConstraints = @UniqueConstraint(columnNames = {"upstream_job_id", "downstream_job_id"}))
public class JobDependency {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "upstream_job_id", nullable = false)
    private Long upstreamJobId;

    @Column(name = "downstream_job_id", nullable = false)
    private Long downstreamJobId;

    private LocalDateTime createdAt = LocalDateTime.now();
}
