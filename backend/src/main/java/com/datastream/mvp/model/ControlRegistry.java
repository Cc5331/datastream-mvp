package com.datastream.mvp.model;

import jakarta.persistence.*;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.AllArgsConstructor;

import java.time.LocalDateTime;

/**
 * 控件注册表实体
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Entity
@Table(name = "control_registry")
public class ControlRegistry {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false, unique = true)
    private String type;            // 控件类型标识：csv_input, mysql_output, field_concat 等

    @Column(nullable = false)
    private String name;            // 显示名称

    @Column(nullable = false)
    private String category;        // 分类：input / output / transform

    @Column(columnDefinition = "TEXT")
    private String description;     // 描述

    @Column(columnDefinition = "TEXT")
    private String paramSchema;     // 参数 JSON Schema（前端动态渲染参数面板用）

    @Column(columnDefinition = "TEXT")
    private String flinkTemplate;   // Flink SQL/Table API 模板

    @Column(nullable = false)
    private String version;         // 版本号

    @Column(nullable = false)
    private String jarPath;         // 控件 JAR 包路径

    private Boolean enabled = true; // 是否启用

    private LocalDateTime createdAt = LocalDateTime.now();
    private LocalDateTime updatedAt = LocalDateTime.now();
}
