package com.datastream.mvp.config;

import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

/**
 * Hadoop 本地运行环境配置：启动时设置 hadoop.home.dir（winutils 所在目录），
 * 供 Parquet 读写本地文件使用，避免 "HADOOP_HOME and hadoop.home.dir are unset" 错误。
 */
@Slf4j
@Configuration
public class HadoopHomeConfig {

    @Value("${app.hadoop-home:}")
    private String hadoopHome;

    @PostConstruct
    public void init() {
        if (hadoopHome != null && !hadoopHome.isBlank()) {
            System.setProperty("hadoop.home.dir", hadoopHome);
            log.info("hadoop.home.dir set to: {}", hadoopHome);
        }
    }
}
