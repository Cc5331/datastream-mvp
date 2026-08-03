-- ========================================
-- 数据流任务管理系统 - MySQL 建表脚本
-- 用于 datagen → mysql_output 测试
-- 默认字段: id INT, name STRING, age INT, salary DOUBLE
-- ========================================

-- 1. 创建数据库（如果不存在）
CREATE DATABASE IF NOT EXISTS flink_demo DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

USE flink_demo;

-- 2. 创建目标表（字段类型与 Flink datagen 默认配置匹配）
CREATE TABLE IF NOT EXISTS user_data (
  id      INT         NOT NULL,
  name    VARCHAR(255) DEFAULT NULL,
  age     INT          DEFAULT NULL,
  salary  DOUBLE       DEFAULT NULL,
  PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 3. 验证表结构
DESC user_data;

-- 4.（可选）清空表
-- TRUNCATE TABLE user_data;

-- 5. 验证数据
-- SELECT * FROM user_data LIMIT 20;
