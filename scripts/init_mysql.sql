-- MySQL initialization script for Flink data output
-- Run this in your MySQL client (adjust password as needed)
-- mysql -u root -p < D:\code\比赛\2026省服务外包\scripts\init_mysql.sql

CREATE DATABASE IF NOT EXISTS flink_demo;
USE flink_demo;

-- Table for Datagen output (matches default Datagen schema)
CREATE TABLE IF NOT EXISTS user_data (
    id INT,
    name VARCHAR(255),
    age INT,
    salary DOUBLE
);

-- Optional: Create user for Flink JDBC connection
-- CREATE USER 'flink'@'%' IDENTIFIED BY 'flink123';
-- GRANT ALL PRIVILEGES ON flink_demo.* TO 'flink'@'%';
-- FLUSH PRIVILEGES;

SELECT 'MySQL init complete. Database flink_demo.user_data created.' AS status;
