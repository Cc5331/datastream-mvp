# 多数据源交叉验收报告

- 执行时间：2026-09-12 10:08:38
- 后端地址：http://localhost:18080
- 结果：**7/7 通过**

| 用例 | 链路 | 结果 | 说明 |
|---|---|---|---|
| E2E-PG-01 | PostgreSQL → CSV | ✅ PASS | 3 行, 列=['id', 'name', 'college', 'gpa', 'enrolled_at'], 首行=['1', '张三', '计算机学院', '3.85', '"2026-09-01 08:30:00"'] |
| E2E-PG-02 | CSV → PostgreSQL(自动建表) | ✅ PASS | 3000 行, 列=sale_id,region,product_category,sales_person,amount,quantity,sale_date,channel |
| E2E-HDFS-01 | PostgreSQL → HDFS | ✅ PASS | HDFS 目录含 part 文件: Found 1 items |
| E2E-HDFS-02 | HDFS → CSV | ✅ PASS | 3 行, 列=['id', 'name', 'college'], 首行=['1', '张三', '计算机学院'] |
| E2E-REDIS-01 | CSV → Redis Lookup → CSV | ✅ PASS | redis_value=华东重点订单（真实 Redis 值，非 key 字符串） |
| E2E-CROSS-01 | CSV → 字段过滤 → PostgreSQL | ✅ PASS | state=COMPLETED count=3000 列=sale_id,region,amount log=Status: COMPLETED (Flink: FINISHED) | 作业已提交，Flink Job ID: 5fb7188d3d39b6820f0f4ab22b1c7c38 |
| E2E-CROSS-02 | PostgreSQL → HDFS（过滤后数据） | ✅ PASS | state=COMPLETED part文件=是 log=Status: COMPLETED (Flink: FINISHED) | Status: RUNNING (Flink: RUNNING) | 作业已提交，Flink Job ID: 2a90ac688e2e0e1f4d5968eade40d0bf |

## 环境

- PostgreSQL 16（Docker，localhost:5433，库 dataflow，表 students/e2e_*）
- Redis 7（Docker，localhost:6379，含 sale:1 演示 key）
- HDFS（Docker，hdfs://localhost:9000）
- Flink 1.18.1（Docker，JobManager 18081 / SQL Gateway 18083）
