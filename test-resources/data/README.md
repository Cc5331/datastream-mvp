# 📊 测试数据资源说明

> 由 gen_all_test_data.py 自动生成
> 生成时间: 2026-07-18 17:17:45

## 数据文件清单

### CSV 测试数据集

| 文件 | 行数 | 用途 |
|------|------|------|
| users.csv | 500 | 用户信息 - 字段拼接/过滤测试 |
| orders.csv | 2,000 | 订单数据 - 全链路处理测试 |
| system_logs.csv | 10,000 | 系统日志 - 大数据量/性能测试 |
| sales.csv | 3,000 | 销售数据 - 区域分析/转换测试 |
| test_input_100000.csv | 100,000 | 基准测试 - 10万行 |
| test_input_1000000.csv | 1,000,000 | 基准测试 - 100万行 |
| test_input_10000000.csv | 10,000,000 | 基准测试 - 1000万行 |

### Excel 测试文件

| 文件 | 说明 |
|------|------|
| sample_data.xlsx | 3个Sheet: 员工信息/产品销售/月度KPI |

### XML 测试文件

| 文件 | 说明 |
|------|------|
| orders.xml | 订单数据 XML (含嵌套 items) |
| sales_report.xml | 销售报表 XML (含 summary + 区域) |

### JSON 测试文件

| 文件 | 说明 |
|------|------|
| user_events.json | 用户事件流 (500条, 嵌套 payload) |
| system_config.json | 系统配置 (扁平结构) |

## DAG JSON 测试场景

| 文件 | 场景 | 并行度 |
|------|------|--------|
| 01-csv-input-csv-output.json | CSV 透传 (基础) | 2 |
| 02-csv-concat-csv.json | CSV 读→字段拼接→写 (核心链路) | 2 |
| 03-csv-xml-convert-csv.json | XML/JSON 转换测试 | 1 |
| 04-orders-full-pipeline.json | 订单全链路处理 | 2 |
| 05-system-logs-analysis.json | 系统日志分析 (大数据量) | 4 |
| 06-sales-by-region.json | 销售区域分析 | 2 |

## 快速测试步骤

### 手动测试 (UI 拖拽)
1. 启动后端: cd backend && mvn spring-boot:run
2. 打开前端: frontend/public/index.html
3. 拖入 CSV 输入, 填路径为 test-resources/data/users.csv
4. 拖入 CSV 输出, 连接, 保存后提交

### 用 DAG JSON 测试
1. 启动后端
2. 在前端导入 DAG JSON: test-resources/dags/02-csv-concat-csv.json
3. 保存 -> 提交运行

### 基准测试
cd test-resources
python benchmark.py

## 数据说明
- 所有 CSV 均为 UTF-8 编码, 逗号分隔, 含表头
- Excel 可在 WPS/Office 直接打开编辑
- DAG JSON 可直接在前端"导入JSON"加载
- 基准测试文件用于 benchmark.py 性能测试
