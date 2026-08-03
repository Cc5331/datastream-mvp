"""
数据流任务管理系统 — 综合测试数据生成器
========================================
生成所有格式的测试数据:
  - CSV 数据集 (小/中/大, 多场景)
  - Excel 数据集 (.xlsx)
  - XML 样本数据 (用于 XML<->JSON 测试)
  - JSON 样本数据 (用于 Kafka/转换测试)
  - 即用 DAG JSON 配置文件
  - 基准测试数据集 (10万/100万/1000万行)

用法: python gen_all_test_data.py
输出目录: test-resources/data/
"""

import csv, json, os, time, math, random
from datetime import datetime, timedelta

DATA_DIR = "test-resources/data"
DAG_DIR  = "test-resources/dags"
os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(DAG_DIR,  exist_ok=True)

# ============ 1. 通用辅助函数 ============

def fmt_size(n):
    for unit in ['B','KB','MB','GB']:
        if n < 1024: return f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} TB"

def write_csv(path, rows, headers):
    with open(path, 'w', newline='', encoding='utf-8') as f:
        w = csv.DictWriter(f, fieldnames=headers)
        w.writeheader()
        w.writerows(rows)
    fs = os.path.getsize(path)
    print(f"  ✅ {os.path.basename(path)}  ({len(rows):,} 行, {fmt_size(fs)})")

# ============ 2. 测试场景数据集 ============

def gen_users_csv():
    """场景: 用户信息表 (用于字段拼接 + 过滤测试)"""
    headers = ["user_id","name","email","phone","department","salary","hire_date","city"]
    rows = []
    depts = ["Engineering","Sales","Marketing","HR","Finance","Operations"]
    cities = ["北京","上海","深圳","杭州","广州","成都"]
    for i in range(1, 501):
        rows.append({
            "user_id": i,
            "name": f"员工_{i}",
            "email": f"user{i}@company.com",
            "phone": f"1{random.choice(['38','39','50','51','52'])}{random.randint(10000000,99999999)}",
            "department": random.choice(depts),
            "salary": round(random.uniform(8000, 50000), 2),
            "hire_date": (datetime(2020,1,1) + timedelta(days=random.randint(0,2000))).strftime("%Y-%m-%d"),
            "city": random.choice(cities),
        })
    write_csv(f"{DATA_DIR}/users.csv", rows, headers)
    return headers

def gen_orders_csv():
    """场景: 订单表 (用于 MySQL/Kafka 输入模拟 + 转换测试)"""
    headers = ["order_id","user_id","product","quantity","amount","status","order_date","pay_method"]
    rows = []
    products = ["笔记本电脑","手机","耳机","键盘","鼠标","显示器","平板","打印机"]
    statuses = ["已完成","待付款","已取消","退款中","待发货"]
    methods  = ["微信支付","支付宝","银行卡","信用卡"]
    for i in range(1, 2001):
        qty = random.randint(1,5)
        rows.append({
            "order_id": f"ORD{i:06d}",
            "user_id": random.randint(1,500),
            "product": random.choice(products),
            "quantity": qty,
            "amount": round(random.uniform(29.9, 9999.0) * qty, 2),
            "status": random.choice(statuses),
            "order_date": (datetime(2024,1,1) + timedelta(days=random.randint(0,545))).strftime("%Y-%m-%d %H:%M:%S"),
            "pay_method": random.choice(methods),
        })
    write_csv(f"{DATA_DIR}/orders.csv", rows, headers)
    return headers

def gen_logs_csv():
    """场景: 系统日志 (大数据量, 用于性能测试 + 字段拼接)"""
    headers = ["log_id","timestamp","level","service","message","duration_ms","trace_id"]
    levels  = ["INFO","WARN","ERROR","DEBUG"]
    services = ["api-gateway","user-service","order-service","payment-service","notification"]
    rows = []
    for i in range(1, 10001):
        rows.append({
            "log_id": i,
            "timestamp": (datetime(2025,1,1) + timedelta(seconds=random.randint(0, 365*24*3600))).isoformat(),
            "level": random.choices(levels, weights=[60,20,5,15])[0],
            "service": random.choice(services),
            "message": f"log_message_{i}_" + random.choice(["request ok","timeout","retry","cache hit","db query"]),
            "duration_ms": random.randint(1, 5000),
            "trace_id": f"trace_{random.randint(100000,999999)}",
        })
    write_csv(f"{DATA_DIR}/system_logs.csv", rows, headers)
    return headers

def gen_sales_csv():
    """场景: 销售数据 (用于 XML/JSON 转换 + 聚合测试)"""
    headers = ["sale_id","region","product_category","sales_person","amount","quantity","sale_date","channel"]
    regions  = ["华北","华东","华南","西南","西北","东北"]
    categories = ["电子产品","家具","服装","食品","图书"]
    channels = ["线上","线下","直播","跨境"]
    names = ["张三","李四","王五","赵六","钱七","孙八","周九"]
    rows = []
    for i in range(1, 3001):
        rows.append({
            "sale_id": f"S{i:05d}",
            "region": random.choice(regions),
            "product_category": random.choice(categories),
            "sales_person": random.choice(names),
            "amount": round(random.uniform(100, 50000), 2),
            "quantity": random.randint(1, 100),
            "sale_date": (datetime(2025,1,1) + timedelta(days=random.randint(0,180))).strftime("%Y-%m-%d"),
            "channel": random.choice(channels),
        })
    write_csv(f"{DATA_DIR}/sales.csv", rows, headers)
    return headers

# ============ 3. Excel 测试数据 ============

def gen_excel():
    """生成带多 sheet 的 Excel 测试文件"""
    try:
        import openpyxl
    except ImportError:
        print("  ⚠️ openpyxl 未安装, 跳过 Excel 生成")
        return

    wb = openpyxl.Workbook()

    # Sheet1: 员工信息
    ws1 = wb.active
    ws1.title = "员工信息"
    ws1.append(["员工ID","姓名","部门","职位","薪资","入职日期"])
    depts = [("张三","技术部","高级工程师",25000),("李四","市场部","市场经理",18000),
             ("王五","销售部","销售主管",22000),("赵六","人事部","HR经理",15000),
             ("钱七","财务部","财务主管",20000),("孙八","技术部","架构师",35000)]
    for i,(name,dept,title,salary) in enumerate(depts,1):
        ws1.append([i,name,dept,title,salary,f"2020-{random.randint(1,12):02d}-{random.randint(1,28):02d}"])

    # Sheet2: 产品销售
    ws2 = wb.create_sheet("产品销售")
    ws2.append(["产品ID","产品名称","类别","单价","库存"])
    products = [("P001","笔记本电脑","电子",5999,200),("P002","机械键盘","电子",899,500),
                ("P003","降噪耳机","电子",1299,350),("P004","4K显示器","电子",2999,150),
                ("P005","人体工学椅","家具",2499,80),("P006","台灯","家具",299,300)]
    for p in products:
        ws2.append(p)

    # Sheet3: 月度 KPI
    ws3 = wb.create_sheet("月度KPI")
    ws3.append(["月份","营业额","订单数","新用户数","退款率"])
    for m in range(1,13):
        ws3.append([f"2025-{m:02d}", round(random.uniform(500000, 2000000),2),
                    random.randint(3000,8000), random.randint(100,500),
                    f"{random.uniform(1,8):.1f}%"])

    path = f"{DATA_DIR}/sample_data.xlsx"
    wb.save(path)
    fs = fmt_size(os.path.getsize(path))
    print(f"  ✅ sample_data.xlsx (3 sheets, {fs})")

# ============ 4. XML 测试数据 ============

def gen_xml():
    """生成 XML 样本 (用于 XML<->JSON 转换测试)"""
    orders_xml = """<?xml version="1.0" encoding="UTF-8"?>
<orders>
  <order id="ORD000001">
    <customer>张三</customer>
    <items>
      <item sku="NB-001" qty="2">
        <name>笔记本电脑 Pro</name>
        <price>6999.00</price>
      </item>
      <item sku="MS-002" qty="1">
        <name>无线鼠标</name>
        <price>199.00</price>
      </item>
    </items>
    <total>14197.00</total>
    <status>已完成</status>
  </order>
  <order id="ORD000002">
    <customer>李四</customer>
    <items>
      <item sku="KB-001" qty="1">
        <name>机械键盘</name>
        <price>899.00</price>
      </item>
    </items>
    <total>899.00</total>
    <status>待发货</status>
  </order>
  <order id="ORD000003">
    <customer>王五</customer>
    <items>
      <item sku="MT-001" qty="1">
        <name>27寸4K显示器</name>
        <price>2999.00</price>
      </item>
      <item sku="HP-002" qty="1">
        <name>降噪耳机</name>
        <price>1299.00</price>
      </item>
      <item sku="PD-001" qty="1">
        <name>移动电源</name>
        <price>199.00</price>
      </item>
    </items>
    <total>4497.00</total>
    <status>已完成</status>
  </order>
</orders>"""

    sales_xml = """<?xml version="1.0" encoding="UTF-8"?>
<salesReport period="2025-Q1">
  <summary>
    <totalRevenue>1258000.00</totalRevenue>
    <totalOrders>4523</totalOrders>
    <avgOrderValue>278.15</avgOrderValue>
  </summary>
  <regionalSales>
    <region name="华北">
      <revenue>358000.00</revenue>
      <orders>1280</orders>
    </region>
    <region name="华东">
      <revenue>425000.00</revenue>
      <orders>1520</orders>
    </region>
    <region name="华南">
      <revenue>312000.00</revenue>
      <orders>1120</orders>
    </region>
    <region name="西南">
      <revenue>163000.00</revenue>
      <orders>603</orders>
    </region>
  </regionalSales>
</salesReport>"""

    for name, content in [("orders.xml", orders_xml), ("sales_report.xml", sales_xml)]:
        path = f"{DATA_DIR}/{name}"
        with open(path, 'w', encoding='utf-8') as f:
            f.write(content)
        fs = fmt_size(os.path.getsize(path))
        print(f"  ✅ {name} ({fs})")

# ============ 5. JSON 测试数据 ============

def gen_json():
    """生成 JSON 样本 (用于 Kafka/MQ 模拟 + 嵌套结构测试)"""
    # 用户事件流
    events = []
    for i in range(1, 501):
        events.append({
            "event_id": f"evt_{i:06d}",
            "event_type": random.choice(["page_view","click","purchase","login","logout","search"]),
            "user_id": random.randint(1, 500),
            "timestamp": (datetime(2025,6,1) + timedelta(seconds=random.randint(0, 30*86400))).isoformat(),
            "payload": {
                "page": random.choice(["/home","/products","/cart","/orders","/profile"]),
                "duration_ms": random.randint(100, 30000),
                "referrer": random.choice(["direct","google","wechat","douyin","email"]),
            }
        })
    path = f"{DATA_DIR}/user_events.json"
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(events, f, ensure_ascii=False, indent=2)
    fs = fmt_size(os.path.getsize(path))
    print(f"  ✅ user_events.json ({len(events):,} 条, {fs})")

    # 配置信息 (扁平结构)
    config = {
        "app_name": "数据流任务管理系统",
        "version": "1.0.0",
        "environment": "test",
        "datasources": {
            "csv": {"enabled": True, "max_file_size_mb": 50},
            "excel": {"enabled": True, "max_file_size_mb": 30},
            "mysql": {"enabled": False, "host": "localhost:3306"},
            "kafka": {"enabled": False, "bootstrap_servers": "localhost:9092"},
        },
        "flink": {
            "cluster_host": "localhost",
            "cluster_port": 8081,
            "default_parallelism": 2,
            "checkpoint_interval_ms": 60000,
        },
        "plugins": {
            "dir": "./plugins",
            "auto_reload_sec": 30,
        }
    }
    path = f"{DATA_DIR}/system_config.json"
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(config, f, ensure_ascii=False, indent=2)
    print(f"  ✅ system_config.json        ({fmt_size(os.path.getsize(path))})")

# ============ 6. 基准测试大文件 ============

def gen_benchmark_csv(size, path):
    """生成基准测试用的 CSV 大文件 (沿用现有 benchmark.py 格式)"""
    COLUMNS = ["id", "name", "email", "phone", "address", "amount", "timestamp"]
    SAMPLE_RECORDS = 1000

    samples = []
    for i in range(SAMPLE_RECORDS):
        samples.append({
            "id": i,
            "name": f"user_{i}",
            "email": f"user{i}@example.com",
            "phone": f"138{i:08d}",
            "address": f"address_{i % 1000}",
            "amount": round(1000 + i * 1.5, 2),
            "timestamp": datetime.now().isoformat()
        })

    start = time.time()
    with open(path, 'w', newline='', encoding='utf-8') as f:
        w = csv.DictWriter(f, fieldnames=COLUMNS)
        w.writeheader()
        for i in range(size):
            w.writerow(samples[i % SAMPLE_RECORDS])
    elapsed = time.time() - start
    fs = fmt_size(os.path.getsize(path))
    rate = int(size / elapsed) if elapsed > 0 else 0
    print(f"  ✅ {os.path.basename(path)} ({size:,} 行, {fs}, 耗时 {elapsed:.1f}s, {rate:,} 行/秒)")

# ============ 7. DAG JSON 场景 ============

def gen_dag_json():
    """生成各种测试场景的 DAG JSON 文件"""

    dags = {
        "01-csv-input-csv-output.json": {
            "jobName": "CSV 透传测试",
            "parallelism": 2,
            "nodes": [
                {"id":"csv_in","type":"csv_input","label":"CSV 输入","params":{"path":f"{DATA_DIR}/users.csv","delimiter":",","hasHeader":True},"x":100,"y":250},
                {"id":"csv_out","type":"csv_output","label":"CSV 输出","params":{"path":f"{DATA_DIR}/output_users.csv","delimiter":","},"x":400,"y":250},
            ],
            "edges":[{"id":"e1","source":"csv_in","target":"csv_out"}]
        },
        "02-csv-concat-csv.json": {
            "jobName": "字段拼接测试",
            "parallelism": 2,
            "nodes": [
                {"id":"csv_in","type":"csv_input","label":"CSV 输入","params":{"path":f"{DATA_DIR}/users.csv","delimiter":",","hasHeader":True},"x":80,"y":250},
                {"id":"concat","type":"field_concat","label":"字段拼接","params":{"fields":["name","department","city"],"separator":" - ","newFieldName":"简介"},"x":350,"y":250},
                {"id":"csv_out","type":"csv_output","label":"CSV 输出","params":{"path":f"{DATA_DIR}/output_concat.csv","delimiter":","},"x":620,"y":250},
            ],
            "edges":[{"id":"e1","source":"csv_in","target":"concat"},{"id":"e2","source":"concat","target":"csv_out"}]
        },
        "03-csv-xml-convert-csv.json": {
            "jobName": "XML<->JSON 转换测试",
            "parallelism": 1,
            "nodes": [
                {"id":"csv_in","type":"csv_input","label":"CSV 输入","params":{"path":f"{DATA_DIR}/sales.csv","delimiter":",","hasHeader":True},"x":80,"y":250},
                {"id":"convert","type":"xml_json","label":"XML<->JSON","params":{"direction":"xml2json","sourceField":"payload","targetField":"result"},"x":350,"y":250},
                {"id":"csv_out","type":"csv_output","label":"CSV 输出","params":{"path":f"{DATA_DIR}/output_converted.csv","delimiter":","},"x":620,"y":250},
            ],
            "edges":[{"id":"e1","source":"csv_in","target":"convert"},{"id":"e2","source":"convert","target":"csv_out"}]
        },
        "04-orders-full-pipeline.json": {
            "jobName": "订单全链路处理",
            "parallelism": 2,
            "nodes": [
                {"id":"orders_in","type":"csv_input","label":"订单输入","params":{"path":f"{DATA_DIR}/orders.csv","delimiter":",","hasHeader":True},"x":80,"y":200},
                {"id":"concat","type":"field_concat","label":"字段拼接","params":{"fields":["product","status"],"separator":"-","newFieldName":"product_status"},"x":330,"y":200},
                {"id":"output","type":"csv_output","label":"结果输出","params":{"path":f"{DATA_DIR}/output_orders_full.csv","delimiter":","},"x":580,"y":200},
            ],
            "edges":[{"id":"e1","source":"orders_in","target":"concat"},{"id":"e2","source":"concat","target":"output"}]
        },
        "05-system-logs-analysis.json": {
            "jobName": "系统日志分析",
            "parallelism": 4,
            "nodes": [
                {"id":"logs_in","type":"csv_input","label":"日志输入","params":{"path":f"{DATA_DIR}/system_logs.csv","delimiter":",","hasHeader":True},"x":80,"y":250},
                {"id":"concat","type":"field_concat","label":"字段拼接","params":{"fields":["service","level","message"],"separator":" | ","newFieldName":"summary"},"x":350,"y":250},
                {"id":"csv_out","type":"csv_output","label":"日志输出","params":{"path":f"{DATA_DIR}/output_logs.csv","delimiter":","},"x":620,"y":250},
            ],
            "edges":[{"id":"e1","source":"logs_in","target":"concat"},{"id":"e2","source":"concat","target":"csv_out"}]
        },
        "06-sales-by-region.json": {
            "jobName": "销售区域分析",
            "parallelism": 2,
            "nodes": [
                {"id":"sales_in","type":"csv_input","label":"销售输入","params":{"path":f"{DATA_DIR}/sales.csv","delimiter":",","hasHeader":True},"x":80,"y":250},
                {"id":"concat","type":"field_concat","label":"拼接区域信息","params":{"fields":["region","product_category","sales_person"],"separator":"/","newFieldName":"region_category"},"x":350,"y":250},
                {"id":"csv_out","type":"csv_output","label":"销售输出","params":{"path":f"{DATA_DIR}/output_sales_region.csv","delimiter":","},"x":620,"y":250},
            ],
            "edges":[{"id":"e1","source":"sales_in","target":"concat"},{"id":"e2","source":"concat","target":"csv_out"}]
        },
    }

    for name, dag in dags.items():
        # 替换相对路径为绝对路径（用于后端运行时）
        dag_json = json.dumps(dag, ensure_ascii=False, indent=2)
        path = f"{DAG_DIR}/{name}"
        with open(path, 'w', encoding='utf-8') as f:
            f.write(dag_json)
        fs = fmt_size(os.path.getsize(path))
        desc = dag["jobName"]
        print(f"  ✅ {name}  ({desc}, {fs})")

# ============ 8. 综合信息文件 ============

def gen_readme():
    """生成测试数据说明"""
    content = """# 📊 测试数据资源说明

> 由 gen_all_test_data.py 自动生成
> 生成时间: %s

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
""" % datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    path = f"{DATA_DIR}/README.md"
    with open(path, 'w', encoding='utf-8') as f:
        f.write(content.lstrip())
    print(f"  ✅ README.md (数据说明文档)")

# ============ Main ============

if __name__ == "__main__":
    print("=" * 55)
    print(" 🗂️  数据流任务管理系统 — 测试数据生成器")
    print("=" * 55)

    t0 = time.time()

    print("\n📁 生成 CSV 数据集...")
    gen_users_csv()
    gen_orders_csv()
    gen_logs_csv()
    gen_sales_csv()

    print("\n📗 生成 Excel 测试文件...")
    gen_excel()

    print("\n📄 生成 XML 样本...")
    gen_xml()

    print("\n📋 生成 JSON 样本...")
    gen_json()

    print("\n🏗️  生成 DAG JSON 测试场景...")
    gen_dag_json()

    print("\n📖 生成数据说明文档...")
    gen_readme()

    elapsed = time.time() - t0
    print(f"\n{'=' * 55}")
    print(f" ✅ 全部完成! 耗时 {elapsed:.1f} 秒")
    print(f" 📁 数据目录: {DATA_DIR}/")
    print(f" 📁 DAG 目录: {DAG_DIR}/")
    print(f" 📄 说明文档: {DATA_DIR}/README.md")
    print("=" * 55)

    total_size = sum(os.path.getsize(os.path.join(dp, f)) for dp,_,fn in os.walk(DATA_DIR) for f in fn) if os.path.exists(DATA_DIR) else 0
    total_size += sum(os.path.getsize(os.path.join(dp, f)) for dp,_,fn in os.walk(DAG_DIR) for f in fn) if os.path.exists(DAG_DIR) else 0
    print(f" 💾 总数据量: {fmt_size(total_size)}")
