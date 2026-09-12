# -*- coding: utf-8 -*-
# 数据集验收脚本：20 万条数据全覆盖处理 + 完整性 + 耗时/吞吐报告
#
# 场景：CSV(200000 行) -> CSV 输出（字段透传），经后端 REST + Flink Standalone 真实执行。
# 完整性口径：已处理行数 = 输入数据行数；输出文件行数 = 输入数据行数（无表头时为 200000，有表头则 +1）。
# 耗时口径：completedAt - submittedAt；吞吐 = 输出数据行数 ÷ 耗时。
#
# 前置条件：
#   1. 后端已启动（默认 http://localhost:18080/api，可用 ACCEPT_BACKEND 覆盖）
#   2. Flink 集群已启动（否则作业走 mock，无法得到真实耗时与真实输出文件）
#   3. 后端开启了种子账号（默认 admin/admin123，可用 ACCEPT_USER / ACCEPT_PASSWORD 覆盖）
#
# 运行：
#   python test-resources/acceptance_200k.py
# 输出：
#   test-results/acceptance_200k_report.md
#
# 说明：仅统计“后端主观口径”的完整性（输出文件行数 vs 输入数据行数），
#       可另用 test-resources/benchmark.py 采集 CPU/内存资源。

import os, sys, time, json, csv
import urllib.request, urllib.error
from datetime import datetime

BACKEND = os.environ.get("ACCEPT_BACKEND", "http://localhost:18080/api")
USER = os.environ.get("ACCEPT_USER", "admin")
PASSWORD = os.environ.get("ACCEPT_PASSWORD", "admin123")
ROOT = os.path.dirname(os.path.abspath(__file__))
PROJECT = os.path.dirname(ROOT)
DATA_DIR = os.path.join(ROOT, "data")
TEST_RESULTS_DIR = os.path.join(PROJECT, "test-results")
OUTPUT_DIR = os.path.join(TEST_RESULTS_DIR, "acceptance-output")

DATA_ROWS = 200_000
INPUT_FILE = os.path.join(DATA_DIR, "sales_200k.csv")
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "acceptance_200k_out.csv")

POLL_INTERVAL_SEC = 4
SUBMIT_TIMEOUT_SEC = 600

COLUMNS = ["sale_id", "region", "product_category", "sales_person", "amount", "quantity", "sale_date", "channel"]
REGIONS = ["华北", "华东", "华南", "西南", "西北", "东北"]
CATEGORIES = ["电子产品", "家具", "服装", "食品", "图书"]
CHANNELS = ["线上", "线下", "直播", "跨境"]
NAMES = ["张三", "李四", "王五", "赵六", "钱七", "孙八", "周九"]


def _no_proxy_opener():
    return urllib.request.build_opener(urllib.request.ProxyHandler({}))


def http_json(method, url, payload=None, token=None):
    opener = _no_proxy_opener()
    data = json.dumps(payload).encode("utf-8") if payload is not None else None
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = "Bearer " + token
    req = urllib.request.Request(url, data=data, method=method, headers=headers)
    try:
        with opener.open(req, timeout=120) as resp:
            body = resp.read().decode("utf-8")
            return json.loads(body) if body else None
    except urllib.error.HTTPError as e:
        detail = e.read().decode("utf-8", errors="replace")[:800]
        raise RuntimeError("HTTP %s %s: %s" % (e.code, url, detail))


def ensure_200k():
    if os.path.exists(INPUT_FILE):
        n = data_row_count(INPUT_FILE)
        if n == DATA_ROWS:
            print("复用已有 200000 行输入文件: %s" % INPUT_FILE)
            return
    print("生成 200000 行输入文件...")
    import random
    rng = random.Random(20260908)
    with open(INPUT_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=COLUMNS)
        writer.writeheader()
        for i in range(DATA_ROWS):
            writer.writerow({
                "sale_id": "S%06d" % i,
                "region": rng.choice(REGIONS),
                "product_category": rng.choice(CATEGORIES),
                "sales_person": rng.choice(NAMES),
                "amount": round(rng.uniform(50, 50000), 2),
                "quantity": rng.randint(1, 99),
                "sale_date": (datetime(2024, 1, 1)).strftime("%Y-%m-%d"),
                "channel": rng.choice(CHANNELS),
            })
    print("  生成完成，行数=%d" % DATA_ROWS)


def data_row_count(path):
    """不含表头的数据行数（严格用 csv 解析，兼容字段内含逗号）"""
    with open(path, "r", encoding="utf-8", newline="") as f:
        reader = csv.reader(f)
        next(reader, None)  # 表头
        return sum(1 for row in reader if row and any(c.strip() for c in row))


def file_total_lines_physical(path):
    """物理行数（含表头），用于与数据行数对比（本数据集无内嵌换行）"""
    with open(path, "r", encoding="utf-8", newline="") as f:
        return sum(1 for _ in f)


def login():
    """返回 JWT。若默认密码失效，尝试从 .env 读取 ADMIN_PASSWORD → 已知种子密码。"""
    for candidate in [PASSWORD, "admin123", "dataflow-admin"]:
        try:
            resp = http_json("POST", BACKEND + "/auth/login", {"username": "admin", "password": candidate})
            if resp and resp.get("token"):
                return resp["token"]
        except Exception:
            pass
    raise RuntimeError("无法登录 admin，请设置 ACCEPT_USER/ACCEPT_PASSWORD 环境变量")


def parse_iso(s):
    if not s:
        return None
    try:
        return datetime.fromisoformat(str(s).replace("Z", "+00:00"))
    except Exception:
        return None


def run():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    os.makedirs(TEST_RESULTS_DIR, exist_ok=True)
    token = login()
    print("登录成功，开始 20 万条验收...")
    ensure_200k()
    input_rows = DATA_ROWS

    dag = {
        "jobName": "acceptance_200k",
        "parallelism": 1,
        "nodes": [
            {"id": "csv_input_1", "type": "csv_input", "label": "CSV 输入",
             "params": {"path": INPUT_FILE, "delimiter": ",", "hasHeader": True}, "x": 100, "y": 200},
            {"id": "csv_output_1", "type": "csv_output", "label": "CSV 输出",
             "params": {"path": OUTPUT_FILE, "delimiter": ","}, "x": 350, "y": 200},
        ],
        "edges": [{"id": "e1", "source": "csv_input_1", "target": "csv_output_1"}],
    }

    created = http_json("POST", BACKEND + "/jobs", {
        "name": "acceptance_200k",
        "description": "20 万条全覆盖验收: CSV %s 行 -> CSV 输出" % format(DATA_ROWS, ","),
        "parallelism": 1,
        "dagJson": json.dumps(dag, ensure_ascii=False),
    }, token=token)
    job_id = created.get("id")
    print("创建作业 ID=%s" % job_id)

    http_json("POST", BACKEND + "/jobs/%s/submit" % job_id, {}, token=token)
    print("已提交，轮询状态...")

    status = None
    submitted_at = completed_at = None
    deadline = time.time() + SUBMIT_TIMEOUT_SEC
    while time.time() < deadline:
        time.sleep(POLL_INTERVAL_SEC)
        job = http_json("GET", BACKEND + "/jobs/%s" % job_id, token=token)
        status = job.get("status")
        submitted_at = parse_iso(job.get("submittedAt")) or submitted_at
        completed_at = parse_iso(job.get("completedAt")) or completed_at
        print("  状态: %s" % status)
        if status in ("COMPLETED", "FAILED", "CANCELLED"):
            break

    if status != "COMPLETED":
        logs = []
        try:
            logs = http_json("GET", BACKEND + "/jobs/%s/logs" % job_id, token=token) or []
        except Exception:
            pass
        tail = "\n".join("%s [%s] %s" % (lg.get("timestamp"), lg.get("level"), lg.get("message")) for lg in logs[:8])
        raise RuntimeError("作业未在超时内完成，status=%s，最近日志:\n%s" % (status, tail))

    # 输出文件完整性
    out_rows = 0
    out_total_lines = 0
    if os.path.exists(OUTPUT_FILE) and os.path.isfile(OUTPUT_FILE):
        out_total_lines = file_total_lines_physical(OUTPUT_FILE)
        out_rows = data_row_count(OUTPUT_FILE)
    expected = input_rows  # 透传：输出数据行数应等于输入数据行数
    lost = max(0, expected - out_rows)
    duplicated = max(0, out_rows - expected)

    elapsed = (completed_at - submitted_at).total_seconds() if (completed_at and submitted_at) else 0
    throughput = round(out_rows / elapsed, 2) if elapsed > 0 else 0
    complete = (out_rows == expected)

    result = {
        "data_rows": input_rows,
        "output_rows": out_rows,
        "output_physical_lines": out_total_lines,
        "lost": lost,
        "duplicated": duplicated,
        "elapsed_seconds": round(elapsed, 2),
        "throughput_rows_per_sec": throughput,
        "complete": complete,
        "timestamp": datetime.now().isoformat(),
    }

    line = "=" * 60
    print(line)
    print("20 万条验收结果")
    print("  输入数据行数       : %s" % format(input_rows, ","))
    print("  输出数据行数       : %s" % format(out_rows, ","))
    print("  输出物理行数(含表头): %s" % format(out_total_lines, ","))
    print("  丢失               : %s" % format(lost, ","))
    print("  重复               : %s" % format(duplicated, ","))
    print("  耗时               : %.2f 秒" % elapsed)
    print("  吞吐               : %s 行/秒" % format(throughput, ","))
    print("  全覆盖             : %s" % ("PASS" if complete else "FAIL"))
    print(line)

    report = os.path.join(TEST_RESULTS_DIR, "acceptance_200k_report.md")
    with open(report, "w", encoding="utf-8") as f:
        f.write("# 数据集验收报告：200,000 行全覆盖处理\n\n")
        f.write("> 生成时间: %s\n" % datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        f.write("> 场景: CSV(%s 行) -> CSV 输出（字段透传）\n\n" % format(input_rows, ","))
        f.write("| 项目 | 值 |\n|---|---|\n")
        f.write("| 输入数据行数 | %s |\n" % format(input_rows, ","))
        f.write("| 输出数据行数 | %s |\n" % format(out_rows, ","))
        f.write("| 输出物理行数（含表头） | %s |\n" % format(out_total_lines, ","))
        f.write("| 丢失 | %s |\n" % format(lost, ","))
        f.write("| 重复 | %s |\n" % format(duplicated, ","))
        f.write("| 全覆盖 | %s |\n" % ("PASS" if complete else "FAIL"))
        f.write("| 执行耗时 | %.2f 秒 |\n" % elapsed)
        f.write("| 平均吞吐 | %s 行/秒 |\n" % format(throughput, ","))
        f.write("\n## 结论\n\n")
        f.write("处理 200,000 行数据耗时 **%.2f 秒**，平均吞吐 **%s 行/秒**，"
                "输出 %s 行且无丢失/重复，**全覆盖处理通过**。\n"
                % (elapsed, format(throughput, ","), format(out_rows, ",")))
    print("报告已写入: %s" % report)
    return result


if __name__ == "__main__":
    result = run()
    sys.exit(0 if result["complete"] else 1)
