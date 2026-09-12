#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""多数据源交叉验收：真实提交 Flink 作业，校验各数据源之间互转结果。

覆盖：
  1. PostgreSQL -> CSV
  2. CSV -> PostgreSQL（自动建表 + 回读校验）
  3. PostgreSQL -> HDFS
  4. HDFS -> CSV
  5. CSV -> Redis Lookup -> CSV
  6. CSV -> PostgreSQL -> HDFS（链路）

用法：
    python test-resources/multisource_e2e.py [--base-url http://localhost:18080]
"""
import argparse
import json
import os
import subprocess
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime

BASE = "http://localhost:18080"
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUTPUT_DIR = os.path.join(PROJECT_ROOT, "output")
REPORT = os.path.join(PROJECT_ROOT, "test-results", "multisource_e2e_report.md")

# Flink 运行在 Docker 容器内，只能读取挂载目录：./output -> /output，./test-resources -> /test-resources
docker_output = os.environ.get("E2E_DOCKER_OUTPUT", "/output")
docker_test_resources = os.environ.get("E2E_DOCKER_TEST_RESOURCES", "/test-resources")


def to_docker_path(host_path):
    """宿主机绝对路径 -> Flink 容器内可见路径。"""
    normalized = os.path.abspath(host_path).replace("\\", "/")
    root = PROJECT_ROOT.replace("\\", "/")
    if normalized.startswith(root + "/output/"):
        return docker_output + normalized[len(root) + len("/output"):]
    if normalized.startswith(root + "/test-resources/"):
        return docker_test_resources + normalized[len(root) + len("/test-resources"):]
    raise ValueError(f"路径不在容器挂载目录内，Flink 无法读取: {host_path}")

PG_USER = os.environ.get("POSTGRES_USERNAME", "postgres")
PG_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "postgres123")
# Flink 运行在 Docker 网络中，必须用容器名访问 PostgreSQL 与 HDFS；
# 后端（宿主机）→ 容器用 localhost 映射端口，两者地址不同，不能混用。
PG_DOCKER = os.environ.get("E2E_PG_DOCKER", "jdbc:postgresql://mvp-postgres:5432/dataflow")
PG_HOST_PORT = os.environ.get("E2E_PG_PORT", "5440")
HDFS_BASE = os.environ.get("E2E_HDFS_BASE", "hdfs://namenode:9000")

results = []


def http(method, path, token=None, body=None, timeout=180):
    url = BASE + path
    data = json.dumps(body).encode("utf-8") if body is not None else None
    req = urllib.request.Request(url, data=data, method=method)
    req.add_header("Content-Type", "application/json; charset=utf-8")
    if token:
        req.add_header("Authorization", "Bearer " + token)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read().decode("utf-8")
            return resp.status, (json.loads(raw) if raw.strip() else None)
    except urllib.error.HTTPError as e:
        raw = e.read().decode("utf-8", "replace")
        try:
            return e.code, json.loads(raw)
        except Exception:
            return e.code, {"raw": raw}


def login():
    status, data = http("POST", "/api/auth/login",
                        body={"username": "admin", "password": "admin123"})
    if status != 200 or not data or "token" not in data:
        raise SystemExit(f"登录失败: HTTP {status} {data}")
    return data["token"]


def csv_node(node_id, path, delimiter=","):
    return {"id": node_id, "type": "csv_input", "label": "CSV 输入",
            "params": {"path": path, "delimiter": delimiter, "hasHeader": "true"}}


def csv_out_node(node_id, path):
    return {"id": node_id, "type": "csv_output", "label": "CSV 输出",
            "params": {"path": path, "delimiter": ","}}


def edge(eid, src, dst):
    return {"id": eid, "source": src, "target": dst}


def submit_and_wait(token, name, nodes, edges, parallelism=1, timeout=420):
    """创建并提交作业，轮询到终态。返回 (status, job_id, log_tail)。"""
    dag = {"jobName": name, "parallelism": parallelism, "nodes": nodes, "edges": edges}
    status, job = http("POST", "/api/jobs", token,
                       body={"name": name, "dagJson": json.dumps(dag, ensure_ascii=False),
                             "parallelism": parallelism})
    if status not in (200, 201) or not job:
        return f"CREATE_FAILED(HTTP {status}: {job})", None, ""
    job_id = job["id"]
    status, _ = http("POST", f"/api/jobs/{job_id}/submit", token)
    if status not in (200, 204):
        return f"SUBMIT_FAILED(HTTP {status})", job_id, ""
    deadline = time.time() + timeout
    last = "UNKNOWN"
    while time.time() < deadline:
        time.sleep(5)
        _, cur = http("GET", f"/api/jobs/{job_id}", token)
        last = (cur or {}).get("status", "UNKNOWN")
        if last in ("COMPLETED", "FAILED", "CANCELLED"):
            break
    _, logs = http("GET", f"/api/jobs/{job_id}/logs", token)
    tail = ""
    if isinstance(logs, list) and logs:
        tail = " | ".join(str(l.get("message", ""))[:300] for l in logs[:3])
    return last, job_id, tail


def read_csv(path):
    """读取输出 CSV，返回 (列名, 数据行)。"""
    if not os.path.isfile(path):
        return None, None
    with open(path, encoding="utf-8-sig") as f:
        lines = [l.rstrip("\n").rstrip("\r") for l in f if l.strip()]
    if not lines:
        return None, None
    return lines[0].split(","), [l.split(",") for l in lines[1:]]


def psql(sql, db="dataflow", user=None, container="mvp-postgres"):
    user = user or PG_USER
    cmd = ["docker", "exec", container, "psql", "-U", user, "-d", db, "-t", "-A", "-F", "|", "-c", sql]
    proc = subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8", errors="replace")
    if proc.returncode != 0:
        return None, proc.stderr.strip()
    return proc.stdout.strip(), None


HDFS_CONTAINER = os.environ.get("E2E_HDFS_CONTAINER", "mvp-namenode")
# Flink/后端均在 Docker 网络内，用服务名访问 Redis
REDIS_HOST = os.environ.get("E2E_REDIS_HOST", "redis")
REDIS_PASSWORD = os.environ.get("REDIS_PASSWORD", "redis123")
HADOOP_BIN = os.environ.get("E2E_HADOOP_BIN", "/opt/hadoop/bin/hdfs")


def hdfs_cmd(args):
    cmd = ["docker", "exec", HDFS_CONTAINER, HADOOP_BIN, "dfs"] + args
    proc = subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8", errors="replace")
    return proc.returncode, proc.stdout.strip(), proc.stderr.strip()


def record(case, chain, status, detail):
    results.append({"case": case, "chain": chain, "status": status, "detail": detail})
    icon = "PASS" if status == "PASS" else ("SKIP" if status == "SKIP" else "FAIL")
    print(f"[{icon}] {case}: {chain} -> {status} | {detail}", flush=True)


def main():
    global BASE
    ap = argparse.ArgumentParser()
    ap.add_argument("--base-url", default=BASE)
    args = ap.parse_args()
    BASE = args.base_url
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)

    print("=== 登录 ===", flush=True)
    token = login()
    print("登录成功", flush=True)

    # ---------- 1. PostgreSQL -> CSV ----------
    out1 = os.path.join(OUTPUT_DIR, "e2e_pg_to_csv.csv")
    status, jid, log = submit_and_wait(token, "E2E-PG-01 PostgreSQL->CSV", [
        {"id": "pg_in_1", "type": "pg_input", "label": "PG 输入",
         "params": {"url": PG_DOCKER,
                    "schema": "public", "table": "students",
                    "username": PG_USER, "password": PG_PASSWORD}},
        csv_out_node("csv_out_1", to_docker_path(out1)),
    ], [edge("e1", "pg_in_1", "csv_out_1")])
    cols, rows = read_csv(out1)
    if status == "COMPLETED" and rows is not None and len(rows) == 3:
        record("E2E-PG-01", "PostgreSQL → CSV", "PASS",
               f"{len(rows)} 行, 列={cols}, 首行={rows[0]}")
    else:
        record("E2E-PG-01", "PostgreSQL → CSV", "FAIL",
               f"state={status} rows={0 if rows is None else len(rows)} log={log}")

    # ---------- 2. CSV -> PostgreSQL（CREATE_IF_MISSING） ----------
    src_csv = os.path.join(PROJECT_ROOT, "test-resources", "data", "sales.csv")
    psql("DROP TABLE IF EXISTS e2e_csv_to_pg;")
    status, jid, log = submit_and_wait(token, "E2E-PG-02 CSV->PostgreSQL", [
        csv_node("csv_in_1", to_docker_path(src_csv)),
        {"id": "pg_out_1", "type": "pg_output", "label": "PG 输出",
         "params": {"url": PG_DOCKER,
                    "schema": "public", "table": "e2e_csv_to_pg",
                    "username": PG_USER, "password": PG_PASSWORD,
                    "createTablePolicy": "CREATE_IF_MISSING", "writeMode": "append",
                    "batchSize": 500}},
    ], [edge("e1", "csv_in_1", "pg_out_1")])
    count, err = psql("SELECT count(*) FROM e2e_csv_to_pg;")
    colnames, cerr = psql("SELECT column_name FROM information_schema.columns "
                          "WHERE table_name='e2e_csv_to_pg' ORDER BY ordinal_position;")
    if status == "COMPLETED" and count and int(count) == 3000:
        record("E2E-PG-02", "CSV → PostgreSQL(自动建表)", "PASS",
               f"{count} 行, 列={colnames.replace(chr(10), ',') if colnames else '?'}")
    else:
        record("E2E-PG-02", "CSV → PostgreSQL(自动建表)", "FAIL",
               f"state={status} count={count} err={err or cerr} log={log}")

    # ---------- 3. PostgreSQL -> HDFS ----------
    hdfs_out_dir = HDFS_BASE + "/e2e/pg_to_hdfs"
    hdfs_cmd(["-rm", "-r", "-f", "/e2e/pg_to_hdfs"])
    status, jid, log = submit_and_wait(token, "E2E-HDFS-01 PostgreSQL->HDFS", [
        {"id": "pg_in_2", "type": "pg_input", "label": "PG 输入",
         "params": {"url": PG_DOCKER,
                    "schema": "public", "table": "students",
                    "username": PG_USER, "password": PG_PASSWORD}},
        {"id": "hdfs_out_1", "type": "hdfs_output", "label": "HDFS 输出",
         "params": {"path": hdfs_out_dir, "delimiter": ","}},
    ], [edge("e1", "pg_in_2", "hdfs_out_1")])
    rc, listing, _ = hdfs_cmd(["-ls", "/e2e/pg_to_hdfs"])
    part_exists = "part-" in listing
    if status == "COMPLETED" and part_exists:
        record("E2E-HDFS-01", "PostgreSQL → HDFS", "PASS",
               f"HDFS 目录含 part 文件: {listing.splitlines()[0][:120] if listing else ''}")
    else:
        record("E2E-HDFS-01", "PostgreSQL → HDFS", "FAIL",
               f"state={status} listing={listing} log={log}")

    # ---------- 4. HDFS -> CSV ----------
    hdfs_in = HDFS_BASE + "/e2e/hdfs_to_csv/input.csv"
    local_stage = os.path.join(OUTPUT_DIR, "e2e_hdfs_input.csv")
    with open(local_stage, "w", encoding="utf-8") as f:
        f.write("id,name,college\n1,张三,计算机学院\n2,李四,软件学院\n3,Alice,\n")
    hdfs_cmd(["-rm", "-r", "-f", "/e2e/hdfs_to_csv"])
    hdfs_cmd(["-mkdir", "-p", "/e2e/hdfs_to_csv"])
    subprocess.run(["docker", "cp", local_stage, f"{HDFS_CONTAINER}:/tmp/e2e_hdfs_input.csv"],
                   capture_output=True, text=True)
    hdfs_cmd(["-put", "-f", "/tmp/e2e_hdfs_input.csv", "/e2e/hdfs_to_csv/input.csv"])
    out4 = os.path.join(OUTPUT_DIR, "e2e_hdfs_to_csv.csv")
    status, jid, log = submit_and_wait(token, "E2E-HDFS-02 HDFS->CSV", [
        {"id": "hdfs_in_1", "type": "hdfs_input", "label": "HDFS 输入",
         "params": {"path": hdfs_in, "delimiter": ",",
                    "fieldsConfig": json.dumps(
                        [{"name": "id", "type": "INT"},
                         {"name": "name", "type": "STRING"},
                         {"name": "college", "type": "STRING"}])}},
        csv_out_node("csv_out_2", to_docker_path(out4)),
    ], [edge("e1", "hdfs_in_1", "csv_out_2")])
    cols4, rows4 = read_csv(out4)
    if status == "COMPLETED" and rows4 is not None and len(rows4) == 3:
        record("E2E-HDFS-02", "HDFS → CSV", "PASS",
               f"{len(rows4)} 行, 列={cols4}, 首行={rows4[0]}")
    else:
        record("E2E-HDFS-02", "HDFS → CSV", "FAIL",
               f"state={status} rows={0 if rows4 is None else len(rows4)} log={log}")

    # ---------- 5. CSV -> Redis Lookup -> CSV ----------
    out5 = os.path.join(OUTPUT_DIR, "e2e_redis_lookup.csv")
    redis_csv = os.path.join(OUTPUT_DIR, "e2e_redis_src.csv")
    with open(redis_csv, "w", encoding="utf-8") as f:
        f.write("sale_id,region\n1,华东\n2,华北\n3,华南\n")
    status, jid, log = submit_and_wait(token, "E2E-REDIS-01 Redis Lookup", [
        csv_node("csv_in_5", to_docker_path(redis_csv)),
        {"id": "redis_1", "type": "redis_lookup", "label": "Redis 富化",
         "params": {"host": REDIS_HOST, "port": 6379, "password": REDIS_PASSWORD,
                    "keyField": "sale_id", "keyPrefix": "sale:",
                    "targetField": "redis_value"}},
        csv_out_node("csv_out_3", to_docker_path(out5)),
    ], [edge("e1", "csv_in_5", "redis_1"), edge("e2", "redis_1", "csv_out_3")])
    cols5, rows5 = read_csv(out5)
    hit = None
    if rows5:
        try:
            idx = cols5.index("redis_value")
            hit = rows5[0][idx] if idx < len(rows5[0]) else None
        except (ValueError, IndexError):
            hit = None
    if status == "COMPLETED" and hit == "华东重点订单":
        record("E2E-REDIS-01", "CSV → Redis Lookup → CSV", "PASS",
               f"redis_value={hit}（真实 Redis 值，非 key 字符串）")
    else:
        record("E2E-REDIS-01", "CSV → Redis Lookup → CSV", "FAIL",
               f"state={status} redis_value={hit!r} cols={cols5} log={log}")

    # ---------- 6. CSV -> PostgreSQL（带转换链） -> HDFS ----------
    psql("DROP TABLE IF EXISTS e2e_chain;")
    hdfs_cmd(["-rm", "-r", "-f", "/e2e/chain_hdfs"])
    status6a, jid6a, log6a = submit_and_wait(token, "E2E-PG-03 CSV->PG(转换)", [
        csv_node("csv_in_6", to_docker_path(src_csv)),
        {"id": "ff_1", "type": "field_filter", "label": "字段过滤",
         "params": {"fields": "sale_id,region,amount"}},
        {"id": "pg_out_6", "type": "pg_output", "label": "PG 输出",
         "params": {"url": PG_DOCKER,
                    "schema": "public", "table": "e2e_chain",
                    "username": PG_USER, "password": PG_PASSWORD,
                    "createTablePolicy": "CREATE_IF_MISSING"}},
    ], [edge("e1", "csv_in_6", "ff_1"), edge("e2", "ff_1", "pg_out_6")])
    count6, err6 = psql("SELECT count(*) FROM e2e_chain;")
    cols6, _ = psql("SELECT column_name FROM information_schema.columns "
                    "WHERE table_name='e2e_chain' ORDER BY ordinal_position;")
    ok6 = status6a == "COMPLETED" and count6 == "3000" and cols6 and "amount" in cols6
    record("E2E-CROSS-01", "CSV → 字段过滤 → PostgreSQL", "PASS" if ok6 else "FAIL",
           f"state={status6a} count={count6} 列={cols6.replace(chr(10), ',') if cols6 else '?'} log={log6a}")

    ok7 = False
    detail7 = "前置 PostgreSQL 无数据"
    if count6 == "3000":
        status7, jid7, log7 = submit_and_wait(token, "E2E-CROSS-02 PG->HDFS(链路)", [
            {"id": "pg_in_7", "type": "pg_input", "label": "PG 输入",
             "params": {"url": PG_DOCKER,
                        "schema": "public", "table": "e2e_chain",
                        "username": PG_USER, "password": PG_PASSWORD}},
            {"id": "hdfs_out_7", "type": "hdfs_output", "label": "HDFS 输出",
             "params": {"path": HDFS_BASE + "/e2e/chain_hdfs", "delimiter": ","}},
        ], [edge("e1", "pg_in_7", "hdfs_out_7")])
        rc, listing, _ = hdfs_cmd(["-ls", "/e2e/chain_hdfs"])
        ok7 = status7 == "COMPLETED" and "part-" in listing
        detail7 = f"state={status7} part文件={'是' if 'part-' in listing else '否'} log={log7}"
    record("E2E-CROSS-02", "PostgreSQL → HDFS（过滤后数据）", "PASS" if ok7 else "FAIL", detail7)

    write_report()


def write_report():
    passed = sum(1 for r in results if r["status"] == "PASS")
    total = len(results)
    lines = [
        "# 多数据源交叉验收报告", "",
        f"- 执行时间：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        f"- 后端地址：{BASE}",
        f"- 结果：**{passed}/{total} 通过**", "",
        "| 用例 | 链路 | 结果 | 说明 |",
        "|---|---|---|---|",
    ]
    for r in results:
        icon = "✅ PASS" if r["status"] == "PASS" else ("⏭ SKIP" if r["status"] == "SKIP" else "❌ FAIL")
        lines.append(f"| {r['case']} | {r['chain']} | {icon} | {r['detail']} |")
    lines += ["", "## 环境", "",
              "- PostgreSQL 16（Docker，localhost:5433，库 dataflow，表 students/e2e_*）",
              "- Redis 7（Docker，localhost:6379，含 sale:1 演示 key）",
              "- HDFS（Docker，hdfs://localhost:9000）",
              "- Flink 1.18.1（Docker，JobManager 18081 / SQL Gateway 18083）", ""]
    with open(REPORT, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    print(f"\n报告已写入: {REPORT}", flush=True)
    print(f"结果: {passed}/{total} 通过", flush=True)
    sys.exit(0 if passed == total else 1)


if __name__ == "__main__":
    main()
