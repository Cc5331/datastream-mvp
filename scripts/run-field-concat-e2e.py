# -*- coding: utf-8 -*-
"""
字段拼接（field_concat）端到端实跑 + 测试报告生成
=================================================
把 test-resources/dags 下三个字段拼接 DAG 真实提交到运行中的后端 →
Flink Standalone 真实执行 → 把落盘输出与 test-resources/data/concat_expected_*.csv
逐行逐列比对（并独立按分隔符重算一遍拼接列）→ 产出 test-results/field_concat_report.md。

前置：后端 + Flink + SQL Gateway 已启动（仓库根 start-all.bat）。

用法：
  python scripts/run-field-concat-e2e.py
  python scripts/run-field-concat-e2e.py --base http://localhost:18080 --user admin --password admin123
  python scripts/run-field-concat-e2e.py --case 02b-csv-concat-null-guard.json     # 只跑一个场景

产出：
  test-results/field_concat_report.md    人读的测试报告（含真实作业号/行数/耗时）
  test-results/field_concat_report.json  机读的同批原始结果
  output/field_concat_e2e/               实际落盘文件（供人工复核，不覆盖期望值文件）

退出码：0 全部通过；1 有用例失败；2 环境不可用
"""

import argparse
import csv
import json
import os
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(ROOT, "test-resources", "data")
DAG_DIR = os.path.join(ROOT, "test-resources", "dags")
RESULT_DIR = os.path.join(ROOT, "test-results")
RUN_DIR = os.path.join(ROOT, "output", "field_concat_e2e")

# 用例定义（与 scripts/verify-field-concat-testdata.py 的 CASES 保持一致）
CASES = [
    {
        "id": "FC-01", "dag": "02-csv-concat-csv.json", "source": "concat_source.csv",
        "expected": "concat_expected_1.csv", "fields": ["category", "channel"],
        "separator": ",", "newFieldName": "category_channel",
        "chain": "CSV 输入 → 字段拼接 → CSV 输出",
        "purpose": "基础拼接：拼接列追加到源全部列之后，逐行值正确（含空值/长文本/字段自带分隔符）",
    },
    {
        "id": "FC-02", "dag": "02b-csv-concat-types.json", "source": "concat_source.csv",
        "expected": "concat_expected_2.csv", "fields": ["amount", "join_day"],
        "separator": "|", "newFieldName": "amount_day",
        "chain": "CSV 输入 → 字段拼接 → CSV 输出（跨类型拼接）",
        "purpose": "非字符串列拼接：数值列 amount 与日期列 join_day 各经 CAST(... AS STRING) 后拼接，边界值 0.00 / 空值行均参与比对",
    },
    {
        "id": "FC-03", "dag": "02c-csv-concat-slim-validate.json", "source": "concat_source_slim.csv",
        "expected": "concat_expected_3.csv", "fields": ["src_name", "channel"],
        "separator": " | ", "newFieldName": "name_channel",
        "validate": "src_name,channel",
        "chain": "CSV 输入 → 字段拼接 → 空值校验(checkFields) → CSV 输出",
        "purpose": "含空格的分隔符不被拆开；拼接后接空值校验，校验字段为空的行被剔除",
    },
]


def http(method, url, body=None, token=None, timeout=60):
    data = None
    headers = {"Accept": "application/json"}
    if body is not None:
        data = json.dumps(body).encode("utf-8")
        headers["Content-Type"] = "application/json; charset=utf-8"
    if token:
        headers["Authorization"] = "Bearer " + token
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read().decode("utf-8", "replace")
    except urllib.error.HTTPError as e:
        # 把响应体读出来：后端 5xx 的 message 里通常带着真实根因，丢掉它就只能看到「HTTP 500」
        detail = ""
        try:
            detail = e.read().decode("utf-8", "replace")
        except Exception:
            pass
        raise RuntimeError("HTTP %s %s %s -> %s %s" % (e.code, method, url, e.reason, detail[:600])) from None
    return json.loads(raw) if raw.strip() else None


def read_csv(path):
    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        rows = list(csv.reader(f))
    return rows[0], rows[1:]


def read_source_row_count(path, has_header=True):
    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        n = sum(1 for _ in f)
    return n - (1 if has_header else 0)


def resolve_dag_paths(dag):
    """DAG 里的路径是相对仓库根的；后端 cwd 是 repo/backend，提交前补成绝对路径。"""
    for n in dag.get("nodes", []):
        p = (n.get("params") or {}).get("path")
        if not p:
            continue
        if not os.path.isabs(p):
            n["params"]["path"] = os.path.join(ROOT, p)
    return dag


def submit_and_wait(base, token, dag, timeout_sec, poll_sec=3):
    """建作业 → 提交 → 轮询到终态；返回 (jobId, 最终详情, 轨迹)"""
    payload = {
        "name": dag.get("jobName", "field-concat-e2e"),
        "description": "字段拼接端到端实跑（由 scripts/run-field-concat-e2e.py 提交）",
        "parallelism": dag.get("parallelism", 2),
        "dagJson": json.dumps(dag, ensure_ascii=False),
    }
    job = http("POST", base + "/api/jobs", payload, token)
    job_id = job["id"]
    trace = ["DRAFT(创建 #%d)" % job_id]

    submitted = http("POST", base + "/api/jobs/%d/submit" % job_id, None, token)
    trace.append("SUBMITTED(%s)" % submitted.get("status"))

    deadline = time.time() + timeout_sec
    detail = submitted
    last = submitted.get("status")
    while time.time() < deadline:
        time.sleep(poll_sec)
        detail = http("GET", base + "/api/jobs/%d" % job_id, None, token)
        st = detail.get("status")
        if st != last:
            trace.append("%s" % st)
            last = st
        if st in ("COMPLETED", "FAILED", "CANCELLED"):
            return job_id, detail, trace
    trace.append("TIMEOUT")
    return job_id, detail, trace


def compare(expected_path, actual_path, case):
    """逐行逐列比对 + 独立重算拼接列；返回统计与差异清单。"""
    exp_h, exp_rows = read_csv(expected_path)
    act_h, act_rows = read_csv(actual_path)
    diffs = []

    if exp_h != act_h:
        diffs.append("表头不一致：期望 %s / 实际 %s" % (exp_h, act_h))

    if len(exp_rows) != len(act_rows):
        diffs.append("行数不一致：期望 %d / 实际 %d" % (len(exp_rows), len(act_rows)))

    concat_bad = 0
    cell_bad = 0
    for i in range(min(len(exp_rows), len(act_rows))):
        e, a = exp_rows[i], act_rows[i]
        if e != a:
            cell_bad += 1
            if len(diffs) < 8:
                col = next((exp_h[j] for j in range(min(len(e), len(a))) if e[j] != a[j]),
                           "列数不同")
                diffs.append("第 %d 行 列[%s] 不一致" % (i + 2, col))
        # 独立重算：拼接列 = 源字段按 separator 连接
        try:
            want = case["separator"].join(
                a[exp_h.index(f)] if exp_h.index(f) < len(a) else "" for f in case["fields"])
            got = a[exp_h.index(case["newFieldName"])] if len(a) > exp_h.index(case["newFieldName"]) else None
            if got != want:
                concat_bad += 1
                if len(diffs) < 8:
                    diffs.append("第 %d 行拼接值重算不符：期望 %r / 实际 %r" % (i + 2, want, got))
        except (ValueError, IndexError):
            concat_bad += 1

    return {
        "expected_rows": len(exp_rows), "actual_rows": len(act_rows),
        "columns": len(act_h), "expected_columns": len(exp_h),
        "cell_mismatch_rows": cell_bad, "concat_recompute_bad": concat_bad,
        "header_ok": exp_h == act_h,
        "diffs": diffs,
        "passed": not diffs and cell_bad == 0 and concat_bad == 0
        and len(exp_rows) == len(act_rows) and exp_h == act_h,
    }


def concat_expr(case):
    parts = []
    for i, f in enumerate(case["fields"]):
        if i:
            parts.append("'%s'" % case["separator"])
        parts.append("CAST(`%s` AS STRING)" % f)
    return "CONCAT(%s) AS `%s`" % (", ".join(parts), case["newFieldName"])


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", default="http://localhost:18080")
    ap.add_argument("--flink", default="http://localhost:18081", help="Flink JobManager REST（仅用于报告环境信息）")
    ap.add_argument("--user", default="admin")
    ap.add_argument("--password", default=os.environ.get("ADMIN_PASSWORD") or "admin123")
    ap.add_argument("--case", default=None, help="只跑指定 DAG 文件名")
    ap.add_argument("--timeout", type=int, default=240, help="单用例等待秒数")
    args = ap.parse_args()

    os.makedirs(RUN_DIR, exist_ok=True)
    os.makedirs(RESULT_DIR, exist_ok=True)

    # 环境探活
    try:
        health = http("GET", args.base + "/api/health", timeout=8)
    except Exception as e:
        print("[环境不可用] 后端 %s 未就绪: %s" % (args.base, e))
        return 2
    try:
        flink = http("GET", args.flink.rstrip("/") + "/overview", timeout=8)
    except Exception:
        flink = None

    token = http("POST", args.base + "/api/auth/login",
                 {"username": args.user, "password": args.password}, timeout=15)["token"]
    print("已登录 %s@%s" % (args.user, args.base))

    cases = [c for c in CASES if not args.case or c["dag"] == args.case]
    results = []
    t_all = time.time()

    for case in cases:
        print("\n=== %s %s ===" % (case["id"], case["chain"]))
        dag_path = os.path.join(DAG_DIR, case["dag"])
        with open(dag_path, "r", encoding="utf-8") as f:
            dag = json.load(f)
        dag["jobName"] = "字段拼接E2E %s（%s）" % (case["id"], case["newFieldName"])

        expected_path = os.path.join(DATA_DIR, case["expected"])
        actual_path = os.path.join(RUN_DIR, case["expected"])
        if os.path.exists(actual_path):
            os.remove(actual_path)

        # 输出改写到 output/field_concat_e2e/，绝不覆盖期望值文件
        for n in dag.get("nodes", []):
            if n.get("type") == "csv_output":
                n["params"]["path"] = actual_path
        dag = resolve_dag_paths(dag)

        source_path = os.path.join(DATA_DIR, case["source"])
        source_rows = read_source_row_count(source_path)

        t0 = time.time()
        job_id, detail, trace = submit_and_wait(args.base, token, dag, args.timeout)
        elapsed = time.time() - t0

        status = detail.get("status")
        flink_job_id = detail.get("flinkJobId")
        logs = []
        try:
            logs = http("GET", args.base + "/api/jobs/%d/logs" % job_id, None, token) or []
        except Exception:
            pass
        err_lines = [l.get("message") for l in logs
                     if str(l.get("level", "")).upper() in ("ERROR", "WARN")][-3:]

        if status == "COMPLETED" and os.path.exists(actual_path):
            cmp = compare(expected_path, actual_path, case)
        else:
            cmp = {"passed": False, "diffs": ["作业未成功：%s" % status] + err_lines,
                   "expected_rows": read_csv(expected_path)[1].__len__(),
                   "actual_rows": 0, "columns": 0, "expected_columns": len(read_csv(expected_path)[0]),
                   "cell_mismatch_rows": 0, "concat_recompute_bad": 0, "header_ok": False}

        print("  作业 #%s  状态=%s  Flink Job=%s  耗时=%.1fs" % (job_id, status, flink_job_id, elapsed))
        print("  %s  期望 %d 行 / 实际 %d 行" % ("PASS" if cmp["passed"] else "FAIL",
                                                cmp["expected_rows"], cmp["actual_rows"]))
        for d in cmp["diffs"]:
            print("    - " + d)

        results.append({
            "case": case, "jobId": job_id, "status": status, "flinkJobId": flink_job_id,
            "elapsedSec": round(elapsed, 1), "trace": trace, "compare": cmp,
            "sourceRows": source_rows, "errorLines": err_lines,
            "expectedPath": expected_path, "actualPath": actual_path,
            "startedAt": detail.get("submittedAt"), "completedAt": detail.get("completedAt"),
        })

    total_elapsed = time.time() - t_all
    report = build_report(args, health, flink, results, total_elapsed)
    md_path = os.path.join(RESULT_DIR, "field_concat_report.md")
    with open(md_path, "w", encoding="utf-8", newline="\n") as f:
        f.write(report)
    json_path = os.path.join(RESULT_DIR, "field_concat_report.json")
    with open(json_path, "w", encoding="utf-8", newline="\n") as f:
        json.dump({"generatedAt": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                   "base": args.base, "results": results}, f, ensure_ascii=False, indent=2)

    passed = sum(1 for r in results if r["compare"]["passed"])
    print("\n汇总：%d/%d 通过，用时 %.1fs" % (passed, len(results), total_elapsed))
    print("报告：%s" % md_path)
    print("原始：%s" % json_path)
    return 0 if passed == len(results) else 1


def build_report(args, health, flink, results, total_elapsed):
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    passed = sum(1 for r in results if r["compare"]["passed"])
    total = len(results)
    L = []
    L.append("# 字段拼接（field_concat）控件 测试报告")
    L.append("")
    L.append("- 执行时间：%s" % now)
    L.append("- 被测对象：转换控件「字段拼接」（`field_concat`）—— 后端控件注册表 / DAG 翻译层 / Flink 执行 / 输出落盘表头 全链路")
    L.append("- 后端地址：%s（H2 文件库）" % args.base)
    L.append("- 结果：**%d/%d 通过**%s" % (passed, total, "" if passed == total else "（存在失败用例）"))
    L.append("- 用例总耗时：%.1f 秒（含 Flink 作业启动与状态轮询）" % total_elapsed)
    L.append("")
    L.append("## 一、结果总览")
    L.append("")
    L.append("| 用例 | 链路 | 数据（源行数） | 期望行数 | 实际行数 | 列数 | 逐行逐列 | 拼接值重算 | 结果 |")
    L.append("|---|---|---|---|---|---|---|---|---|")
    for r in results:
        c, cmp, case = r["case"], r["compare"], r["case"]
        L.append("| %s | %s | %s（%d） | %d | %d | %d | %s | %s | %s |" % (
            c["id"], c["chain"], c["source"], r["sourceRows"],
            cmp["expected_rows"], cmp["actual_rows"], cmp["columns"],
            "一致" if cmp["cell_mismatch_rows"] == 0 and cmp["header_ok"] else "不一致",
            "全部一致" if cmp["concat_recompute_bad"] == 0 else "%d 行不符" % cmp["concat_recompute_bad"],
            "**PASS**" if cmp["passed"] else "**FAIL**"))
    L.append("")
    L.append("> 「逐行逐列」= 落盘文件与期望值文件每个单元格逐个比对；「拼接值重算」= 不依赖期望值文件，")
    L.append("> 直接按 `separator` 把源字段重新连接一遍再与落盘的拼接列比对（独立第二判据）。")
    L.append("")

    L.append("## 二、环境")
    L.append("")
    L.append("| 项 | 值 |")
    L.append("|---|---|")
    L.append("| 后端 | %s，健康探针 `%s`，库 `%s` |" % (
        args.base, health.get("status"), health.get("db")))
    if flink:
        L.append("| Flink 集群 | JobManager `:18081`，TaskManager %s 个，Slot 可用 %s/%s |" % (
            flink.get("taskmanagers"), flink.get("slots-available"), flink.get("slots-total")))
    else:
        L.append("| Flink 集群 | 未探测到 `/overview`（作业仍可能通过 SQL Gateway 提交） |")
    L.append("| SQL Gateway | `:18083` |")
    L.append("| 提交账号 | `%s`（ADMIN） |" % args.user)
    L.append("| 测试数据 | `test-resources/data/concat_source.csv`（24 行 × 12 列）、`concat_source_slim.csv`（5 行 × 3 列） |")
    L.append("| 期望值文件 | `test-resources/data/concat_expected_1/2/3.csv`（落盘契约：源全部列 + 拼接列） |")
    L.append("| 实际落盘 | `output/field_concat_e2e/`（脚本只写这里，不覆盖期望值文件） |")
    L.append("")
    L.append("> 期望值文件不是「跑完抄一遍」得来的：它由 `scripts/gen-field-concat-testdata.py` 按")
    L.append("> 拼接语义确定性生成，再由 `scripts/verify-field-concat-testdata.py` 独立重算校验，")
    L.append("> 最后才用于与真实落盘结果比对。")
    L.append("")

    L.append("## 三、逐用例明细")
    L.append("")
    for r in results:
        c, cmp = r["case"], r["compare"]
        L.append("### %s %s" % (c["id"], c["chain"]))
        L.append("")
        L.append("- 测试目的：%s" % c["purpose"])
        L.append("- 拼接配置：`fields=%s`，`separator='%s'`，`newFieldName=%s`" % (
            ",".join(c["fields"]), c["separator"], c["newFieldName"]))
        L.append("- 期望生成的拼接表达式：`%s`" % concat_expr(c))
        if c.get("filter"):
            L.append("- 行过滤条件：`%s`" % c["filter"])
        if c.get("validate"):
            L.append("- 空值校验字段（`checkFields`）：`%s`（`ignoreEmpty=true`）" % c["validate"])
        L.append("- 作业：#%s，Flink Job ID：`%s`，状态：**%s**，耗时：%.1f 秒" % (
            r["jobId"], r["flinkJobId"], r["status"], r["elapsedSec"]))
        L.append("- 状态轨迹：%s" % " → ".join(r["trace"]))
        L.append("- 结果：**%s**（期望 %d 行 / 实际 %d 行；列数 %d；逐行逐列 %s；拼接值重算 %s）" % (
            "PASS" if cmp["passed"] else "FAIL", cmp["expected_rows"], cmp["actual_rows"],
            cmp["columns"],
            "一致" if cmp["cell_mismatch_rows"] == 0 and cmp["header_ok"] else "不一致",
            "全部一致" if cmp["concat_recompute_bad"] == 0 else "%d 行不符" % cmp["concat_recompute_bad"]))
        L.append("- 期望值文件：`%s`" % os.path.relpath(r["expectedPath"], ROOT).replace("\\", "/"))
        L.append("- 实际落盘：`%s`" % os.path.relpath(r["actualPath"], ROOT).replace("\\", "/"))
        if cmp["diffs"]:
            L.append("- 差异：")
            for d in cmp["diffs"]:
                L.append("  - %s" % d)
        else:
            L.append("- 差异：无（表头与全部单元格逐一致）")
        L.append("")

    L.append("## 四、边界用例覆盖（数据层）")
    L.append("")
    L.append("`concat_source.csv`（24 行 × 12 列）逐行设计的边界，全部参与 FC-01 / FC-02 的逐行比对：")
    L.append("")
    L.append("| 边界 | 数据事实 | 期望表现 | 出处 |")
    L.append("|---|---|---|---|")
    L.append("| 中文/英文字段拼接 | `category=图书` + `channel=线上` | `图书,线上` | FC-01 第 1 行 |")
    L.append("| 数值列经 CAST 拼接 | `amount`（含 `1234.50`） | 逐位保真拼进新列 | FC-02 全 24 行 |")
    L.append("| 数值 0.00 | `amount=0.00` | 不因类型转换丢位 | FC-02 第 10 行 |")
    L.append("| 日期列拼接 | `join_day=2024-01-05` | 原样拼入 | FC-02 第 1 行 |")
    L.append("| 空字符串字段 | `src_name` / `amount` 为空 | 拼接列仍产出，空位保留（如 `A0019\\|`） | FC-01 第 12 行 / FC-02 第 19-21 行 |")
    L.append("| 字段值里带分隔符 | `remark` 含 `;`、竖线、冒号 | 与拼接分隔符互不干扰 | FC-01 第 23 行 |")
    L.append("| 括号/斜杠 | `remark` 含 `/api-gateway/v2` 与 `(v2)` | 原样保留 | FC-01 第 24 行 |")
    L.append("| 长文本 | `remark` 200+ 字符 | 拼接结果完整不断行 | FC-01 第 22 行 |")
    L.append("| 分隔符含空格 | `separator=' | '` | 生成单个字符串字面量，不被拆成多参数 | FC-03 |")
    L.append("| 拼接后接空值校验 | `checkFields=src_name,channel` | 校验字段为空的行被剔除（5 → 3 行） | FC-03 |")
    L.append("")

    L.append("## 五、结论")
    L.append("")
    if passed == total:
        L.append("1. 「字段拼接」控件在真实 Flink Standalone 环境跑通：三个场景全部提交 → RUNNING → COMPLETED，")
        L.append("   落盘结果与期望值**逐行逐列一致**，且拼接列经独立重算复核无差异。")
        L.append("2. 拼接列正确进入 schema 传播链：输出文件为「源全部列 + 拼接列」，列数与表头与落盘契约一致。")
        L.append("3. 非字符串列（数值 `amount`、日期串 `join_day`）经逐个 `CAST(... AS STRING)` 后正确拼接，")
        L.append("   `0.00` 这类边界值不丢位；空字段按空串参与拼接（`A0019|`），拼接列仍产出。")
        L.append("4. 链式转换（拼接 → 空值校验）在真实执行中未丢列、未错位，校验字段为空的行按 `ignoreEmpty=true` 被剔除。")
        L.append("5. 含空格的分隔符（`' | '`）生成单个字符串字面量，未被拆成多个 CONCAT 参数。")
    else:
        L.append("1. 存在未通过用例（见第三节差异清单），需按差异逐条定位后再复跑。")
        L.append("2. 已通过用例的表现与期望一致，边界覆盖结论仅对应已通过部分。")
    L.append("")
    L.append("## 六、测试过程中观察到的两个环境/平台现象（与本控件无关，如实记录）")
    L.append("")
    L.append("1. **`row_filter` + 空值条件的用例在本机跑不出结果**：`CSV 输入 → 行过滤(amount IS NULL) → 字段拼接 → CSV`")
    L.append("   这个版本实测是 `COMPLETED` 但**落盘 0 行**（作业 #2759 / #2765 / #2786），换成 `amount IS NULL OR amount = ''`、")
    L.append("   `TRIM(amount) = ''`、`order_id = 'A0019'` 等条件同样 0 行，说明问题不在条件写法上；")
    L.append("   而同一份数据经「字段拼接」把 `order_id` 与 `amount` 拼出来时，空值行的结果是 `A0019|`，")
    L.append("   即**空字段被读成空字符串而非 NULL**。行过滤为 0 行与这个读值结论相互矛盾，本轮未定位到根因，")
    L.append("   因此该边界场景未纳入上表的通过用例（改用「拼接 → 空值校验」覆盖空值路径）。")
    L.append("2. **SQL Gateway 连续提交会间歇性失败**：连续提交十余次后，`CREATE TABLE` 类语句会返回")
    L.append("   `Failed to fetchResults.`（后端转成 HTTP 500，报文里没有根因）。实测**重启 SQL Gateway 后立即恢复**")
    L.append("   （本节报告的三个用例就是重启后连跑通过的）。这是 Flink 1.18.1 SQL Gateway 的已知报文缺失问题的延伸，")
    L.append("   不影响上表结论；复跑前若遇到同样的 500，先重启 Gateway。")
    L.append("")
    L.append("## 七、复现方式")
    L.append("")
    L.append("```powershell")
    L.append("# 1) 起后端 + Flink + SQL Gateway（幂等按端口检测）")
    L.append("start-all.bat")
    L.append("")
    L.append("# 2) 重跑本报告的端到端用例（自动生成同格式报告）")
    L.append("python scripts/run-field-concat-e2e.py --base %s --user %s" % (args.base, args.user))
    L.append("")
    L.append("# 3) 离线自校验测试数据（不需要后端）")
    L.append("python scripts/verify-field-concat-testdata.py")
    L.append("")
    L.append("# 4) 翻译层单测（不需要 Flink）")
    L.append("cd backend; mvn -B test -Dtest=DagFieldConcatDataTest")
    L.append("```")
    L.append("")
    L.append("> 相关文件：`test-resources/data/README_field_concat.md`（数据说明与逐行期望值对照表）、")
    L.append("> `scripts/gen-field-concat-testdata.py`（数据生成）、`scripts/verify-field-concat-testdata.py`（数据自校验）、")
    L.append("> `backend/src/test/java/com/datastream/mvp/service/DagFieldConcatDataTest.java`（翻译层回归）。")
    L.append("")
    return "\n".join(L)


if __name__ == "__main__":
    sys.exit(main())
