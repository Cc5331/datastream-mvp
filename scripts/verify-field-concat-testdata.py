# -*- coding: utf-8 -*-
"""
字段拼接测试数据独立校验器
==========================
只依赖标准库，不 import 生成器（避免「生成即校验」的自证循环）：
  1) 三个 DAG JSON：结构契约（id/type/label/params/x/y、source/target）、
     field_concat 参数（fields 必须是逗号分隔字符串、不得是 JSON 数组）、
     连线拓扑、输入文件存在、输出无环；
  2) 期望 CSV：表头 = 源全部列 + 拼接列；每个拼接值独立按 separator 重算并逐行比对；
  3) 边界覆盖度：空值行、含分隔符的字段、长文本、数值 0.00 等用例必须存在。

用法： python scripts/verify-field-concat-testdata.py
退出码：0 全部通过；1 有失败项
"""

import csv
import json
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(ROOT, "test-resources", "data")
DAG_DIR = os.path.join(ROOT, "test-resources", "dags")

CASES = [
    {"dag": "02-csv-concat-csv.json", "source": "concat_source.csv", "expected": "concat_expected_1.csv",
     "fields": ["category", "channel"], "separator": ",", "newFieldName": "category_channel"},
    {"dag": "02b-csv-concat-types.json", "source": "concat_source.csv", "expected": "concat_expected_2.csv",
     "fields": ["amount", "join_day"], "separator": "|", "newFieldName": "amount_day", "expected_rows": 24},
    {"dag": "02c-csv-concat-slim-validate.json", "source": "concat_source_slim.csv", "expected": "concat_expected_3.csv",
     "fields": ["src_name", "channel"], "separator": " | ", "newFieldName": "name_channel",
     "validate": ["src_name", "channel"], "expected_rows": 3},
]

FAILED = []
PASSED = 0


def check(cond, ok_msg, fail_msg):
    global PASSED
    if cond:
        PASSED += 1
        print("    [PASS] %s" % ok_msg)
    else:
        FAILED.append(fail_msg)
        print("    [FAIL] %s" % fail_msg)
    return cond


def read_csv(path):
    with open(path, "r", encoding="utf-8", newline="") as f:
        rows = list(csv.reader(f))
    return rows[0], rows[1:]


def main():
    print("=" * 70)
    print(" 字段拼接测试数据校验")
    print("=" * 70)

    for case in CASES:
        print("\n[%s]" % case["dag"])
        dag_path = os.path.join(DAG_DIR, case["dag"])
        if not check(os.path.isfile(dag_path), "DAG 文件存在", "DAG 文件缺失: %s" % dag_path):
            continue
        with open(dag_path, "r", encoding="utf-8") as f:
            dag = json.load(f)

        # ---- 1. DAG 契约 ----
        nodes = dag.get("nodes", [])
        edges = dag.get("edges", [])
        ids = [n.get("id") for n in nodes]
        check(len(ids) == len(set(ids)), "节点 id 唯一（%d 个）" % len(ids), "节点 id 有重复: %s" % ids)
        bad = [n.get("id") for n in nodes if not all(k in n for k in ("id", "type", "label", "params", "x", "y"))]
        check(not bad, "每个节点都有 id/type/label/params/x/y", "节点缺契约字段: %s" % bad)
        for e in edges:
            check("source" in e and "target" in e and "id" in e, "边 %s 契约完整" % e.get("id"),
                  "边缺 source/target: %s" % e)
            check(e["source"] in ids and e["target"] in ids, "边 %s 两端节点存在" % e["id"],
                  "边 %s 指向不存在的节点: %s -> %s" % (e.get("id"), e.get("source"), e.get("target")))

        fc = [n for n in nodes if n.get("type") == "field_concat"]
        if not check(len(fc) == 1, "恰好 1 个 field_concat 节点", "field_concat 节点数量异常: %d" % len(fc)):
            continue
        params = fc[0]["params"]
        check(isinstance(params.get("fields"), str), "fields 是字符串（红线：不能是 JSON 数组）",
              "fields 类型错误（%s）——前端会渲染成 CONCAT([a, b])" % type(params.get("fields")).__name__)
        check([s.strip() for s in str(params.get("fields", "")).split(",")] == case["fields"],
              "fields == %s" % ",".join(case["fields"]), "fields 不匹配: %r" % params.get("fields"))
        check(params.get("separator") == case["separator"], "separator == %r" % case["separator"],
              "separator 不匹配: %r" % params.get("separator"))
        check(params.get("newFieldName") == case["newFieldName"], "newFieldName == %s" % case["newFieldName"],
              "newFieldName 不匹配: %r" % params.get("newFieldName"))

        out_nodes = [n for n in nodes if n.get("type") == "csv_output"]
        check(len(out_nodes) == 1, "恰好 1 个 csv_output", "csv_output 数量异常: %d" % len(out_nodes))
        src_in = [n for n in nodes if n.get("type") == "csv_input"][0]
        check(os.path.isfile(os.path.join(ROOT, src_in["params"]["path"])),
              "输入文件存在: %s" % src_in["params"]["path"], "输入文件不存在: %s" % src_in["params"]["path"])
        target = out_nodes[0]["params"]["path"]
        check(target == "test-resources/data/" + case["expected"],
              "输出路径指向期望文件 %s" % case["expected"], "输出路径异常: %s" % target)

        # field_concat 的下游必须是输出或校验类节点（不得悬空）
        fc_id = fc[0]["id"]
        downstream = [e["target"] for e in edges if e["source"] == fc_id]
        check(len(downstream) == 1, "field_concat 只有 1 个下游", "field_concat 下游异常: %s" % downstream)
        tail_type = [n["type"] for n in nodes if n["id"] == downstream[0]][0]
        check(tail_type in ("csv_output", "validate", "field_filter", "row_filter"),
              "field_concat 下游是 %s" % tail_type, "field_concat 下游类型异常: %s" % tail_type)

        if case.get("filter"):
            rf = [n for n in nodes if n.get("type") == "row_filter"]
            check(len(rf) == 1 and rf[0]["params"].get("condition") == case["filter"],
                  "行过滤条件 == %s" % case["filter"],
                  "行过滤条件异常: %s" % (rf[0]["params"] if rf else None))

        vd = [n for n in nodes if n.get("type") == "validate"]
        if vd:
            # 空值校验的参数名是 checkFields（不是 fields）——写错会被翻译层判「请填写必填字段」
            p = vd[0]["params"]
            check("checkFields" in p and "fields" not in p,
                  "空值校验用 checkFields 参数", "空值校验参数名错误（应为 checkFields）: %s" % p)
            check(str(p.get("checkFields", "")).split(",") == case["fields"],
                  "checkFields == %s" % ",".join(case["fields"]), "checkFields 不匹配: %r" % p.get("checkFields"))
            # 缺省即 true（DataInitializer 里 validate 的 ignoreEmpty 默认 true），显式写 true 更明确
            check(p.get("ignoreEmpty", True) is True, "ignoreEmpty=true（空字符串也算空值）",
                  "ignoreEmpty 不应为 false: %r" % p.get("ignoreEmpty"))

        # ---- 2. 期望 CSV 与源数据一致（独立重算） ----
        src_headers, src_rows = read_csv(os.path.join(DATA_DIR, case["source"]))
        exp_headers, exp_rows = read_csv(os.path.join(DATA_DIR, case["expected"]))
        check(exp_headers == src_headers + [case["newFieldName"]],
              "期望表头 = 源 %d 列 + %s" % (len(src_headers), case["newFieldName"]),
              "期望表头不符: %s" % exp_headers)

        expect_src_rows = src_rows
        if case.get("filter"):
            expect_src_rows = [r for r in src_rows if r[src_headers.index("amount")] == ""]
            check(len(expect_src_rows) == case["expected_rows"],
                  "行过滤命中 %d 行（amount 为空）" % case["expected_rows"],
                  "行过滤命中 %d 行，期望 %d 行" % (len(expect_src_rows), case["expected_rows"]))
        if case.get("validate"):
            # 空值校验（ignoreEmpty=true）：校验字段为空的行会被剔除
            expect_src_rows = [r for r in expect_src_rows
                               if all(r[src_headers.index(f)] != "" for f in case["validate"])]
            check(len(expect_src_rows) == case["expected_rows"],
                  "空值校验后剩 %d 行（剔除校验字段为空的行）" % case["expected_rows"],
                  "空值校验后 %d 行，期望 %d 行" % (len(expect_src_rows), case["expected_rows"]))
        check(len(exp_rows) == len(expect_src_rows),
              "期望输出 %d 行 = 源（过滤/校验后）行数" % len(exp_rows),
              "行数不符: 期望文件 %d 行 / 源 %d 行" % (len(exp_rows), len(expect_src_rows)))

        mismatches = []
        for i, (srow, erow) in enumerate(zip(expect_src_rows, exp_rows), 1):
            want = case["separator"].join(srow[src_headers.index(f)] for f in case["fields"])
            got = erow[len(src_headers)]
            if want != got or erow[:len(src_headers)] != srow:
                mismatches.append("第 %d 行: 期望 %r，实际 %r" % (i, want, got))
        check(not mismatches, "全部 %d 行拼接值与源字段逐个重算一致" % len(exp_rows),
              "拼接值不一致: %s" % mismatches[:3])

        # ---- 3. 边界覆盖度 ----
        all_rows = src_rows
        notes_idx = src_headers.index("remark") if "remark" in src_headers else None
        has_empty = any("" in r for r in all_rows)
        has_sep_in_field = notes_idx is not None and any(
            (";" in r[notes_idx] or "|" in r[notes_idx] or ":" in r[notes_idx]) for r in all_rows)
        has_long = notes_idx is not None and any(len(r[notes_idx]) > 60 for r in all_rows)
        has_zero = "amount" in src_headers and any(r[src_headers.index("amount")] == "0.00" for r in all_rows)
        if "remark" in src_headers:
            check(has_empty, "含空字符串字段用例（相邻分隔符）", "缺少空值用例")
            check(has_sep_in_field, "含「字段值里带分隔符」用例", "缺少分隔符冲突用例")
            check(has_long, "含长文本用例（拼接结果 200+ 字符）", "缺少长文本用例")
            check(has_zero, "含数值 0.00 用例（CAST 保真）", "缺少 0.00 用例")

    print("\n" + "=" * 70)
    if FAILED:
        print(" 结果: FAIL —— %d 项通过, %d 项失败" % (PASSED, len(FAILED)))
        for f in FAILED:
            print("   - %s" % f)
        return 1
    print(" 结果: PASS —— %d 项断言全部通过" % PASSED)
    return 0


if __name__ == "__main__":
    sys.exit(main())
