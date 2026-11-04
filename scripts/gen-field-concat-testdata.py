# -*- coding: utf-8 -*-
"""
字段拼接（field_concat）控件专用测试数据生成器
=============================================
产出（全部相对仓库根目录）：

  test-resources/data/concat_source.csv          源数据 24 行 / 12 列，逐行覆盖拼接边界
  test-resources/data/concat_source_slim.csv     源数据（2 列字符串 → | 拼接 / 空值校验用）
  test-resources/data/concat_expected_1.csv      期望输出（源全部列 + 拼接列 category_channel）
  test-resources/data/concat_expected_2.csv      期望输出（02 场景：amount 为空的 3 行）
  test-resources/data/concat_expected_3.csv      期望输出（03 场景：src_name 为空的 3 行）
  test-resources/dags/02-csv-concat-csv.json     DAG：CSV → 字段拼接 → CSV
  test-resources/dags/02b-csv-concat-null-guard.json  DAG：CSV → 行过滤 → 字段拼接 → CSV（边界专项）
  test-resources/dags/02c-csv-concat-slim-validate.json DAG：CSV → 字段拼接 → 空值校验 → CSV
  test-resources/data/README_field_concat.md     说明 + 期望值对照表

覆盖的拼接边界（每一行都是一个人工可核对的用例）：
  ① 纯中文字段        ② 数值整型 / 保留两位小数（0.00 属于 CAST 保真用例）
  ③ 日期字段（YYYY-MM-DD，作字符串拼接不参与类型推断）
  ④ 空字符串字段（CSV 里是两个连续分隔符 → Flink 读成 NULL）
  ⑤ 含分隔符本身的字段（remark 里有 ";"、"|"、":"）
  ⑥ 含方括号/斜杠/点号的字段（api-gateway、(v2)）
  ⑦ 长文本字段（拼接结果 200+ 字符）

用法： python scripts/gen-field-concat-testdata.py
生成全部为确定性数据（无随机数），重复执行结果完全一致。
"""

import csv
import json
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(ROOT, "test-resources", "data")
DAG_DIR = os.path.join(ROOT, "test-resources", "dags")

# ============ 1. 源数据定义 ============

HEADERS = ["order_id", "src_name", "channel", "category", "dept", "amount", "qty",
           "city", "pay_method", "join_day", "remark", "url"]

# 24 行：前 21 行为正常业务值，后 3 行为空值/长文本边界
ROWS = [
    ["A0001", "张三", "线上", "图书", "运营部", "1234.50", "2", "北京", "微信支付", "2024-01-05", "首次下单", "https://shop.example.com/item/1001"],
    ["A0002", "李四", "线下", "服饰", "销售部", "89.99", "1", "上海", "支付宝", "2024-01-06", "门店自提", "https://shop.example.com/item/1002"],
    ["A0003", "王五", "直播", "家电", "技术部", "12999.00", "1", "广州", "银行卡", "2024-02-11", "大额订单-需回访", "https://shop.example.com/item/1003"],
    ["A0004", "赵六", "跨境", "美妆", "市场部", "560.00", "3", "深圳", "信用卡", "2024-02-12", "跨境直邮", "https://shop.example.com/item/1004"],
    ["A0005", "钱七", "线上", "数码", "研发部", "7800.00", "1", "杭州", "微信支付", "2024-03-01", "保修三年", "https://shop.example.com/item/1005"],
    ["A0006", "孙八", "线下", "家居", "运营部", "199.00", "2", "成都", "支付宝", "2024-03-02", "同城配送", "https://shop.example.com/item/1006"],
    ["A0007", "周九", "直播", "食品", "销售部", "45.50", "10", "武汉", "微信支付", "2024-03-15", "生鲜-冷链", "https://shop.example.com/item/1007"],
    ["A0008", "吴十", "跨境", "图书", "技术部", "312.00", "4", "西安", "银行卡", "2024-04-01", "含税价", "https://shop.example.com/item/1008"],
    ["A0009", "郑一", "线上", "服饰", "市场部", "1280.00", "1", "南京", "信用卡", "2024-04-02", "会员折扣", "https://shop.example.com/item/1009"],
    ["A0010", "王二", "线下", "家电", "研发部", "0.00", "1", "重庆", "微信支付", "2024-04-18", "赠品-金额为0", "https://shop.example.com/item/1010"],
    ["A0011", "冯三", "直播", "美妆", "运营部", "399.00", "2", "苏州", "支付宝", "2024-05-05", "直播间专享", "https://shop.example.com/item/1011"],
    ["A0012", "", "跨境", "数码", "销售部", "8888.00", "1", "天津", "银行卡", "2024-05-06", "来源渠道未知", "https://shop.example.com/item/1012"],
    ["A0013", "陈四", "线上", "家居", "技术部", "1450.75", "1", "长沙", "信用卡", "2024-05-20", "含安装费", "https://shop.example.com/item/1013"],
    ["A0014", "褚五", "线下", "食品", "市场部", "78.20", "6", "青岛", "微信支付", "2024-06-01", "临期特价", "https://shop.example.com/item/1014"],
    ["A0015", "卫六", "直播", "图书", "研发部", "268.00", "3", "厦门", "支付宝", "2024-06-11", "开学季", "https://shop.example.com/item/1015"],
    ["A0016", "蒋七", "跨境", "服饰", "运营部", "3200.00", "2", "昆明", "银行卡", "2024-06-12", "含关税", "https://shop.example.com/item/1016"],
    ["A0017", "沈八", "线上", "家电", "销售部", "5600.00", "1", "郑州", "信用卡", "2024-07-01", "以旧换新", "https://shop.example.com/item/1017"],
    ["A0018", "韩九", "线下", "美妆", "技术部", "129.00", "3", "合肥", "微信支付", "2024-07-02", "母亲节礼盒", "https://shop.example.com/item/1018"],
    # 后 3 行：空值 / 长文本边界，拼接结果里空字段会留下相邻分隔符
    ["A0019", "杨十", "直播", "数码", "市场部", "", "1", "济南", "支付宝", "2024-07-19", "金额缺失-待补录", "https://shop.example.com/item/1019"],
    ["A0020", "朱一", "跨境", "家居", "研发部", "", "2", "福州", "银行卡", "2024-08-01", "金额缺失-线下补录", "https://shop.example.com/item/1020"],
    ["A0021", "秦二", "线上", "食品", "运营部", "", "5", "贵阳", "信用卡", "2024-08-02", "金额缺失-赠品", "https://shop.example.com/item/1021"],
    ["A0022", "尤三", "线下", "图书", "销售部", "999999.99", "1", "兰州", "微信支付", "2024-08-20",
     "超长备注：客户要求分批发货，第一批走空运，第二批走海运，需要客服全程跟进并同步物流单号，"
     "同时备注发票需开具增值税专用发票，抬头为公司名称，税号已提供；此外还有一项特殊说明，"
     "本单包含赠品一件，赠品不参与退换，签收前请务必当面验货。", "https://shop.example.com/item/1022"],
    ["A0023", "许四", "直播", "服饰", "技术部", "650.00", "2", "太原", "支付宝", "2024-09-01",
     "备注含分隔符;分号|竖线:冒号", "https://shop.example.com/item/1023"],
    ["A0024", "何五", "跨境", "家电", "市场部", "4200.00", "1", "石家庄", "银行卡", "2024-09-02",
     "接口来源 /api-gateway/v2 与 (v2) 括号", "https://shop.example.com/item/1024"],
]

SLIM_HEADERS = ["order_id", "src_name", "channel"]
SLIM_ROWS = [["A0001", "张三", "线上"], ["A0002", "", "线下"], ["A0003", "王五", "直播"],
             ["A0004", "赵六", ""], ["A0005", "钱七", "跨境"]]

# 三个输出场景（与 DAG JSON / Java 单测 / README 三处共用同一份定义）
SCENARIOS = {
    "01": {
        "dag": "02-csv-concat-csv.json",
        "source": "concat_source.csv",
        "expected": "concat_expected_1.csv",
        "fields": ["category", "channel"],
        "separator": ",",
        "newFieldName": "category_channel",
        "desc": "基础拼接：category + channel（逗号分隔）",
    },
    "02": {
        "dag": "02b-csv-concat-types.json",
        "source": "concat_source.csv",
        "expected": "concat_expected_2.csv",
        "fields": ["amount", "join_day"],
        "separator": "|",
        "newFieldName": "amount_day",
        # 数值列 + 日期串：翻译层对每个字段都生成 CAST(... AS STRING)，非字符串列必须能拼进来
        "desc": "类型拼接：数值列 amount + 日期列 join_day（'|' 分隔，验证 CAST 到 STRING）",
    },
    "03": {
        "dag": "02c-csv-concat-slim-validate.json",
        "source": "concat_source_slim.csv",
        "expected": "concat_expected_3.csv",
        "fields": ["src_name", "channel"],
        "separator": " | ",
        "newFieldName": "name_channel",
        "post": {"type": "validate", "params": {"checkFields": "src_name,channel", "ignoreEmpty": True},
                 "desc": "拼接后校验 src_name / channel 非空（参数名是 checkFields）"},
        # 空值校验会剔除 src_name 或 channel 为空的行，期望值文件必须体现这个语义
        "validate_fields": ["src_name", "channel"],
        "desc": "管道分隔：src_name + channel（' | '），拼接后接空值校验",
    },
}


def write_csv(path, headers, rows):
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f, lineterminator="\n")
        w.writerow(headers)
        w.writerows(rows)
    print("  [write] %-34s %d 行 / %d 列" % (os.path.basename(path), len(rows), len(headers)))
    return path


def concat_value(row, headers, fields, separator):
    """系统语义：CONCAT(CAST(列 AS STRING)...)，NULL 列在 Flink CONCAT 中结果为 NULL。"""
    values = []
    for name in fields:
        if name not in headers:
            raise SystemExit("字段不存在: %s（可用: %s）" % (name, ",".join(headers)))
        values.append(row[headers.index(name)])
    return separator.join(values)


def build_expected(headers, rows, fields, separator, new_field):
    """期望输出 = 源全部列 + 拼接列（FlinkJobStatusChecker.applyTransformHeader 的落盘契约）。"""
    out_headers = list(headers) + [new_field]
    out_rows = [list(r) + [concat_value(r, headers, fields, separator)] for r in rows]
    return out_headers, out_rows


def dag_json(scenario):
    """生成可被前端「导入 JSON」与后端 translate() 直接消费的 DAG。"""
    src = scenario["source"]
    fields_csv = ",".join(scenario["fields"])
    sep = scenario["separator"]
    nfn = scenario["newFieldName"]
    filter_cfg = scenario.get("filter")
    post = scenario.get("post")

    src_path = "test-resources/data/%s" % src
    out_path = "test-resources/data/%s" % scenario["expected"]

    nodes = []
    edges = []
    x = 60
    src_id = "csv_input_1"
    nodes.append({
        "id": src_id, "type": "csv_input", "label": "CSV 输入（%s）" % src,
        "params": {"path": src_path, "delimiter": ",", "hasHeader": True},
        "x": x, "y": 260,
    })

    prev = src_id
    if filter_cfg:
        rid = "row_filter_1"
        nodes.append({
            "id": rid, "type": "row_filter", "label": "行过滤：%s" % filter_cfg["desc"],
            "params": {"condition": filter_cfg["condition"]},
            "x": x + 240, "y": 260,
        })
        edges.append({"id": "e1", "source": prev, "target": rid})
        prev = rid

    fc_id = "fc_1"
    nodes.append({
        "id": fc_id, "type": "field_concat",
        "label": "字段拼接（%s → %s）" % (fields_csv, nfn),
        "params": {"fields": fields_csv, "separator": sep, "newFieldName": nfn},
        "x": x + (480 if filter_cfg else 240), "y": 260,
    })
    edges.append({"id": "e%d" % (len(edges) + 1), "source": prev, "target": fc_id})
    prev = fc_id

    if post:
        pid = "%s_1" % post["type"]
        nodes.append({
            "id": pid, "type": post["type"], "label": "%s：%s" % (post["type"], post["desc"]),
            "params": dict(post["params"]),
            "x": x + 720, "y": 260,
        })
        edges.append({"id": "e%d" % (len(edges) + 1), "source": prev, "target": pid})
        prev = pid

    nodes.append({
        "id": "csv_output_1", "type": "csv_output", "label": "CSV 输出（期望值对照 %s）" % scenario["expected"],
        "params": {"path": out_path, "delimiter": ","},
        "x": x + (960 if post else (720 if filter_cfg else 480)), "y": 260,
    })
    edges.append({"id": "e%d" % (len(edges) + 1), "source": prev, "target": "csv_output_1"})

    return {"jobName": "字段拼接测试 %s（%s）" % (scenario_key_of(scenario), scenario["desc"]),
            "parallelism": 2, "nodes": nodes, "edges": edges}


def scenario_key_of(scenario):
    for k, v in SCENARIOS.items():
        if v is scenario:
            return k
    return "?"


# ============ 2. 生成 ============

def main():
    os.makedirs(DATA_DIR, exist_ok=True)
    os.makedirs(DAG_DIR, exist_ok=True)
    print("=" * 68)
    print(" 字段拼接（field_concat）测试数据生成器")
    print("=" * 68)

    print("\n[1/4] 源数据")
    src_path = write_csv(os.path.join(DATA_DIR, "concat_source.csv"), HEADERS, ROWS)
    write_csv(os.path.join(DATA_DIR, "concat_source_slim.csv"), SLIM_HEADERS, SLIM_ROWS)

    print("\n[2/4] 期望输出（= 源全部列 + 拼接列）")
    expected = {}
    for key in sorted(SCENARIOS):
        sc = SCENARIOS[key]
        headers = HEADERS if sc["source"] == "concat_source.csv" else SLIM_HEADERS
        rows = SLIM_ROWS if headers is SLIM_HEADERS else ROWS
        if sc.get("validate_fields"):
            # 空值校验（ignoreEmpty=true）会剔除校验字段为空的行 → 期望值文件必须同口径
            V = [headers.index(f) for f in sc["validate_fields"]]
            rows = [r for r in rows if all(r[i] != "" for i in V)]
        out_headers, out_rows = build_expected(headers, rows, sc["fields"], sc["separator"], sc["newFieldName"])
        write_csv(os.path.join(DATA_DIR, sc["expected"]), out_headers, out_rows)
        expected[key] = (out_headers, out_rows)

    print("\n[3/4] DAG JSON")
    for key in sorted(SCENARIOS):
        sc = SCENARIOS[key]
        dag = dag_json(sc)
        path = os.path.join(DAG_DIR, sc["dag"])
        with open(path, "w", encoding="utf-8", newline="\n") as f:
            json.dump(dag, f, ensure_ascii=False, indent=2)
            f.write("\n")
        print("  [write] %-40s %s" % (sc["dag"], sc["desc"]))

    print("\n[4/4] 对照表 README")
    write_readme(expected)

    print("\n生成完毕。校验： python scripts/verify-field-concat-testdata.py")


def write_readme(expected):
    lines = []
    lines.append("# 字段拼接（field_concat）测试数据")
    lines.append("")
    lines.append("> 由 `scripts/gen-field-concat-testdata.py` 生成（确定性数据，可重复执行）")
    lines.append("> 校验：`python scripts/verify-field-concat-testdata.py`；翻译层回归：`mvn -B test -Dtest=DagFieldConcatDataTest`")
    lines.append("")
    lines.append("## 文件清单")
    lines.append("")
    lines.append("| 文件 | 说明 |")
    lines.append("|------|------|")
    lines.append("| concat_source.csv | 源数据 %d 行 / %d 列，逐行覆盖拼接边界 |" % (len(ROWS), len(HEADERS)))
    lines.append("| concat_source_slim.csv | 源数据 %d 行 / %d 列（短表，便于人工核对） |" % (len(SLIM_ROWS), len(SLIM_HEADERS)))
    lines.append("| concat_expected_1.csv | 01 场景期望输出：源全部列 + `category_channel` = category + \",\" + channel（24 行） |")
    lines.append("| concat_expected_2.csv | 02 场景期望输出：源全部列 + `amount_day` = amount + \"\\|\" + join_day（24 行） |")
    lines.append("| concat_expected_3.csv | 03 场景期望输出：空值校验后剩 3 行 + `name_channel` = src_name + \" \\| \" + channel |")
    lines.append("")
    lines.append("DAG（前端「导入 JSON」直接用，路径为相对仓库根）：")
    lines.append("")
    lines.append("| DAG | 链路 | 拼接配置 |")
    lines.append("|-----|------|----------|")
    for key in sorted(SCENARIOS):
        sc = SCENARIOS[key]
        chain = "CSV → "
        if sc.get("filter"):
            chain += "行过滤 → "
        chain += "字段拼接"
        if sc.get("post"):
            chain += " → 空值校验"
        chain += " → CSV"
        lines.append("| %s | %s | fields=%s，separator='%s'，newFieldName=%s |"
                     % (sc["dag"], chain, ",".join(sc["fields"]),
                        sc["separator"].replace("|", "\\|"), sc["newFieldName"]))
    lines.append("")
    lines.append("## 期望值对照表（01 场景：`category_channel` = `category` + \",\" + `channel`）")
    lines.append("")
    lines.append("| # | category | channel | 期望 category_channel | 备注 |")
    lines.append("|---|----------|---------|----------------------|------|")
    notes = {
        10: "金额 0.00（CAST 保真，不丢小数位）",
        12: "src_name 空字符串（Flink 读成 NULL）",
        19: "amount 为空 → 02 场景过滤命中",
        20: "amount 为空 → 02 场景过滤命中",
        21: "amount 为空 → 02 场景过滤命中",
        22: "长文本备注（含全角逗号/分号，拼接结果 200+ 字符）",
        23: "备注含半角分号/竖线/冒号（即分隔符本身出现在字段值里）",
        24: "备注含斜杠 / 与 (v2) 括号",
    }
    for i, row in enumerate(ROWS, 1):
        cat = row[HEADERS.index("category")]
        ch = row[HEADERS.index("channel")]
        lines.append("| %d | %s | %s | `%s` | %s |"
                     % (i, cat or "(空)", ch or "(空)", cat + "," + ch, notes.get(i, "")))
    lines.append("")
    lines.append("## 用途")
    lines.append("")
    lines.append("1. **画布手工验证**：拖 CSV 输入 → 字段拼接 → CSV 输出，`fields` 填 `category,channel`、")
    lines.append("   `separator` 填 `,`、`newFieldName` 填 `category_channel`，输入路径选 `concat_source.csv`，")
    lines.append("   跑完把输出与 `concat_expected_1.csv` 逐行对比。")
    lines.append("2. **导入 JSON 快验**：直接导入 `02-csv-concat-csv.json`，提交运行后对比期望文件。")
    lines.append("3. **一键实跑对比**（后端 + Flink 已启动时）：")
    lines.append("")
    lines.append("   ```powershell")
    lines.append("   pwsh scripts/run-field-concat-demo.ps1                              # 场景 01（单场景，PowerShell 版）")
    lines.append("   python scripts/run-field-concat-e2e.py --base http://localhost:18080 --user admin")
    lines.append("   ```")
    lines.append("")
    lines.append("   后者会把三个场景依次真实提交到 Flink，跑完与期望文件逐行逐列比对，并自动生成")
    lines.append("   `test-results/field_concat_report.md`（含真实作业号 / Flink Job ID / 行数 / 耗时，可直接作为测试报告）。")
    lines.append("   两个脚本都把 DAG 的输出改写到 `output/` 下再提交，绝不覆盖期望值文件。")
    lines.append("4. **数据自校验**（纯离线，不需要后端）：")
    lines.append("")
    lines.append("   ```powershell")
    lines.append("   python scripts/verify-field-concat-testdata.py")
    lines.append("   ```")
    lines.append("")
    lines.append("5. **翻译层回归**：`DagFieldConcatDataTest` 把这三个 DAG 真实喂给 `DagTranslationService.translate()`，")
    lines.append("   断言生成的 Flink SQL 含正确 CONCAT 表达式、schema 已传播出拼接列、且不含 `CONCAT([` 之类的非法语法。")
    lines.append("")
    lines.append("   ```powershell")
    lines.append("   cd backend; mvn -B test -Dtest=DagFieldConcatDataTest")
    lines.append("   ```")
    lines.append("")
    lines.append("## 路径注意")
    lines.append("")
    lines.append("DAG 里的路径是**相对仓库根**的（与 `dags/` 下其它测试 DAG 一致）：后端从仓库根启动时可直接提交；")
    lines.append("从 `backend/` 目录启动或用单测加载时，需要先把相对路径补成绝对路径（两个实跑脚本与 `DagFieldConcatDataTest` 都已按此处理）。")
    lines.append("")
    lines.append("## 一个实测提醒：CSV 空字段是「空字符串」，不是 NULL")
    lines.append("")
    lines.append("`concat_source.csv` 里 `amount` / `src_name` 为空白的行，被 Flink 的 csv 源读成**空字符串**——")
    lines.append("实测证据：拿这份数据把 `order_id` 与 `amount` 拼出来，空值行的结果是 `A0019|`（`CONCAT` 遇 NULL 应整列为 NULL）。")
    lines.append("所以用行过滤挑这些行时，条件要写成 `amount IS NULL OR amount = ''`（或 `TRIM(amount) = ''`）才稳；")
    lines.append("只写 `amount IS NULL` 命中不到（本机实测该链路还为 0 行，根因未定位，见 `test-results/field_concat_report.md` 第六节）。")
    lines.append("`validate`（空值校验）控件本身已同时判 `IS NULL` 与 `<> ''`，不受该差异影响。")
    lines.append("")
    with open(os.path.join(DATA_DIR, "README_field_concat.md"), "w", encoding="utf-8", newline="\n") as f:
        f.write("\n".join(lines))
    print("  [write] README_field_concat.md")


if __name__ == "__main__":
    main()
