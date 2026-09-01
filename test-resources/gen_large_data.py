# -*- coding: utf-8 -*-
"""
大数据集生成器：销售 / 大学生 数据集，4 种格式（CSV / Excel / XML / JSON）
输出目录: test-resources/data/large/
用法:
  python gen_large_data.py            # 生成全部数据集（各 15 万行）
  python gen_large_data.py student    # 只生成大学生数据集
  python gen_large_data.py sales 200000  # 指定数据集 + 行数
"""
import csv, json, os, random, sys, time
from datetime import datetime, timedelta

DEFAULT_ROWS = 150_000
OUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "large")
os.makedirs(OUT_DIR, exist_ok=True)


def fmt_size(n):
    for unit in ["B", "KB", "MB", "GB"]:
        if n < 1024:
            return f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} GB"


# ============ 销售数据集 ============
HEADERS_SALES = ["sale_id", "region", "product_category", "sales_person", "amount", "quantity", "sale_date", "channel"]
REGIONS = ["华北", "华东", "华南", "西南", "西北", "东北"]
CATEGORIES = ["电子产品", "家具", "服装", "食品", "图书"]
CHANNELS = ["线上", "线下", "直播", "跨境"]
SALE_NAMES = ["张三", "李四", "王五", "赵六", "钱七", "孙八", "周九"]


def make_sales_row(i, rng):
    return {
        "sale_id": f"S{i:06d}",
        "region": rng.choice(REGIONS),
        "product_category": rng.choice(CATEGORIES),
        "sales_person": rng.choice(SALE_NAMES),
        "amount": round(rng.uniform(50, 50000), 2),
        "quantity": rng.randint(1, 99),
        "sale_date": (datetime(2024, 1, 1) + timedelta(days=rng.randint(0, 730))).strftime("%Y-%m-%d"),
        "channel": rng.choice(CHANNELS),
    }


# ============ 大学生数据集 ============
HEADERS_STUDENT = ["student_id", "name", "gender", "age", "college", "major", "grade", "gpa", "courses", "monthly_expense", "enroll_date"]
SURNAMES = ["王", "李", "张", "刘", "陈", "杨", "黄", "赵", "周", "吴", "徐", "孙", "马", "朱", "胡", "郭", "何", "林", "罗", "郑"]
GIVEN = ["伟", "芳", "娜", "敏", "静", "磊", "军", "洋", "勇", "艳", "杰", "涛", "明", "超", "秀英", "霞", "平", "刚", "桂英", "文", "辉", "强"]
COLLEGES = ["计算机学院", "经济管理学院", "外国语学院", "机械工程学院", "电子信息学院", "理学院", "医学院", "法学院"]
MAJORS = ["软件工程", "计算机科学与技术", "金融学", "会计学", "英语", "机械设计制造及其自动化", "电子信息工程", "应用数学", "临床医学", "法学"]
GRADES = ["2022级", "2023级", "2024级", "2025级"]
GENDERS = ["男", "女"]


def make_student_row(i, rng):
    name = rng.choice(SURNAMES) + rng.choice(GIVEN)
    return {
        "student_id": f"U{i:08d}",
        "name": name,
        "gender": rng.choice(GENDERS),
        "age": rng.randint(17, 25),
        "college": rng.choice(COLLEGES),
        "major": rng.choice(MAJORS),
        "grade": rng.choice(GRADES),
        "gpa": round(rng.uniform(0.5, 4.0), 2),
        "courses": rng.randint(10, 60),
        "monthly_expense": round(rng.uniform(300, 3000), 2),
        "enroll_date": (datetime(2021, 9, 1) + timedelta(days=rng.randint(0, 1460))).strftime("%Y-%m-%d"),
    }


DATASETS = {
    "sales": ("sales_150k", HEADERS_SALES, make_sales_row),
    "student": ("student_150k", HEADERS_STUDENT, make_student_row),
}


def gen_csv(path, rows, headers, rowfn, rng):
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=headers)
        w.writeheader()
        for i in range(1, rows + 1):
            w.writerow(rowfn(i, rng))
    return os.path.getsize(path)


def gen_xlsx(path, rows, headers, rowfn, rng):
    import xlsxwriter
    wb = xlsxwriter.Workbook(path)
    ws = wb.add_worksheet("Data")
    for c, h in enumerate(headers):
        ws.write(0, c, h)
    for i in range(1, rows + 1):
        r = rowfn(i, rng)
        ws.write_row(i, 0, [r[h] for h in headers])
    wb.close()
    return os.path.getsize(path)


def gen_json(path, rows, headers, rowfn, rng):
    with open(path, "w", encoding="utf-8") as f:
        f.write("[\n")
        for i in range(1, rows + 1):
            f.write(json.dumps(rowfn(i, rng), ensure_ascii=False))
            f.write(",\n" if i < rows else "\n")
        f.write("]\n")
    return os.path.getsize(path)


def gen_xml(path, rows, headers, rowfn, rng):
    with open(path, "w", encoding="utf-8") as f:
        f.write('<?xml version="1.0" encoding="UTF-8"?>\n<records>\n')
        for i in range(1, rows + 1):
            r = rowfn(i, rng)
            f.write("  <record>\n")
            for h in headers:
                f.write(f"    <{h}>{r[h]}</{h}>\n")
            f.write("  </record>\n")
        f.write("</records>\n")
    return os.path.getsize(path)


def main():
    args = [a for a in sys.argv[1:]]
    selected = args[0] if args and args[0] in DATASETS else "all"
    rows = int(args[1]) if len(args) > 1 and args[1].isdigit() else DEFAULT_ROWS
    keys = list(DATASETS) if selected == "all" else [selected]

    t0 = time.time()
    total = 0
    for key in keys:
        prefix, headers, rowfn = DATASETS[key]
        rng = random.Random(20260825 + hash(key) % 1000)
        print(f"生成 [{key}] {rows:,} 行 x 4 格式 -> {OUT_DIR}")
        for name, fn in [("csv", gen_csv), ("xlsx", gen_xlsx), ("json", gen_json), ("xml", gen_xml)]:
            path = os.path.join(OUT_DIR, f"{prefix}.{name}")
            sz = fn(path, rows, headers, rowfn, rng)
            total += sz
            print(f"  OK {prefix}.{name}  ({fmt_size(sz)})")

    readme = os.path.join(OUT_DIR, "README.md")
    lines = [
        "# large/ 大数据集",
        "",
        "## 数据集",
        "| 数据集 | 文件 | 说明 |",
        "|--------|------|------|",
        "| 销售 | sales_150k.csv/.xlsx/.json/.xml | 15 万行销售数据（region/category/person/amount/quantity/date/channel） |",
        "| 大学生 | student_150k.csv/.xlsx/.json/.xml | 15 万行大学生数据（学院/专业/年级/GPA/选课/月消费/入学日期） |",
        "",
        "## 字段",
        "- sales: " + ", ".join(HEADERS_SALES),
        "- student: " + ", ".join(HEADERS_STUDENT),
        "",
        f"- 每文件 {rows:,} 行，UTF-8 编码，中文正常",
        "- 生成: test-resources/gen_large_data.py（python gen_large_data.py [sales|student] [行数]）",
    ]
    with open(readme, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")
    print("  OK README.md")
    print(f"完成，总大小 {fmt_size(total)}，耗时 {time.time() - t0:.1f}s")


if __name__ == "__main__":
    main()
