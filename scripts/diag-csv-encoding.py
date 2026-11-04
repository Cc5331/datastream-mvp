# -*- coding: utf-8 -*-
"""只读诊断：判断 test-resources/data 下几个 CSV 的真实字节编码，并尝试「乱码还原」。
输出写到 logs/diag/encoding_probe.txt（UTF-8），避免控制台 GBK 显示乱码。"""
import os

OUT = os.path.join("logs", "diag", "encoding_probe.txt")
os.makedirs(os.path.dirname(OUT), exist_ok=True)
lines = []

CANDIDATES = ["orders.csv", "sales.csv", "users.csv", "system_logs.csv", "sales_demo_converted.csv"]

def probe(name, path):
    with open(path, "rb") as f:
        raw = f.read(200000)
    out = [f"=== {name} ({os.path.getsize(path)} bytes) ==="]
    out.append("head hex: " + raw[:48].hex(" "))

    utf8 = None
    try:
        utf8 = raw.decode("utf-8")
        out.append("strict UTF-8 decode: OK")
    except UnicodeDecodeError as e:
        out.append(f"strict UTF-8 decode: FAIL ({e.reason} at {e.start})")

    gbk = None
    try:
        gbk = raw.decode("gb18030")
        out.append("GB18030 decode: OK")
    except UnicodeDecodeError as e:
        out.append(f"GB18030 decode: FAIL ({e.reason})")

    def sample(text, tag):
        if text is None:
            return
        first_row = text.splitlines()[:2]
        out.append(f"[{tag}] header: {first_row[0][:120] if first_row else ''}")
        if len(first_row) > 1:
            out.append(f"[{tag}] row1  : {first_row[1][:120]}")

    sample(utf8, "as-UTF-8")
    sample(gbk, "as-GB18030")

    # 乱码还原：把「按 GB18030 解出来的文本」再按 GB18030 编码回字节，然后用 UTF-8 严格解码
    for tag, text in (("gb18030->utf8", gbk), ("utf8->gb18030", utf8)):
        if text is None:
            continue
        try:
            if tag == "gb18030->utf8":
                repaired = text.encode("gb18030").decode("utf-8")
            else:
                repaired = text.encode("utf-8").decode("gb18030")
        except (UnicodeDecodeError, UnicodeEncodeError) as e:
            out.append(f"[repair {tag}] FAIL ({type(e).__name__})")
            continue
        first_row = repaired.splitlines()[:2]
        out.append(f"[repair {tag}] header: {first_row[0][:120] if first_row else ''}")
        if len(first_row) > 1:
            out.append(f"[repair {tag}] row1  : {first_row[1][:120]}")
    out.append("")
    return out

for name in CANDIDATES:
    p = os.path.join("test-resources", "data", name)
    if os.path.exists(p):
        lines.extend(probe(name, p))

with open(OUT, "w", encoding="utf-8") as f:
    f.write("\n".join(lines))
print("written:", OUT)
