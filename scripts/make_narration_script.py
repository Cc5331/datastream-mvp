#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
按剪辑决策表生成"对着念"的旁白文稿：Markdown（改稿用）+ DOCX（打印/对照录）。

统计口径：字数 = 中日韩字符 + 字母数字（不含标点）；语速 = 字数 / 段落时长。
参考语速：演示旁白 4.0–5.0 字/秒从容；>5.5 要赶；>6.5 必须删减或加时长。

用法：python scripts/make_narration_script.py
"""
import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from build_demo_video import TIMELINE  # noqa: E402  仅读决策表，不会触发剪辑

from docx import Document                      # noqa: E402
from docx.shared import Pt, Cm, RGBColor       # noqa: E402
from docx.enum.text import WD_ALIGN_PARAGRAPH  # noqa: E402
from docx.oxml.ns import qn                    # noqa: E402

WORK = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "output", "_video_work")
PUNCT = "，。、；：？！“”‘’（）《》—…·,.;:?!\"'()[]{}<>-/\\| \n\t"


def words(text):
    return len([c for c in text if c not in PUNCT])


def fmt(sec):
    return f"{int(sec) // 60}:{sec % 60:04.1f}"


def speed_note(cps):
    if cps <= 4.0:
        return "从容（可停顿）"
    if cps <= 5.0:
        return "正常语速"
    if cps <= 5.8:
        return "略快，注意别赶"
    if cps <= 6.6:
        return "偏快：可删修饰语"
    return "太赶：建议删句或延长该段"


def rows():
    out, t = [], 0.0
    for item in TIMELINE:
        if item[0] == "card":
            _, name, dur, label, narration = item
            src = "卡片：" + name
        else:
            _, s, a, b, label, narration = item
            dur = b - a
            src = f"录屏 {s} {fmt(a)}–{fmt(b)}"
        n = words(narration)
        out.append(dict(label=label, start=t, end=t + dur, dur=dur, text=narration,
                        chars=n, cps=n / dur if dur else 0, note=speed_note(n / dur if dur else 0),
                        src=src))
        t += dur
    return out, t


def build_md(rs, total, path):
    lines = ["# 《作品演示录像》旁白文稿（对着念版）", ""]
    lines.append(f"- 成片时长 **{fmt(total)}**（{total:.0f} 秒，赛事限时 5 分钟）")
    lines.append(f"- 旁白总字数 **{sum(r['chars'] for r in rs)}** 字，整体语速 "
                 f"**{sum(r['chars'] for r in rs) / total:.1f} 字/秒**（4.0–5.0 为从容）")
    lines.append("- 录法：每段独立录一条，文件名用时间段（如 `01_0000-0006.wav`），最后我按段对齐合轨")
    lines.append("")
    lines.append("> 念稿提示：**逗号处轻停、句号处停半拍**；数字读清楚（如 11,628 读「一万一千六百二十八」）；")
    lines.append("> 术语统一读法：Flink SQL（弗林克 SQL）、Flink 1.18（弗林克一点一八）、AntV X6（X 六）、DAG（迪埃居）")
    lines.append("")
    lines.append("---")
    lines.append("")
    for i, r in enumerate(rs, 1):
        lines.append(f"## {i:02d}　{fmt(r['start'])}–{fmt(r['end'])}（{r['dur']:.1f}s）　{r['label']}")
        lines.append(f"- 语速：**{r['cps']:.1f} 字/秒**（{r['chars']} 字）　{r['note']}")
        lines.append(f"- 画面：{r['src']}")
        lines.append("")
        lines.append(f"**念：** {r['text']}")
        lines.append("")
        lines.append("---")
        lines.append("")
    lines.append("## 录制注意")
    for tip in [
        "麦克风距嘴 15–20cm，先录 10 秒环境音便于降噪；每段开头留 1 秒静默（对齐用）。",
        "念错就整段重录（不要中间改口），我按段落替换，不用重剪画面。",
        "语速以 **4.5 字/秒** 为基准：若某段念完比画面短，就在句尾加停顿；若超时，优先删括号内的修饰语。",
        "片头与片尾放慢到 3.5 字/秒，技术实现段最赶（内容多），可适当提速到 5.5 字/秒。",
        "全部录完后连同音频一起发我：我负责合轨 + 音量归一化，需要的话再出一版烧字幕的成片。",
    ]:
        lines.append(f"- {tip}")
    lines.append("")
    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    return path


def build_docx(rs, total, path):
    doc = Document()
    st = doc.styles["Normal"]
    st.font.size = Pt(11)
    st.font.name = "微软雅黑"
    st.element.rPr.rFonts.set(qn("w:eastAsia"), "微软雅黑")
    for s in doc.sections:
        s.left_margin = s.right_margin = Cm(2.0)
        s.top_margin = s.bottom_margin = Cm(1.8)

    h = doc.add_paragraph()
    r = h.add_run("《作品演示录像》旁白文稿（对着念版）")
    r.bold = True
    r.font.size = Pt(20)
    r.font.color.rgb = RGBColor(0x1F, 0x2A, 0x44)
    h.alignment = WD_ALIGN_PARAGRAPH.CENTER

    meta = doc.add_paragraph()
    meta.add_run(f"成片 {fmt(total)}（限时 5 分钟）｜旁白共 {sum(x['chars'] for x in rs)} 字｜"
                 f"整体 {sum(x['chars'] for x in rs) / total:.1f} 字/秒").font.size = Pt(10)

    t = doc.add_table(rows=1, cols=5)
    t.style = "Light Grid Accent 1"
    for i, txt in enumerate(["#", "时间段 / 时长", "段落", "语速", "画面来源"]):
        cell = t.rows[0].cells[i]
        cell.text = ""
        run = cell.paragraphs[0].add_run(txt)
        run.bold = True
        run.font.size = Pt(9)
    for i, x in enumerate(rs, 1):
        cells = t.add_row().cells
        for j, val in enumerate([f"{i:02d}", f"{fmt(x['start'])}–{fmt(x['end'])}（{x['dur']:.1f}s）",
                                 x["label"], f"{x['cps']:.1f} 字/秒 · {x['note']}", x["src"]]):
            cells[j].text = ""
            run = cells[j].paragraphs[0].add_run(str(val))
            run.font.size = Pt(9)

    doc.add_paragraph()
    p = doc.add_paragraph()
    p.add_run("逐段念稿").bold = True

    for i, x in enumerate(rs, 1):
        p = doc.add_paragraph()
        run = p.add_run(f"{i:02d}　{fmt(x['start'])}–{fmt(x['end'])}　{x['label']}"
                        f"　（{x['dur']:.1f}s，{x['cps']:.1f} 字/秒 · {x['note']}）")
        run.bold = True
        run.font.size = Pt(12)
        run.font.color.rgb = RGBColor(0x2B, 0x6C, 0xB0)
        body = doc.add_paragraph()
        run = body.add_run(x["text"])
        run.font.size = Pt(14)
        body.paragraph_format.space_after = Pt(14)

    p = doc.add_paragraph()
    p.add_run("录制注意").bold = True
    for tip in [
        "麦克风距嘴 15–20cm；每段开头留 1 秒静默（对齐用）；念错整段重录。",
        "基准语速 4.5 字/秒；片头片尾放慢到 3.5，技术实现段可到 5.5。",
        "数字念清楚：11,628 → 一万一千六百二十八；217,795 → 二十一万七千七百九十五。",
        "术语读法：Flink SQL（弗林克 SQL）、Flink 1.18（弗林克一点一八）、AntX X6（X 六）、DAG（迪埃居）。",
        "音频发我后：我合轨 + 音量归一化，需要可再出烧字幕版成片。",
    ]:
        doc.add_paragraph(tip, style="List Bullet").runs[0].font.size = Pt(10)

    doc.save(path)
    return path


if __name__ == "__main__":
    rs, total = rows()
    md = build_md(rs, total, os.path.join(WORK, "旁白文稿.md"))
    dx = build_docx(rs, total, os.path.join(WORK, "旁白文稿.docx"))
    print("已生成:", md)
    print("已生成:", dx)
    print(f"\n总时长 {fmt(total)}，共 {len(rs)} 段，旁白 {sum(r['chars'] for r in rs)} 字，"
          f"整体 {sum(r['chars'] for r in rs) / total:.1f} 字/秒\n")
    for i, r in enumerate(rs, 1):
        flag = "[!]" if r["cps"] > 5.8 else "   "
        print(f"  {flag} {i:02d} {fmt(r['start'])}–{fmt(r['end'])} {r['dur']:5.1f}s "
              f"{r['chars']:4d}字 {r['cps']:4.1f}字/秒 {r['note']:12s} {r['label']}")
