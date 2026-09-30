#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
把答辩 Markdown 文档转成可打印的 Word（.docx）：
  docs/答辩讲稿-8分钟.md          -> docs/答辩讲稿-8分钟.docx
  docs/答辩问题库-标准答案.md      -> docs/答辩问题库-标准答案.docx

设计与排版取向（面向"打印出来带进答辩现场"）：
  · A4 + 2.2cm 页边距，正文 宋体 10.5pt / 行距 1.3，加页码页脚
  · H1 居中 18pt 深蓝；H2 14pt 带下边框（模拟分隔线）；H3 12pt
  · Markdown 表格 → Word 真表格（Table Grid，表头灰底加粗居中，自动列宽）
  · 代码块 → 等宽 9pt + 浅灰底纹（保留缩进与原文）
  · 引用块（> ）→ 缩进 + 左侧竖线 + 灰底（"用法说明 / 提醒"）
  · 行内 **加粗**、`代码`、*斜体* 逐段解析；⭐ 与 ⚠️ 用深红色标记便于现场抓重点

用法：python scripts/gen-defense-docs.py
"""
import os
import re
import sys
import hashlib
from docx import Document
from docx.shared import Pt, Cm, RGBColor
from docx.enum.text import WD_ALIGN_PARAGRAPH, WD_LINE_SPACING
from docx.enum.table import WD_TABLE_ALIGNMENT
from docx.oxml.ns import qn
from docx.oxml import OxmlElement

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DOCS = os.path.join(ROOT, "docs")

JOBS = [
    ("答辩讲稿-8分钟.md", "答辩讲稿-8分钟.docx", {}),
    ("答辩问题库-标准答案.md", "答辩问题库-标准答案.docx", {}),
    ("答辩速查小抄.md", "答辩速查小抄.docx", {"compact": True}),   # 小抄要压到 2 页内，用紧凑版式
]

# 紧凑版式（小抄）：更小页边距与字号、更紧行距
COMPACT = dict(body=10.0, line=1.15, t_size=9.0, h1=15.5, h2=12.0, h3=11.0, quote=9.5)

CN_FONT = "宋体"
CN_HEAD = "微软雅黑"
EN_FONT = "Times New Roman"
MONO_FONT = "Consolas"

INK = RGBColor(0x1A, 0x1A, 0x1A)
BLUE = RGBColor(0x14, 0x3C, 0x64)
RED = RGBColor(0xB0, 0x1C, 0x1C)
GREY = RGBColor(0x55, 0x55, 0x55)

# ----------------------------------------------------------------- xml helpers
def set_font(run, cn=CN_FONT, en=EN_FONT, size=10.5, bold=False, color=None, italic=False):
    run.font.name = en
    run.font.size = Pt(size)
    run.font.bold = bold
    run.font.italic = italic
    if color is not None:
        run.font.color.rgb = color
    rpr = run._element.get_or_add_rPr()
    rfonts = rpr.find(qn("w:rFonts"))
    if rfonts is None:
        rfonts = OxmlElement("w:rFonts")
        rpr.append(rfonts)
    rfonts.set(qn("w:ascii"), en)
    rfonts.set(qn("w:hAnsi"), en)
    rfonts.set(qn("w:eastAsia"), cn)


def shade(element, fill):
    """给段落或单元格加底纹（w:shd）"""
    pr = element.get_or_add_pPr() if element.tag.endswith("}p") else element.get_or_add_tcPr()
    shd = OxmlElement("w:shd")
    shd.set(qn("w:val"), "clear")
    shd.set(qn("w:color"), "auto")
    shd.set(qn("w:fill"), fill)
    pr.append(shd)


def para_border(p, sides=("left",), color="BFBFBF", sz=18):
    pPr = p._p.get_or_add_pPr()
    bdr = OxmlElement("w:pBdr")
    for side in sides:
        el = OxmlElement("w:" + side)
        el.set(qn("w:val"), "single")
        el.set(qn("w:sz"), str(sz))
        el.set(qn("w:space"), "6")
        el.set(qn("w:color"), color)
        bdr.append(el)
    pPr.append(bdr)


def add_footer_pagenum(section):
    p = section.footer.paragraphs[0]
    p.alignment = WD_ALIGN_PARAGRAPH.CENTER
    r = p.add_run()
    set_font(r, size=9, color=GREY)
    fld = OxmlElement("w:fldSimple")
    fld.set(qn("w:instr"), "PAGE")
    run_el = OxmlElement("w:r")
    t = OxmlElement("w:t")
    t.text = "1"
    run_el.append(t)
    fld.append(run_el)
    p._p.append(fld)


def set_repeat_header(row):
    trPr = row._tr.get_or_add_trPr()
    el = OxmlElement("w:tblHeader")
    el.set(qn("w:val"), "true")
    trPr.append(el)


# ----------------------------------------------------------------- inline parse
TOKEN = re.compile(r"(\*\*.+?\*\*|`[^`]+`|\*[^*\s][^*]*?\*|⭐|⚠️)")


def add_inline(p, text, size=10.5, base_bold=False, cn=CN_FONT):
    """解析 **加粗** / `代码` / *斜体* / ⭐ / ⚠️ 并写入段落"""
    for piece in TOKEN.split(text):
        if not piece:
            continue
        if piece.startswith("**") and piece.endswith("**") and len(piece) > 4:
            r = p.add_run(piece[2:-2])
            set_font(r, cn=cn, size=size, bold=True, color=INK)
        elif piece.startswith("`") and piece.endswith("`") and len(piece) > 2:
            r = p.add_run(piece[1:-1])
            set_font(r, cn=MONO_FONT, en=MONO_FONT, size=size - 0.5, color=RGBColor(0x8A, 0x2B, 0x00))
        elif piece in ("⭐", "⚠️"):
            r = p.add_run(piece)
            set_font(r, cn=cn, size=size, bold=True, color=RED)
        elif piece.startswith("*") and piece.endswith("*") and len(piece) > 2:
            r = p.add_run(piece[1:-1])
            set_font(r, cn=cn, size=size, italic=True, color=INK)
        else:
            r = p.add_run(piece)
            set_font(r, cn=cn, size=size, bold=base_bold, color=INK)


def split_row(line):
    """拆分 Markdown 表格行，尊重 \\| 转义"""
    s = line.strip()
    if s.startswith("|"):
        s = s[1:]
    if s.endswith("|"):
        s = s[:-1]
    cells, buf, i = [], "", 0
    while i < len(s):
        if s[i] == "\\" and i + 1 < len(s) and s[i + 1] == "|":
            buf += "|"
            i += 2
            continue
        if s[i] == "|":
            cells.append(buf.strip())
            buf = ""
            i += 1
            continue
        buf += s[i]
        i += 1
    cells.append(buf.strip())
    return cells


def is_sep_row(line):
    return bool(re.match(r"^\s*\|?[\s:|-]+\|[\s:|-]*$", line)) and "-" in line


# ----------------------------------------------------------------- block writer
def new_doc(title, opt=None):
    opt = opt or {}
    doc = Document()
    st = doc.styles["Normal"]
    st.font.name = EN_FONT
    st.font.size = Pt(opt.get("body", 10.5))
    st.element.rPr.rFonts.set(qn("w:eastAsia"), CN_FONT)
    pf = st.paragraph_format
    pf.line_spacing_rule = WD_LINE_SPACING.MULTIPLE
    pf.line_spacing = opt.get("line", 1.3)
    pf.space_after = Pt(4)

    sec = doc.sections[0]
    sec.page_width, sec.page_height = Cm(21.0), Cm(29.7)
    if opt.get("compact"):
        sec.left_margin = sec.right_margin = Cm(1.5)
        sec.top_margin = Cm(1.4)
        sec.bottom_margin = Cm(1.2)
    else:
        sec.left_margin = sec.right_margin = Cm(2.2)
        sec.top_margin = Cm(2.0)
        sec.bottom_margin = Cm(1.8)
    add_footer_pagenum(sec)
    return doc


def h1(doc, text, opt=None):
    opt = opt or {}
    p = doc.add_paragraph()
    p.alignment = WD_ALIGN_PARAGRAPH.CENTER
    p.paragraph_format.space_before = Pt(0)
    p.paragraph_format.space_after = Pt(10)
    r = p.add_run(text)
    set_font(r, cn=CN_HEAD, size=opt.get("h1", 17), bold=True, color=BLUE)
    return p


def h2(doc, text, opt=None):
    opt = opt or {}
    p = doc.add_paragraph()
    p.paragraph_format.space_before = Pt(opt.get("h2_before", 14))
    p.paragraph_format.space_after = Pt(6)
    p.paragraph_format.keep_with_next = True
    r = p.add_run(text)
    set_font(r, cn=CN_HEAD, size=opt.get("h2", 13.5), bold=True, color=BLUE)
    para_border(p, sides=("bottom",), color="9DB7D0", sz=8)
    return p


def h3(doc, text, opt=None):
    opt = opt or {}
    size = opt.get("h3", 11.5)
    p = doc.add_paragraph()
    p.paragraph_format.space_before = Pt(opt.get("h3_before", 10))
    p.paragraph_format.space_after = Pt(3)
    p.paragraph_format.keep_with_next = True
    add_inline(p, text, size=size)
    for r in p.runs:
        col = r.font.color.rgb if (r.font.color and r.font.color.type is not None) else BLUE
        set_font(r, cn=CN_HEAD, size=size, bold=True, color=col)
    return p


def body(doc, text, indent=0.0, opt=None):
    opt = opt or {}
    p = doc.add_paragraph()
    p.alignment = WD_ALIGN_PARAGRAPH.JUSTIFY
    if indent:
        p.paragraph_format.left_indent = Cm(indent)
    add_inline(p, text, size=opt.get("body", 10.5))
    return p


def bullet(doc, text, level=0, opt=None):
    opt = opt or {}
    p = doc.add_paragraph(style="List Bullet")
    p.alignment = WD_ALIGN_PARAGRAPH.JUSTIFY
    p.paragraph_format.left_indent = Cm(0.75 + 0.6 * level)
    p.paragraph_format.space_after = Pt(2)
    add_inline(p, text, size=opt.get("body", 10.5))
    return p


def quote(doc, text, opt=None):
    opt = opt or {}
    p = doc.add_paragraph()
    p.alignment = WD_ALIGN_PARAGRAPH.JUSTIFY
    p.paragraph_format.left_indent = Cm(0.5)
    p.paragraph_format.right_indent = Cm(0.2)
    p.paragraph_format.space_before = Pt(3)
    p.paragraph_format.space_after = Pt(5)
    add_inline(p, text, size=opt.get("quote", 10))
    shade(p._p, "F4F6F8")
    para_border(p, sides=("left",), color="9DB7D0", sz=18)
    return p


def code_block(doc, lines):
    for i, ln in enumerate(lines):
        p = doc.add_paragraph()
        p.paragraph_format.left_indent = Cm(0.4)
        p.paragraph_format.space_after = Pt(0 if i < len(lines) - 1 else 6)
        p.paragraph_format.space_before = Pt(4 if i == 0 else 0)
        p.paragraph_format.line_spacing = 1.0
        r = p.add_run(ln if ln else " ")
        set_font(r, cn=MONO_FONT, en=MONO_FONT, size=9, color=RGBColor(0x22, 0x22, 0x22))
        shade(p._p, "F2F2F2")
    return


def table(doc, header, rows, opt=None):
    opt = opt or {}
    ts = opt.get("t_size", 9.5)
    ncol = max([len(header)] + [len(r) for r in rows]) if rows else len(header)
    t = doc.add_table(rows=1, cols=ncol)
    t.style = "Table Grid"
    t.alignment = WD_TABLE_ALIGNMENT.CENTER
    t.autofit = True
    hdr = t.rows[0]
    set_repeat_header(hdr)
    for i in range(ncol):
        cell = hdr.cells[i]
        cell.text = ""
        p = cell.paragraphs[0]
        p.alignment = WD_ALIGN_PARAGRAPH.CENTER
        p.paragraph_format.space_after = Pt(1)
        p.paragraph_format.line_spacing = 1.1
        add_inline(p, header[i] if i < len(header) else "", size=ts, base_bold=True)
        for r in p.runs:
            r.font.bold = True
        shade(cell._tc, "E8EDF3")
    for row in rows:
        cells = t.add_row().cells
        for i in range(ncol):
            cell = cells[i]
            cell.text = ""
            p = cell.paragraphs[0]
            p.paragraph_format.space_after = Pt(1)
            p.paragraph_format.line_spacing = 1.15
            add_inline(p, row[i] if i < len(row) else "", size=ts)
    doc.add_paragraph().paragraph_format.space_after = Pt(2)
    return t


def hr(doc):
    p = doc.add_paragraph()
    p.paragraph_format.space_before = Pt(4)
    p.paragraph_format.space_after = Pt(6)
    para_border(p, sides=("bottom",), color="D0D0D0", sz=6)
    return p


# ----------------------------------------------------------------- main convert
def convert(src, dst, opt=None):
    opt = opt or {}
    with open(src, "r", encoding="utf-8") as f:
        raw = f.read()
    raw = raw.lstrip("\ufeff\u200b")
    lines = raw.split("\n")

    doc = new_doc(os.path.basename(src), opt)
    stats = dict(h1=0, h2=0, h3=0, tables=0, rows=0, code=0, quotes=0, bullets=0, paras=0)

    i = 0
    while i < len(lines):
        line = lines[i]
        s = line.strip()

        # 代码围栏
        if s.startswith("```"):
            buf = []
            i += 1
            while i < len(lines) and not lines[i].strip().startswith("```"):
                buf.append(lines[i].rstrip())
                i += 1
            i += 1
            code_block(doc, buf)
            stats["code"] += 1
            continue

        # 表格
        if s.startswith("|") and i + 1 < len(lines) and is_sep_row(lines[i + 1]):
            header = split_row(s)
            i += 2
            rows = []
            while i < len(lines) and lines[i].strip().startswith("|"):
                if not is_sep_row(lines[i]):
                    rows.append(split_row(lines[i]))
                i += 1
            table(doc, header, rows, opt)
            stats["tables"] += 1
            stats["rows"] += len(rows)
            continue

        # 标题
        m = re.match(r"^(#{1,6})\s+(.*)$", s)
        if m:
            lvl, txt = len(m.group(1)), m.group(2).strip()
            if lvl == 1:
                h1(doc, txt, opt)
                stats["h1"] += 1
            elif lvl == 2:
                h2(doc, txt, opt)
                stats["h2"] += 1
            elif re.match(r"^Q\d+", txt):
                # 问答题头：与下面的"一句话答案"保持在同一页，避免题干与答案被分页截断
                size = opt.get("h3", 11.5)
                p = doc.add_paragraph()
                p.paragraph_format.space_before = Pt(11)
                p.paragraph_format.space_after = Pt(3)
                p.paragraph_format.keep_with_next = True
                p.paragraph_format.keep_together = True
                add_inline(p, txt, size=size)
                for r in p.runs:
                    col = r.font.color.rgb if (r.font.color and r.font.color.type is not None) else BLUE
                    set_font(r, cn=CN_HEAD, size=size, bold=True, color=col)
                stats["h3"] += 1
            else:
                h3(doc, txt, opt)
                stats["h3"] += 1
            i += 1
            continue

        # 分隔线
        if re.match(r"^-{3,}$", s):
            hr(doc)
            i += 1
            continue

        # 引用块（合并连续行）
        if s.startswith(">"):
            buf = []
            while i < len(lines) and lines[i].strip().startswith(">"):
                buf.append(re.sub(r"^\s*>\s?", "", lines[i]).rstrip())
                i += 1
            text = " ".join(x.strip() for x in buf if x.strip())
            if text:
                quote(doc, text, opt)
                stats["quotes"] += 1
            continue

        # 复选框列表（必须先于普通列表匹配，否则 "- [x] 项" 会被当成普通列表项）
        m = re.match(r"^(\s*)[-*]\s+\[( |x|X)\]\s+(.*)$", line)
        if m:
            mark = "☑" if m.group(2).lower() == "x" else "☐"
            p = doc.add_paragraph()
            p.paragraph_format.left_indent = Cm(0.75 + 0.6 * (len(m.group(1)) // 2))
            p.paragraph_format.space_after = Pt(2)
            r = p.add_run(mark + " ")
            set_font(r, cn=CN_FONT, size=opt.get("body", 10.5))
            add_inline(p, m.group(3).strip(), size=opt.get("body", 10.5))
            stats["bullets"] += 1
            i += 1
            continue

        # 列表
        m = re.match(r"^(\s*)[-*]\s+(.*)$", line)
        if m:
            level = len(m.group(1)) // 2
            bullet(doc, m.group(2).strip(), level, opt)
            stats["bullets"] += 1
            i += 1
            continue

        # 空行
        if not s:
            i += 1
            continue

        body(doc, s, 0.0, opt)
        stats["paras"] += 1
        i += 1

    doc.save(dst)
    sha = hashlib.sha256(open(dst, "rb").read()).hexdigest()[:16]
    size = os.path.getsize(dst)
    print("[OK] %s -> %s  (%.0f KB, sha256:%s…)" % (os.path.basename(src), os.path.basename(dst), size / 1024, sha))
    print("     标题 %d/%d/%d · 表格 %d 个(%d 行) · 代码块 %d · 引用 %d · 列表项 %d · 段落 %d"
          % (stats["h1"], stats["h2"], stats["h3"], stats["tables"], stats["rows"],
             stats["code"], stats["quotes"], stats["bullets"], stats["paras"]))
    return stats


if __name__ == "__main__":
    total = dict(tables=0, rows=0, code=0)
    for src_name, dst_name, job_opt in JOBS:
        src = os.path.join(DOCS, src_name)
        dst = os.path.join(DOCS, dst_name)
        opt = dict(COMPACT) if job_opt.get("compact") else {}
        if not os.path.exists(src):
            print("[MISS] 找不到源文件：%s" % src)
            sys.exit(1)
        st = convert(src, dst, opt)
        for k in total:
            total[k] += st[k]
    print("\n合计：表格 %d 个 / 表格行 %d 行 / 代码块 %d 个" % (total["tables"], total["rows"], total["code"]))
