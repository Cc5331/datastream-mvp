#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""生成演示视频所需的静态卡片（1920x1080）：片头 / 技术架构 / 性能测试 / 片尾。

排版规则：所有文字都走 center_fit / left_fit —— 按**实测文本宽度自动缩放字号**，
永不超出安全边距；任何越界都会打印 [overflow] 告警（历史 bug：性能卡评审要点那行字出框）。
"""
import os
from PIL import Image, ImageDraw, ImageFont

CJK = r"C:\Windows\Fonts\msyh.ttc"
CJK_B = r"C:\Windows\Fonts\msyhbd.ttc"
W, H = 1920, 1080
MARGIN = 60                      # 安全边距：任何文字不得越过
BOX_L, BOX_R = 140, 1780         # 卡片内主内容区（与架构卡分层框对齐）
OUT = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "output", "_video_work", "cards")

BG_TOP = (18, 32, 62)
BG_BOTTOM = (10, 18, 38)
ACCENT = (64, 158, 255)
ACCENT2 = (0, 214, 179)

_MEASURE = ImageDraw.Draw(Image.new("RGB", (8, 8)))
_ISSUES = []


def font(size, bold=False):
    return ImageFont.truetype(CJK_B if bold else CJK, size)


def width_of(text, f):
    return _MEASURE.textlength(text, font=f)


def fit_font(text, size, max_width, bold=False, floor=15):
    """逐磅缩小字号直到文本宽度不超过 max_width。"""
    while size > floor:
        f = font(size, bold)
        if width_of(text, f) <= max_width:
            return f
        size -= 1
    return font(floor, bold)


def guard(text, x, w, max_width):
    if x < MARGIN - 1 or x + w > W - MARGIN + 1 or w > max_width + 1:
        _ISSUES.append(f"overflow: x={x:.0f} w={w:.0f} max={max_width:.0f} :: {text[:30]}")


def center_fit(d, text, size, y, fill=(255, 255, 255), max_width=None, bold=False):
    max_width = max_width or (W - 2 * MARGIN)
    f = fit_font(text, size, max_width, bold)
    w = width_of(text, f)
    x = (W - w) / 2
    guard(text, x, w, max_width)
    d.text((x, y), text, font=f, fill=fill)
    return f


def left_fit(d, text, size, y, x, max_width, fill=(226, 236, 250), bold=False):
    f = fit_font(text, size, max_width, bold)
    w = width_of(text, f)
    guard(text, x, w, max_width)
    d.text((x, y), text, font=f, fill=fill)
    return f


def gradient_bg():
    img = Image.new("RGB", (W, H), BG_TOP)
    d = ImageDraw.Draw(img)
    for y in range(H):
        k = y / H
        c = tuple(int(BG_TOP[i] + (BG_BOTTOM[i] - BG_TOP[i]) * k) for i in range(3))
        d.line([(0, y), (W, y)], fill=c)
    for x in range(0, W, 60):
        d.line([(x, 0), (x, H)], fill=(28, 46, 82))
    for y in range(0, H, 60):
        d.line([(0, y), (W, y)], fill=(28, 46, 82))
    return img


def card_title():
    img = gradient_bg()
    d = ImageDraw.Draw(img)
    d.rounded_rectangle([BOX_L, 380, BOX_R, 700], radius=24, fill=(12, 24, 48), outline=ACCENT, width=3)
    center_fit(d, "通用流处理任务管理平台", 96, 420, bold=True, max_width=BOX_R - BOX_L - 80)
    center_fit(d, "可视化 DAG 编排 · Flink SQL 自动翻译 · 实时监控与智能告警", 38, 560,
               (170, 200, 240), max_width=BOX_R - BOX_L - 80)
    center_fit(d, "MVP v1.0   |   2026-09", 30, 630, ACCENT2)
    img.save(os.path.join(OUT, "title.png"))


def card_arch():
    img = gradient_bg()
    d = ImageDraw.Draw(img)
    center_fit(d, "技术实现方式", 60, 70, bold=True)
    center_fit(d, "前端画布 → DAG 契约 → 翻译层 → Flink 集群 → 输出与监控", 34, 155, (170, 200, 240),
               max_width=W - 4 * MARGIN)
    layers = [
        ("前端", "Vue 3 + AntV X6 画布 · Element Plus · 31 个内置控件拖拽编排", ACCENT),
        ("后端", "Spring Boot 3.2 + JDK 17；DagTranslationService 把 DAG 翻译成 Flink SQL，三路降级提交", ACCENT2),
        ("执行层", "Flink 1.18 Standalone：JobManager / TaskManager / SQL Gateway，作业真实运行", (255, 170, 80)),
        ("元数据", "H2 / MySQL 12 张表：作业·版本·日志·调度·趋势·告警·诊断·用户·审计", (190, 130, 255)),
    ]
    y = 250
    for name, desc, color in layers:
        d.rounded_rectangle([BOX_L, y, BOX_R, y + 150], radius=18, fill=(12, 24, 48), outline=color, width=3)
        d.rounded_rectangle([BOX_L, y, 400, y + 150], radius=18, fill=color)
        d.text((200, y + 48), name, font=font(46, True), fill=(10, 20, 40))
        left_fit(d, desc, 31, y + 46, 440, BOX_R - 440 - 40)
        y += 175
    center_fit(d, "后端 166 项 + 前端 49 项自动化测试，CI 全模块验证", 30, 990, (150, 180, 220))
    img.save(os.path.join(OUT, "arch.png"))


def card_perf():
    img = gradient_bg()
    d = ImageDraw.Draw(img)
    center_fit(d, "性能测试方案与结果", 60, 60, bold=True)
    center_fit(d, "真实提交 Flink 作业统计端到端吞吐（benchmark.py），非单元基准", 32, 145, (170, 200, 240),
               max_width=W - 4 * MARGIN)

    x0, y0, cw, rh = 300, 235, 330, 92
    headers = ["数据量", "并行度 1", "并行度 2", "并行度 4"]
    rows = [["100,000 行", "11,628 行/s", "18,774 行/s", "49,032 行/s"],
            ["1,000,000 行", "76,139 行/s", "112,015 行/s", "217,795 行/s"]]
    for i, htxt in enumerate(headers):
        d.rounded_rectangle([x0 + i * cw, y0, x0 + (i + 1) * cw - 12, y0 + rh], radius=12, fill=ACCENT)
        left_fit(d, htxt, 34, y0 + 24, x0 + i * cw + 20, cw - 60, (10, 20, 40), bold=True)
    for r, row in enumerate(rows):
        for i, cell in enumerate(row):
            yy = y0 + rh + 14 + r * (rh + 14)
            d.rounded_rectangle([x0 + i * cw, yy, x0 + (i + 1) * cw - 12, yy + rh], radius=12,
                                fill=(16, 30, 58) if i == 0 else (14, 40, 44), outline=(60, 90, 130), width=2)
            left_fit(d, cell, 34, yy + 24, x0 + i * cw + 20, cw - 60,
                     ACCENT2 if i == 3 else (226, 236, 250), bold=(i == 3))

    center_fit(d, "测试机 32 vCPU / 16 GB，CSV 透传，2026-08 实测", 28, 588, (150, 180, 220))
    center_fit(d, "并行度 1 → 4 吞吐接近线性提升：翻译层生成的 SQL 与 Sink 配置未成为瓶颈", 34, 652,
               (255, 255, 255), max_width=W - 4 * MARGIN)
    center_fit(d, "百万行数据并行度 4 约 4.6 秒完成；另有 20 万条全链路验收脚本与 15 万行四格式大数据集", 30, 718,
               (170, 200, 240), max_width=W - 4 * MARGIN)

    # 评审要点框：加宽到与其它卡一致，文字左边距留 40，右侧同样留 40
    box_top, box_bottom = 786, 952
    d.rounded_rectangle([BOX_L, box_top, BOX_R, box_bottom], radius=16, fill=(12, 24, 48), outline=ACCENT2, width=3)
    left_fit(d, "评审要点① 方案：benchmark.py 经 REST 建作业 → 提交真实 Flink → 按端到端耗时计吞吐",
             30, box_top + 32, BOX_L + 40, BOX_R - BOX_L - 80)
    left_fit(d, "评审要点② 结果：见上表（完整报告 test-results/benchmark_report.md）",
             30, box_top + 84, BOX_L + 40, BOX_R - BOX_L - 80)
    img.save(os.path.join(OUT, "perf.png"))


def card_end():
    img = gradient_bg()
    d = ImageDraw.Draw(img)
    center_fit(d, "谢谢观看", 92, 300, bold=True)
    center_fit(d, "通用流处理任务管理平台 · 可视化编排 + Flink SQL 自动翻译 + 实时监控告警", 36, 440,
               (170, 200, 240), max_width=W - 4 * MARGIN)
    lines = [
        "前端 3000 · 后端 18080 · Flink 18081 · SQL Gateway 18083",
        "Spring Boot 3.2 · Flink 1.18 · Vue 3 + AntV X6 · H2 / MySQL",
        "31 个内置控件 · JWT + RBAC + 审计 · AI 智能体 · 215 项自动化测试",
    ]
    y = 560
    for ln in lines:
        center_fit(d, ln, 32, y, ACCENT2, max_width=W - 4 * MARGIN)
        y += 70
    img.save(os.path.join(OUT, "end.png"))


if __name__ == "__main__":
    os.makedirs(OUT, exist_ok=True)
    card_title(); card_arch(); card_perf(); card_end()
    print("卡片已生成:", OUT)
    for f in sorted(os.listdir(OUT)):
        print("  ", f, os.path.getsize(os.path.join(OUT, f)) // 1024, "KB")
    if _ISSUES:
        print("\n[排版告警] 共", len(_ISSUES), "处越界：")
        for s in _ISSUES:
            print("  ", s)
        raise SystemExit(1)
    print("\n排版自检通过：无文字越界")
