#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""生成演示视频所需的静态卡片（1920x1080）：片头 / 技术架构 / 性能测试 / 片尾。"""
import os
from PIL import Image, ImageDraw, ImageFont

CJK = r"C:\Windows\Fonts\msyh.ttc"
CJK_B = r"C:\Windows\Fonts\msyhbd.ttc"
W, H = 1920, 1080
OUT = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "output", "_video_work", "cards")

BG_TOP = (18, 32, 62)
BG_BOTTOM = (10, 18, 38)
ACCENT = (64, 158, 255)
ACCENT2 = (0, 214, 179)


def font(size, bold=False):
    return ImageFont.truetype(CJK_B if bold else CJK, size)


def gradient_bg():
    img = Image.new("RGB", (W, H), BG_TOP)
    d = ImageDraw.Draw(img)
    for y in range(H):
        k = y / H
        c = tuple(int(BG_TOP[i] + (BG_BOTTOM[i] - BG_TOP[i]) * k) for i in range(3))
        d.line([(0, y), (W, y)], fill=c)
    # 网格
    for x in range(0, W, 60):
        d.line([(x, 0), (x, H)], fill=(28, 46, 82))
    for y in range(0, H, 60):
        d.line([(0, y), (W, y)], fill=(28, 46, 82))
    return img


def center(d, text, f, y, fill=(255, 255, 255), x=None):
    w = d.textlength(text, font=f)
    d.text((x if x is not None else (W - w) / 2, y), text, font=f, fill=fill)
    return w


def card_title():
    img = gradient_bg()
    d = ImageDraw.Draw(img)
    d.rounded_rectangle([120, 380, 1800, 700], radius=24, fill=(12, 24, 48), outline=ACCENT, width=3)
    center(d, "通用流处理任务管理平台", font(96, True), 420)
    center(d, "可视化 DAG 编排 · Flink SQL 自动翻译 · 实时监控与智能告警", font(38), 560, (170, 200, 240))
    center(d, "MVP v1.0   |   2026-09", font(30), 630, ACCENT2)
    img.save(os.path.join(OUT, "title.png"))


def card_arch():
    img = gradient_bg()
    d = ImageDraw.Draw(img)
    center(d, "技术实现方式", font(60, True), 70)
    center(d, "前端画布 → DAG 契约 → 翻译层 → Flink 集群 → 输出与监控", font(34), 155, (170, 200, 240))
    layers = [
        ("前端", "Vue 3 + AntV X6 画布 · Element Plus · 31 个内置控件拖拽编排", ACCENT),
        ("后端", "Spring Boot 3.2 + JDK 17；DagTranslationService 把 DAG 翻译成 Flink SQL，三路降级提交", ACCENT2),
        ("执行层", "Flink 1.18 Standalone：JobManager / TaskManager / SQL Gateway，作业真实运行", (255, 170, 80)),
        ("元数据", "H2 / MySQL 12 张表：作业·版本·日志·调度·趋势·告警·诊断·用户·审计", (190, 130, 255)),
    ]
    y = 250
    for name, desc, color in layers:
        d.rounded_rectangle([140, y, 1780, y + 150], radius=18, fill=(12, 24, 48), outline=color, width=3)
        d.rounded_rectangle([140, y, 400, y + 150], radius=18, fill=color)
        d.text((200, y + 48), name, font=font(46, True), fill=(10, 20, 40))
        d.text((440, y + 40), desc, font=font(31), fill=(226, 236, 250))
        y += 175
    center(d, "后端 166 项 + 前端 49 项自动化测试，CI 全模块验证", font(30), 990, (150, 180, 220))
    img.save(os.path.join(OUT, "arch.png"))


def card_perf():
    img = gradient_bg()
    d = ImageDraw.Draw(img)
    center(d, "性能测试方案与结果", font(60, True), 60)
    center(d, "真实提交 Flink 作业统计端到端吞吐（benchmark.py），非单元基准", font(32), 145, (170, 200, 240))

    # 表格
    x0, y0, cw, rh = 300, 240, 330, 96
    headers = ["数据量", "并行度 1", "并行度 2", "并行度 4"]
    rows = [["100,000 行", "11,628 行/s", "18,774 行/s", "49,032 行/s"],
            ["1,000,000 行", "76,139 行/s", "112,015 行/s", "217,795 行/s"]]
    for i, htxt in enumerate(headers):
        d.rounded_rectangle([x0 + i * cw, y0, x0 + (i + 1) * cw - 12, y0 + rh], radius=12, fill=ACCENT)
        center(d, htxt, font(34, True), y0 + 26, (10, 20, 40), x=x0 + i * cw + 20)
    for r, row in enumerate(rows):
        for i, cell in enumerate(row):
            yy = y0 + rh + 16 + r * (rh + 16)
            d.rounded_rectangle([x0 + i * cw, yy, x0 + (i + 1) * cw - 12, yy + rh], radius=12,
                                fill=(16, 30, 58) if i == 0 else (14, 40, 44), outline=(60, 90, 130), width=2)
            center(d, cell, font(34, True if i == 3 else False), yy + 26, ACCENT2 if i == 3 else (226, 236, 250),
                   x=x0 + i * cw + 20)
    center(d, "测试机 32 vCPU / 16 GB，CSV 透传，2026-08 实测", font(28), 596, (150, 180, 220))
    center(d, "并行度 1 → 4 吞吐接近线性提升：翻译层生成的 SQL 与 Sink 配置未成为瓶颈", font(34), 664, (255, 255, 255))
    center(d, "百万行数据并行度 4 约 4.6 秒完成；另有 20 万条全链路验收脚本与 15 万行四格式大数据集", font(30), 736, (170, 200, 240))
    d.rounded_rectangle([300, 800, 1620, 950], radius=16, fill=(12, 24, 48), outline=ACCENT2, width=3)
    d.text((340, 830), "评审要点① 方案：benchmark.py 经 REST 建作业 → 提交真实 Flink → completedAt−submittedAt 计吞吐",
           font=font(30), fill=(226, 236, 250))
    d.text((340, 880), "评审要点② 结果：见上表（报告 test-results/benchmark_report.md）", font=font(30), fill=(226, 236, 250))
    img.save(os.path.join(OUT, "perf.png"))


def card_end():
    img = gradient_bg()
    d = ImageDraw.Draw(img)
    center(d, "谢谢观看", font(92, True), 300)
    center(d, "通用流处理任务管理平台 · 可视化编排 + Flink SQL 自动翻译 + 实时监控告警", font(36), 440, (170, 200, 240))
    lines = [
        "前端 3000 · 后端 18080 · Flink 18081 · SQL Gateway 18083",
        "Spring Boot 3.2 · Flink 1.18 · Vue 3 + AntV X6 · H2 / MySQL",
        "31 个内置控件 · JWT + RBAC + 审计 · AI 智能体 · 215 项自动化测试",
    ]
    y = 560
    for ln in lines:
        center(d, ln, font(32), y, ACCENT2)
        y += 70
    img.save(os.path.join(OUT, "end.png"))


if __name__ == "__main__":
    os.makedirs(OUT, exist_ok=True)
    card_title(); card_arch(); card_perf(); card_end()
    print("卡片已生成:", OUT)
    for f in sorted(os.listdir(OUT)):
        print("  ", f, os.path.getsize(os.path.join(OUT, f)) // 1024, "KB")
