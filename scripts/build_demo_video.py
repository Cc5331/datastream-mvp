#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
按 5 分钟赛制剪辑《作品演示录像》：
  卡片（片头/架构/性能/片尾，由 make_video_cards.py 生成）+ 三段录屏的可片段落 → 拼接 → 成片 + 字幕 SRT

素材（均 1920x1080 / 60fps）：
  A = ~/Videos/NVIDIA/Desktop/Desktop 2026.09.17 - 09.29.23.03.mp4   7:11
  B = ~/Videos/NVIDIA/Desktop/Desktop 2026.09.17 - 10.04.55.05.mp4   8:06
  AI= ~/Desktop/9月17日.mp4                                          0:57

段落锚点均经"抽帧逐点核对"确认（见 output/_video_work/qc_segs.jpg）：
  A@72-84   工作台（统计卡 + 最近作业 + 快捷入口）
  B@276-286 作业管理列表          B@4-26    画布编排（控件库 + 画布 + 参数面板）
  B@381-392 参数与提交（画布+参数） B@158-172 作业运行结果（列表状态）
  B@209-216 控件库（31 个控件清单） AI 四段   生成 / 画布 / 人工确认 / 落库

用法：
  python scripts/build_demo_video.py            # 出成片（无音轨，供配旁白）
  python scripts/build_demo_video.py --burn     # 额外输出烧字幕版本
"""
import os
import re
import subprocess
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from video_tool import cut, concat, probe  # noqa: E402

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
WORK = os.path.join(ROOT, "output", "_video_work")
CARDS = os.path.join(WORK, "cards")
HOME = os.path.expanduser("~")
SRC_A = os.path.join(HOME, "Videos", "NVIDIA", "Desktop", "Desktop 2026.09.17 - 09.29.23.03.mp4")
SRC_B = os.path.join(HOME, "Videos", "NVIDIA", "Desktop", "Desktop 2026.09.17 - 10.04.55.05.mp4")
SRC_AI = os.path.join(HOME, "Desktop", "9月17日.mp4")

# ---------------------------------------------------------------- 剪辑决策表
# ("card", 卡片文件名, 时长, 段落名, 旁白) / ("src", 源代号, 起, 止, 段落名, 旁白)
TIMELINE = [
    ("card", "title.png", 10, "片头",
     "通用流处理任务管理平台——把画布编排、Flink SQL 翻译、集群运行、结果落库与实时监控做成了一站式平台。"),
    ("card", "arch.png", 22, "技术实现方式",
     "技术实现上分四层：前端用 Vue 3 与 AntV X6 做 DAG 画布；后端 Spring Boot 把画布 DAG 翻译成 Flink SQL；执行层是 Flink 1.18 集群；元数据用 H2 或 MySQL 保存作业、版本、日志、告警与审计。"),
    ("src", "A", 72.0, 84.0, "工作台",
     "登录后进入工作台：总作业数、今日新增与最近作业一目了然，权限分管理员、操作员与观察员三种角色。"),
    ("src", "B", 276.0, 286.0, "作业管理",
     "作业管理页能看到每个作业的状态、耗时与错误详情，支持搜索、状态筛选、详情抽屉与一键复制。"),
    ("src", "B", 4.0, 26.0, "画布编排",
     "核心是可视化编排。平台内置 31 个控件，覆盖文件、JDBC、Kafka、HDFS 的输入输出，以及字段拼接、过滤、去重、空值校验等转换能力；从左侧拖拽控件到画布，连线定义数据流向，右侧参数面板按控件 Schema 自动渲染。"),
    ("src", "B", 381.0, 392.0, "参数与提交",
     "参数填好后保存并提交：后端把 DAG 翻译成 Flink SQL 提交到集群，状态每 5 秒回写；提交前还有静态检查兜住无效作业。"),
    ("src", "B", 158.0, 172.0, "作业运行结果",
     "作业运行结果与失败原因都会回写到列表，失败作业可以点开看日志，也可以一键诊断。"),
    ("src", "AI", 0.8, 8.6, "AI 生成 DAG 1/4",
     "AI 助手支持自然语言生成数据流：输入一句需求，系统依据控件注册表生成合法的 DAG。"),
    ("src", "AI", 9.6, 19.0, "AI 生成 DAG 2/4",
     "生成结果直接落到画布上：三个节点、两条连线，参数也已经按 Schema 填好。"),
    ("src", "AI", 20.0, 31.0, "AI 生成 DAG 3/4",
     "注意这里是关键的安全设计：AI 产物只是草稿，必须人工确认后才能提交运行，服务端强制校验，客户端绕不过去。"),
    ("src", "AI", 46.8, 56.5, "AI 生成 DAG 4/4",
     "这是生成作业真实跑完的结果：MySQL 表里出现了 AI 拼出来的 category_channel 列，2971 行数据。"),
    ("card", "perf.png", 24, "性能测试",
     "性能测试跑的是真实链路：脚本通过后端接口建作业、提交到真实 Flink 集群，按端到端耗时换算吞吐。10 万行并行度 4 达到每秒 4.9 万行，100 万行达到每秒 21.7 万行，并行度提升接近线性。"),
    ("card", "end.png", 5, "片尾",
     "以上就是我们的作品，谢谢观看。"),
]

SRC_MAP = {"A": SRC_A, "B": SRC_B, "AI": SRC_AI}
# AI 录屏原始文件若被移走，退回使用先前从源文件切好的片段（ai_part1..4.mp4，工作目录内）
AI_FALLBACK = [os.path.join(WORK, f"ai_part{i}.mp4") for i in (1, 2, 3, 4)]


def seg_duration(item):
    return item[2] if item[0] == "card" else item[3] - item[2]


def hms(sec):
    h, m = int(sec // 3600), int(sec % 3600 // 60)
    return f"{h:02d}:{m:02d}:{sec % 60:06.3f}".replace(".", ",")


def build(burn=False, reuse=True):
    os.makedirs(WORK, exist_ok=True)
    parts, cues, t = [], [], 0.0
    ai_idx = 0
    for item in TIMELINE:
        dur = seg_duration(item)
        if item[0] == "card":
            _, name, _, label, narration = item
            out = os.path.join(WORK, "part_" + name.replace(".png", "") + ".mp4")
            if not (reuse and os.path.exists(out) and os.path.getsize(out) > 0):
                subprocess.run([__import__("imageio_ffmpeg").get_ffmpeg_exe(), "-hide_banner",
                                "-loglevel", "error", "-y", "-loop", "1", "-i", os.path.join(CARDS, name),
                                "-t", str(dur), "-vf", "fps=30,format=yuv420p", "-c:v", "libx264",
                                "-preset", "medium", "-crf", "20", "-movflags", "+faststart", out], check=True)
        else:
            _, src, start, end, label, narration = item
            safe = re.sub(r"[^\w\u4e00-\u9fff]+", "_", label)[:8]
            out = os.path.join(WORK, f"part_{safe}_{int(start)}_{int(end)}.mp4")
            if src == "AI" and not os.path.exists(SRC_AI):
                # 原始 AI 录屏不在原位：直接用先前切好的片段（保持内容一致）
                src_file = AI_FALLBACK[ai_idx]
                if not os.path.exists(src_file):
                    raise FileNotFoundError(f"AI 录屏原件与备份片段都不存在：{SRC_AI} / {src_file}")
                if not (reuse and os.path.exists(out) and os.path.getsize(out) > 0):
                    cut(src_file, out, 0, 99)
                ai_idx += 1
            else:
                if not (reuse and os.path.exists(out) and os.path.getsize(out) > 0):
                    cut(SRC_MAP[src], out, start, end)
        parts.append(out)
        cues.append((t, t + dur, narration))
        t += dur
        print(f"  {label:18s} {t - dur:6.1f}s → {t:6.1f}s")

    lst = os.path.join(WORK, "final_list.txt")
    with open(lst, "w", encoding="utf-8") as f:
        for p in parts:
            f.write("file '%s'\n" % os.path.abspath(p).replace("\\", "/"))
    master = os.path.join(WORK, "作品演示录像_5min.mp4")
    concat(lst, master)
    print("\n  成片:", probe(master), master)

    srt = os.path.join(WORK, "作品演示录像.srt")
    idx = 1
    with open(srt, "w", encoding="utf-8") as f:
        for start, end, text in cues:
            if len(text) > 44:
                mid = len(text) // 2
                cut_at = text.find("，", mid)
                if cut_at < 0:
                    cut_at = text.find("；", mid)
                if cut_at < 0:
                    cut_at = mid
                chunks = [text[:cut_at + 1], text[cut_at + 1:]]
            else:
                chunks = [text]
            span = (end - start) / len(chunks)
            for i, c in enumerate(chunks):
                f.write(f"{idx}\n{hms(start + i * span)} --> {hms(start + (i + 1) * span)}\n{c.strip()}\n\n")
                idx += 1
    print("  字幕:", srt)

    if burn:
        burned = os.path.join(WORK, "作品演示录像_5min_带字幕.mp4")
        srt_esc = srt.replace("\\", "/").replace(":", "\\:")
        subprocess.run([__import__("imageio_ffmpeg").get_ffmpeg_exe(), "-hide_banner", "-loglevel", "error", "-y",
                        "-i", master, "-vf",
                        f"subtitles='{srt_esc}':force_style='FontName=Microsoft YaHei,FontSize=20,"
                        "PrimaryColour=&H00FFFFFF,OutlineColour=&H80000000,BorderStyle=3,Outline=1,Shadow=0,MarginV=28'",
                        "-c:v", "libx264", "-preset", "medium", "-crf", "21", "-c:a", "copy",
                        "-movflags", "+faststart", burned], check=True)
        print("  烧字幕版:", probe(burned), burned)
    return master


if __name__ == "__main__":
    build(burn="--burn" in sys.argv)
