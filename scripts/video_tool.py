#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
视频素材工具：抽帧 → 生成带时间戳的"素材地图"（contact sheet），用于剪辑定位与成片质检。

用法：
  python scripts/video_tool.py sheet <video> <out.jpg> [--every 15] [--cols 5] [--rows 6] [--width 320]
  python scripts/video_tool.py probe <video>
  python scripts/video_tool.py cut   <video> <out.mp4> <start> <end> [--crop x:y:w:h]
  python scripts/video_tool.py concat <list.txt> <out.mp4>
"""
import os
import re
import subprocess
import sys

import imageio_ffmpeg
from PIL import Image, ImageDraw, ImageFont

FFMPEG = imageio_ffmpeg.get_ffmpeg_exe()
CJK_FONT = r"C:\Windows\Fonts\msyh.ttc"


def run(args):
    return subprocess.run([FFMPEG, "-hide_banner", "-loglevel", "error", "-y"] + args,
                          capture_output=True, text=True, errors="replace")


def probe(video):
    cp = subprocess.run([FFMPEG, "-hide_banner", "-i", video],
                        capture_output=True, text=True, errors="replace")
    out = cp.stderr or cp.stdout or ""
    info = {}
    m = re.search(r"Duration: (\d+):(\d+):(\d+)\.(\d+)", out)
    if m:
        info["duration"] = int(m.group(1)) * 3600 + int(m.group(2)) * 60 + int(m.group(3))
    m = re.search(r"Stream #0:0.*?, (\d+x\d+)", out)
    if m:
        info["size"] = m.group(1)
    m = re.search(r"(\d+(?:\.\d+)?) fps", out)
    if m:
        info["fps"] = float(m.group(1))
    info["has_audio"] = "Audio:" in out
    return info


def hms(seconds):
    return f"{int(seconds) // 60:02d}:{int(seconds) % 60:02d}"


def extract_frames(video, every, workdir):
    os.makedirs(workdir, exist_ok=True)
    for f in os.listdir(workdir):
        os.remove(os.path.join(workdir, f))
    # 统一抽帧：每 every 秒一帧，缩放到 480 宽（够看清界面文字）
    r = run(["-i", video, "-vf", f"fps=1/{every},scale=480:-1", "-q:v", "3",
             os.path.join(workdir, "f_%04d.jpg")])
    if r.returncode != 0:
        raise RuntimeError(r.stderr)
    return sorted(os.listdir(workdir))


def make_sheet(video, out_jpg, every=15, cols=5, max_tiles=30):
    workdir = os.path.join(os.path.dirname(out_jpg), "_frames_" + os.path.basename(out_jpg).split(".")[0])
    frames = extract_frames(video, every, workdir)
    frames = frames[:max_tiles]
    if not frames:
        raise RuntimeError("no frames extracted")
    tw, th = Image.open(os.path.join(workdir, frames[0])).size
    rows = (len(frames) + cols - 1) // cols
    label_h = 22
    sheet = Image.new("RGB", (cols * tw, rows * (th + label_h)), "#111111")
    draw = ImageDraw.Draw(sheet)
    font = ImageFont.truetype(CJK_FONT, 16)
    for i, name in enumerate(frames):
        img = Image.open(os.path.join(workdir, name))
        x, y = (i % cols) * tw, (i // cols) * (th + label_h)
        sheet.paste(img, (x, y))
        t = hms(i * every)
        draw.rectangle([x, y + th, x + tw, y + th + label_h], fill="#000000")
        draw.text((x + 6, y + th + 2), f"{t}  ({i * every}s)", font=font, fill="#ffff00")
    sheet.save(out_jpg, quality=88)
    return out_jpg, len(frames)


def cut(video, out, start, end, crop=None, size="1920:1080", fps=30):
    vf = []
    if crop:
        vf.append(f"crop={crop}")
    vf.append(f"scale={size}")
    vf.append(f"fps={fps}")
    r = run(["-ss", str(start), "-to", str(end), "-i", video, "-vf", ",".join(vf),
             "-c:v", "libx264", "-preset", "medium", "-crf", "20", "-pix_fmt", "yuv420p",
             "-c:a", "aac", "-b:a", "160k", "-movflags", "+faststart", out])
    if r.returncode != 0 or not os.path.exists(out) or os.path.getsize(out) == 0:
        raise RuntimeError(f"切片失败 {video} [{start}-{end}] -> {out}: {(r.stderr or '')[-400:]}")
    return out


def concat(list_file, out):
    run(["-f", "concat", "-safe", "0", "-i", list_file, "-c", "copy", "-movflags", "+faststart", out])
    return out


if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else ""
    if cmd == "probe":
        print(probe(sys.argv[2]))
    elif cmd == "sheet":
        every = 15
        if "--every" in sys.argv:
            every = int(sys.argv[sys.argv.index("--every") + 1])
        print(make_sheet(sys.argv[2], sys.argv[3], every=every))
    elif cmd == "cut":
        args = sys.argv[2:]
        crop = args[4] if len(args) > 4 and args[4].startswith("--crop=") else None
        if crop:
            crop = crop.split("=", 1)[1]
        print(cut(args[0], args[1], args[2], args[3], crop=crop))
    elif cmd == "concat":
        print(concat(sys.argv[2], sys.argv[3]))
    else:
        print(__doc__)
