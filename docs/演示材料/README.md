# 演示材料

本目录放**体积小、需要跟着代码走**的演示物料；成片视频体积大（174MB / 10MB），不适合进仓库，
已作为 **GitHub Release 附件**发布（见仓库 Releases 页）。

| 文件 | 说明 |
|------|------|
| `title.png` / `arch.png` / `perf.png` / `end.png` | 成片用的四张卡片（片头 / 技术实现 / 性能测试 / 片尾），1920×1080，可单独用于 PPT |
| `旁白文稿.md` / `旁白文稿.docx` | 按成片 13 段整理的「对着念」旁白稿（含实测语速核算、录制注意） |
| `作品演示录像.srt` | 与成片时间轴对齐的字幕（19 条，UTF-8，剪映/PR 可直接导入） |
| `口播稿_注册功能.md` | 注册功能口播稿（极简/精简/主推/详版四档 + 配合画面表） |
| `口播稿_画布拖拽演示.md` | 画布拖拽（CSV 输入 → Excel 输出）口播稿四档 + 演示步骤 |
| `作品演示录像脚本.md`（上一级目录） | 8 分钟完整分镜脚本（13 段 + 口播 + 操作清单） |

## 复现 / 重新剪辑

剪辑与卡片生成脚本都在 `scripts/` 下（依赖 `ffmpeg`，可用 `pip install imageio-ffmpeg` 提供）：

```bash
python scripts/make_video_cards.py       # 生成四张卡片（含排版越界自检）
python scripts/build_demo_video.py       # 按 TIMELINE 剪辑决策表出成片 + 字幕 SRT
python scripts/build_demo_video.py --burn  # 额外输出烧字幕版本
python scripts/make_narration_script.py  # 生成「对着念」旁白文稿（md + docx）
python scripts/video_tool.py sheet <video> <out.jpg> --every 15   # 抽帧素材地图
```
