# 字段拼接（field_concat）测试数据

> 由 `scripts/gen-field-concat-testdata.py` 生成（确定性数据，可重复执行）
> 校验：`python scripts/verify-field-concat-testdata.py`；翻译层回归：`mvn -B test -Dtest=DagFieldConcatDataTest`

## 文件清单

| 文件 | 说明 |
|------|------|
| concat_source.csv | 源数据 24 行 / 12 列，逐行覆盖拼接边界 |
| concat_source_slim.csv | 源数据 5 行 / 3 列（短表，便于人工核对） |
| concat_expected_1.csv | 01 场景期望输出：源全部列 + `category_channel` = category + "," + channel（24 行） |
| concat_expected_2.csv | 02 场景期望输出：源全部列 + `amount_day` = amount + "\|" + join_day（24 行） |
| concat_expected_3.csv | 03 场景期望输出：空值校验后剩 3 行 + `name_channel` = src_name + " \| " + channel |

DAG（前端「导入 JSON」直接用，路径为相对仓库根）：

| DAG | 链路 | 拼接配置 |
|-----|------|----------|
| 02-csv-concat-csv.json | CSV → 字段拼接 → CSV | fields=category,channel，separator=','，newFieldName=category_channel |
| 02b-csv-concat-types.json | CSV → 字段拼接 → CSV | fields=amount,join_day，separator='\|'，newFieldName=amount_day |
| 02c-csv-concat-slim-validate.json | CSV → 字段拼接 → 空值校验 → CSV | fields=src_name,channel，separator=' \| '，newFieldName=name_channel |

## 期望值对照表（01 场景：`category_channel` = `category` + "," + `channel`）

| # | category | channel | 期望 category_channel | 备注 |
|---|----------|---------|----------------------|------|
| 1 | 图书 | 线上 | `图书,线上` |  |
| 2 | 服饰 | 线下 | `服饰,线下` |  |
| 3 | 家电 | 直播 | `家电,直播` |  |
| 4 | 美妆 | 跨境 | `美妆,跨境` |  |
| 5 | 数码 | 线上 | `数码,线上` |  |
| 6 | 家居 | 线下 | `家居,线下` |  |
| 7 | 食品 | 直播 | `食品,直播` |  |
| 8 | 图书 | 跨境 | `图书,跨境` |  |
| 9 | 服饰 | 线上 | `服饰,线上` |  |
| 10 | 家电 | 线下 | `家电,线下` | 金额 0.00（CAST 保真，不丢小数位） |
| 11 | 美妆 | 直播 | `美妆,直播` |  |
| 12 | 数码 | 跨境 | `数码,跨境` | src_name 空字符串（Flink 读成 NULL） |
| 13 | 家居 | 线上 | `家居,线上` |  |
| 14 | 食品 | 线下 | `食品,线下` |  |
| 15 | 图书 | 直播 | `图书,直播` |  |
| 16 | 服饰 | 跨境 | `服饰,跨境` |  |
| 17 | 家电 | 线上 | `家电,线上` |  |
| 18 | 美妆 | 线下 | `美妆,线下` |  |
| 19 | 数码 | 直播 | `数码,直播` | amount 为空 → 02 场景过滤命中 |
| 20 | 家居 | 跨境 | `家居,跨境` | amount 为空 → 02 场景过滤命中 |
| 21 | 食品 | 线上 | `食品,线上` | amount 为空 → 02 场景过滤命中 |
| 22 | 图书 | 线下 | `图书,线下` | 长文本备注（含全角逗号/分号，拼接结果 200+ 字符） |
| 23 | 服饰 | 直播 | `服饰,直播` | 备注含半角分号/竖线/冒号（即分隔符本身出现在字段值里） |
| 24 | 家电 | 跨境 | `家电,跨境` | 备注含斜杠 / 与 (v2) 括号 |

## 用途

1. **画布手工验证**：拖 CSV 输入 → 字段拼接 → CSV 输出，`fields` 填 `category,channel`、
   `separator` 填 `,`、`newFieldName` 填 `category_channel`，输入路径选 `concat_source.csv`，
   跑完把输出与 `concat_expected_1.csv` 逐行对比。
2. **导入 JSON 快验**：直接导入 `02-csv-concat-csv.json`，提交运行后对比期望文件。
3. **一键实跑对比**（后端 + Flink 已启动时）：

   ```powershell
   pwsh scripts/run-field-concat-demo.ps1                              # 场景 01（单场景，PowerShell 版）
   python scripts/run-field-concat-e2e.py --base http://localhost:18080 --user admin
   ```

   后者会把三个场景依次真实提交到 Flink，跑完与期望文件逐行逐列比对，并自动生成
   `test-results/field_concat_report.md`（含真实作业号 / Flink Job ID / 行数 / 耗时，可直接作为测试报告）。
   两个脚本都把 DAG 的输出改写到 `output/` 下再提交，绝不覆盖期望值文件。
4. **数据自校验**（纯离线，不需要后端）：

   ```powershell
   python scripts/verify-field-concat-testdata.py
   ```

5. **翻译层回归**：`DagFieldConcatDataTest` 把这三个 DAG 真实喂给 `DagTranslationService.translate()`，
   断言生成的 Flink SQL 含正确 CONCAT 表达式、schema 已传播出拼接列、且不含 `CONCAT([` 之类的非法语法。

   ```powershell
   cd backend; mvn -B test -Dtest=DagFieldConcatDataTest
   ```

## 路径注意

DAG 里的路径是**相对仓库根**的（与 `dags/` 下其它测试 DAG 一致）：后端从仓库根启动时可直接提交；
从 `backend/` 目录启动或用单测加载时，需要先把相对路径补成绝对路径（两个实跑脚本与 `DagFieldConcatDataTest` 都已按此处理）。

## 一个实测提醒：CSV 空字段是「空字符串」，不是 NULL

`concat_source.csv` 里 `amount` / `src_name` 为空白的行，被 Flink 的 csv 源读成**空字符串**——
实测证据：拿这份数据把 `order_id` 与 `amount` 拼出来，空值行的结果是 `A0019|`（`CONCAT` 遇 NULL 应整列为 NULL）。
所以用行过滤挑这些行时，条件要写成 `amount IS NULL OR amount = ''`（或 `TRIM(amount) = ''`）才稳；
只写 `amount IS NULL` 命中不到（本机实测该链路还为 0 行，根因未定位，见 `test-results/field_concat_report.md` 第六节）。
`validate`（空值校验）控件本身已同时判 `IS NULL` 与 `<> ''`，不受该差异影响。
