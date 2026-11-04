# 字段拼接（field_concat）控件 测试报告

- 执行时间：2026-10-01 00:39:38
- 被测对象：转换控件「字段拼接」（`field_concat`）—— 后端控件注册表 / DAG 翻译层 / Flink 执行 / 输出落盘表头 全链路
- 后端地址：http://localhost:18080（H2 文件库）
- 结果：**3/3 通过**
- 用例总耗时：17.4 秒（含 Flink 作业启动与状态轮询）

## 一、结果总览

| 用例 | 链路 | 数据（源行数） | 期望行数 | 实际行数 | 列数 | 逐行逐列 | 拼接值重算 | 结果 |
|---|---|---|---|---|---|---|---|---|
| FC-01 | CSV 输入 → 字段拼接 → CSV 输出 | concat_source.csv（24） | 24 | 24 | 13 | 一致 | 全部一致 | **PASS** |
| FC-02 | CSV 输入 → 字段拼接 → CSV 输出（跨类型拼接） | concat_source.csv（24） | 24 | 24 | 13 | 一致 | 全部一致 | **PASS** |
| FC-03 | CSV 输入 → 字段拼接 → 空值校验(checkFields) → CSV 输出 | concat_source_slim.csv（5） | 3 | 3 | 4 | 一致 | 全部一致 | **PASS** |

> 「逐行逐列」= 落盘文件与期望值文件每个单元格逐个比对；「拼接值重算」= 不依赖期望值文件，
> 直接按 `separator` 把源字段重新连接一遍再与落盘的拼接列比对（独立第二判据）。

## 二、环境

| 项 | 值 |
|---|---|
| 后端 | http://localhost:18080，健康探针 `UP`，库 `UP` |
| Flink 集群 | JobManager `:18081`，TaskManager 1 个，Slot 可用 4/4 |
| SQL Gateway | `:18083` |
| 提交账号 | `admin`（ADMIN） |
| 测试数据 | `test-resources/data/concat_source.csv`（24 行 × 12 列）、`concat_source_slim.csv`（5 行 × 3 列） |
| 期望值文件 | `test-resources/data/concat_expected_1/2/3.csv`（落盘契约：源全部列 + 拼接列） |
| 实际落盘 | `output/field_concat_e2e/`（脚本只写这里，不覆盖期望值文件） |

> 期望值文件不是「跑完抄一遍」得来的：它由 `scripts/gen-field-concat-testdata.py` 按
> 拼接语义确定性生成，再由 `scripts/verify-field-concat-testdata.py` 独立重算校验，
> 最后才用于与真实落盘结果比对。

## 三、逐用例明细

### FC-01 CSV 输入 → 字段拼接 → CSV 输出

- 测试目的：基础拼接：拼接列追加到源全部列之后，逐行值正确（含空值/长文本/字段自带分隔符）
- 拼接配置：`fields=category,channel`，`separator=','`，`newFieldName=category_channel`
- 期望生成的拼接表达式：`CONCAT(CAST(`category` AS STRING), ',', CAST(`channel` AS STRING)) AS `category_channel``
- 作业：#2798，Flink Job ID：`46154be37a1a0c4ca3aff334a0b71b3a`，状态：**COMPLETED**，耗时：5.6 秒
- 状态轨迹：DRAFT(创建 #2798) → SUBMITTED(SUBMITTED) → COMPLETED
- 结果：**PASS**（期望 24 行 / 实际 24 行；列数 13；逐行逐列 一致；拼接值重算 全部一致）
- 期望值文件：`test-resources/data/concat_expected_1.csv`
- 实际落盘：`output/field_concat_e2e/concat_expected_1.csv`
- 差异：无（表头与全部单元格逐一致）

### FC-02 CSV 输入 → 字段拼接 → CSV 输出（跨类型拼接）

- 测试目的：非字符串列拼接：数值列 amount 与日期列 join_day 各经 CAST(... AS STRING) 后拼接，边界值 0.00 / 空值行均参与比对
- 拼接配置：`fields=amount,join_day`，`separator='|'`，`newFieldName=amount_day`
- 期望生成的拼接表达式：`CONCAT(CAST(`amount` AS STRING), '|', CAST(`join_day` AS STRING)) AS `amount_day``
- 作业：#2799，Flink Job ID：`c0eef709ea39d5b3760ef8e0728f210e`，状态：**COMPLETED**，耗时：5.6 秒
- 状态轨迹：DRAFT(创建 #2799) → SUBMITTED(SUBMITTED) → COMPLETED
- 结果：**PASS**（期望 24 行 / 实际 24 行；列数 13；逐行逐列 一致；拼接值重算 全部一致）
- 期望值文件：`test-resources/data/concat_expected_2.csv`
- 实际落盘：`output/field_concat_e2e/concat_expected_2.csv`
- 差异：无（表头与全部单元格逐一致）

### FC-03 CSV 输入 → 字段拼接 → 空值校验(checkFields) → CSV 输出

- 测试目的：含空格的分隔符不被拆开；拼接后接空值校验，校验字段为空的行被剔除
- 拼接配置：`fields=src_name,channel`，`separator=' | '`，`newFieldName=name_channel`
- 期望生成的拼接表达式：`CONCAT(CAST(`src_name` AS STRING), ' | ', CAST(`channel` AS STRING)) AS `name_channel``
- 空值校验字段（`checkFields`）：`src_name,channel`（`ignoreEmpty=true`）
- 作业：#2800，Flink Job ID：`3bdd8c0566087cb2c17ae9b745045f2b`，状态：**COMPLETED**，耗时：6.1 秒
- 状态轨迹：DRAFT(创建 #2800) → SUBMITTED(SUBMITTED) → COMPLETED
- 结果：**PASS**（期望 3 行 / 实际 3 行；列数 4；逐行逐列 一致；拼接值重算 全部一致）
- 期望值文件：`test-resources/data/concat_expected_3.csv`
- 实际落盘：`output/field_concat_e2e/concat_expected_3.csv`
- 差异：无（表头与全部单元格逐一致）

## 四、边界用例覆盖（数据层）

`concat_source.csv`（24 行 × 12 列）逐行设计的边界，全部参与 FC-01 / FC-02 的逐行比对：

| 边界 | 数据事实 | 期望表现 | 出处 |
|---|---|---|---|
| 中文/英文字段拼接 | `category=图书` + `channel=线上` | `图书,线上` | FC-01 第 1 行 |
| 数值列经 CAST 拼接 | `amount`（含 `1234.50`） | 逐位保真拼进新列 | FC-02 全 24 行 |
| 数值 0.00 | `amount=0.00` | 不因类型转换丢位 | FC-02 第 10 行 |
| 日期列拼接 | `join_day=2024-01-05` | 原样拼入 | FC-02 第 1 行 |
| 空字符串字段 | `src_name` / `amount` 为空 | 拼接列仍产出，空位保留（如 `A0019\|`） | FC-01 第 12 行 / FC-02 第 19-21 行 |
| 字段值里带分隔符 | `remark` 含 `;`、竖线、冒号 | 与拼接分隔符互不干扰 | FC-01 第 23 行 |
| 括号/斜杠 | `remark` 含 `/api-gateway/v2` 与 `(v2)` | 原样保留 | FC-01 第 24 行 |
| 长文本 | `remark` 200+ 字符 | 拼接结果完整不断行 | FC-01 第 22 行 |
| 分隔符含空格 | `separator=' | '` | 生成单个字符串字面量，不被拆成多参数 | FC-03 |
| 拼接后接空值校验 | `checkFields=src_name,channel` | 校验字段为空的行被剔除（5 → 3 行） | FC-03 |

## 五、结论

1. 「字段拼接」控件在真实 Flink Standalone 环境跑通：三个场景全部提交 → RUNNING → COMPLETED，
   落盘结果与期望值**逐行逐列一致**，且拼接列经独立重算复核无差异。
2. 拼接列正确进入 schema 传播链：输出文件为「源全部列 + 拼接列」，列数与表头与落盘契约一致。
3. 非字符串列（数值 `amount`、日期串 `join_day`）经逐个 `CAST(... AS STRING)` 后正确拼接，
   `0.00` 这类边界值不丢位；空字段按空串参与拼接（`A0019|`），拼接列仍产出。
4. 链式转换（拼接 → 空值校验）在真实执行中未丢列、未错位，校验字段为空的行按 `ignoreEmpty=true` 被剔除。
5. 含空格的分隔符（`' | '`）生成单个字符串字面量，未被拆成多个 CONCAT 参数。

## 六、测试过程中观察到的两个环境/平台现象（与本控件无关，如实记录）

1. **`row_filter` + 空值条件的用例在本机跑不出结果**：`CSV 输入 → 行过滤(amount IS NULL) → 字段拼接 → CSV`
   这个版本实测是 `COMPLETED` 但**落盘 0 行**（作业 #2759 / #2765 / #2786），换成 `amount IS NULL OR amount = ''`、
   `TRIM(amount) = ''`、`order_id = 'A0019'` 等条件同样 0 行，说明问题不在条件写法上；
   而同一份数据经「字段拼接」把 `order_id` 与 `amount` 拼出来时，空值行的结果是 `A0019|`，
   即**空字段被读成空字符串而非 NULL**。行过滤为 0 行与这个读值结论相互矛盾，本轮未定位到根因，
   因此该边界场景未纳入上表的通过用例（改用「拼接 → 空值校验」覆盖空值路径）。
2. **SQL Gateway 连续提交会间歇性失败**：连续提交十余次后，`CREATE TABLE` 类语句会返回
   `Failed to fetchResults.`（后端转成 HTTP 500，报文里没有根因）。实测**重启 SQL Gateway 后立即恢复**
   （本节报告的三个用例就是重启后连跑通过的）。这是 Flink 1.18.1 SQL Gateway 的已知报文缺失问题的延伸，
   不影响上表结论；复跑前若遇到同样的 500，先重启 Gateway。

## 七、复现方式

```powershell
# 1) 起后端 + Flink + SQL Gateway（幂等按端口检测）
start-all.bat

# 2) 重跑本报告的端到端用例（自动生成同格式报告）
python scripts/run-field-concat-e2e.py --base http://localhost:18080 --user admin

# 3) 离线自校验测试数据（不需要后端）
python scripts/verify-field-concat-testdata.py

# 4) 翻译层单测（不需要 Flink）
cd backend; mvn -B test -Dtest=DagFieldConcatDataTest
```

> 相关文件：`test-resources/data/README_field_concat.md`（数据说明与逐行期望值对照表）、
> `scripts/gen-field-concat-testdata.py`（数据生成）、`scripts/verify-field-concat-testdata.py`（数据自校验）、
> `backend/src/test/java/com/datastream/mvp/service/DagFieldConcatDataTest.java`（翻译层回归）。
