# AGENTS.md — 本项目工作记忆（DSH 工作区指令，自动注入）

> 给 AI 编码助手的工作区指令。项目自有文档：`DEVELOPMENT.md`（开发维护指南·逻辑框架锁定版）、`README.md`（用户向）。

## 0. 最高优先规则（动代码前必做）

**修改任何代码之前，先读 `DEVELOPMENT.md`。**

按这个顺序用它：

1. 需求定位 → 第 5 节「逻辑框架」找它属于哪一环（5.1 端到端链路 / 5.2 DAG 契约 / 5.3 控件注册表 / 5.4 翻译层 / 5.5 三路提交 / 5.6 前端画布 / 5.7 状态与日志）。
2. 文件定位 → 第 6 节「关键文件地图」看清每个文件的职责与改动注意。
3. 动手前 → 扫第 8 节「红线与踩坑清单」。
4. 改完 → 跑第 9 节「回归验证清单」。
5. 若必须突破框架 → 先在 DEVELOPMENT.md 文末「框架变更记录」追加一行（日期 | 变更 | 影响文件/章节），再改代码。

核心原则：**逻辑框架不变，只在框架内扩展。** 不读文档就动手 = 违规。

## 1. 项目速览

可视化 DAG 画布编排数据流任务 → 翻译成 Flink SQL → 提交 Flink Standalone 运行 → 结果输出文件/MySQL，前端展示状态、日志、监控、血缘、告警。

- 后端 Spring Boot 3.2.5 / JDK 17（`backend/`，端口 8080，默认 H2 file `backend/data/mvpdb`，可切 MySQL）
- 前端 Vue 3 全局版 + Element Plus + AntV X6（`frontend/public/`，端口 3000，`serve.js` 静态服务 + `/api` 反代）
- Flink 1.18.1 Standalone：JobManager 8081、SQL Gateway 8083、MySQL 3306（Docker 版 3307）
- 核心链路：`DagTranslationService.translate(DAG→Flink SQL)` 与 `FlinkJobStatusChecker`（状态轮询/输出后处理）是两个最敏感的文件

## 2. 硬红线（违反必出 bug）

1. **控件注册表是双份**：后端 `DataInitializer` 与前端 `app.js` 的 `getBuiltinControls()` 必须同步，只改一端 → 「画布有控件但提交失败」。
2. **DAG JSON 字段名是契约**：`id/type/label/params/x/y`、`source/target`，前后端都别改名。
3. **画布重建必须走 `loadDagToCanvas`**：它自带 dagLoadSeq 防抖 + 双 rAF 等待 + 碰撞排布；绕过它自己写 `clearCells + addNode` 会触发 X6 3.1.7 同 id 节点重叠 bug。
4. **编码**：新增 Java 文件不要用 `Set-Content` 写（BOM 导致 `javac` 报 `\ufeff`）；`app.js` 带 BOM、Java 是 CRLF，改写时保留；MySQL 表必须 utf8mb4。
5. **路径类参数一律 `.trim()`**（历史 bug：前导空格导致文件找不到）。
6. **新增输入类控件必须接入 `nodeSchemas` schema 传播链**，否则下游输出拿不到字段。
7. **改动要跑回归**：后端 `cd backend; mvn -B test`；前端 `cd frontend; npm run check`（`node --check public/js/app.js`）。
8. **提交失败先看日志**：`GET /api/jobs/{id}/logs`；Flink 没起时提交走 mock，作业会标 COMPLETED 但没真跑。

## 3. 环境注意

- 本机为中文路径 `D:\code\比赛\2026省服务外包`；Flink 提交中文输出路径在 JDK 17 上有编码坑，必要时用英文目录。
- `start-all.bat` 一键启动（幂等，端口检测用 netstat，避免 HTTP_PROXY 干扰）；Flink 启停在 `D:\code\flink-1.18.1\bin\start-cluster.bat`。

## 4. 文档纪律

- 每次功能性改动完成后，在 `DEVELOPMENT.md` 文末「框架变更记录」表追加一行：`| 日期 | 变更摘要 | 影响文件/章节 |`。
- 新增控件 / 新链路实测通过后，同步更新第 1.2 节「已实测链路」。
