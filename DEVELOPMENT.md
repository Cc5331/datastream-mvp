# 数据流任务管理系统 — 开发维护指南（逻辑框架锁定版）

> 本文件用于指导项目**后续所有完善工作**。核心原则：**逻辑框架不变，只在框架内扩展**。
> 每次动代码前先读本文档对应章节 + 对应模块源码；改完按第 9 节做回归验证。

---

## 0. 怎么使用本文档

1. 接到完善需求后，先在「第 5 节 逻辑框架」中定位它属于哪一环（前端画布 / DAG 契约 / 翻译层 / 提交层 / 状态轮询 / 输出后处理）。
2. 阅读对应模块源码，确认改动只在框架内进行。
3. 改完必须跑「第 9 节 回归验证清单」。
4. 如果某个改动必须突破框架，先在文末「框架变更记录」登记，并同步更新相关章节，避免框架悄悄漂移。

---

## 1. 项目定位与当前能力

### 1.1 一句话定位
可视化 DAG 画布编排数据流任务 → 翻译成 Flink SQL → 提交到 Flink Standalone 集群运行 → 结果输出到文件 / MySQL，前端展示作业状态与日志。

### 1.2 当前已实现功能（2026-08 实测可用）
- **画布**：左侧控件库拖拽控件到画布、端口连线、点选配置参数、Delete 删除、双击删除、保存 / 提交 / 导出 / 导入 JSON、新建画布、清空、Ctrl+S 保存。
- **控件（17 个内置）**：
  - 输入：`datagen_input`（模拟数据）、`csv_input`、`excel_input`、`json_input`（JSON Lines 读取）、`kafka_input`、`mysql_input`（**JDBC 读库 + 字段自动推导**）
  - 输出：`csv_output`、`excel_output`、`json_output`（JSON Lines 写出 + part 自动合并）、`kafka_output`、`mysql_output`（**写入自动建表**）
  - 转换：`field_concat`（字段拼接）、`xml_json`（XML 与 JSON 互转，需 UDF jar）、`field_filter`（字段过滤）、`field_rename`（字段改名）、`row_filter`（行过滤）、`json_parse`（JSON 字段解析）
- **已实测链路**：
  - CSV → CSV / Excel / MySQL（自动建表，中文无乱码）
  - Excel → CSV / MySQL（POI 转临时 CSV 接入）
  - Datagen → CSV / Excel
  - MySQL → CSV（JDBC 读库，字段自动推导）
  - JSON → JSON（JSON Lines 读写 + part 合并单文件）
  - CSV → JSON（field_filter / field_rename / row_filter / json_parse 转换链路）
  - CSV → Kafka → CSV（Docker Kafka 3.8 实测：生产者写 3000 条 JSON 消息，消费者读回写 CSV，中文正常）
- **作业管理**：状态机 DRAFT → SUBMITTED → RUNNING → COMPLETED / FAILED / CANCELLED；Flink 状态每 15s 轮询回写；日志落库可查。
- **参数面板**：按控件 paramSchema 动态渲染（string / number / boolean / enum / array）。

### 1.3 未实现 / TODO（后续完善方向）
- 控件插件热加载（plugin 包）：只有骨架，MVP 实际使用内置注册表。
- 性能基准测试脚本在 test-resources，未纳入 CI。

---

## 2. 技术栈与版本（不要随意升级）

| 组件 | 版本 / 说明 |
|---|---|
| JDK | 17 |
| Spring Boot | 3.2.5（web / data-jpa / validation） |
| 元数据库 | H2 file：backend/data/mvpdb（application.yml）；可切 MySQL（application-mysql.yml） |
| 业务 MySQL | 本机 localhost:3306，root/YOUR_MYSQL_PASSWORD，库 dataflow；Docker 版映射 3307 |
| Flink | 1.18.1 Standalone：JobManager 8081、SQL Gateway 8083、sql-client |
| 前端 | Vue 3（unpkg 全局版）+ Element Plus 2.9.1 + AntV X6 3.1.7 + axios；serve.js 静态服务并反代 /api |
| 其他 | Lombok、Jackson、Apache Commons CSV、Apache POI（Excel）、java.net.http（调 Flink REST） |

---

## 3. 目录结构（当前实际）

    D:\code\比赛\2026省服务外包
    ├── backend/                        # Spring Boot 后端
    │   ├── src/main/java/com/datastream/mvp/
    │   │   ├── MvpBackendApplication.java
    │   │   ├── config/                 # DataInitializer(控件种子) / GlobalExceptionHandler / WebConfig
    │   │   ├── controller/             # JobController / ControlRegistryController
    │   │   ├── dag/                    # DagDefinition(DAG模型) / DagExecutor / FlinkDagExecutor / LocalDagExecutor
    │   │   ├── model/                  # JobDefinition / ControlRegistry / JobLog
    │   │   ├── plugin/                 # 控件插件 SPI（骨架，未启用）
    │   │   ├── repository/             # JPA Repository × 3
    │   │   └── service/                # JobService / DagTranslationService / MysqlTableCreator / ExcelPreprocessor / ExcelOutputConverter / FlinkJobStatusChecker / ControlRegistryService
    │   ├── src/main/resources/         # application.yml / application-mysql.yml / application-linux.yml
    │   └── data/mvpdb.mv.db            # H2 元数据库文件（运行时生成）
    ├── frontend/
    │   ├── serve.js                    # 静态服务 + /api 反代（无缓存头）
    │   ├── package.json
    │   └── public/
    │       ├── index.html              # 主页面（三个视图 + 帮助 + 日志弹窗）
    │       ├── css/style.css
    │       └── js/app.js               # 全部前端逻辑（Vue3 + X6）
    ├── flink-1.18.1/                   # Flink 发行版副本（本机运行时用 D:\code\flink-1.18.1）
    ├── udf/                            # UDF jar 工程（XmlToJson / JsonToXml 等）
    ├── scripts/                        # init_mysql.sql / test-mysql-flow.ps1
    ├── docker/                         # Docker 化相关（flink 镜像等）
    ├── docker-compose.yml              # 全容器化编排（MySQL:3307 / Kafka / Flink / 后端 / 前端）
    ├── output/                         # 作业输出目录（CSV/Excel 等）
    ├── test-resources/                 # 测试数据（sales.csv / sample_data.xlsx）与基准脚本
    ├── start-all.bat / start-docker.bat / stop-docker.bat / start_sql_gw.bat
    ├── deploy-linux.sh / stop-linux.sh
    └── README.md                       # 用户向 README

---

## 4. 启动 / 停止 / 验证命令（Windows 本机，当前实际）

### 4.1 端口一览
| 服务 | 端口 |
|---|---|
| 前端 | 3000 |
| 后端 | 8080（H2 console：http://localhost:8080/h2-console，JDBC jdbc:h2:file:./data/mvpdb，user sa，空密码） |
| Flink JobManager（Web UI） | 8081 |
| Flink SQL Gateway | 8083 |
| MySQL | 3306（Docker 版 3307） |

### 4.2 启动
    # 1) 后端（在 backend 目录，日志重定向到 backend_h2.log）
    cd D:\code\比赛\2026省服务外包\backend
    mvn spring-boot:run

    # 2) 前端（在 frontend 目录）
    cd D:\code\比赛\2026省服务外包\frontend
    node serve.js

    # 3) Flink Standalone 集群（FLINK_HOME 在本机为 D:\code\flink-1.18.1）
    D:\code\flink-1.18.1\bin\start-cluster.bat

    # 4) Flink SQL Gateway（可选，翻译层会自动探测）
    java -cp "D:\code\flink-1.18.1\lib\*" org.apache.flink.table.gateway.SqlGateway -D sql-gateway.endpoint.rest.port=8083

### 4.3 停止
    # 按端口找 PID 再杀
    Get-NetTCPConnection -LocalPort 3000,8080,8081,8083 -State Listen | Select-Object LocalPort,OwningProcess
    Stop-Process -Id <PID> -Force
    # Flink 集群停止
    D:\code\flink-1.18.1\bin\stop-cluster.bat

### 4.4 常用验证命令
    curl http://localhost:8080/api/jobs              # 作业列表
    curl http://localhost:8080/api/controls          # 控件注册表
    curl http://localhost:8081/jobs/overview         # Flink 作业
    curl http://localhost:8081/taskmanagers          # Flink TaskManager（若 404 说明集群未起）
    mysql -u root -pYOUR_MYSQL_PASSWORD dataflow -e "SHOW TABLES;"

---

## 5. 逻辑框架（核心，改动必须遵循）

### 5.1 端到端链路
    前端画布(X6) → dagJson → 保存(job_definition) → 提交 POST /api/jobs/{id}/submit
      → JobService.submit
      → DagTranslationService.translate(DAG → Flink SQL)      【翻译层，核心】
      → submitToFlink（三路降级：SQL Client / SQL Gateway / Flink REST，全失败则 mock）
      → FlinkJobStatusChecker 每 15s 轮询 /jobs/overview 回写状态与日志
      → 输出类后处理（ExcelOutputConverter 等）在状态完成时执行

### 5.2 DAG JSON 契约（前后端必须一致，勿改字段名）
    {
      "jobName": "新作业",
      "parallelism": 2,
      "nodes": [
        { "id": "csv_input_1", "type": "csv_input", "label": "CSV 输入",
          "params": { "path": "D:\\...\\sales.csv" }, "x": 178.01, "y": 135.19 }
      ],
      "edges": [ { "id": "e1", "source": "csv_input_1", "target": "csv_output_1" } ]
    }
- 节点：id / type / label / params / x / y（x/y 可缺省，前端自动排布；DagDefinition.DagNode 用 @JsonIgnoreProperties(ignoreUnknown=true)）。
- 边：id / source / target（sourcePort/targetPort 可选）。
- 前端端口：每个节点固定 left(输入) / right(输出) 两个端口，端口 id 为 ${nodeId}-in / ${nodeId}-out。

### 5.3 控件注册表（双份，必须同步）
- **后端**：DataInitializer 启动时清空重建 control_registry（含 type/name/category/paramSchema/flinkTemplate）。
- **前端**：getBuiltinControls() 是后端不可用时的降级副本（app.js 内）。
- **新增控件必须两端同步**：后端注册表驱动翻译层，前端副本驱动画布展示与参数面板。只改一端会导致「画布有控件但提交失败」或「翻译报未知控件」。

### 5.4 DAG → Flink SQL 翻译（DagTranslationService，最大单文件，改这里要最小心）
- translate() 先 topologicalSort，再逐节点按 type 分支生成 SQL：
  - datagen_input → generateDatagenDDL + 从 fieldsConfig 提取字段（extractFieldsFromDatagenConfig）。
  - csv_input → 校验文件存在、autoDetectDelimiter 探测分隔符；无 fieldsConfig 时 detectCsvColumns 读表头自动生成列（含中文列名、去重）。
  - excel_input → ExcelPreprocessor.convertToCsv 转临时 CSV 后走 CSV 逻辑（hasHeader 默认 true）。
  - kafka_input / kafka_output → 模板渲染。
  - csv_output / excel_output → 模板 + 临时表（stripCsvHeaderIfNeeded 处理表头）。
  - mysql_output → 提交前 MysqlTableCreator.ensureTable 自动建表，再生成 JDBC sink。
  - field_concat / xml_json → transform 分支（findTransformSql 等）。
- **schema 传播**：nodeSchemas（节点 id → DDL 字段串）贯穿全流程；findIncomingSourceTable / findTransformBetween / findTransformSql / findIncomingSourceSchema 处理「输入→转换→输出」中间节点；**新增控件必须接入这条 schema 链**，否则下游输出拿不到字段。
- 路径类参数统一 .trim() 后再用（历史 bug：前导空格导致路径失效）。

### 5.5 提交三路降级（submitToFlink）
1. checkFlinkCluster()：探测 flinkHost:flinkPort(8081) TCP 是否通。
2. 通 → submitViaSqlClient（写临时 .sql 调 sql-client）→ 失败降级 trySqlGateway（SQL Gateway REST）→ 失败降级 submitViaFlinkRestApi（REST 提交 + 轮询新 job id）。
3. 不通 / 全部失败 → 返回 mock flink-job-<uuid>（本地无集群也能走完流程）。
4. cancelFlinkJob：mock id（flink-job- 前缀）直接跳过 REST。

### 5.6 前端画布逻辑（app.js）
- initGraph()：创建 X6 Graph（网格、拖拽平移、滚轮缩放、连线配置、节点点击/删除/双击事件）。
- loadDagToCanvas(dag)（**异步，三道防护，勿回退**）：
  1. dagLoadSeq 序号防抖：快速切换作业 / 清空 / 新建时，只有最后一次加载生效；
  2. graph.clearCells() 后**必须等待双 rAF（带 100ms 兜底）再 addNode**——X6 3.1.7 异步渲染在「同批 clear+add 相同 id」时偶发残留旧视图，导致同一节点渲染两次（视觉重叠），这是已修复的历史 bug，防护不能删；
  3. 矩形碰撞检测自动排布：无坐标或冲突节点按行错开（宽 160 + 间距 20，超 1200px 换行），有真实坐标的节点保持原位置。
- 拖拽添加：onDragStart 写入 dataTransfer，setupDropHandler 监听 drop 按落点 clientToLocal 放置。
- 参数面板：node:click 填充 nodeParams / nodeParamSchema；applyParams 写回节点 data.params（保存前也会自动调用，改参数不丢）。
- 作业切换：作业列表「编辑」/ 双击行 → openJob → loadDagToCanvas。
- 提交前校验 validateDagForSubmit：有节点必须连线、输出节点必须有路径。

### 5.7 状态与日志
- FlinkJobStatusChecker @Scheduled(fixedRate=15000) 只轮询 SUBMITTED/RUNNING 的作业，映射 Flink 状态到本地状态。
- JobLog 落库，GET /api/jobs/{id}/logs 查询（按时间倒序）。
- ExcelOutputConverter：CSV 结果 → .xlsx 后处理（在状态轮询链中完成时触发）。

---

## 6. 关键文件地图

### 6.1 后端
| 文件 | 职责 | 改动注意 |
|---|---|---|
| service/DagTranslationService.java | DAG→SQL、提交、schema 传播 | 新增控件主要改这里；先看 5.4 |
| service/JobService.java | 作业 CRUD、提交入口、日志 | 状态流转别乱改 |
| service/MysqlTableCreator.java | MySQL 自动建表 + 类型映射 | 类型映射表、保留字反引号、utf8mb4 |
| service/ExcelPreprocessor.java | Excel→临时 CSV（WorkbookFactory，支持 .xls/.xlsx） | 临时文件放 output/*_temp_csv |
| service/ExcelOutputConverter.java | CSV→.xlsx 后处理 | 输出文件路径来自节点 params.path |
| service/FlinkJobStatusChecker.java | 15s 状态轮询 | 只轮询活跃作业 |
| config/DataInitializer.java | 控件种子（11 个） | 新增控件第一站 |
| controller/JobController.java | REST 路由 | 已有 preview 接口（可用未完善） |
| dag/DagDefinition.java | DAG 模型 | 字段名即契约，勿改 |
| application.yml | H2、Flink 地址、端口 | Flink 配置集中在 flink.* |

### 6.2 前端
| 文件 | 职责 | 改动注意 |
|---|---|---|
| frontend/serve.js | 静态服务 + /api 反代 | 无缓存头；端口 3000；后端 8080 |
| public/index.html | 三个视图模板、参数面板、帮助/日志弹窗 | 参数面板按 paramSchema 动态渲染 |
| public/js/app.js | 全部前端逻辑 | 带 BOM；编辑脚本要保留 BOM（见 8） |
| public/css/style.css | 布局样式 | 画布容器 #dag-canvas |

---

## 7. 扩展指南（在框架内加功能的标准做法）

### 7.1 新增一个控件（标准流程）
1. DataInitializer 注册控件（type/name/category/paramSchema/flinkTemplate）。
2. frontend/js/app.js 的 getBuiltinControls() 同步一份（降级副本）。
3. DagTranslationService.translate() 加对应 type 分支：
   - input：生成 CREATE TABLE DDL，并把字段写入 nodeSchemas；
   - output：findIncomingSourceTable 拿上游表，生成 INSERT INTO ... SELECT；
   - transform：接 schema 传播（findIncomingSourceSchema + 生成转换 SQL）。
4. 输出类控件如需后处理，参考 ExcelOutputConverter 挂到状态完成链路。
5. 前端参数面板无需改（自动按 paramSchema 渲染）。
6. 跑通一条实测链路（如 9.2），并把结果追加到 1.2。

### 7.2 补齐 MySQL Input（读库，当前最大缺口）
- 在 translate() 加 mysql_input 分支：用 JDBC connector DDL（url/table/username/password），字段从表结构自动推导（参考 MysqlTableCreator 的反向逻辑：SELECT ... LIMIT 0 或 JDBC DatabaseMetaData 拿列）。
- 接入 nodeSchemas，让下游输出能拿到字段。
- 前端 paramSchema 已存在（url/table/username/password），无需新增。

### 7.3 新增文件格式转换（如 JSON 输入/输出）
- 参照 excel_input 的做法：加一个 JsonPreprocessor（输入转临时 CSV）或直接在 translate() 生成对应 connector DDL（如 format=json）。
- 输出类参照 excel_output：CSV 结果落盘后做格式转换，输出路径统一走 output/。

### 7.4 改 UI 流程（如作业下拉切换）
- 所有「重新加载画布」都必须走 loadDagToCanvas（自带防抖 + 双 rAF + 碰撞检测），不要自己写 clearCells + addNode，否则会踩 X6 重叠 bug。
- 新增画布级状态时，注意 currentJobId / currentJobName / selectedNode / nodeParams 的复位逻辑（参考 newCanvas/clearCanvas）。

---

## 8. 红线与踩坑清单（务必遵守）

1. **X6 重叠 bug**：任何画布清空+重建都必须经过双 rAF 等待（requestAnimationFrame × 2，兜底 setTimeout 100ms），否则同 id 节点会残留旧视图。loadDagToCanvas 已封装，不要绕过。
2. **控件注册表双份同步**：后端 DataInitializer 与前端 getBuiltinControls 必须一致，缺一不可。
3. **DAG 字段名是契约**：id/type/label/params/x/y、source/target 勿改名；DagDefinition 与前端 exportDagJson 要同步。
4. **中文/编码**：
   - MySQL 表 DEFAULT CHARSET=utf8mb4（MysqlTableCreator 已做），乱码先查这里。
   - 新增 Java 文件不要用 Set-Content 写（会加 BOM 导致 javac 报 \ufeff 错误），用 Node 写或写后去 BOM。
   - app.js 带 BOM、后端 Java 是 CRLF：编辑脚本先统一为 LF，写回时保留 BOM / CRLF。
5. **本环境 apply_patch 不可用**（Access denied）：改文件用「写 _x.cjs → node _x.cjs → 删除」的方式。
6. **PowerShell 复杂内嵌命令会被策略拒**（如内嵌 mvn + 等待 + Start-Process）：拆成简单命令分步执行。
7. **路径参数一律 .trim()**（历史 bug：前导空格导致文件找不到）。
8. **start-all.bat 的中文路径是乱码**：在中文路径机器上以手动命令启动为准（见 4.2）。
9. **H2 元数据库**在 backend/data/mvpdb；切换 MySQL 用 --spring.profiles.active=mysql（application-mysql.yml）。
10. **提交失败先看日志**：GET /api/jobs/{id}/logs；翻译错误会置作业 FAILED 并写 ERROR 日志。
11. **Excel 输出临时文件**：output/*_temp_csv、*.tmp 是中间产物，可清理，不影响功能。
12. **Flink 集群没起时**：提交走 mock，作业状态会标 COMPLETED 但不会真跑；测试真实链路必须先起 Flink（4.2）。

---

## 9. 回归验证清单（每次改完必跑）

### 9.1 服务健康
- 前端 3000 返回 200、后端 8080 返回 200、Flink 8081 返回 200（如已启动）。

### 9.2 核心链路（用 test-resources/data/sales.csv、sample_data.xlsx）
- [ ] CSV → CSV：output/dag_sales_out2.csv 生成，行数 = 源行数 + 表头，中文正常。
- [ ] CSV → Excel：output/*.xlsx 可打开。
- [ ] CSV → MySQL：dataflow.auto_* 表自动创建，行数正确，中文无乱码（HEX 检查 E4B89C... 为正常 UTF-8）。
- [ ] Excel → CSV / MySQL：中文表头保留。
- [ ] Datagen → CSV：生成 N 行。

### 9.3 前端交互
- [ ] 新建画布 → 拖 3+ 控件 → 连线 → 保存 → 重新打开：布局不重叠、参数不丢。
- [ ] 连续切换多个作业（尤其脚本/API 建的无坐标作业）：节点不重叠、无重复渲染。
- [ ] 参数面板：改参数 → 应用 → 保存 → 重开，值保持。
- [ ] 导出 JSON → 导入 JSON → 画布一致。

---

## 10. 后续完善路线（建议顺序）

1. **补齐 MySQL Input（读库）**——框架已有占位，收益最大。
2. **更多转换控件**（字段过滤、字段改名、行过滤、JSON 解析）——按 7.1 流程。
3. **文件格式扩展**（JSON / Parquet / 多 Sheet Excel）——按 7.3 流程。
4. **作业级增强**：定时调度、版本历史、复制作业。
5. **性能基准**：完善 test-resources/benchmark.py，产出 8vCPU 报告。
6. **Docker 全容器化收尾**：docker-compose.yml 已有雏形，补齐后端/前端镜像与一键脚本。
7. **UDF 扩展**：udf/ 工程追加自定义函数，翻译层 CREATE FUNCTION 注入。

---

## 附：框架变更记录

| 日期 | 变更 | 影响 |
|---|---|---|
| 2026-08-03 | 修复 X6 切换作业控件重叠：loadDagToCanvas 双 rAF 等待 + dagLoadSeq 防抖 + 矩形碰撞自动排布 | 前端 app.js，5.6 |
| 2026-08-03 | Excel 转换健壮性：WorkbookFactory.create 支持 .xls/.xlsx；路径 trim | DagTranslationService / ExcelPreprocessor |
| 2026-08-03 | MySQL 输出自动建表 MysqlTableCreator（类型映射 + 保留字反引号 + utf8mb4） | 新增 service |
| 2026-08-02 | 元数据库默认 H2 file；保留 application-mysql.yml 切换方案 | application.yml |
| 2026-08-03 | 完善 README.md：11 控件清单、API 表、启动方式、Docker、使用流程，修复旧文档代码块损坏 | 仅文档，无代码逻辑变更 |
| 2026-08-05 | 补充 P0 功能缺口：mysql_input 读库（JDBC 自动推导字段）、json_input/output、field_filter/rename、row_filter、json_parse 转换控件、Kafka fieldsConfig | DagTranslationService / DataInitializer / FlinkJobStatusChecker / 前端注册表 |
| 2026-08-05 | 修复前端「导入 JSON」失效：index.html 文件 input 缺少 @change="importDag" 绑定 | 前端 index.html |
| 2026-08-05 | 修复 MySQL/JSON/Datagen/Kafka 输入转 CSV/Excel 缺失字段名表头：findSourceHeader 支持 JDBC 列名与 fieldsConfig，并沿 transform 链精确合成表头（field_filter/field_rename/json_parse/field_concat） | FlinkJobStatusChecker |
| 2026-08-05 | Kafka 链路实测：docker compose 启动 Kafka 3.8（宿主机 29092），CSV→Kafka 输出 3000 条 JSON 消息、Kafka→CSV 消费回写验证通过；注意 bootstrapServers 用宿主机地址 localhost:29092 | 仅验证 + 文档 |

> 新改动请在下方继续追加，保持框架可追溯。
