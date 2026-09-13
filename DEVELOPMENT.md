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
- **控件（31 个内置）**：
  - 输入：Datagen、CSV、Excel、JSON、XML、Parquet、HDFS、Kafka、MySQL、PostgreSQL、Oracle。
  - 输出：CSV、Excel、JSON、XML、Parquet、HDFS、Kafka、MySQL、PostgreSQL、Oracle。
  - 转换：字段拼接/过滤/改名、行过滤、JSON 解析、XML↔JSON、Redis 富化、去重、空值校验、条件路由。
- **已实测链路**：
  - CSV → CSV / Excel / MySQL（自动建表，中文无乱码）
  - Excel → CSV / MySQL（POI 转临时 CSV 接入）
  - Datagen → CSV / Excel
  - MySQL → CSV（JDBC 读库，字段自动推导）
  - JSON → JSON（JSON Lines 读写 + part 合并单文件）
  - CSV → JSON（field_filter / field_rename / row_filter / json_parse 转换链路）
  - CSV → Kafka → CSV（Docker Kafka 3.8 实测：生产者写 3000 条 JSON 消息，消费者读回写 CSV，中文正常）
- **作业管理**：状态机 DRAFT → SUBMITTED → RUNNING → COMPLETED / FAILED / CANCELLED（另有 WAITING/BLOCKED 用于依赖编排）；Flink 状态每 **5s** 轮询回写；日志落库可查；一键复制作业（名称追加「（副本）」）；按 cron 定时自动提交（JobScheduler 每 30s 扫描，运行中不重复提交）；上线/下线生命周期——上线后流式作业后台持续运行、异常 10 分钟窗口内自动重启最多 3 次，批量作业按 cron 周期重跑，Flink 不可用禁止上线。
- **参数面板**：按控件 paramSchema 动态渲染（string / number / boolean / enum / array）。
- **企业能力（2026-08/09 迭代）**：JWT + RBAC（ADMIN/OPERATOR/VIEWER，含 owner 隔离）+ 操作审计 + 用户管理 CRUD；作业版本历史与回滚；作业依赖编排（环检测、上游完成自动触发）与工作流图；数据血缘；实时监控面板（吞吐趋势 / 背压 / checkpoint / 历史卡片兜底）；Kafka 可视化（SSE 实时消息流）；故障感知告警中心（10s 扫描 + DB 去重 + 恢复闭环 + webhook/邮件）；AI 层（NL2Pipeline 自然语言建作业、智能诊断规则引擎 + LLM 归因、草稿人工确认）；数据预览。

### 1.3 未实现 / TODO（后续完善方向）
- 控件插件热加载：PluginLoaderService 已能扫描 `backend/plugins` 注册外部插件，但 MVP 仍以内置注册表为准，尚未做插件的端到端验证与 UI 管理。
- 性能基准脚本已能真实提交 Flink 作业并产出报告（test-results/benchmark_report.md），但未纳入 CI。
- 测试空白区：MysqlTableCreator 仍无独立测试（README 主推的自动建表功能）；FlinkJobStatusChecker 仅有常量断言，状态认领/回写分支无回归保护；previewFile 的 allowed-roots 与防穿越逻辑无测试（安全高风险）。详见第 10 节。

---

## 2. 技术栈与版本（不要随意升级）

| 组件 | 版本 / 说明 |
|---|---|
| JDK | 17 |
| Spring Boot | 3.2.5（web / data-jpa / validation） |
| 元数据库 | H2 file：backend/data/mvpdb（application.yml，默认）；可切 MySQL（application-mysql.yml，库 mvp_backend）。**H2 Web 控制台默认关闭**（`H2_CONSOLE_ENABLED=true` 才开，且仅 ADMIN 可访问） |
| 业务 MySQL | 本机 localhost:3306；控件默认 JDBC 库为 `flink_demo`（DataInitializer），docker-compose 另建 `datastream`；账号/密码见 `.env`（`MYSQL_USERNAME`/`MYSQL_PASSWORD`）；Docker 版映射 3307 |
| Flink | 1.18.1 Standalone。本机 `D:\code\flink-1.18.1\conf\flink-conf.yaml` 写的是 **JobManager 18081 / SQL Gateway 18083**（start-all.bat 同步注入 `FLINK_CLUSTER_PORT`/`FLINK_SQL_GATEWAY_PORT`）；application.yml 默认值仍是 8081/8083，手工起后端时必须显式覆盖端口，否则连不上集群 |
| 前端 | Vue 3 + Element Plus 2.9.1 + AntV X6 3.1.8 + ECharts 5.5.1 + axios，**全部本地化在 frontend/public/vendor（无 CDN 依赖）**；serve.js 静态服务并反代 /api |
| 其他 | Lombok、Jackson、Apache Commons CSV、Apache POI（Excel）、OSHI 6.4.13（资源监控）、JJWT 0.12.5（JWT）、Hadoop 3.3.6 + parquet-hadoop 1.13.1、PostgreSQL 42.7.3 / Oracle ojdbc11、Jedis + Caffeine（UDF）、java.net.http（调 Flink REST） |

---

## 3. 目录结构（当前实际）

    D:\code\比赛\2026省服务外包
    ├── AGENTS.md                       # 工作区指令（AI 助手自动加载：动代码前先读本文件）
    ├── pom.xml                         # 根聚合 POM（plugin-sdk / plugin-example / backend / udf）
    ├── backend/                        # Spring Boot 后端（103 个 Java 文件 / 约 12.4k 行）
    │   ├── plugin-sdk/                 # 控件插件 SPI（DataStreamPlugin，无第三方依赖）
    │   ├── plugin-example/             # 插件示例（redis-connector，走 META-INF/services）
    │   ├── src/main/java/com/datastream/mvp/
    │   │   ├── MvpBackendApplication.java
    │   │   ├── ai/                     # LlmClient(多服务商/加密落盘) / AgentService(NL2Pipeline + 诊断)
    │   │   ├── audit/                  # @Audit 注解 + AuditAspect(audit_log 落库)
    │   │   ├── config/                 # DataInitializer(控件种子 31 个) / SecurityConfig / WebConfig / GlobalExceptionHandler / HadoopHomeConfig
    │   │   ├── controller/             # 11 个：Job/Auth/User/Ai/Alert/Audit/Monitor/Lineage/ControlRegistry/KafkaMonitor/Preview
    │   │   ├── dag/                    # DagDefinition(DAG模型) / DagExecutor / FlinkDagExecutor / LocalDagExecutor
    │   │   ├── model/                  # 11 个实体：JobDefinition / ControlRegistry / JobLog / JobVersion / JobDependency / AppUser / AuditLog / AlertRecord / DiagnosisReport / ScheduleHistory / TrendPoint
    │   │   ├── plugin/                 # 控件插件 SPI + PluginLoaderService（已启用，扫 backend/plugins）
    │   │   ├── repository/             # JPA Repository × 11
    │   │   ├── security/               # JwtUtil / JwtAuthFilter / CurrentUser / SecurityUtils
    │   │   ├── service/                # 30 个，核心见 6.1
    │   │   └── util/                   # JdbcUrlUtil / ManagedFiles / MysqlIdentifier / LanguageDetector
    │   ├── src/main/resources/         # application.yml / application-mysql.yml / application-linux.yml + ai-prompts/(中英提示词模板)
    │   ├── src/test/java/              # 22 个测试类 / 87 个用例
    │   └── data/                       # H2 元数据库 mvpdb.mv.db + ai_providers.json + .ai_config_key（本机密钥，均已被 .gitignore 覆盖）
    ├── frontend/
    │   ├── serve.js                    # 静态服务 + /api 反代（无缓存头，路径穿越防护）
    │   ├── test/app.test.js            # 前端 9 个用例（含控件注册表前后端一致性校验）
    │   └── public/
    │       ├── index.html              # 单页应用（9 个视图）
    │       ├── css/style.css
    │       ├── js/app.js               # 全部前端逻辑（2451 行，Vue3 + X6）
    │       └── vendor/                 # Vue/ElementPlus/X6/ECharts 本地化依赖
    ├── flink-1.18.1/                   # Flink 发行版副本（本机运行时用 D:\code\flink-1.18.1）
    ├── udf/                            # UDF jar 工程（XmlToJson / JsonToXml / RedisLookup）
    ├── scripts/                        # init_mysql.sql / test-mysql-flow.ps1
    ├── docker/                         # Dockerfile（flink 镜像、prepare-flink-jars.bat 等）
    ├── docker-compose.yml              # 全容器化编排（MySQL:3307 / Kafka / Flink / 后端 / 前端）
    ├── data/                           # 本机业务库初始化脚本与样例数据（mysql_setup.sql / sales.csv）
    ├── ai_sidecar/                     # 早期 Python(FastAPI+Ollama) 试验侧车：**当前架构未使用**，保留参考
    ├── output/                         # 作业输出目录（CSV/Excel 等，gitignore）
    ├── test-resources/                 # 测试数据（sales.csv / sample_data.xlsx / large 大数据集）与基准脚本
    ├── test-results/                   # 性能基准报告产物
    ├── docs/                           # 目前为空目录（README 中原「详细设计文档」已并入本文件）
    ├── start-all.bat / start-docker.bat / stop-docker.bat / start_sql_gw.bat
    ├── deploy-linux.sh / stop-linux.sh
    └── README.md                       # 用户向 README

---

## 4. 启动 / 停止 / 验证命令（Windows 本机，当前实际）

### 4.1 端口一览（2026-09-11 实测）
| 服务 | 端口 |
|---|---|
| 前端 | 3000 |
| 后端 | **start-all.bat 用 18080**（`set SERVER_PORT=18080`）；直接 `mvn spring-boot:run` 用 application.yml 默认 8080 |
| H2 console | 默认 **关闭**；需要时 `H2_CONSOLE_ENABLED=true` 启动，再以 ADMIN 登录后访问 /h2-console（JDBC jdbc:h2:file:./data/mvpdb，user sa，空密码） |
| Flink JobManager（Web UI） | 本机 **18081**（flink-conf.yaml rest.port）；Docker 容器内 8081 → 宿主 18081 |
| Flink SQL Gateway | 本机 **18083**；Docker 容器内 8083 → 宿主 18083 |
| MySQL | 3306（Docker 版 3307） |
| Kafka | 29092（docker compose 单节点） |

> 本机 Flink 用的是 18081/18083（8083 曾被系统进程占用），因此**手工**起后端时务必带
> `-DFLINK_CLUSTER_PORT=18081 -DFLINK_SQL_GATEWAY_PORT=18083`（或对应环境变量），否则提交会走 mock。

### 4.2 启动
    # 0) 一键启动（推荐，幂等：按 netstat 探活跳过已在跑的服务）
    start-all.bat            # Kafka → Flink(JM 18081/TM) → SQL Gateway 18083 → 后端 18080 → 前端 3000
    #    注意：它只启动已编译好的 backend\target\mvp-backend-1.0.0.jar，不自动编译；
    #    改了后端代码要先 cd backend && mvn -DskipTests package

    # 1) 后端（手工方式：在 backend 目录，端口与 Flink 端口都要对齐）
    cd D:\code\比赛\2026省服务外包\backend
    mvn spring-boot:run -Dspring-boot.run.jvmArguments="-DFLINK_CLUSTER_PORT=18081 -DFLINK_SQL_GATEWAY_PORT=18083"

    # 2) 前端（在 frontend 目录）
    cd D:\code\比赛\2026省服务外包\frontend
    node serve.js

    # 3) Flink Standalone 集群（FLINK_HOME 在本机为 D:\code\flink-1.18.1，conf 里 rest.port=18081）
    D:\code\flink-1.18.1\bin\start-cluster.bat

    # 4) Flink SQL Gateway（可选；本机端口与 flink-conf 一致为 18083，start_sql_gw.bat 等价）
    start_sql_gw.bat
    # 等价命令：
    java -cp "D:\code\flink-1.18.1\lib\*" org.apache.flink.table.gateway.SqlGateway -Dsql-gateway.endpoint.rest.port=18083 -Drest.address=localhost -Drest.port=18081

### 4.3 停止
    # 按端口找 PID 再杀（start-all.bat 起的服务端口是 3000,18080,18081,18083）
    Get-NetTCPConnection -LocalPort 3000,18080,18081,18083 -State Listen | Select-Object LocalPort,OwningProcess
    Stop-Process -Id <PID> -Force
    # Flink 集群停止
    D:\code\flink-1.18.1\bin\stop-cluster.bat

### 4.4 常用验证命令
    curl http://localhost:18080/api/jobs              # 作业列表（手工起后端时是 8080）
    curl http://localhost:18080/api/controls          # 控件注册表
    curl http://localhost:18081/jobs/overview         # Flink 作业
    curl http://localhost:18081/taskmanagers          # Flink TaskManager（若 404 说明集群未起）
    mysql -u root -p flink_demo -e "SHOW TABLES;"     # 密码见 .env / MYSQL_PASSWORD

### 4.5 构建与测试（改完必跑，见第 9 节）
    mvn -B -f pom.xml test          # 根聚合：全 5 模块；后端 87 个用例（2026-09-13 实测全绿）
    cd frontend; node --test test/app.test.js   # 前端 9 个用例（含控件注册表前后端一致性校验）

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
- translate() 先 cleanOutputTempDirs → 头部 SET/UDF 注册（xml_json、redis_lookup）→ topologicalSort → 逐节点按 type 分支生成 SQL → 边循环生成 INSERT：
  - datagen_input → generateDatagenDDL + 从 fieldsConfig 提取字段（extractFieldsFromDatagenConfig）。
  - csv_input → 校验文件存在、autoDetectDelimiter 探测分隔符；无 fieldsConfig 时 detectCsvColumns 读表头自动生成列（含中文列名、去重）。
  - excel_input / xml_input / parquet_input / json_input(array) → 对应 Preprocessor 转临时 CSV 后走 CSV 逻辑。
  - json_input(lines) → filesystem + json connector。
  - mysql_input / pg_input / oracle_input / hdfs_input → JDBC/filesystem DDL，字段由元数据或 fieldsConfig 推导。
  - kafka_input / kafka_output → 模板渲染。
  - csv_output / json_output → 模板渲染 + path 改 `<path>.tmp`；excel/parquet/xml_output → 先写 `*_temp_csv` 临时 CSV，作业完成后由对应 Converter 转目标格式。
  - mysql_output → 提交前 MysqlTableCreator.ensureTable 自动建表，再生成 JDBC sink。
  - transform（field_concat / xml_json / dedupe / validate / route / field_filter / field_rename / row_filter / json_parse / redis_lookup）→ **不生成独立表**，SQL 在边循环里内联进 INSERT。
- **schema 传播**：nodeSchemas（节点 id → DDL 字段串）贯穿全流程，输出/转换节点统一通过 **findIncomingSourceSchema** 取上游字段；**新增控件必须接入这条 schema 链**，否则下游输出拿不到字段。
  - 注意：`findTransformBetween` / `findTransformSql` / `pollFlinkJob` / `generateDDLFromTemplate` 已无调用（历史残留，勿当作现行链路）。
- **表头是另一条链（容易漏改）**：FlinkJobStatusChecker 侧的 findSourceHeader → sourceHeaderFields → applyTransformHeader 会**重新解析 DAG JSON** 合成输出表头，不复用 nodeSchemas。新增输入/转换控件若要输出表头，这里也要加分支。
- 路径类参数统一 .trim() 后再用（历史 bug：前导空格导致路径失效）。

### 5.5 提交降级链（submitToFlink，**实际顺序与旧文档相反**）
1. checkFlinkCluster()：裸 Socket 探测 flinkHost:flinkPort 是否通（当前默认端口见 4.1）。
2. 通 → submitViaSqlClient，内部按顺序尝试：
   a. **trySqlGateway**（SQL Gateway REST，主力路径）；
   b. 失败 → trySqlClientScript（本地 .sql + sql-client，依赖 Git bash 路径）；
   c. 失败 → submitViaFlinkRestApi（**注意：它不做提交**，只是轮询 /jobs/overview 认领新出现的 jid）。
3. 不通 或 三段全失败 → 返回 mock id（`flink-job-<uuid>`）。
4. cancelFlinkJob：mock id（`flink-job-` / `mock-` 前缀）直接跳过 REST。
- **mock 的真实后果**：Flink 未启动时提交不会报错，但作业会**一直停在 SUBMITTED**（overview 请求异常被轮询器吞掉，永远不会进入「不在 overview → COMPLETED」分支）。要验证真实链路必须先起集群。
- 已知脆弱点：Gateway 侧按 `;` 裸切分 SQL，单条语句失败只 warn 继续，可能出现「半提交但仍返回 jobId」。

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

### 5.7 状态与日志（间隔已配置化，比旧文档快 3 倍）
- FlinkJobStatusChecker `@Scheduled(fixedRateString = "${app.monitor.status-poll-ms:5000}")` → **默认 5s**（旧文档写 15s），只轮询 SUBMITTED/RUNNING 的作业，按 FLINK_TO_LOCAL_STATUS 映射状态。
- HealthMonitor 健康扫描默认 **10s**（`app.monitor.health-scan-ms`，旧文档写 30s）；HeartbeatMonitor 心跳探测默认 10s（`app.monitor.heartbeat-scan-ms`）。
- 失败即时告警：FlinkJobStatusChecker 置 FAILED 的瞬间直接调 HealthMonitor.notifyJobFailed（事件驱动），JobService.submit 失败分支同样即时通知。
- JobLog 落库，GET /api/jobs/{id}/logs 查询（按时间倒序）。
- 输出后处理：作业转 COMPLETED（或 CANCELLED）时触发 Excel/Xml/Parquet 转换与 CSV/JSON part 合并；cancel/offline 也会显式调用 finalizeJobOutputs。
- **自动 COMPLETED 的判定**：真实 jid 的作业「不在 /jobs/overview 里」即被置为 COMPLETED（含 Flink 重启后归档的情况）。这意味着 Flink 重启可能把 RUNNING 作业误判完成，与 HeartbeatMonitor 的 JOB_HEARTBEAT_LOST 语义存在冲突，改这块要一起考虑。

---

## 6. 关键文件地图

### 6.1 后端
| 文件 | 职责 | 改动注意 |
|---|---|---|
| service/DagTranslationService.java | DAG→SQL、提交降级、schema 传播 | 新增控件主要改这里；先看 5.4 / 5.5 |
| service/JobService.java | 作业 CRUD、提交入口、上线/下线、版本与调度 | 状态流转别乱改；submit 无锁，注意并发触发 |
| service/FlinkJobStatusChecker.java | 5s 状态轮询 + 输出后处理 + 表头合成 | 表头逻辑是独立一条链，见 5.4/5.7 |
| service/MysqlTableCreator.java | MySQL 自动建表 + 类型映射 | 类型映射表、保留字反引号、utf8mb4 |
| service/ExcelPreprocessor.java / JsonPreprocessor / XmlPreprocessor / ParquetPreprocessor | 各种格式 → 临时 CSV | 临时文件放 output/*_temp_csv，注意编码 BOM |
| service/ExcelOutputConverter.java / XmlOutputConverter / ParquetOutputConverter | CSV → 目标格式后处理 | 输出路径来自节点 params.path |
| service/MonitorService.java + FlinkMetricsService | Flink 指标采集、趋势缓冲、历史卡片兜底 | 2s 采集走 HTTP，注意阻塞单线程调度器 |
| service/HealthMonitor.java / AlertService.java / AlertRetentionService.java / HeartbeatMonitor.java | 告警判定、去重、恢复闭环、通知、保留策略 | 新增检查项要同步 shouldAlert/markAlerted |
| ai/LlmClient.java / ai/AgentService.java + ai/PromptCatalog | 多服务商 LLM 客户端、NL2Pipeline、智能诊断 | API Key 加密密钥来自环境变量或 backend/data/.ai_config_key，**不得硬编码** |
| service/JobScheduler.java / DependencyService.java | cron 调度、依赖编排与自动触发 | 30s 扫描；运行中作业不重复提交 |
| service/AuditService（audit 包）/ UserService.java / LineageService.java / PreviewService.java / KafkaMonitorService.java | 审计、用户管理、血缘、预览、Kafka 可视化 | 新端点记得 @PreAuthorize + owner 校验 |
| config/DataInitializer.java | 控件种子（**31 个**，启动时清空重建） | 新增控件第一站，必须与前端 getBuiltinControls 同步 |
| config/SecurityConfig.java / WebConfig.java | 鉴权规则、CORS 白名单 | H2 console 默认关闭且仅 ADMIN；CORS 默认只放行本机 3000 |
| controller/JobController.java | 作业 REST 路由（11 个控制器之一） | 写操作需 ADMIN/OPERATOR + owner |
| dag/DagDefinition.java | DAG 模型 | 字段名即契约，勿改 |
| application.yml | H2、Flink 地址、端口、监控/告警间隔、AI、CORS | 全部可用环境变量覆盖 |

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
2. frontend/js/app.js 的 getBuiltinControls() 同步一份（降级副本）：**类型必须一一对应，且 paramSchema 字面量 JS 求值后必须是合法 JSON**（前端测试 `frontend/test/app.test.js` 会自动校验这两点，不同步会红）。
3. DagTranslationService.translate() 加对应 type 分支：
   - input：生成 CREATE TABLE DDL，并把字段写入 nodeSchemas；
   - output：用 findIncomingSourceSchema 取上游字段（决定 DDL 列），生成 INSERT INTO ... SELECT；
   - transform：接 schema 传播（findIncomingSourceSchema + 生成内联转换 SQL）。
4. 输出类控件如需后处理，参考 ExcelOutputConverter 挂到状态完成链路。
5. 需要输出表头的控件，还要在 FlinkJobStatusChecker 的 sourceHeaderFields / applyTransformHeader 加分支（见 5.4）。
6. 前端参数面板无需改（自动按 paramSchema 渲染）。
7. 跑通一条实测链路（如 9.2），并把结果追加到 1.2。

### 7.2 JDBC 输入（已实现）
- `mysql_input`、`pg_input`、`oracle_input` 均通过 JDBC connector 读取并自动推导字段。
- 所有 JDBC 输入必须接入 `nodeSchemas`，表名需按数据库方言校验和引用，凭据使用环境变量。

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
5. **本机没有 apply_patch**：改文件用编辑器工具或「写 _x.cjs → node _x.cjs → 删」的方式。
6. **PowerShell 复杂内嵌命令会被策略拒**（如内嵌 mvn + 等待 + Start-Process）：拆成简单命令分步执行。
7. **路径参数一律 .trim()**（历史 bug：前导空格导致文件找不到）。
8. **start-all.bat 的中文路径是乱码**：在中文路径机器上以手动命令启动为准（见 4.2）。
9. **H2 元数据库**在 backend/data/mvpdb；切换 MySQL 用 --spring.profiles.active=mysql（application-mysql.yml）。
10. **提交失败先看日志**：GET /api/jobs/{id}/logs；翻译错误会置作业 FAILED 并写 ERROR 日志。
11. **Excel 输出临时文件**：output/*_temp_csv、*.tmp 是中间产物，可清理，不影响功能。
12. **Flink 集群没起时**：提交走 mock，作业会**停在 SUBMITTED**（不会自动 COMPLETED），要测真实链路必须先起 Flink（4.2）。
13. **H2 Web 控制台默认关闭**：不要为了排障把它改成 `enabled: true` 提交；需要时用 `H2_CONSOLE_ENABLED=true` 临时起，且它现在只对 ADMIN 开放。
14. **CORS 是白名单**（`app.security.cors-allowed-origins` / `CORS_ALLOWED_ORIGINS`）：不要改回 `*` + 凭据组合；前端 3000 走 serve.js 反代，本身不需要跨域。
15. **AI 相关密钥不得硬编码**：加密密钥来自 `AI_CONFIG_ENCRYPTION_KEY`，未配置时用 backend/data/.ai_config_key 本机密钥文件；两个文件都在 .gitignore 内，别提交。
16. **控件 paramSchema 必须是合法 JSON**：前端降级副本里写 `D:\code\...` 这类单反斜杠会让 JSON.parse 抛错（历史 bug，已修 11 个控件）。

---

## 9. 回归验证清单（每次改完必跑）

### 9.1 服务健康
- 前端 3000 返回 200、后端 18080（或手工启动的 8080）返回 200、Flink 18081 返回 200（如已启动）。
- 后端 `/api/controls` 返回 **29** 个控件（重启后 DataInitializer 会清空重建注册表）。

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

### 9.4 自动化测试（改完必跑，2026-09-11 实测全绿）
- [ ] `mvn -B -f pom.xml test` → 5 模块 BUILD SUCCESS，后端 **71** 个用例 0 失败。
- [ ] `cd frontend; node --test test/app.test.js` → **9** 个用例 0 失败（含前后端控件注册表一致性 + paramSchema 合法性）。
- [ ] 改了安全配置时的手工验证：未登录访问 `/api/jobs` 返回 401；`/h2-console` 未登录不可访问（默认还是关闭状态）。

---

## 10. 后续完善路线（2026-09-11 重排）
1. **安全收尾**：token 吊销（黑名单/版本号）、Monitor/Kafka/Preview 端点补角色与 owner 限制、ownerId 为空的历史作业收紧可见性、审计与日志脱敏。
2. **Flink 状态语义修正**：区分「作业完成」与「Flink 重启后归档」，消除与 HeartbeatMonitor 的冲突；mock 作业增加超时兜底状态。
3. **并发安全**：JobService.submit 加状态 CAS/锁，定时器与依赖触发不重复提交；输出合并加锁，避免与轮询器并发写同一批 part。
4. **调度线程池隔离**：MonitorService 的 2s HTTP 采集与告警/调度分池，避免互相阻塞。
5. **补测试空白**：FlinkJobStatusChecker、MysqlTableCreator、PluginLoaderService、Kafka/Parquet/HDFS 链路、调度与用户隔离。
6. **前端工程化**：拆分 app.js 巨石、清理死 CSS、定时器/图表统一 dispose（可选引入构建步骤）。
7. **AI 侧**：多服务商接入更多模型、诊断规则库扩充、Prompt 版本管理。
8. **Docker 收尾**：docker-compose 已可用，补齐 compose 环境下的初始化剧本与一键验收脚本。

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
| 2026-08-05 | Kafka 输入体验开关：kafka_input 新增 autoStop/stopAfterSeconds，运行超时后自动取消 Flink 作业并合并输出；合并逻辑兼容 .part-*.inprogress（cancel 后也能出完整文件） | DataInitializer / 前端注册表 / FlinkJobStatusChecker |
| 2026-08-05 | 性能基准：test-resources/benchmark.py 改造为真实提交（POST /api/jobs 建作业 → submit → 15s 轮询 → completedAt-submittedAt 算吞吐），psutil 可选采集 CPU/内存，报告输出 test-results/benchmark_report.md；实测 100k/1M 行 × 并行度 1/2/4 | test-resources/benchmark.py / test-results/ |
| 2026-08-05 | Docker 全链路验证：修复 compose 健康检查（镜像 /bin/sh 为 dash 不支持 /dev/tcp，改 bash -c）、SQL Gateway rest.address 指向 jobmanager；build+up 全容器启动并跑通 CSV→CSV 真实链路 | docker-compose.yml / docker/ |
| 2026-08-05 | 作业复制 + 定时调度：POST /api/jobs/{id}/copy 深拷贝 DAG 重置 DRAFT；JobDefinition 新增 cronExpression/scheduleEnabled/nextFireTime，JobScheduler 每 30s 扫描按 cron 提交（运行中不重复、失败推进下次）；前端作业列表新增复制/调度按钮与调度对话框 | JobController / JobService / JobScheduler / JobDefinition / JobDefinitionRepository / 前端 app.js + index.html |
| 2026-08-10 | XML/JSON 输入输出增强：新增 xml_input/xml_output 控件；json_input 支持 auto/lines/array 模式（array 转临时 CSV 自动推导 schema）；json_output 支持 lines/array（array 合并为 JSON 数组）；XML 输出在作业完成后由临时 CSV 转 XML（rootTag/rowTag 包裹）；JsonPreprocessor/XmlPreprocessor 兼容 UTF-8 BOM | DagTranslationService / FlinkJobStatusChecker / DataInitializer / XmlPreprocessor / JsonPreprocessor / XmlOutputConverter / 前端 app.js 注册表 |
| 2026-08-15 | 数据预览 + 画布可视化增强：新增 PreviewService/PreviewController（GET /api/preview/file 预览 CSV/Excel/JSON/XML/TXT 前 N 行）；前端节点 label 带分类图标（📥输入/🔧转换/📤输出）、连线改虚线、参数面板加「📊 预览数据」按钮与预览弹窗；xml_json UDF 字段级转换实测通过（CSV→xml_json→CSV，作业 COMPLETED） | PreviewService / PreviewController / 前端 app.js + index.html / README |
| 2026-08-15 | 参数面板按钮对齐：📊 预览数据 + 应用参数 改为同一行 flex 等宽并排 | 前端 index.html |

| 2026-08-15 | 实时监控面板 + Parquet 读写 + 数据质量控件 + 输出清理召回：新增 MonitorService/MonitorController（GET /api/monitor/overview 聚合 Flink REST 吞吐/背压/checkpoint，前端 5s 轮询）；HadoopHomeConfig 修复 parquet 输出 winutils 缺失，ParquetPreprocessor 类型感知取值，findSourceHeader 支持 parquet_input 表头；新增 dedupe（ROW_NUMBER 保整行）/validate/route 数据质量控件；cleanOutputTempDirs 提交时清理 *_output 临时产物，finalizeRecentlyCompletedJobs 对 3 分钟内完成作业重试合并修复竞态 | MonitorService / MonitorController / HadoopHomeConfig / ParquetPreprocessor / DagTranslationService / FlinkJobStatusChecker / DataInitializer / 前端 app.js + index.html / README |
| 2026-08-15 | 实时监控面板增强：运行中作业展示实时吞吐/背压/checkpoint；最近结束作业（≤6 条）展示总处理行数与耗时，Flink 快速作业指标缺失时按输出文件行数兜底（CSV/Excel）；前端状态标签分色、历史无指标显示 — | MonitorService / 前端 index.html + app.js |
| 2026-08-15 | 实时监控面板修复 Flink 归档依赖：JobManager 重启后历史作业详情丢失导致卡片不可用，历史卡片改为不依赖 Flink——耗时回退 DB completedAt-submittedAt、行数始终按输出文件兜底（CSV/Excel），仅运行中作业需要 Flink 详情 | MonitorService |
| 2026-08-15 | 实时监控历史卡片数据增强：已结束作业展示平均吞吐（行/s）= 输出行数 ÷ 运行时长；运行时间支持 Flink 执行时长 → DB completedAt-submittedAt → 取消作业 updatedAt-submittedAt 三级兜底；前端运行时间支持毫秒显示 | MonitorService / 前端 index.html + app.js |
| 2026-08-15 | 前端依赖本地化：Vue3.4.38 / ElementPlus2.9.1 / icons-vue2.3.2 / X6 3.1.8 / axios1.6.8 全部下载到 frontend/public/vendor 并由 index.html 本地引用，彻底移除 unpkg CDN 依赖（解决国内/离线网络下整页空白），无头 Edge 实测监控页 6 卡片 + 画布 24 控件正常 | frontend/public/vendor/ + index.html |
| 2026-08-15 | 前端 UI 整体美化：style.css 重写现代数据平台视觉（深蓝渐变头栏/白色半透明 tab/控件圆角卡片 hover 浮起/画布 22px 圆点网格/监控卡片 3px 渐变条+悬浮阴影/日志深色框/Element Plus 圆角胶囊覆盖/滚动条美化），index.html 监控空态改 empty-tip 类；无头 Edge 实测监控页 6 卡片 + 画布 24 控件 + 计算样式（渐变头栏/网格背景/卡片阴影）全部生效 | frontend/public/css/style.css + index.html |
| 2026-08-15 | 实时监控页面重写 + 修复空白根因：原 monitor-view 被错误嵌套进 v-show 隐藏的 controls-view（缺 </div> 闭合）导致整页不可见；已修复嵌套并重写监控页（顶部统计卡 4 项/自动刷新开关/手动刷新/最后更新时间/错误重试态/状态圆点 pulse），无头 Edge 实测监控页 6 卡片可见、画布 24 控件/作业 76 行/控件 24 行回归正常 | index.html / app.js / style.css |
| 2026-08-16 | P0 自动化测试：新增 backend/src/test 三个测试类——DagTranslationServiceTest（7 用例：CSV→CSV schema 传播/输出路径 .tmp 重写、datagen DDL、field_concat 新字段传播、JSON lines→JSON、CSV 文件缺失异常、xml_json UDF 注入、未知控件异常）、JsonPreprocessorTest（3）、XmlPreprocessorTest（1），mvn test 共 11 用例全绿；前端 package.json 新增 check 脚本（node --check public/js/app.js） | backend/src/test/* / frontend/package.json |
| 2026-08-16 | P0 清理：删除根目录调试残留 _resp.json/_sched.cjs/_t.json/backend_h2.log，.gitignore 补充下划线前缀临时文件模式 | .gitignore |
| 2026-08-16 | P1 监控趋势图：MonitorService 新增内存环形缓冲（每作业 120 点 @5s，记录吞吐 in/out 与背压 ratio）+ GET /api/monitor/trends 返回运行中作业时间序列；前端 ECharts 5.5.1 本地化（vendor/echarts.min.js），监控页新增吞吐趋势折线图（time 轴、多作业多色、5s 轮询、无数据空态） | MonitorService / MonitorController / index.html / app.js / style.css / vendor/echarts.min.js |
| 2026-08-16 | P1 画布撤销/重做：X6 history 启用（initGraph history.enabled），工具栏新增撤销/重做按钮（canUndo/canRedo 随模型变更刷新），快捷键 Ctrl+Z 撤销 / Ctrl+Y 与 Ctrl+Shift+Z 重做；loadDagToCanvas/clearCanvas/newCanvas 完成后 history.clean() 使加载/清空不进撤销栈 | app.js / index.html |
| 2026-08-16 | 环境问题记录：JDK 17 在中文 Windows 上 sun.jnu.encoding 固定 GBK（-Dsun.jnu.encoding 无法覆盖），Flink 提交中文输出路径（如 D:\code\比赛\2026省服务外包\output\*.csv）时 TaskManager 报 Failed to create parent directory；英文路径作业正常。临时规避：输出路径用英文目录；根治需 JDK 18+ 或专项调研 Hadoop 路径编码 | 验证记录 |
| 2026-08-16 | 实时监控卡片新增删除按钮：每张监控卡片头部增加删除图标按钮（title=删除此作业），点击确认后调用 DELETE /api/jobs/{id}，成功同步刷新作业列表与监控卡片/趋势；无头 Edge 实测 6 卡片各 1 个删除按钮渲染正常 | app.js（deleteMonitorJob）/ index.html |
| 2026-08-16 | 中文路径问题定位与修复：实证验证「后端→代理→SQL Gateway→JobManager→TaskManager→中文输出目录」全链路在 DAG 中文完整时正常（CSV 中文输入→Flink→中文路径 CSV 输出，内容含中文正确）；此前失败根因是调试脚本经 PowerShell 建作业时中文被替换为 ?（0x3F）污染 DAG。修复 start-all.bat 内被破坏为 ?? 的硬编码路径（改为 %~dp0 相对定位 + MAVEN_OPTS/JAVA_TOOL_OPTIONS UTF-8 防御），恢复 deploy-linux.sh / stop-linux.sh 被 ? 损坏的中文文案 | start-all.bat / deploy-linux.sh / stop-linux.sh / 验证记录 |
| 2026-08-16 | 作业列表搜索/状态筛选：作业管理页新增名称/ID/Flink Job ID 关键词搜索框 + 状态筛选下拉（DRAFT/SUBMITTED/RUNNING/COMPLETED/FAILED/CANCELLED）+ 结果计数（共 N/M），纯前端 computed 过滤；浏览器实测 72→3（关键词）与 59/72（COMPLETED） | 前端 app.js + index.html |
| 2026-08-16 | GitHub Actions CI：新增 .github/workflows/ci.yml（push/PR 触发，backend 用 temurin JDK17 跑 mvn -B test，frontend 用 Node18 跑 npm run check）；单测硬编码的本机绝对路径改为从 user.dir 上级定位 test-resources（Linux CI 可移植） | .github/workflows/ci.yml / DagTranslationServiceTest / XmlPreprocessorTest |
| 2026-08-16 | 作业版本历史/回滚：新增 JobVersion 快照表（保存时自动生成版本号，create=1，update/回滚递增），GET /api/jobs/{id}/versions 列表、POST /api/jobs/{id}/rollback/{versionId} 回滚（恢复 name/dagJson/parallelism 并记录新版本）；前端作业列表新增「历史」按钮与版本弹窗（版本号/名称/并行度/时间 + 回滚确认）；修复 deleteByJobId 缺事务导致的删除 500 | JobVersion / JobVersionRepository / JobService / JobController / 前端 app.js + index.html |
| 2026-08-16 | 示例作业一键导入：画布工具栏新增「示例作业」下拉，内置 3 个示例 DAG（Datagen→CSV、Datagen→JSON、CSV→字段过滤→CSV）一键 loadDagToCanvas 载入（作为未保存新作业）；浏览器实测加载 2 节点 1 连线 | 前端 app.js + index.html |
| 2026-08-16 | README 架构图：新增「〇、系统架构」mermaid 全链路图（前端画布→JobService→翻译层→三路提交→Flink Standalone→输出/监控→元数据库），并更新 start-all.bat 乱码过时提示 | README.md |
| 2026-08-16 | 敏感信息环境变量化：MySQL 输入/输出与预览的用户名/密码支持留空或 ${MYSQL_USERNAME}/${MYSQL_PASSWORD} 占位符，提交/建表/预览时由后端解析为环境变量默认值；控件 paramSchema 默认值改为占位符（不再预填真实密码），DAG JSON 与元数据库不存明文密码；.env 已在 .gitignore | DataInitializer / MysqlTableCreator / DagTranslationService / JobService / 前端 app.js |

| 2026-08-16 | 修复取消作业不合并输出：JobService.cancel 将状态置为 CANCELLED 后立即调用 FlinkJobStatusChecker.finalizeJobOutputs（合并 CSV/JSON part 文件、转换 Excel/XML/Parquet），流式样例作业（Datagen/Kafka）取消后也能产出完整输出文件；此前轮询器只遍历 RUNNING/SUBMITTED，取消后合并逻辑永不触发 | JobService / FlinkJobStatusChecker |

| 2026-08-16 | 企业级 P0 迭代 1：RBAC + JWT + 审计。新增 AppUser（ADMIN/OPERATOR/VIEWER，种子账号 admin/admin123、operator/operator123、viewer/viewer123，密码可用 app.security 环境变量覆盖）、JwtUtil + JwtAuthFilter + SecurityConfig（无状态、方法级 @PreAuthorize、401/403 JSON 处理器）、@Audit 注解 + AuditAspect + AuditLog 表 + GET /api/audit 分页查询、AuthController（/api/auth/login /me /logout）；JobController 全量接 RBAC（写=ADMIN/OPERATOR，owner 隔离，VIEWER 只读），JobDefinition 增 ownerId/ownerName，历史作业回填 admin；前端：登录页 + axios 拦截器（Bearer token / 401 自动登出）+ 顶部用户菜单 + 「操作审计」视图（ADMIN/OPERATOR）+ 作业表「创建人」列 + 「只看我的」开关 + VIEWER 隐藏保存/提交/删除等按钮 | security/* / audit/* / AppUser / AuditLog / AuthController / AuditController / JobController / JobService / DataInitializer / pom.xml / application.yml / 前端 app.js + index.html + style.css |
| 2026-08-16 | 企业级 P0 迭代 2：作业依赖编排。新增 JobDependency 表 + DependencyService（保存依赖全量覆盖、环检测、自依赖拒绝、状态联动 WAITING/BLOCKED）、GET/PUT /api/jobs/{id}/dependencies + GET /api/jobs/workflow；JobScheduler 每 30s 扫描：上游全部 COMPLETED → 自动提交下游，上游 FAILED/CANCELLED → 下游 BLOCKED + webhook 告警；JobDefinition.JobStatus 增 WAITING/BLOCKED（H2 旧枚举 CHECK 约束已删除）；JobService.delete 联动清理依赖边；前端：作业行「⛓ 依赖」弹窗（多选上游）+「工作流」ECharts 视图（节点按状态着色 + 依赖箭头）；实测 A(CSV→CSV) 完成后 B(CSV→JSON) 自动提交并完成 | JobDependency / DependencyService / JobScheduler / JobService / JobController / 前端 app.js + index.html |
| 2026-08-16 | 企业级 P0 迭代 3：数据血缘。新增 LineageService + LineageController：GET /api/lineage/job/{id} 解析作业 DAG 的输入/输出资产（文件路径/库表/Kafka topic/Datagen），GET /api/lineage/workflow 组合全工作流「作业节点+资产节点+作业内边+跨作业依赖边」；前端作业行「🧬 血缘」弹窗用 ECharts 展示资产流向（左输入→作业→右输出）；按 owner 隔离 | LineageService / LineageController / 前端 app.js + index.html |

| 2026-08-16 | 登录页独立全屏化 + 视觉升级：未登录时整个主应用 v-if="user" 不渲染，仅显示独立登录界面（左侧深蓝渐变插画区 + 右侧白色表单卡片）；新增自绘数据流主题 SVG 配图 frontend/public/images/login-hero.svg（节点连线流动动画、输入/转换/输出节点、数据库落库）；登录成功后懒初始化 X6 画布（此前依赖主应用常驻渲染），登出时 graph.dispose() 释放；补充 user-menu 样式（此前迭代 1 的 CSS 追加脚本因解析错误未落盘，本次一并补齐）；修复 style.css 登录样式缺失问题 | frontend/index.html + style.css + app.js + images/login-hero.svg |

| 2026-08-16 | 帮助中心重设计：顶部「帮助」按钮改为下拉菜单（快速上手/控件说明/快捷键/技巧与 FAQ/关于，命令直达对应标签）；帮助弹窗改为 820px 五 Tab 布局——快速上手 6 步引导、控件说明动态读取后端控件注册表按输入/转换/输出分组展示（悬停显示描述）、快捷键表格（kbd 键帽样式）、技巧与 FAQ（流式作业取消/提交排查/依赖编排/血缘/定时调度）、关于（技术栈 + 服务端口 + 演示账号）；新增 .help-* 系列样式 | frontend/index.html + style.css + app.js |

> 新改动请在下方继续追加，保持框架可追溯。
| 2026-08-17 | 演示矩阵扩展为 6×6（加入 Kafka）：新增 11 个作业——CSV/Excel/JSON/XML/MySQL→Kafka（写 `kafka_sales_flow`）+ Kafka→CSV/Excel/JSON/XML/MySQL/Kafka（读 `kafka_sales_flow`，autoStop 体验模式）；为保持数据格式一致，由矩阵产物生成 sale 格式样例 `test-resources/data/sales_demo.{xlsx,json,xml}`；修复并发消费冲突：Kafka 输入 group.id 由节点 id 派生，此前 6 个消费作业节点 id 相同导致 JMX MBean 冲突/同组抢分区，改为唯一节点 id 后逐个验证——Kafka→CSV 2.4MB、Excel 1.4MB、JSON 7.3MB、XML 12.4MB、MySQL 42000 行、Kafka→Kafka echo topic 42000 条，全部通过 | 演示作业数据重建 + 前端 app.js |
| 2026-08-17 | 输出清理修复 + 演示作业重建为 5×5 矩阵：修复 Excel/Parquet 输出 finalize 成功时只删 temp 目录、遗留 `<path>_merged.csv` 的 bug（现一并删除，作业450 重跑仅剩 xlsx 验证通过）；演示作业重建为 25 个 5×5 矩阵作业（CSV/Excel/JSON/XML/MySQL 输入 × 同 5 类输出全组合，`演示 CSV→Excel` 命名，全部带 x/y 坐标修复画布空问题、输出统一 output/m_*.{csv,xlsx,json,xml} + MySQL 自动建表 m_*）；25 个作业全部真实 Flink 提交并 COMPLETED（核验无 mock 降级、文件与表数据正确）；删除 78 个历史作业（备份见 backend/_job_backup_20260817.json） | FlinkJobStatusChecker / 作业数据重建 |
| 2026-08-17 | 调度改自然语言（小白友好）：调度弹窗新增「大白话描述」输入（每5分钟 / 每小时 / 每天凌晨2点 / 每周一早上9点 / 每月1号0点 等，支持下午/晚上自动 +12、X点半=30 分）一键识别为 6 段 cron；新增 12 个常用频率预设标签（点击即选）；cron 输入框实时翻译成中文（cronToHuman：每分钟/每小时/每天 HH:mm/周几/每月N号），作业列表「调度」列也显示中文描述；修正原提示中 Quartz 风格的 `?` 占位（Spring CronExpression 不支持）；FAQ 同步更新。nlToCron/cronToHuman 单测 13+7 用例全过 | 前端 app.js + index.html |
| 2026-08-16 | 补齐缺失的输入数据文件：批量扫描 78 个作业的 DAG 输入节点（csv/excel/xml/json/parquet），定位并补齐 3 个缺失输入——test-resources/data/quality_test.csv（由 quality_demo.csv 恢复，供 6 个质量控件作业 dedupe/validate/route 使用）、output/multi_test.xlsx（openpyxl 生成 3 sheet：Users/Data/Summary，Users 来自 users.csv 51 行，供 excel_input sheetName 测试）、output/test_parquet_out.parquet（复用后端 ParquetOutputConverter 由 users.csv 前 30 行生成，8 列，ParquetPreprocessor 回读验证 30 行）；docker_fullchain_verify3 的 /data/sales.csv 为容器内挂载路径，保持不动；复扫后仅剩该 docker 路径 | 数据文件（test-resources/data、output）+ 变更记录 |
| 2026-08-16 | 实时监控数据保留：作业运行结束后趋势曲线不再清空（GET /api/monitor/trends 返回运行中与最近结束作业的缓冲曲线，运行中排前、结束后按最近时间倒序）；重新提交作业时按 submittedAt 识别新运行批次并重置趋势缓冲，只保留当前运行曲线；历史卡片在 Flink 归档与输出文件统计都拿不到时，用最近一次运行的实时快照兜底展示「结束前吞吐/背压」；前端趋势标题/空态文案同步更新，历史卡片新增结束前吞吐/背压展示。实测：datagen→CSV 运行采集 12 点→取消后趋势保留+卡片显示 4200 行/平均 100 行/s→重新运行后缓冲重置只留新曲线 | MonitorService / app.js / index.html |
| 2026-08-16 | Kafka 全链路实测 + 参数面板修复：docker compose 启动 Kafka 3.8（宿主机 localhost:29092），主题 csv-to-kafka / kafka_sales_flow / xml-to-kafka-demo；实测 生产者#678（sales.csv 3000 行 → kafka_sales_flow，JSON 消息 3000 条）→ 消费者#679（earliest-offset 消费 → kafka_consumer_out.csv 3001 行含表头，中文正常）；修复后端 kafka_input 控件 paramSchema 中 autoStop/stopAfterSeconds 误放在 properties 外导致参数面板不渲染「体验模式：消费完自动停止」开关的问题（与前端副本对齐，重启后 /api/controls 验证生效） | DataInitializer / 验证记录 |
| 2026-08-17 | 企业级 P0 迭代 4：作业上线/下线。JobDefinition 新增 online/onlineSince/onlineRestartCount/lastRestartAt；上线校验 Flink 集群可用（不可用直接报错，禁止 mock 降级），流式作业（kafka_input/datagen_input）立即提交并强制 kafka_input autoStop=false，批量作业要求已配置 cron 并启用调度、立即执行一次；下线复用 CANCELLED：停调度、取消 Flink 作业、finalizeJobOutputs；JobScheduler 新增 superviseOnlineJobs——流式异常（FAILED/CANCELLED/COMPLETED）或批量失败时自动重新提交，10 分钟窗口最多 3 次，超限 webhook 告警（ONLINE_JOB_STOPPED）并自动下线；POST /api/jobs/{id}/online|offline（ADMIN/OPERATOR + 审计）；前端作业表状态列「在线」绿色徽标 + 上线/下线按钮 + FAQ。实测：Kafka 流式上线 RUNNING→外部取消→自动重启（restartCount=1）；批量 CSV→MySQL 上线后按每分钟 cron 重跑出第二个 Flink Job；下线后不再拉起；停 Flink 后上线返回 500「Flink 集群不可用」 | JobDefinition / JobService / JobScheduler / JobController / JobDefinitionRepository / DagTranslationService / 前端 app.js + index.html |
| 2026-08-16 | 修复吞吐趋势恒为 0：Flink SQL 的 Source→Sink 融合算子链上通用 numRecordsIn/OutPerSecond 恒为 0，原 collectLiveMetrics 取该通用指标导致趋势图无波动；改为先拉取 vertex 全部指标名，分块查询含 numRecordsIn/OutPerSecond 的算子作用域指标（Source__*.numRecordsOutPerSecond、StreamingFileWriter/Sink__*.numRecordsInPerSecond 等），去数字前缀按算子聚合求和取最大值；实测 datagen 300 行/s 趋势从 0→90→140→190 正常波动，overview 实时吞吐同步修复 | MonitorService |
| 2026-08-16 | 吞吐趋势图 Y 轴固定刻度：max=500 行/s、min=0，避免自动缩放导致曲线跳动（超过 500 会被截断，可在 updateTrendChart 调整） | 前端 app.js |
| 2026-08-17 | AI 智能化层三条线：① NL2Pipeline Agent（DeepSeek 自然语言→DRAFT 作业，AI 只按控件注册表生成合法类型/参数，落库 DRAFT + 人工确认后提交）；② 故障感知告警中心（HealthMonitor 每 30s 扫描 JOB_FAILED / ONLINE_JOB_STOPPED / THROUGHPUT_ZERO / BACKPRESSURE_HIGH / CHECKPOINT_FAILED，15 分钟去重，落库 + webhook + 邮件通知）；③ 智能诊断 Agent（本地 8 类高频错误规则引擎优先，未命中走 DeepSeek 归因，返回 rootCause / evidence / suggestions / paramFixes 一键修复）；DeepSeek Key 与 SMTP 授权码等敏感参数仅入 .env 不提交仓库 | LlmClient / AgentService / AiController / AlertService / HealthMonitor / AlertController / AlertRecord / 前端 index.html + app.js + style.css |
| 2026-08-17 | 告警中心新增删除：AlertController 增加 DELETE /api/alerts/{id}（按 id 删除，保持现有无状态风格）；前端告警表格操作列新增「删除」按钮，ElMessageBox 确认后调用删除并刷新列表、未读角标同步更新 | AlertController / 前端 index.html + app.js |
| 2026-08-17 | 修复审计日志加载 400：index.html 中 @click/@keyup.enter 裸绑定 loadAudit 会把 PointerEvent 当 page 参数传入（请求变成 /api/audit?page=[object+PointerEvent] 被 Tomcat 拒绝）；改为显式 loadAudit()，扫描确认其它裸绑定函数均无参数不受影响 | 前端 index.html |
| 2026-08-17 | 作业管理页去臃肿（A1+A2+A3）：① 操作列从 11 按钮折叠为 编辑/运行/上线(下线)/详情/删除 + 「⋮ 更多」下拉（复制/调度/依赖/诊断/日志/历史/血缘），列宽 560→360 并 fixed=right；② 工具栏新增「列设置」弹层，默认隐藏 描述/并行度/Flink Job ID/更新时间 四列可勾选显隐；③ 新增右侧详情抽屉（el-drawer，10 个信息字段 + 全部操作入口），支持「详情」按钮或双击行打开；双击行由直接编辑改为打开详情，编辑入口保留在按钮与抽屉内 | 前端 index.html + app.js + style.css |
| 2026-08-17 | 修复告警重复发送：HealthMonitor 去重从内存 Map 改为 DB 持久化——离散事件（JOB_FAILED / ONLINE_JOB_STOPPED）按「故障发生时间（completedAt/lastRestartAt/updatedAt）vs 最近一条告警 createdAt」判断，同一故障在后端重启后不再重复告警；持续状态（THROUGHPUT_ZERO / BACKPRESSURE_HIGH / CHECKPOINT_FAILED）改为 DB 15 分钟冷却（重启同样生效）；新增 shouldAlert 帮助方法 | HealthMonitor |
| 2026-08-20 | AI 助手界面美化（仅视觉层）：AI 助手页重构为「渐变 Hero + 流程步骤条 + 输入卡片 + 生成结果流程预览（输入/转换/输出彩色节点 + 箭头 + 统计）」，诊断弹窗改为自定义头部（图标 + 作业号）、根因/证据/建议卡片化、空状态与来源标识；所有 Vue 绑定、方法名与 JS 逻辑保持不变 | 前端 index.html + style.css |
| 2026-08-25 | 修复 MySQL 8 认证失败（Public Key Retrieval is not allowed）：新增 util/JdbcUrlUtil.normalize 统一为 jdbc:mysql URL 补 allowPublicKeyRetrieval=true，应用于自动建表（MysqlTableCreator）、MySQL 输入字段推导与翻译层 DDL（DagTranslationService）、输出预览（JobService）；同步更新控件注册表/前端内置控件默认 JDBC URL 与 AI 提示词示例 | JdbcUrlUtil + MysqlTableCreator + DagTranslationService + JobService + DataInitializer + AgentService + app.js |

| 2026-08-25 | Kafka 数据可视化页面（具象化 Kafka 传入/输出数据）：后端新增 GET /api/kafka/topics（topic 列表 + 可读消息量）与 GET /api/kafka/stream（SSE 实时订阅流，参数 topic/from/maxMessages/durationMs，SseEmitter + 独立 daemon 线程，连接断开自动中断消费）；前端新增「Kafka 可视化」导航与视图——Topic 选择（从最早/实时）、开始/停止接收、4 张统计卡片（已接收消息/实时速率/Topic 可读总量/连接状态）、Producer→Broker→Consumer 数据流转动画、ECharts 吞吐曲线（60 点滚动窗口）、实时消息流面板（partition/offset/时间/JSON 内容，上限 200 条自动滚动）；修复 app.js 主应用 return 对象 `stopKafkaStream` 后缺失逗号导致的 SyntaxError（node --check 通过）；实测：admin 登录→Kafka 可视化→kafka_sales_flow（3000 条）→开始接收→消息流与吞吐图表实时刷新，控制台 0 报错 | KafkaMonitorService + KafkaMonitorController + 前端 index.html + style.css + app.js |

| 2026-08-25 | 修复 Flink Windows 启动/停止脚本（一键启停回归可用）：start-cluster.bat 此前被改为 Git Bash 调用不存在的 start-cluster.sh 且中文 echo 乱码导致启动失败，stop-cluster.bat 同样损坏；重写为纯 ASCII + CRLF 版本——start-cluster.bat 直接用 java 启动 JobManager(8081)+TaskManager+SQL Gateway(8083)（SQL Gateway 需要 FLINK_CONF_DIR 环境变量，脚本内已 set，不再传 --configDir），JAVA 自动探测 %JAVA_HOME% 并回退硬编码路径，内置 curl 健康检查（避免 %{http_code} 与 > 被 cmd 解析），支持 -nopause 参数便于自动化；start-flink-cluster.bat 改为转发 start-cluster.bat；stop-cluster.bat 用 PowerShell 按命令行特征（StandaloneSessionClusterEntrypoint / TaskManagerRunner / SqlGateway）精确结束 Flink 进程，不会误杀 mvp-backend；等待改用 ping -n 代替 timeout（非交互不报 Input redirection 错误）。实测 停→启 全流程通过：8081/8083 健康检查 OK、TaskManager 1 个 4 slots 注册 | D:\code\flink-1.18.1\bin\start-cluster.bat + start-flink-cluster.bat + stop-cluster.bat |

| 2026-08-25 | 新增 15 万行大数据集（4 格式）供性能/演示测试：新增 test-resources/data/large/ 目录，统一销售业务数据（sale_id/region/product_category/sales_person/amount/quantity/sale_date/channel，中文正常）各 150,000 行——sales_150k.csv(8.7MB)、sales_150k.xlsx(6.2MB)、sales_150k.json(26.6MB)、sales_150k.xml(43.9MB)，共 85.4MB；生成脚本 test-resources/gen_large_data.py（内置 Python 3.12 运行，可传行数参数重新生成，xlsxwriter 写 Excel）；验证：CSV/Excel 150001 行含表头、JSON/XML 150000 条、中文抽查正常 | 数据文件 + gen_large_data.py |

| 2026-08-25 | large/ 新增「大学生」大数据集：student_150k.csv(12.8MB)/.xlsx(8.1MB)/.json(33.9MB)/.xml(53.8MB) 各 150,000 行，字段 student_id/name/gender/age/college/major/grade/gpa/courses/monthly_expense/enroll_date（中文学院/专业/年级，GPA 0.5-4.0）；gen_large_data.py 升级为多数据集生成器（python gen_large_data.py [sales|student] [行数]，内置 Python 3.12 + xlsxwriter，默认 all 150000）；验证：CSV/Excel 150001 行含表头、JSON/XML 150000 条、中文抽查正常，目录总 194MB | 数据文件 + gen_large_data.py |

| 2026-08-25 | 修复「点击编辑后画布为空」：loadDagToCanvas 在 graph 未初始化时直接静默 return，而多个入口（作业编辑 openJob / AI 诊断一键修复 / 版本回滚 / 导入 JSON）均无画布初始化保护（仅 AI 打开与示例作业有 if(!graph) 前置初始化），登出再登录或特定时序下 graph 为 null 时点编辑即得空白画布；改为 loadDagToCanvas 开头自动补初始化（initGraph + setupDropHandler + 双 rAF/100ms 兜底等待）再加载，覆盖全部入口；已排查 42 个作业 dagJson 全部含节点与坐标、无重复节点 id、控件 paramSchema 全部合法（排除数据因素）；浏览器回归：8 个作业编辑、详情抽屉编辑、快速切换、登出重登后立即编辑均正常，控制台 0 报错 | 前端 app.js |

| 2026-08-25 | 完善 start-all.bat 一键启动（幂等 + 修复 SQL Gateway BindException）：原脚本 [2/4] 无条件启动 SQL Gateway，端口已被占用时抛 BindException；且 [1/4][3/4] 用 curl 检查被本机 HTTP_PROXY(127.0.0.1:15721) 劫持导致误判。重写：① 全部端口检查统一用 netstat LISTENING（不依赖 HTTP/代理，start-cluster.bat 健康检查同步改）；② SQL Gateway 启动前检测 8083，已监听则跳过；③ Backend 用顶层 goto 结构启动 + 轮询等待最多 2 分钟（避免 if 括号块内 goto 标签语法错误）；④ 前端改用 node serve.js（原 npx serve 依赖网络）；⑤ 等待全部用 ping -n 替代 timeout（非交互不报错）；⑥ 支持 -nopause 参数便于自动化；⑦ 结尾真实状态汇总。另修复后端无法启动的根因：backend/target/maven-status/.../inputFiles.lst 损坏导致 maven-compiler-plugin 增量编译 BUILD FAILURE（Error reading old mojo status），删除 target 后全量重建成功。实测：完整冷启动（Flink→SGW→Backend→Frontend 全起 + 汇总 OK）与全服务运行时幂等跳过（不再 BindException）均通过 | start-all.bat + start-cluster.bat + backend target 重建 |

| 2026-09-02 | 修复告警中心「刷新」报 500：前端 `@click="loadAlerts"` 裸绑定把点击事件 `$event` 当 `page` 参数传入，`page-1=NaN` → `GET /api/alerts?page=NaN`，后端 Spring 转 int `MethodArgumentTypeMismatchException` 被全局 RuntimeException 兜底成 500（与审计日志裸绑定 loadAudit 同类坑）；改为 `@click="loadAlerts()"`。并后端 GlobalExceptionHandler 新增 `MethodArgumentTypeMismatchException` → 400 兜底（page=NaN/abc 等非法数字不再 500），同时规避前端误传事件/坏参数整类问题 | 前端 index.html + 后端 GlobalExceptionHandler |

| 2026-09-02 | 告警及时性优化（故障感知提速）：根因=链路上两个轮询间隔叠加（FlinkJobStatusChecker 15s 检测失败 + HealthMonitor 30s 扫描才告警，最坏 ~45s）。优化：① 两个间隔配置化并调低——HealthMonitor health-scan-ms 默认 30s→10s、FlinkJobStatusChecker status-poll-ms 默认 15s→5s（application.yml app.monitor.*，可环境变量覆盖）；② 事件驱动——FlinkJobStatusChecker 在「置 FAILED」瞬间调用 HealthMonitor.notifyJobFailed(job) 即时告警，不再等下一次扫描；去重复用 checkFailed（DB 按故障发生时间去重），不会重复告警。实测：提交期失败（坏 CSV 路径）→ 告警约 6s（原最坏 30s），无重复告警；运行期失败在 FAILED 转换即告警 | HealthMonitor + FlinkJobStatusChecker + application.yml + HealthMonitorTest |
| 2026-09-03 | 告警中心四项增强：① 保留策略——AlertRetentionService 每 6h 清理（保留 30 天且最多 5000 条，app.alert.retention-* 可配，0=关闭）；② 历史告警归属回填——DataInitializer 启动时按 jobId 反查作业 ownerId 补齐（作业已删则归 admin）；③ 恢复闭环——HealthMonitor 登记 activeFailures，作业恢复 RUNNING/吞吐恢复/背压恢复/checkpoint 恢复时发送 INFO 级 ALERT_RESOLVED；④ 通知渠道——Webhook 失败重试 2 次（2s/4s 退避，异步不阻塞）、INFO 级不发邮件、按 URL 自动适配钉钉/企业微信/飞书消息格式（自定义 webhook 保持原生 JSON 含 level 字段） | AlertRetentionService（新增）+ DataInitializer + HealthMonitor + AlertService + AlertRecordRepository + application.yml |
| 2026-09-03 | 故障感知层补全（企业验收三点）：① 资源监控——引入 OSHI 6.4.13 采集 CPU/内存/磁盘使用率，任一 ≥ 阈值（app.monitor.resource-*-percent，默认 90）持续 2 分钟触发 RESOURCE_HIGH 告警，恢复后 RESOLVED 闭环（系统级 job=null）；② 日志关键字扫描——supervise 对运行中作业扫描最近 ERROR 日志（含 Exception），水位线（logScanWatermark）保证同一条只告一次 LOG_ERROR；③ 邮件内容达标——正文改为结构化 HTML（级别/故障类型/发生时间/作业 ID/作业名/Flink Job ID/详情）+「智能诊断报告生成中」引导语，主题带【严重】【警告】前缀 | pom.xml + HealthMonitor + AlertService |
| 2026-09-04 | 告警延迟进一步压缩（提交期失败 0.8s 实测）：此前运行期失败已有事件驱动（FlinkJobStatusChecker 置 FAILED 瞬间 notifyJobFailed），但「提交期失败」（JobService.submit catch 分支置 FAILED）仍要等 HealthMonitor 扫描（最坏 10s）。补齐：JobService.submit 的 catch 中置 FAILED 后同步调用 healthMonitor.notifyJobFailed(job)，并补 completedAt（故障发生时间用于 DB 去重）。端到端实测：创建坏路径 CSV 作业→submit→告警出现仅 **0.8 秒**（原最坏 ~10s）；HealthMonitorTest 回归通过，DB 去重保证轮询器不重复告警 | JobService + JobServiceTest |
| 2026-09-04 | 修复「重复运行失败作业不再告警」：手动重跑同一坏作业第 2 次起无告警。两个根因：① JobService.submit 失败分支 completedAt 只在首次为 null 时写入，重跑时 completedAt 未刷新 → 去重判断 lastAt>=occurred 误判为同一旧故障；改为每次失败都刷新 completedAt（每次重跑=新故障）；② shouldAlert 对离散事件也先过 15 分钟内存冷却（alertCooldown），冷却期内直接拦截事件驱动的即时告警；改为离散事件（occurrenceTime!=null）跳过内存冷却、仅用 DB 时间比较去重（同一次故障内轮询器/扫描器重复检测仍被 lastAt>=occurred 拦住），持续状态（吞吐/背压/checkpoint）保持内存+DB 双冷却。端到端实测：同一作业连续 3 次错误提交，3 次均在 ~1 秒独立产生 JOB_FAILED 告警；38 项后端测试回归通过 | JobService + HealthMonitor |
| 2026-09-04 | 第一批多数据源控件（PG/Oracle 输入 + Redis 字段富化）：① pg_input/oracle_input——复用 flink-connector-jdbc（PG/Oracle 方言内置），通用 generateJdbcInputDDL + inferJdbcFields（SELECT * WHERE 1=0 元数据推导，Oracle 表名/用户名自动大写），后端 pom 补 postgresql-42.7.3 + ojdbc11（字段推导用），flink/lib 补同驱动（运行时用）；② redis_lookup 富化控件——udf 工程新增 RedisLookupUdf（ScalarFunction + Jedis 连接池 open() 初始化 + Caffeine TTL 60s 缓存，shade relocation），翻译层按节点注入 CREATE FUNCTION redis_lookup，transform 分支生成 SELECT *, redis_lookup(...) AS targetField，FlinkJobStatusChecker transformTypes/applyTransformHeader 同步追加 targetField；③ 部署修复——flink-conf.yaml rest.port 统一 18081（此前 overrides 重复键残留 8081，Gateway REST 提交连 8081 被拒 Connection refused）、sql-gateway.port 固化 18083（本机 8083 被 ArmouryCrate Bound 占用）。端到端实测：CSV→redis_lookup→CSV COMPLETED（extra_info 列成功富化）；PG students→CSV COMPLETED（字段自动推导）；Oracle DEMO_STUDENTS→CSV COMPLETED；38 项后端测试回归通过 | DagTranslationService + DataInitializer + FlinkJobStatusChecker + RedisLookupUdf（udf）+ pom.xml ×2 + flink-conf.yaml + 前端 app.js |
| 2026-09-05 | HDFS 输入/输出控件：新增 hdfs_input（filesystem+CSV，fieldsConfig 显式 schema）与 hdfs_output（沿上游 schema 写 HDFS 目录 part 文件），后端注册表与前端降级副本同步；修复输出 schema 按节点别名取值导致含连字符节点退化为 data STRING；本机 Flink 部署 Hadoop uber JAR，并通过 HADOOP_CONF_DIR/hdfs-site.xml 启用 dfs.client.use.datanode.hostname，使宿主机 Flink 可访问 Docker DataNode。真实端到端验证：hdfs://localhost:9000/data/students.csv 查询返回 2 行；VALUES→hdfs://localhost:9000/output/result 生成 part 文件且内容正确 | DagTranslationService + DataInitializer + DagTranslationServiceTest + 前端 app.js + Flink/HDFS 本地配置 |
| 2026-09-05 | 修复资源告警邮件反复发送：日志确认 Windows 内存长期在 88%~97% 且阈值为 90%，原逻辑在持续超限时每 15 分钟重复发送 RESOURCE_HIGH，并在略低于 90% 时立即恢复，形成临界值抖动。改为同一活跃故障只告警一次；恢复需 CPU/内存/磁盘全部低于各自阈值 5 个百分点并连续保持 2 分钟，避免 89%/91% 往返触发；增加确定性回归测试覆盖持续超限不重发、临界值不恢复、稳定恢复闭环 | HealthMonitor + HealthMonitorTest |
| 2026-09-07 | P0-P2 安全与工程治理：静态资源路径限制在 public；控件写接口仅 ADMIN；文件预览限制目录/大小；JWT 与种子账号改为显式环境配置；输出临时目录删除限制在 OUTPUT_ROOT；MySQL 预览表名校验；AI 诊断补 owner 校验；修复列表提交错误画布校验与 Kafka 流重启竞态；插件源码去 BOM 并设 UTF-8/Java17；新增根聚合 POM，CI 执行全模块验证；Docker 排除旧 MySQL 驱动 | serve.js + security/config/service/controller + app.js + Maven/CI/Docker + docs |
| 2026-09-08 | 企业验收补齐（P0：5 项 + P1：3 项）。① 用户管理 CRUD：仅 ADMIN 的 /api/users 列表/创建/改/重置密码/删除，BCrypt、用户名密码校验、防删当前账号、保留至少一个启用管理员、操作审计；② AI 草稿确认模型：JobDefinition 增 source(MANUAL/AI)、confirmationStatus(NOT_REQUIRED/PENDING/CONFIRMED)、confirmedBy/ByName/At；服务端强制推导确认状态（忽略客户端伪造，防绕过），未确认 AI 草稿禁止提交/上线，POST /api/jobs/{id}/confirm 记录确认人，前端提交 AI 草稿先确认；③ 独立心跳检测：HeartbeatMonitor 监测 JobManager/TaskManager 存活与 RUNNING 作业从集群消失，CLUSTER_HEARTBEAT_LOST / JOB_HEARTBEAT_LOST + 恢复闭环，DB 冷却去重；④ 诊断报告持久化：DiagnosisReport 实体 + /api/ai/diagnose/{jobId}(GET) 历史查询；⑤ 故障自动诊断闭环：AlertService 发布 AlertTriggeredEvent，@Async AutoDiagnosisListener 自动调用 AgentService.autoDiagnose 生成 trigger=事件 的报告（无循环依赖）；⑥ 双语 NL2Pipeline：LanguageDetector 语言检测 + 英文提示词；⑦ Prompt 模板管理：ai-prompts/nl2pipeline.{zh,en}.txt 外部资源 + {{registry}} 注入；⑧ 20 万条验收脚本 acceptance_200k.py（全覆盖/完整性/耗时吞吐报告）；后端 66 测试全绿，Maven Reactor 5/5 | UserController/UserService/JobDefinition/JobService/HeartbeatMonitor/DiagnosisReport/AutoDiagnosisListener/AlertService/AgentService/PromptCatalog/LanguageDetector/acceptance_200k.py + docs |
| 2026-09-10 | 企业认证门户升级：登录封面重设计为深色数据编排品牌页（能力指标、动态数据链路、响应式布局）；登录/注册双模式切换；新增公开 POST /api/auth/register，自助注册执行用户名/显示名/密码强度校验、BCrypt 哈希、重复账号冲突保护，账号固定为启用 VIEWER 并记录 REGISTER 审计；前后端认证测试通过 | AuthController / SecurityConfig / 前端 index.html + app.js + style.css |
| 2026-09-10 | 监控页吞吐趋势支持逐条移除：独立刻度模式下每条作业曲线右侧新增删除按钮，仅清理该作业趋势（内存缓冲 + 落库趋势点），不影响作业本身；后端新增 MonitorService.removeTrend 与 DELETE /api/monitor/trends/{jobId}，删除失败只降级内存清理不阻断热路径；后端 71 测试、前端 8 测试全绿 | MonitorService / MonitorController / 前端 index.html + app.js + style.css |
| 2026-09-11 | 未提交工作收口：把 2026-09-07~09-10 期间已完成但散落在工作区的 66 个文件（安全与工程治理、用户管理、AI 草稿确认、心跳检测、诊断持久化与自动诊断、双语 Prompt、认证门户、趋势移除、验收脚本）整理为一次可追溯提交 `5f5671d`；提交前核对暂存清单，确认 `.env` / `backend/data/` / `output/` / `large/` 等敏感与大数据目录被 .gitignore 排除 | 66 files（含新增 pom.xml 根聚合、HeartbeatMonitor、UserService、ai-prompts 等） |
| 2026-09-11 | **P0 安全加固**：① H2 Web 控制台默认关闭（`H2_CONSOLE_ENABLED`，且 `SecurityConfig` 由 permitAll 改为 **仅 ADMIN**）——此前未认证即可读写元数据库；② CORS 由 `allowedOriginPatterns("*") + allowCredentials(true)` 改为可配置白名单（`app.security.cors-allowed-origins` / `CORS_ALLOWED_ORIGINS`，默认只放行本机 3000，写 `*` 时自动关闭凭据）；③ 删除 LlmClient 中硬编码的加密兜底密钥 `dataflow-mvp-ai-provider-dev-key-2026`，改为「环境变量 `AI_CONFIG_ENCRYPTION_KEY` → 本机自动生成密钥文件 `backend/data/.ai_config_key`」，并提供 `AI_LEGACY_ENCRYPTION_KEY` 一次性迁移与「落盘密钥不可用时回退环境变量 Key」的兜底，避免 AI 能力中断。**运行时冒烟验证（2026-09-12）**：未登录访问 `/api/jobs`、`/api/controls`、`/h2-console/` 均返回 401；CORS 白名单来源 `http://localhost:3000` 返回 `Access-Control-Allow-Origin` + `Allow-Credentials: true`，非白名单来源返回 403 且无 allow-origin 头；启动日志无「H2 console available」行（控制台未启用）；`backend/data/.ai_config_key` 首启自动生成（32 字节随机），旧的固定密钥密文解不开时按预期回退环境变量 Key 并给出可操作告警 | SecurityConfig / WebConfig / application.yml / LlmClient / .env.example |
| 2026-09-11 | 两个确定性缺陷修复：① `HealthMonitor.checkLogKeywords` 中恒真条件 `... \|\| msg.length() > 0`（等于任意 ERROR 日志都命中）改为 `!msg.isBlank()`；② 前端 `getBuiltinControls()` 降级副本补齐缺失的 `datagen_input`（后端 29 个 vs 前端 28 个），修复 3 处历史编辑残留（行首多余逗号造成的**数组空元素**、缩进错乱、两条控件挤在同一行），并规整 11 个非法 JSON 的 paramSchema（`D:\code\...` 单反斜杠 / 嵌套 `\"` 未二次转义 → 参数面板 `JSON.parse` 抛错）；新增前端测试「内置控件降级副本与后端注册表保持一致」用 DataInitializer 反查类型清单，前后端控件数量/类型/paramSchema 合法性纳入 CI | HealthMonitor / 前端 app.js + test/app.test.js |
| 2026-09-11 | 文档纠偏：把第 2 节技术栈、第 3 节目录结构（11 Controller / 27 Service / 11 Repository、plugin-sdk、ai_sidecar、data、docs、test-results）、第 4 节端口与启动命令（本机 18080/18081/18083、start-all.bat 不编译、需带 FLINK_CLUSTER_PORT）、第 5.4 节 schema 链与死代码说明、第 5.5 节提交顺序（**Gateway 优先**，REST 仅轮询认领）与 mock 真实后果（停在 SUBMITTED）、第 5.7 节轮询间隔（5s/10s/10s）、第 6.1 节文件地图（DataInitializer 种子 **29** 个）、第 7.1 节新增控件流程、第 8 节红线（新增 H2/CORS/AI 密钥/paramSchema 四条）、第 9 节回归清单（补自动化测试）与第 10 节路线全部改写；README 同步端口、轮询间隔、mock 说明、H2 开关、环境变量表、docs 现状 | DEVELOPMENT.md / README.md / .env.example |
| 2026-09-11 | 新增根目录 `AGENTS.md` 工作区指令（DSH 自动注入，每个会话首次请求加载）：固化「改代码前先读 DEVELOPMENT.md」硬规则 + 第 5/6/8/9 节使用顺序 + 硬红线清单 + 文档纪律，避免每次重复口头交代 | 新增 AGENTS.md（根目录） |
| 2026-09-12 | 多数据源互转 P0-1：修复 redis_lookup 仅输出 key、未查询 Redis 的问题，生成 SQL 现真实调用四参 RedisLookupUdf（host/port/password/keyPrefix+keyField），端口范围与必填字段服务端校验、SQL 字符串转义；UDF 可在 endpoint 变化时安全重建连接池；Docker Compose 增 Redis 7.4、持久卷、健康检查及幂等演示 key 初始化；DAG 翻译 11 测试、UDF 构建与 Compose 配置校验通过 | DagTranslationService / RedisLookupUdf / DagTranslationServiceTest / docker-compose.yml |
| 2026-09-12 | 多数据源互转 P0-2：新增 PostgreSQL 双向转换（pg_output）。翻译层按上游 Schema 生成 JDBC Sink DDL，提交前经 PostgresqlTableCreator 按 FAIL_IF_MISSING / CREATE_IF_MISSING / VALIDATE_EXISTING 策略校验或建表；schema/table 白名单正则 + 双引号引用防注入，字段名支持中文；凭据走 POSTGRES_USERNAME/POSTGRES_PASSWORD 环境变量；新增 generateJdbcOutputDDL 与 batchSize 边界校验；日志中 password 统一脱敏为 ******；前后端控件注册表同步（29→30）；Docker 增 PostgreSQL 16 服务、初始化 SQL 与 healthcheck | DagTranslationService / PostgresqlTableCreator / DataInitializer / application.yml / docker-compose.yml / 前端 app.js |
| 2026-09-12 | 多数据源互转 P0-3：HDFS 验收闭环。hdfs_input/hdfs_output 增加 Schema 缺失与 delimiter 单字符校验，杜绝 null 拼入 DDL；Docker 增 NameNode/DataNode（含 dfs.client.use.datanode.hostname）、共享 core-site.xml/hdfs-site.xml 与 HADOOP_CONF_DIR 注入 JobManager/TaskManager/SQL Gateway；prepare-flink-jars 补 Hadoop uber JAR | DagTranslationService / docker-compose.yml / docker/hadoop-conf / prepare-flink-jars.bat |
| 2026-09-12 | 多数据源互转 P1-1：远程数据源预览与表头链。新增 POST /api/preview/node，仅接受 jobId+nodeId，由 JobService.findByIdForUser 做 owner 校验后从已保存 DAG 读取节点参数（不接受客户端伪造连接串）；支持 pg/oracle/mysql 输入输出与 hdfs_input，JDBC 主机与 HDFS authority 走配置白名单防 SSRF，标识符校验 + 只读限行查询 + 8s 超时，错误信息不回显密码；前端按节点类型分流并在未保存时提示先保存；FlinkJobStatusChecker.sourceHeaderFields 补齐 pg_input/oracle_input（JDBC 元数据）与 hdfs_input（fieldsConfig）表头 | PreviewController / PreviewService / FlinkJobStatusChecker / application.yml / 前端 app.js |
| 2026-09-12 | 多数据源互转 P1-2：新增 Oracle 双向转换（oracle_output）。OracleTableCreator 统一大写规范、双引号引用、NUMBER/VARCHAR2/BINARY_DOUBLE/CLOB 类型映射与三种建表策略；oracle_input 改为 schema.table 限定名并复用同一标识符校验，凭据走 ORACLE_USERNAME/ORACLE_PASSWORD；Docker 增 Oracle Free 23 profile（gvenzl/oracle-free，不阻塞默认轻量编排）与初始化 SQL；前后端控件同步（30→31）；后端 84 测试、前端 10 测试、Maven Reactor 5/5、Compose 默认与 oracle profile 校验全绿 | DagTranslationService / OracleTableCreator / DataInitializer / docker-compose.yml / data/oracle_setup.sql / 前端 app.js + docs |
| 2026-09-12 | 修复容器化部署下两处配置缺失（仅全容器运行时暴露，本机直跑不复现）：① **告警邮件不发信**——`docker-compose.yml` 的 backend 段漏传 SMTP_* 环境变量，容器内始终走 `[MAIL-FALLBACK] 未配置 SMTP 跳过发送`；补齐 SMTP_HOST/PORT/USER/PASSWORD/TO 透传后实测邮件成功发送至配置邮箱。② **Kafka 可视化连接失败**——`KafkaMonitorService` 默认 `localhost:29092`（宿主机地址），但后端在容器内需走 `kafka:9092`；新增 `app.kafka.bootstrap-servers` 配置项（`APP_KAFKA_BOOTSTRAP_SERVERS` 可覆盖），compose 注入 `kafka:9092` 并加 `depends_on: kafka(healthy)`；实测 `/api/kafka/topics` 返回 200 与完整 topic 列表 | docker-compose.yml / application.yml |① 新增 4 个故障演示作业覆盖告警中心不同事件——`故障演示-JOB_FAILED`（CSV 输入缺失，提交期失败）、`故障演示-THROUGHPUT_ZERO`（Kafka 空 topic 持续零吞吐）、`故障演示-心跳丢失与上线停止`（流式作业上线后异常重启 3 次超限自动下线）、`故障演示-错误路径`（保留）；实测触发 `JOB_FAILED`、`THROUGHPUT_ZERO`、`ONLINE_JOB_STOPPED`、`ALERT_RESOLVED` 四类事件。② **修复 HealthMonitor 三类指标告警完全失效**：原 `queryDoubleMetric` 查**作业级** `numRecordsOutPerSecond`，但 Flink SQL 融合链上该指标只在**算子(vertex)作用域**上报（形如 `0.numRecordsOutPerSecond`），作业级恒返回空 → `THROUGHPUT_ZERO`/`BACKPRESSURE_HIGH`/`CHECKPOINT_FAILED` 永不触发；已改为遍历全部 vertex 取最大值，并处理「指标值为 0/NaN 时 Flink 省略 value 字段」的边界（按 0 计，正是零吞吐目标场景）。③ **修复 mock 占位 ID 误认领历史作业**：`FlinkJobStatusChecker` 解析 `flink-job-*` 时按「时间最接近」匹配且**无上限**，实测把 976 秒前的旧作业认领给新提交的作业，导致新作业状态被旧作业的 `CANCELLED` 覆盖；已加 `MOCK_RESOLVE_MAX_DIFF_MS`(5 分钟) 窗口并补 `FlinkJobStatusCheckerTest` 回归。后端 87 测试全绿 | HealthMonitor / FlinkJobStatusChecker / FlinkJobStatusCheckerTest + 故障演示作业数据 |
① 清理——128 个作业中 E2E 调试产物每类保留 1 个（删 72 个）、删除 10 个零散调试作业与 1 个 SUBMITTED 残留，保留 36 个演示矩阵 + 7 个 E2E 能力证据 + 「故障演示-错误路径」「LLM诊断演示」；清理前全量备份至 `backend/_job_backup_20260912_120233.json`（已被 .gitignore 覆盖）② 新增 5 个多数据源演示作业（演示 PostgreSQL→CSV / CSV→PostgreSQL / PostgreSQL→HDFS / HDFS→CSV / CSV→Redis富化→CSV），命名与既有演示矩阵一致、含画布坐标，全部真实 Flink 提交 COMPLETED；最终 50 个作业（43 COMPLETED）③ 排障记录：Flink TaskManager 会缓存 NameNode 的旧容器 IP，HDFS 重建后必须同时重建 Flink 集群，否则报 `Connection refused namenode:9000` | backend/_job_backup_*.json + 演示作业数据 |
`app.ai.provider` 与 Compose 默认由 openai 改为 **deepseek**（DEEPSEEK_API_KEY/BASE_URL/MODEL 优先，OPENAI_* 降级为可选），模型白名单默认 `deepseek-v4-flash,deepseek-v4-flash-vision-exp`；修复「provider=openai 导致 apiKey 取空的 OPENAI_API_KEY，落盘密钥解密失败后无环境变量可回退 → configured=false」问题——补齐 .env 的 AI_PROVIDER 并归档失效的 ai_providers.json，使环境变量接管；实测 /api/ai/test 返回连接成功，/api/ai/diagnose/{jobId} 对失败作业返回准确根因与 3 条可操作建议 | application.yml / docker-compose.yml / .env.example / .env（不入库） |
| 2026-09-12 | 多数据源交叉 E2E 验收 **7/7 通过**（test-resources/multisource_e2e.py，真实 Flink 提交）：PostgreSQL→CSV(3行) / CSV→PostgreSQL(3000行+自动建表) / PostgreSQL→HDFS(part 文件) / HDFS→CSV(表头正确过滤) / CSV→Redis Lookup→CSV(取到真实 Redis 值「华东重点订单」) / CSV→字段过滤→PostgreSQL(3000行,列=sale_id/region/amount) / PostgreSQL→HDFS 链路。验收暴露并修复 5 个真实缺陷：① hdfs_input 把 CSV 表头当数据行读入——新增 hasHeader 参数，按 STRING 全列读取+SQL 层 `WHERE 首列 <> 字段名` 过滤表头，且 nodeSchemas 同步为 STRING 避免 sink 类型校验失败；② Flink 容器未挂载 test-resources，作业读不到输入数据——compose 为 JM/TM/SG/Backend 增加挂载（须可写，表头剥离要生成 .nohdr）；③ HDFS 容器权限——hdfs-site.xml 关闭 dfs.permissions.enabled（演示环境）；④ HDFS 集群重建后 DataNode 集群 ID 不匹配——清卷重建；⑤ 脚本 `.bat` 与 `docker exec` 路径/子命令格式修正（hdfs dfs 子命令需 `-` 前缀）。另将手动 HDFS 容器替换为 compose 管理的 namenode/datanode（apache/hadoop:3.3.6，含持久卷与健康检查） | test-resources/multisource_e2e.py + DagTranslationService + DataInitializer + docker-compose.yml + docker/hadoop-conf/ + 前端 app.js + docs |
| 2026-09-13 | **全项目对抗性缺陷审查与 P0/P1 修复**（审查 4 个维度：并发与资源、安全边界、DAG 翻译正确性、配置部署与文档）。**P0**：① 6 个 `@Scheduled` 任务共用 Spring 默认单线程调度器，MonitorService 每 2s 逐作业发 Flink HTTP 会把 HealthMonitor(10s)/状态轮询(5s)/心跳(10s)/调度(30s) 全部饿死 → 新增 `spring.task.scheduling.pool.size`(默认 4) 与 `spring.task.execution.pool`，并把告警邮件抽到独立有界线程池 `alertTaskExecutor`（`AlertMailDispatcher` @Async），避免 SMTP 不可达时每个告警阻塞扫描线程 10s；② 控件默认路径写死 Windows 盘符（容器内不存在），且输入分支不做容器映射（`json_input`/`xml_input`/`parquet_input`/`excel_input` 依赖 `File.isFile()` 直接报“文件不存在”）→ 新增 `app.paths.data-root`/`test-resources-root` 与 `app.storage.output-root` 在种子时解析为绝对路径，输入分支补 `resolveRuntimePath(path,"/data")`，MySQL/PG/Oracle 默认 JDBC URL 也改为可注入（`MYSQL_URL`/`POSTGRES_URL`/`ORACLE_URL`，容器内指向服务名）；③ `GET /api/jobs/{id}/preview` 直接按 DAG 里的 path 读文件，绕过 allowed-roots 白名单，可读回 `.env`（含 JWT_SECRET → 可伪造 ADMIN token）→ 改为复用 `PreviewService.assertAllowedRead`；④ `MonitorController` 的 overview/trends/removeTrend 无角色与 owner 校验，任意登录用户（含自助注册 VIEWER）可读全站运行数据、删他人趋势 → overview/trends 按当前用户过滤（ADMIN 全量），删除加 `@PreAuthorize` + owner 校验。**P1**：⑤ `renderTemplate` 对所有参数原样替换 → 统一 `escapeSqlLiteral`，并补齐 CSV/Kafka/datagen/excel/parquet/xml 分支的 path/topic/bootstrap/delimiter 转义；⑥ `extractFieldsFromDdlTemplate` 取第一个 `)`，`DECIMAL(10,2)` 被截成非法 schema → 改为括号配对；`replaceDataStringInDDL` 加 `Matcher.quoteReplacement`；⑦ JDBC `precision=0` 生成 `DECIMAL(1,0)` → 改 `DECIMAL(38,18)`；MySQL `STRING→VARCHAR(255)` 截断长文本/中文 → 改 `TEXT`、`BINARY→VARBINARY(4096)`；⑧ SQL Gateway 逐语句失败只 `log.warn + continue` 导致缺表 INSERT 仍被标“已提交” → 改为抛出并带语句序号；`flinkSql.split(";")` 在密码/路径含分号时截断语句 → 改为引号感知的 `splitSqlStatements`；⑨ `submitToFlink` 在集群在线但提交全失败时返回假 `flink-job-UUID`，前端显示“提交成功” → 提交路径改为返回 `SubmitResult` 三态（真实 Job ID / 集群已接受但未取到 ID / 明确失败）：明确失败时抛错让作业落 FAILED，已接受但未取到 ID 时仍保留占位 ID 交状态轮询认领（避免慢启动作业被误标 FAILED）；⑩ mock 占位 ID 认领：同一轮多个占位作业会认领同一个真实作业、且可能认领提交前启动的旧作业 → 候选消费后移除 + 只认领 start-time 不早于提交时刻(30s 容差)的作业；⑪ 作业删除后 `trendBuffer`/`lastLiveSnapshot` 与 HealthMonitor 六个去重 Map 不清理 → `JobService.delete` 联动 `MonitorService.removeTrend` + 新增 `HealthMonitor.forgetJob`；⑫ compose 缺 `MYSQL_USERNAME`/`MYSQL_PASSWORD`/`HADOOP_HOME`，预览白名单丢 `/test-resources` → 全部补齐，容器内 `HADOOP_HOME` 置空避免回落 Windows 默认值。**文档**：修正测试数(18 类/71 用例→22 类/87 用例)、Service 数(27→30)、视图数(10→9)、容器数(7→12，Oracle 另为 profile)、测试空白区描述。后端 **87 测试全绿** | DagTranslationService / JobService / MonitorService / MonitorController / HealthMonitor / FlinkJobStatusChecker / AlertService / AlertMailDispatcher(新) / AsyncExecutorConfig(新) / DataInitializer / PreviewService / MysqlTableCreator / application.yml / docker-compose.yml / .env.example / JobServiceTest / DEVELOPMENT.md / README.md |
