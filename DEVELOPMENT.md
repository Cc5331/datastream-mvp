# 通用流处理任务管理平台 — 开发维护指南（逻辑框架锁定版）

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
    ├── backend/                        # Spring Boot 后端（新增首期治理聚合 API，详见 controller/service）
    │   ├── plugin-sdk/                 # 控件插件 SPI（DataStreamPlugin，无第三方依赖）
    │   ├── plugin-example/             # 插件示例（redis-connector，走 META-INF/services）
    │   ├── src/main/java/com/datastream/mvp/
    │   │   ├── MvpBackendApplication.java
    │   │   ├── ai/                     # LlmClient(多服务商/加密落盘) / AgentService(NL2Pipeline + 诊断)
    │   │   ├── audit/                  # @Audit 注解 + AuditAspect(audit_log 落库)
    │   │   ├── config/                 # DataInitializer(控件种子 31 个) / SecurityConfig / WebConfig / GlobalExceptionHandler / HadoopHomeConfig
    │   │   ├── controller/             # 14 个：原 11 个 + Dashboard / DataSourceCatalog / ClusterHealth
    │   │   ├── dag/                    # DagDefinition(DAG模型) / DagExecutor / FlinkDagExecutor / LocalDagExecutor
    │   │   ├── model/                  # 13 个实体/枚举：原 11 个 + DataSourceConnection / DataSourceType
    │   │   ├── plugin/                 # 控件插件 SPI + PluginLoaderService（已启用，扫 backend/plugins）
    │   │   ├── repository/             # JPA Repository × 12（新增 DataSourceConnectionRepository）
    │   │   ├── security/               # JwtUtil / JwtAuthFilter / CurrentUser / SecurityUtils
    │   │   ├── service/                # 39 个，另增 DataSourceConnection/DataSourceConnectionTester/DataSourceSecretCipher/DataSourceNodeResolver
    │   │   └── util/                   # JdbcUrlUtil / ManagedFiles / MysqlIdentifier / LanguageDetector
    │   ├── src/main/resources/         # application.yml / application-mysql.yml / application-linux.yml + ai-prompts/(中英提示词模板)
    │   ├── src/test/java/              # 30 个测试类 / 112 个用例
    │   └── data/                       # H2 元数据库 mvpdb.mv.db + ai_providers.json + .ai_config_key（本机密钥，均已被 .gitignore 覆盖）
    ├── frontend/
    │   ├── serve.js                    # 静态服务 + /api 反代（无缓存头，路径穿越防护）
    │   ├── test/app.test.js            # 前端 28 个用例（含数据源 CRUD、六类型表单、权限与凭据语义、画布数据源引用、按钮图标一致性）
    │   └── public/
    │       ├── index.html              # 单页应用（14 个视图，含工作台/模板/数据源/运营/集群）
    │       ├── css/style.css
    │       ├── js/app.js               # 全部前端逻辑（2451 行，Vue3 + X6）
    │       └── vendor/                 # Vue/ElementPlus/X6/ECharts 本地化依赖
    ├── flink-1.18.1/                   # Flink 发行版副本（本机运行时用 D:\code\flink-1.18.1）
    ├── udf/                            # UDF jar 工程（XmlToJson / JsonToXml / RedisLookup）
    ├── scripts/                        # init_mysql.sql / test-mysql-flow.ps1 / gen_db_design_doc.py（生成数据库说明书）
    ├── docker/                         # Dockerfile（flink 镜像、prepare-flink-jars.bat 等）
    ├── docker-compose.yml              # 全容器化编排（MySQL:3307 / Kafka / Flink / 后端 / 前端）
    ├── data/                           # 本机业务库初始化脚本与样例数据（mysql_setup.sql / sales.csv）
    ├── ai_sidecar/                     # 早期 Python(FastAPI+Ollama) 试验侧车：**当前架构未使用**，保留参考
    ├── output/                         # 作业输出目录（CSV/Excel 等，gitignore）
    ├── test-resources/                 # 测试数据（sales.csv / sample_data.xlsx / large 大数据集）与基准脚本
    ├── test-results/                   # 性能基准报告产物
    ├── docs/                           # 作品演示录像脚本 + 数据库设计说明书（docx / ER 图 / H2 与 MySQL DDL）
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
    curl http://localhost:18080/api/health            # 健康探针（免鉴权、无副作用、真查一次库；DOWN 返 503）
    curl http://localhost:18080/api/jobs              # 作业列表（手工起后端时是 8080）
    curl http://localhost:18080/api/controls          # 控件注册表
    curl http://localhost:18081/jobs/overview         # Flink 作业
    curl http://localhost:18081/taskmanagers          # Flink TaskManager（若 404 说明集群未起）
    mysql -u root -p flink_demo -e "SHOW TABLES;"     # 密码见 .env / MYSQL_PASSWORD

### 4.4.1 健康探针与看门狗（2026-09-16 新增，防"端口在听但不响应"的僵死）
    # 单次探针：健康退出码 0，不健康 1（适合脚本/CI）
    powershell -ExecutionPolicy Bypass -File scripts\watchdog.ps1 -Once
    # 演示前预检：一次检查 前端/后端+DB/Flink/SQL Gateway/Kafka 五项，退出码=失败项数
    powershell -ExecutionPolicy Bypass -File scripts\watchdog.ps1 -Preflight     # 或双击 scripts\watchdog.bat -Preflight
    # 常驻看门狗：默认 30s 探一次，连续 3 次失败自动重启后端并写 logs\watchdog.log
    powershell -ExecutionPolicy Bypass -File scripts\watchdog.ps1
    # 单独启动后端（注入 .env、独立进程，看门狗内部也用它）
    powershell -ExecutionPolicy Bypass -File scripts\start-backend.ps1

### 4.5 构建与测试（改完必跑，见第 9 节）
    mvn -B -f pom.xml test          # 根聚合：全 5 模块；后端 115 个用例（2026-09-14 实测全绿）
    cd frontend; node --test test/app.test.js   # 前端 28 个用例（含数据源 CRUD、六类型表单与权限校验、按钮图标一致性）

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
- 输出后处理：统一走 `finalizeJobOutputs(job)`（内部持作业分片锁 `service/JobLocks`），
  作业转 COMPLETED/CANCELLED 时触发 Excel/Xml/Parquet 转换与 CSV/JSON part 合并；cancel/offline、Kafka 自动停止、3 分钟补合并也走同一入口。
- **「不在 overview」的判定（2026-09-15 修正）**：不再一律判成功。轮询发现真实 jid 的作业消失时，
  先比对**集群一代标识**（`/taskmanagers` 在册 TaskManager 的注册 ID 集合，`clusterGeneration`）：
  - 标识变化（JM 重启 / TM 重新注册）→ 作业是随集群丢的 → 置 **FAILED** + ERROR 日志 + 立即告警；
  - 标识未变或取不到 → 视为 Flink 正常结束后移出 overview → 置 COMPLETED（保持既有输出合并）。
  首次观测不算换代，避免误报；与 HeartbeatMonitor 的 JOB_HEARTBEAT_LOST 语义现已一致。
- **重复提交防护**：`JobService.submit` 持锁并校验状态，SUBMITTED/RUNNING 再提交返回 409；`cancel` 共用同一把锁。

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
17. **不要用 SQL 保留字做节点 id**：节点 id 经 `sanitize` 后直接作为 Flink 表名。裸用 `out` 这类保留字建表会被集群拒绝（只报 `Internal server error`，JobManager 日志里没有原因，极难定位）；画布自动生成的 `xxx_output_1` 形式安全。手写或脚本建作业时尤其注意。
18. **模板占位符必须能取到值**：节点 params 只保存用户显式填写的参数，带 `default` 的可选项不落库；模板引用这类参数依赖 `withSchemaDefaults` 按 paramSchema 补默认值，否则 `${xxx}` 会原样进入 Flink SQL。
19. **Flink Windows 发行版没有 `sql-client.sh`**：只有 `.bat`，需经 `cmd /c` 调用；降级路径已改为先探测存在性再执行，勿改回硬编码 `.sh`。
20. **index.html 里的图标标签必须写 kebab-case**（`<data-board />` 而非 `<DataBoard />`）。它是 DOM 模板，浏览器会把标签名小写化，Vue 的 capitalize 只能把 `databoard` 还原成 `Databoard`，与注册名 `DataBoard` 不匹配 → **图标静默不渲染、控制台无任何报错**，极难定位。同理避免 `<view />`、`<keyboard />` 这类与原生标签重名的图标（实测不渲染），改用 `<zoom-in />`、`<operation />`。已加回归用例拦截。
21. **提交作业只能走 `JobService.submit`**：它现在持有作业分片锁（`service/JobLocks`）并校验状态，SUBMITTED/RUNNING 会返回 409。不要再在调度器/依赖服务里自行判断状态后绕过它提交，也不要删掉该状态前置校验——那是「手动提交 + cron + 依赖触发」三路并发时防止重复 Flink 作业的唯一防线。
22. **输出后处理必须走 `FlinkJobStatusChecker.finalizeJobOutputs`**：合并/转换已收敛到这一个持锁入口（轮询完成、取消、Kafka 自动停止、补合并重试都会调用）。不要在别处直接调 `mergeXxxOutputsIfNeeded` / `convertXxxOutputsIfNeeded`，`mergeCsvParts` 是「先删后写」，并发调用会丢数据。
23. **权限判断统一走 `security/JobAccess`**：`assertCanAccess` 抛 401/403（ownerId 为空仅 ADMIN 可见），`canAccess` 用于列表过滤。端点角色限制用**类级** `@PreAuthorize`（如 `/api/kafka/**`、`/api/preview/**` 为 ADMIN/OPERATOR），前端入口要同步用 `canEdit` 隐藏，否则 VIEWER 点进去只会拿到 403。
24. **JWT 与用户状态变更要同步**：`AppUser.tokenVersion` 参与签发（claim `tv`），`JwtAuthFilter` 每请求比对；新增「改密码 / 重置密码 / 登出 / 强制下线」逻辑时必须调用 `UserService.revokeTokens`（或 `bumpTokenVersion`），否则旧 token 在有效期内仍然可用。
25. **`/api/health` 必须保持免鉴权且无副作用**：它是看门狗与容器 healthcheck 的唯一探针，写审计/日志会污染数据，加鉴权会让探针失效。判定健康要同时覆盖「HTTP 链路 + 数据库」，只探端口抓不住僵死。
26. **`.ps1` 脚本必须存成 UTF-8 with BOM**：Windows PowerShell 5.1 会把无 BOM 的脚本按 GBK 解码，中文注释/字符串变乱码并直接导致 `ParserError`（本仓库 `scripts/*.ps1` 均为 BOM；用编辑器保存时选「UTF-8 with BOM」）。`.bat` 脚本反之——写纯 ASCII，避免 cmd 代码页乱码。
27. **打包前先停后端**：运行中的后端会锁住 `backend/target/mvp-backend-1.0.0.jar`，`mvn package` 的 repackage 阶段会失败并把 jar 变成 0.5MB 的瘦包（此时线上进程仍在跑旧代码，极易误判「改了没生效」）。正确顺序：停后端 → `mvn -DskipTests package` → `scripts\start-backend.ps1` 拉起。
28. **控件注册表：内置按 type upsert，插件行永不删**（2026-09-16 修）：`DataInitializer.seedBuiltInControls()` 只**原地更新**同名内置控件（保留管理员设置的 `enabled` 与 `createdAt`），并只清理「jarPath=built-in 且已从代码移除」的陈旧行；插件行（jarPath=jar 名）一律保留，jar 被移出目录时由 `PluginLoaderService.removeOrphanPluginControls` 回收。**不要再改回 `deleteAll()` 重建**——插件加载器是 `@PostConstruct`（早于 `CommandLineRunner`），清库会把插件控件每次启动都抹掉。另外：原「控件管理」页（纯只读、与画布控件库和帮助中心重复）已于同日下线，控件展示统一走**画布左侧控件库**与**帮助中心「控件说明」**；插件 `type` 必须唯一（与内置同名时保留内置并在日志告警），插件模板必须是可执行 SQL。
29. **插件 SPI 有两个接口，改动加载器时两个都要认**：`com.datastream.plugin.DataStreamPlugin`（plugin-sdk，对外推荐）与 `com.datastream.mvp.plugin.ControlPlugin`（内置）。历史上加载器只认后者，导致按文档实现的插件（含 `backend/plugin-example`）被静默跳过、从来加载不了——`isPluginClass` 必须同时判断两者，`PluginLoaderServiceTest` 已加回归。
30. **容器/document/window 级事件监听必须幂等绑定**：`setupDropHandler()`（5 个调用点）、`initGraph()`（含 onMounted 的无条件调用）这类可重入函数，每次执行都 `addEventListener` 而不移除的话，一次用户操作会触发 N 次回调。历史现象：**拖一个控件落下两个**（两个 drop 监听各 addNode 一次，节点 id 相差几毫秒）；同理曾出现 resize 监听叠加。写法固定为「保存句柄 → 重绑前 removeEventListener → 再 add」，画布重入则用 `if (graph) return;` 兜住。改动画布交互后跑 `node scripts/verify-canvas.cjs` 真浏览器验证（静态测试只能守住代码形状）。
31. **参数面板是「待应用」状态，节点上的 `data.params` 才是事实**：面板初值来自 `paramSchema` 默认值（或节点已有值），改动只有经 `applyParams()` 才写回节点。因此三条链路必须补齐——① 拖拽/新增节点时用 `defaultParamsFromSchema()` 初始化 `params`（否则面板看着有默认路径、节点里是空的 → 提交前检查报「缺少必填参数: path」）；② 切换选中节点前先 `applyParams()`（否则上一个节点的改动被静默丢弃）；③ **提交前先 `applyParams()` 再 `saveDag({silent:true})`** 再跑 preflight——因为 preflight 校验的是**已保存的 DAG**，不先落盘就会出现「画布上明明填了、检查却说缺参数」的错位。
32. **用户管理的不变量（改 `/api/users` 或「用户管理」页时必须守住）**：① 端点保持**类级** `@PreAuthorize("hasRole('ADMIN')")`（前端入口与视图同样只对 ADMIN 展示，但真正的边界在后端）；② **不能删除当前登录账号**（`delete(id, currentUserId)`，前端对本人删除按钮同时 `:disabled`）；③ **必须保留至少一个启用管理员**（`protectLastAdmin`，删除 / 降级 / 禁用三条路径都要过）；④ 重置密码或禁用账号后必须让旧登录态失效（`bumpTokenVersion`，与红线 24 一致）；⑤ 四类写操作全部写审计（USER_CREATE / USER_UPDATE / USER_PASSWORD_RESET / USER_DELETE）。改完跑 `node scripts/verify-users-crud.cjs`（真浏览器 7 项断言）+ `frontend/test/users-admin.test.js`。

---

## 9. 回归验证清单（每次改完必跑）

### 9.1 服务健康
- 前端 3000 返回 200、后端 18080（或手工启动的 8080）返回 200、Flink 18081 返回 200（如已启动）。
- **后端用 `/api/health` 判定**：返回 `{"status":"UP","db":"UP",...}` 且耗时 < 1s；只探端口不够——
  2026-09-16 出现过「端口在监听、连接能建立，但请求永不响应」的僵死，浏览器登录会一直转圈。
  一键预检：`powershell -File scripts\watchdog.ps1 -Preflight`（或双击 `scripts\watchdog.bat -Preflight`）。
- 后端 `/api/controls` 返回与 `DataInitializer` 中 `createControl` 数量一致的控件（当前 **31** 个；重启后 DataInitializer 会清空重建注册表）。

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

### 9.4 自动化测试（改完必跑，2026-09-16 实测全绿）
- [ ] `mvn -B -f pom.xml test` → 5 模块 BUILD SUCCESS，后端 **166** 个用例 0 失败。
- [ ] `cd frontend; node --test test/*.test.js` → **56** 个用例 0 失败（控件注册表一致性、按钮图标、数据源 CRUD、账户资料与在线状态、参数类型转换、模板与 AI 示例、端点权限入口隐藏、资源生命周期、拖拽落点幂等、参数持久化、用户管理界面接线）。
- [ ] 改了用户/权限相关代码时跑一次真浏览器验证：`node scripts/verify-users-crud.cjs`
      → 7 项断言全 PASS（管理菜单入口、列表、新建、编辑、重置密码、删除、自删保护、审计四类记录；脚本会自行删除测试账号）。
- [ ] 改了画布交互（拖拽/连线/参数面板/加载 DAG）时跑一次真浏览器验证：`node scripts/verify-canvas.cjs`
      → 6 项断言全 PASS：一次拖拽=一个控件、原生拖拽可连线、拖入的输入节点自带默认 path、输出节点带默认 path、
      **提交前检查无错误**、面板改参数直接保存即落库（脚本自行删除验证用作业，不污染演示数据）。
      需要 3000/18080 已启动 + Edge；`ws` 模块不可解析时用 `DSH_WS_NODE_MODULES` 指定。
- [ ] 改了安全/并发相关代码时的针对性回归：`FlinkJobStatusCheckerTest`（集群换代判定）、`JobLocksTest`（分片锁互斥）、`JobAccessTest`（归属规则）、`JwtUtilTest`（令牌版本）、`JobServiceTest`（重复提交被拒）、`PluginLoaderServiceTest`（两种 SPI 接口）、`DataInitializerControlsTest`（内置 upsert 不清插件行）。
- [ ] 改了安全配置时的手工验证：未登录访问 `/api/jobs` 返回 401；`/h2-console` 未登录不可访问（默认还是关闭状态）。

---

## 10. 后续完善路线（2026-09-15 修复后重排）

> 2026-09-11 列出的 8 条路线先与并行开发提交逐条核对（判定标 ✅/🟡/⬜），
> 随后把其中「仍开放」的 6 项在 2026-09-15 全部修完（提交见文末变更记录：`51538c4` 状态语义、
> `c9def90` token 吊销、`863d19c` owner 收紧、`73a4e6d` 端点权限、`46a3c7d` 并发安全、`11d42e9` 前端生命周期）。

1. **安全收尾** ✅
   - ✅ Monitor 端点已按 owner 过滤（`MonitorController` 调 `SecurityUtils.currentUser()`），删除趋势需 ADMIN/OPERATOR
   - ✅ `/api/preview/file` 的 allowed-roots 白名单已抽出 `PreviewService.assertAllowedRead`，作业输出预览复用同一份校验（修掉「DAG 里填任意路径读回 .env」）
   - ✅ 用户停用/角色变更**下一请求立即生效**（`JwtAuthFilter` 每请求按 userId 回查数据库并校验 enabled）
   - ✅ **token 可吊销**：`AppUser.tokenVersion` + JWT claim `tv`，改密/管理员重置/登出即失效（`UserService.revokeTokens`）
   - ✅ `KafkaMonitorController`、`PreviewController` 已加类级 ADMIN/OPERATOR 限制（前端入口同步对 VIEWER 隐藏）
   - ✅ `ownerId` 为空的作业收紧为**仅 ADMIN 可见**，归属校验收敛到 `security/JobAccess`（原四处实现规则不一致）
2. **Flink 状态语义修正** ✅（mock 超时兜底除外）
   - ✅ mock 认领已加时间上限（`MOCK_RESOLVE_MAX_DIFF_MS`，超出则拒绝认领并从候选移除）
   - ✅ 消失判定不再一律成功：新增**集群一代标识**（在册 TaskManager 注册 ID 集合），
     换代（JM 重启/TM 重新注册）导致的消失判 **FAILED** 并即时告警，未换代才判 COMPLETED
   - ⬜ 仍未做：mock 作业的超时兜底状态（集群不通时一直停在 SUBMITTED，属可观测性问题）
3. **并发安全** ✅
   - ✅ `JobService.submit` 全程持作业分片锁（`service/JobLocks`，64 分片）并新增状态前置校验：
     SUBMITTED/RUNNING 再提交直接 409，手动/定时/依赖三路并发不再拉起重复 Flink 作业
   - ✅ `cancel` 与 `submit` 共用同一把锁；5 处重复的输出后处理收敛为 `finalizeJobOutputs(job)`（持锁执行），
     轮询判定完成、取消、Kafka 自动停止、补合并不再并发操作同一批 part
   - ⬜ 残留：`cleanOutputTempDirs` 在提交时清同名 `.tmp` / `_temp_csv`（现在同一作业同时只允许一次提交，
     风险已大幅降低，但「上线作业被监督重启」与「多作业共用同一输出路径」仍建议后续加路径级互斥）
4. **调度线程池隔离** ✅ 已修：`spring.task.scheduling.pool.size=${APP_SCHEDULING_POOL_SIZE:4}`，2s 采集不再独占唯一调度线程；告警邮件另有独立有界线程池（`AsyncExecutorConfig.alertTaskExecutor`，CallerRunsPolicy）
5. **补测试空白** 🟡 测试持续增长（2026-09-15 复核：**33 类 / 136 用例**，本轮再新增 `JobLocksTest`、`JobAccessTest`、`JwtUtilTest` 与多组用例）
   - ⬜ 仍无：`PluginLoaderService`、Parquet / HDFS / Excel 输入输出链路、`JobScheduler`、多用户隔离的端到端用例
6. **前端工程化** 🟡 资源泄漏已修（resize 监听句柄化、`disposeAllCharts()` 统一释放、登出停流停轮询）；
   ⬜ 巨石仍在：`app.js` 3381+ 行、`index.html` 1400+ 行，死 CSS 未清理，建议后续按视图拆分或引入构建步骤
7. **AI 侧** 🟡：诊断已统一切 DeepSeek（`e694dae`），提示词模板与示例扩充到 16/10（`1f190d6`）；⬜ 模型白名单外的多服务商编排、诊断规则库版本化仍未做
8. **Docker 收尾** 🟡：compose 已扩到多数据库（PG/Oracle 相关服务、初始化 SQL、healthcheck），修掉容器内 SMTP 与 Kafka bootstrap 缺失；⬜ 仍缺 compose 环境下的一键验收剧本

> 下一批优先级建议：**Flink mock 作业超时兜底（可观测性）→ 多用户/调度端到端用例 → app.js 拆分 → compose 验收剧本**。

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
| 2026-09-14 | **用户端与管理端首期平台化增强**：为避免立即引入多租户、JobRun 和凭据迁移，采用零数据库迁移方案。新增个人工作台、内置模板中心、连接器与已用资产目录（按 owner 聚合且敏感参数不返回）、作业生命周期/日志/调度/告警时间线、Flink 集群健康、ADMIN 运营总览、审计多条件筛选和无副作用 DAG preflight；前端导航按开发/运维/管理分组，提交前展示结构、连通性、控件、必填参数和并行度检查结果，集群页 15s 自动刷新并在离页时释放定时器。模板实例化重建节点/边 ID 并清空作业上下文，运行时间线明确为现有数据聚合而非伪造运行实例。后端 **91** 测试、前端 **14** 测试全绿 | Dashboard / DataSourceCatalog / JobTimeline / ClusterHealth / DagPreflight / Audit 高级筛选 / JobController / 前端 index.html + app.js + style.css + tests / README / DEVELOPMENT |
| 2026-09-14 | 修复首期服务无法启动：`ClusterHealthService` 同时定义了 `@Value` 注入的主构造器和包级测试构造器，但未标 `@Autowired`，Spring 在多构造器场景下回退查找无参构造器并抛 `No default constructor found`，导致应用整体启动失败（单测直接 `new` 实例化，因此 91 个用例仍全绿、掩盖了该缺陷）。已在主构造器补 `@Autowired`；并确认其余新增 Service 均为 Lombok 单构造器、不受影响。**回归验证**：重新打包后应用启动成功，新增 6 个接口全部 200，真实 Flink CSV→CSV 作业提交 COMPLETED（输出 3001 行含中文表头），VIEWER 访问 admin-overview/audit 返回 403、访问 workbench/catalog 返回 200，资产目录凭据字段命中 0，审计 from>to 返回 400 | ClusterHealthService / 验证记录 |
| 2026-09-14 | 修复 Docker 迁移后 Flink 集群无法启动：JobManager 报 `BindException: Could not start actor system on any port in port range 6123`，根因是 Docker Desktop/WSL 在 Windows 上把 **6122-6221 登记为保留端口区间**（`netsh int ipv4 show excludedportrange protocol=tcp`），`jobmanager.rpc.port: 6123` 被系统占用；该端口在配置文件中还有重复键（第 2 行与 overrides 段第 42 行），本次一并统一为 **16123** 并加注释说明原因。修复后 JobManager（18081）与 RPC（16123）正常监听，TaskManager 1 个 / 4 Slots 注册，`/api/cluster/health` 返回 UP | flink-conf.yaml（本机与项目内副本） / 验证记录 |
| 2026-09-14 | **首期页面完整性与按钮视觉完善**：针对新增页面“有数据但展示不完整、失败后仅 Toast、旧数据残留、按钮缺少视觉锚点”等问题，补齐工作台总作业/上线数与最近作业信息、模板空态/统计/覆盖确认、数据源真实 enabled 与资产字段/关联作业、管理端角色分布/状态分布/最近告警、集群 DOWN 错误卡/服务端检查时间/Slot 使用率、时间线“最近提交以来”语义与请求序列保护、preflight 服务错误态和 warnings（高并行度/节点缺 label）；所有 **105 个 Element Plus 按钮**统一加入语义图标并移除不稳定的字符图标。新增页面均补 loading/空态/错误态/重试，ADMIN 403 自动回工作台。后端 **92** 测试、前端 **18** 测试全绿 | DataSourceCatalogService / DagPreflightService + tests / 前端 index.html + app.js + app.test.js / README / DEVELOPMENT |
| 2026-09-14 | **持久化数据源管理闭环**：在不修改历史 DAG 契约和翻译链的前提下新增 `DataSourceConnection` 独立资源，支持 MySQL/PostgreSQL/Oracle/Kafka/Redis/HDFS 六类连接的 owner 隔离 CRUD、启停、未保存/已保存配置测试与乐观锁；凭据以 AES-GCM 随机 IV 加密，密钥优先取环境变量，未配置时持久化到本机密钥文件，API 仅返回 `credentialConfigured`，密码留空保留旧凭据、显式开关才清除。连接配置采用字段白名单并限制目标主机，JDBC/Kafka/Redis/HDFS 测试均设置短超时、失败只返回脱敏结果；删除前防御性检查 DAG 中的 `dataSourceId` 引用。前端数据源页扩展为「已保存连接 / 连接器目录 / DAG 内联资产」三部分，提供六类型动态表单、搜索筛选、测试/编辑/删除和 VIEWER 只读体验。旧 DAG 继续使用内联连接参数，下一阶段再接 `dataSourceId`。后端 **102** 测试、前端 **23** 测试全绿 | DataSourceConnection/Type/Repository/DTO/SecretCipher/ConnectionTester/ConnectionService/Controller / application.yml / .env.example / 前端 index.html + app.js + style.css + tests / README / DEVELOPMENT |
| 2026-09-14 | 数据源管理运行时验收（真实后端 + H2，非单测）：创建 200 且响应字段为 `config/credentialConfigured/description/enabled/id/name/ownerId/ownerName/type/updatedAt/version`，**不含明文密码、不含 `encryptedCredentials`/`credentials` 字段**；直接读 H2 确认 `ENCRYPTED_CREDENTIALS` 为 Base64 密文（明文不入库）；密码留空更新后 `credentialConfigured` 仍为 true（保留凭据生效）；同 owner 重名 409；VIEWER 读他人数据源与创建均为 403；OPERATOR 列表看不到 admin 的数据源；连接测试失败返回 `{success:false, message:"连接失败，请检查地址、凭据和服务状态"}` 无堆栈无密码；非法配置 400；DAG 引用该数据源时删除 409（提示「数据源已被作业引用，无法删除」），解除引用后删除 200。验收产生的临时数据已清理 | 验证记录 |
| 2026-09-14 | **DAG 接入 dataSourceId + 提交层两处缺陷修复**：① 新增 `DataSourceNodeResolver`，节点参数含 `dataSourceId` 时用持久化数据源的配置与解密凭据覆盖连接级参数（url/username/password/bootstrapServers/host/port），提交与预览链路按作业 owner 校验使用权，无引用的旧 DAG 完全不变；前端参数面板新增「已保存数据源」下拉（按节点类型过滤、仅启用项），`dataSourceId` 保持数值类型不被 trim。② 修复 `create table` 模板占位符未替换：节点 params 只存用户显式填写的值，带默认值的可选项（如 `csv_output` 的 `delimiter`）不落库，渲染时 `${delimiter}` 原样进入 Flink SQL 导致建表被拒；新增 `withSchemaDefaults` 按 paramSchema 补默认值，输入/输出两处渲染点均接入。③ 修复 Gateway 语句失败被吞：原轮询只识别 COMPLETED/FINISHED/RUNNING，`ERROR` 状态会一直空转到耗尽，最终报出误导性的「未接受任何 INSERT 语句」；改为 ERROR/FAILED/CANCELED 立即抛错并带出真实错误，INSERT 进入 RUNNING 即视为已接受，轮询超时按待认领处理。④ 修复 Windows 降级路径必然失败：Flink Windows 发行版只有 `sql-client.bat` 没有 `.sh`，原代码硬拼 `.sh` 并空等 30s；改为先探测脚本存在性，`.bat` 经 `cmd /c` 调用，找不到时明确跳过。**实测**：MySQL 数据源连接测试 13ms 通过；引用数据源的作业 `MySQL student(15万行) → CSV` 真实提交 Flink 并 **COMPLETED**，输出 150000 行中文正常，日志无明文密码；后端 **112** 测试、前端 **24** 测试全绿 | DataSourceNodeResolver(新) / DagTranslationService / JobService / DagPreflightService / DataSourceCatalogService + tests / 前端 app.js + index.html / DEVELOPMENT |
| 2026-09-14 | **按钮视觉统一（图标+文字不空着）**：① 修复图标与文字排版——原模板中大量 `</el-icon>文字` 紧贴无空格，导致按钮内图标与文案挤在一起；统一补空格并新增 `.el-button > .el-icon + span/small { margin-left:4px }` 兜住 `<span>` 包裹文案的情况。② 快捷入口布局修正——`.quick-grid button` 样式按原生按钮设计，改用 el-button 后 padding 被覆盖导致图标偏左，改为 `inline-flex + center + gap:6px` 居中。③ 下拉菜单统一加图标：作业「更多」菜单的复制/调度/依赖/诊断/日志/历史、导航「开发资源/运行运维/管理/帮助」与「模板中心/控件管理/AI助手/实时监控/集群健康/Kafka可视化/工作流/告警中心/操作审计」全部换为 Element Plus 语义图标。④ 移除按钮内的 emoji（`✨ 生成作业 DAG`），避免与 el-icon 重复装饰。⑤ **修正测试假阳性**：原「所有按钮都有图标」用例用 `<el-button[\s\S]*?</el-button>` 跨按钮贪婪匹配，会把「无图标按钮」吞进上一个匹配块而误判通过（实测漏检 10 个多行按钮）；改为按 `indexOf` 逐块配对提取，并新增「按钮不混用 emoji」用例。前端 **25** 测试全绿 | 前端 index.html + style.css + test/app.test.js / README / DEVELOPMENT |
| 2026-09-14 | 按钮视觉收尾（第二轮的补漏）：① 补齐最后两个漏改的下拉项图标——`数据源中心`（Coin）、`运营总览`（DataAnalysis），此前脚本因 Vue 属性含 `>` 而漏检；② 快捷入口按钮尺寸统一——原 `.quick-grid > .el-button` 只在 grid 里自适应，`数据源与资产` 等较长文案会把按钮撑高，改为 `grid-auto-rows:1fr` + `height:100%` + `min-height:56px` 保证四宫格等高，并去掉首个按钮的 `type="primary" plain` 避免与其余三个色差；③ 顶部导航补 `text-decoration:none` 消除下划线；④ 新增「下拉菜单项统一带图标」回归用例（按行解析，跳过动态渲染项）。前端 **26** 测试全绿 | 前端 index.html + style.css + test/app.test.js / README / DEVELOPMENT |
| 2026-09-14 | **修复图标静默不渲染（本轮最隐蔽的缺陷）**：页面上大量按钮/菜单只有文字没有图标，但控制台无报错。根因是 `index.html` 作为 **DOM 模板**，浏览器会把标签名小写化——`<DataBoard />` 变成 `<databoard>`，Vue 组件名解析只能还原到 `Databoard`，与注册名 `DataBoard` 不匹配，于是被当作未知元素原样输出。实测 8 个导航按钮仅 1 个渲染出 svg。修复：把模板中 **147 处** PascalCase 图标统一改为 kebab-case（`<data-board />`），并在 app.js 注册时同时注册 PascalCase 与 kebab 别名、把 `app.use(ElementPlus)` 提到注册之前。另有两类图标名与原生标签冲突（`<view />`、`<keyboard />`）实测同样不渲染，改用 `<zoom-in />`、`<operation />`。**实测验证**：修复后导航 8/8、全页 849 个图标仅剩 1 个来自 Element Plus 内部组件，均正常渲染。同时补 `fmtDateTime` 统一列表时间格式（`2026-09-14T18:15:23.230719` → `2026-09-14 18:15`）、放宽「最近作业」操作列宽度避免按钮换行。新增两条回归用例（kebab-case 与原生标签冲突检查）。前端 **27** 测试全绿 | 前端 index.html + js/app.js + test/app.test.js / DEVELOPMENT |
| 2026-09-14 | **修复 AI 诊断「LLM 返回为空」**：推理模型（服务端实际返回 `deepseek-flash`）会先输出 `reasoning_content`，`content` 才是应答。原实现只读 `content`，一旦为空就抛「LLM 返回为空」并附上原始响应，无法判断原因。实测该诊断场景 **reasoning_tokens 可占满全部预算**（reasoning=8000 / completion=8000，`finish_reason=length`），推理未收敛导致 content 始终为空。修复：① 抽出 `parseChatCompletion` 统一两个 chat/completions 分支的解析，content 为空时带出 `finish_reason` / `completion_tokens` / `reasoning_tokens` 并给出可操作提示（调大 `app.ai.max-tokens`）；② `max_tokens` 由硬编码 4000 改为可配置（`app.ai.max-tokens` / `AI_MAX_TOKENS`，默认 16000）；③ 诊断 prompt 增加「控制在 3 句以内，不要穷举所有可能性」，抑制推理模型的发散思考。**实测**：修复前 39s 后 502，修复后 **5.3s 返回 200**，rootCause 准确指出「SQL Gateway 只收到 DDL 没有 INSERT」并给出 3 条可操作建议。后端 **115** 测试全绿 | LlmClient / AgentService / application.yml / .env.example / LlmClientParseJsonTest / DEVELOPMENT |
| 2026-09-14 | 帮助按钮补齐下拉箭头：`<el-icon style="..."><ArrowDown /></el-icon>` 是全项目最后一处 PascalCase 图标——因 `<el-icon>` 带 `style` 属性，逃过了之前「`<el-icon>` 紧跟标签名」的正则，故未转 kebab-case 而不渲染。修正为 `<arrow-down />`，并给四个下拉按钮统一加 `.dropdown-arrow` 类（间距 2px、12px 字号、透明度 .85），视觉一致。**同时修掉测试自身的同类盲点**：`/<el-icon>/` 字面量匹配不到 `<el-icon class="...">`，会把已带图标的按钮误判为缺失；改为 `/<el-icon\b/`。前端 **27** 测试全绿，浏览器实测帮助按钮 2 图标 2 svg | 前端 index.html + style.css + test/app.test.js / DEVELOPMENT |
| 2026-09-14 | **修复画布编辑不可用（高度塌陷为 0）**：用户反馈「画布编辑功能异常」。用 CDP 连真实浏览器排查，发现 `#dag-canvas` 的 `clientHeight` 为 0、内联样式被写成 `width: 0px; height: 0px;`，网格不可见、无法拖拽连线。根因：登录后默认进入工作台，画布所在视图处于 `v-show` 隐藏状态（尺寸 0），`initGraph` 仍以 `container.clientWidth/clientHeight` 初始化 X6，X6 把 0 写进内联样式**永久覆盖 flex 布局**；此前画布是首屏视图，所以从未暴露。修复：① `initGraph` 在容器宽或高为 0 时直接返回，不在隐藏状态初始化；② `watch(currentView)` 新增 canvas 分支，切到画布时按真实尺寸初始化或 `graph.resize`。**实测**：画布高度 665px 稳定，示例作业载入渲染 2 节点，无控制台异常。新增回归用例。前端 **28** 测试全绿 | 前端 app.js + test/app.test.js / DEVELOPMENT |
| 2026-09-14 | **修复「之前能跑的作业突然无法提交：节点 in_csv 参数类型错误: hasHeader」**：新增的 preflight 按控件 `paramSchema` 严格校验 `params` 类型，但 CSV/Excel/HDFS/Kafka/Datagen 等控件的参数**最初是按字符串实现**的（后端 `DagTranslationService` 全程 `getOrDefault(...).toString()`、模板 `${delimiter}` 直接替换），schema 是后来才补声明的 `boolean`/`number`，于是历史 DAG 与前端内置示例里存的 `"hasHeader": "true"` 被判成类型错误。全量排查出 3 类不一致：① 旧 DAG/示例的 `hasHeader`、`rowsPerSecond` 为字符串；② 前端 `el-switch` 用 `v-model` 直绑字符串，`el-input-number` 绑定隐藏的 `stopAfterSeconds` 时被 Element Plus 写入 `undefined`，预检会读到 `"undefined"` 报错；③ `row_filter` 模板 `${condition}` 因该参数无 schema 默认值、用户未显式填写时**原样拼进 Flink SQL**（`WHERE ${condition}` 语法错误）。修复分四层：**预检容错**——`DagPreflightService` 对 `boolean` 接受 `"true"/"false"`、对 `number/integer` 接受可解析数字串，空串按「未提供」处理，非法值仍报错；**前端按类型保存**——`applyParams` 依 schema 把开关统一写布尔、数值写数字、空值不落库，新增 `paramProxy`/`setParam` 代理控件读写以兼容历史字符串值，节点选中时用 schema 默认值兜底未保存的可选参数；**示例与模板对齐**——`SAMPLE_DAGS` 的 `hasHeader`/`rowsPerSecond` 改为布尔与数字（模板中心直接复用，故一并修正），`row_filter` 模板改用新增的 `${params.condition}` 占位符（缺参替换为空串而非原样输出）。**测试**：后端新增 2 个用例（字符串形式放行、非法值与空串边界）；前端新增 2 个用例（类型转换与服务端回显、示例参数与 schema 一致）。后端 **117** 测试、前端 **30** 测试全绿 | DagPreflightService + test / DataInitializer(row_filter 模板) / DagTranslationService(renderTemplate) / 前端 app.js + index.html + test/app.test.js / DEVELOPMENT |
| 2026-09-15 | **修复落库作业 `Flink SQL 第 4 条语句执行失败` 并显式化 MySQL 建表策略**：作业 #2386（CSV→字段拼接→MySQL）提交即 FAILED，AI 诊断为「password 为空/表不存在」。实际排查（Gateway 会话逐条复现）推翻了该结论：`MysqlTableCreator` 日志显示表**已成功创建**（`dataflow.ai_demo`，9 列），凭据也正常（`resolveCredential` 从 `MYSQL_PASSWORD` 注入，非节点内联）。真因是 `field_concat` 生成了**非法 SQL**——`fields` 参数为 JSON 数组，`renderTemplate` 直接 `toString()` 得到 `CONCAT([product_category, channel])`，方括号触发 `SqlParseException: Encountered "["`。修复：① 新增 `buildFieldConcatSelect`，把 fields 解析为字段列表并逐个 `quoteFlinkField` + `CAST(... AS STRING)`，支持 JSON 数组与逗号分隔两种历史写法，新增 `separator` 生效；② `field_concat` schema 的 fields 由 `array` 改为 `string`（逗号分隔），从源头避免数组被拼进 SQL；③ `MysqlTableCreator` 补 `createTablePolicy`（CREATE_IF_MISSING / FAIL_IF_MISSING / VALIDATE_EXISTING，**未设置时默认自动建表**，且改为先查 `information_schema` 再建、不再无脑 `IF NOT EXISTS`），与 PG/Oracle 对齐；④ 拆出 `mysqlOutputParamSchema`，让 mysql_output 暴露建表策略（mysql_input 不受影响）；⑤ AI 提示词补充「mysql_output 必须带 CREATE_IF_MISSING」「field_concat.fields 用逗号分隔字符串」；⑥ 预检对 `string` 类型容忍数组值（旧 DAG 兼容）。**实测**：删表后重新提交作业 #2386 → **COMPLETED**，自动建表 9 列、写入 3000 行、拼接列 `category_channel` 值正确（`图书,跨境`）。后端 **135** 测试全绿 | DagTranslationService / DataInitializer / MysqlTableCreator / DagPreflightService / ai-prompts(nl2pipeline.zh\|en) / DagTranslationServiceTest / DagPreflightServiceTest / MysqlTableCreatorTest(新) / DataInitializerSchemaTest(新) / DEVELOPMENT |
| 2026-09-15 | **丰富模板中心与 AI 示例，并修复链式 transform 与模板路径不可用**：模板中心由 3 个扩到 **16 个**（实时采集 / 格式转换 / 字段处理 / 数据清洗 / 数据落库五类，分类下拉改为从模板动态生成），AI 试试模块由 3 条扩到 **10 条**，覆盖拼接、过滤、去重、空值校验、Excel/XML/JSON/Parquet 与 MySQL 落库。**修复三类缺陷**：① **模板路径本机不可用**——模板沿用容器语义的 `/data`、`/test-resources`、`/output` 占位，而 `resolveRuntimePath` 在 Windows 直接返回原值，套用后提交必报「CSV 输入文件不存在」（用探针作业实测复现）；新增 `resolveTemplatePath`，从控件默认路径反推项目根目录再拼占位符，与 compose 的挂载语义一致。② **链式 transform 从未被支持**——边循环对 `transform→transform` 直接 `continue`，导致中间节点（如 row_filter 后的 field_concat）从未生成表，下游引用了不存在的表名；抽出 `buildTransformSql` 供两处复用，链中间节点物化为 `CREATE TEMPORARY VIEW` 并按节点去重，末尾节点仍内联进 INSERT，保持单 transform 作业 SQL 形态不变。③ **MySQL 模板缺 url**——节点 params 不回退 schema 默认值，模板必须显式写入 JDBC URL 与 `createTablePolicy`。**验证**：16 个模板全部真实提交并跑完（串行等待空闲槽位，`csv2pipeline` 的 3000 行过滤后落库 2395 行），产出 CSV/JSON/XML/Excel/Parquet 与 MySQL 表 `sales_demo`(3000)、`sales_pipeline`(2395) 均正确。排查中另确认 `csv2parquet`/`csv2csv_validate` 的失败是 TaskManager **4 个 Slot 被常驻流式作业占满**所致，非模板问题。后端 **136** 测试、前端 **37** 测试全绿 | SAMPLE_DAGS + BUILTIN_TEMPLATES + resolveTemplatePath / DagTranslationService(buildTransformSql + 临时视图) / 前端 index.html + app.test.js / DagTranslationServiceTest / DEVELOPMENT |
| 2026-09-15 | **账户资料、头像与在线状态闭环**：`AppUser` 新增头像键、签名、展示状态、最近登录/活动与更新时间；`GET/PUT /api/auth/me` 返回并更新完整个人资料，登录记录活动时间，普通用户无法修改用户名/角色/启停状态。头像采用 `backend/data/avatars` 文件存储、数据库仅保存 UUID 键；上传限 JPEG/PNG 2MB，先读图片头校验最大 4096px/总像素后才解码，并通过 subsampling + 512px 归一化重编码清除 EXIF，读取必须携带 JWT，替换/删除清理旧文件。导航栏最右侧改为头像+在线状态点，下拉提供个人资料、快捷状态切换和退出，弹窗支持名称、签名、展示状态与头像操作；头像经 Axios Blob 携带 Bearer token 读取，使用 `avatarVersion` 破缓存并用请求序号避免 Blob URL 竞态泄漏。在线状态分离 `statusPreference` 与服务端推导的 `publicStatus`：前端 60 秒 HTTP 心跳、90 秒超时离线、20 秒内重复心跳不落库，防并发重入，恢复可见/联网时补心跳，退出/401/卸载清理定时器。`JwtAuthFilter` 每次请求以数据库最新用户状态建立权限，管理员降权/停用与名称修改下一请求即生效。真实隔离后端验收：资料更新 `BUSY`、心跳、头像上传→鉴权读取(image/png)→删除全通过。后端 **125** 测试、前端 **33** 测试全绿 | AppUser / AuthController / UserService / AvatarStorageService / AppUserRepository / JwtAuthFilter / SecurityConfig / application.yml / .env.example / 前端 index.html + app.js + style.css + tests / DEVELOPMENT |
| 2026-09-15 | P0/P1 路线交叉核对（对照并行提交 99ac2fe / 65e1d6e / c404a73 / 7ed8bbf / fdc6b73 / 34172e0 / 1f190d6 / 236e8c9 之后的源码逐条复核，HEAD=8dfde85）：第 10 节路线改为带状态标注（✅ 已完成 / 🟡 部分 / ⬜ 仍开放）并写明证据——**已修**：调度线程池隔离（`spring.task.scheduling.pool.size=4` + 告警邮件独立有界线程池 `AsyncExecutorConfig`）、mock 认领加时间上限（`MOCK_RESOLVE_MAX_DIFF_MS`，修掉认领 976 秒前旧作业）、Monitor 端点按 owner 过滤且删除趋势需 ADMIN/OPERATOR、预览白名单抽为 `PreviewService.assertAllowedRead` 并被作业输出预览复用（堵住读 `.env` 的路径）、`JwtAuthFilter` 每请求回查用户使停用/改角色下一请求即生效、测试从 18 类 71 用例增至 **33 类 136 用例**；**仍开放**：token 无吊销（无 tokenVersion/passwordChangedAt）、`KafkaMonitorController` 与 `PreviewController` 无角色限制、`assertCanAccess` 仍对 ownerId 为空的历史作业放开、Flink 重启时「不在 overview 即 COMPLETED」误判（与 JOB_HEARTBEAT_LOST 语义冲突）、`JobService.submit` 无锁/CAS 与 `cleanOutputTempDirs`+merge 的并发写窗口、`app.js` 从 2451 行涨到 **3381 行**、PluginLoaderService / Parquet / HDFS / JobScheduler 仍无测试。另修正 §9.1 控件数 29→31（新增 pg_output / oracle_output，前后端注册表一致性校验通过） | DEVELOPMENT.md |
| 2026-09-15 | **P0/P1 六项逐个修复**（承接上一轮交叉核对结论，每项独立提交）：① **Flink 状态语义**（`51538c4`）——新增集群一代标识（`/taskmanagers` 在册 TM 注册 ID 集合 `clusterGeneration`），「不在 overview」不再一律判成功：换代（JM 重启/TM 重新注册）→ FAILED + ERROR 日志 + 事件驱动告警，未换代才 COMPLETED，与 JOB_HEARTBEAT_LOST 语义对齐；探测只在有作业消失时惰性执行一次，不给 5s 热路径加固定开销。② **token 吊销**（`c9def90`）——`AppUser.tokenVersion` + JWT claim `tv` + `JwtAuthFilter` 逐请求比对，改密/管理员重置/登出（`UserService.revokeTokens`）立即失效旧 token，旧 token 无该 claim 按 0 兼容。③ **无归属作业收紧**（`863d19c`）——新增 `security/JobAccess` 作为唯一归属规则（ADMIN 全量、普通用户仅本人、ownerId 为空仅 ADMIN），JobService/DependencyService/LineageService/MonitorService 四处重复实现全部委托，修掉「空归属即放行」。④ **端点权限**（`73a4e6d`）——`/api/kafka/**` 与 `/api/preview/**` 加类级 ADMIN/OPERATOR 限制，前端 Kafka 导航与「预览数据」按钮对 VIEWER 隐藏。⑤ **并发安全**（`46a3c7d`）——新增 `service/JobLocks`（64 分片可重入锁池），`submit` 全程持锁并新增状态前置校验（SUBMITTED/RUNNING 再提交 409），`cancel` 共用同一把锁，5 处重复的输出后处理收敛为持锁的 `finalizeJobOutputs`，杜绝重复 Flink 作业与 part 合并竞态；`online` 遇「已在运行」改为直接接管不再重复提交。⑥ **前端资源生命周期**（`11d42e9`）——resize 监听句柄化（重绑前先移除，修掉 initGraph 重入导致的监听器叠加）、新增 `disposeAllCharts()` 统一释放 4 个 ECharts 与 X6 画布，`onUnmounted` 与 `logout` 都清理监听/图表并停 Kafka 流与轮询。**测试**：新增 `JobLocksTest`(4)、`JobAccessTest`(5)、`JwtUtilTest`(3)、`FlinkJobStatusCheckerTest`(+2)、`JobServiceTest`(+2)、`UserServiceTest`(+3)、`KafkaMonitorControllerTest`(+1) 与前端 `security.test.js`(2)、`lifecycle.test.js`(2)；后端 **156** 用例、前端 **41** 用例全绿；§8 红线补 4 条（提交入口/输出后处理/权限判断/令牌版本），§10 路线同步标注完成状态 | FlinkJobStatusChecker / JobLocks(新) / JobService / JobAccess(新) / JwtUtil+JwtAuthFilter+AppUser / UserService / AuthController / KafkaMonitorController / PreviewController / MonitorService / DependencyService / LineageService / 前端 app.js + index.html + 测试 ×5 / DEVELOPMENT.md |
| 2026-09-16 | **健康探针 + 看门狗（专治"端口在听但请求不响应"的僵死）**：起因为当日实测事故——后端 JVM 跑约 24h 后僵死：18080 端口在监听、TCP 能建立、acceptor/poller 正常、工作线程全空闲、CPU 三秒零增长，但**任何请求（含 `/api/auth/login`）永不返回**，浏览器登录一直转圈；线程转储无死锁无阻塞，属进程级僵死，重启即恢复（此前根因排查耗时较长，故补自动化手段）。改动：① 新增 `HealthController`（`GET /api/health`，`SecurityConfig` 中 permitAll）——免鉴权、无副作用（不写审计/日志，避免探针刷满审计表）、**真查一次数据库**，DB 异常返回 503；只探端口抓不住这类故障。② 新增 `scripts/start-backend.ps1`——注入根目录 `.env`（缺 `JWT_SECRET` 直接报错，避免启动即失败）、设置 `SERVER_PORT`/`FLINK_CLUSTER_PORT`/`FLINK_SQL_GATEWAY_PORT`、`Start-Process` 独立进程启动并轮询端口就绪。③ 新增 `scripts/watchdog.ps1` + `watchdog.bat`——周期探 `/api/health` 并记录耗时，连续 N 次（默认 3）失败即判定僵死：停掉占用端口的进程 → 调 start-backend.ps1 重启 → 复检；含重启冷却 60s、每小时上限 5 次、观察模式 `-NoRestart`、单次探针 `-Once`（退出码 0/1）、演示前预检 `-Preflight`（前端/后端+DB/Flink/SQL Gateway/Kafka 五项表格 + 后端耗时）、日志 `logs/watchdog.log`（5MB 自动滚动）、显式禁用系统代理避免本机探测被劫持。**实测**：① 直接杀掉后端 → 2 次探测后判定僵死并自动拉起，复检 UP（18ms）、登录链路正常；② 用「只监听不响应」的假服务占住 18080 **复现原始故障** → 探针 5s 超时命中 → 看门狗杀掉假服务、拉起真后端；③ `-Preflight` 输出 5/6 通过（Kafka 未启动，符合真实状态，退出码=失败项数）。另：§8 红线补 2 条（`.ps1` 必须 UTF-8 with BOM，否则 PS 5.1 按 GBK 解析报错；打包前必须先停后端，否则 jar 被锁会导致 repackage 失败并留下 0.5MB 瘦包） | HealthController + SecurityConfig / scripts/start-backend.ps1 + watchdog.ps1 + watchdog.bat / HealthControllerTest / DEVELOPMENT §4.4·§4.4.1·§9.1·§8 / README |
| 2026-09-16 | **下线「控件管理」页（A 方案：摘入口）**：该页是纯只读表格（类型/名称/分类/描述/版本/来源/启用 disabled 开关），唯一数据源是 `GET /api/controls`，与「画布左侧控件库」和「帮助中心 → 控件说明」两处重复；后端虽提供 ADMIN 的 `POST/PUT/DELETE /api/controls`，但 `DataInitializer` 启动时 `deleteAll()` 后只重建内置控件（且插件加载器是 `@PostConstruct`，早于 `CommandLineRunner`，插件控件同样会被清空），翻译层又按 `node.getType()` 硬编码分支——即"能改的入口 + 改完没用的结果"，容易误导使用者与评委。改动：① 移除顶部「开发资源」下拉中的「控件管理」入口与其独立视图（`v-show="currentView === 'controls'"`）；② 删除对应 `.controls-view` 样式；③ **保留** `loadControls()` 与 `GET /api/controls`（画布控件库按 category 过滤注册表，必须依赖）；④ 帮助下拉的「控件说明」本就绑定 `openHelp`（弹窗内 tab），保持不动，成为控件清单的唯一展示入口。新增前端回归用例 `frontend/test/controls-page.test.js`（2 例：不得再出现独立控件视图、帮助中心入口与注册表消费链仍在），防止页面被误加回来。前端 **43** 测试全绿 | 前端 index.html + style.css + test/controls-page.test.js / 学习路径.md / DEVELOPMENT §8 红线新增第 28 条 |
| 2026-09-16 | **修复插件 SPI 链路（两个真缺陷，插件功能此前名存实亡）**：① **插件控件每次启动被清空**——`PluginLoaderService` 是 `@PostConstruct`（早于 `CommandLineRunner` 的 `DataInitializer`），而后者「count>0 就 `deleteAll()` 再重建内置控件」，于是插件注册的行启动即被抹掉。改为 `seedBuiltInControls()` 按 type **upsert**：同名内置控件原地更新（保留管理员设置的 `enabled` 与 `createdAt`）、只清理「jarPath=built-in 且已从代码移除」的陈旧行、插件行一律保留；并新增 `removeOrphanPluginControls()`：jar 被移出插件目录后，下次启动回收其控件行，避免控件库留孤儿。② **SDK 接口不被识别**——加载器只判断内置的 `com.datastream.mvp.plugin.ControlPlugin`，而 README 与 `backend/plugin-sdk` 让第三方实现 `com.datastream.plugin.DataStreamPlugin`（`backend/plugin-example` 亦然），且 `backend/pom.xml` 根本没有 plugin-sdk 依赖，导致按文档写的插件被静默跳过、从来加载不了。现补 plugin-sdk 依赖 + `isPluginClass` 同时识别两种接口（`field()` 适配取字段），与内置同 type 时保留内置定义并告警，已存在的行保留 `enabled`。**端到端验证**：用 javac + jar 造了一个真实插件 jar（实现 SDK 接口、type=`demo_probe_plugin`）放入 `backend/plugins/` → 启动后 `/api/controls` = **32**（31 内置 + 1 插件，日志 `Plugin registered ... jar=probe-plugin-1.0.0.jar`、`保留插件控件 1 个`）→ **重启后端后仍为 32，插件控件存活**（回归点）；随后移除该 jar 再重启 → **31**，日志 `Removed control of missing plugin: demo_probe_plugin` + `Cleaned 1 orphan plugin control(s)`。测试：新增 `DataInitializerControlsTest`(4) 与 `PluginLoaderServiceTest`(4，含两种接口识别/冲突保留内置/孤儿清理)；后端 **166** 用例、前端 **43** 用例全绿。文档：README §九 重写（两种接口、重启不丢、孤儿回收、type 唯一、模板必须可执行 SQL、替换 jar 前停后端、1 分钟验证步骤）、§8 红线 28 改写并新增 29（两个接口都要认） | DataInitializer / PluginLoaderService / backend/pom.xml / DataInitializerControlsTest(新) / PluginLoaderServiceTest(新) / README §九 / DEVELOPMENT §8 |
| 2026-09-16 | **修复「拖拽控件落下两个」**：用户实测拖一个控件画布上出现两个。根因是 `setupDropHandler()`（有 5 个调用点：onMounted、登录成功、切到画布视图、AI 结果、loadDagToCanvas 补初始化）每次都直接 `container.addEventListener('drop'/'dragover', 匿名函数)` 且从不移除；典型时序——onMounted 时画布视图隐藏（`initGraph` 因容器 0 尺寸提前返回）但 `setupDropHandler` 仍绑定了第 1 个监听，切到画布视图后 `if(!graph){ initGraph(); setupDropHandler(); }` 绑定第 2 个 → 一次 drop 触发两次 `addNode`（两个节点 id 相差几毫秒）。修复：① `setupDropHandler` 改为**幂等绑定**（保存 `dropHandler`/`dragoverHandler` 句柄，重绑前先 removeEventListener）；② `initGraph` 增加重入保护 `if (graph) return;`，避免同一容器叠加第二个 X6 实例（节点/连线重复渲染）。**验证**：新增无头浏览器脚本 `scripts/verify-drag-single-node.cjs`（Edge + CDP：登录 → 切画布视图 → 合成一次真实 drop → 数 `.x6-node`），修复后 **1 个节点 ✅**；为证明该脚本能抓住缺陷，临时停用"重绑前移除"复现旧行为 → **2 个节点 ❌（id `...3389718` / `...3389722`）**，随后还原（与备份逐字节一致）；另加前端静态回归 `frontend/test/drag-drop.test.js`（2 例：监听幂等、initGraph 重入保护）。前端 **45** 用例 0 失败，§8 红线新增第 30 条、§9.4 回归清单加入真浏览器验证步骤 | 前端 app.js + test/drag-drop.test.js + scripts/verify-drag-single-node.cjs（新）/ DEVELOPMENT §8·§9.4 |
| 2026-09-16 | **修复「面板里填了参数，提交前检查却说缺少必填参数: path」**：用户截图反馈——画布上 CSV 输入节点在参数面板里能看到「文件路径」，点提交运行却被 preflight 拦下「节点 csv_input_xxx 缺少必填参数: path」。三个关联缺陷：① **拖拽建节点时 `params` 固定为 `{}`**，面板显示的只是 paramSchema 默认值（`node:click` 里用 schema 默认值兜底填充面板），节点里其实是空的；② **切换节点时不写回面板改动**（`node:click` 直接改 `selectedNode`），上一个节点改过的参数被静默丢弃；③ **提交前不落盘**——`saveDag()` 会 `applyParams()`，但 `submitJob()` 不会，而后端 preflight 校验的是**已保存的 DAG**，于是"画布现状"与"被校验的版本"错位。修复：① 新增 `defaultParamsFromSchema(schema)`，拖拽建节点时用 paramSchema 的 `default` 初始化 `params`（面板所见 = 节点所存）；② `node:click` 切换前 `if (selectedNode.value && selectedNode.value !== node) applyParams();`；③ `submitJob()` 改为「applyParams → `saveDag({silent:true})` → preflight → 提交」，`saveDag` 新增 `options.silent`（静默保存不弹 toast）并返回成功标记。**真浏览器端到端验证**：把只验证「一次拖拽=一个控件」的脚本升级为 `scripts/verify-canvas.cjs`（新增 CDP **原生指针输入**——探测确认 X6 连线只认原生事件，合成 JS MouseEvent 无效），**6/6 全 PASS**：一次拖拽=一个控件、原生拖拽可连出一条边、落库的 csv_input 带 `path=...sales.csv`、csv_output 带 `path=...output.csv`、**preflight errors=[]**（原报错场景）、面板改参数直接保存即落库（无需先点应用参数）；脚本用后自行删除验证作业（HTTP 200）。测试：新增 `frontend/test/params-persist.test.js`（4 例，含"applyParams→saveDag→preflight"顺序断言），前端 **49** 用例 0 失败；旧脚本 `verify-drag-single-node.cjs` 由 `verify-canvas.cjs` 取代。§8 红线新增第 31 条（参数面板=待应用状态，三条链路必须补齐） | 前端 app.js（defaultParamsFromSchema / node:click / submitJob / saveDag）+ test/params-persist.test.js（新）+ scripts/verify-canvas.cjs（取代旧脚本）/ DEVELOPMENT §8·§9.4 |
| 2026-09-17 | **产品更名：数据流任务管理系统 → 通用流处理任务管理平台**：按需求把产品名在**整个工作区**统一替换（同时收编旧变体「数据流任务管理平台」），共 **18 个文件 30 处**——前端 `index.html`（浏览器标题 / 顶栏 h1 / 帮助中心「关于」）、`README.md`、`DEVELOPMENT.md`、`backend/pom.xml` 描述、`data/mysql_setup.sql`、`test-resources/*`（system_config.json、benchmark.py、run-benchmark.bat）、`test-results/benchmark_report.md`、`gen_test_data.py`、`scripts/test-mysql-flow.ps1`、`docs/作品演示录像脚本.md`、`scripts/*`（视频卡片 / 成片 / DB 说明书生成器）、`frontend/public/css/style.css`。替换用 Python 逐文件执行并**保留原 BOM 与换行**（5 个文件原带 BOM），规避 javac `\ufeff` 与 PowerShell 编码坑。**同步重生成**一切内嵌旧名的产物：视频卡片（片头/片尾）、成片 `作品演示录像_5min.mp4`、`旁白文稿.md/.docx`、`docs/数据库设计说明书.docx` + `docs/数据库-ER图.png`。回归：前端 **49 用例 0 失败**、`node --check app.js` 通过、三个 Python 生成器语法通过；工作区已无旧名残留（`.claude/worktrees/*` 是并行会话的工作副本，刻意未动） | index.html · README.md · DEVELOPMENT.md · backend/pom.xml · data/mysql_setup.sql · test-resources/* · test-results/benchmark_report.md · gen_test_data.py · scripts/* · docs/* || 2026-09-17 | **修掉演示卡片「字出框」**：性能测试卡底部评审要点框（原 300–1620）内文字按固定字号 30 直接绘制，长句越过框线。改为统一排版层：新增 `fit_font/center_fit/left_fit`，**按实测文本宽度逐磅缩小字号**（下限 15pt），框加宽到与其它卡一致的 140–1780 并留 40pt 内边距；同时加**排版自检**——任何一行越过 60pt 安全边距即打印 `[overflow]` 并以退出码 1 失败，避免以后再漏。四张卡（片头/架构/性能/片尾）全部改为自适应排版，自检通过、抽帧确认；成片 `作品演示录像_5min.mp4` 已重建并同步桌面交付目录 | scripts/make_video_cards.py · output/_video_work/cards/* + 成片 || 2026-09-18 | **补齐「用户管理」界面（增删查改）**：后端 `/api/users` 自 2026-09-08 起已具备完整 CRUD（类级 ADMIN 限制 + 审计 + 防删当前账号 + 保留至少一个启用管理员 + 重置密码吊销 token），但**前端一直没有入口与界面**（顶栏「管理」下只有操作审计与运营总览），管理端实际做不到增删用户。本次补齐：① 顶栏「管理 → 用户管理」（仅 ADMIN 可见）；② 新视图 `users-view`——搜索用户名/显示名、按角色筛选、前端分页列表（ID/用户名/显示名/角色/状态/创建时间），角色与状态用彩色 tag，当前登录账号带「当前登录」标记；③ 四个操作：**新建**（用户名 3–32 位字母数字下划线、显示名必填、密码 ≥8 位且含字母和数字，角色默认 VIEWER）、**编辑**（显示名/角色/启用开关，用户名不可改）、**重置密码**（同一强度校验，提示旧登录态失效）、**删除**（ElMessageBox 二次确认）；④ 保护措施在界面上同步体现——本人删除按钮 `:disabled`、非 ADMIN 进入即弹回工作台并提示、表头展示「启用中的管理员 N 个」。**验证**：新增真浏览器端到端脚本 `scripts/verify-users-crud.cjs`（Edge + CDP：登录 → 管理菜单 → 列表 → 新建 → 编辑 → 重置密码 → 删除 → 自删保护 → 审计四类记录，脚本自清理测试账号）**7/7 PASS**；新增前端静态回归 `frontend/test/users-admin.test.js`（7 例：入口权限、视图与四操作、API 五方法、权限兜底、自删保护、密码强度、模板变量暴露），前端 **56** 用例 0 失败；后端 **166** 用例 0 失败（`UserServiceTest` 13 例已覆盖删除/降级/重置密码三处保护）。文档：README 增「用户管理」功能条目与 `/api/users` 接口表、§8 红线新增第 32 条、§9.4 加入验证命令 | frontend/public/index.html · js/app.js · frontend/test/users-admin.test.js（新）· scripts/verify-users-crud.cjs（新）· README.md · DEVELOPMENT.md |