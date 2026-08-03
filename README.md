# 数据流任务管理系统（DataStream MVP）

> 基于 **Spring Boot 3.2 + Apache Flink 1.18 + Vue 3 + AntV X6** 的可视化数据流任务编排平台：在画布上拖拽控件搭建 DAG，一键翻译为 Flink 作业并提交 Standalone 集群运行，结果输出到文件或 MySQL。

## 一、功能特性

- **可视化画布**：AntV X6 拖拽建节点、端口连线、点选配置参数、Delete 键 / 双击删除、保存 / 提交 / 导出 / 导入 JSON、新建画布、清空画布、Ctrl+S 保存。
- **11 个内置控件**：CSV / Excel / MySQL / Kafka / Datagen 输入输出 + 字段拼接 + XML↔JSON 转换。
- **作业管理**：状态机 DRAFT → SUBMITTED → RUNNING → COMPLETED / FAILED / CANCELLED，Flink 状态每 15s 轮询回写，日志落库可查。
- **动态参数面板**：按控件 paramSchema 自动渲染 string / number / boolean / enum / array 类型参数。
- **MySQL 自动建表**：MySQL 输出控件自动建表（utf8mb4、类型映射、中文无乱码）。
- **已实测链路**：CSV → CSV / Excel / MySQL；Excel → CSV / MySQL；Datagen → CSV / Excel。

## 二、快速启动

### 方式一：手动启动（推荐）

#### 1. 启动 Flink 集群（提交真实作业前必须启动）

```bat
D:\code\flink-1.18.1\bin\start-cluster.bat
start_sql_gw.bat     :: SQL Gateway（端口 8083，可选）
```

验证：浏览器打开 http://localhost:8081 能看到 Flink Web UI。

> 注意：Flink 未启动时，作业提交走 mock 模式，状态会显示 COMPLETED，但不会真正执行。

#### 2. 启动后端

```bat
cd backend
mvn spring-boot:run
```

- 后端地址：http://localhost:8080
- 元数据库（默认 H2）：http://localhost:8080/h2-console ，JDBC URL `jdbc:h2:file:./data/mvpdb`，用户名 `sa`，密码留空
- 启动时自动初始化 11 个内置控件

#### 3. 启动前端

```bat
cd frontend
npm run dev        :: 等价于 node serve.js
```

- 前端地址：http://localhost:3000 （serve.js 提供静态服务并反向代理 /api 到 8080）

### 方式二：一键脚本

```bat
start-all.bat
```

> 注意：`start-all.bat` 内嵌的中文路径在部分机器上会显示为乱码，启动失败时请改用方式一手动执行。

### 方式三：Docker 全容器化

```bat
start-docker.bat        :: 一键构建并启动 7 个容器
docker compose ps       :: 查看状态，全部应为 healthy
```

详见「六、Docker 部署」。

## 三、使用流程（如何测试）

1. 按「快速启动」依次启动 Flink、后端、前端。
2. 浏览器打开 http://localhost:3000 。
3. 从左侧控件库拖拽「CSV 输入」和「CSV 输出」到画布。
4. 从 CSV 输入节点右侧端口拖线连接到 CSV 输出节点左侧端口。
5. 点选节点，在右侧参数面板填写：
   - CSV 输入 `path`：`D:\code\比赛\2026省服务外包\test-resources\data\sales.csv`
   - CSV 输出 `path`：`D:\code\比赛\2026省服务外包\output\out.csv`
6. 点击「保存」，再点击「提交运行」。
7. 到 http://localhost:8081 查看 Flink Job 运行状态；到项目根目录 `output/` 查看输出文件。
8. 使用 MySQL 输出控件时，提交成功后在 `dataflow` 库中查看自动创建的表。

## 四、内置控件（11 个）

| 分类 | type | 说明 | 状态 |
|------|------|------|------|
| 输入 | datagen_input | 内置随机数据生成器 | 可用 |
| 输入 | csv_input | 读取 CSV 文件 | 可用 |
| 输入 | excel_input | 读取 .xls / .xlsx（POI） | 可用 |
| 输入 | kafka_input | 消费 Kafka 主题 | 需自建 Kafka |
| 输入 | mysql_input | 读取 MySQL 表 | 规划中（仅注册，翻译层未实现） |
| 转换 | field_concat | 多字段拼接为新字段 | 可用 |
| 转换 | xml_json | XML ↔ JSON 互转 | 需 UDF jar |
| 输出 | csv_output | 写 CSV 文件 | 可用 |
| 输出 | excel_output | 写 .xlsx（临时 CSV + POI 转换） | 可用 |
| 输出 | kafka_output | 写 Kafka 主题 | 需自建 Kafka |
| 输出 | mysql_output | 写 MySQL 表（自动建表 utf8mb4） | 可用 |

## 五、元数据库（H2 / MySQL）

- 默认 **H2 file**：`backend/data/mvpdb`（`application.yml`），零安装即可跑通全流程。
- 切换 MySQL（`application-mysql.yml`）：

```bat
cd backend
mvn spring-boot:run -Dspring-boot.run.profiles=mysql
```

## 六、Docker 部署

一条命令启动 MySQL + Kafka + Flink（JobManager / TaskManager / SQL Gateway）+ 后端 + 前端，无需在本机安装 Java / Maven / Node / Flink。

### 前置条件

- 安装并启动 [Docker Desktop](https://www.docker.com/products/docker-desktop/)（Windows 建议开启 WSL2 后端）。
- 预留约 5GB 磁盘空间（镜像 + 数据卷）。

### 一键启动

```bat
start-docker.bat
```

脚本自动完成：① 同步本机 UDF / 连接器 jar 到 `docker/flink/lib/`；② 构建后端、前端、Flink 三个镜像；③ 启动全部 7 个容器并等待健康检查通过。

也可手动执行：

```bat
docker compose up -d --build
```

### 访问地址

| 服务 | 地址 | 说明 |
|------|------|------|
| 前端页面 | http://localhost:3000 | 画布编排入口 |
| 后端 API | http://localhost:8080 | REST API |
| Flink Web UI | http://localhost:8081 | 作业监控 |
| SQL Gateway | http://localhost:8083 | Flink SQL 网关 |
| MySQL | localhost:3307 | root / root123 |
| Kafka | localhost:29092 | bootstrap.servers |

> 本机 3306 / 9092 可能被本地 MySQL / Kafka 占用，容器映射到 3307 / 29092 避免冲突。

### 画布内填写的容器参数

| 控件 | 参数 | 值 |
|------|------|-----|
| MySQL 输出 | url | `jdbc:mysql://mysql:3306/flink_demo?useSSL=false&serverTimezone=Asia/Shanghai` |
| MySQL 输出 | username / password | `root` / `root123` |
| Kafka 输入 / 输出 | bootstrap.servers | `kafka:9092` |
| CSV 输入 | path | `/data/xxx.csv`（对应项目根目录 `data/`） |
| CSV 输出 | path | `/output/xxx.csv`（对应项目根目录 `output/`） |

首次启动 MySQL 会自动执行 `data/mysql_setup.sql`，创建 `flink_demo.user_data` 表，datagen → MySQL 测试作业可直接使用。

### 验证 / 停止

```bat
docker compose ps                   :: 全部应为 healthy
curl http://localhost:8081/overview :: Flink 集群信息
curl http://localhost:8083/v1/info  :: SQL Gateway 信息
```

```bat
stop-docker.bat                :: 停止容器（保留数据卷）
docker compose down -v         :: 停止并删除数据卷
```

### 目录挂载

| 宿主机目录 | 容器内路径 | 用途 |
|-----------|-----------|------|
| ./backend/data | /app/data | 后端 H2 数据库持久化 |
| ./backend/plugins | /app/plugins | 控件插件热加载 |
| ./data | /data | 作业输入数据（CSV / Excel） |
| ./output | /output | 作业输出文件 |

## 七、API 接口

### 控件注册表（/api/controls）

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | /api/controls | 控件列表 |
| GET | /api/controls/{id} | 控件详情 |
| GET | /api/controls/type/{type} | 按类型查控件 |
| POST | /api/controls | 创建控件 |
| PUT | /api/controls/{id} | 更新控件 |
| DELETE | /api/controls/{id} | 删除控件 |

### 作业（/api/jobs）

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | /api/jobs | 作业列表 |
| POST | /api/jobs | 创建作业 |
| GET | /api/jobs/{id} | 作业详情 |
| PUT | /api/jobs/{id} | 更新作业 |
| DELETE | /api/jobs/{id} | 删除作业 |
| POST | /api/jobs/{id}/submit | 提交作业到 Flink |
| POST | /api/jobs/{id}/cancel | 取消作业 |
| GET | /api/jobs/{id}/logs | 作业日志 |
| GET | /api/jobs/{id}/preview | DAG 翻译预览 |

## 八、项目结构

```
项目根目录
├── backend/                 # Spring Boot 后端
│   ├── src/main/java/com/datastream/mvp/
│   │   ├── MvpBackendApplication.java
│   │   ├── config/          # DataInitializer（控件种子）/ WebConfig / 异常处理
│   │   ├── controller/      # JobController / ControlRegistryController
│   │   ├── dag/             # DAG 模型 + DagExecutor / FlinkDagExecutor / LocalDagExecutor
│   │   ├── model/           # JobDefinition / ControlRegistry / JobLog
│   │   ├── repository/      # JPA 数据访问
│   │   └── service/         # DagTranslationService / MysqlTableCreator 等
│   ├── plugin-sdk/          # 控件插件开发 SDK
│   ├── plugins/             # 热加载插件目录
│   └── pom.xml
├── frontend/
│   ├── public/              # 静态 SPA（index.html / css / js/app.js）
│   ├── serve.js             # 静态服务 + /api 反向代理（端口 3000）
│   └── package.json
├── flink/                   # Git Bash 包装脚本与配置示例
├── flink-1.18.1/            # 本地实际运行的 Flink 发行版（也可位于 D:\code\flink-1.18.1）
├── docker/flink/            # Flink 自定义镜像（Dockerfile + UDF / 连接器 jar）
├── docker-compose.yml       # 全容器化编排（7 个服务）
├── data/                    # 输入数据挂载目录
├── output/                  # 输出文件目录（CSV / Excel）
├── test-resources/          # 测试数据 + benchmark.py
├── udf/                     # UDF 工程
├── docs/                    # 项目文档
├── scripts/                 # 辅助脚本
├── start-all.bat            # 一键启动（本机）
├── start-docker.bat         # Docker 一键启动
└── stop-docker.bat          # Docker 一键停止
```

## 九、插件热加载

将实现 ControlPlugin SPI 接口的 JAR 放入 `backend/plugins/` 目录，系统自动加载并注册到前端控件库。

```java
// 插件 SDK 示例（backend/plugin-sdk）
public class RedisLookupPlugin implements DataStreamPlugin {
    public String getType() { return "redis_lookup"; }
    public String getName() { return "Redis 异步查询"; }
    // ...
}
```

## 十、技术栈

| 组件 | 版本 / 说明 |
|------|------|
| JDK | 17 |
| Spring Boot | 3.2.5（web / data-jpa / validation） |
| 元数据库 | H2 file（默认）/ MySQL（profile 切换） |
| Apache Flink | 1.18.1 Standalone（JobManager 8081 / SQL Gateway 8083） |
| 前端 | Vue 3 + Element Plus 2.9.1 + AntV X6 3.1.7 + axios |
| 其他 | Lombok、Jackson、Apache Commons CSV、Apache POI（Excel） |

## 十一、相关文档

- `DEVELOPMENT.md`：开发维护指南（逻辑框架 / 扩展规范 / 红线 / 回归清单）
- `docs/`：详细设计文档
- `test-resources/benchmark.py`：性能基准测试脚本

---
