# 数据流任务管理系统 - DataStream MVP

> 基于 Flink + Spring Boot + AntV X6 的可视化数据流任务编排平台

## 项目结构

`
├── backend/                    # Spring Boot 后端服务
│   ├── pom.xml                 # Maven 项目配置
│   ├── Dockerfile              # 后端容器镜像
│   ├── .dockerignore           # 构建上下文忽略
│   ├── src/main/java/.../
│   │   ├── MvpBackendApplication.java
│   │   ├── config/             # Web 配置 + 初始化数据
│   │   ├── controller/         # REST API 控制器
│   │   ├── dag/                # DAG 定义 + 执行引擎
│   │   ├── model/              # 实体模型
│   │   ├── plugin/             # 插件热加载 SPI
│   │   ├── repository/         # JPA 数据访问
│   │   └── service/            # 业务服务层
│   ├── plugin-sdk/             # 控件插件开发 SDK
│   └── plugins/                # 热加载插件目录
├── frontend/
│   ├── Dockerfile              # 前端容器镜像
│   ├── .dockerignore           # 构建上下文忽略
│   ├── public/                 # 静态 SPA
│   │   ├── index.html          # 主页面
│   │   ├── css/style.css       # 样式
│   │   └── js/app.js           # Vue3 + AntV X6 应用
├── flink/
│   ├── conf/flink-conf.yaml    # Flink 集群配置
│   ├── bin/start-cluster.bat   # 启动脚本
│   └── examples/               # DAG JSON 示例
├── test-resources/
│   ├── benchmark.py            # 性能基准测试脚本
│   └── data/                   # 测试数据目录
├── docker/
│   └── flink/                 # Flink 自定义镜像（Dockerfile + UDF/连接器 jar）
├── docker-compose.yml         # Docker Compose 全容器化编排
├── start-docker.bat           # Docker 一键启动
├── stop-docker.bat            # Docker 一键停止
└── docs/                       # 项目文档
`

## 快速启动

### 1. 启动后端

`ash
cd backend
mvn spring-boot:run
`

启动后自动初始化 8 个内置控件，访问 http://localhost:8080/h2-console 查看数据库。

### 2. 启动前端

直接用浏览器打开 rontend/public/index.html（或使用 Live Server）。

### 3. 启动 Flink 集群（可选）

`ash
flink\\bin\\start-cluster.bat
`

访问 Flink Web UI: http://localhost:8081

### 4. 运行基准测试

`ash
cd test-resources
python benchmark.py
`

## Docker 全容器化部署（推荐）

一条命令启动 MySQL + Kafka + Flink（JobManager / TaskManager / SQL Gateway）+ 后端 + 前端，
无需在本机安装 Java / Maven / Node / Flink。

### 前置条件

- 安装并启动 [Docker Desktop](https://www.docker.com/products/docker-desktop/)（Windows 建议开启 WSL2 后端）
- 预留约 5GB 磁盘空间（镜像 + 数据卷）

### 一键启动

```bat
start-docker.bat
```

脚本会自动完成：① 把本机已构建的 UDF / 连接器 jar 同步到 `docker/flink/lib/`；② 构建后端、前端、Flink 三个镜像；③ 启动全部 7 个容器并等待健康检查通过。

也可以手动执行：

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
| MySQL | localhost:3307 | 用户 root / 密码 root123 |
| Kafka | localhost:29092 | - |

> 本机 3306 / 9092 可能被本地 MySQL / Kafka 占用，容器映射到 3307 / 29092 避免冲突。

### 画布中填写容器内参数

| 控件 | 参数 | 值 |
|------|------|-----|
| MySQL 输出 | url | `jdbc:mysql://mysql:3306/flink_demo?useSSL=false&serverTimezone=Asia/Shanghai` |
| MySQL 输出 | username / password | `root` / `root123` |
| Kafka 输入/输出 | bootstrap.servers | `kafka:9092` |
| CSV 输入 | path | `/data/xxx.csv`（对应项目根目录 `data/`） |
| CSV 输出 | path | `/output/xxx.csv`（对应项目根目录 `output/`） |

首次启动 MySQL 会自动执行 `data/mysql_setup.sql`，创建 `flink_demo.user_data` 表，datagen → MySQL 的测试作业可以直接使用。

### 验证部署

```bat
docker compose ps                   :: 全部应为 healthy
curl http://localhost:8081/overview :: Flink 集群信息
curl http://localhost:8083/v1/info  :: SQL Gateway 信息
```

### 停止 / 清理

```bat
stop-docker.bat                :: 停止容器（保留数据卷）
docker compose down -v         :: 停止并删除数据卷（MySQL 数据、checkpoint 等）
```

### 目录挂载说明

| 宿主机目录 | 容器内路径 | 用途 |
|-----------|-----------|------|
| `./backend/data` | `/app/data` | 后端 H2 数据库持久化 |
| `./backend/plugins` | `/app/plugins` | 控件插件热加载 |
| `./data` | `/data` | 作业输入数据（CSV / Excel） |
| `./output` | `/output` | 作业输出文件（CSV / Excel） |

> Excel 输出：作业写临时 CSV 到共享目录，后端在作业完成后自动转成 .xlsx，
> 最终文件位于项目根目录 `output/`。

## API 接口

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | /api/controls | 获取控件列表 |
| GET | /api/controls/{id} | 获取控件详情 |
| POST | /api/controls | 创建控件 |
| GET | /api/jobs | 获取作业列表 |
| POST | /api/jobs | 创建作业 |
| PUT | /api/jobs/{id} | 更新作业 |
| POST | /api/jobs/{id}/submit | 提交作业到 Flink |
| GET | /api/jobs/{id}/logs | 获取作业日志 |

## 内置控件

### 输入
- CSV 输入：读取 CSV 文件作为数据源
- Excel 输入：读取 .xlsx 文件
- MySQL 输入：从 MySQL 表消费数据
- Kafka 输入：从 Kafka 主题消费消息

### 转换
- 字段拼接：多字段拼接为新字段
- XML<->JSON：XML/JSON 格式互转

### 输出
- CSV 输出：数据写入 CSV 文件
- MySQL 输出：数据写入 MySQL 表
- Kafka 输出：数据写入 Kafka 主题

## 插件热加载

将实现 ControlPlugin SPI 接口的 JAR 放入 ackend/plugins/ 目录，系统自动加载并注册到前端控件库。

`java
// 插件 SDK 示例
public class RedisLookupPlugin implements DataStreamPlugin {
    public String getType() { return \"redis_lookup\"; }
    public String getName() { return \"Redis 异步查询\"; }
    // ...
}
`

## 技术栈

- **后端**: Spring Boot 3.2 + JPA + H2/MySQL
- **流计算**: Apache Flink 1.18
- **前端**: Vue 3 + Element Plus + AntV X6
- **插件**: Java SPI + URLClassLoader 热加载
