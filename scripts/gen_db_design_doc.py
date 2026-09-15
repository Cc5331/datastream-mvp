#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
生成《数据库设计说明书》：
  1) 用 Pillow 画 ER 图（实体方框 + 关系连线 + 基数标注）
  2) 用 python-docx 输出 docs/数据库设计说明书.docx（含表结构设计、索引、DDL 附录）

事实来源：backend/src/main/java/com/datastream/mvp/model/*.java（JPA 实体）
         与 Hibernate 导出的真实 DDL（docs/schema-h2.sql、docs/schema-mysql.sql）
用法：python scripts/gen_db_design_doc.py
"""
import os
from docx import Document
from docx.shared import Pt, Cm, RGBColor
from docx.enum.text import WD_ALIGN_PARAGRAPH
from docx.enum.table import WD_TABLE_ALIGNMENT
from docx.oxml.ns import qn
from PIL import Image, ImageDraw, ImageFont

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DOCS = os.path.join(ROOT, "docs")
DOCX = os.path.join(DOCS, "数据库设计说明书.docx")
ER_PNG = os.path.join(DOCS, "数据库-ER图.png")

CJK_FONT = r"C:\Windows\Fonts\msyh.ttc"
CJK_FONT_BOLD = r"C:\Windows\Fonts\msyhbd.ttc"

# ---------------------------------------------------------------- 表结构定义
# key: P=主键 F=逻辑外键 U=唯一约束 I=索引列
SCHEMA = [
    dict(
        table="app_user", cn="用户表", module="安全与权限",
        desc="系统账号与 RBAC 角色，同时承载个人资料、在线状态与令牌版本。",
        cols=[
            ("id", "bigint", "否", "P", "", "主键，自增"),
            ("username", "varchar(255)", "否", "U", "", "登录名，全局唯一"),
            ("password_hash", "varchar(255)", "否", "", "", "BCrypt 哈希（不存明文，不返回前端）"),
            ("display_name", "varchar(255)", "是", "", "", "显示名"),
            ("role", "enum(ADMIN/OPERATOR/VIEWER)", "否", "", "VIEWER", "角色：管理员/操作员/观察员"),
            ("enabled", "tinyint(1)", "否", "", "1", "是否启用；停用后下一请求即失去权限"),
            ("avatar_key", "varchar(255)", "是", "", "", "头像文件键（文件落 backend/data/avatars，库内不存二进制）"),
            ("signature", "varchar(200)", "是", "", "", "个性签名"),
            ("status_preference", "enum(ONLINE/AWAY/BUSY/INVISIBLE/OFFLINE)", "是", "", "ONLINE", "用户设置的在线状态；实际在线由心跳推导"),
            ("token_version", "int", "是", "", "0", "令牌版本：改密/重置/登出 +1，使旧 JWT 立即失效"),
            ("last_login_at", "datetime(6)", "是", "", "", "最近登录时间"),
            ("last_seen_at", "datetime(6)", "是", "", "", "最近活动时间（60s 心跳，90s 判离线）"),
            ("updated_at", "datetime(6)", "是", "", "", "更新时间"),
            ("created_at", "datetime(6)", "是", "", "", "创建时间"),
        ],
        indexes=["UNIQUE(username)"],
        notes=["不使用物理外键：其它表的 owner_id / user_id 通过服务层维护（见 2.4）。"],
    ),
    dict(
        table="job_definition", cn="作业定义表", module="作业编排",
        desc="核心表：一条记录 = 一个数据流作业（DAG 画布 + 状态机 + 调度 + 上线 + AI 归属）。",
        cols=[
            ("id", "bigint", "否", "P", "", "主键，自增"),
            ("name", "varchar(255)", "否", "", "", "作业名称"),
            ("description", "text", "是", "", "", "描述"),
            ("dag_json", "text", "否", "", "", "DAG 定义 JSON（nodes/edges/params），翻译层据此生成 Flink SQL"),
            ("status", "enum(DRAFT/SUBMITTED/RUNNING/COMPLETED/FAILED/CANCELLED/WAITING/BLOCKED)", "否", "", "DRAFT", "作业状态机"),
            ("flink_job_id", "text", "是", "", "", "Flink 作业 ID；mock 占位为 flink-job-* 前缀"),
            ("parallelism", "int", "是", "", "1", "并行度"),
            ("source", "enum(MANUAL/AI)", "是", "", "MANUAL", "来源：手工创建或 AI 生成"),
            ("confirmation_status", "enum(NOT_REQUIRED/PENDING/CONFIRMED)", "是", "", "", "AI 草稿确认状态（服务端强制推导）"),
            ("confirmed_by", "bigint", "是", "F", "", "确认人用户 ID → app_user.id"),
            ("confirmed_by_name", "varchar(255)", "是", "", "", "确认人名称（冗余，便于展示）"),
            ("confirmed_at", "datetime(6)", "是", "", "", "确认时间"),
            ("owner_id", "bigint", "是", "F", "", "创建人用户 ID → app_user.id（RBAC 资源隔离依据）"),
            ("owner_name", "varchar(255)", "是", "", "", "创建人名称（冗余）"),
            ("cron_expression", "varchar(255)", "是", "", "", "定时调度表达式（6 段 Spring cron）"),
            ("schedule_enabled", "tinyint(1)", "是", "", "0", "是否启用定时调度"),
            ("next_fire_time", "datetime(6)", "是", "", "", "下次触发时间（JobScheduler 每 30s 扫描）"),
            ("schedule_max_retries", "int", "是", "", "0", "调度失败最大重试次数"),
            ("schedule_retries", "int", "是", "", "0", "当前已重试次数"),
            ("webhook_url", "varchar(255)", "是", "", "", "该作业告警的 webhook 地址"),
            ("online", "tinyint(1)", "是", "", "0", "是否上线（受监管持续运行）"),
            ("online_since", "datetime(6)", "是", "", "", "最近上线时间"),
            ("online_restart_count", "int", "是", "", "0", "10 分钟窗口内自动重启次数"),
            ("last_restart_at", "datetime(6)", "是", "", "", "最近自动重启时间"),
            ("submitted_at", "datetime(6)", "是", "", "", "最近提交时间（吞吐统计基准）"),
            ("completed_at", "datetime(6)", "是", "", "", "结束时间（告警去重与耗时统计依据）"),
            ("created_at", "datetime(6)", "是", "", "", "创建时间"),
            ("updated_at", "datetime(6)", "是", "", "", "更新时间"),
        ],
        indexes=[],
        notes=[
            "DAG 用 JSON 列而非拆表：控件类型与参数随注册表演进，JSON 可免 DDL 变更；结构校验由翻译层 + 预检服务完成。",
            "状态机：DRAFT →(提交/调度/依赖触发) SUBMITTED → RUNNING → COMPLETED/FAILED/CANCELLED；依赖编排引入 WAITING/BLOCKED。",
        ],
    ),
    dict(
        table="job_log", cn="作业日志表", module="作业编排",
        desc="作业状态流转与运行期日志，前端「日志」弹窗与故障诊断的证据来源。",
        cols=[
            ("id", "bigint", "否", "P", "", "主键，自增"),
            ("job_id", "bigint", "否", "F", "", "所属作业 → job_definition.id"),
            ("level", "varchar(255)", "否", "", "", "日志级别 INFO/WARN/ERROR"),
            ("message", "text", "否", "", "", "日志内容"),
            ("timestamp", "datetime(6)", "是", "", "", "产生时间"),
        ],
        indexes=[],
        notes=["建议后续按数据量补 (job_id, timestamp) 复合索引；当前查询按 job_id 过滤，量级可控。"],
    ),
    dict(
        table="job_version", cn="作业版本表", module="作业编排",
        desc="作业保存/回滚的历史快照，支持一键回滚到任意版本。",
        cols=[
            ("id", "bigint", "否", "P", "", "主键，自增"),
            ("job_id", "bigint", "否", "F", "", "所属作业 → job_definition.id"),
            ("version_no", "int", "是", "", "", "版本号（创建=1，每次保存/回滚递增）"),
            ("name", "text", "是", "", "", "该版本的作业名"),
            ("dag_json", "text", "否", "", "", "该版本的 DAG 快照"),
            ("parallelism", "int", "是", "", "", "该版本的并行度"),
            ("created_at", "datetime(6)", "是", "", "", "生成时间"),
        ],
        indexes=[],
        notes=["删除作业时联删本表记录（JobService.delete → versionRepo.deleteByJobId）。"],
    ),
    dict(
        table="job_dependency", cn="作业依赖表", module="作业编排",
        desc="作业间的上下游依赖边，构成跨作业工作流（DAG）。",
        cols=[
            ("id", "bigint", "否", "P", "", "主键，自增"),
            ("upstream_job_id", "bigint", "否", "F,U", "", "上游作业 → job_definition.id"),
            ("downstream_job_id", "bigint", "否", "F,U", "", "下游作业 → job_definition.id"),
            ("created_at", "datetime(6)", "是", "", "", "建立时间"),
        ],
        indexes=["UNIQUE(upstream_job_id, downstream_job_id)"],
        notes=[
            "唯一约束防止重复依赖边；环检测在服务层（DFS 可达性判断），不在库层。",
            "删除作业时联删相关边（deleteByUpstreamJobIdOrDownstreamJobId）。",
        ],
    ),
    dict(
        table="schedule_history", cn="调度历史表", module="作业编排",
        desc="定时调度每次触发的执行记录（成功/重试/失败）。",
        cols=[
            ("id", "bigint", "否", "P", "", "主键，自增"),
            ("job_id", "bigint", "是", "F", "", "所属作业 → job_definition.id"),
            ("job_name", "varchar(255)", "是", "", "", "作业名（冗余，便于列表展示）"),
            ("trigger_time", "datetime(6)", "是", "", "", "触发时间"),
            ("status", "varchar(255)", "是", "", "", "SUCCESS / RETRY / FAILED"),
            ("message", "text", "是", "", "", "结果说明或失败原因"),
            ("flink_job_id", "varchar(255)", "是", "", "", "本次触发生成的 Flink 作业 ID"),
        ],
        indexes=[],
        notes=[],
    ),
    dict(
        table="trend_point", cn="监控趋势点表", module="监控与告警",
        desc="吞吐/背压时间序列点，支撑监控页趋势曲线（内存环形缓冲 + 落库双写）。",
        cols=[
            ("id", "bigint", "否", "P", "", "主键，自增"),
            ("job_id", "bigint", "否", "F", "", "所属作业 → job_definition.id"),
            ("t", "bigint", "否", "", "", "采样时间（epoch 毫秒）"),
            ("input_rate", "double", "否", "", "", "入速率 records/s（列名避开保留字 IN）"),
            ("output_rate", "double", "否", "", "", "出速率 records/s（趋势主值，列名避开关键字 OUT）"),
            ("bp", "double", "否", "", "", "背压比例 0~1"),
            ("run_key", "varchar(64)", "是", "", "", "运行批次标识（submittedAt 派生），新运行清旧点"),
            ("status", "varchar(16)", "是", "", "", "采样时的作业状态快照"),
        ],
        indexes=[],
        notes=[
            "保留策略：每个作业只保留最近一次运行（runKey）的点，重新提交即清理旧行，避免无界增长。",
            "前端删除单条趋势会同时清内存缓冲与库内该作业的点（DELETE /api/monitor/trends/{jobId}）。",
        ],
    ),
    dict(
        table="alert_record", cn="告警记录表", module="监控与告警",
        desc="告警中心数据源：作业失败、重启超限、吞吐归零、高背压、Checkpoint 失败、调度失败、上线停止等。",
        cols=[
            ("id", "bigint", "否", "P", "", "主键，自增"),
            ("level", "varchar(255)", "否", "", "", "级别 INFO / WARN / CRITICAL"),
            ("event", "varchar(255)", "否", "", "", "事件码（JOB_FAILED / THROUGHPUT_ZERO / ALERT_RESOLVED …）"),
            ("job_id", "bigint", "是", "F", "", "关联作业 → job_definition.id（系统级告警为空）"),
            ("job_name", "varchar(255)", "是", "", "", "作业名（冗余）"),
            ("owner_id", "bigint", "是", "F,I", "", "归属用户 → app_user.id（告警可见性隔离）"),
            ("message", "text", "否", "", "", "告警正文"),
            ("read_flag", "tinyint(1)", "是", "I", "0", "是否已读"),
            ("created_at", "datetime(6)", "是", "I", "", "产生时间"),
        ],
        indexes=["INDEX idx_alert_owner_created(owner_id, created_at)", "INDEX idx_alert_read_created(read_flag, created_at)"],
        notes=[
            "两个复合索引分别支撑「按归属人分页拉取告警」与「未读计数 / 已读筛选」。",
            "保留策略：AlertRetentionService 每 6h 清理，默认保留 30 天且最多 5000 条（可配置，0 = 不限制）。",
            "去重不靠唯一约束：离散事件按故障发生时间与最近一条告警比较，持续状态用 15 分钟冷却。",
        ],
    ),
    dict(
        table="diagnosis_report", cn="诊断报告表", module="监控与告警",
        desc="智能诊断结果快照：本地规则引擎或大模型给出的根因、证据与一键修复建议。",
        cols=[
            ("id", "bigint", "否", "P", "", "主键，自增"),
            ("job_id", "bigint", "是", "F", "", "关联作业 → job_definition.id"),
            ("owner_id", "bigint", "是", "F", "", "归属用户 → app_user.id"),
            ("job_name", "varchar(64)", "是", "", "", "作业名（冗余）"),
            ("trigger", "varchar(64)", "是", "", "", "触发方式：manual 或告警事件码（自动诊断）"),
            ("source", "varchar(64)", "是", "", "", "结论来源：rule / 模型名 / local-fallback"),
            ("root_cause", "varchar(1000)", "是", "", "", "根因"),
            ("evidence", "varchar(4000)", "是", "", "", "证据（日志片段、指标、配置）"),
            ("suggestions", "varchar(4000)", "是", "", "", "建议列表（JSON 数组字符串）"),
            ("param_fixes", "text", "是", "", "", "一键修复参数（JSON：节点 id → 参数修正值）"),
            ("packet", "text", "是", "", "", "诊断入参快照（已脱敏，密码替换为 ******）"),
            ("created_at", "datetime(6)", "是", "", "", "生成时间"),
        ],
        indexes=[],
        notes=["大字段截断设计：根因/证据/建议限制长度，完整报文只存脱敏快照，避免库膨胀。"],
    ),
    dict(
        table="audit_log", cn="操作审计表", module="安全与权限",
        desc="所有写操作的审计留痕（登录、作业增删改、上下线、调度、用户管理、AI 配置变更等）。",
        cols=[
            ("id", "bigint", "否", "P", "", "主键，自增"),
            ("user_id", "bigint", "是", "F", "", "操作人 → app_user.id（登录失败时为 NULL）"),
            ("username", "varchar(255)", "是", "", "", "操作人登录名（冗余，账号删除后仍可追溯）"),
            ("action", "varchar(255)", "否", "", "", "动作码（JOB_CREATE / JOB_SUBMIT / LOGOUT / USER_DELETE …）"),
            ("target_type", "varchar(255)", "是", "", "", "目标类型 JOB / USER / CONTROL 等"),
            ("target_id", "varchar(255)", "是", "", "", "目标标识"),
            ("detail", "text", "是", "", "", "详情"),
            ("ip", "varchar(255)", "是", "", "", "来源 IP"),
            ("created_at", "datetime(6)", "是", "", "", "发生时间"),
        ],
        indexes=[],
        notes=["审计由 AOP 切面（AuditAspect）自动写入，与业务解耦；查询接口仅 ADMIN/OPERATOR 可用。"],
    ),
    dict(
        table="control_registry", cn="控件注册表", module="平台能力",
        desc="内置数据源/转换/输出控件的元数据（当前 31 个），驱动画布控件库与 DAG → SQL 翻译。",
        cols=[
            ("id", "bigint", "否", "P", "", "主键，自增"),
            ("type", "varchar(255)", "否", "U", "", "控件类型标识（csv_input / mysql_output / field_concat …）"),
            ("name", "varchar(255)", "否", "", "", "显示名称"),
            ("category", "varchar(255)", "否", "", "", "分类 input / output / transform"),
            ("description", "text", "是", "", "", "控件说明（前端悬停提示）"),
            ("param_schema", "text", "是", "", "", "参数 JSON Schema（前端动态渲染参数面板）"),
            ("flink_template", "text", "是", "", "", "Flink SQL 模板（可选，翻译层按需使用）"),
            ("version", "varchar(255)", "否", "", "", "控件版本"),
            ("jar_path", "varchar(255)", "否", "", "", "控件 JAR 路径（内置控件为 built-in）"),
            ("enabled", "tinyint(1)", "是", "", "1", "是否启用"),
            ("created_at", "datetime(6)", "是", "", "", "创建时间"),
            ("updated_at", "datetime(6)", "是", "", "", "更新时间"),
        ],
        indexes=["UNIQUE(type)"],
        notes=[
            "启动时由 DataInitializer 清空重建（只读种子数据），保证与代码内注册表一致。",
            "前端另有一份降级副本 getBuiltinControls()，由前端测试校验前后端类型清单一致。",
        ],
    ),
    dict(
        table="data_source_connection", cn="数据源连接表", module="平台能力",
        desc="可复用的数据源连接配置：DAG 节点可引用它，凭据加密落盘、按创建人隔离。",
        cols=[
            ("id", "bigint", "否", "P", "", "主键，自增"),
            ("name", "varchar(128)", "否", "U", "", "连接名（同一创建人下唯一）"),
            ("type", "enum(MYSQL/POSTGRESQL/ORACLE/KAFKA/REDIS/HDFS)", "否", "", "", "数据源类型"),
            ("description", "text", "是", "", "", "说明"),
            ("config_json", "text", "否", "", "", "连接配置（host/port/db 等非敏感项，JSON）"),
            ("encrypted_credentials", "text", "是", "", "", "凭据密文（AES-GCM，密钥不入库）"),
            ("credential_configured", "tinyint(1)", "否", "", "0", "是否已配置凭据（前端据此显示状态）"),
            ("enabled", "tinyint(1)", "否", "", "1", "是否启用"),
            ("owner_id", "bigint", "否", "F,I", "", "创建人 → app_user.id（资源隔离）"),
            ("owner_name", "varchar(128)", "否", "", "", "创建人名称"),
            ("version", "bigint", "是", "", "", "乐观锁版本（@Version，防并发覆盖）"),
            ("created_at", "datetime(6)", "否", "I", "", "创建时间"),
            ("updated_at", "datetime(6)", "否", "", "", "更新时间"),
        ],
        indexes=[
            "UNIQUE(owner_id, name)",
            "INDEX idx_data_source_owner(owner_id)",
            "INDEX idx_data_source_updated(updated_at)",
        ],
        notes=[
            "凭据加密：DataSourceSecretCipher 用 AES-GCM，密钥取自 DATA_SOURCE_ENCRYPTION_KEY（或 APP/AI 配置密钥），"
            "缺省时使用 backend/data/.data_source_key 本机密钥文件——密钥与库分离，导出数据库也无法解密。",
            "version 列实现乐观锁，避免两个请求同时改同一连接时后写覆盖先写。",
        ],
    ),
]

RELATIONS = [
    ("app_user", "job_definition", "1 : N", "job_definition.owner_id", "创建人；作业按创建人隔离（非管理员仅见自己的作业）"),
    ("app_user", "data_source_connection", "1 : N", "data_source_connection.owner_id", "数据源归属；连接信息仅创建人与管理员可见"),
    ("app_user", "audit_log", "1 : N", "audit_log.user_id", "审计归属；登录失败时为 NULL"),
    ("app_user", "alert_record", "1 : N", "alert_record.owner_id", "告警归属；历史告警启动时按作业归属回填"),
    ("job_definition", "job_log", "1 : N", "job_log.job_id", "运行日志；删除作业不删日志（保留证据）"),
    ("job_definition", "job_version", "1 : N", "job_version.job_id", "版本快照；删除作业时联删"),
    ("job_definition", "schedule_history", "1 : N", "schedule_history.job_id", "调度触发历史"),
    ("job_definition", "trend_point", "1 : N", "trend_point.job_id", "趋势点；每作业只保留最近一次运行"),
    ("job_definition", "alert_record", "1 : N", "alert_record.job_id", "作业级告警；系统级告警 job_id 为空"),
    ("job_definition", "diagnosis_report", "1 : N", "diagnosis_report.job_id", "诊断报告（手动/自动）"),
    ("job_definition", "job_dependency", "1 : N", "job_dependency.downstream_job_id", "依赖边：上游 → 下游"),
    ("job_definition", "job_dependency", "1 : N", "job_dependency.upstream_job_id", "依赖边：下游 → 上游（同一表两条关系，构成 N:M）"),
]

STATUS_FLOW = [
    ("DRAFT", "画布保存后的初始态", "提交 / 定时触发 / 依赖触发 → SUBMITTED"),
    ("SUBMITTED", "已提交 Flink，等待运行", "Flink 状态回写 RUNNING / FAILED / COMPLETED"),
    ("RUNNING", "集群中运行中", "完成 → COMPLETED；异常 → FAILED；取消 → CANCELLED"),
    ("WAITING", "依赖编排：等待上游完成", "上游全部 COMPLETED → 自动提交为 SUBMITTED"),
    ("BLOCKED", "依赖编排：上游失败/取消", "需人工介入或重新触发上游"),
    ("COMPLETED", "运行成功（触发输出后处理）", "可重跑、可调度、可上线"),
    ("FAILED", "失败（提交期或运行期）", "秒级事件驱动告警 + 可一键诊断"),
    ("CANCELLED", "已取消/已下线（触发输出合并）", "可重新提交"),
]

ALERT_EVENTS = [
    ("JOB_FAILED", "CRITICAL", "作业失败（提交期或运行期），事件驱动秒级触发"),
    ("ONLINE_JOB_STOPPED", "CRITICAL", "上线作业异常且 10 分钟窗口重启超限，自动下线"),
    ("THROUGHPUT_ZERO", "WARN", "运行中吞吐为 0 持续 2 分钟"),
    ("BACKPRESSURE_HIGH", "WARN", "算子背压持续 60 秒"),
    ("CHECKPOINT_FAILED", "WARN", "Checkpoint 失败"),
    ("RESOURCE_HIGH", "WARN", "CPU/内存/磁盘超阈值持续 2 分钟（系统级，job_id 为空）"),
    ("LOG_ERROR", "WARN", "运行中作业日志出现 ERROR"),
    ("CLUSTER_HEARTBEAT_LOST", "CRITICAL", "JobManager/TaskManager 心跳丢失"),
    ("JOB_HEARTBEAT_LOST", "CRITICAL", "RUNNING 作业从集群消失"),
    ("SCHEDULE_FAILED", "WARN", "定时调度重试超限"),
    ("DEPENDENCY_BLOCKED", "WARN", "上游失败导致下游阻塞"),
    ("ALERT_RESOLVED", "INFO", "故障恢复闭环通知（只落库 + webhook，不发邮件）"),
]


# ---------------------------------------------------------------- ER 图
def draw_er(path):
    """ER 图：四列布局 + 左右 FK 总线；方框高度按内容计算，列间留连线走廊。"""
    W, H = 1900, 1420
    img = Image.new("RGB", (W, H), "#ffffff")
    d = ImageDraw.Draw(img)
    f_title = ImageFont.truetype(CJK_FONT_BOLD, 38)
    f_sub = ImageFont.truetype(CJK_FONT, 19)
    f_box = ImageFont.truetype(CJK_FONT_BOLD, 22)
    f_col = ImageFont.truetype(CJK_FONT, 19)
    f_rel = ImageFont.truetype(CJK_FONT, 18)
    f_legend = ImageFont.truetype(CJK_FONT, 19)

    d.text((40, 26), "数据流任务管理平台 —— 数据库 ER 图（12 张表）", font=f_title, fill="#1f2a44")
    d.text((40, 76), "实线=外键逻辑关系（1:N，服务层维护，无物理外键）；job_dependency 为作业间 N:M 依赖边",
           font=f_sub, fill="#5a6478")

    LINE_H, HEAD_H, PAD = 29, 44, 12

    def box_h(cols):
        return HEAD_H + LINE_H * len(cols) + PAD

    def box(x, y, w, title, cols, accent):
        hh = box_h(cols)
        d.rounded_rectangle([x, y, x + w, y + hh], radius=10, fill="#f8fafc", outline=accent, width=3)
        d.rounded_rectangle([x, y, x + w, y + HEAD_H], radius=10, fill=accent)
        d.rectangle([x, y + HEAD_H - 14, x + w, y + HEAD_H], fill=accent)
        # 标题过长时自动缩小字号，避免被方框裁掉
        tfont = f_box
        if d.textlength(title, font=f_box) > w - 24:
            tfont = ImageFont.truetype(CJK_FONT_BOLD, 17)
        d.text((x + 12, y + (10 if tfont is f_box else 13)), title, font=tfont, fill="#ffffff")
        yy = y + HEAD_H + 6
        for c in cols:
            d.text((x + 14, yy), c, font=f_col, fill="#2d3748")
            yy += LINE_H
        return dict(x=x, y=y, w=w, h=hh, cx=x + w // 2, cy=y + hh // 2)

    def stack(entries, x, w, start_y, gap=34):
        out, y = [], start_y
        for title, cols, accent in entries:
            out.append(box(x, y, w, title, cols, accent))
            y += box_h(cols) + gap
        return out

    def label(text, x, y, color):
        if not text:
            return
        tw = 10 * len(text) + 16
        d.rectangle([x, y - 3, x + tw, y + 22], fill="#ffffff")
        d.text((x + 6, y), text, font=f_rel, fill=color)

    def bus_right(src, bus_x, targets, color, tag="1 : N"):
        d.line([src["x"] + src["w"], src["cy"], bus_x, src["cy"]], fill=color, width=3)
        ys = [t["cy"] for t in targets] + [src["cy"]]
        d.line([bus_x, min(ys), bus_x, max(ys)], fill=color, width=3)
        for t in targets:
            d.line([bus_x, t["cy"], t["x"], t["cy"]], fill=color, width=3)
            if tag:
                label(tag, bus_x + 10, t["cy"] - 12, color)

    # ---- 列坐标（列间走廊用于连线与基数标注）
    C1, W1 = 180, 330
    C2, W2 = 600, 420
    C3, W3 = 1160, 360
    C4, W4 = 1580, 300
    TOP = 150
    GREEN, ORANGE, GRAY, PINK, PURPLE = "#2f855a", "#c05621", "#4a5568", "#b83280", "#6b46c1"

    # ---- 第 1 列：安全与平台
    col1 = stack([
        ("app_user 用户表", ["PK id", "UK username", "role / enabled", "token_version", "last_login/seen_at"], GREEN),
        ("data_source_connection 数据源", ["PK id", "UK (owner_id,name)", "type / config_json",
                                           "encrypted_credentials", "version（乐观锁）"], GREEN),
        ("audit_log 审计日志", ["PK id", "FK user_id", "action / target_type", "detail / ip", "created_at"], GREEN),
        ("control_registry 控件注册表", ["PK id", "UK type", "category", "param_schema", "flink_template"], PURPLE),
    ], C1, W1, TOP)
    b_user, b_ds, b_audit, b_ctrl = col1

    # ---- 第 2 列：核心作业表
    b_job = box(C2, TOP, W2, "job_definition 作业定义（核心表）",
                ["PK id", "name / description", "dag_json（画布 DAG 全量）", "status（8 态状态机）",
                 "flink_job_id / parallelism", "FK owner_id → app_user", "source / confirmation_status",
                 "cron_expression / schedule_*", "online / online_restart_count",
                 "submitted_at / completed_at"], ORANGE)

    # ---- 第 3 列：作业从表
    col3 = stack([
        ("job_log 作业日志", ["PK id", "FK job_id", "level", "message(TEXT)", "timestamp"], GRAY),
        ("job_version 版本快照", ["PK id", "FK job_id", "version_no", "dag_json（快照）", "created_at"], GRAY),
        ("job_dependency 依赖边（N:M）", ["PK id", "FK upstream_job_id", "FK downstream_job_id",
                                          "UK(upstream, downstream)", "created_at"], GRAY),
        ("schedule_history 调度历史", ["PK id", "FK job_id", "trigger_time", "status", "message / flink_job_id"], GRAY),
        ("trend_point 监控趋势点", ["PK id", "FK job_id", "t（epoch ms）", "input_rate / output_rate",
                                    "bp / run_key / status"], GRAY),
    ], C3, W3, TOP, gap=30)

    # ---- 第 4 列：告警与诊断
    col4 = stack([
        ("alert_record 告警记录", ["PK id", "FK job_id", "FK owner_id", "level / event", "read_flag / created_at"], PINK),
        ("diagnosis_report 诊断报告", ["PK id", "FK job_id", "trigger / source", "root_cause / evidence",
                                       "param_fixes / packet"], PINK),
    ], C4, W4, TOP + 130, gap=70)
    b_alert, b_diag = col4

    # ---- 关系连线
    # 1) app_user → data_source / audit / alert（左侧 FK 总线）
    bus_x = 70
    d.line([b_user["x"], b_user["cy"], bus_x, b_user["cy"]], fill=GREEN, width=3)
    d.line([bus_x, b_user["cy"], bus_x, b_alert["y"] - 50], fill=GREEN, width=3)
    for t in (b_ds, b_audit):
        d.line([bus_x, t["cy"], t["x"], t["cy"]], fill=GREEN, width=3)
        label("1 : N", bus_x + 10, t["cy"] - 12, GREEN)
    label("1 : N", bus_x + 10, b_user["cy"] - 12, GREEN)
    d.line([bus_x, b_alert["y"] - 50, b_alert["cx"], b_alert["y"] - 50], fill=GREEN, width=3)
    d.line([b_alert["cx"], b_alert["y"] - 50, b_alert["cx"], b_alert["y"]], fill=GREEN, width=3)
    label("1 : N", b_alert["cx"] - 88, b_alert["y"] - 46, GREEN)

    # 2) app_user → job_definition
    d.line([b_user["x"] + b_user["w"], b_job["cy"], b_job["x"], b_job["cy"]], fill=GREEN, width=3)
    label("1 : N", C1 + W1 + 16, b_job["cy"] - 12, GREEN)

    # 3) job_definition → 作业从表（第 3 列）
    bus_right(b_job, 1090, col3, ORANGE)

    # 4) job_definition → alert_record / diagnosis_report（第 4 列，走廊窄，基数见 3.2）
    bus_right(b_job, 1550, [b_alert, b_diag], PINK, tag=None)

    # 5) alert_record → diagnosis_report（告警事件触发自动诊断）
    y_mid = (b_alert["y"] + b_alert["h"] + b_diag["y"]) // 2
    d.line([b_alert["cx"], b_alert["y"] + b_alert["h"], b_alert["cx"], y_mid], fill=PINK, width=3)
    d.line([b_alert["cx"], y_mid, b_diag["x"] - 26, y_mid], fill=PINK, width=3)
    d.line([b_diag["x"] - 26, y_mid, b_diag["x"] - 26, b_diag["y"]], fill=PINK, width=3)
    label("事件触发", b_alert["cx"] + 8, y_mid - 12, PINK)

    # 6) control_registry 无外键：仅被翻译层读取（控件元数据 → DAG 转 SQL）
    d.line([b_ctrl["x"] + b_ctrl["w"], b_ctrl["cy"], b_job["x"], b_ctrl["cy"]], fill=PURPLE, width=3)
    label("读取", C1 + W1 + 16, b_ctrl["cy"] - 12, PURPLE)

    # ---- 图例
    d.rectangle([40, H - 66, W - 40, H - 22], fill="#edf2f7")
    d.text((56, H - 58), "图例：PK 主键　UK 唯一约束　FK 逻辑外键（服务层维护）　1:N 一对多　"
                         "实线=外键从属关系　job_dependency 同表两条边实现作业间 N:M 依赖",
           font=f_legend, fill="#2d3748")
    img.save(path)
    return path


# ---------------------------------------------------------------- Word 文档
def set_cjk(run, name="微软雅黑"):
    run.font.name = name
    run._element.rPr.rFonts.set(qn("w:eastAsia"), name)


def h(doc, text, level):
    p = doc.add_heading(level=level)
    run = p.add_run(text)
    set_cjk(run)
    run.font.color.rgb = RGBColor(0x1F, 0x2A, 0x44)
    return p


def para(doc, text, bold=False, size=10.5, align=None):
    p = doc.add_paragraph()
    run = p.add_run(text)
    run.bold = bold
    run.font.size = Pt(size)
    set_cjk(run)
    if align:
        p.alignment = align
    return p


def bullet(doc, text):
    p = doc.add_paragraph(style="List Bullet")
    run = p.add_run(text)
    run.font.size = Pt(10.5)
    set_cjk(run)
    return p


def make_table(doc, headers, rows, widths=None):
    t = doc.add_table(rows=1, cols=len(headers))
    t.style = "Light Grid Accent 1"
    t.alignment = WD_TABLE_ALIGNMENT.CENTER
    hdr = t.rows[0].cells
    for i, htxt in enumerate(headers):
        hdr[i].text = ""
        run = hdr[i].paragraphs[0].add_run(htxt)
        run.bold = True
        run.font.size = Pt(9.5)
        set_cjk(run)
    for row in rows:
        cells = t.add_row().cells
        for i, val in enumerate(row):
            cells[i].text = ""
            run = cells[i].paragraphs[0].add_run(str(val))
            run.font.size = Pt(9)
            set_cjk(run)
    if widths:
        for r in t.rows:
            for i, w in enumerate(widths):
                r.cells[i].width = Cm(w)
    return t


def build_doc():
    doc = Document()
    style = doc.styles["Normal"]
    style.font.size = Pt(10.5)
    style.font.name = "微软雅黑"
    style.element.rPr.rFonts.set(qn("w:eastAsia"), "微软雅黑")
    for s in doc.sections:
        s.left_margin = s.right_margin = Cm(2.2)
        s.top_margin = s.bottom_margin = Cm(2.0)

    # ---------- 封面
    for _ in range(3):
        doc.add_paragraph()
    para(doc, "数据库设计说明书", bold=True, size=30, align=WD_ALIGN_PARAGRAPH.CENTER)
    doc.add_paragraph()
    para(doc, "可视化数据流任务管理平台（DataStream MVP）", size=15, align=WD_ALIGN_PARAGRAPH.CENTER)
    para(doc, "ER 设计 · 表结构设计 · 索引与约束 · DDL 附录", size=12, align=WD_ALIGN_PARAGRAPH.CENTER)
    for _ in range(5):
        doc.add_paragraph()
    make_table(doc,
               ["项目", "内容"],
               [["文档名称", "数据库设计说明书"],
                ["适用系统", "可视化数据流任务管理平台（画布编排 → Flink SQL → Flink 集群 → 输出/监控/告警）"],
                ["数据库版本", "H2 2.x（开发默认，file 模式）/ MySQL 8.x（生产，InnoDB + utf8mb4）"],
                ["表数量", "12 张（作业编排 5、监控告警 3、安全权限 2、平台能力 2）"],
                ["文档版本", "V1.0"],
                ["编写日期", "2026-09-15"],
                ["事实来源", "JPA 实体源码 + Hibernate 导出的真实 DDL（附录 A / B）"]],
               widths=[3.5, 13.0])
    doc.add_page_break()

    # ---------- 1 文档说明
    h(doc, "1. 文档说明", 1)
    h(doc, "1.1 目的与范围", 2)
    para(doc, "本文档描述可视化数据流任务管理平台的数据库设计，包含：实体关系（ER）设计、12 张表的完整表结构、"
              "索引与约束设计、关键字段的设计取舍，以及 H2 / MySQL 两种部署形态的建表 DDL。"
              "范围覆盖元数据库（作业、版本、日志、调度、趋势、告警、诊断、用户、审计、控件注册表、数据源连接）"
              "的全部持久化结构；不包含 Flink 集群自身的状态存储，也不包含被平台读写的业务目标库（MySQL/PG/Oracle）结构。")
    h(doc, "1.2 设计依据", 2)
    bullet(doc, "以代码为唯一事实来源：实体类 backend/src/main/java/com/datastream/mvp/model/*.java（JPA 注解）。")
    bullet(doc, "表结构以 Hibernate 依据实体自动生成的真实 DDL 为准（docs/schema-h2.sql、docs/schema-mysql.sql），"
                "开发环境 ddl-auto=update 自动建表与加列。")
    bullet(doc, "命名与字段语义与 DEVELOPMENT.md 的框架约定保持一致（DAG 契约、RBAC 归属、告警事件码等）。")
    h(doc, "1.3 术语", 2)
    make_table(doc, ["术语", "说明"],
               [["作业（Job）", "一次数据流任务的完整定义与运行记录，DAG 由画布编排生成"],
                ["DAG", "有向无环图，节点=控件实例（输入/转换/输出），边=数据流向"],
                ["控件（Control）", "可拖拽的数据源/转换/输出单元，元数据存 control_registry"],
                ["runKey", "一次运行的批次标识，用于趋势点按批次隔离"],
                ["逻辑外键", "表间引用关系仅由服务层维护，数据库层不建 FOREIGN KEY 约束"]],
               widths=[3.5, 13.0])

    # ---------- 2 总体设计
    h(doc, "2. 数据库总体设计", 1)
    h(doc, "2.1 选型与部署", 2)
    make_table(doc, ["环境", "数据库", "连接方式", "说明"],
               [["开发/演示（默认）", "H2 2.x", "jdbc:h2:file:./data/mvpdb",
                 "零安装启动，sa 空密码；H2 Web 控制台默认关闭（H2_CONSOLE_ENABLED=true 且仅 ADMIN 可访问）"],
                ["生产（推荐）", "MySQL 8.x", "jdbc:mysql://host:3306/mvp_backend",
                 "profile=mysql；InnoDB + utf8mb4（中文无乱码）"],
                ["容器化", "MySQL 8（compose）", "宿主 3307 → 容器 3306", "docker-compose 一键起全栈"]],
               widths=[3.6, 3.0, 5.0, 5.0])
    para(doc, "Hibernate 方言随数据源自动切换（H2Dialect / MySQLDialect），实体层无需改动；"
              "两种形态表结构一致，差异仅在物理类型（见 2.2 与附录）。")
    h(doc, "2.2 命名与类型约定", 2)
    make_table(doc, ["约定项", "规则", "示例"],
               [["表名", "小写下划线，单数名词", "job_definition、alert_record"],
                ["列名", "Java 驼峰自动转小写下划线（Spring Boot 默认策略）", "ownerId → owner_id"],
                ["主键", "自增 bigint，名统一为 id", "id bigint"],
                ["时间", "datetime(6) / timestamp(6)，由应用侧写入（LocalDateTime）", "created_at、completed_at"],
                ["布尔", "MySQL bit、H2 boolean", "enabled、read_flag"],
                ["枚举", "MySQL enum(...)、H2 varchar + check 约束，存字符串不存序号", "status、role、source"],
                ["大文本", "TEXT（DAG JSON、日志正文、诊断证据、模板等）", "dag_json、message"],
                ["浮点", "double（float(53)），用于速率与背压比例", "output_rate、bp"],
                ["保留字规避", "列名避开 SQL 保留字：入/出速率用 input_rate/output_rate，趋势时间用 t", "input_rate、t"]],
               widths=[2.8, 8.2, 5.5])
    h(doc, "2.3 表清单", 2)
    make_table(doc, ["#", "表名", "中文名", "所属模块", "作用", "量级预估"],
               [["1", "job_definition", "作业定义表", "作业编排", "作业主数据 + 状态机 + 调度 + 上线 + AI 归属", "千级"],
                ["2", "job_log", "作业日志表", "作业编排", "状态流转与运行期日志", "十万级"],
                ["3", "job_version", "作业版本表", "作业编排", "保存/回滚历史快照", "万级"],
                ["4", "job_dependency", "作业依赖表", "作业编排", "跨作业依赖边（N:M）", "千级"],
                ["5", "schedule_history", "调度历史表", "作业编排", "定时触发执行记录", "万级"],
                ["6", "trend_point", "监控趋势点表", "监控告警", "吞吐/背压时间序列", "万级（按批次滚动清理）"],
                ["7", "alert_record", "告警记录表", "监控告警", "故障/恢复事件与通知状态", "万级（保留 30 天/5000 条）"],
                ["8", "diagnosis_report", "诊断报告表", "监控告警", "智能诊断结论与一键修复建议", "千级"],
                ["9", "app_user", "用户表", "安全权限", "账号、角色、在线状态、令牌版本", "百级"],
                ["10", "audit_log", "操作审计表", "安全权限", "写操作留痕", "万级"],
                ["11", "control_registry", "控件注册表", "平台能力", "控件元数据（驱动画布与翻译）", "百级（启动重建）"],
                ["12", "data_source_connection", "数据源连接表", "平台能力", "可复用连接配置 + 加密凭据", "百级"]],
               widths=[1.0, 4.2, 2.8, 2.2, 4.6, 2.4])
    h(doc, "2.4 引用完整性设计（无物理外键）", 2)
    para(doc, "全部表间引用使用 bigint 外键列 + 服务层维护，未创建数据库级 FOREIGN KEY 约束，原因与配套措施：")
    bullet(doc, "作业/用户删除需要保留历史证据（日志、审计、诊断报告），物理外键的级联或 RESTRICT 会与之冲突。")
    bullet(doc, "删除作业时由 JobService.delete 显式处理：联删 job_version 与 job_dependency 边，"
                "并清理监控内存态；日志/告警/诊断保留供追溯。")
    bullet(doc, "无物理外键便于后续按业务域垂直拆分（作业域 / 监控域），不需要先解约束。")
    bullet(doc, "代价是引用一致性依赖服务层：所有写路径统一经过 Service 层，并在启动时做历史数据回填"
                "（如历史作业/告警的 owner_id）。")
    doc.add_page_break()

    # ---------- 3 ER
    h(doc, "3. ER 设计", 1)
    h(doc, "3.1 ER 图", 2)
    para(doc, "图 3-1 实体关系图（外键逻辑关系、基数与依赖方向；详细语义见 3.2）")
    doc.add_picture(ER_PNG, width=Cm(16.6))
    doc.paragraphs[-1].alignment = WD_ALIGN_PARAGRAPH.CENTER
    h(doc, "3.2 实体关系说明", 2)
    make_table(doc, ["主实体", "从实体", "基数", "关联列", "关系说明"],
               [[a, b, c, d, e] for (a, b, c, d, e) in RELATIONS],
               widths=[3.0, 3.4, 1.4, 4.4, 4.6])
    h(doc, "3.3 作业状态机（job_definition.status）", 2)
    make_table(doc, ["状态", "含义", "流转条件 / 去向"],
               [[s, m, t] for (s, m, t) in STATUS_FLOW], widths=[2.6, 6.0, 8.0])
    h(doc, "3.4 告警事件字典（alert_record.event）", 2)
    make_table(doc, ["事件码", "级别", "触发条件"],
               [[c, lv, ds] for (c, lv, ds) in ALERT_EVENTS], widths=[4.6, 2.2, 9.8])
    doc.add_page_break()

    # ---------- 4 表结构
    h(doc, "4. 表结构设计", 1)
    para(doc, "以下按模块列出 12 张表的完整结构。类型列为口径统一后的逻辑类型（实际物理类型见附录 B）；"
              "「键」列中 P=主键、F=逻辑外键、U=唯一约束、I=索引列。")
    order = ["作业编排", "监控与告警", "安全与权限", "平台能力"]
    for mi, mod in enumerate(order, start=1):
        h(doc, f"4.{mi} {mod}模块", 2)
        for ti, tbl in enumerate([t for t in SCHEMA if t["module"] == mod], start=1):
            h(doc, f"4.{mi}.{ti} 表 {tbl['table']}（{tbl['cn']}）", 3)
            para(doc, tbl["desc"])
            make_table(doc, ["字段名", "类型", "可空", "键", "默认值", "说明"],
                       [[c[0], c[1], c[2], c[3], c[4], c[5]] for c in tbl["cols"]],
                       widths=[3.4, 4.4, 1.2, 1.0, 1.8, 4.8])
            if tbl["indexes"]:
                para(doc, "索引与约束：", bold=True, size=10)
                for it in tbl["indexes"]:
                    bullet(doc, it)
            if tbl["notes"]:
                para(doc, "设计说明：", bold=True, size=10)
                for nt in tbl["notes"]:
                    bullet(doc, nt)
    doc.add_page_break()

    # ---------- 5 索引与约束汇总
    h(doc, "5. 索引与约束汇总", 1)
    make_table(doc, ["表", "索引 / 约束", "列", "支撑的查询场景"],
               [["app_user", "UNIQUE", "username", "登录鉴权按用户名精确查找"],
                ["control_registry", "UNIQUE", "type", "按控件类型取元数据 / 启动重建去重"],
                ["job_dependency", "UNIQUE", "upstream_job_id, downstream_job_id", "防止重复依赖边"],
                ["data_source_connection", "UNIQUE", "owner_id, name", "同一创建人下连接名唯一"],
                ["data_source_connection", "INDEX idx_data_source_owner", "owner_id", "按创建人列出自己的数据源"],
                ["data_source_connection", "INDEX idx_data_source_updated", "updated_at", "按更新时间倒序分页"],
                ["alert_record", "INDEX idx_alert_owner_created", "owner_id, created_at", "按归属人分页拉取告警列表"],
                ["alert_record", "INDEX idx_alert_read_created", "read_flag, created_at", "未读计数与已读筛选"]],
               widths=[4.4, 5.4, 4.6, 5.2])
    para(doc, "未建索引的说明：job_log / trend_point / schedule_history 按 job_id 等值过滤后再小范围排序，"
              "当前数据量下顺序扫描成本可接受；若日志量继续增长，建议补 (job_id, timestamp) 与 (job_id, t) 复合索引。")

    # ---------- 6 关键设计说明
    h(doc, "6. 关键设计说明", 1)
    make_table(doc, ["主题", "设计", "理由"],
               [["DAG 存储", "job_definition.dag_json 存整图 JSON，不拆节点/边表",
                 "控件类型与参数随注册表演进，JSON 免 DDL 变更；图规模小，翻译层一次性解析"],
                ["版本快照", "job_version 全量快照 dag_json",
                 "回滚需要精确复原历史配置，差量存储会引入合并复杂度"],
                ["趋势数据", "内存环形缓冲（热路径）+ trend_point 落库（冷数据）",
                 "2s 采样全量落库压力大；只保留最近一次运行的点，重启后仍可恢复曲线"],
                ["告警去重", "无唯一索引，靠 DB 时间比较（离散事件）+ 15 分钟冷却（持续状态）",
                 "故障发生时间可回溯，且需支持「重跑同一作业再次告警」，唯一索引会误抑制"],
                ["告警保留", "AlertRetentionService 定时清理（30 天 / 5000 条，可配）",
                 "告警是运维快照而非账本，长期保留会拖慢分页查询与未读统计"],
                ["凭据安全", "encrypted_credentials 存 AES-GCM 密文，密钥在环境变量或本机密钥文件",
                 "数据库导出/备份不泄露凭据；密钥与数据分离，容器化可用 Secret 注入"],
                ["令牌吊销", "app_user.token_version + JWT claim tv",
                 "无状态 JWT 无法逐 token 吊销，用版本号实现改密/登出后「全端下线」"],
                ["资源隔离", "owner_id 冗余在作业、数据源、告警、诊断等业务表",
                 "列表查询与分页直接按 owner_id 过滤，避免多表 JOIN；非管理员仅见本人数据"],
                ["AI 内容治理", "job_definition.source / confirmation_status / confirmed_by*",
                 "AI 生成的作业必须人工确认后才能提交，状态由服务端推导，客户端无法伪造"]],
               widths=[2.6, 6.6, 6.6])

    # ---------- 附录
    doc.add_page_break()
    h(doc, "附录 A　H2 建表 DDL（开发/演示库，自动生成）", 1)
    para(doc, "来源：Hibernate schema-generation（H2Dialect）。开发环境由 ddl-auto=update 自动建表，本脚本仅作文档留档。")
    for line in open(os.path.join(DOCS, "schema-h2.sql"), encoding="utf-8").read().strip().splitlines():
        p = doc.add_paragraph()
        run = p.add_run(line)
        run.font.size = Pt(7.5)
        run.font.name = "Consolas"
    doc.add_page_break()
    h(doc, "附录 B　MySQL 建表 DDL（生产库，自动生成）", 1)
    para(doc, "来源：Hibernate schema-generation（MySQLDialect）。生产库建议以本脚本初始化，字符集使用 utf8mb4。")
    for line in open(os.path.join(DOCS, "schema-mysql.sql"), encoding="utf-8").read().strip().splitlines():
        p = doc.add_paragraph()
        run = p.add_run(line)
        run.font.size = Pt(7.5)
        run.font.name = "Consolas"

    doc.save(DOCX)
    return DOCX


if __name__ == "__main__":
    os.makedirs(DOCS, exist_ok=True)
    draw_er(ER_PNG)
    out = build_doc()
    print("ER 图:", ER_PNG)
    print("说明书:", out)
    print("表数量:", len(SCHEMA), "关系数量:", len(RELATIONS))
