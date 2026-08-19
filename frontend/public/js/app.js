﻿﻿﻿﻿﻿﻿﻿// ===== API 配置 =====
const API_BASE = '/api';

const api = {
    async getControls(category) {
        const url = category ? `${API_BASE}/controls?category=${category}` : `${API_BASE}/controls`;
        const res = await axios.get(url);
        return res.data;
    },
    async getJobs() {
        const res = await axios.get(`${API_BASE}/jobs`);
        return res.data;
    },
    async getJob(id) {
        const res = await axios.get(`${API_BASE}/jobs/${id}`);
        return res.data;
    },
    async createJob(job) {
        const res = await axios.post(`${API_BASE}/jobs`, job);
        return res.data;
    },
    async updateJob(id, job) {
        const res = await axios.put(`${API_BASE}/jobs/${id}`, job);
        return res.data;
    },
    async deleteJob(id) {
        await axios.delete(`${API_BASE}/jobs/${id}`);
    },
    async submitJob(id) {
        const res = await axios.post(`${API_BASE}/jobs/${id}/submit`);
        return res.data;
    },
    async cancelJob(id) {
        await axios.post(`${API_BASE}/jobs/${id}/cancel`);
    },
    async onlineJob(id) {
        const res = await axios.post(`${API_BASE}/jobs/${id}/online`);
        return res.data;
    },
    async offlineJob(id) {
        const res = await axios.post(`${API_BASE}/jobs/${id}/offline`);
        return res.data;
    },
    async copyJob(id) {
        const res = await axios.post(`${API_BASE}/jobs/${id}/copy`);
        return res.data;
    },
    async updateSchedule(id, payload) {
        const res = await axios.post(`${API_BASE}/jobs/${id}/schedule`, payload);
        return res.data;
    },
    async getScheduleHistory(id) {
        const res = await axios.get(`${API_BASE}/jobs/${id}/schedule-history`);
        return res.data;
    },
    async getLogs(jobId) {
        const res = await axios.get(`${API_BASE}/jobs/${jobId}/logs`);
        return res.data;
    },
    async getVersions(jobId) {
        const res = await axios.get(`${API_BASE}/jobs/${jobId}/versions`);
        return res.data;
    },
    async rollbackJob(jobId, versionId) {
        const res = await axios.post(`${API_BASE}/jobs/${jobId}/rollback/${versionId}`);
        return res.data;
    },
    async previewFile(path, limit = 20) {
        const res = await axios.get(`${API_BASE}/preview/file`, { params: { path, limit } });
        return res.data;
    },
    async getMonitorOverview() {
        const res = await axios.get(`${API_BASE}/monitor/overview`);
        return res.data;
    },
    async getMonitorTrends() {
        const res = await axios.get(`${API_BASE}/monitor/trends`);
        return res.data;
    },
    async login(username, password) {
        const res = await axios.post(`${API_BASE}/auth/login`, { username, password });
        return res.data;
    },
    async me() {
        const res = await axios.get(`${API_BASE}/auth/me`);
        return res.data;
    },
    async logout() {
        await axios.post(`${API_BASE}/auth/logout`);
    },
    async getAudit(params) {
        const res = await axios.get(`${API_BASE}/audit`, { params });
        return res.data;
    },
    async getDependencies(jobId) {
        const res = await axios.get(`${API_BASE}/jobs/${jobId}/dependencies`);
        return res.data;
    },
    async saveDependencies(jobId, upstreamIds) {
        const res = await axios.put(`${API_BASE}/jobs/${jobId}/dependencies`, { upstreamIds });
        return res.data;
    },
    async getWorkflow() {
        const res = await axios.get(`${API_BASE}/jobs/workflow`);
        return res.data;
    },
    async getJobLineage(jobId) {
        const res = await axios.get(`${API_BASE}/lineage/job/${jobId}`);
        return res.data;
    },
    async aiNl2Pipeline(prompt) {
        const res = await axios.post(API_BASE + '/ai/nl2pipeline', { prompt });
        return res.data;
    },
    async aiDiagnose(jobId) {
        const res = await axios.post(API_BASE + '/ai/diagnose/' + jobId);
        return res.data;
    },
    async getAlerts() {
        const res = await axios.get(API_BASE + '/alerts');
        return res.data;
    },
    async getAlertUnread() {
        const res = await axios.get(API_BASE + '/alerts/unread-count');
        return res.data;
    },
    async markAlertRead(id) {
        await axios.post(API_BASE + '/alerts/' + id + '/read');
    },
    async markAllAlertsRead() {
        await axios.post(API_BASE + '/alerts/read-all');
    },
    async deleteAlert(id) {
        await axios.delete(API_BASE + '/alerts/' + id);
    }
};

// ===== 颜色工具 =====
const CATEGORY_COLORS = {
    input: { bg: '#e1f3d8', border: '#67c23a', label: '输入', icon: '📥' },
    transform: { bg: '#d9ecff', border: '#409eff', label: '转换', icon: '🔧' },
    output: { bg: '#fdf0d9', border: '#e6a23c', label: '输出', icon: '📤' }
};

// ===== 示例作业（一键导入画布）=====
const DEFAULT_OUTPUT_DIR = 'D:\\code\\比赛\\2026省服务外包\\output';
const SAMPLE_DAGS = {
    'datagen2csv': {
        jobName: '示例：Datagen → CSV',
        parallelism: 1,
        nodes: [
            { id: 'dg_1', type: 'datagen_input', label: 'Datagen', params: { rowsPerSecond: '50', fieldsConfig: '[{"name":"id","type":"INT"},{"name":"name","type":"STRING"},{"name":"score","type":"DOUBLE"}]' }, x: 120, y: 140 },
            { id: 'csv_out_1', type: 'csv_output', label: 'CSV 输出', params: { path: DEFAULT_OUTPUT_DIR + '\\sample_output.csv', hasHeader: 'true', delimiter: ',' }, x: 430, y: 140 }
        ],
        edges: [ { id: 'e1', source: 'dg_1', target: 'csv_out_1' } ]
    },
    'datagen2json': {
        jobName: '示例：Datagen → JSON',
        parallelism: 1,
        nodes: [
            { id: 'dg_1', type: 'datagen_input', label: 'Datagen', params: { rowsPerSecond: '50', fieldsConfig: '[{"name":"id","type":"INT"},{"name":"name","type":"STRING"}]' }, x: 120, y: 140 },
            { id: 'json_out_1', type: 'json_output', label: 'JSON 输出', params: { path: DEFAULT_OUTPUT_DIR + '\\sample_output.json', mode: 'lines' }, x: 430, y: 140 }
        ],
        edges: [ { id: 'e1', source: 'dg_1', target: 'json_out_1' } ]
    },
    'csv2csv_filter': {
        jobName: '示例：CSV → 字段过滤 → CSV',
        parallelism: 1,
        nodes: [
            { id: 'csv_in_1', type: 'csv_input', label: 'CSV 输入', params: { path: 'D:\\code\\比赛\\2026省服务外包\\test-resources\\data\\sales.csv', hasHeader: 'true', delimiter: ',' }, x: 120, y: 140 },
            { id: 'ff_1', type: 'field_filter', label: '字段过滤', params: { fields: 'sale_id,amount' }, x: 330, y: 140 },
            { id: 'csv_out_1', type: 'csv_output', label: 'CSV 输出', params: { path: DEFAULT_OUTPUT_DIR + '\\sample_filtered.csv', hasHeader: 'true', delimiter: ',' }, x: 540, y: 140 }
        ],
        edges: [ { id: 'e1', source: 'csv_in_1', target: 'ff_1' }, { id: 'e2', source: 'ff_1', target: 'csv_out_1' } ]
    }
};

// ===== Vue App =====
const { createApp, ref, reactive, computed, onMounted, watch, nextTick, toRaw } = Vue;
const { ElMessage, ElMessageBox } = ElementPlus;

const app = createApp({
    setup() {
        // 状态
        const currentView = ref('canvas');
        const controls = ref([]);
        const jobs = ref([]);
        const jobSearch = ref('');
        const jobStatusFilter = ref('');
        const jobCols = reactive({ description: false, parallelism: false, flinkJobId: false, updatedAt: false });
        const jobDetailVisible = ref(false);
        const jobDetailRow = ref(null);
        const jobName = ref('未命名作业');
        const parallelism = ref(1);
        const currentJobName = ref('');
        const controlTab = ref('input');
        const saving = ref(false);
        const previewVisible = ref(false);
        const previewLoading = ref(false);
        const previewData = ref(null);
        const submitting = ref(false);
        const showHelp = ref(false);
        const helpTab = ref('quick');
        const showLogs = ref(false);
        const jobLogs = ref([]);
        const jobErrors = ref({});
        const scheduleDialogVisible = ref(false);
        const scheduleJob = ref(null);
        const scheduleEnabled = ref(false);
        const cronExpression = ref('');
        const scheduleMaxRetries = ref(0);
        const webhookUrl = ref('');
        const scheduleNlText = ref('');
        const scheduleCronHuman = ref('');
        const schedulePresets = [
            { label: '每 5 分钟', cron: '0 */5 * * * *' },
            { label: '每 15 分钟', cron: '0 */15 * * * *' },
            { label: '每 30 分钟', cron: '0 */30 * * * *' },
            { label: '每小时', cron: '0 0 * * * *' },
            { label: '每天 00:00', cron: '0 0 0 * * *' },
            { label: '每天 02:00（凌晨）', cron: '0 0 2 * * *' },
            { label: '每天 08:00（早上）', cron: '0 0 8 * * *' },
            { label: '每天 12:00（中午）', cron: '0 0 12 * * *' },
            { label: '每天 18:00（晚上）', cron: '0 0 18 * * *' },
            { label: '每周一 09:00', cron: '0 0 9 * * 1' },
            { label: '每周五 18:00', cron: '0 0 18 * * 5' },
            { label: '每月 1 号 00:00', cron: '0 0 0 1 * *' }
        ];
        const scheduleHistory = ref([]);
        const scheduleHistoryLoading = ref(false);
        const versionDialogVisible = ref(false);
        const jobVersions = ref([]);
        const versionLoading = ref(false);
        const versionJob = ref(null);
        const monitorJobs = ref([]);
        const monitorLoading = ref(false);
        const monitorError = ref('');
        const monitorAutoRefresh = ref(true);
        const monitorLastUpdated = ref('');
        const monitorTrends = ref([]);
        const monitorTrendUpdated = ref('');
        let trendChart = null;
        const canUndo = ref(false);
        const canRedo = ref(false);
        let monitorTimer = null;
        let alertTimer = null;

        // ===== AI 助手 / 告警中心 =====
        const aiPrompt = ref('');
        const aiLoading = ref(false);
        const aiError = ref('');
        const aiResult = ref(null);
        const aiResultDag = ref(null);
        const aiExamples = [
            '读取 D:\\code\\比赛\\2026省服务外包\\test-resources\\data\\sales.csv，把 product_category 和 channel 拼接成新列 category_channel，写出到 MySQL 表 ai_demo',
            '读取 sales.csv，只保留 sale_id/region/amount 三列，写到 D:\\code\\比赛\\2026省服务外包\\output\\ai_filtered.csv',
            '从 MySQL 表 flink_demo.user_data 读取数据，过滤 id > 1，输出到 JSON 文件'
        ];
        const alerts = ref([]);
        const alertUnread = ref(0);
        const alertsLoading = ref(false);
        const diagnoseVisible = ref(false);
        const diagnoseLoading = ref(false);
        const diagnoseResult = ref(null);
        const diagnoseJob = ref(null);
        const hasParamFixes = computed(() => {
            const pf = diagnoseResult.value && diagnoseResult.value.paramFixes;
            return !!pf && typeof pf === 'object' && Object.keys(pf).length > 0;
        });

        // ===== 认证状态（JWT + RBAC）=====
        const token = ref(localStorage.getItem('token') || '');
        const user = ref(null);
        const loginUsername = ref('');
        const loginPassword = ref('');
        const loginError = ref('');
        const loginLoading = ref(false);
        const onlyMine = ref(false);
        const canEdit = computed(() => !!user.value && user.value.role !== 'VIEWER');
        const canViewAudit = computed(() => !!user.value && (user.value.role === 'ADMIN' || user.value.role === 'OPERATOR'));

        axios.interceptors.request.use(cfg => {
            if (token.value) cfg.headers.Authorization = 'Bearer ' + token.value;
            return cfg;
        });
        axios.interceptors.response.use(r => r, err => {
            if (err.response && err.response.status === 401) {
                const hadSession = !!user.value || !!token.value;
                if (hadSession) {
                    token.value = '';
                    localStorage.removeItem('token');
                    user.value = null;
                    ElMessage.warning('登录已过期，请重新登录');
                }
            }
            return Promise.reject(err);
        });

        // 选中节点状态
        const selectedNode = ref(null);
        const nodeParams = ref({});
        const nodeParamSchema = ref({});

        // X6 实例
        let graph = null;
        let currentJobId = null;
        let dagLoadSeq = 0;

        // 初始化画布
        function initGraph() {
            const container = document.getElementById('dag-canvas');
            if (!container) return;

            graph = new X6.Graph({
                container,
                width: container.clientWidth,
                height: container.clientHeight,
                grid: { visible: true, size: 20, type: 'doubleMesh' },
                panning: { enabled: true, eventTypes: ['leftMouseDown'] },
                mousewheel: { enabled: true, zoomAtMousePosition: true },
                connecting: {
                    router: 'manhattan',
                    connector: { name: 'rounded' },
                    anchor: 'center',
                    sourceAnchor: 'right',
                    targetAnchor: 'left',
                    allowBlank: false,
                    allowMulti: false,
                    createEdge() {
                        return graph.createEdge({
                            attrs: {
                                line: { stroke: '#909399', strokeWidth: 2, strokeDasharray: '5 5', targetMarker: { name: 'block', width: 8, height: 6 } }
                            },
                            zIndex: 1
                        });
                    }
                },
                highlighting: {
                    nodeAvailable: { name: 'stroke', args: { padding: 8, attrs: { stroke: '#409eff', strokeWidth: 2 } } }
                },
                selecting: { enabled: true, multiple: false },
                history: { enabled: true }
            });

            // 节点点击
            graph.on('node:click', ({ node }) => {
                selectedNode.value = node;
                const data = node.getData() || {};
                nodeParams.value = { ...(data.params || {}) };
                nodeParamSchema.value = data.paramSchema || {};
            });

            // 点击空白取消选中
            graph.on('blank:click', () => {
                selectedNode.value = null;
                nodeParams.value = {};
                nodeParamSchema.value = {};
            });

            // 节点删除
            graph.on('node:removed', ({ node }) => {
                if (selectedNode.value === node) {
                    selectedNode.value = null;
                    nodeParams.value = {};
                    nodeParamSchema.value = {};
                }
            });

            // 节点双击删除
            graph.on('node:dblclick', ({ node }) => {
                graph.removeNode(node);
                ElMessage.info('节点已删除');
            });

            // 监听画布 resize
            window.addEventListener('resize', () => {
                if (graph) {
                    graph.resize(container.clientWidth, container.clientHeight);
                }
            });

            // 历史状态刷新（撤销/重做按钮可用性）
            refreshHistoryState();
            ['node:added', 'node:removed', 'edge:added', 'edge:removed',
                'node:change:position', 'node:change:data', 'edge:change:source', 'edge:change:target']
                .forEach(evt => graph.on(evt, () => setTimeout(refreshHistoryState, 0)));
        }

        function refreshHistoryState() {
            canUndo.value = !!(graph && graph.history && graph.history.canUndo());
            canRedo.value = !!(graph && graph.history && graph.history.canRedo());
        }
        function undoDag() { if (graph && graph.history && graph.history.canUndo()) { graph.history.undo(); setTimeout(refreshHistoryState, 0); } }
        function redoDag() { if (graph && graph.history && graph.history.canRedo()) { graph.history.redo(); setTimeout(refreshHistoryState, 0); } }

        // 拖拽开始
        function onDragStart(event, ctrl) {
            event.dataTransfer.setData('text/plain', JSON.stringify({
                type: ctrl.type,
                name: ctrl.name,
                category: ctrl.category,
                paramSchema: ctrl.paramSchema ? JSON.parse(ctrl.paramSchema) : {},
                flinkTemplate: ctrl.flinkTemplate
            }));
            event.dataTransfer.effectAllowed = 'copy';
        }

        // 监听放置事件 (使用全局事件)
        function setupDropHandler() {
            const container = document.getElementById('dag-canvas');
            if (!container) return;

            container.addEventListener('dragover', (e) => {
                e.preventDefault();
                e.dataTransfer.dropEffect = 'copy';
            });

            container.addEventListener('drop', (e) => {
                e.preventDefault();
                try {
                    const data = JSON.parse(e.dataTransfer.getData('text/plain'));
                    if (!graph) return;

                    const point = graph.clientToLocal(e.clientX, e.clientY);
                    const nodeId = `${data.type}_${Date.now()}`;
                    const colors = CATEGORY_COLORS[data.category] || CATEGORY_COLORS.transform;

                    const node = graph.addNode({
                        id: nodeId,
                        x: point.x - 60,
                        y: point.y - 20,
                        width: 160,
                        height: 50,
                        label: `${colors.icon} ${data.name}`,
                        attrs: {
                            body: {
                                fill: colors.bg,
                                stroke: colors.border,
                                strokeWidth: 2,
                                rx: 8,
                                ry: 8
                            },
                            label: {
                                text: `${colors.icon} ${data.name}`,
                                fill: '#303133',
                                fontSize: 13,
                                fontWeight: 'bold',
                                textAnchor: 'middle',
                                textVerticalAnchor: 'middle'
                            }
                        },
                        ports: {
                            groups: {
                                left: { position: 'left', attrs: { circle: { r: 4, magnet: true, stroke: colors.border, fill: '#fff', strokeWidth: 2 } } },
                                right: { position: 'right', attrs: { circle: { r: 4, magnet: true, stroke: colors.border, fill: '#fff', strokeWidth: 2 } } }
                            },
                            items: [
                                { id: `${nodeId}-in`, group: 'left' },
                                { id: `${nodeId}-out`, group: 'right' }
                            ]
                        },
                        data: {
                            type: data.type,
                            name: data.name,
                            category: data.category,
                            params: {},
                            paramSchema: data.paramSchema
                        }
                    });

                    ElMessage.success(`添加控件: ${data.name}`);
                } catch (err) {
                    console.error('Drop error:', err);
                }
            });
        }

        // 按分类获取控件
        function controlsByCategory(category) {
            return controls.value.filter(c => c.category === category && c.enabled !== false);
        }

        // 加载控件
        async function loadControls() {
            try {
                controls.value = await api.getControls();
            } catch (err) {
                console.warn('从后端加载控件失败，使用内置控件');
                controls.value = getBuiltinControls();
            }
        }

        // 内置控件（后端不可用时的降级）
        function getBuiltinControls() {
            return [
                { type: 'csv_input', name: 'CSV 输入', category: 'input', description: '读取 CSV 文件作为数据源', version: '1.0.0', paramSchema: '{"type":"object","properties":{"path":{"type":"string","title":"文件路径","default":"/data/input.csv"},"delimiter":{"type":"string","title":"分隔符","default":","},"hasHeader":{"type":"boolean","title":"包含表头","default":true}},"required":["path"]}' },
                { type: 'csv_output', name: 'CSV 输出', category: 'output', description: '将数据写入 CSV 文件', version: '1.0.0', paramSchema: '{"type":"object","properties":{"path":{"type":"string","title":"输出路径","default":"D:\\\\code\\\\比赛\\\\2026省服务外包\\\\output\\\\output.csv"},"delimiter":{"type":"string","title":"分隔符","default":","},"sheetName":{"type":"string","title":"工作表名（默认 Data，多输出写同一文件时可区分 sheet）","default":"Data"}},"required":["path"]}' },
                { type: 'excel_input', name: 'Excel 输入', category: 'input', description: '读取 Excel (.xlsx) 文件', version: '1.0.0', paramSchema: '{"type":"object","properties":{"path":{"type":"string","title":"文件路径","default":"/data/input.xlsx"},"sheetName":{"type":"string","title":"工作表名（留空取第一个）","default":""}},"required":["path"]}' },
                { type: 'mysql_input', name: 'MySQL 输入', category: 'input', description: '从 MySQL 表读取数据', version: '1.0.0', paramSchema: '{"type":"object","properties":{"url":{"type":"string","title":"JDBC URL","default":"jdbc:mysql://localhost:3306/flink_demo"},"table":{"type":"string","title":"表名","default":"source_table"},"username":{"type":"string","title":"用户名","default":"${MYSQL_USERNAME}"},"password":{"type":"string","title":"密码（留空或 \\${MYSQL_PASSWORD} 取环境变量）","default":"${MYSQL_PASSWORD}"}},"required":["url","table","username"]}' },
                { type: 'mysql_output', name: 'MySQL 输出', category: 'output', description: '将数据写入 MySQL 表', version: '1.0.0', paramSchema: '{"type":"object","properties":{"url":{"type":"string","title":"JDBC URL","default":"jdbc:mysql://localhost:3306/flink_demo"},"table":{"type":"string","title":"表名","default":"target_table"},"username":{"type":"string","title":"用户名","default":"${MYSQL_USERNAME}"},"password":{"type":"string","title":"密码（留空或 \\${MYSQL_PASSWORD} 取环境变量）","default":"${MYSQL_PASSWORD}"}},"required":["url","table","username"]}' },
                                { type: 'kafka_input', name: 'Kafka 输入', category: 'input', description: '从 Kafka 主题读取消息流', version: '1.0.0', paramSchema: '{"type":"object","properties":{"topic":{"type":"string","title":"主题","default":"input-topic"},"bootstrapServers":{"type":"string","title":"Bootstrap Servers","default":"localhost:9092"},"fieldsConfig":{"type":"string","title":"字段定义（JSON 数组）","default":"[{\"name\":\"key\",\"type\":\"STRING\"},{\"name\":\"value\",\"type\":\"STRING\"}]"},"autoStop":{"type":"boolean","title":"体验模式：消费完自动停止","default":false},"stopAfterSeconds":{"type":"number","title":"自动停止延迟（秒）","default":30}},"required":["topic","bootstrapServers"]}' },
                { type: 'kafka_output', name: 'Kafka 输出', category: 'output', description: '将数据写入 Kafka 主题', version: '1.0.0', paramSchema: '{"type":"object","properties":{"topic":{"type":"string","title":"主题","default":"output-topic"},"bootstrapServers":{"type":"string","title":"Bootstrap Servers","default":"localhost:9092"}},"required":["topic","bootstrapServers"]}' },
                { type: 'excel_output', name: 'Excel 输出', category: 'output', description: '将数据写出为 Excel (.xlsx) 文件（内部 CSV→Excel 转换）', version: '1.0.0', paramSchema: '{"type":"object","properties":{"path":{"type":"string","title":"输出路径","default":"D:\\\\code\\\\比赛\\\\2026省服务外包\\\\output\\\\output.xlsx"},"delimiter":{"type":"string","title":"分隔符","default":","},"sheetName":{"type":"string","title":"工作表名（默认 Data，多输出写同一文件时可区分 sheet）","default":"Data"}},"required":["path"]}' },                { type: 'parquet_input', name: 'Parquet 输入', category: 'input', description: '读取 Parquet (.parquet) 文件', version: '1.0.0', paramSchema: '{"type":"object","properties":{"path":{"type":"string","title":"文件路径","default":"D:\\code\\比赛\\2026省服务外包\\output\\input.parquet"},"delimiter":{"type":"string","title":"分隔符","default":","}},"required":["path"]}' },
                { type: 'parquet_output', name: 'Parquet 输出', category: 'output', description: '将数据写出为 Parquet (.parquet) 文件（内部 CSV→Parquet 转换）', version: '1.0.0', paramSchema: '{"type":"object","properties":{"path":{"type":"string","title":"输出路径","default":"D:\\code\\比赛\\2026省服务外包\\output\\output.parquet"},"delimiter":{"type":"string","title":"分隔符","default":","}},"required":["path"]}' },
                { type: 'field_concat', name: '字段拼接', category: 'transform', description: '将多个字段拼接为一个新字段', version: '1.0.0', paramSchema: '{"type":"object","properties":{"fields":{"type":"array","title":"输入字段","items":{"type":"string"}},"separator":{"type":"string","title":"分隔符","default":","},"newFieldName":{"type":"string","title":"新字段名","default":"concat_field"}},"required":["fields","newFieldName"]}' },
                { type: 'xml_json', name: 'XML<->JSON', category: 'transform', description: 'XML 和 JSON 格式互转', version: '1.0.0', paramSchema: '{"type":"object","properties":{"direction":{"type":"string","title":"转换方向","enum":["xml2json","json2xml"],"default":"xml2json"},"sourceField":{"type":"string","title":"源字段","default":"payload"},"targetField":{"type":"string","title":"目标字段","default":"result"}},"required":["direction","sourceField"]}' },
                { type: 'dedupe', name: '去重', category: 'transform', description: '按字段去重（留空=整行去重）', version: '1.0.0', paramSchema: '{"type":"object","properties":{"dedupeFields":{"type":"string","title":"去重字段（逗号分隔，留空=整行去重）","default":""}},"required":[]}' },
                { type: 'validate', name: '空值校验', category: 'transform', description: '丢弃指定字段为空的行', version: '1.0.0', paramSchema: '{"type":"object","properties":{"checkFields":{"type":"string","title":"必填字段（逗号分隔）","default":"id"},"ignoreEmpty":{"type":"boolean","title":"空字符串也算空值","default":true}},"required":["checkFields"]}' },
                { type: 'route', name: '条件路由', category: 'transform', description: '按字段值分流：第一条出边=匹配，其余=不匹配', version: '1.0.0', paramSchema: '{"type":"object","properties":{"routeField":{"type":"string","title":"路由字段","default":"status"},"matchValues":{"type":"string","title":"匹配值（逗号分隔）","default":"SUCCESS"}},"required":["routeField","matchValues"]}' },
,                { type: 'field_filter', name: '字段过滤', category: 'transform', description: '只保留指定字段', version: '1.0.0', paramSchema: '{"type":"object","properties":{"fields":{"type":"string","title":"保留字段（逗号分隔）","default":"id,name"}},"required":["fields"]}' },
                { type: 'field_rename', name: '字段改名', category: 'transform', description: '字段重命名', version: '1.0.0', paramSchema: '{"type":"object","properties":{"mappings":{"type":"string","title":"改名映射（old=new，逗号分隔）","default":"id=userId"}},"required":["mappings"]}' },
                { type: 'row_filter', name: '行过滤', category: 'transform', description: '按条件过滤行', version: '1.0.0', paramSchema: '{"type":"object","properties":{"condition":{"type":"string","title":"过滤条件（SQL WHERE 表达式）","default":"age > 18"}},"required":["condition"]}' },
                { type: 'json_parse', name: 'JSON 解析', category: 'transform', description: '从 JSON 字段解析出多个字段', version: '1.0.0', paramSchema: '{"type":"object","properties":{"sourceField":{"type":"string","title":"JSON 源字段","default":"payload"},"fieldsConfig":{"type":"string","title":"解析字段（JSON 数组）","default":"[{\"name\":\"id\",\"type\":\"INT\"},{\"name\":\"name\",\"type\":\"STRING\"}]"}},"required":["sourceField","fieldsConfig"]}' },
                { type: 'json_input', name: 'JSON 输入', category: 'input', description: '读取 JSON 文件（auto 自动识别 / lines 逐行 / array 数组）', version: '1.0.0', paramSchema: '{"type":"object","properties":{"path":{"type":"string","title":"文件路径","default":"D:\\code\\比赛\\2026省服务外包\\test-resources\\data\\sample.json"},"mode":{"type":"string","title":"文件模式","enum":["auto","lines","array"],"default":"auto"},"delimiter":{"type":"string","title":"分隔符","default":","},"encoding":{"type":"string","title":"文件编码","default":"UTF-8"},"fieldsConfig":{"type":"string","title":"字段定义（JSON 数组，lines 模式用）","default":"[{\"name\":\"id\",\"type\":\"INT\"},{\"name\":\"name\",\"type\":\"STRING\"}]"}},"required":["path"]}' },
                { type: 'json_output', name: 'JSON 输出', category: 'output', description: '将数据写入 JSON 文件（lines 逐行 / array 数组）', version: '1.0.0', paramSchema: '{"type":"object","properties":{"path":{"type":"string","title":"输出路径（.json）","default":"D:\\code\\比赛\\2026省服务外包\\output\\output.json"},"mode":{"type":"string","title":"输出模式","enum":["lines","array"],"default":"lines"}},"required":["path"]}' },
                { type: 'xml_input', name: 'XML 输入', category: 'input', description: '读取 XML 文件（记录列表结构，嵌套子结构保留为 JSON）', version: '1.0.0', paramSchema: '{"type":"object","properties":{"path":{"type":"string","title":"文件路径","default":"D:\\code\\比赛\\2026省服务外包\\test-resources\\data\\orders.xml"},"rowTag":{"type":"string","title":"行元素名（留空自动探测）","default":""},"delimiter":{"type":"string","title":"分隔符","default":","},"encoding":{"type":"string","title":"文件编码","default":"UTF-8"}},"required":["path"]}' },
                { type: 'xml_output', name: 'XML 输出', category: 'output', description: '将数据写出为 XML 文件（rootTag/rowTag 包裹）', version: '1.0.0', paramSchema: '{"type":"object","properties":{"path":{"type":"string","title":"输出路径（.xml）","default":"D:\\code\\比赛\\2026省服务外包\\output\\output.xml"},"rootTag":{"type":"string","title":"根元素名","default":"root"},"rowTag":{"type":"string","title":"行元素名","default":"record"},"encoding":{"type":"string","title":"输出编码","default":"UTF-8"}},"required":["path"]}' }
            ];
        }

        // 加载作业列表
        async function loadJobs() {
            try {
                jobs.value = await api.getJobs();
            } catch (err) {
                console.warn('加载作业列表失败:', err);
                jobs.value = [];
            }
        }

        // 作业列表搜索 / 状态筛选（纯前端过滤）
        const filteredJobs = computed(() => {
            const kw = (jobSearch.value || '').trim().toLowerCase();
            const st = jobStatusFilter.value;
            return jobs.value.filter(j => {
                if (onlyMine.value && user.value && j.ownerName && j.ownerName !== user.value.username) return false;
                if (st && j.status !== st) return false;
                if (!kw) return true;
                return (j.name || '').toLowerCase().includes(kw)
                    || String(j.id).includes(kw)
                    || (j.flinkJobId || '').toLowerCase().includes(kw);
            });
        });

        // ===== 登录 / 登出 =====
        async function login() {
            if (!loginUsername.value || !loginPassword.value) {
                loginError.value = '请输入用户名和密码';
                return;
            }
            loginLoading.value = true;
            loginError.value = '';
            try {
                const data = await api.login(loginUsername.value, loginPassword.value);
                token.value = data.token;
                user.value = data.user;
                localStorage.setItem('token', data.token);
                ElMessage.success('欢迎，' + (data.user.displayName || data.user.username));
                currentView.value = 'canvas';
                await loadControls();
                await loadJobs();
                nextTick(() => {
                    setTimeout(() => {
                        if (!graph) { initGraph(); setupDropHandler(); }
                    }, 100);
                });
            } catch (err) {
                loginError.value = err.response?.data?.error || err.response?.data?.message || '登录失败，请重试';
            } finally {
                loginLoading.value = false;
            }
        }
        async function logout() {
            try { await api.logout(); } catch (_) {}
            if (alertTimer) { clearInterval(alertTimer); alertTimer = null; }
            token.value = '';
            user.value = null;
            localStorage.removeItem('token');
            currentView.value = 'canvas';
            jobs.value = [];
            controls.value = [];
            if (graph) { try { graph.dispose(); } catch (_) {} graph = null; }
            ElMessage.info('已退出登录');
        }

        // ===== 操作审计 =====
        const auditRows = ref([]);
        const auditTotal = ref(0);
        const auditPage = ref(0);
        const auditSize = ref(20);
        const auditKeyword = ref('');
        const auditLoading = ref(false);
        async function loadAudit(page = auditPage.value) {
            if (!canViewAudit.value) return;
            auditLoading.value = true;
            try {
                const data = await api.getAudit({ page, size: auditSize.value, keyword: auditKeyword.value || undefined });
                auditRows.value = data.content || [];
                auditTotal.value = data.total || 0;
                auditPage.value = data.page || 0;
            } catch (err) {
                if (!(err.response && err.response.status === 401)) {
                    ElMessage.error('加载审计日志失败: ' + (err.response?.data?.error || err.message));
                }
            } finally {
                auditLoading.value = false;
            }
        }
        function onAuditPage(p) { loadAudit(p - 1); }

        // 打开帮助中心并定位到指定标签
        function openHelp(tab) {
            helpTab.value = tab || 'quick';
            showHelp.value = true;
        }

        // ===== 实时监控 =====
        const monitorStats = computed(() => {
            const stats = { total: monitorJobs.value.length, running: 0, completed: 0, failed: 0 };
            for (const j of monitorJobs.value) {
                const s = String(j.status || '').toUpperCase();
                if (s === 'RUNNING' || s === 'SUBMITTED') stats.running++;
                else if (s === 'COMPLETED') stats.completed++;
                else if (s === 'FAILED' || s === 'CANCELLED') stats.failed++;
            }
            return stats;
        });
        async function loadMonitor() {
            monitorLoading.value = true;
            monitorError.value = '';
            try {
                monitorJobs.value = await api.getMonitorOverview();
                monitorLastUpdated.value = new Date().toLocaleTimeString('zh-CN');
                loadTrends();
            } catch (err) {
                monitorError.value = (err && err.message) ? err.message : String(err);
                console.warn('加载监控数据失败:', err);
            } finally {
                monitorLoading.value = false;
            }
        }
        function refreshMonitor() { loadMonitor(); }
        function startMonitorPolling() {
            stopMonitorPolling();
            if (!monitorAutoRefresh.value) return;
            loadMonitor();
            monitorTimer = setInterval(loadMonitor, 5000);
        }
        function stopMonitorPolling() { if (monitorTimer) { clearInterval(monitorTimer); monitorTimer = null; } }
        function fmtNum(v) { const n = Number(v); if (isNaN(n)) return '-'; return n >= 1000 ? (n / 1000).toFixed(1) + 'k' : String(Math.round(n)); }
        function bpClass(level) { if (!level) return ''; const l = String(level).toLowerCase(); return l === 'ok' ? 'bp-ok' : l === 'low' ? 'bp-low' : l === 'high' ? 'bp-high' : ''; }
        function cpText(cp) { if (!cp || cp.error) return 'N/A'; if (!cp.lastCompletedTs) return '尚未完成'; const d = new Date(cp.lastCompletedTs); return '完成 ' + d.toLocaleTimeString() + ' · ' + Math.round(cp.endToEndDuration || 0) + 'ms'; }
        function fmtDuration(ms) { if (!ms || ms < 0) return '-'; if (ms < 1000) return ms + 'ms'; const sec = Math.floor(ms / 1000); const h = Math.floor(sec / 3600); const m = Math.floor((sec % 3600) / 60); const s0 = sec % 60; return h > 0 ? h + 'h' + m + 'm' : m > 0 ? m + 'm' + s0 + 's' : s0 + 's'; }
        function statusTagType(status) { const s = String(status || '').toUpperCase(); if (s === 'COMPLETED') return 'success'; if (s === 'FAILED') return 'danger'; if (s === 'RUNNING' || s === 'SUBMITTED') return 'warning'; return 'info'; }
        const TREND_COLORS = ['#3b82f6', '#10b981', '#f59e0b', '#ef4444', '#8b5cf6', '#06b6d4'];
        function initTrendChart() {
            if (trendChart || typeof echarts === 'undefined') return;
            const el = document.getElementById('monitorTrendChart');
            if (!el || el.offsetParent === null) return;
            trendChart = echarts.init(el);
            updateTrendChart();
        }
        async function loadTrends() {
            try {
                monitorTrends.value = await api.getMonitorTrends();
                monitorTrendUpdated.value = new Date().toLocaleTimeString('zh-CN');
            } catch (err) {
                console.warn('加载趋势数据失败:', err);
            }
            updateTrendChart();
        }
        function updateTrendChart() {
            if (!trendChart) return;
            const series = monitorTrends.value.map((t, i) => ({
                name: t.name,
                type: 'line',
                smooth: true,
                showSymbol: false,
                emphasis: { focus: 'series' },
                lineStyle: { width: 2, color: TREND_COLORS[i % TREND_COLORS.length] },
                itemStyle: { color: TREND_COLORS[i % TREND_COLORS.length] },
                data: (t.points || []).map(pt => [pt.t, Math.round(pt.out || 0)])
            }));
            const hasData = series.some(s => s.data.length > 0);
            trendChart.setOption({
                title: hasData ? undefined : { text: '暂无吞吐数据（运行中或最近结束的作业）', left: 'center', top: 'middle', textStyle: { color: '#94a3b8', fontSize: 13, fontWeight: 'normal' } },
                tooltip: { trigger: 'axis' },
                legend: hasData ? { top: 0, type: 'scroll', textStyle: { color: '#64748b' } } : undefined,
                grid: { left: 56, right: 20, top: 32, bottom: 28 },
                xAxis: { type: 'time', axisLabel: { color: '#64748b' }, axisLine: { lineStyle: { color: '#e2e8f0' } } },
                yAxis: { type: 'value', name: '行/s', min: 0, max: 500, nameTextStyle: { color: '#94a3b8' }, axisLabel: { color: '#64748b' }, splitLine: { lineStyle: { color: '#f1f5f9' } } },
                series: series.length ? series : [{ type: 'line', data: [] }]
            }, true);
        }
        // ===== 实时监控 END =====

        // ===== AI 助手（NL2Pipeline）=====
        async function generatePipeline() {
            if (!aiPrompt.value || !aiPrompt.value.trim()) { ElMessage.warning('请输入自然语言描述'); return; }
            aiLoading.value = true; aiError.value = ''; aiResult.value = null; aiResultDag.value = null;
            try {
                const data = await api.aiNl2Pipeline(aiPrompt.value.trim());
                aiResult.value = data;
                try { aiResultDag.value = JSON.parse(data.dag); } catch (_) { aiResultDag.value = null; }
                ElMessage.success('AI 已生成 DRAFT 作业 #' + data.job.id + '，请人工确认后提交');
                await loadJobs();
            } catch (err) {
                aiError.value = err.response && err.response.data && err.response.data.error ? err.response.data.error : (err.message || '生成失败');
            } finally { aiLoading.value = false; }
        }
        function openAiResultInCanvas() {
            if (!aiResultDag.value) { ElMessage.warning('DAG 解析失败'); return; }
            currentView.value = 'canvas';
            currentJobId = aiResult.value.job.id;
            currentJobName.value = aiResult.value.job.name;
            jobName.value = aiResult.value.job.name;
            parallelism.value = aiResult.value.job.parallelism || 1;
            nextTick(() => {
                if (!graph) { initGraph(); setupDropHandler(); }
                loadDagToCanvas(aiResultDag.value);
            });
            ElMessage.success('已打开画布，请人工确认参数后保存/提交');
        }

        // ===== 告警中心 =====
        async function loadAlerts() {
            alertsLoading.value = true;
            try {
                alerts.value = await api.getAlerts();
                const u = await api.getAlertUnread();
                alertUnread.value = (u && u.unread) || 0;
            } catch (err) {
                if (!(err.response && err.response.status === 401)) console.warn('加载告警失败:', err);
            } finally { alertsLoading.value = false; }
        }
        async function markAlertRead(id) {
            try { await api.markAlertRead(id); } catch (_) {}
            await loadAlerts();
        }
        async function markAllRead() {
            try { await api.markAllAlertsRead(); } catch (_) {}
            await loadAlerts();
        }
        async function deleteAlert(id) {
            try {
                await ElMessageBox.confirm('确定删除此告警？删除后不可恢复。', '删除告警', { type: 'warning' });
                await api.deleteAlert(id);
                ElMessage.success('告警已删除');
                await loadAlerts();
            } catch (err) {
                if (err !== 'cancel') {
                    ElMessage.error('删除失败');
                }
            }
        }

        // ===== AI 智能诊断 =====
        function openDiagnose(row) {
            diagnoseJob.value = row;
            diagnoseResult.value = null;
            diagnoseVisible.value = true;
            runDiagnose();
        }
        async function runDiagnose() {
            if (!diagnoseJob.value) return;
            diagnoseLoading.value = true;
            try {
                diagnoseResult.value = await api.aiDiagnose(diagnoseJob.value.id);
            } catch (err) {
                ElMessage.error('诊断失败: ' + (err.response && err.response.data && err.response.data.error ? err.response.data.error : err.message));
            } finally { diagnoseLoading.value = false; }
        }
        async function applyDiagnoseFixes() {
            const fixes = diagnoseResult.value && diagnoseResult.value.paramFixes;
            if (!fixes || typeof fixes !== 'object' || Object.keys(fixes).length === 0) { ElMessage.warning('没有可预填的参数修正'); return; }
            if (!diagnoseJob.value || !diagnoseJob.value.dagJson) { ElMessage.warning('作业没有 DAG'); return; }
            try {
                const dag = JSON.parse(diagnoseJob.value.dagJson);
                for (const node of dag.nodes) {
                    if (fixes[node.id]) {
                        node.params = Object.assign({}, node.params, fixes[node.id]);
                    }
                }
                await api.updateJob(diagnoseJob.value.id, { name: diagnoseJob.value.name, dagJson: JSON.stringify(dag), parallelism: diagnoseJob.value.parallelism });
                await loadJobs();
                currentView.value = 'canvas';
                currentJobId = diagnoseJob.value.id;
                currentJobName.value = diagnoseJob.value.name;
                jobName.value = diagnoseJob.value.name;
                parallelism.value = diagnoseJob.value.parallelism || 1;
                nextTick(() => { loadDagToCanvas(dag); });
                ElMessage.success('参数已写入画布并保存，请重新提交作业');
            } catch (err) {
                ElMessage.error('应用修正失败: ' + (err.response && err.response.data && err.response.data.message ? err.response.data.message : err.message));
            }
        }

        // 保存 DAG
        function validateDagForSubmit() {
            if (!graph) return null;
            const cells = graph.getCells();
            const nodes = cells.filter(c => c.isNode()).map(n => ({
                type: n.getData()?.type,
                params: n.getData()?.params || {}
            }));
            const hasEdge = cells.some(c => c.isEdge());
            if (nodes.length > 0 && !hasEdge) {
                return '画布上有节点但没有连线，请先连接节点';
            }
            for (const n of nodes) {
                if (n.type === 'csv_output' || n.type === 'excel_output') {
                    const p = n.params.path;
                    if (!p || (typeof p === 'string' && p.trim() === '')) {
                        return '输出节点缺少路径：请点击节点，在参数面板填写输出路径';
                    }
                }
            }
            return null;
        }

        async function saveDag() {
            saving.value = true;
            try {
                const checkMsg = validateDagForSubmit();
                if (checkMsg) {
                    ElMessage.warning(checkMsg);
                    saving.value = false;
                    return;
                }
                if (selectedNode.value) applyParams();
                const dag = exportDagJson();
                const jobData = {
                    name: jobName.value || '未命名作业',
                    dagJson: JSON.stringify(dag),
                    parallelism: parallelism.value
                };

                let saved;
                if (currentJobId) {
                    saved = await api.updateJob(currentJobId, jobData);
                } else {
                    saved = await api.createJob(jobData);
                    currentJobId = saved.id;
                }

                currentJobName.value = saved.name;
                ElMessage.success('DAG 已保存 (ID: ' + saved.id + ')');
                await loadJobs();
            } catch (err) {
                ElMessage.error('保存失败: ' + (err.response?.data?.message || err.message));
            } finally {
                saving.value = false;
            }
        }

        // 提交作业
        async function submitJob() {
            const checkMsg = validateDagForSubmit();
            if (checkMsg) {
                ElMessage.warning(checkMsg);
                return;
            }
            if (!currentJobId) {
                ElMessage.warning('请先保存 DAG');
                return;
            }
            submitting.value = true;
            try {
                await api.submitJob(currentJobId);
                ElMessage.success('作业已提交到 Flink 集群');
                await loadJobs();

                // 获取日志
                jobLogs.value = await api.getLogs(currentJobId);
                showLogs.value = true;
            } catch (err) {
                ElMessage.error('提交失败: ' + (err.response?.data?.message || err.message));
                if (currentJobId) { jobErrors.value[currentJobId] = err.response?.data?.message || err.message; }
                try {
                    if (currentJobId) {
                        jobLogs.value = await api.getLogs(currentJobId);
                        showLogs.value = true;
                    }
                } catch (_) {}
            } finally {
                submitting.value = false;
            }
        }

        // 导出 DAG JSON
        function exportDagJson() {
            if (!graph) return null;
            const cells = graph.getCells();
            const nodes = cells.filter(c => c.isNode()).map(n => ({
                id: n.id,
                type: n.getData()?.type || 'unknown',
                label: n.getData()?.name || n.getLabel() || '未知',
                params: n.getData()?.params || {},
                x: n.position().x,
                y: n.position().y
            }));

            const edges = cells.filter(c => c.isEdge()).map(e => ({
                id: e.id,
                source: e.getSourceCellId(),
                target: e.getTargetCellId()
            }));

            return {
                jobName: jobName.value,
                parallelism: parallelism.value,
                nodes,
                edges
            };
        }

        // 导出 DAG (下载文件)
        function exportDag() {
            const dag = exportDagJson();
            if (!dag || dag.nodes.length === 0) {
                ElMessage.warning('画布为空');
                return;
            }
            const blob = new Blob([JSON.stringify(dag, null, 2)], { type: 'application/json' });
            const url = URL.createObjectURL(blob);
            const a = document.createElement('a');
            a.href = url;
            a.download = `${jobName.value || 'dag'}.json`;
            a.click();
            URL.revokeObjectURL(url);
            ElMessage.success('DAG 已导出');
        }

        // 导入 DAG
        function importDagTrigger() {
            document.querySelector('input[type="file"]').click();
        }

        function importDag(event) {
            const file = event.target.files[0];
            if (!file) return;

            const reader = new FileReader();
            reader.onload = (e) => {
                try {
                    const dag = JSON.parse(e.target.result);
                    loadDagToCanvas(dag);
                    ElMessage.success('DAG 已导入');
                } catch (err) {
                    ElMessage.error('导入失败: JSON 格式错误');
                }
            };
            reader.readAsText(file);
            event.target.value = '';
        }

        // 将 DAG JSON 加载到画布
        async function loadDagToCanvas(dag) {
            if (!graph) return;
            const dagLoadSnapshot = ++dagLoadSeq;
            graph.clearCells();
            selectedNode.value = null;
            nodeParams.value = {};
            nodeParamSchema.value = {};

            jobName.value = dag.jobName || '未命名作业';
            parallelism.value = dag.parallelism || 1;

            if (!dag.nodes) return;

            // 等待 X6 异步渲染完成视图清理，避免同 id 节点残留导致控件重叠
            await new Promise(resolve => {
                let settled = false;
                const finish = () => { if (!settled) { settled = true; resolve(); } };
                requestAnimationFrame(() => requestAnimationFrame(finish));
                setTimeout(finish, 100);
            });
            // 若期间又加载/清空了画布，放弃本次渲染
            if (dagLoadSnapshot !== dagLoadSeq) return;

            // 添加节点（矩形碰撞检测，重叠时整行错开，避免叠在一起无法点击）
            const placedRects = [];
            const NODE_W = 160, NODE_H = 50, GAP = 20;
            function rectCollides(r) {
                return placedRects.some(p => r.x < p.x + p.w && r.x + r.w > p.x && r.y < p.y + p.h && r.y + r.h > p.y);
            }
            const nodeMap = {};
            dag.nodes.forEach(n => {
                const type = n.type;
                // 尝试从控件列表中找到对应控件信息
                const ctrl = controls.value.find(c => c.type === type);
                const category = ctrl?.category || 'transform';
                const colors = CATEGORY_COLORS[category] || CATEGORY_COLORS.transform;

                let nx = (typeof n.x === 'number') ? n.x : 100;
                let ny = (typeof n.y === 'number') ? n.y : 100;
                let tries = 0;
                while (rectCollides({ x: nx, y: ny, w: NODE_W, h: NODE_H }) && tries < 500) {
                    if (nx < 1200) { nx += NODE_W + GAP; } else { nx = 100; ny += NODE_H + GAP; }
                    tries++;
                }
                placedRects.push({ x: nx, y: ny, w: NODE_W, h: NODE_H });

                const node = graph.addNode({
                    id: n.id,
                    x: nx,
                    y: ny,
                    width: 160,
                    height: 50,
                    label: `${colors.icon} ${n.label || type}`,
                    attrs: {
                        body: { fill: colors.bg, stroke: colors.border, strokeWidth: 2, rx: 8, ry: 8 },
                        label: { text: `${colors.icon} ${n.label || type}`, fill: '#303133', fontSize: 13, fontWeight: 'bold', textAnchor: 'middle', textVerticalAnchor: 'middle' }
                    },
                    ports: {
                        groups: {
                            left: { position: 'left', attrs: { circle: { r: 4, magnet: true, stroke: colors.border, fill: '#fff', strokeWidth: 2 } } },
                            right: { position: 'right', attrs: { circle: { r: 4, magnet: true, stroke: colors.border, fill: '#fff', strokeWidth: 2 } } }
                        },
                        items: [
                            { id: n.id + '-in', group: 'left' },
                            { id: n.id + '-out', group: 'right' }
                        ]
                    },
                    data: {
                        type: n.type,
                        name: n.label,
                        category: category,
                        params: n.params || {},
                        paramSchema: ctrl?.paramSchema ? JSON.parse(ctrl.paramSchema) : {}
                    }
                });
                nodeMap[n.id] = node;
            });

            // 添加连线
            if (dag.edges) {
                dag.edges.forEach(e => {
                    if (nodeMap[e.source] && nodeMap[e.target]) {
                        graph.addEdge({
                            id: e.id || `edge_${Date.now()}_${Math.random()}`,
                            source: { cell: e.source, port: e.source + '-out' },
                            target: { cell: e.target, port: e.target + '-in' },
                            attrs: {
                                line: { stroke: '#909399', strokeWidth: 2, strokeDasharray: '5 5', targetMarker: { name: 'block', width: 8, height: 6 } }
                            }
                        });
                    }
                });
            }

            // 加载完成：清空历史栈（加载/切换作业不计入撤销）
            if (graph.history) graph.history.clean();
        }

        // 清空画布
        function clearCanvas() {
            if (graph) {
                graph.clearCells();
                if (graph.history) graph.history.clean();
                dagLoadSeq++;
                selectedNode.value = null;
                nodeParams.value = {};
                nodeParamSchema.value = {};
                currentJobId = null;
                currentJobName.value = '';
                ElMessage.info('画布已清空');
            }
        }

        // 新建画布
        function newCanvas() {
            if (graph) {
                graph.clearCells();
                if (graph.history) graph.history.clean();
                dagLoadSeq++;
                selectedNode.value = null;
                nodeParams.value = {};
                nodeParamSchema.value = {};
            }
            currentJobId = null;
            currentJobName.value = '';
            jobName.value = '未命名作业';
            parallelism.value = 1;
            ElMessage.success('已新建画布');
        }

        // 一键导入示例作业到画布（作为未保存的新作业）
        function loadSampleDag(key) {
            const dag = SAMPLE_DAGS[key];
            if (!dag) { ElMessage.warning('示例作业不存在: ' + key); return; }
            if (!graph) { ElMessage.warning('画布尚未就绪'); return; }
            loadDagToCanvas(dag);
            currentJobId = null;
            currentJobName.value = '';
            ElMessage.success('示例作业已载入画布：' + dag.jobName);
        }

        // 应用参数
        function applyParams() {
            if (!selectedNode.value) return;
            const node = selectedNode.value;
            const data = node.getData() || {};
            data.params = {};
            for (const [k, v] of Object.entries(nodeParams.value)) {
                data.params[k] = typeof v === 'string' ? v.trim() : v;
            }
            node.setData(data);
            ElMessage.success('参数已应用');
        }

        // 预览节点数据（读取输入/输出文件前 N 行）
        const previewRows = computed(() => {
            const d = previewData.value;
            if (!d || !d.columns || !d.rows) return [];
            return d.rows.map(row => {
                const obj = {};
                d.columns.forEach((c, i) => { obj['c_' + i] = row[i] ?? ''; });
                return obj;
            });
        });

        async function previewNode() {
            if (!selectedNode.value) return;
            const data = selectedNode.value.getData() || {};
            const path = data.params?.path;
            if (!path || String(path).trim() === '') {
                ElMessage.warning('该节点没有路径参数，无法预览（MySQL/Datagen 请提交后在作业日志查看）');
                return;
            }
            previewLoading.value = true;
            try {
                const res = await api.previewFile(String(path).trim());
                previewData.value = res;
                previewVisible.value = true;
            } catch (err) {
                ElMessage.error('预览失败: ' + (err.response?.data?.message || err.message));
            } finally {
                previewLoading.value = false;
            }
        }

        // 打开作业
        async function openJob(row) {
            currentView.value = 'canvas';
            currentJobId = row.id;
            jobName.value = row.name;
            currentJobName.value = row.name;
            parallelism.value = row.parallelism || 1;

            if (row.dagJson) {
                try {
                    const dag = JSON.parse(row.dagJson);
                    await nextTick();
                    loadDagToCanvas(dag);
                } catch (err) {
                    ElMessage.warning('DAG JSON 解析失败');
                    currentJobId = null;
                    currentJobName.value = '';
                }
            }
        }

        // 提交作业 (作业列表)
        async function submitJobById(id) {
            const checkMsg = validateDagForSubmit();
            if (checkMsg) {
                ElMessage.warning(checkMsg);
                return;
            }
            try {
                await api.submitJob(id);
                ElMessage.success('作业已提交');
                await loadJobs();
            } catch (err) {
                ElMessage.error('提交失败: ' + (err.response?.data?.message || err.message));
                jobErrors.value[id] = err.response?.data?.message || err.message;
            }
        }

        // 上线 / 下线
        function isStreamingDag(dagJson) {
            try {
                const dag = JSON.parse(dagJson || '{}');
                const nodes = dag.nodes || [];
                return nodes.some(n => n.type === 'kafka_input' || n.type === 'datagen_input');
            } catch (_) { return false; }
        }
        async function onlineJobById(row) {
            try {
                if (!isStreamingDag(row.dagJson) && !row.cronExpression) {
                    ElMessage.warning('批量作业上线前请先配置 cron（作业行「调度」按钮设置）');
                    return;
                }
                await api.onlineJob(row.id);
                ElMessage.success('作业已上线，进入受监管持续处理');
                await loadJobs();
            } catch (err) {
                ElMessage.error('上线失败: ' + (err.response?.data?.message || err.message));
                jobErrors.value[row.id] = err.response?.data?.message || err.message;
            }
        }
        async function offlineJobById(row) {
            try {
                await ElMessageBox.confirm('确定下线此作业？运行中的 Flink 作业将被取消，调度将停止。', '下线确认', { type: 'warning' });
                await api.offlineJob(row.id);
                ElMessage.success('作业已下线');
                await loadJobs();
            } catch (err) {
                if (err !== 'cancel') {
                    ElMessage.error('下线失败: ' + (err.response?.data?.message || err.message));
                }
            }
        }

        // 删除作业
        async function deleteJob(id) {
            try {
                await ElMessageBox.confirm('确定删除此作业？', '确认');
                await api.deleteJob(id);
                ElMessage.success('已删除');
                await loadJobs();
            } catch (err) {
                if (err !== 'cancel') {
                    ElMessage.error('删除失败');
                }
            }
        }


        // 作业详情抽屉 + 操作列“更多”菜单
        function openJobDetail(row) {
            jobDetailRow.value = row;
            jobDetailVisible.value = true;
        }
        function onJobMore(cmd, row) {
            if (cmd === 'copy') copyJobById(row.id);
            else if (cmd === 'schedule') openScheduleDialog(row);
            else if (cmd === 'deps') openDepDialog(row);
            else if (cmd === 'diagnose') openDiagnose(row);
            else if (cmd === 'logs') viewLogs(row);
            else if (cmd === 'history') openVersions(row);
            else if (cmd === 'lineage') openLineageDialog(row);
        }
        async function deleteJobFromDetail(id) {
            try {
                await ElMessageBox.confirm('确定删除此作业？', '确认');
                await api.deleteJob(id);
                ElMessage.success('已删除');
                jobDetailVisible.value = false;
                await loadJobs();
            } catch (err) {
                if (err !== 'cancel') ElMessage.error('删除失败');
            }
        }

        async function deleteMonitorJob(id) {
            try {
                await ElMessageBox.confirm('确定删除此作业？删除后监控卡片将移除。', '删除作业');
                await api.deleteJob(id);
                ElMessage.success('已删除');
                await loadJobs();
                await loadMonitor();
                loadTrends();
            } catch (err) {
                if (err !== 'cancel') {
                    ElMessage.error('删除失败');
                }
            }
        }

        // 复制作业
        async function copyJobById(id) {
            try {
                const copied = await api.copyJob(id);
                ElMessage.success('已复制为新作业: ' + copied.name);
                await loadJobs();
            } catch (err) {
                ElMessage.error('复制失败: ' + (err.response?.data?.message || err.message));
            }
        }

        // ===== 自然语言调度 =====
        const SCHEDULE_DOW = { 一: 1, 二: 2, 三: 3, 四: 4, 五: 5, 六: 6, 日: 7, 天: 7 };
        const SCHEDULE_WEEK_TEXT = { 0: '周日', 1: '周一', 2: '周二', 3: '周三', 4: '周四', 5: '周五', 6: '周六', 7: '周日' };
        function scheduleHour(period, h) {
            if (h >= 12) return (h === 12 && period === '凌晨') ? 0 : h;
            if (period === '下午' || period === '晚上' || period === '傍晚') return h + 12;
            return h;
        }
        function dowName(d) {
            const up = String(d).toUpperCase();
            const map = { SUN: '周日', MON: '周一', TUE: '周二', WED: '周三', THU: '周四', FRI: '周五', SAT: '周六' };
            if (map[up]) return map[up];
            if (/^\d+$/.test(d)) return SCHEDULE_WEEK_TEXT[Number(d) % 7] || d;
            return d;
        }
        function nlToCron(text) {
            const t = text.trim();
            let m;
            m = t.match(/^每\s*(\d+)\s*分钟$/); if (m) return { cron: '0 */' + m[1] + ' * * * *', text: '每 ' + m[1] + ' 分钟' };
            m = t.match(/^每分钟$/); if (m) return { cron: '0 * * * * *', text: '每 1 分钟' };
            m = t.match(/^每\s*(\d+)\s*小时$/); if (m) return { cron: '0 0 */' + m[1] + ' * * *', text: '每 ' + m[1] + ' 小时' };
            m = t.match(/^每小时$/); if (m) return { cron: '0 0 * * * *', text: '每小时' };
            m = t.match(/^每?天(凌晨|早上|早晨|上午|中午|下午|晚上|傍晚)?\s*(\d{1,2})\s*[点时:：]\s*(\d{1,2})?\s*(分|半)?$/);
            if (m) {
                const h = scheduleHour(m[1], Number(m[2]));
                const mi = m[4] === '半' ? '30' : (m[3] ? String(Number(m[3])).padStart(2, '0') : '00');
                return { cron: '0 ' + mi + ' ' + String(h).padStart(2, '0') + ' * * *', text: '每天 ' + String(h).padStart(2, '0') + ':' + mi };
            }
            m = t.match(/^每周([一二三四五六日天])(凌晨|早上|早晨|上午|中午|下午|晚上|傍晚)?\s*(\d{1,2})\s*[点时:：]\s*(\d{1,2})?\s*(分|半)?$/);
            if (m) {
                const h = scheduleHour(m[2], Number(m[3]));
                const mi = m[5] === '半' ? '30' : (m[4] ? String(Number(m[4])).padStart(2, '0') : '00');
                return { cron: '0 ' + mi + ' ' + String(h).padStart(2, '0') + ' * * ' + SCHEDULE_DOW[m[1]], text: '每周' + m[1] + ' ' + String(h).padStart(2, '0') + ':' + mi };
            }
            m = t.match(/^每月\s*(\d{1,2})\s*[号日](凌晨|早上|早晨|上午|中午|下午|晚上|傍晚)?\s*(\d{1,2})\s*[点时:：]\s*(\d{1,2})?\s*(分|半)?$/);
            if (m) {
                const d = String(Number(m[1]));
                const h = scheduleHour(m[2], Number(m[3]));
                const mi = m[5] === '半' ? '30' : (m[4] ? String(Number(m[4])).padStart(2, '0') : '00');
                return { cron: '0 ' + mi + ' ' + String(h).padStart(2, '0') + ' ' + d + ' * *', text: '每月 ' + d + ' 号 ' + String(h).padStart(2, '0') + ':' + mi };
            }
            return null;
        }
        function cronToHuman(cron) {
            if (!cron || !cron.trim()) return '';
            const p = cron.trim().split(/\s+/);
            if (p.length < 6) return '';
            const sec = p[0], min = p[1], hour = p[2], dom = p[3], mon = p[4], dow = p[5];
            const secIv = sec.match(/^\*\/(\d+)$/);
            if (secIv && min === '*') return '每 ' + secIv[1] + ' 秒';
            const minIv = min.match(/^\*\/(\d+)$/);
            if (minIv && hour === '*') return '每 ' + minIv[1] + ' 分钟';
            const hourIv = hour.match(/^\*\/(\d+)$/);
            if (hourIv && min === '0') return '每 ' + hourIv[1] + ' 小时';
            if (hour === '*' && min === '0') return '每小时';
            const time = (/^\d+$/.test(hour) && /^\d+$/.test(min)) ? hour.padStart(2, '0') + ':' + min.padStart(2, '0') : '';
            if (dom !== '*' && dom !== '?') return '每月 ' + dom + ' 号' + (time ? ' ' + time : '');
            if (dow !== '*' && dow !== '?') {
                const wd = dow.includes('-') ? dowName(dow.split('-')[0]) + '至' + dowName(dow.split('-')[1]) : dowName(dow);
                return wd + (time ? ' ' + time : '');
            }
            if (time) return '每天 ' + time;
            return '（自定义：' + cron + '）';
        }
        function applySchedulePreset(p) {
            cronExpression.value = p.cron;
            scheduleNlText.value = '';
            scheduleCronHuman.value = p.label;
        }
        function parseNlSchedule() {
            const text = (scheduleNlText.value || '').trim();
            if (!text) { ElMessage.warning('请先输入调度描述，例如：每5分钟'); return; }
            const r = nlToCron(text);
            if (!r) { ElMessage.warning('暂无法识别，试试：每5分钟 / 每小时 / 每天凌晨2点 / 每周一早上9点 / 每月1号0点'); return; }
            cronExpression.value = r.cron;
            scheduleNlText.value = '';
            scheduleCronHuman.value = r.text;
            ElMessage.success('已识别：' + r.text);
        }
        watch(cronExpression, (v) => { scheduleCronHuman.value = cronToHuman(v); });

        // 打开调度设置对话框
        function openScheduleDialog(row) {
            scheduleJob.value = row;
            scheduleEnabled.value = !!row.scheduleEnabled;
            cronExpression.value = row.cronExpression || '';
            scheduleMaxRetries.value = row.scheduleMaxRetries ?? 0;
            webhookUrl.value = row.webhookUrl || '';
            scheduleNlText.value = '';
            scheduleCronHuman.value = cronToHuman(row.cronExpression || '');
            scheduleDialogVisible.value = true;
            loadScheduleHistory();
        }

        async function loadScheduleHistory() {
            if (!scheduleJob.value) return;
            scheduleHistoryLoading.value = true;
            try {
                scheduleHistory.value = await api.getScheduleHistory(scheduleJob.value.id);
            } catch (err) {
                scheduleHistory.value = [];
                ElMessage.warning('调度历史加载失败: ' + (err.response?.data?.message || err.message));
            } finally {
                scheduleHistoryLoading.value = false;
            }
        }

        // 保存调度设置
        async function saveSchedule() {
            if (!scheduleJob.value) return;
            if (scheduleEnabled.value && !cronExpression.value.trim()) {
                ElMessage.warning('启用定时调度必须填写 cron 表达式');
                return;
            }
            try {
                const saved = await api.updateSchedule(scheduleJob.value.id, {
                    scheduleEnabled: scheduleEnabled.value,
                    cronExpression: cronExpression.value.trim(),
                    scheduleMaxRetries: Number(scheduleMaxRetries.value) || 0,
                    webhookUrl: webhookUrl.value.trim()
                });
                ElMessage.success('调度设置已保存');
                scheduleDialogVisible.value = false;
                await loadJobs();
            } catch (err) {
                ElMessage.error('保存失败: ' + (err.response?.data?.message || err.message));
            }
        }

        // 查看作业日志
        async function viewLogs(row) {
            if (!row || !row.id) {
                ElMessage.warning('作业不存在');
                return;
            }
            try {
                jobLogs.value = await api.getLogs(row.id);
                showLogs.value = true;
            } catch (err) {
                ElMessage.error('加载日志失败: ' + (err.response?.data?.message || err.message));
            }
        }

        // 查看作业版本历史
        async function openVersions(row) {
            if (!row || !row.id) { ElMessage.warning('作业不存在'); return; }
            versionJob.value = row;
            versionDialogVisible.value = true;
            versionLoading.value = true;
            try {
                jobVersions.value = await api.getVersions(row.id);
            } catch (err) {
                ElMessage.error('加载版本历史失败: ' + (err.response?.data?.message || err.message));
            } finally {
                versionLoading.value = false;
            }
        }

        // 回滚到指定版本
        async function rollbackVersion(versionId) {
            if (!versionJob.value) return;
            try {
                await ElMessageBox.confirm('确定回滚到版本 ' + versionId + ' 吗？当前 DAG 将被覆盖。', '回滚确认', { type: 'warning' });
            } catch (ignored) { return; }
            try {
                await api.rollbackJob(versionJob.value.id, versionId);
                ElMessage.success('已回滚');
                versionDialogVisible.value = false;
                await loadJobs();
                if (currentJobId === versionJob.value.id) {
                    const fresh = await api.getJob(versionJob.value.id);
                    loadDagToCanvas(JSON.parse(fresh.dagJson));
                }
            } catch (err) {
                ElMessage.error('回滚失败: ' + (err.response?.data?.message || err.message));
            }
        }

        // 状态标签颜色
        function statusTag(status) {
            const map = { DRAFT: 'info', SUBMITTED: 'warning', RUNNING: 'success', COMPLETED: 'success', FAILED: 'danger', CANCELLED: 'warning', WAITING: 'warning', BLOCKED: 'danger' };
            return map[status] || 'info';
        }

        // ===== 作业依赖编排 =====
        const depDialogVisible = ref(false);
        const depJob = ref(null);
        const depCandidates = ref([]);
        const depSelected = ref([]);
        const depLoading = ref(false);
        const depSaving = ref(false);
        async function openDepDialog(row) {
            depJob.value = row;
            depCandidates.value = jobs.value.filter(j => j.id !== row.id);
            depSelected.value = [];
            depDialogVisible.value = true;
            depLoading.value = true;
            try {
                const deps = await api.getDependencies(row.id);
                depSelected.value = deps.map(d => d.upstreamJobId);
            } catch (err) {
                ElMessage.error('加载依赖失败: ' + (err.response?.data?.message || err.message));
            } finally {
                depLoading.value = false;
            }
        }
        async function saveDeps() {
            if (!depJob.value) return;
            depSaving.value = true;
            try {
                const saved = await api.saveDependencies(depJob.value.id, depSelected.value);
                ElMessage.success('依赖关系已保存（上游 ' + saved.length + ' 个）');
                depDialogVisible.value = false;
                await loadJobs();
            } catch (err) {
                ElMessage.error('保存依赖失败: ' + (err.response?.data?.error || err.response?.data?.message || err.message));
            } finally {
                depSaving.value = false;
            }
        }

        // ===== 工作流视图 =====
        const workflowJobs = ref([]);
        const workflowEdges = ref([]);
        const workflowLoading = ref(false);
        const workflowError = ref('');
        const workflowUpdated = ref('');
        let workflowChart = null;
        function wfStatusColor(status) {
            const map = { DRAFT: '#94a3b8', SUBMITTED: '#f59e0b', RUNNING: '#3b82f6', COMPLETED: '#10b981', FAILED: '#ef4444', CANCELLED: '#8b5cf6', WAITING: '#eab308', BLOCKED: '#dc2626' };
            return map[status] || '#94a3b8';
        }
        function initWorkflowChart() {
            if (workflowChart || typeof echarts === 'undefined') return;
            const el = document.getElementById('workflowChart');
            if (!el || el.offsetParent === null) return;
            workflowChart = echarts.init(el);
            renderWorkflowChart();
        }
        function renderWorkflowChart() {
            if (!workflowChart) return;
            const shortName = (name) => {
                const s = String(name || '');
                return s.length > 12 ? s.slice(0, 12) + '…' : s;
            };
            const nodes = workflowJobs.value.map(j => ({
                id: String(j.id),
                name: String(j.id),
                symbolSize: 46,
                itemStyle: { color: wfStatusColor(j.status), shadowBlur: 10, shadowColor: 'rgba(0,0,0,0.18)' },
                label: { show: true, formatter: shortName(j.name), fontSize: 11, color: '#1f2937', position: 'bottom' },
                tooltip: { formatter: shortName(j.name) + ' (#' + j.id + ')<br/>状态: ' + j.status }
            }));
            const links = workflowEdges.value.map(e => ({ source: String(e.source), target: String(e.target) }));
            workflowChart.setOption({
                tooltip: { trigger: 'item', backgroundColor: 'rgba(255,255,255,0.96)', borderColor: '#e5e7eb', textStyle: { color: '#1f2937' } },
                series: [{
                    type: 'graph', layout: 'force', roam: true, draggable: true,
                    force: { repulsion: 480, edgeLength: 130, gravity: 0.08 },
                    edgeSymbol: ['none', 'arrow'], edgeSymbolSize: 8,
                    lineStyle: { color: '#94a3b8', width: 2, curveness: 0.08 },
                    data: nodes, links: links
                }]
            }, true);
        }
        async function loadWorkflow() {
            workflowLoading.value = true;
            workflowError.value = '';
            try {
                const data = await api.getWorkflow();
                workflowJobs.value = data.jobs || [];
                workflowEdges.value = data.edges || [];
                workflowUpdated.value = new Date().toLocaleTimeString('zh-CN');
                nextTick(initWorkflowChart);
                renderWorkflowChart();
            } catch (err) {
                if (!(err.response && err.response.status === 401)) {
                    workflowError.value = (err && err.message) ? err.message : String(err);
                }
            } finally {
                workflowLoading.value = false;
            }
        }

        // ===== 数据血缘弹窗 =====
        const lineageDialogVisible = ref(false);
        const lineageJob = ref(null);
        const lineageAssets = ref([]);
        const lineageLoading = ref(false);
        let lineageChart = null;
        function lineageColor(kind, direction) {
            if (direction === 'input') return '#3b82f6';
            return '#f59e0b';
        }
        function initLineageChart() {
            if (lineageChart || typeof echarts === 'undefined') return;
            const el = document.getElementById('lineageChart');
            if (!el || el.offsetParent === null) return;
            lineageChart = echarts.init(el);
            renderLineageChart();
        }
        function renderLineageChart() {
            if (!lineageChart) return;
            const j = lineageJob.value || {};
            const inputs = lineageAssets.value.filter(a => a.direction === 'input');
            const outputs = lineageAssets.value.filter(a => a.direction === 'output');
            const nodes = [{ id: 'job', name: (j.name || '作业') + '\n(#' + j.id + ')', x: 300, y: 160, symbolSize: 62, itemStyle: { color: '#6366f1' } }];
            const links = [];
            inputs.forEach((a, i) => {
                const id = 'in_' + i;
                nodes.push({ id, name: a.asset, x: 90, y: 60 + i * 80, symbolSize: 34, itemStyle: { color: '#3b82f6' }, category: 'input' });
                links.push({ source: id, target: 'job' });
            });
            outputs.forEach((a, i) => {
                const id = 'out_' + i;
                nodes.push({ id, name: a.asset, x: 520, y: 60 + i * 80, symbolSize: 34, itemStyle: { color: '#f59e0b' }, category: 'output' });
                links.push({ source: 'job', target: id });
            });
            lineageChart.setOption({
                tooltip: { trigger: 'item' },
                series: [{
                    type: 'graph', layout: 'none', roam: true,
                    label: { show: true, fontSize: 11, color: '#334155', width: 170, overflow: 'break' },
                    edgeSymbol: ['none', 'arrow'], edgeSymbolSize: 8,
                    lineStyle: { color: '#94a3b8', width: 1.5 },
                    data: nodes, links
                }]
            }, true);
        }
        async function openLineageDialog(row) {
            lineageJob.value = row;
            lineageAssets.value = [];
            lineageDialogVisible.value = true;
            lineageLoading.value = true;
            try {
                const data = await api.getJobLineage(row.id);
                lineageAssets.value = data.assets || [];
                lineageJob.value = data.job || row;
                nextTick(initLineageChart);
                renderLineageChart();
            } catch (err) {
                ElMessage.error('加载血缘失败: ' + (err.response?.data?.message || err.message));
            } finally {
                lineageLoading.value = false;
            }
        }

        // 键盘快捷键
        function handleKeydown(e) {
            if ((e.ctrlKey || e.metaKey) && e.key.toLowerCase() === 'z') {
                e.preventDefault();
                if (e.shiftKey) { redoDag(); } else { undoDag(); }
                return;
            }
            if ((e.ctrlKey || e.metaKey) && e.key.toLowerCase() === 'y') {
                e.preventDefault();
                redoDag();
                return;
            }
            if (e.ctrlKey && e.key === 's') {
                e.preventDefault();
                saveDag();
            }
            if (e.key === 'Delete') {
                if (selectedNode.value && graph) {
                    graph.removeNode(selectedNode.value);
                    selectedNode.value = null;
                }
            }
        }

        watch(currentView, (v) => {
            if (v === 'monitor') { loadMonitor(); startMonitorPolling(); nextTick(initTrendChart); }
            else if (v === 'audit') { loadAudit(); }
            else if (v === 'workflow') { loadWorkflow(); }
            else if (v === 'alerts') { loadAlerts(); }
            else { stopMonitorPolling(); }
        });

        watch(monitorAutoRefresh, (on) => { if (on) { startMonitorPolling(); } else { stopMonitorPolling(); } });

        onMounted(async () => {
            const savedToken = localStorage.getItem('token');
            if (savedToken) {
                token.value = savedToken;
                try {
                    user.value = await api.me();
                } catch (_) {
                    token.value = '';
                    localStorage.removeItem('token');
                }
            }
            if (user.value) {
                await loadControls();
                await loadJobs();
                loadAlerts();
                alertTimer = setInterval(() => { if (user.value) loadAlerts(); }, 30000);
                // 登录态下延迟初始化画布
                setTimeout(() => {
                    initGraph();
                    setupDropHandler();
                }, 100);
            }

            document.addEventListener('keydown', handleKeydown);
        });

        return {
            currentView, controls, jobs, jobSearch, jobStatusFilter, filteredJobs, jobName, parallelism, currentJobName,
            controlTab, saving, submitting, showHelp, showLogs, jobLogs,
            selectedNode, nodeParams, nodeParamSchema, jobErrors,
            previewVisible, previewLoading, previewData, previewRows, previewNode,
            controlsByCategory, onDragStart, saveDag, submitJob,
            exportDag, importDagTrigger, importDag, clearCanvas, newCanvas, applyParams,
            openJob, submitJobById, deleteJob, deleteJobFromDetail, viewLogs, statusTag, undoDag, redoDag, canUndo, canRedo,
            openJobDetail, onJobMore, jobCols, jobDetailVisible, jobDetailRow,
            onlineJobById, offlineJobById,
            copyJobById, deleteMonitorJob, openScheduleDialog, saveSchedule, loadScheduleHistory,
            openVersions, rollbackVersion, versionDialogVisible, jobVersions, versionLoading,
            loadSampleDag, SAMPLE_DAGS,
            scheduleDialogVisible, scheduleJob, scheduleEnabled, cronExpression,
            scheduleMaxRetries, webhookUrl, scheduleHistory, scheduleHistoryLoading,
            schedulePresets, scheduleNlText, scheduleCronHuman, applySchedulePreset, parseNlSchedule, cronToHuman,
            monitorJobs, monitorLoading, monitorError, monitorAutoRefresh, monitorLastUpdated, monitorStats, refreshMonitor,
            monitorTrends, monitorTrendUpdated,
            fmtNum, bpClass, cpText, fmtDuration, statusTagType,
            user, canEdit, canViewAudit, loginUsername, loginPassword, loginError, loginLoading,
            login, logout, onlyMine, helpTab, openHelp,
            auditRows, auditTotal, auditPage, auditSize, auditKeyword, auditLoading, loadAudit, onAuditPage,
            depDialogVisible, depJob, depCandidates, depSelected, depLoading, depSaving, openDepDialog, saveDeps,
            workflowJobs, workflowEdges, workflowLoading, workflowError, workflowUpdated, loadWorkflow,
            lineageDialogVisible, lineageJob, lineageAssets, lineageLoading, openLineageDialog,
            aiPrompt, aiLoading, aiError, aiResult, aiResultDag, aiExamples, generatePipeline, openAiResultInCanvas,
            alerts, alertUnread, alertsLoading, loadAlerts, markAlertRead, markAllRead, deleteAlert,
            diagnoseVisible, diagnoseLoading, diagnoseResult, diagnoseJob, hasParamFixes, openDiagnose, runDiagnose, applyDiagnoseFixes
        };
    }
});

// 注册 Element Icon 组件
for (const [key, component] of Object.entries(ElementPlusIconsVue)) {
    app.component(key, component);
}

app.use(ElementPlus);
app.mount('#app');
