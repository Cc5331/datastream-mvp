// ===== API 配置 =====
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
    async confirmJob(id) {
        const res = await axios.post(`${API_BASE}/jobs/${id}/confirm`);
        return res.data;
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
    async batchOnlineJobs(ids, scheduleEnabled, cronExpression) {
        const res = await axios.post(`${API_BASE}/jobs/batch-online`, { ids, scheduleEnabled, cronExpression });
        return res.data;
    },
    async batchOfflineJobs(ids) {
        const res = await axios.post(`${API_BASE}/jobs/batch-offline`, { ids });
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
    async previewNode(jobId, nodeId, limit = 20) {
        const res = await axios.post(`${API_BASE}/preview/node`, { jobId, nodeId, limit });
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
    async removeMonitorTrend(jobId) {
        const res = await axios.delete(`${API_BASE}/monitor/trends/${jobId}`);
        return res.data;
    },
    async login(username, password) {
        const res = await axios.post(`${API_BASE}/auth/login`, { username, password });
        return res.data;
    },
    async register(username, displayName, password) {
        const res = await axios.post(`${API_BASE}/auth/register`, { username, displayName, password });
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
    async getAiModels() {
        const res = await axios.get(API_BASE + '/ai/models');
        return res.data;
    },
    async getAiConfig() {
        const res = await axios.get(API_BASE + '/ai/config');
        return res.data;
    },
    async updateAiConfig(config) {
        const res = await axios.put(API_BASE + '/ai/config', config);
        return res.data;
    },
    async testAiConnection(model) {
        const res = await axios.post(API_BASE + '/ai/test', { model });
        return res.data;
    },
    async getAiProviders() {
        const res = await axios.get(API_BASE + '/ai/providers');
        return res.data;
    },
    async saveAiProvider(provider) {
        const res = await axios.post(API_BASE + '/ai/providers', provider);
        return res.data;
    },
    async activateAiProvider(id) {
        const res = await axios.post(`${API_BASE}/ai/providers/${encodeURIComponent(id)}/activate`);
        return res.data;
    },
    async testAiProvider(id, model) {
        const res = await axios.post(`${API_BASE}/ai/providers/${encodeURIComponent(id)}/test`, { model });
        return res.data;
    },
    async deleteAiProvider(id) {
        await axios.delete(`${API_BASE}/ai/providers/${encodeURIComponent(id)}`);
    },
    async aiNl2Pipeline(prompt, model) {
        const res = await axios.post(API_BASE + '/ai/nl2pipeline', { prompt, model });
        return res.data;
    },
    async aiDiagnose(jobId, model) {
        const params = model ? { model } : {};
        const res = await axios.post(API_BASE + '/ai/diagnose/' + jobId, null, { params });
        return res.data;
    },
    async getAlerts(page = 0, size = 20, read = null) {
        const params = { page, size };
        if (read !== null) params.read = read;
        const res = await axios.get(API_BASE + '/alerts', { params });
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
    },
    async batchMarkAlertsRead(ids) {
        const res = await axios.post(API_BASE + '/alerts/batch-read', ids);
        return res.data;
    },
    async batchDeleteAlerts(ids) {
        const res = await axios.post(API_BASE + '/alerts/batch-delete', ids);
        return res.data;
    },
    async getKafkaTopics() {
        const res = await axios.get(API_BASE + '/kafka/topics');
        return res.data;
    },
    async getWorkbench() {
        const res = await axios.get(API_BASE + '/dashboard/workbench');
        return res.data;
    },
    async getAdminOverview() {
        const res = await axios.get(API_BASE + '/dashboard/admin-overview');
        return res.data;
    },
    async getDataSourceCatalog() {
        const res = await axios.get(API_BASE + '/data-sources/catalog');
        return res.data;
    },
    async getJobTimeline(jobId, scope = 'ALL') {
        const res = await axios.get(`${API_BASE}/jobs/${jobId}/timeline`, { params: { scope } });
        return res.data;
    },
    async getPreflight(jobId) {
        const res = await axios.get(`${API_BASE}/jobs/${jobId}/preflight`);
        return res.data;
    },
    async getClusterHealth() {
        const res = await axios.get(API_BASE + '/cluster/health');
        return res.data;
    }
};

// ===== 颜色工具 =====
const CATEGORY_COLORS = {
    input: { bg: '#e1f3d8', border: '#67c23a', label: '输入', icon: '📥' },
    transform: { bg: '#d9ecff', border: '#409eff', label: '转换', icon: '🔧' },
    output: { bg: '#fdf0d9', border: '#e6a23c', label: '输出', icon: '📤' }
};

// ===== 示例作业（一键导入画布）=====
const DEFAULT_OUTPUT_DIR = '/output';
const SAMPLE_DAGS = {
    'datagen2csv': {
        jobName: '示例：Datagen → CSV',
        parallelism: 1,
        nodes: [
            { id: 'dg_1', type: 'datagen_input', label: 'Datagen', params: { rowsPerSecond: '50', fieldsConfig: '[{"name":"id","type":"INT"},{"name":"name","type":"STRING"},{"name":"score","type":"DOUBLE"}]' }, x: 120, y: 140 },
            { id: 'csv_out_1', type: 'csv_output', label: 'CSV 输出', params: { path: DEFAULT_OUTPUT_DIR + '/sample_output.csv', hasHeader: 'true', delimiter: ',' }, x: 430, y: 140 }
        ],
        edges: [ { id: 'e1', source: 'dg_1', target: 'csv_out_1' } ]
    },
    'datagen2json': {
        jobName: '示例：Datagen → JSON',
        parallelism: 1,
        nodes: [
            { id: 'dg_1', type: 'datagen_input', label: 'Datagen', params: { rowsPerSecond: '50', fieldsConfig: '[{"name":"id","type":"INT"},{"name":"name","type":"STRING"}]' }, x: 120, y: 140 },
            { id: 'json_out_1', type: 'json_output', label: 'JSON 输出', params: { path: DEFAULT_OUTPUT_DIR + '/sample_output.json', mode: 'lines' }, x: 430, y: 140 }
        ],
        edges: [ { id: 'e1', source: 'dg_1', target: 'json_out_1' } ]
    },
    'csv2csv_filter': {
        jobName: '示例：CSV → 字段过滤 → CSV',
        parallelism: 1,
        nodes: [
            { id: 'csv_in_1', type: 'csv_input', label: 'CSV 输入', params: { path: '/data/sales.csv', hasHeader: 'true', delimiter: ',' }, x: 120, y: 140 },
            { id: 'ff_1', type: 'field_filter', label: '字段过滤', params: { fields: 'sale_id,amount' }, x: 330, y: 140 },
            { id: 'csv_out_1', type: 'csv_output', label: 'CSV 输出', params: { path: DEFAULT_OUTPUT_DIR + '/sample_filtered.csv', hasHeader: 'true', delimiter: ',' }, x: 540, y: 140 }
        ],
        edges: [ { id: 'e1', source: 'csv_in_1', target: 'ff_1' }, { id: 'e2', source: 'ff_1', target: 'csv_out_1' } ]
    }
};

// 模板中心在 SAMPLE_DAGS 的合法 DAG 上叠加展示元数据，不改变 DAG 契约。
const BUILTIN_TEMPLATES = [
    { key: 'datagen2csv', category: '实时采集', title: '模拟数据写入 CSV', description: '快速搭建 Datagen 到 CSV 的流式采集链路', tags: ['Datagen', 'CSV'] },
    { key: 'datagen2json', category: '实时采集', title: '模拟数据写入 JSON', description: '生成结构化测试数据并输出 JSON Lines', tags: ['Datagen', 'JSON'] },
    { key: 'csv2csv_filter', category: '批量处理', title: 'CSV 字段筛选', description: '读取 CSV、保留指定字段并写入新文件', tags: ['CSV', '字段过滤'] }
].map(meta => ({ ...meta, dag: SAMPLE_DAGS[meta.key] }));

// ===== Vue App =====
const { createApp, ref, reactive, computed, onMounted, onUnmounted, watch, nextTick, toRaw } = Vue;
const { ElMessage, ElMessageBox } = ElementPlus;

const app = createApp({
    setup() {
        // 状态
        const currentView = ref('workbench');
        const controls = ref([]);
        const jobs = ref([]);
        const selectedJobs = ref([]);
        const batchOperating = ref(false);
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
        const batchOnlineDialogVisible = ref(false);
        const batchScheduleEnabled = ref(true);
        const batchCronExpression = ref('0 */5 * * * *');
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
        const monitorAutoScale = ref(true);
        const monitorTrendMode = ref('separate');
        const monitorLastUpdated = ref('');
        const monitorTrends = ref([]);
        const monitorTrendUpdated = ref('');
        let trendChart = null;
        const canUndo = ref(false);
        const canRedo = ref(false);
        let monitorTimer = null;
        let trendTimer = null;
        let alertTimer = null;
        let clusterTimer = null;

        // ===== 首期门户视图 =====
        const workbench = ref({ statusCounts: {}, unreadAlerts: 0, recentJobs: [] });
        const workbenchLoading = ref(false);
        const templates = ref(BUILTIN_TEMPLATES);
        const templateKeyword = ref('');
        const templateCategory = ref('');
        const templatePreviewVisible = ref(false);
        const templatePreview = ref(null);
        const filteredTemplates = computed(() => templates.value.filter(item => {
            const keyword = templateKeyword.value.trim().toLowerCase();
            return (!templateCategory.value || item.category === templateCategory.value)
                && (!keyword || [item.title, item.description, item.category, ...(item.tags || [])].join(' ').toLowerCase().includes(keyword));
        }));
        const dataSourceCatalog = ref({ connectors: [], assets: [] });
        const dataSourcesLoading = ref(false);
        const adminOverview = ref({});
        const adminOverviewLoading = ref(false);
        const clusterHealth = ref({});
        const clusterLoading = ref(false);
        const clusterLastUpdated = ref('');
        const timelineVisible = ref(false);
        const timelineLoading = ref(false);
        const timelineScope = ref('ALL');
        const timelineJob = ref(null);
        const timelineEvents = ref([]);
        const preflightVisible = ref(false);
        const preflightResult = ref({ errors: [], warnings: [] });

        // ===== AI 助手 / 告警中心 =====
        const aiPrompt = ref('');
        const aiModels = ref([]);
        const aiSelectedModel = ref('');
        const aiProvider = ref('');
        const aiConfigured = ref(false);
        const aiConfigVisible = ref(false);
        const aiProvidersVisible = ref(false);
        const aiProviders = ref([]);
        const aiProviderOperating = ref('');
        const aiConfigSaving = ref(false);
        const aiTesting = ref(false);
        const aiConfigForm = reactive({ id: '', name: '', provider: 'openai', baseUrl: 'https://api.openai.com/v1', apiKey: '', defaultModel: 'gpt-4.1-mini', modelsText: 'gpt-4.1-mini,gpt-4.1,gpt-4o-mini', wireApi: 'chat_completions', reasoningEffort: 'medium', storeResponses: false });
        const aiLoading = ref(false);
        const aiError = ref('');
        const aiResult = ref(null);
        const aiResultDag = ref(null);
        const aiExamples = [
            '读取 D:\\code\\比赛\\2026省服务外包\\data\\sales.csv，把 product_category 和 channel 拼接成新列 category_channel，写出到 MySQL 表 ai_demo',
            '读取 D:\\code\\比赛\\2026省服务外包\\data\\sales.csv，只保留 sale_id/region/amount 三列，写到 D:\\code\\比赛\\2026省服务外包\\output\\ai_filtered.csv',
            '从 MySQL 表 flink_demo.user_data 读取数据，过滤 id > 1，输出到 JSON 文件'
        ];
        const alerts = ref([]);
        const alertUnread = ref(0);
        const alertsLoading = ref(false);
        const alertsError = ref('');
        const alertPage = ref(1);
        const alertSize = ref(20);
        const alertTotal = ref(0);
        const selectedAlerts = ref([]);
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
        const authMode = ref('login');
        const loginUsername = ref('');
        const loginPassword = ref('');
        const loginError = ref('');
        const loginLoading = ref(false);
        const registerUsername = ref('');
        const registerDisplayName = ref('');
        const registerPassword = ref('');
        const registerPasswordConfirm = ref('');
        const registerAgreed = ref(false);
        const registerError = ref('');
        const registerLoading = ref(false);
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
                    stopAlertPolling();
                    alerts.value = [];
                    alertUnread.value = 0;
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
                { type: 'datagen_input', name: 'Datagen', category: 'input', description: '生成测试数据（按行/秒持续产生）', version: '1.0.0', paramSchema: '{"type":"object","properties":{"rowsPerSecond":{"type":"number","title":"Rows/s","default":10}},"required":[]}' },
                { type: 'csv_input', name: 'CSV 输入', category: 'input', description: '读取 CSV 文件作为数据源', version: '1.0.0', paramSchema: '{"type":"object","properties":{"path":{"type":"string","title":"文件路径","default":"/data/input.csv"},"delimiter":{"type":"string","title":"分隔符","default":","},"hasHeader":{"type":"boolean","title":"包含表头","default":true}},"required":["path"]}' },
                { type: 'csv_output', name: 'CSV 输出', category: 'output', description: '将数据写入 CSV 文件', version: '1.0.0', paramSchema: '{"type":"object","properties":{"path":{"type":"string","title":"输出路径","default":"D:\\\\code\\\\比赛\\\\2026省服务外包\\\\output\\\\output.csv"},"delimiter":{"type":"string","title":"分隔符","default":","},"sheetName":{"type":"string","title":"工作表名（默认 Data，多输出写同一文件时可区分 sheet）","default":"Data"}},"required":["path"]}' },
                { type: 'excel_input', name: 'Excel 输入', category: 'input', description: '读取 Excel (.xlsx) 文件', version: '1.0.0', paramSchema: '{"type":"object","properties":{"path":{"type":"string","title":"文件路径","default":"/data/input.xlsx"},"sheetName":{"type":"string","title":"工作表名（留空取第一个）","default":""}},"required":["path"]}' },
                { type: 'mysql_input', name: 'MySQL 输入', category: 'input', description: '从 MySQL 表读取数据', version: '1.0.0', paramSchema: '{"type":"object","properties":{"url":{"type":"string","title":"JDBC URL","default":"jdbc:mysql://localhost:3306/flink_demo?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=Asia/Shanghai"},"table":{"type":"string","title":"表名","default":"source_table"},"username":{"type":"string","title":"用户名","default":"${MYSQL_USERNAME}"},"password":{"type":"string","title":"密码（留空或 \\\\${MYSQL_PASSWORD} 取环境变量）","default":"${MYSQL_PASSWORD}"}},"required":["url","table","username"]}' },
                { type: 'pg_input', name: 'PostgreSQL 输入', category: 'input', description: '从 PostgreSQL 表读取数据（JDBC 自动推导字段）', version: '1.0.0', paramSchema: '{"type":"object","properties":{"url":{"type":"string","title":"JDBC URL","default":"jdbc:postgresql://localhost:5432/dataflow"},"table":{"type":"string","title":"表名","default":"source_table"},"username":{"type":"string","title":"用户名","default":"${POSTGRES_USERNAME}"},"password":{"type":"string","title":"密码","default":"${POSTGRES_PASSWORD}"}},"required":["url","table"]}' },
                { type: 'pg_output', name: 'PostgreSQL 输出', category: 'output', description: '写入 PostgreSQL 表（可选安全自动建表）', version: '1.0.0', paramSchema: '{"type":"object","properties":{"url":{"type":"string","title":"JDBC URL","default":"jdbc:postgresql://localhost:5432/dataflow"},"schema":{"type":"string","title":"Schema","default":"public"},"table":{"type":"string","title":"表名","default":"target_table"},"username":{"type":"string","title":"用户名","default":"${POSTGRES_USERNAME}"},"password":{"type":"string","title":"密码","default":"${POSTGRES_PASSWORD}"},"createTablePolicy":{"type":"string","title":"建表策略","enum":["FAIL_IF_MISSING","CREATE_IF_MISSING","VALIDATE_EXISTING"],"default":"FAIL_IF_MISSING"},"writeMode":{"type":"string","title":"写入模式","enum":["append"],"default":"append"},"batchSize":{"type":"number","title":"批量条数","default":1000}},"required":["url","schema","table"]}' },
                { type: 'oracle_input', name: 'Oracle 输入', category: 'input', description: '从 Oracle 表读取数据（JDBC 自动推导字段，表名大写）', version: '1.0.0', paramSchema: '{"type":"object","properties":{"url":{"type":"string","title":"JDBC URL","default":"jdbc:oracle:thin:@localhost:1521/FREEPDB1"},"schema":{"type":"string","title":"Schema","default":"DATAFLOW"},"table":{"type":"string","title":"表名（大写）","default":"SOURCE_TABLE"},"username":{"type":"string","title":"用户名","default":"${ORACLE_USERNAME}"},"password":{"type":"string","title":"密码","default":"${ORACLE_PASSWORD}"}},"required":["url","table"]}' },
                { type: 'oracle_output', name: 'Oracle 输出', category: 'output', description: '写入 Oracle 表（可选安全自动建表）', version: '1.0.0', paramSchema: '{"type":"object","properties":{"url":{"type":"string","title":"JDBC URL","default":"jdbc:oracle:thin:@localhost:1521/FREEPDB1"},"schema":{"type":"string","title":"Schema","default":"DATAFLOW"},"table":{"type":"string","title":"表名","default":"TARGET_TABLE"},"username":{"type":"string","title":"用户名","default":"${ORACLE_USERNAME}"},"password":{"type":"string","title":"密码","default":"${ORACLE_PASSWORD}"},"createTablePolicy":{"type":"string","title":"建表策略","enum":["FAIL_IF_MISSING","CREATE_IF_MISSING","VALIDATE_EXISTING"],"default":"FAIL_IF_MISSING"},"writeMode":{"type":"string","title":"写入模式","enum":["append"],"default":"append"},"batchSize":{"type":"number","title":"批量条数","default":500}},"required":["url","schema","table"]}' },
                { type: 'redis_lookup', name: 'Redis 富化', category: 'transform', description: '根据字段A查询 Redis 扩充新字段（GET keyPrefix+值）', version: '1.0.0', paramSchema: '{"type":"object","properties":{"host":{"type":"string","title":"Redis 地址","default":"localhost"},"port":{"type":"number","title":"端口","default":6379},"password":{"type":"string","title":"密码（留空无密码）","default":"redis123"},"keyField":{"type":"string","title":"字段A（值作 Redis key）","default":"id"},"keyPrefix":{"type":"string","title":"key 前缀（如 user: → GET user:1）","default":""},"targetField":{"type":"string","title":"扩充字段名","default":"extra_info"}},"required":["keyField","targetField"]}' },
                { type: 'hdfs_input', name: 'HDFS 输入', category: 'input', description: '读取 HDFS 上的 CSV 文件（hdfs://，字段需声明）', version: '1.0.0', paramSchema: '{"type":"object","properties":{"path":{"type":"string","title":"HDFS 路径","default":"hdfs://localhost:9000/data/input.csv"},"delimiter":{"type":"string","title":"分隔符","default":","},"hasHeader":{"type":"boolean","title":"首行是表头（读入后自动过滤）","default":true},"fieldsConfig":{"type":"string","title":"字段定义（JSON 数组）","default":"[{\\"name\\":\\"id\\",\\"type\\":\\"INT\\"},{\\"name\\":\\"name\\",\\"type\\":\\"STRING\\"}]"}},"required":["path","fieldsConfig"]}' },
                { type: 'hdfs_output', name: 'HDFS 输出', category: 'output', description: '将数据写出到 HDFS CSV 目录（hdfs://，目录内生成 part 文件）', version: '1.0.0', paramSchema: '{"type":"object","properties":{"path":{"type":"string","title":"HDFS 输出目录","default":"hdfs://localhost:9000/output/result"},"delimiter":{"type":"string","title":"分隔符","default":","}},"required":["path"]}' },
                { type: 'mysql_output', name: 'MySQL 输出', category: 'output', description: '将数据写入 MySQL 表', version: '1.0.0', paramSchema: '{"type":"object","properties":{"url":{"type":"string","title":"JDBC URL","default":"jdbc:mysql://localhost:3306/flink_demo?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=Asia/Shanghai"},"table":{"type":"string","title":"表名","default":"target_table"},"username":{"type":"string","title":"用户名","default":"${MYSQL_USERNAME}"},"password":{"type":"string","title":"密码（留空或 \\\\${MYSQL_PASSWORD} 取环境变量）","default":"${MYSQL_PASSWORD}"}},"required":["url","table","username"]}' },
                { type: 'kafka_input', name: 'Kafka 输入', category: 'input', description: '从 Kafka 主题读取消息流', version: '1.0.0', paramSchema: '{"type":"object","properties":{"topic":{"type":"string","title":"主题","default":"input-topic"},"bootstrapServers":{"type":"string","title":"Bootstrap Servers","default":"localhost:9092"},"fieldsConfig":{"type":"string","title":"字段定义（JSON 数组）","default":"[{\\"name\\":\\"key\\",\\"type\\":\\"STRING\\"},{\\"name\\":\\"value\\",\\"type\\":\\"STRING\\"}]"},"autoStop":{"type":"boolean","title":"体验模式：消费完自动停止","default":false},"stopAfterSeconds":{"type":"number","title":"自动停止延迟（秒）","default":30}},"required":["topic","bootstrapServers"]}' },
                { type: 'kafka_output', name: 'Kafka 输出', category: 'output', description: '将数据写入 Kafka 主题', version: '1.0.0', paramSchema: '{"type":"object","properties":{"topic":{"type":"string","title":"主题","default":"output-topic"},"bootstrapServers":{"type":"string","title":"Bootstrap Servers","default":"localhost:9092"}},"required":["topic","bootstrapServers"]}' },
                { type: 'excel_output', name: 'Excel 输出', category: 'output', description: '将数据写出为 Excel (.xlsx) 文件（内部 CSV→Excel 转换）', version: '1.0.0', paramSchema: '{"type":"object","properties":{"path":{"type":"string","title":"输出路径","default":"D:\\\\code\\\\比赛\\\\2026省服务外包\\\\output\\\\output.xlsx"},"delimiter":{"type":"string","title":"分隔符","default":","},"sheetName":{"type":"string","title":"工作表名（默认 Data，多输出写同一文件时可区分 sheet）","default":"Data"}},"required":["path"]}' },
                { type: 'parquet_input', name: 'Parquet 输入', category: 'input', description: '读取 Parquet (.parquet) 文件', version: '1.0.0', paramSchema: '{"type":"object","properties":{"path":{"type":"string","title":"文件路径","default":"D:\\\\code\\\\比赛\\\\2026省服务外包\\\\output\\\\input.parquet"},"delimiter":{"type":"string","title":"分隔符","default":","}},"required":["path"]}' },
                { type: 'parquet_output', name: 'Parquet 输出', category: 'output', description: '将数据写出为 Parquet (.parquet) 文件（内部 CSV→Parquet 转换）', version: '1.0.0', paramSchema: '{"type":"object","properties":{"path":{"type":"string","title":"输出路径","default":"D:\\\\code\\\\比赛\\\\2026省服务外包\\\\output\\\\output.parquet"},"delimiter":{"type":"string","title":"分隔符","default":","}},"required":["path"]}' },
                { type: 'field_concat', name: '字段拼接', category: 'transform', description: '将多个字段拼接为一个新字段', version: '1.0.0', paramSchema: '{"type":"object","properties":{"fields":{"type":"array","title":"输入字段","items":{"type":"string"}},"separator":{"type":"string","title":"分隔符","default":","},"newFieldName":{"type":"string","title":"新字段名","default":"concat_field"}},"required":["fields","newFieldName"]}' },
                { type: 'xml_json', name: 'XML<->JSON', category: 'transform', description: 'XML 和 JSON 格式互转', version: '1.0.0', paramSchema: '{"type":"object","properties":{"direction":{"type":"string","title":"转换方向","enum":["xml2json","json2xml"],"default":"xml2json"},"sourceField":{"type":"string","title":"源字段","default":"payload"},"targetField":{"type":"string","title":"目标字段","default":"result"}},"required":["direction","sourceField"]}' },
                { type: 'dedupe', name: '去重', category: 'transform', description: '按字段去重（留空=整行去重）', version: '1.0.0', paramSchema: '{"type":"object","properties":{"dedupeFields":{"type":"string","title":"去重字段（逗号分隔，留空=整行去重）","default":""}},"required":[]}' },
                { type: 'validate', name: '空值校验', category: 'transform', description: '丢弃指定字段为空的行', version: '1.0.0', paramSchema: '{"type":"object","properties":{"checkFields":{"type":"string","title":"必填字段（逗号分隔）","default":"id"},"ignoreEmpty":{"type":"boolean","title":"空字符串也算空值","default":true}},"required":["checkFields"]}' },
                { type: 'route', name: '条件路由', category: 'transform', description: '按字段值分流：第一条出边=匹配，其余=不匹配', version: '1.0.0', paramSchema: '{"type":"object","properties":{"routeField":{"type":"string","title":"路由字段","default":"status"},"matchValues":{"type":"string","title":"匹配值（逗号分隔）","default":"SUCCESS"}},"required":["routeField","matchValues"]}' },
                { type: 'field_filter', name: '字段过滤', category: 'transform', description: '只保留指定字段', version: '1.0.0', paramSchema: '{"type":"object","properties":{"fields":{"type":"string","title":"保留字段（逗号分隔）","default":"id,name"}},"required":["fields"]}' },
                { type: 'field_rename', name: '字段改名', category: 'transform', description: '字段重命名', version: '1.0.0', paramSchema: '{"type":"object","properties":{"mappings":{"type":"string","title":"改名映射（old=new，逗号分隔）","default":"id=userId"}},"required":["mappings"]}' },
                { type: 'row_filter', name: '行过滤', category: 'transform', description: '按条件过滤行', version: '1.0.0', paramSchema: '{"type":"object","properties":{"condition":{"type":"string","title":"过滤条件（SQL WHERE 表达式）","default":"age > 18"}},"required":["condition"]}' },
                { type: 'json_parse', name: 'JSON 解析', category: 'transform', description: '从 JSON 字段解析出多个字段', version: '1.0.0', paramSchema: '{"type":"object","properties":{"sourceField":{"type":"string","title":"JSON 源字段","default":"payload"},"fieldsConfig":{"type":"string","title":"解析字段（JSON 数组）","default":"[{\\"name\\":\\"id\\",\\"type\\":\\"INT\\"},{\\"name\\":\\"name\\",\\"type\\":\\"STRING\\"}]"}},"required":["sourceField","fieldsConfig"]}' },
                { type: 'json_input', name: 'JSON 输入', category: 'input', description: '读取 JSON 文件（auto 自动识别 / lines 逐行 / array 数组）', version: '1.0.0', paramSchema: '{"type":"object","properties":{"path":{"type":"string","title":"文件路径","default":"D:\\\\code\\\\比赛\\\\2026省服务外包\\\\test-resources\\\\data\\\\sample.json"},"mode":{"type":"string","title":"文件模式","enum":["auto","lines","array"],"default":"auto"},"delimiter":{"type":"string","title":"分隔符","default":","},"encoding":{"type":"string","title":"文件编码","default":"UTF-8"},"fieldsConfig":{"type":"string","title":"字段定义（JSON 数组，lines 模式用）","default":"[{\\"name\\":\\"id\\",\\"type\\":\\"INT\\"},{\\"name\\":\\"name\\",\\"type\\":\\"STRING\\"}]"}},"required":["path"]}' },
                { type: 'json_output', name: 'JSON 输出', category: 'output', description: '将数据写入 JSON 文件（lines 逐行 / array 数组）', version: '1.0.0', paramSchema: '{"type":"object","properties":{"path":{"type":"string","title":"输出路径（.json）","default":"D:\\\\code\\\\比赛\\\\2026省服务外包\\\\output\\\\output.json"},"mode":{"type":"string","title":"输出模式","enum":["lines","array"],"default":"lines"}},"required":["path"]}' },
                { type: 'xml_input', name: 'XML 输入', category: 'input', description: '读取 XML 文件（记录列表结构，嵌套子结构保留为 JSON）', version: '1.0.0', paramSchema: '{"type":"object","properties":{"path":{"type":"string","title":"文件路径","default":"D:\\\\code\\\\比赛\\\\2026省服务外包\\\\test-resources\\\\data\\\\orders.xml"},"rowTag":{"type":"string","title":"行元素名（留空自动探测）","default":""},"delimiter":{"type":"string","title":"分隔符","default":","},"encoding":{"type":"string","title":"文件编码","default":"UTF-8"}},"required":["path"]}' },
                { type: 'xml_output', name: 'XML 输出', category: 'output', description: '将数据写出为 XML 文件（rootTag/rowTag 包裹）', version: '1.0.0', paramSchema: '{"type":"object","properties":{"path":{"type":"string","title":"输出路径（.xml）","default":"D:\\\\code\\\\比赛\\\\2026省服务外包\\\\output\\\\output.xml"},"rootTag":{"type":"string","title":"根元素名","default":"root"},"rowTag":{"type":"string","title":"行元素名","default":"record"},"encoding":{"type":"string","title":"输出编码","default":"UTF-8"}},"required":["path"]}' }
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

        // ===== 登录 / 注册 / 登出 =====
        function switchAuthMode() {
            authMode.value = authMode.value === 'login' ? 'register' : 'login';
            loginError.value = '';
            registerError.value = '';
        }
        async function register() {
            registerError.value = '';
            const username = registerUsername.value.trim();
            const displayName = registerDisplayName.value.trim();
            if (!/^[A-Za-z0-9_]{3,32}$/.test(username)) {
                registerError.value = '用户名须为 3–32 位字母、数字或下划线';
                return;
            }
            if (!displayName) {
                registerError.value = '请输入显示名称';
                return;
            }
            if (registerPassword.value.length < 8 || !/[A-Za-z]/.test(registerPassword.value) || !/\d/.test(registerPassword.value)) {
                registerError.value = '密码至少 8 位，且须同时包含字母和数字';
                return;
            }
            if (registerPassword.value !== registerPasswordConfirm.value) {
                registerError.value = '两次输入的密码不一致';
                return;
            }
            if (!registerAgreed.value) {
                registerError.value = '请先同意平台账号与数据安全规范';
                return;
            }
            registerLoading.value = true;
            try {
                await api.register(username, displayName, registerPassword.value);
                loginUsername.value = username;
                loginPassword.value = '';
                registerPassword.value = '';
                registerPasswordConfirm.value = '';
                authMode.value = 'login';
                ElMessage.success('账号创建成功，请登录');
            } catch (err) {
                registerError.value = err.response?.data?.error || err.response?.data?.message || '注册失败，请稍后重试';
            } finally {
                registerLoading.value = false;
            }
        }
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
                currentView.value = 'workbench';
                await loadControls();
                await loadJobs();
                startAlertPolling();
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
            stopAlertPolling();
            alerts.value = [];
            alertUnread.value = 0;
            token.value = '';
            user.value = null;
            localStorage.removeItem('token');
            currentView.value = 'workbench';
            jobs.value = [];
            controls.value = [];
            if (graph) { try { graph.dispose(); } catch (_) {} graph = null; }
            ElMessage.info('已退出登录');
        }

        async function loadWorkbench() {
            workbenchLoading.value = true;
            try { workbench.value = await api.getWorkbench(); }
            catch (err) { ElMessage.error('工作台加载失败: ' + (err.response?.data?.message || err.message)); }
            finally { workbenchLoading.value = false; }
        }
        function openTemplatePreview(item) { templatePreview.value = item; templatePreviewVisible.value = true; }
        function cloneTemplateDag(item) {
            const dag = JSON.parse(JSON.stringify(item.dag));
            const stamp = Date.now().toString(36);
            const idMap = new Map();
            dag.nodes.forEach((node, index) => { const id = `tpl_${stamp}_n${index + 1}`; idMap.set(node.id, id); node.id = id; });
            dag.edges.forEach((edge, index) => {
                edge.id = `tpl_${stamp}_e${index + 1}`;
                edge.source = idMap.get(edge.source);
                edge.target = idMap.get(edge.target);
                delete edge.sourcePort;
                delete edge.targetPort;
            });
            return dag;
        }
        async function useTemplate(item) {
            const dag = cloneTemplateDag(item);
            currentJobId = null;
            currentJobName.value = '';
            jobName.value = dag.jobName;
            parallelism.value = dag.parallelism || 1;
            currentView.value = 'canvas';
            templatePreviewVisible.value = false;
            await loadDagToCanvas(dag);
        }
        async function loadDataSources() {
            dataSourcesLoading.value = true;
            try { dataSourceCatalog.value = await api.getDataSourceCatalog(); }
            catch (err) { ElMessage.error('数据源中心加载失败: ' + (err.response?.data?.message || err.message)); }
            finally { dataSourcesLoading.value = false; }
        }
        async function loadAdminOverview() {
            if (user.value?.role !== 'ADMIN') return;
            adminOverviewLoading.value = true;
            try { adminOverview.value = await api.getAdminOverview(); }
            catch (err) { ElMessage.error('管理总览加载失败: ' + (err.response?.data?.message || err.message)); }
            finally { adminOverviewLoading.value = false; }
        }
        async function loadClusterHealth() {
            clusterLoading.value = true;
            try { clusterHealth.value = await api.getClusterHealth(); clusterLastUpdated.value = new Date().toLocaleTimeString('zh-CN'); }
            catch (err) { ElMessage.error('集群健康加载失败: ' + (err.response?.data?.message || err.message)); }
            finally { clusterLoading.value = false; }
        }
        function startClusterPolling() { stopClusterPolling(); loadClusterHealth(); clusterTimer = setInterval(loadClusterHealth, 15000); }
        function stopClusterPolling() { if (clusterTimer) { clearInterval(clusterTimer); clusterTimer = null; } }
        async function loadTimeline() {
            if (!timelineJob.value) return;
            timelineLoading.value = true;
            try {
                const data = await api.getJobTimeline(timelineJob.value.id, timelineScope.value);
                timelineEvents.value = Array.isArray(data) ? data : (data.events || data.content || []);
            } catch (err) { ElMessage.error('运行时间线加载失败: ' + (err.response?.data?.message || err.message)); }
            finally { timelineLoading.value = false; }
        }
        function openTimeline(row) { timelineJob.value = row; timelineScope.value = 'ALL'; timelineVisible.value = true; loadTimeline(); }
        function eventTagType(level) { return level === 'ERROR' ? 'danger' : level === 'WARN' ? 'warning' : level === 'INFO' ? 'success' : 'info'; }
        async function runPreflight(id) {
            const result = await api.getPreflight(id);
            preflightResult.value = { errors: result.errors || [], warnings: result.warnings || [] };
            preflightVisible.value = true;
            await nextTick();
            if (preflightResult.value.errors.length) throw new Error('提交前检查发现错误，请修复后重试');
            if (preflightResult.value.warnings.length) {
                await ElMessageBox.confirm('提交前检查存在警告，是否继续提交？', 'Preflight 检查', { type: 'warning' });
            }
            return result;
        }

        // ===== 操作审计 =====
        const auditRows = ref([]);
        const auditTotal = ref(0);
        const auditPage = ref(0);
        const auditSize = ref(20);
        const auditKeyword = ref('');
        const auditFilters = reactive({ username: '', action: '', targetType: '', targetId: '', ip: '', range: [] });
        const auditLoading = ref(false);
        async function loadAudit(page = auditPage.value) {
            if (!canViewAudit.value) return;
            auditLoading.value = true;
            try {
                const data = await api.getAudit({
                    page, size: auditSize.value, keyword: auditKeyword.value || undefined,
                    username: auditFilters.username || undefined, action: auditFilters.action || undefined,
                    targetType: auditFilters.targetType || undefined, targetId: auditFilters.targetId || undefined,
                    ip: auditFilters.ip || undefined,
                    from: auditFilters.range?.[0] || undefined, to: auditFilters.range?.[1] || undefined
                });
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
        function resetAuditFilters() {
            auditKeyword.value = '';
            Object.assign(auditFilters, { username: '', action: '', targetType: '', targetId: '', ip: '', range: [] });
            loadAudit(0);
        }

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
            // 趋势独立、更高频刷新（2s），不串行在 overview 之后
            trendTimer = setInterval(loadTrends, 2000);
        }
        function stopMonitorPolling() {
            if (monitorTimer) { clearInterval(monitorTimer); monitorTimer = null; }
            if (trendTimer) { clearInterval(trendTimer); trendTimer = null; }
        }
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
        const TREND_WINDOW_POINTS = 40;
        const TREND_ROW_HEIGHT = 220;
        /**
         * 趋势删除按钮的垂直位置：与 ECharts separate 模式每行标题对齐。
         * 仅统计有数据点的曲线，顺序与 updateTrendChart 中渲染的 title 一致。
         */
        function trendActionTop(trend) {
            const visible = monitorTrends.value.filter(t => (t.points || []).length > 0);
            const index = visible.findIndex(t => t.id === trend.id);
            if (index < 0 || (trend.points || []).length === 0) return -9999;
            return index * TREND_ROW_HEIGHT + 26;
        }
        function trendYAxisScale(series) {
            // 按最近可见窗口计算峰值，避免历史大峰值拉平低吞吐曲线
            const values = series.flatMap(item => {
                const pts = item.data || [];
                const start = Math.max(0, pts.length - TREND_WINDOW_POINTS);
                return pts.slice(start).map(point => Number(point[1]) || 0);
            });
            const peak = values.length ? Math.max(...values) : 0;
            if (peak <= 0) return { max: 1, interval: 1 };
            const headroom = peak * 0.2;
            const targetMax = peak + headroom;
            const roughInterval = targetMax / 5;
            const magnitude = Math.pow(10, Math.floor(Math.log10(roughInterval)));
            const normalized = roughInterval / magnitude;
            const niceFactor = normalized <= 1 ? 1 : normalized <= 2 ? 2 : normalized <= 5 ? 5 : 10;
            const interval = niceFactor * magnitude;
            return { max: Math.max(1, Math.ceil(targetMax / interval) * interval), interval };
        }
        function updateTrendChart() {
            if (!trendChart || trendChart.isDisposed?.() || currentView.value !== 'monitor') return;
            const chartEl = document.getElementById('monitorTrendChart');
            if (!chartEl || !chartEl.isConnected || chartEl.offsetParent === null
                    || chartEl.clientWidth === 0 || chartEl.clientHeight === 0) return;
            // 切换视图时 ECharts 可能仍保留 tooltip 的延时定位任务，resize 前先关闭。
            trendChart.dispatchAction({ type: 'hideTip' });
            const trends = monitorTrends.value.filter(t => (t.points || []).length > 0);
            if (!trends.length) {
                if (chartEl) chartEl.style.height = '260px';
                trendChart.resize();
                trendChart.setOption({
                    title: { text: '暂无吞吐数据（运行中或最近结束的作业）', left: 'center', top: 'middle', textStyle: { color: '#94a3b8', fontSize: 13, fontWeight: 'normal' } },
                    xAxis: { show: false }, yAxis: { show: false }, series: []
                }, true);
                return;
            }

            if (monitorTrendMode.value === 'separate') {
                const rowHeight = TREND_ROW_HEIGHT;
                if (chartEl) chartEl.style.height = Math.max(280, trends.length * rowHeight + 8) + 'px';
                trendChart.resize();
                const grids = [], xAxes = [], yAxes = [], titles = [], series = [];
                trends.forEach((trend, index) => {
                    const data = (trend.points || []).map(pt => [pt.t, Number(pt.out || 0)]);
                    const values = data.map(point => point[1]);
                    const peak = Math.max(0, ...values);
                    const positive = values.filter(value => value > 0);
                    const average = positive.length ? positive.reduce((sum, value) => sum + value, 0) / positive.length : 0;
                    const scale = trendYAxisScale([{ data }]);
                    const color = TREND_COLORS[index % TREND_COLORS.length];
                    grids.push({
                        left: 72, right: 42, top: index * rowHeight + 54, height: 126,
                        show: true, backgroundColor: '#fbfdff', borderColor: '#eef2f7', borderWidth: 1
                    });
                    xAxes.push({ type: 'time', gridIndex: index, axisLabel: { color: '#64748b', margin: 12 }, axisLine: { lineStyle: { color: '#dbe4ef' } } });
                    yAxes.push({
                        type: 'value', gridIndex: index, min: 0,
                        max: monitorAutoScale.value ? scale.max : undefined,
                        interval: monitorAutoScale.value ? scale.interval : undefined,
                        axisLabel: { color: '#64748b', margin: 12 },
                        splitLine: { lineStyle: { color: '#edf2f7' } }
                    });
                    titles.push({
                        text: `${trend.name}  峰值 ${fmtNum(peak)} · 平均 ${fmtNum(average)} 行/s`,
                        subtext: '吞吐量（行/s）',
                        left: 72, top: index * rowHeight + 8,
                        textStyle: { color: '#334155', fontSize: 13, fontWeight: 600 },
                        subtextStyle: { color: '#94a3b8', fontSize: 11, lineHeight: 18 }
                    });
                    series.push({
                        name: trend.name, type: 'line', xAxisIndex: index, yAxisIndex: index,
                        data, smooth: false, showSymbol: true, symbolSize: 6,
                        lineStyle: { width: 2.5, color }, itemStyle: { color },
                        areaStyle: { color, opacity: 0.1 }, emphasis: { focus: 'series' },
                        markPoint: peak > 0 ? { symbolSize: 38, data: [{ type: 'max', name: '峰值' }], label: { formatter: p => fmtNum(p.value), fontSize: 10 } } : undefined,
                        markLine: average > 0 ? { silent: true, symbol: 'none', lineStyle: { color, type: 'dashed', opacity: 0.55 }, data: [{ yAxis: average, name: '平均' }], label: { position: 'insideEndTop', distance: 6, formatter: `平均 ${fmtNum(average)}`, color: '#64748b', backgroundColor: 'rgba(255,255,255,.85)', padding: [2, 5] } } : undefined
                    });
                });
                trendChart.setOption({ title: titles, tooltip: { trigger: 'axis' }, legend: { show: false }, grid: grids, xAxis: xAxes, yAxis: yAxes, series }, true);
                return;
            }

            if (chartEl) chartEl.style.height = '300px';
            trendChart.resize();
            const series = trends.map((trend, index) => {
                const color = TREND_COLORS[index % TREND_COLORS.length];
                return {
                    name: trend.name, type: 'line', smooth: false, showSymbol: true, symbolSize: 5,
                    emphasis: { focus: 'series' }, lineStyle: { width: 2, color }, itemStyle: { color },
                    areaStyle: { color, opacity: 0.06 },
                    data: (trend.points || []).map(pt => [pt.t, Number(pt.out || 0)])
                };
            });
            const yScale = trendYAxisScale(series);
            trendChart.setOption({
                title: [], tooltip: { trigger: 'axis' }, legend: { top: 0, type: 'scroll', textStyle: { color: '#64748b' } },
                grid: { left: 66, right: 28, top: 38, bottom: 32 },
                xAxis: { type: 'time', gridIndex: 0, axisLabel: { color: '#64748b' }, axisLine: { lineStyle: { color: '#e2e8f0' } } },
                yAxis: { type: 'value', gridIndex: 0, name: '行/s', min: 0, max: monitorAutoScale.value ? yScale.max : undefined, interval: monitorAutoScale.value ? yScale.interval : undefined, axisLabel: { color: '#64748b' }, splitLine: { lineStyle: { color: '#f1f5f9' } } },
                series
            }, true);
        }
        // ===== 实时监控 END =====

        // ===== Kafka 可视化 =====
        const kafkaTopics = ref([]);
        const kafkaSelectedTopic = ref('');
        const kafkaFromMode = ref('beginning');
        const kafkaStreaming = ref(false);
        const kafkaMessages = ref([]);
        const kafkaMsgCount = ref(0);
        const kafkaMsgRate = ref(0);
        const kafkaError = ref('');
        const kafkaStreamController = ref(null);
        const kafkaRateTimer = ref(null);
        const kafkaLastCount = ref(0);
        const kafkaSeq = ref(0);
        const kafkaFeedRef = ref(null);
        const kafkaChart = ref(null);
        const kafkaTopicTotal = computed(() => {
            const t = kafkaTopics.value.find(x => x.name === kafkaSelectedTopic.value);
            return t ? t.messageCount : 0;
        });

        function kafkaPretty(value) {
            if (!value) return '(空消息)';
            try { return JSON.stringify(JSON.parse(value), null, 2); } catch (_) { return value; }
        }

        async function loadKafkaTopics() {
            kafkaError.value = '';
            try {
                const list = await api.getKafkaTopics();
                kafkaTopics.value = list;
                if (!kafkaSelectedTopic.value && list.length) kafkaSelectedTopic.value = list[0].name;
            } catch (e) {
                kafkaTopics.value = [];
                kafkaError.value = 'Kafka 连接失败：' + (e.response && e.response.data && e.response.data.error ? e.response.data.error : (e.message || ''));
            }
        }

        function initKafkaChart() {
            const el = document.getElementById('kafkaThroughputChart');
            if (!el || kafkaChart.value) return;
            kafkaChart.value = echarts.init(el);
            kafkaChart.value.setOption({
                grid: { left: 40, right: 16, top: 26, bottom: 26 },
                tooltip: { trigger: 'axis' },
                xAxis: { type: 'category', boundaryGap: false, data: [], axisLabel: { fontSize: 10, color: '#64748b' } },
                yAxis: { type: 'value', min: 0, max: 500, splitLine: { lineStyle: { color: '#eef2f7' } }, axisLabel: { fontSize: 10, color: '#64748b' } },
                series: [{
                    name: '条/s',
                    type: 'line',
                    smooth: true,
                    showSymbol: false,
                    data: [],
                    lineStyle: { width: 2, color: '#3b82f6' },
                    areaStyle: { color: 'rgba(59,130,246,0.12)' }
                }]
            });
        }

        function pushKafkaMessage(data) {
            if (data && data.error) { kafkaError.value = 'Kafka 流错误：' + data.error; return; }
            kafkaSeq.value++;
            kafkaMsgCount.value++;
            const now = new Date();
            kafkaMessages.value.push({
                seq: kafkaSeq.value,
                partition: data.partition,
                offset: data.offset,
                time: now.toLocaleTimeString('zh-CN', { hour12: false }) + '.' + String(now.getMilliseconds()).padStart(3, '0'),
                pretty: kafkaPretty(data.value)
            });
            if (kafkaMessages.value.length > 200) kafkaMessages.value.splice(0, kafkaMessages.value.length - 200);
            nextTick(() => {
                if (kafkaFeedRef.value) kafkaFeedRef.value.scrollTop = kafkaFeedRef.value.scrollHeight;
            });
        }

        async function startKafkaStream() {
            stopKafkaStream();
            if (!kafkaSelectedTopic.value) { ElMessage.warning('请先选择 Topic'); return; }
            kafkaMessages.value = [];
            kafkaMsgCount.value = 0;
            kafkaMsgRate.value = 0;
            kafkaLastCount.value = 0;
            kafkaError.value = '';
            const ctrl = new AbortController();
            kafkaStreamController.value = ctrl;
            kafkaStreaming.value = true;
            const rateTimer = setInterval(() => {
                const delta = kafkaMsgCount.value - kafkaLastCount.value;
                kafkaLastCount.value = kafkaMsgCount.value;
                kafkaMsgRate.value = delta;
                if (kafkaChart.value) {
                    const now = new Date().toLocaleTimeString('zh-CN', { hour12: false });
                    const opt = kafkaChart.value.getOption();
                    const times = opt.xAxis[0].data;
                    const rates = opt.series[0].data;
                    times.push(now);
                    rates.push(delta);
                    if (times.length > 60) { times.shift(); rates.shift(); }
                    kafkaChart.value.setOption({ xAxis: { data: times }, series: [{ data: rates }] });
                }
            }, 1000);
            kafkaRateTimer.value = rateTimer;
            const url = API_BASE + '/kafka/stream?topic=' + encodeURIComponent(kafkaSelectedTopic.value) + '&from=' + kafkaFromMode.value + '&durationMs=0';
            try {
                const res = await fetch(url, { headers: { 'Authorization': 'Bearer ' + token.value }, signal: ctrl.signal });
                if (!res.ok) throw new Error('HTTP ' + res.status);
                const reader = res.body.getReader();
                const decoder = new TextDecoder();
                let buffer = '';
                while (true) {
                    const { done, value } = await reader.read();
                    if (done) break;
                    buffer += decoder.decode(value, { stream: true });
                    let idx;
                    while ((idx = buffer.indexOf('\n\n')) >= 0) {
                        const block = buffer.slice(0, idx);
                        buffer = buffer.slice(idx + 2);
                        const line = block.split('\n').find(l => l.startsWith('data:'));
                        if (line) {
                            try { pushKafkaMessage(JSON.parse(line.slice(5).trim())); } catch (_) {}
                        }
                    }
                }
            } catch (e) {
                if (e.name !== 'AbortError') kafkaError.value = 'Kafka 流断开：' + e.message;
            } finally {
                clearInterval(rateTimer);
                if (kafkaStreamController.value === ctrl) {
                    kafkaStreaming.value = false;
                    kafkaStreamController.value = null;
                    if (kafkaRateTimer.value === rateTimer) kafkaRateTimer.value = null;
                }
            }
        }

        function stopKafkaStream() {
            if (kafkaStreamController.value) {
                try { kafkaStreamController.value.abort(); } catch (_) {}
                kafkaStreamController.value = null;
            }
            kafkaStreaming.value = false;
            if (kafkaRateTimer.value) { clearInterval(kafkaRateTimer.value); kafkaRateTimer.value = null; }
        }

        // ===== AI 助手（NL2Pipeline）=====
        async function loadAiModels() {
            try {
                const data = await api.getAiModels();
                aiModels.value = data.models || [];
                aiProvider.value = data.provider || '';
                aiConfigured.value = !!data.configured;
                if (!aiSelectedModel.value || !aiModels.value.includes(aiSelectedModel.value)) {
                    aiSelectedModel.value = data.defaultModel || aiModels.value[0] || '';
                }
            } catch (err) {
                aiModels.value = [];
                aiError.value = '模型列表加载失败: ' + (err.response?.data?.message || err.message);
            }
        }

        async function loadAiProviders() {
            try {
                aiProviders.value = await api.getAiProviders();
            } catch (err) {
                ElMessage.error('加载服务商失败: ' + (err.response?.data?.error || err.message));
            }
        }

        async function openAiProviders() {
            await loadAiProviders();
            aiProvidersVisible.value = true;
        }

        function openAiConfig(profile) {
            aiConfigForm.id = profile?.id || '';
            aiConfigForm.name = profile?.name || '';
            aiConfigForm.provider = profile?.provider || 'openai';
            aiConfigForm.baseUrl = profile?.baseUrl || 'https://api.openai.com/v1';
            aiConfigForm.apiKey = '';
            aiConfigForm.defaultModel = profile?.defaultModel || 'gpt-4.1-mini';
            aiConfigForm.modelsText = (profile?.models || ['gpt-4.1-mini']).join(',');
            aiConfigForm.wireApi = profile?.wireApi || 'chat_completions';
            aiConfigForm.reasoningEffort = profile?.reasoningEffort || 'medium';
            aiConfigForm.storeResponses = !!profile?.storeResponses;
            aiConfigVisible.value = true;
        }

        function aiConfigPayload() {
            return {
                id: aiConfigForm.id || null,
                name: aiConfigForm.name,
                provider: aiConfigForm.provider,
                baseUrl: aiConfigForm.baseUrl,
                apiKey: aiConfigForm.apiKey,
                defaultModel: aiConfigForm.defaultModel,
                models: aiConfigForm.modelsText.split(',').map(item => item.trim()).filter(Boolean),
                wireApi: aiConfigForm.wireApi,
                reasoningEffort: aiConfigForm.reasoningEffort,
                storeResponses: aiConfigForm.storeResponses
            };
        }

        async function saveAiConfig() {
            aiConfigSaving.value = true;
            try {
                const saved = await api.saveAiProvider(aiConfigPayload());
                aiConfigForm.id = saved.id;
                aiConfigForm.apiKey = '';
                ElMessage.success('服务商已保存并切换为当前服务商');
                await Promise.all([loadAiModels(), loadAiProviders()]);
                aiConfigVisible.value = false;
            } catch (err) {
                ElMessage.error('保存失败: ' + (err.response?.data?.error || err.response?.data?.message || err.message));
            } finally {
                aiConfigSaving.value = false;
            }
        }

        async function testAiConnection() {
            aiTesting.value = true;
            try {
                const saved = await api.saveAiProvider(aiConfigPayload());
                aiConfigForm.id = saved.id;
                const result = await api.testAiProvider(saved.id, aiConfigForm.defaultModel);
                ElMessage.success(result.message || 'API 连接成功');
                aiConfigForm.apiKey = '';
                await Promise.all([loadAiModels(), loadAiProviders()]);
            } catch (err) {
                ElMessage.error('连接测试失败: ' + (err.response?.data?.error || err.response?.data?.message || err.message));
            } finally {
                aiTesting.value = false;
            }
        }

        async function activateAiProvider(profile) {
            aiProviderOperating.value = profile.id;
            try {
                await api.activateAiProvider(profile.id);
                await Promise.all([loadAiModels(), loadAiProviders()]);
                ElMessage.success(`已切换到 ${profile.name}`);
            } catch (err) {
                ElMessage.error('切换失败: ' + (err.response?.data?.error || err.message));
            } finally {
                aiProviderOperating.value = '';
            }
        }

        async function testSavedAiProvider(profile) {
            aiProviderOperating.value = profile.id;
            try {
                const result = await api.testAiProvider(profile.id, profile.defaultModel);
                await Promise.all([loadAiModels(), loadAiProviders()]);
                ElMessage.success(`${profile.name}: ${result.message}`);
            } catch (err) {
                ElMessage.error(`${profile.name} 测试失败: ` + (err.response?.data?.error || err.message));
            } finally {
                aiProviderOperating.value = '';
            }
        }

        async function deleteAiProvider(profile) {
            try {
                await ElMessageBox.confirm(`确定删除服务商“${profile.name}”？`, '删除服务商', { type: 'warning' });
                await api.deleteAiProvider(profile.id);
                await loadAiProviders();
                ElMessage.success('服务商已删除');
            } catch (err) {
                if (err !== 'cancel') ElMessage.error('删除失败: ' + (err.response?.data?.error || err.message));
            }
        }

        async function generatePipeline() {
            if (!aiPrompt.value || !aiPrompt.value.trim()) { ElMessage.warning('请输入自然语言描述'); return; }
            aiLoading.value = true; aiError.value = ''; aiResult.value = null; aiResultDag.value = null;
            try {
                const data = await api.aiNl2Pipeline(aiPrompt.value.trim(), aiSelectedModel.value);
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
        async function loadAlerts(page = alertPage.value) {
            alertsLoading.value = true;
            alertsError.value = '';
            try {
                const data = await api.getAlerts(Math.max(0, page - 1), alertSize.value);
                alerts.value = data.content || [];
                alertTotal.value = data.totalElements || 0;
                alertPage.value = (data.number || 0) + 1;
                const u = await api.getAlertUnread();
                alertUnread.value = (u && u.unread) || 0;
            } catch (err) {
                if (!(err.response && err.response.status === 401)) {
                    alertsError.value = err.response?.data?.message || err.message || '告警加载失败';
                    console.warn('加载告警失败:', err);
                }
            } finally { alertsLoading.value = false; }
        }
        function onAlertPage(page) { loadAlerts(page); }
        function startAlertPolling() {
            if (alertTimer) clearInterval(alertTimer);
            loadAlerts();
            alertTimer = setInterval(() => { if (user.value) loadAlerts(); }, 30000);
        }
        function stopAlertPolling() {
            if (alertTimer) { clearInterval(alertTimer); alertTimer = null; }
        }
        async function markAlertRead(id) {
            try {
                await api.markAlertRead(id);
                await loadAlerts();
            } catch (err) {
                ElMessage.error('标记已读失败: ' + (err.response?.data?.message || err.message));
            }
        }
        async function markAllRead() {
            try {
                await api.markAllAlertsRead();
                alertPage.value = 1;
                await loadAlerts(1);
            } catch (err) {
                ElMessage.error('全部已读失败: ' + (err.response?.data?.message || err.message));
            }
        }
        async function deleteAlert(id) {
            try {
                await ElMessageBox.confirm('确定删除此告警？删除后不可恢复。', '删除告警', { type: 'warning' });
                await api.deleteAlert(id);
                ElMessage.success('告警已删除');
                const targetPage = alerts.value.length === 1 && alertPage.value > 1 ? alertPage.value - 1 : alertPage.value;
                await loadAlerts(targetPage);
            } catch (err) {
                if (err !== 'cancel') {
                    ElMessage.error('删除失败: ' + (err.response?.data?.message || err.message));
                }
            }
        }
        function onAlertSelectionChange(selection) {
            selectedAlerts.value = selection.map(r => r.id);
        }
        function selectedAlertIds() {
            return selectedAlerts.value.length ? [...selectedAlerts.value] : [];
        }
        async function batchMarkRead() {
            const ids = selectedAlertIds();
            if (!ids.length) { ElMessage.warning('请先勾选要标记已读的告警'); return; }
            try {
                const res = await api.batchMarkAlertsRead(ids);
                ElMessage.success(`已标记 ${res.updated || 0} 条告警为已读`);
                selectedAlerts.value = [];
                await loadAlerts();
            } catch (err) {
                ElMessage.error('批量标记已读失败: ' + (err.response?.data?.message || err.message));
            }
        }
        async function batchDeleteSelected() {
            const ids = selectedAlertIds();
            if (!ids.length) { ElMessage.warning('请先勾选要删除的告警'); return; }
            try {
                await ElMessageBox.confirm(`确定删除选中的 ${ids.length} 条告警？删除后不可恢复。`, '批量删除告警', { type: 'warning' });
                const res = await api.batchDeleteAlerts(ids);
                ElMessage.success(`已删除 ${res.deleted || 0} 条告警`);
                selectedAlerts.value = [];
                await loadAlerts();
            } catch (err) {
                if (err !== 'cancel') {
                    ElMessage.error('批量删除失败: ' + (err.response?.data?.message || err.message));
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
                diagnoseResult.value = await api.aiDiagnose(diagnoseJob.value.id, aiSelectedModel.value);
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

        async function confirmAiDraftIfNeeded(id) {
            const job = await api.getJob(id);
            if (job.source !== 'AI' || job.confirmationStatus === 'CONFIRMED') return;
            await ElMessageBox.confirm(
                '该作业由 AI 生成。请确认已检查节点、连线和参数，确认后才可提交运行。',
                '人工确认 AI 草稿',
                { type: 'warning', confirmButtonText: '已检查并确认', cancelButtonText: '暂不提交' }
            );
            await api.confirmJob(id);
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
                await confirmAiDraftIfNeeded(currentJobId);
                await runPreflight(currentJobId);
                await api.submitJob(currentJobId);
                ElMessage.success('作业已提交到 Flink 集群');
                await loadJobs();

                // 获取日志
                jobLogs.value = await api.getLogs(currentJobId);
                showLogs.value = true;
            } catch (err) {
                if (err === 'cancel' || err === 'close') return;
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
            // v-show 切换到画布后先等待容器获得真实尺寸，避免 X6 以 0×0 初始化而不渲染节点。
            await nextTick();
            const container = document.getElementById('dag-canvas');
            if (!container) throw new Error('画布容器不存在');
            await new Promise(resolve => {
                let attempts = 0;
                const check = () => {
                    if ((container.clientWidth > 0 && container.clientHeight > 0) || attempts++ >= 10) resolve();
                    else requestAnimationFrame(check);
                };
                check();
            });
            // 画布未初始化时先初始化（覆盖编辑/导入/回滚/诊断等入口，避免静默返回导致画布空白）
            if (!graph) {
                initGraph();
                setupDropHandler();
                await new Promise(resolve => {
                    let settled = false;
                    const finish = () => { if (!settled) { settled = true; resolve(); } };
                    requestAnimationFrame(() => requestAnimationFrame(finish));
                    setTimeout(finish, 100);
                });
            }
            if (!graph) throw new Error('画布初始化失败');
            graph.resize(container.clientWidth, container.clientHeight);
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

            // 确保保存坐标位于当前可视区域；浏览器尺寸变化后也不会出现“节点已加载但画布看起来为空”。
            if (dag.nodes.length > 0) graph.zoomToFit({ padding: 40, maxScale: 1 });

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
            const remoteTypes = new Set(['mysql_input', 'mysql_output', 'pg_input', 'pg_output', 'oracle_input', 'oracle_output', 'hdfs_input']);
            const isRemote = remoteTypes.has(data.type);
            const path = data.params?.path;
            if (isRemote && !currentJobId) {
                ElMessage.warning('远程数据源预览前请先保存作业，以执行权限校验');
                return;
            }
            if (!isRemote && (!path || String(path).trim() === '')) {
                ElMessage.warning('该节点没有可预览的数据源');
                return;
            }
            previewLoading.value = true;
            try {
                const res = isRemote
                    ? await api.previewNode(currentJobId, selectedNode.value.id)
                    : await api.previewFile(String(path).trim());
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
            try {
                // 列表接口可能只返回摘要，编辑时重新读取完整作业，确保 dagJson 存在且为最新版本。
                const job = await api.getJob(row.id);
                jobName.value = job.name;
                currentJobName.value = job.name;
                parallelism.value = job.parallelism || 1;
                if (!job.dagJson) throw new Error('作业没有 DAG 数据');
                const dag = typeof job.dagJson === 'string' ? JSON.parse(job.dagJson) : job.dagJson;
                await loadDagToCanvas(dag);
                if (!dag.nodes || dag.nodes.length === 0) ElMessage.warning('该作业的 DAG 中没有节点');
            } catch (err) {
                console.error('打开作业失败:', err);
                ElMessage.error('打开作业失败: ' + (err.response?.data?.message || err.message));
                currentJobId = null;
                currentJobName.value = '';
            }
        }

        // 提交作业 (作业列表)
        async function submitJobById(id) {
            try {
                await confirmAiDraftIfNeeded(id);
                await runPreflight(id);
                await api.submitJob(id);
                ElMessage.success('作业已提交');
                await loadJobs();
            } catch (err) {
                if (err === 'cancel' || err === 'close') return;
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

        function onJobSelectionChange(rows) {
            selectedJobs.value = rows;
        }

        async function batchChangeOnline(online) {
            if (!selectedJobs.value.length) {
                ElMessage.warning('请先选择作业');
                return;
            }
            if (online) {
                batchOnlineDialogVisible.value = true;
                return;
            }
            try {
                await ElMessageBox.confirm(`确定批量下线选中的 ${selectedJobs.value.length} 个作业？`, '批量下线确认', { type: 'warning' });
                batchOperating.value = true;
                const result = await api.batchOfflineJobs(selectedJobs.value.map(row => row.id));
                showBatchResult('下线', result);
                await loadJobs();
            } catch (err) {
                if (err !== 'cancel') {
                    ElMessage.error('批量下线失败: ' + (err.response?.data?.message || err.message));
                }
            } finally {
                batchOperating.value = false;
            }
        }

        function showBatchResult(action, result) {
            const succeeded = result.succeeded?.length || 0;
            const failed = result.failed?.length || 0;
            if (failed) {
                const detail = result.failed.map(item => `#${item.id}: ${item.message}`).join('；');
                ElMessage.warning(`批量${action}完成：成功 ${succeeded}，失败 ${failed}。${detail}`);
            } else {
                ElMessage.success(`已成功${action} ${succeeded} 个作业`);
            }
        }

        async function confirmBatchOnline() {
            if (batchScheduleEnabled.value && !batchCronExpression.value.trim()) {
                ElMessage.warning('启用调度时必须填写 Cron 表达式');
                return;
            }
            try {
                batchOperating.value = true;
                const result = await api.batchOnlineJobs(
                    selectedJobs.value.map(row => row.id),
                    batchScheduleEnabled.value,
                    batchCronExpression.value.trim()
                );
                batchOnlineDialogVisible.value = false;
                showBatchResult('上线', result);
                await loadJobs();
            } catch (err) {
                ElMessage.error('批量上线失败: ' + (err.response?.data?.message || err.message));
            } finally {
                batchOperating.value = false;
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

        /** 单独移除某条吞吐趋势曲线（不动作业本身） */
        async function removeMonitorTrend(trend) {
            try {
                await ElMessageBox.confirm(
                    `确定移除「${trend.name}」的吞吐趋势曲线？仅清除曲线数据，作业本身保留。`,
                    '移除趋势曲线',
                    { type: 'warning', confirmButtonText: '移除', cancelButtonText: '取消' }
                );
            } catch (_) {
                return;
            }
            try {
                await api.removeMonitorTrend(trend.id);
                monitorTrends.value = monitorTrends.value.filter(t => t.id !== trend.id);
                ElMessage.success('已移除趋势曲线');
                loadTrends();
            } catch (err) {
                ElMessage.error('移除失败: ' + (err.response?.data?.message || err.message));
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
            if (v !== 'monitor') {
                stopMonitorPolling();
                if (trendChart && !trendChart.isDisposed?.()) {
                    trendChart.dispatchAction({ type: 'hideTip' });
                    trendChart.dispose();
                }
                trendChart = null;
            }
            if (v !== 'kafka') stopKafkaStream();
            if (v !== 'cluster') stopClusterPolling();

            if (v === 'workbench') { loadWorkbench(); }
            else if (v === 'data-sources') { loadDataSources(); }
            else if (v === 'admin-overview') { loadAdminOverview(); }
            else if (v === 'cluster') { startClusterPolling(); }
            else if (v === 'monitor') { loadMonitor(); startMonitorPolling(); nextTick(initTrendChart); }
            else if (v === 'kafka') { loadKafkaTopics(); nextTick(initKafkaChart); }
            else if (v === 'audit') { loadAudit(); }
            else if (v === 'workflow') { loadWorkflow(); }
            else if (v === 'alerts') { loadAlerts(); }
            else if (v === 'ai') { loadAiModels(); }
        });

        watch(monitorAutoRefresh, (on) => { if (on) { startMonitorPolling(); } else { stopMonitorPolling(); } });
        watch([monitorTrendMode, monitorAutoScale], () => updateTrendChart());

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
                await loadWorkbench();
                startAlertPolling();
                // 登录态下延迟初始化画布
                setTimeout(() => {
                    initGraph();
                    setupDropHandler();
                }, 100);
            }

            document.addEventListener('keydown', handleKeydown);
        });

        onUnmounted(() => {
            stopAlertPolling();
            stopMonitorPolling();
            stopClusterPolling();
            stopKafkaStream();
            document.removeEventListener('keydown', handleKeydown);
        });

        return {
            currentView, controls, jobs, selectedJobs, batchOperating, jobSearch, jobStatusFilter, filteredJobs, jobName, parallelism, currentJobName,
            workbench, workbenchLoading, loadWorkbench, templates, filteredTemplates, templateKeyword, templateCategory, templatePreviewVisible, templatePreview, openTemplatePreview, useTemplate,
            dataSourceCatalog, dataSourcesLoading, loadDataSources, adminOverview, adminOverviewLoading, loadAdminOverview,
            clusterHealth, clusterLoading, clusterLastUpdated, loadClusterHealth,
            timelineVisible, timelineLoading, timelineScope, timelineJob, timelineEvents, openTimeline, loadTimeline, eventTagType,
            preflightVisible, preflightResult,
            controlTab, saving, submitting, showHelp, showLogs, jobLogs,
            selectedNode, nodeParams, nodeParamSchema, jobErrors,
            previewVisible, previewLoading, previewData, previewRows, previewNode,
            controlsByCategory, onDragStart, saveDag, submitJob,
            exportDag, importDagTrigger, importDag, clearCanvas, newCanvas, applyParams,
            openJob, submitJobById, deleteJob, deleteJobFromDetail, viewLogs, statusTag, undoDag, redoDag, canUndo, canRedo,
            openJobDetail, onJobMore, jobCols, jobDetailVisible, jobDetailRow,
            onlineJobById, offlineJobById, onJobSelectionChange, batchChangeOnline, confirmBatchOnline,
            copyJobById, deleteMonitorJob, openScheduleDialog, saveSchedule, loadScheduleHistory,
            openVersions, rollbackVersion, versionDialogVisible, jobVersions, versionLoading,
            loadSampleDag, SAMPLE_DAGS,
            scheduleDialogVisible, batchOnlineDialogVisible, batchScheduleEnabled, batchCronExpression,
            scheduleJob, scheduleEnabled, cronExpression,
            scheduleMaxRetries, webhookUrl, scheduleHistory, scheduleHistoryLoading,
            schedulePresets, scheduleNlText, scheduleCronHuman, applySchedulePreset, parseNlSchedule, cronToHuman,
            monitorJobs, monitorLoading, monitorError, monitorAutoRefresh, monitorAutoScale, monitorTrendMode, monitorLastUpdated, monitorStats, refreshMonitor,
            monitorTrends, monitorTrendUpdated, removeMonitorTrend, trendActionTop,
            fmtNum, bpClass, cpText, fmtDuration, statusTagType,
            user, canEdit, canViewAudit, authMode, loginUsername, loginPassword, loginError, loginLoading,
            registerUsername, registerDisplayName, registerPassword, registerPasswordConfirm, registerAgreed, registerError, registerLoading,
            login, register, switchAuthMode, logout, onlyMine, helpTab, openHelp,
            auditRows, auditTotal, auditPage, auditSize, auditKeyword, auditFilters, auditLoading, loadAudit, onAuditPage, resetAuditFilters,
            depDialogVisible, depJob, depCandidates, depSelected, depLoading, depSaving, openDepDialog, saveDeps,
            workflowJobs, workflowEdges, workflowLoading, workflowError, workflowUpdated, loadWorkflow,
            lineageDialogVisible, lineageJob, lineageAssets, lineageLoading, openLineageDialog,
            aiPrompt, aiModels, aiSelectedModel, aiProvider, aiConfigured, aiConfigVisible, aiProvidersVisible, aiProviders, aiProviderOperating, aiConfigSaving, aiTesting, aiConfigForm, aiLoading, aiError, aiResult, aiResultDag, aiExamples, loadAiModels, loadAiProviders, openAiProviders, openAiConfig, saveAiConfig, testAiConnection, activateAiProvider, testSavedAiProvider, deleteAiProvider, generatePipeline, openAiResultInCanvas,
            kafkaTopics, kafkaSelectedTopic, kafkaFromMode, kafkaStreaming, kafkaMessages, kafkaMsgCount, kafkaMsgRate, kafkaError, kafkaTopicTotal, kafkaFeedRef, loadKafkaTopics, startKafkaStream, stopKafkaStream,
            alerts, alertUnread, alertsLoading, alertsError, alertPage, alertSize, alertTotal,
            loadAlerts, onAlertPage, markAlertRead, markAllRead, deleteAlert,
            onAlertSelectionChange, batchMarkRead, batchDeleteSelected, selectedAlerts,
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
