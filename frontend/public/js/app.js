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
    async submitJob(id) {
        const res = await axios.post(`${API_BASE}/jobs/${id}/submit`);
        return res.data;
    },
    async cancelJob(id) {
        await axios.post(`${API_BASE}/jobs/${id}/cancel`);
    },
    async getLogs(jobId) {
        const res = await axios.get(`${API_BASE}/jobs/${jobId}/logs`);
        return res.data;
    }
};

// ===== 颜色工具 =====
const CATEGORY_COLORS = {
    input: { bg: '#e1f3d8', border: '#67c23a', label: '输入' },
    transform: { bg: '#d9ecff', border: '#409eff', label: '转换' },
    output: { bg: '#fdf0d9', border: '#e6a23c', label: '输出' }
};

// ===== Vue App =====
const { createApp, ref, computed, onMounted, watch, nextTick, toRaw } = Vue;
const { ElMessage, ElMessageBox } = ElementPlus;

const app = createApp({
    setup() {
        // 状态
        const currentView = ref('canvas');
        const controls = ref([]);
        const jobs = ref([]);
        const jobName = ref('未命名作业');
        const parallelism = ref(1);
        const currentJobName = ref('');
        const controlTab = ref('input');
        const saving = ref(false);
        const submitting = ref(false);
        const showHelp = ref(false);
        const showLogs = ref(false);
        const jobLogs = ref([]);
        const jobErrors = ref({});

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
                                line: { stroke: '#909399', strokeWidth: 2, targetMarker: { name: 'block', width: 8, height: 6 } }
                            },
                            zIndex: 1
                        });
                    }
                },
                highlighting: {
                    nodeAvailable: { name: 'stroke', args: { padding: 8, attrs: { stroke: '#409eff', strokeWidth: 2 } } }
                },
                selecting: { enabled: true, multiple: false }
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
        }

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
                        label: data.name,
                        attrs: {
                            body: {
                                fill: colors.bg,
                                stroke: colors.border,
                                strokeWidth: 2,
                                rx: 8,
                                ry: 8
                            },
                            label: {
                                text: data.name,
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
                { type: 'csv_output', name: 'CSV 输出', category: 'output', description: '将数据写入 CSV 文件', version: '1.0.0', paramSchema: '{"type":"object","properties":{"path":{"type":"string","title":"输出路径","default":"D:\\\\code\\\\比赛\\\\2026省服务外包\\\\output\\\\output.csv"},"delimiter":{"type":"string","title":"分隔符","default":","}},"required":["path"]}' },
                { type: 'excel_input', name: 'Excel 输入', category: 'input', description: '读取 Excel (.xlsx) 文件', version: '1.0.0', paramSchema: '{"type":"object","properties":{"path":{"type":"string","title":"文件路径","default":"/data/input.xlsx"}},"required":["path"]}' },
                { type: 'mysql_input', name: 'MySQL 输入', category: 'input', description: '从 MySQL 表读取数据', version: '1.0.0', paramSchema: '{"type":"object","properties":{"url":{"type":"string","title":"JDBC URL","default":"jdbc:mysql://localhost:3306/flink_demo"},"table":{"type":"string","title":"表名","default":"source_table"},"username":{"type":"string","title":"用户名","default":"root"},"password":{"type":"string","title":"密码","default":""}},"required":["url","table","username"]}' },
                { type: 'mysql_output', name: 'MySQL 输出', category: 'output', description: '将数据写入 MySQL 表', version: '1.0.0', paramSchema: '{"type":"object","properties":{"url":{"type":"string","title":"JDBC URL","default":"jdbc:mysql://localhost:3306/flink_demo"},"table":{"type":"string","title":"表名","default":"target_table"},"username":{"type":"string","title":"用户名","default":"root"},"password":{"type":"string","title":"密码","default":""}},"required":["url","table","username"]}' },
                                { type: 'kafka_input', name: 'Kafka 输入', category: 'input', description: '从 Kafka 主题读取消息流', version: '1.0.0', paramSchema: '{"type":"object","properties":{"topic":{"type":"string","title":"主题","default":"input-topic"},"bootstrapServers":{"type":"string","title":"Bootstrap Servers","default":"localhost:9092"},"fieldsConfig":{"type":"string","title":"字段定义（JSON 数组）","default":"[{\"name\":\"key\",\"type\":\"STRING\"},{\"name\":\"value\",\"type\":\"STRING\"}]"}},"required":["topic","bootstrapServers"]}' },
                { type: 'kafka_output', name: 'Kafka 输出', category: 'output', description: '将数据写入 Kafka 主题', version: '1.0.0', paramSchema: '{"type":"object","properties":{"topic":{"type":"string","title":"主题","default":"output-topic"},"bootstrapServers":{"type":"string","title":"Bootstrap Servers","default":"localhost:9092"}},"required":["topic","bootstrapServers"]}' },
                { type: 'excel_output', name: 'Excel 输出', category: 'output', description: '将数据写出为 Excel (.xlsx) 文件（内部 CSV→Excel 转换）', version: '1.0.0', paramSchema: '{"type":"object","properties":{"path":{"type":"string","title":"输出路径","default":"D:\\\\code\\\\比赛\\\\2026省服务外包\\\\output\\\\output.xlsx"},"delimiter":{"type":"string","title":"分隔符","default":","}},"required":["path"]}' },                { type: 'field_concat', name: '字段拼接', category: 'transform', description: '将多个字段拼接为一个新字段', version: '1.0.0', paramSchema: '{"type":"object","properties":{"fields":{"type":"array","title":"输入字段","items":{"type":"string"}},"separator":{"type":"string","title":"分隔符","default":","},"newFieldName":{"type":"string","title":"新字段名","default":"concat_field"}},"required":["fields","newFieldName"]}' },
                { type: 'xml_json', name: 'XML<->JSON', category: 'transform', description: 'XML 和 JSON 格式互转', version: '1.0.0', paramSchema: '{"type":"object","properties":{"direction":{"type":"string","title":"转换方向","enum":["xml2json","json2xml"],"default":"xml2json"},"sourceField":{"type":"string","title":"源字段","default":"payload"},"targetField":{"type":"string","title":"目标字段","default":"result"}},"required":["direction","sourceField"]}' }
,                { type: 'field_filter', name: '字段过滤', category: 'transform', description: '只保留指定字段', version: '1.0.0', paramSchema: '{"type":"object","properties":{"fields":{"type":"string","title":"保留字段（逗号分隔）","default":"id,name"}},"required":["fields"]}' },
                { type: 'field_rename', name: '字段改名', category: 'transform', description: '字段重命名', version: '1.0.0', paramSchema: '{"type":"object","properties":{"mappings":{"type":"string","title":"改名映射（old=new，逗号分隔）","default":"id=userId"}},"required":["mappings"]}' },
                { type: 'row_filter', name: '行过滤', category: 'transform', description: '按条件过滤行', version: '1.0.0', paramSchema: '{"type":"object","properties":{"condition":{"type":"string","title":"过滤条件（SQL WHERE 表达式）","default":"age > 18"}},"required":["condition"]}' },
                { type: 'json_parse', name: 'JSON 解析', category: 'transform', description: '从 JSON 字段解析出多个字段', version: '1.0.0', paramSchema: '{"type":"object","properties":{"sourceField":{"type":"string","title":"JSON 源字段","default":"payload"},"fieldsConfig":{"type":"string","title":"解析字段（JSON 数组）","default":"[{\"name\":\"id\",\"type\":\"INT\"},{\"name\":\"name\",\"type\":\"STRING\"}]"}},"required":["sourceField","fieldsConfig"]}' },
                { type: 'json_input', name: 'JSON 输入', category: 'input', description: '读取 JSON 文件（JSON Lines）', version: '1.0.0', paramSchema: '{"type":"object","properties":{"path":{"type":"string","title":"文件路径","default":"D:\\code\\比赛\\2026省服务外包\\test-resources\\data\\sample.json"},"fieldsConfig":{"type":"string","title":"字段定义（JSON 数组）","default":"[{\"name\":\"id\",\"type\":\"INT\"},{\"name\":\"name\",\"type\":\"STRING\"}]"}},"required":["path"]}' },
                { type: 'json_output', name: 'JSON 输出', category: 'output', description: '将数据写入 JSON 文件（JSON Lines）', version: '1.0.0', paramSchema: '{"type":"object","properties":{"path":{"type":"string","title":"输出路径（.json）","default":"D:\\code\\比赛\\2026省服务外包\\output\\output.json"}},"required":["path"]}' }
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
                    label: n.label || type,
                    attrs: {
                        body: { fill: colors.bg, stroke: colors.border, strokeWidth: 2, rx: 8, ry: 8 },
                        label: { text: n.label || type, fill: '#303133', fontSize: 13, fontWeight: 'bold', textAnchor: 'middle', textVerticalAnchor: 'middle' }
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
                                line: { stroke: '#909399', strokeWidth: 2, targetMarker: { name: 'block', width: 8, height: 6 } }
                            }
                        });
                    }
                });
            }
        }

        // 清空画布
        function clearCanvas() {
            if (graph) {
                graph.clearCells();
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

        // 状态标签颜色
        function statusTag(status) {
            const map = { DRAFT: 'info', SUBMITTED: '', RUNNING: 'success', COMPLETED: '', FAILED: 'danger', CANCELLED: 'warning' };
            return map[status] || 'info';
        }

        // 键盘快捷键
        function handleKeydown(e) {
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

        onMounted(async () => {
            await loadControls();
            await loadJobs();

            // 延迟初始化画布
            setTimeout(() => {
                initGraph();
                setupDropHandler();
            }, 100);

            document.addEventListener('keydown', handleKeydown);
        });

        return {
            currentView, controls, jobs, jobName, parallelism, currentJobName,
            controlTab, saving, submitting, showHelp, showLogs, jobLogs,
            selectedNode, nodeParams, nodeParamSchema, jobErrors,
            controlsByCategory, onDragStart, saveDag, submitJob,
            exportDag, importDagTrigger, importDag, clearCanvas, newCanvas, applyParams,
            openJob, submitJobById, deleteJob, viewLogs, statusTag
        };
    }
});

// 注册 Element Icon 组件
for (const [key, component] of Object.entries(ElementPlusIconsVue)) {
    app.component(key, component);
}

app.use(ElementPlus);
app.mount('#app');
