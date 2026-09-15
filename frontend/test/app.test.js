const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

const root = path.resolve(__dirname, '..');
const html = fs.readFileSync(path.join(root, 'public/index.html'), 'utf8');
const appJs = fs.readFileSync(path.join(root, 'public/js/app.js'), 'utf8');

test('主页面加载核心前端资源', () => {
  assert.match(html, /public\/js\/app\.js|js\/app\.js/);
  assert.match(html, /id="dag-canvas"/);
  assert.match(html, /vue(?:\.global)?(?:\.prod)?\.js|vue@3/i);
});

test('app.js 可以被 JavaScript 引擎解析', () => {
  assert.doesNotThrow(() => new vm.Script(appJs, { filename: 'app.js' }));
});

test('画布重载保留防重复渲染机制', () => {
  assert.match(appJs, /dagLoadSeq/);
  assert.match(appJs, /requestAnimationFrame/);
  assert.match(appJs, /loadDagToCanvas/);
  assert.match(appJs, /await api\.getJob\(row\.id\)/);
  assert.match(appJs, /await loadDagToCanvas\(dag\)/);
  assert.match(appJs, /graph\.zoomToFit/);
});

test('提交前仍执行 DAG 校验', () => {
  assert.match(appJs, /validateDagForSubmit/);
  assert.match(appJs, /exportDagJson/);
});

test('吞吐趋势支持独立动态刻度和统计标记', () => {
  assert.match(appJs, /function trendYAxisScale/);
  assert.match(appJs, /monitorTrendMode\.value === 'separate'/);
  assert.match(appJs, /max: monitorAutoScale\.value \? scale\.max/);
  assert.match(appJs, /markPoint/);
  assert.match(appJs, /markLine/);
  assert.match(appJs, /currentView\.value !== 'monitor'/);
  assert.match(appJs, /chartEl\.offsetParent === null/);
  assert.match(appJs, /v !== 'monitor'/);
  assert.match(appJs, /trendChart\.dispose\(\)/);
});

test('每条吞吐趋势曲线提供独立删除按钮', () => {
  assert.match(appJs, /async removeMonitorTrend\(jobId\)/);
  assert.match(appJs, /function removeMonitorTrend\(trend\)/);
  assert.match(appJs, /function trendActionTop\(trend\)/);
  assert.match(appJs, /removeMonitorTrend\(trend\.id\)/);
  assert.match(appJs, /removeMonitorTrend, trendActionTop/);
  assert.match(html, /class="monitor-trend-actions"/);
  assert.match(html, /class="trend-delete-btn"/);
  assert.match(html, /@click="removeMonitorTrend\(trend\)"/);
});

test('告警中心支持分页、登录轮询和错误反馈', () => {
  assert.match(appJs, /data\.content \|\| \[\]/);
  assert.match(appJs, /function startAlertPolling/);
  assert.match(appJs, /function stopAlertPolling/);
  assert.match(appJs, /alertsError\.value/);
  assert.match(html, /user && user\.role !== 'VIEWER'/);
  assert.match(html, /el-pagination/);
});

test('告警中心支持批量标记已读与批量删除', () => {
  assert.match(appJs, /async function batchMarkRead/);
  assert.match(appJs, /async function batchDeleteSelected/);
  assert.match(appJs, /batchMarkAlertsRead/);
  assert.match(appJs, /batchDeleteAlerts/);
  assert.match(appJs, /\/alerts\/batch-read/);
  assert.match(appJs, /\/alerts\/batch-delete/);
  assert.match(appJs, /function onAlertSelectionChange/);
  assert.match(appJs, /selectedAlerts\.value/);
  assert.match(html, /type="selection"/);
  assert.match(html, /@selection-change="onAlertSelectionChange"/);
  assert.match(html, /@click="batchMarkRead"/);
  assert.match(html, /@click="batchDeleteSelected"/);
  assert.match(html, /:disabled="!selectedAlerts\.length"/);
  // 刷新按钮必须显式 loadAlerts()，避免 Vue 把点击事件当 page 参数传入（page=NaN → 500）
  assert.match(html, /@click="loadAlerts\(\)"[^>]*>[\s\S]*?刷新<\/el-button>/);
});

test('远程数据源预览使用已保存作业与节点标识', () => {
  assert.match(appJs, /async previewNode\(jobId, nodeId, limit = 20\)/);
  assert.match(appJs, /preview\/node/);
  assert.match(appJs, /remoteTypes = new Set/);
  assert.match(appJs, /api\.previewNode\(currentJobId, selectedNode\.value\.id\)/);
  assert.match(appJs, /远程数据源预览前请先保存作业/);
});

test('内置控件降级副本与后端注册表保持一致', () => {
  // 红线：DataInitializer 与 getBuiltinControls 必须同步，否则后端不可用时画布缺控件
  const backendSrc = fs.readFileSync(
    path.resolve(root, '..', 'backend/src/main/java/com/datastream/mvp/config/DataInitializer.java'), 'utf8');
  const backendTypes = [...backendSrc.matchAll(/createControl\("([a-z0-9_]+)"/g)].map(m => m[1]).sort();

  const block = appJs.slice(appJs.indexOf('function getBuiltinControls()'));
  const arrayLiteral = block.match(/return\s*\[([\s\S]*?)\n\s*\];/);
  assert.ok(arrayLiteral, 'getBuiltinControls 应返回控件数组');
  const feControls = vm.runInNewContext('[' + arrayLiteral[1] + ']');

  assert.equal(feControls.length, backendTypes.length, '内置控件降级副本数量应与后端一致');
  assert.equal(feControls.filter(c => !c).length, 0, '控件数组不能有空元素');
  // Array.from 归一到当前 realm，避免 vm 跨 realm 原型导致的 deepEqual 误报
  const feTypes = Array.from(feControls, c => c && c.type).sort();

  assert.deepEqual(feTypes, backendTypes, '前后端控件类型必须一一对应');

  for (const c of Array.from(feControls)) {
    assert.doesNotThrow(() => JSON.parse(c.paramSchema), `控件 ${c.type} 的 paramSchema 必须是合法 JSON`);
    assert.ok(c.name && c.category, `控件 ${c.type} 缺少 name/category`);
  }
});

test('首期视图与约定 API 已接入', () => {
  for (const view of ['workbench', 'templates', 'data-sources', 'cluster', 'admin-overview']) assert.match(html, new RegExp(`currentView === '${view}'`));
  for (const endpoint of ['dashboard/workbench', 'dashboard/admin-overview', 'data-sources/catalog', 'cluster/health']) assert.ok(appJs.includes(endpoint), `缺少 API ${endpoint}`);
  assert.match(appJs, /jobs\/\$\{jobId\}\/timeline/);
  assert.match(appJs, /jobs\/\$\{jobId\}\/preflight/);
});

test('数据源管理接入完整 CRUD 与两种连接测试 API', () => {
  for (const method of ['getDataSources', 'getDataSource', 'createDataSource', 'updateDataSource', 'deleteDataSource', 'testDataSource', 'testSavedDataSource']) {
    assert.match(appJs, new RegExp(`async ${method}\\(`), `缺少 API 方法 ${method}`);
  }
  assert.match(appJs, /axios\.get\(API_BASE \+ '\/data-sources'\)/);
  assert.match(appJs, /axios\.post\(API_BASE \+ '\/data-sources', payload\)/);
  assert.match(appJs, /axios\.put\(`\$\{API_BASE\}\/data-sources\/\$\{id\}`, payload\)/);
  assert.match(appJs, /axios\.delete\(`\$\{API_BASE\}\/data-sources\/\$\{id\}`\)/);
  assert.match(appJs, /axios\.post\(API_BASE \+ '\/data-sources\/test', payload\)/);
  assert.match(appJs, /data-sources\/\$\{id\}\/test/);
  assert.match(appJs, /Promise\.allSettled\(\[loadSavedDataSources\(\), loadDataSourceCatalog\(\)\]\)/);
});

test('VIEWER 看不到数据源写操作与测试操作', () => {
  assert.match(html, /v-if="canEdit" type="primary" @click="openCreateDataSource"/);
  assert.match(html, /<el-table-column v-if="canEdit" label="操作"/);
  assert.match(html, /v-if="canEdit" :loading="dataSourceTesting" @click="testDataSourceConnection"/);
  assert.match(html, /v-if="canEdit" type="primary" :loading="dataSourceSaving"/);
  assert.match(appJs, /user\.value\.role !== 'VIEWER'/);
});

test('数据源密码不回填且空值保留语义明确', () => {
  const editFlow = appJs.slice(appJs.indexOf('async function openEditDataSource'), appJs.indexOf('function validateDataSourceForm'));
  assert.match(editFlow, /username: '', password: ''/);
  assert.doesNotMatch(editFlow, /item\.credentials|item\.password/);
  assert.match(html, /留空保留现有密码/);
  assert.match(html, /clearCredentials/);
  assert.match(appJs, /credentials: \{ username: dataSourceForm\.username\.trim\(\), password: dataSourceForm\.password \}/);
});

test('数据源表单覆盖六类动态配置', () => {
  for (const type of ['MYSQL', 'POSTGRESQL', 'ORACLE', 'KAFKA', 'REDIS', 'HDFS']) {
    assert.ok(html.includes(`dataSourceForm.type === '${type}'`) || appJs.includes(`${type}: {`), `缺少 ${type} 表单`);
  }
  for (const field of ['parameters', 'sslMode', 'serviceName', 'bootstrapServers', 'securityProtocol', 'saslMechanism', 'database', 'ssl', 'uri', 'testPath']) {
    assert.ok(html.includes(`dataSourceForm.config.${field}`), `缺少动态字段 ${field}`);
  }
  assert.match(html, /startsWith\('SASL'\)/);
  assert.match(html, /dataSourceForm.type !== 'HDFS'/);
});

test('数据源页面分为已保存连接、连接器目录和 DAG 内联资产', () => {
  for (const title of ['已保存连接', '连接器目录', 'DAG 内联资产']) assert.ok(html.includes(title));
  assert.match(html, /已保存连接暂未自动替换历史 DAG 内联连接/);
  assert.match(html, /dataSourceAddress\(scope\.row\)/);
  assert.match(html, /user\?\.role === 'ADMIN'/);
  assert.match(appJs, /err\?\.response\?\.status === 409/);
});

test('内置模板是合法 DAG 且使用时重建标识并清理作业上下文', () => {
  const sampleBlock = appJs.slice(appJs.indexOf('const SAMPLE_DAGS'), appJs.indexOf('// 模板中心'));
  assert.match(sampleBlock, /nodes\s*:/);
  assert.match(sampleBlock, /edges\s*:/);
  assert.match(appJs, /JSON\.parse\(JSON\.stringify\(item\.dag\)\)/);
  assert.match(appJs, /idMap\.set\(node\.id, id\)/);
  assert.match(appJs, /edge\.source = idMap\.get\(edge\.source\)/);
  assert.match(appJs, /currentJobId = null/);
  assert.match(appJs, /await loadDagToCanvas\(dag\)/);
});

test('preflight 位于人工确认之后和实际提交之前', () => {
  const canvasSubmit = appJs.slice(appJs.indexOf('async function submitJob()'), appJs.indexOf('// 导出 DAG JSON'));
  const listSubmit = appJs.slice(appJs.indexOf('async function submitJobById'), appJs.indexOf('// 删除作业'));
  for (const flow of [canvasSubmit, listSubmit]) {
    const confirmAt = flow.indexOf('confirmAiDraftIfNeeded');
    const preflightAt = flow.indexOf('runPreflight');
    const submitAt = flow.indexOf('api.submitJob');
    assert.ok(confirmAt >= 0 && confirmAt < preflightAt && preflightAt < submitAt, '提交顺序应为 AI 确认 → preflight → submit');
  }
  assert.match(appJs, /preflightResult\.value\.errors\.length\) throw/);
  assert.match(canvasSubmit, /validateDagForSubmit\(\)/, '画布提交保留本地 DAG 校验');
  assert.doesNotMatch(listSubmit, /validateDagForSubmit\(\)/, '列表运行不应错误校验当前画布');
});

test('所有 Element Plus 按钮都有语义图标', () => {
  // 逐个按钮块提取：从 <el-button 开始累积到配对的 </el-button>
  // （按钮可能是多行，用行级扫描会把「图标在下一行」误判为缺失）
  const missing = [];
  let cursor = 0;
  let count = 0;
  while (true) {
    const start = html.indexOf('<el-button', cursor);
    if (start === -1) break;
    const end = html.indexOf('</el-button>', start);
    if (end === -1) break;
    const block = html.slice(start, end + '</el-button>'.length);
    count += 1;
    // 图标标签可能带 class/style，不能按字面量判断
    if (!/<el-icon\b/.test(block)) missing.push(block.replace(/\s+/g, ' ').slice(0, 110));
    cursor = end + 1;
  }
  assert.ok(count > 0, '页面应包含 el-button');
  assert.deepEqual(missing, [], `以下按钮缺少图标:\n${missing.join('\n')}`);
});

test('按钮不混用 emoji 图标', () => {
  const emoji = /[\u{1F300}-\u{1FAFF}\u{2600}-\u{27BF}\u{2190}-\u{21FF}\u{25A0}]/u;
  const offenders = [];
  let cursor = 0;
  while (true) {
    const start = html.indexOf('<el-button', cursor);
    if (start === -1) break;
    const end = html.indexOf('</el-button>', start);
    if (end === -1) break;
    const block = html.slice(start, end);
    if (emoji.test(block)) offenders.push(block.replace(/\s+/g, ' ').slice(0, 110));
    cursor = end + 1;
  }
  assert.deepEqual(offenders, [], '按钮应统一使用 Element Plus 图标而非 emoji/符号字符');
});

test('下拉菜单项统一带图标', () => {
  const missing = [];
  html.split('\n').forEach((line, index) => {
    const match = line.match(/<el-dropdown-item[^>]*>([\s\S]*?)<\/el-dropdown-item>/);
    if (!match) return;
    // 动态渲染的项（如示例作业名）内容来自数据，不要求硬编码图标
    if (/v-for/.test(line) && /\{\{/.test(match[1])) return;
    if (!/<el-icon\b/.test(match[1])) {
      missing.push(`${index + 1}: ${match[1].replace(/<[^>]+>/g, '').trim().slice(0, 30)}`);
    }
  });
  assert.deepEqual(missing, [], `以下下拉项缺少图标:\n${missing.join('\n')}`);
});

test('图标标签使用 kebab-case 且不撞原生标签名', () => {
  // index.html 是 DOM 模板：浏览器会把标签名小写化，PascalCase 写法（<DataBoard />）
  // 解析成 <databoard>，Vue 无法匹配到组件，图标静默不渲染（不报错，极难发现）。
  // 注意 <el-icon> 可能带 style/class 等属性，匹配时必须容忍。
  const pascal = [...html.matchAll(/<el-icon\b[^>]*>\s*<([A-Z][A-Za-z0-9]*)\s*\/>/g)].map(m => m[1]);
  assert.deepEqual(pascal, [], `图标标签必须用 kebab-case，以下为 PascalCase：${pascal.join(', ')}`);

  // <view> / <keyboard> 经实测在浏览器中不解析为组件（静默失效），需用等价图标替代
  const brokenNames = ['view', 'keyboard'];
  const hits = brokenNames.filter(name => new RegExp(`<el-icon\\b[^>]*>\\s*<${name}\\s*/>`).test(html));
  assert.deepEqual(hits, [], `图标名与原生标签冲突、不会渲染，需换等价图标：${hits.join(', ')}`);
});

test('首期页面展示真实字段并覆盖空态、错误态和重试', () => {
  for (const field of ['totalJobs', 'onlineJobs', 'updatedAt', 'enabled', 'direction', 'nodeId', 'totalUsers', 'recentAlerts', 'checkedAt', 'slotUsageText']) {
    assert.ok(html.includes(field) || appJs.includes(field), `缺少页面字段 ${field}`);
  }
  for (const state of ['workbenchError', 'dataSourcesError', 'adminOverviewError', 'clusterError', 'timelineError']) assert.ok(appJs.includes(state), `缺少错误态 ${state}`);
  assert.match(html, /没有匹配的模板/);
  assert.match(html, /暂无连接器/);
  assert.match(html, /当前账号的作业尚未引用可识别资产/);
  assert.match(appJs, /async function openJobDetailById/);
  assert.match(appJs, /err\.response\?\.status === 403/);
});

test('模板覆盖有确认与恢复提示，时间线切换和关闭会清理旧数据', () => {
  assert.match(appJs, /graph\.getNodes\(\)\.length/);
  assert.match(appJs, /已保留当前画布/);
  assert.match(appJs, /timelineEvents\.value = \[\]/);
  assert.match(appJs, /function clearTimeline\(\)/);
  assert.match(html, /@closed="clearTimeline"/);
  assert.match(html, /最近提交以来/);
  assert.match(html, /timelineError/);
});

test('preflight 服务失败仍打开结果对话框并阻止提交', () => {
  assert.match(appJs, /serviceError: '提交前检查服务不可用/);
  assert.match(appJs, /preflightVisible\.value = true/);
  assert.match(appJs, /已阻止提交/);
  assert.match(html, /preflightResult\.serviceError/);
});

test('集群健康页 15 秒刷新且离开和卸载时清理 timer', () => {
  assert.match(appJs, /clusterTimer = setInterval\(loadClusterHealth, 15000\)/);
  assert.match(appJs, /if \(v !== 'cluster'\) stopClusterPolling\(\)/);
  const unmount = appJs.slice(appJs.indexOf('onUnmounted(() =>'));
  assert.match(unmount, /stopClusterPolling\(\)/);
  assert.match(appJs, /clearInterval\(clusterTimer\)/);
});


test('画布节点可引用已保存数据源并保持旧 DAG 兼容', () => {
  assert.match(appJs, /NODE_DATA_SOURCE_TYPES/);
  assert.match(appJs, /const nodeDataSourceOptions = computed/);
  assert.match(appJs, /item\.type === type && item\.enabled/);
  // dataSourceId 必须保持数值类型，不能被当普通字符串 trim
  assert.match(appJs, /if \(k === 'dataSourceId'\)/);
  assert.match(html, /nodeDataSourceOptions\.length/);
  assert.match(html, /nodeParams\.dataSourceId/);
  assert.match(html, /选择后由后端注入该数据源的地址与凭据/);
});

test('布尔与数值参数按 schema 类型保存并可回显历史字符串值', () => {
  // 预检按 schema 校验类型，旧数据里 hasHeader 存的是字符串 "true"，表单必须能回显且写回布尔
  const idx = appJs.indexOf('function applyParams()');
  assert.ok(idx > 0, '应有 applyParams');
  const body = appJs.slice(idx, appJs.indexOf('// 预览节点数据（读取输入/输出文件前 N 行）'));
  assert.match(body, /const schemaProps = nodeParamSchema\.value\?\.properties \|\| \{\}/);
  assert.match(body, /if \(type === 'boolean'\)/);
  assert.match(body, /if \(type === 'number' \|\| type === 'integer'\)/);
  // 空值不写入，避免隐藏的可选数值项触发类型校验
  assert.match(body, /String\(v\)\.trim\(\) === ''\) continue;/);
  // 控件用代理读写，兼容字符串 "true" 并归一化为布尔
  assert.match(appJs, /function paramProxy\(key, param\)/);
  assert.match(appJs, /String\(value\)\.trim\(\) === 'true'/);
  assert.match(appJs, /function setParam\(key, value\)/);
  assert.match(html, /:model-value="paramProxy\(key, param\)"/);
  assert.match(html, /@update:model-value="setParam\(key, \$event\)"/);
  // 节点选中时用 schema 默认值兜底未保存的可选参数
  assert.match(appJs, /if \(params\[key\] === undefined && prop\.default !== undefined\) params\[key\] = prop\.default;/);
});

test('内置示例 DAG 的参数类型与控件 schema 一致', () => {
  const start = appJs.indexOf('const SAMPLE_DAGS = {');
  assert.ok(start > 0, '应有 SAMPLE_DAGS');
  const samples = appJs.slice(start, appJs.indexOf('// 模板中心在 SAMPLE_DAGS'));
  assert.doesNotMatch(samples, /hasHeader: 'true'/, '示例中开关必须是布尔而非字符串');
  assert.doesNotMatch(samples, /rowsPerSecond: '50'/, '示例中数值必须保持数值类型');
  assert.match(samples, /hasHeader: true/);
});

test('个人资料与导航栏头像形成完整交互闭环', () => {
  const userMenu = html.slice(html.indexOf('<div class="user-menu"'), html.indexOf('</el-header>'));
  assert.match(userMenu, /class="profile-trigger"/, '导航栏应以头像作为个人菜单入口');
  assert.match(userMenu, /:src="avatarObjectUrl"/, '头像应使用鉴权后的 Blob URL');
  assert.match(userMenu, /handleProfileCommand/);
  assert.match(userMenu, /command="profile"/);
  assert.match(userMenu, /command="logout"/);
  assert.doesNotMatch(userMenu, /@click="logout"/, '退出按钮应收进头像菜单，保证头像位于最右侧');
  assert.match(html, /v-model="profileVisible" title="个人资料"/);
  assert.match(html, /profileForm\.displayName/);
  assert.match(html, /profileForm\.signature/);
  assert.match(html, /profileForm\.statusPreference/);
  assert.doesNotMatch(html, /v-html="profileForm\.signature"/, '签名只能按文本显示');
  assert.match(appJs, /responseType: 'blob'/, '鉴权头像应通过 Axios Blob 加载');
  assert.match(appJs, /URL\.revokeObjectURL/);
});

test('个人资料提供用户名与密码修改入口', () => {
  assert.match(html, /修改登录用户名/);
  assert.match(html, /修改账号密码/);
  assert.match(html, /v-model="credentialVisible"/);
  assert.match(html, /credentialForm\.currentPassword/);
  assert.match(html, /credentialForm\.newPassword/);
  assert.match(html, /credentialForm\.confirmPassword/);
  assert.match(appJs, /auth\/me\/username/);
  assert.match(appJs, /auth\/me\/password/);
  assert.match(appJs, /function openCredentialDialog\(mode\)/);
  assert.match(appJs, /async function saveCredential\(\)/);
});

test('模板中心的模板与 SAMPLE_DAGS 一一对应且分类动态生成', () => {
  const dagStart = appJs.indexOf('const SAMPLE_DAGS = {');
  const dagEnd = appJs.indexOf('// 模板中心在 SAMPLE_DAGS');
  const dagKeys = new Set([...appJs.slice(dagStart, dagEnd).matchAll(/^    '([a-z0-9_]+)': \{/gm)].map(m => m[1]));
  const metaStart = appJs.indexOf('const BUILTIN_TEMPLATES');
  const metaBlock = appJs.slice(metaStart, appJs.indexOf('].map(meta =>', metaStart));
  const metaKeys = [...metaBlock.matchAll(/\{ key: '([a-z0-9_]+)'/g)].map(m => m[1]);

  assert.ok(metaKeys.length >= 12, '模板数量应充足，当前 ' + metaKeys.length);
  const missing = metaKeys.filter(k => !dagKeys.has(k));
  assert.deepEqual(missing, [], '模板引用了不存在的 DAG: ' + missing.join(', '));
  // 分类下拉改为动态生成，避免硬编码分类与模板脱节
  assert.match(appJs, /const templateCategories = computed\(/);
  assert.match(html, /v-for="cat in templateCategories"/);
  assert.doesNotMatch(html, /<el-option label="批量处理" value="批量处理">/, '不应保留硬编码分类');
});

test('模板路径占位符解析为真实目录', () => {
  // 模板里的 /data、/test-resources、/output 是容器语义占位，本机需换成绝对路径
  assert.match(appJs, /const TEMPLATE_PATH_ROOTS = \['\/test-resources', '\/output', '\/data'\]/);
  assert.match(appJs, /function resolveTemplatePath\(path\)/);
  assert.match(appJs, /function resolveTemplatePaths\(node\)/);
  const cloneBody = appJs.slice(appJs.indexOf('function cloneTemplateDag'), appJs.indexOf('async function useTemplate'));
  assert.match(cloneBody, /resolveTemplatePaths\(node\)/, '应用模板时必须解析路径');
});

test('AI 示例覆盖主要控件组合', () => {
  const start = appJs.indexOf('const aiExamples = [');
  const block = appJs.slice(start, appJs.indexOf('];', start));
  const count = (block.match(/^\s+'/gm) || []).length;
  assert.ok(count >= 8, 'AI 示例应有足够数量，当前 ' + count);
  for (const kw of ['拼接', '去重', '为空', 'excel', 'xml', 'json', 'mysql']) {
    assert.ok(block.toLowerCase().includes(kw), '示例应覆盖场景: ' + kw);
  }
});

test('模板事件绑定的方法都在 setup 返回值中暴露', () => {
  // 模板引用未暴露的方法会在点击时抛 "xxx is not a function"（loadJobs 曾因此漏出），
  // 静态扫出这类遗漏比等用户点出来便宜
  const returnStart = appJs.lastIndexOf('return {');
  assert.ok(returnStart > 0, '应有 setup 返回对象');
  const returnBlock = appJs.slice(returnStart, appJs.indexOf('};', returnStart));
  const exposed = new Set(returnBlock.match(/[A-Za-z_$][\w$]*/g));
  const boundCalls = new Set();
  const events = html.matchAll(/@(?:click|change|command|select|input|blur|focus|keyup|clear|remove|close|open)[\w.]*\s*=\s*"([^"]+)"/g);
  for (const event of events) {
    for (const call of event[1].matchAll(/([A-Za-z_$][\w$]*)\s*\(/g)) boundCalls.add(call[1]);
  }
  const missing = [...boundCalls].filter(name => !exposed.has(name));
  assert.deepEqual(missing, [], '模板调用了未暴露的方法: ' + missing.join(', '));
});

test('用户在线状态使用独立 HTTP 心跳并在退出时清理', () => {
  assert.match(appJs, /async heartbeat\(\)/);
  assert.match(appJs, /auth\/presence\/heartbeat/);
  assert.match(appJs, /presenceTimer = setInterval\(sendPresenceHeartbeat, 60000\)/);
  assert.match(appJs, /heartbeatInFlight/, '心跳请求应防止并发重入');
  assert.match(appJs, /function stopPresenceHeartbeat\(\)/);
  const logout = appJs.slice(appJs.indexOf('async function logout()'), appJs.indexOf('function getErrorMessage'));
  assert.match(logout, /stopPresenceHeartbeat\(\)/);
  const unmount = appJs.slice(appJs.indexOf('onUnmounted(() =>'));
  assert.match(unmount, /stopPresenceHeartbeat\(\)/);
});

test('画布不在隐藏状态下初始化 X6', () => {
  // X6 以 0 尺寸初始化会把 width/height:0px 写进内联样式并永久覆盖 flex 布局，
  // 导致画布高度为 0、无法拖拽连线；登录后默认停在工作台时正是这种情形。
  const idx = appJs.indexOf('function initGraph()');
  assert.ok(idx > 0, '应有 initGraph');
  const body = appJs.slice(idx, idx + 800);
  assert.match(body, /clientWidth === 0/, 'initGraph 必须在容器尺寸为 0 时直接返回');
  assert.match(body, /clientHeight === 0\) return/, 'initGraph 必须在容器高度为 0 时直接返回');
  // 切回画布视图时要重新初始化或 resize
  const viewIdx = appJs.indexOf("v === 'canvas'");
  assert.ok(viewIdx > 0, 'watch 中应有 canvas 分支');
  assert.match(appJs.slice(viewIdx, viewIdx + 600), /graph\.resize\(/, '切到画布视图需适配真实尺寸');
});
