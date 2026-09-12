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
  assert.match(html, /@click="loadAlerts\(\)" *>刷新/);
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

