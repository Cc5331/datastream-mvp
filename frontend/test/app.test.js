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

test('告警中心支持分页、登录轮询和错误反馈', () => {
  assert.match(appJs, /data\.content \|\| \[\]/);
  assert.match(appJs, /function startAlertPolling/);
  assert.match(appJs, /function stopAlertPolling/);
  assert.match(appJs, /alertsError\.value/);
  assert.match(html, /user && user\.role !== 'VIEWER'/);
  assert.match(html, /el-pagination/);
});
