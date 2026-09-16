const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

// 拖拽落点回归（2026-09-16 用户实测：拖一个控件出现两个）：
// setupDropHandler 有多个调用点（onMounted / 登录成功 / 切到画布视图 / loadDagToCanvas 补初始化），
// 历史上每次调用都直接 addEventListener 且从不移除 → 一次 drop 触发 N 个回调 → 落下 N 个节点。
// 同时 initGraph 也必须有重入保护，否则同一容器会挂两个 X6 实例（节点/连线重复渲染）。
const root = path.resolve(__dirname, '..');
const appJs = fs.readFileSync(path.join(root, 'public/js/app.js'), 'utf8');

test('拖拽监听幂等绑定：重绑前先移除旧句柄', () => {
  assert.match(appJs, /let dropHandler = null;/, '需保存 drop 监听句柄');
  assert.match(appJs, /let dragoverHandler = null;/, '需保存 dragover 监听句柄');
  assert.match(appJs, /if \(dragoverHandler\) container\.removeEventListener\('dragover', dragoverHandler\);/,
    '重绑 dragover 前必须移除旧监听');
  assert.match(appJs, /if \(dropHandler\) container\.removeEventListener\('drop', dropHandler\);/,
    '重绑 drop 前必须移除旧监听');
  assert.match(appJs, /container\.addEventListener\('drop', dropHandler\)/, '应绑定具名句柄');
  // 不允许再出现匿名监听（那正是重复绑定的来源）
  assert.doesNotMatch(appJs, /container\.addEventListener\('drop', \(e\)/, '不应再使用匿名 drop 监听');
  assert.doesNotMatch(appJs, /container\.addEventListener\('dragover', \(e\)/, '不应再使用匿名 dragover 监听');
});

test('画布初始化有重入保护，同一容器只挂一个 X6 实例', () => {
  const initIdx = appJs.indexOf('function initGraph()');
  assert.ok(initIdx > 0, '应有 initGraph');
  const body = appJs.slice(initIdx, initIdx + 1200);
  assert.match(body, /if \(graph\) return;/, 'graph 已存在时必须直接返回，避免叠加第二个画布实例');
  // 尺寸为 0（视图隐藏）时跳过初始化的既有防护不能丢
  assert.match(body, /clientWidth === 0 \|\| container\.clientHeight === 0\) return;/);
});
