const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

// 控件管理页已下线（2026-09-16）：它是纯只读表格，与「画布控件库 + 帮助中心控件说明」重复，
// 且容易让人以为能在线编辑控件（后端写接口在启动时会被 DataInitializer 的 deleteAll 清掉）。
// 本用例防止它被误加回来，同时确认控件展示的两条正路仍在。
const root = path.resolve(__dirname, '..');
const html = fs.readFileSync(path.join(root, 'public/index.html'), 'utf8');
const appJs = fs.readFileSync(path.join(root, 'public/js/app.js'), 'utf8');

test('独立的控件管理页已移除，控件展示走画布控件库与帮助中心', () => {
  assert.doesNotMatch(html, /currentView === 'controls'/, '不应再有独立的控件管理视图');
  assert.doesNotMatch(html, /class="controls-view"/, '不应再有 controls-view 容器');
  // 帮助下拉的「控件说明」保留，且必须走帮助弹窗（openHelp），不是切主视图
  assert.match(html, /<el-dropdown trigger="click" @command="openHelp">/, '帮助下拉应绑定 openHelp');
  assert.match(html, /<el-dropdown-item command="controls"><el-icon><grid \/><\/el-icon>\s*控件说明<\/el-dropdown-item>/,
    '帮助中心仍应提供「控件说明」入口');
  assert.match(appJs, /function openHelp\(tab\)/, 'openHelp 应接受标签参数以定位到控件说明页');
});

test('控件注册表仍驱动画布控件库与帮助中心', () => {
  // 画布左侧控件库按分类过滤注册表
  assert.match(appJs, /controlsByCategory/, '控件库/帮助中心仍按分类消费注册表');
  assert.match(appJs, /async function loadControls\(\)/, 'loadControls 不能删（画布控件库依赖它）');
  assert.match(appJs, /await api\.getControls\(\)/, '仍从 /api/controls 读取注册表');
});
