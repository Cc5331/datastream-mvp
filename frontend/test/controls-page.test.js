const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

// 控件管理页：2026-09-16 曾因「纯只读表格 + 与画布控件库/帮助中心重复」下线（A 方案摘入口），
// 2026-09-18 按需求恢复，并在恢复时增强为「带统计/搜索/分类筛选/插件来源标识」的只读视图。
// 本用例守住它别再被摘掉，同时确认注册表消费链（画布控件库 + 帮助中心控件说明）仍在。
const root = path.resolve(__dirname, '..');
const html = fs.readFileSync(path.join(root, 'public/index.html'), 'utf8');
const appJs = fs.readFileSync(path.join(root, 'public/js/app.js'), 'utf8');
const css = fs.readFileSync(path.join(root, 'public/css/style.css'), 'utf8');

test('开发资源下拉里有「控件管理」入口，且指向独立视图', () => {
  assert.match(html, /<el-dropdown-item command="controls"><el-icon><grid \/><\/el-icon>控件管理<\/el-dropdown-item>/,
    '开发资源下拉需有「控件管理」入口（图标 + 文字）');
  assert.match(html, /v-show="currentView === 'controls'"/, '应有独立的 controls 视图');
  assert.match(html, /class="controls-view"/, '视图容器需带 controls-view 类');
  assert.match(css, /\.controls-view \{ padding:18px; height:100%; overflow:auto; \}/, 'style.css 需有 controls-view 样式');
});

test('视图表格列与只读语义保持', () => {
  for (const col of ['类型标识', '名称', '分类', '描述', '版本', '来源', '启用']) {
    assert.ok(html.includes(`label="${col}"`), `控件表应含「${col}」列`);
  }
  assert.match(html, /<el-switch v-model="scope\.row\.enabled" disabled><\/el-switch>/,
    '启用列应是 disabled 开关（只读展示，页面不提供在线编辑）');
  assert.match(html, /controls-hint/, '需保留只读说明');
  assert.match(html, /DataInitializer/, '说明里要交代注册表由后端启动种子维护');
});

test('增强项：统计口径、搜索与分类筛选', () => {
  assert.match(html, /\{\{ controlStats\.input \}\} \/ 转换 \{\{ controlStats\.transform \}\} \/ 输出 \{\{ controlStats\.output \}\}/, '头部需展示分类统计');
  assert.match(html, /内置 \{\{ controlStats\.builtin \}\} \+ 插件 \{\{ controlStats\.plugin \}\}/, '头部需区分内置与插件');
  assert.match(html, /v-model="controlKeyword"/, '需有关键词搜索');
  assert.match(html, /<el-option label="全部分类" value=""><\/el-option>/, '分类筛选需含「全部分类」');
  assert.match(html, /命中 \{\{ controlRows\.length \}\} 个/);
  assert.match(html, /isPluginControl\(scope\.row\) \? '插件 ' \+ scope\.row\.jarPath : '内置'/, '来源列需区分内置/插件');
  assert.match(appJs, /const controlStats = computed/, '需有统计 computed');
  assert.match(appJs, /const controlRows = computed/, '需有筛选 computed');
  assert.match(appJs, /const isPluginControl = \(row\) => !!\(row && row\.jarPath && row\.jarPath !== 'built-in'\)/);
});

test('切到该视图会刷新注册表，且 loadControls 维护 loading 状态', () => {
  assert.match(appJs, /else if \(v === 'controls'\) \{ loadControls\(\); \}/, '切视图应触发 loadControls');
  const idx = appJs.indexOf('async function loadControls()');
  const body = appJs.slice(idx, idx + 420);
  assert.match(body, /controlsLoading\.value = true;/, 'loadControls 需置 loading');
  assert.match(body, /finally \{[\s\S]{0,60}?controlsLoading\.value = false;/, 'loadControls 需在 finally 复位 loading');
  assert.match(body, /await api\.getControls\(\)/, '仍从 /api/controls 读取注册表');
});

test('注册表仍驱动画布控件库与帮助中心控件说明', () => {
  assert.match(appJs, /function controlsByCategory\(category\)/, '画布控件库仍按分类消费注册表');
  assert.match(html, /<el-dropdown trigger="click" @command="openHelp">/, '帮助下拉应绑定 openHelp');
  assert.match(html, /<el-dropdown-item command="controls"><el-icon><grid \/><\/el-icon>\s*控件说明<\/el-dropdown-item>/,
    '帮助中心仍应提供「控件说明」入口（与导航的「控件管理」是两个入口）');
  assert.match(appJs, /function openHelp\(tab\)/, 'openHelp 应接受标签参数以定位到控件说明页');
});

test('模板用到的名字都在 setup 返回值里暴露', () => {
  const tail = appJs.slice(appJs.lastIndexOf('return {'));
  for (const name of ['controlsLoading', 'controlKeyword', 'controlCategoryFilter', 'controlRows', 'controlStats', 'isPluginControl']) {
    assert.ok(new RegExp(`(^|[\\s,])${name}(,|\\s|$)`, 'm').test(tail), `setup 返回值缺少 ${name}`);
  }
});
