const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

const root = path.resolve(__dirname, '..');
const html = fs.readFileSync(path.join(root, 'public/index.html'), 'utf8');
const appJs = fs.readFileSync(path.join(root, 'public/js/app.js'), 'utf8');

test('拖入操作系统文件时不再当成控件库拖拽（先判 files 分支）', () => {
  const open = appJs.indexOf('dropHandler = (e) => {');
  assert.ok(open > -1, 'drop 处理函数应存在');
  const body = appJs.slice(open, open + 900);
  const filesBranch = body.indexOf('e.dataTransfer.files');
  const controlBranch = body.indexOf("getData('text/plain')");
  assert.ok(filesBranch > -1, 'drop 必须先识别拖入的文件');
  assert.ok(controlBranch > -1, '控件库拖拽分支不能丢');
  assert.ok(filesBranch < controlBranch, '文件分支必须在控件库分支之前，否则会去 JSON.parse 文件拖拽');
  assert.match(body, /handleDroppedFiles\(e\.dataTransfer\.files/, '文件拖拽应交给 handleDroppedFiles');
});

test('拖入文件走「上传 → 分析 → 用户确认」链路，确认前不落节点', () => {
  assert.match(appJs, /async analyzeImportedFile\(file\)/);
  assert.match(appJs, /\/files\/analyze/);
  assert.match(appJs, /async discardImportedFile\(path\)/);
  assert.match(appJs, /\/files\/uploaded/);
  assert.match(appJs, /async function handleDroppedFiles\(files, clientX, clientY\)/);
  assert.match(appJs, /function openImportDialog\(result\)/);
  assert.match(appJs, /function confirmImportNode\(\)/);
  assert.match(appJs, /async function onImportDialogClosed\(\)/);
  // 只有「确认生成」这条路径会落节点：handleDroppedFiles 里不得直接 addNodeOnCanvas
  const upload = appJs.slice(appJs.indexOf('async function handleDroppedFiles'), appJs.indexOf('function openImportDialog('));
  assert.doesNotMatch(upload, /addNodeOnCanvas\(/, '分析结果不能直接生成节点，必须等用户确认');
});

test('节点创建逻辑收敛到 addNodeOnCanvas（拖拽与导入共用一份 X6 配置）', () => {
  const callCount = (appJs.match(/graph\.addNode\(/g) || []).length;
  assert.equal(callCount, 2, 'graph.addNode 应只在 addNodeOnCanvas 与 loadDagToCanvas 中出现');
  assert.match(appJs, /function addNodeOnCanvas\(data, point, paramsOverride\)/);
  // 控件库拖拽仍然用 paramSchema 默认值初始化 params（红线：否则面板有值、节点是空的）
  assert.match(appJs, /params: defaultParamsFromSchema\(data\.paramSchema\)/);
  // 文件导入的分析结果通过 setData 合并进节点 params（不绕过 X6 的 change:data / 撤销栈）
  const body = appJs.slice(appJs.indexOf('function addNodeOnCanvas('), appJs.indexOf('// ===== 拖拽文件导入'));
  assert.match(body, /node\.setData\(\{/);
  assert.match(body, /Object\.assign\(\{\}, node\.getData\(\)\.params/, '导入参数需与 schema 默认值合并');
});

test('确认框参数按控件类型映射到翻译层认识的字段名', () => {
  const body = appJs.slice(appJs.indexOf('function buildImportParams(fields)'), appJs.indexOf('function confirmImportNode()'));
  for (const token of ["params.delimiter", "params.hasHeader", "params.fieldsConfig", "params.sheetName", "params.mode", "params.encoding", "params.rowTag"]) {
    assert.ok(body.includes(token), `缺少参数映射 ${token}`);
  }
  // excel_input 的翻译分支不使用 fieldsConfig，不要给它塞这个概念上无效的参数
  const excel = body.slice(body.indexOf("type === 'excel_input'"), body.indexOf("type === 'json_input'"));
  assert.doesNotMatch(excel, /fieldsConfig/, 'excel_input 不应写 fieldsConfig');
});

test('取消导入会丢弃暂存文件，确认则保留', () => {
  const body = appJs.slice(appJs.indexOf('async function onImportDialogClosed()'), appJs.indexOf('async function onImportDialogClosed()') + 700);
  assert.match(body, /if \(importConfirmed\.value\)/, '确认生成后不能删除暂存文件');
  assert.match(body, /api\.discardImportedFile\(path\)/);
});

test('确认弹窗结构与按钮图标齐备', () => {
  assert.match(html, /v-model="importDialogVisible"/);
  assert.match(html, /@closed="onImportDialogClosed"/);
  assert.match(html, /:close-on-click-modal="false"/);
  assert.match(html, /v-for="\(warning, idx\) in importForm\.warnings"/);
  assert.match(html, /<el-switch v-model="importForm\.hasHeader">/);
  assert.match(html, /importPreviewColumnDefs/);
  assert.match(html, /@click="confirmImportNode"/);
  assert.match(html, /@click="importDialogVisible = false"/);
  // 红线 34：按钮必须带图标；图标名不得与原生标签冲突（files / close / check 均安全）
  assert.match(html, /<el-button @click="importDialogVisible = false"><el-icon><close \/><\/el-icon>/);
  assert.match(html, /<el-button type="primary" @click="confirmImportNode"><el-icon><check \/><\/el-icon>/);
});

test('后端导入接口存在且与预览同权限', () => {
  const controller = fs.readFileSync(path.join(root,
    '../backend/src/main/java/com/datastream/mvp/controller/FileImportController.java'), 'utf8');
  assert.match(controller, /@RequestMapping\("\/api\/files"\)/);
  assert.match(controller, /@PreAuthorize\("hasAnyRole\('ADMIN','OPERATOR'\)"\)/);
  assert.match(controller, /@PostMapping\("\/analyze"\)/);
  assert.match(controller, /@DeleteMapping\("\/uploaded"\)/);
  assert.match(controller, /MultipartFile file/);

  const service = fs.readFileSync(path.join(root,
    '../backend/src/main/java/com/datastream/mvp/service/FileImportService.java'), 'utf8');
  // 暂存目录必须在 output 之下（output 在 app.preview.allowed-roots 内，节点才能预览）
  assert.match(service, /app\.storage\.output-root/);
  assert.match(service, /app\.files\.upload-subdir/);
  // 丢弃接口不能变成任意文件删除
  assert.match(service, /只能丢弃本次导入的暂存文件/);
});

test('新增的 setup 状态与函数已暴露给模板', () => {
  const exported = appJs.slice(appJs.lastIndexOf('return {'));
  for (const name of ['importDialogVisible', 'importForm', 'fmtFileSize',
    'IMPORT_CONTROL_OPTIONS', 'IMPORT_TYPE_OPTIONS', 'IMPORT_DELIMITER_OPTIONS',
    'importSupportsHeader', 'importPreviewColumnDefs', 'importPreviewRows',
    'confirmImportNode', 'onImportDialogClosed']) {
    assert.ok(exported.includes(name), `setup() 未返回 ${name}`);
  }
});

test('app.js 仍可被 JavaScript 引擎解析', () => {
  assert.doesNotThrow(() => new vm.Script(appJs, { filename: 'app.js' }));
});
