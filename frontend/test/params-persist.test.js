const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

// 参数持久化回归（2026-09-16 用户实测：面板里明明填了文件路径，提交前检查却报「缺少必填参数: path」）：
//   ① 拖拽建节点时 params 固定为 {} —— 面板显示的只是 schema 默认值，节点里其实是空的；
//   ② 切换节点时不写回面板改动 —— 上一个节点改的参数被静默丢弃；
//   ③ 提交前不落盘 —— 后端 preflight 校验的是「已保存的 DAG」，与画布现状错位。
const root = path.resolve(__dirname, '..');
const appJs = fs.readFileSync(path.join(root, 'public/js/app.js'), 'utf8');

test('拖拽建节点时用 paramSchema 默认值初始化 params', () => {
  assert.match(appJs, /function defaultParamsFromSchema\(schema\)/, '应有默认值提取函数');
  assert.match(appJs, /params: defaultParamsFromSchema\(data\.paramSchema\)/,
    'addNode 时 params 需由 paramSchema 默认值初始化，不能是空对象');
  assert.doesNotMatch(appJs, /params: \{\},\s*\n\s*paramSchema: data\.paramSchema/,
    '不允许再出现 params: {} 的写法');
  // 默认值提取要过滤掉没有 default 的字段
  const fnIdx = appJs.indexOf('function defaultParamsFromSchema(schema)');
  const body = appJs.slice(fnIdx, fnIdx + 420);
  assert.match(body, /prop\.default !== undefined && prop\.default !== null/,
    '只吸收显式声明了 default 的字段');
});

test('切换节点前写回上一个节点的面板改动', () => {
  const idx = appJs.indexOf("graph.on('node:click'");
  assert.ok(idx > 0, '应有 node:click 处理');
  const body = appJs.slice(idx, idx + 420);
  assert.match(body, /if \(selectedNode\.value && selectedNode\.value !== node\) applyParams\(\);/,
    '切换节点前必须 applyParams()，否则面板改动被静默丢弃');
});

test('提交前先应用参数并静默保存，再做提交前检查', () => {
  const idx = appJs.indexOf('async function submitJob()');
  assert.ok(idx > 0, '应有 submitJob');
  const body = appJs.slice(idx, idx + 900);
  const applyAt = body.indexOf('applyParams()');
  const saveAt = body.indexOf('saveDag({ silent: true })');
  const preflightAt = body.indexOf('runPreflight(');
  assert.ok(applyAt > 0, '提交前需 applyParams()');
  assert.ok(saveAt > 0, '提交前需 saveDag({ silent: true }) 落盘');
  assert.ok(preflightAt > 0, '提交前需 runPreflight()');
  assert.ok(applyAt < saveAt && saveAt < preflightAt,
    '顺序必须是：应用参数 → 保存 → 提交前检查（preflight 校验的是已保存的 DAG）');
});

test('saveDag 支持静默模式且返回结果', () => {
  assert.match(appJs, /async function saveDag\(options = \{\}\)/, 'saveDag 需接受选项');
  assert.match(appJs, /if \(!silent\) ElMessage\.success\('DAG 已保存/, '静默模式不应弹保存成功提示');
  assert.match(appJs, /return true;/, 'saveDag 需返回成功标记供提交链路判断');
});
