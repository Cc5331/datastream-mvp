const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

// 字段拼接控件（转换控件区域）回归：
//   画布「转换」页签的控件来自后端注册表，后端不可用时用 getBuiltinControls() 降级副本。
//   历史缺口：降级副本把 fields 声明成 array，而参数面板对 array 不给默认值
//   （defaultParamsFromSchema 只吸收有 default 的字段），拖出来的节点 params 里没有 fields，
//   提交时后端翻译层直接报「字段拼接: 请填写要拼接的字段」。
//   本用例把两端 schema 钉死为「逗号分隔字符串」，并守住默认值存在。
const root = path.resolve(__dirname, '..');
const appJs = fs.readFileSync(path.join(root, 'public/js/app.js'), 'utf8');

function builtinConcatControl() {
  const block = appJs.slice(appJs.indexOf('function getBuiltinControls()'));
  const arrayLiteral = block.match(/return\s*\[([\s\S]*?)\n\s*\];/);
  assert.ok(arrayLiteral, 'getBuiltinControls 应返回控件数组');
  const controls = vm.runInNewContext('[' + arrayLiteral[1] + ']');
  const ctrl = Array.from(controls).find(c => c && c.type === 'field_concat');
  assert.ok(ctrl, '降级副本应包含 field_concat（字段拼接）控件');
  return ctrl;
}

test('字段拼接控件在转换分组且名为「字段拼接」', () => {
  const ctrl = builtinConcatControl();
  assert.equal(ctrl.category, 'transform', '字段拼接必须归属转换控件区域');
  assert.equal(ctrl.name, '字段拼接');
});

test('字段拼接的 fields 是逗号分隔字符串且带默认值', () => {
  const ctrl = builtinConcatControl();
  const schema = JSON.parse(ctrl.paramSchema);
  const fields = schema.properties.fields;
  assert.equal(fields.type, 'string', 'fields 必须是逗号分隔字符串，array 型拿不到默认值');
  assert.equal(typeof fields.default, 'string', 'fields 必须有字符串默认值，拖拽建节点时才不会空着');
  assert.ok(fields.default.includes(','), '默认值应是逗号分隔的示例字段');
  assert.equal(schema.properties.newFieldName.type, 'string');
  assert.ok(schema.required.includes('fields') && schema.required.includes('newFieldName'),
    'fields / newFieldName 应为必填');
  assert.ok(fields.placeholder, '应给出字段名写法提示（前端参数面板读 param.placeholder）');
});

test('前后端字段拼接 schema 一致（红线：双份注册表）', () => {
  const backendSrc = fs.readFileSync(
    path.resolve(root, '..', 'backend/src/main/java/com/datastream/mvp/config/DataInitializer.java'), 'utf8');
  const idx = backendSrc.indexOf('createControl("field_concat"');
  assert.ok(idx > 0, '后端应注册 field_concat');
  const block = backendSrc.slice(idx, backendSrc.indexOf('"1.0.0"', idx));
  assert.ok(block.includes('"字段拼接"'), '后端控件名也是「字段拼接」');
  assert.ok(block.includes('"transform"'), '后端归属 transform');
  assert.ok(block.includes('\\"fields\\":{\\"type\\":\\"string\\"'), '后端 fields 也必须是 string');

  const ctrl = builtinConcatControl();
  const feSchema = JSON.parse(ctrl.paramSchema);
  assert.ok(block.includes('\\"default\\":\\"' + feSchema.properties.fields.default + '\\"'),
    '前后端 fields 默认值必须一致');
  assert.ok(block.includes('\\"newFieldName\\":{\\"type\\":\\"string\\",\\"title\\":\\"新字段名\\",\\"default\\":\\"' +
    feSchema.properties.newFieldName.default + '\\"'),
    '前后端 newFieldName 默认值必须一致');
});

test('参数面板按 schema 默认值初始化节点 params（不依赖 array 分支）', () => {
  assert.match(appJs, /function defaultParamsFromSchema\(schema\)/);
  assert.match(appJs, /params: defaultParamsFromSchema\(data\.paramSchema\)/);
  const ctrl = builtinConcatControl();
  const schema = JSON.parse(ctrl.paramSchema);
  const params = {};
  for (const [key, prop] of Object.entries(schema.properties)) {
    if (prop && prop.default !== undefined && prop.default !== null) params[key] = prop.default;
  }
  assert.equal(params.fields, schema.properties.fields.default,
    '拖出字段拼接节点时 params.fields 应已有默认值（字段拼接开箱可用）');
  assert.ok(params.newFieldName, 'params.newFieldName 也应有默认值');
});
