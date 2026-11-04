const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const root = path.resolve(__dirname, '..');
const html = fs.readFileSync(path.join(root, 'public/index.html'), 'utf8');
const appJs = fs.readFileSync(path.join(root, 'public/js/app.js'), 'utf8');
const backendRoot = path.resolve(root, '..', 'backend/src/main/java/com/datastream/mvp');

test('告警聚合：告警中心展示合并次数，且后端有聚合器', () => {
  // 前端：重复列 + ×N 标记
  assert.match(html, /label="重复"/);
  assert.match(html, /scope\.row\.occurrences \|\| 1\) > 1/);
  assert.match(html, /×\{\{ scope\.row\.occurrences \}\}/);

  // 后端：Aggregator 存在，AlertService 用它降噪，INFO（恢复通知）不参与聚合
  const aggregator = fs.readFileSync(path.join(backendRoot, 'service/AlertAggregator.java'), 'utf8');
  assert.match(aggregator, /public String signatureOf\(String event, String message, java\.util\.Collection<String> noise\)/);
  assert.match(aggregator, /app\.alert\.aggregation-window-ms/);
  assert.match(aggregator, /public boolean allowAutoDiagnosis\(String signature\)/);

  const alertService = fs.readFileSync(path.join(backendRoot, 'service/AlertService.java'), 'utf8');
  assert.match(alertService, /private final AlertAggregator alertAggregator;/);
  assert.match(alertService, /!\s*"INFO"\.equalsIgnoreCase\(lvl\)/, 'INFO 级恢复通知必须逐条闭环，不聚合');
  assert.match(alertService, /同类告警已合并/);
  assert.match(alertService, /mergeIntoExisting/);
});

test('文件导入建议：对话框有 AI 建议入口、建议面板与一键改名', () => {
  assert.match(appJs, /async inspectImportedFile\(path, controlType\)/);
  assert.match(appJs, /\/ai\/inspect-file/);
  assert.match(appJs, /async function loadImportAdvice\(\)/);
  assert.match(appJs, /function applyImportRenames\(\)/);
  assert.match(appJs, /const importAdvice = ref\(null\)/);
  // 打开/关闭对话框要复位建议，避免上一个文件的建议残留
  const open = appJs.slice(appJs.indexOf('function openImportDialog('), appJs.indexOf('function buildImportParams('));
  assert.match(open, /importAdvice\.value = null/);
  const closed = appJs.slice(appJs.indexOf('async function onImportDialogClosed()'),
    appJs.indexOf('async function onImportDialogClosed()') + 800);
  assert.match(closed, /importAdvice\.value = null/);

  assert.match(html, /class="file-import-advice"/);
  assert.match(html, /@click="loadImportAdvice"/);
  assert.match(html, /@click="applyImportRenames"/);
  assert.match(html, /v-for="\(item, idx\) in \(importAdvice\.suggestions \|\| \[\]\)"/);
  // 建议来自模型还是本地规则要标出来（source）
  assert.match(html, /\{\{ importAdvice\.source \}\}/);
});

test('Kafka 字段推断：仅 kafka_input 节点出现按钮，写入 fieldsConfig 并立即应用', () => {
  assert.match(appJs, /async inferKafkaSchema\(topic, sampleSize\)/);
  assert.match(appJs, /\/kafka\/infer-schema/);
  assert.match(appJs, /async function inferKafkaFields\(\)/);
  assert.match(appJs, /nodeParams\.value\.fieldsConfig = res\.fieldsConfig/);
  assert.match(appJs, /applyParams\(\);/, '推断后必须写回节点，不能只改面板');
  // 推断结果里的字段名来自 Kafka 消息，弹窗用 HTML 拼接 -> 必须转义
  assert.match(appJs, /function escapeHtmlText\(value\)/);
  assert.match(appJs, /escapeHtmlText\(col\.name\)/);

  assert.match(html, /v-if="canEdit && selectedNode\?\.data\?\.type === 'kafka_input'"/);
  assert.match(html, /@click="inferKafkaFields"/);
  assert.match(html, /从 topic 推断字段/);
});

test('三个新能力的后端端点与权限', () => {
  const ai = fs.readFileSync(path.join(backendRoot, 'controller/AiController.java'), 'utf8');
  assert.match(ai, /@PostMapping\("\/inspect-file"\)/);
  assert.match(ai, /@PreAuthorize\("hasAnyRole\('ADMIN','OPERATOR'\)"\)[\s\S]{0,200}inspectFile/);

  const kafka = fs.readFileSync(path.join(backendRoot, 'controller/KafkaMonitorController.java'), 'utf8');
  assert.match(kafka, /@org\.springframework\.web\.bind\.annotation\.PostMapping\("\/infer-schema"\)/);
  assert.match(kafka, /kafkaSchemaInferenceService\.infer/);

  const advisor = fs.readFileSync(path.join(backendRoot, 'ai/ImportAdvisorService.java'), 'utf8');
  // 建议器只发结构 + 少量样本行（与 AgentService 同一脱敏口径）
  assert.match(advisor, /LLM_SAMPLE_ROWS = 3/);
  assert.match(advisor, /llmClient\.isConfigured\(\)/);

  const inference = fs.readFileSync(path.join(backendRoot, 'service/KafkaSchemaInferenceService.java'), 'utf8');
  assert.match(inference, /fieldsConfig/);
  assert.match(inference, /DataTypeInference\.infer/);
});

test('新增状态与函数已暴露给模板', () => {
  const exported = appJs.slice(appJs.lastIndexOf('return {'));
  for (const name of ['importAdvice', 'importAdviceLoading', 'loadImportAdvice', 'applyImportRenames',
    'kafkaInferLoading', 'inferKafkaFields']) {
    assert.ok(exported.includes(name), `setup() 未返回 ${name}`);
  }
});
