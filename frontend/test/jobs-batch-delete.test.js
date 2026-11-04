const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

const root = path.resolve(__dirname, '..');
const appJs = fs.readFileSync(path.join(root, 'public/js/app.js'), 'utf8');
const html = fs.readFileSync(path.join(root, 'public/index.html'), 'utf8');
const backendRoot = path.resolve(root, '..', 'backend/src/main/java/com/datastream/mvp');

test('作业列表有「批量删除」按钮：未选中禁用、带图标、走确认', () => {
  assert.match(html, /@click="confirmBatchDelete"/);
  assert.match(html, /<el-button size="small" type="danger" :disabled="!selectedJobs\.length" :loading="batchOperating" @click="confirmBatchDelete"><el-icon><delete \/><\/el-icon> 批量删除<\/el-button>/);
  // 与批量上/下线同一行、同一个 batchOperating 加载态
  const bar = html.slice(html.indexOf('批量上线'), html.indexOf('批量下线') + 400);
  assert.match(bar, /批量删除/, '批量删除应和批量上/下线放在一起');
});

test('批量删除前端逻辑：二次确认 + 运行中提示 + 逐项结果回报 + 刷新列表', () => {
  assert.match(appJs, /async function confirmBatchDelete\(\)/);
  assert.match(appJs, /batchDeleteJobs\(ids\)/);
  assert.match(appJs, /async batchDeleteJobs\(ids\)[\s\S]{0,120}\/jobs\/batch-delete/);

  const fn = appJs.slice(appJs.indexOf('async function confirmBatchDelete()'),
    appJs.indexOf('async function confirmBatchOnline()'));
  // 破坏性操作必须二次确认，且明说不可恢复
  assert.match(fn, /ElMessageBox\.confirm\(/, '必须二次确认');
  assert.match(fn, /删除后不可恢复/, '确认文案要说明不可恢复');
  // 运行中的作业会被跳过 -> 先告知用户，避免"点了没删掉却不知道原因"
  assert.match(fn, /row\.status === 'SUBMITTED' \|\| row\.status === 'RUNNING'/);
  assert.match(fn, /会被跳过/);
  // 结果回报 + 刷新
  assert.match(fn, /showBatchResult\('删除', result\)/);
  assert.match(fn, /await loadJobs\(\)/);
  assert.match(fn, /batchOperating\.value = true/);
  assert.match(fn, /batchOperating\.value = false/);
  // 取消确认不应该弹错误
  assert.match(fn, /if \(err !== 'cancel'\)/);
});

test('批量删除已暴露给模板，且不影响既有批量上/下线', () => {
  const exported = appJs.slice(appJs.lastIndexOf('return {'));
  assert.ok(exported.includes('confirmBatchDelete'), 'setup() 未返回 confirmBatchDelete');
  assert.ok(exported.includes('batchChangeOnline'));
  assert.ok(exported.includes('batchOperating'));
  assert.doesNotThrow(() => new vm.Script(appJs, { filename: 'app.js' }));
});

test('后端端点：POST /api/jobs/batch-delete，权限/审计/逐项独立事务', () => {
  const controller = fs.readFileSync(path.join(backendRoot, 'controller/JobController.java'), 'utf8');
  assert.match(controller, /@PostMapping\("\/batch-delete"\)/);
  assert.match(controller, /@Audit\(action = "JOB_BATCH_DELETE"/);
  assert.match(controller, /public Map<String, Object> batchDelete\(@RequestBody BatchJobRequest request\)/);
  // 与批量上/下线同一返回契约
  assert.match(controller, /result\.put\("succeeded", succeeded\)/);
  assert.match(controller, /result\.put\("failed", failed\)/);
  // 逐项 owner 校验 + 运行中跳过 + 单项失败不影响其余
  assert.match(controller, /jobService\.findByIdForUser\(id, currentUser\(\)\)/);
  assert.match(controller, /jobService\.assertDeletable\(job\)/);
  assert.match(controller, /jobService\.delete\(id\)/);

  const service = fs.readFileSync(path.join(backendRoot, 'service/JobService.java'), 'utf8');
  assert.match(service, /public void assertDeletable\(JobDefinition job\)/);
  assert.match(service, /请先取消或下线后再删除/);
  // 删除必须仍是事务方法（版本/依赖/作业三处删除要同一个事务）
  assert.match(service, /@Transactional\s+public void delete\(Long id\)/);
});

test('批量删除不会被写成外层事务（红线：内层异常不连坐其它项）', () => {
  const controller = fs.readFileSync(path.join(backendRoot, 'controller/JobController.java'), 'utf8');
  const block = controller.slice(controller.indexOf('public Map<String, Object> batchDelete'),
    controller.indexOf('private Map<String, Object> runBatch'));
  assert.doesNotMatch(block, /@Transactional/, '批量方法不能加外层 @Transactional，否则内层失败会把外层标记 rollback-only');
});
