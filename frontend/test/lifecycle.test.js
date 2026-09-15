const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

// 资源生命周期回归：initGraph 可重入（登录懒初始化 / 切换视图 / 重载画布都会走到），
// 早期实现每次都给 window 挂一个匿名 resize 监听且从不移除，反复登录登出会累积监听器与图表实例。
const root = path.resolve(__dirname, '..');
const appJs = fs.readFileSync(path.join(root, 'public/js/app.js'), 'utf8');

test('画布 resize 监听使用可移除句柄，重绑前先解绑', () => {
  assert.match(appJs, /let resizeHandler = null/, '需保存 resize 监听句柄');
  assert.match(appJs, /if \(resizeHandler\) \{\s*window\.removeEventListener\('resize', resizeHandler\);/,
    'initGraph 重入时必须先移除旧监听');
  assert.match(appJs, /window\.addEventListener\('resize', resizeHandler\)/, '应绑定命名句柄而非匿名函数');
  assert.doesNotMatch(appJs, /window\.addEventListener\('resize', \(\) => \{/, '不应再使用匿名 resize 监听');
});

test('卸载与登出会释放图表实例、画布与 resize 监听', () => {
  assert.match(appJs, /function disposeAllCharts\(\)/, '需有统一的图表释放函数');
  for (const chart of ['trendChart', 'kafkaChart.value', 'workflowChart', 'lineageChart']) {
    assert.ok(appJs.includes(chart), `disposeAllCharts 需覆盖 ${chart}`);
  }
  const unmountIdx = appJs.indexOf('onUnmounted(() => {');
  assert.ok(unmountIdx > 0, '应有 onUnmounted 钩子');
  const unmountBody = appJs.slice(unmountIdx, unmountIdx + 700);
  assert.match(unmountBody, /disposeAllCharts\(\)/, '卸载时必须释放图表与画布');
  assert.match(unmountBody, /removeEventListener\('resize', resizeHandler\)/, '卸载时必须移除 resize 监听');

  const logoutIdx = appJs.indexOf('async function logout()');
  const logoutBody = appJs.slice(logoutIdx, logoutIdx + 900);
  assert.match(logoutBody, /disposeAllCharts\(\)/, '登出时应释放图表与画布');
  assert.match(logoutBody, /removeEventListener\('resize', resizeHandler\)/, '登出时应移除 resize 监听');
  assert.match(logoutBody, /stopKafkaStream\(\)/, '登出时应停止 Kafka SSE 流');
});
