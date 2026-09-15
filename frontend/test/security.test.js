const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

// 权限可见性回归：后端 /api/kafka/** 与 /api/preview/** 已收紧为 ADMIN/OPERATOR，
// 前端必须同步隐藏入口，否则 VIEWER 点进去只会拿到 403。
const root = path.resolve(__dirname, '..');
const html = fs.readFileSync(path.join(root, 'public/index.html'), 'utf8');
const appJs = fs.readFileSync(path.join(root, 'public/js/app.js'), 'utf8');

test('Kafka 可视化入口与数据预览按钮对 VIEWER 隐藏', () => {
  assert.match(html, /<el-dropdown-item v-if="canEdit" command="kafka">/,
    'Kafka 可视化导航需带 v-if="canEdit"（后端已限制 ADMIN/OPERATOR）');
  assert.match(html, /<el-button v-if="canEdit"[\s\S]{0,160}?@click="previewNode"/,
    '「预览数据」按钮需带 v-if="canEdit"（后端 /api/preview 已限制 ADMIN/OPERATOR）');
  assert.match(appJs, /const canEdit = computed\(/, 'canEdit 需由角色推导，供上面两处使用');
  assert.match(appJs, /\['ADMIN',\s*'OPERATOR'\]\.includes\([^)]*role|role\s*!==\s*'VIEWER'/,
    'canEdit 判定需基于角色（非 VIEWER）');
});

test('登出会调用后端吊销接口而不是只清本地 token', () => {
  assert.match(appJs, /api\.logout\(\)|post\(['"]\/auth\/logout['"]/, '登出需调用 /api/auth/logout');
  assert.match(appJs, /function logout\(/, '应有 logout 方法');
});
