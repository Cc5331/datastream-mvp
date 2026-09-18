const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

// 用户管理（增删查改）回归：后端 /api/users 早已具备完整 CRUD（类级 ADMIN 限制 + 审计 +
// 防删当前账号 + 保留至少一个启用管理员），但前端此前没有入口与界面（本次补齐）。
// 这些用例守住"界面接线"不被改坏：入口权限、视图、API、权限兜底、保护规则、参数暴露。
const root = path.resolve(__dirname, '..');
const html = fs.readFileSync(path.join(root, 'public/index.html'), 'utf8');
const appJs = fs.readFileSync(path.join(root, 'public/js/app.js'), 'utf8');

test('顶栏入口：用户管理仅 ADMIN 可见，且指向 users 视图', () => {
  assert.match(html, /v-if="user\.role === 'ADMIN'" command="users"/,
    '「用户管理」入口必须带 ADMIN 判断');
  assert.match(html, /command="users"[^>]*>[\s\S]{0,80}?用户管理/);
});

test('存在用户管理视图，且具备列表与四个操作入口', () => {
  assert.match(html, /v-show="currentView === 'users'"/, '应有 users 视图容器');
  assert.match(html, /v-for|:data="pagedUsers"/, '视图应绑定用户列表');
  assert.match(html, /@click="openCreateUser"/, '应有新建入口');
  assert.match(html, /@click="openEditUser\(scope\.row\)"/, '应有编辑入口');
  assert.match(html, /@click="openResetUserPassword\(scope\.row\)"/, '应有重置密码入口');
  assert.match(html, /@click="confirmDeleteUser\(scope\.row\)"/, '应有删除入口');
  assert.match(html, /el-pagination/, '应有分页');
  assert.match(html, /v-model="userKeyword"/, '应有搜索');
});

test('api 层覆盖 REST 五个动作', () => {
  assert.match(appJs, /async getUsers\(\)[\s\S]{0,120}?axios\.get\(`\$\{API_BASE\}\/users`\)/);
  assert.match(appJs, /async createUser\(payload\)[\s\S]{0,140}?axios\.post\(`\$\{API_BASE\}\/users`/);
  assert.match(appJs, /async updateUser\(id, payload\)[\s\S]{0,160}?axios\.put\(`\$\{API_BASE\}\/users\/\$\{id\}`/);
  assert.match(appJs, /async resetUserPassword\(id, password\)[\s\S]{0,160}?\/users\/\$\{id\}\/password/);
  assert.match(appJs, /async deleteUser\(id\)[\s\S]{0,120}?axios\.delete\(`\$\{API_BASE\}\/users\/\$\{id\}`/);
});

test('权限与非管理员兜底：切到 users 会加载，非 ADMIN 会被弹回工作台', () => {
  assert.match(appJs, /else if \(v === 'users'\) \{ loadUsers\(\); \}/, '切视图应触发加载');
  const idx = appJs.indexOf('async function loadUsers()');
  const body = appJs.slice(idx, idx + 420);
  assert.match(body, /if \(user\.value\?\.role !== 'ADMIN'\)[\s\S]{0,120}?currentView\.value = 'workbench'/,
    '非 ADMIN 加载用户列表时必须弹回工作台');
});

test('保护规则：当前登录账号不可删除（按钮禁用 + 逻辑拦截）', () => {
  assert.match(html, /:disabled="isSelf\(scope\.row\)"/, '自己的删除按钮应禁用');
  const idx = appJs.indexOf('async function confirmDeleteUser(row)');
  const body = appJs.slice(idx, idx + 320);
  assert.match(body, /if \(isSelf\(row\)\) \{ ElMessage\.warning\('不能删除当前登录账号'\); return; \}/,
    '即使绕过按钮也要拦下自删');
  assert.match(appJs, /const isSelf = \(row\) => !!row && row\.id === user\.value\?\.id;/);
});

test('删除与重置密码都有二次确认，密码强度与注册/后端一致', () => {
  const del = appJs.slice(appJs.indexOf('async function confirmDeleteUser(row)'), appJs.indexOf('function openResetUserPassword'));
  assert.match(del, /ElMessageBox\.confirm\(/, '删除需二次确认');
  const rp = appJs.slice(appJs.indexOf('async function saveResetUserPassword()'));
  assert.match(rp.slice(0, 400), /pwd\.length < 8 \|\| !\/\[A-Za-z\]\/\.test\(pwd\) \|\| !\/\\d\/\.test\(pwd\)/,
    '重置密码需与注册一致的强度校验（≥8 位且含字母和数字）');
  const sv = appJs.slice(appJs.indexOf('async function saveUser()'));
  assert.match(sv.slice(0, 900), /用户名须为 3–32 位字母、数字或下划线/, '新建用户需校验用户名');
});

test('模板用到的名字都在 setup 返回值里暴露', () => {
  for (const name of ['usersLoading', 'pagedUsers', 'filteredUsers', 'userKeyword', 'userRoleFilter',
    'userPage', 'userSize', 'userDialogVisible', 'userDialogMode', 'userForm', 'userSaving',
    'resetPwdVisible', 'resetPwdSaving', 'resetPwdTarget', 'resetPwdValue', 'adminCount',
    'roleLabel', 'roleTagType', 'isSelf', 'loadUsers', 'openCreateUser', 'openEditUser',
    'saveUser', 'confirmDeleteUser', 'openResetUserPassword', 'saveResetUserPassword']) {
    const re = new RegExp(`(^|[\\s,])${name}(,|\\s|$)`, 'm');
    assert.ok(re.test(appJs.slice(appJs.lastIndexOf('return {'))), `setup 返回值缺少 ${name}`);
  }
});
