/**
 * 无头浏览器端到端验证：用户管理「增删查改」（Edge + CDP）。
 *
 * 覆盖：管理菜单入口(ADMIN) → 列表 → 新建 → 编辑(改显示名) → 重置密码 → 删除
 *       → 校验后端审计记录出现 USER_CREATE / USER_UPDATE / USER_PASSWORD_RESET / USER_DELETE
 * 脚本自清理：无论成功失败都会尝试删掉本次创建的测试账号。
 *
 * 前置：后端 18080 + 前端 3000 已启动；admin/admin123 可用；有 Edge；ws 可解析。
 * 用法：node scripts/verify-users-crud.cjs
 * 退出码：0 = 全部 PASS；1 = 有断言失败；2/3 = 环境问题
 */
const { spawn } = require('child_process');
const http = require('http');
const net = require('net');
const fs = require('fs');
const path = require('path');
const os = require('os');

const EDGE = process.env.EDGE_PATH || 'C:\\Program Files (x86)\\Microsoft\\Edge\\Application\\msedge.exe';
const APP = process.env.VERIFY_APP || 'http://127.0.0.1:3000';
const API = process.env.VERIFY_API || 'http://127.0.0.1:18080';
const USER = process.env.VERIFY_USER || 'admin';
const PASSWORD = process.env.VERIFY_PASSWORD || 'admin123';
const USER_DATA = path.join(os.tmpdir(), 'dsh-edge-users-' + process.pid);
let PORT = parseInt(process.env.CDP_PORT || '0', 10);

function loadWs() {
    try { return require('ws'); } catch (_) { /* fallthrough */ }
    const extra = process.env.DSH_WS_NODE_MODULES;
    if (extra) return require(path.join(extra, 'ws'));
    console.error('ERROR: 找不到 ws 模块（可设 DSH_WS_NODE_MODULES=<含 ws 的 node_modules>）');
    process.exit(2);
}
const WebSocket = loadWs();
const sleep = ms => new Promise(r => setTimeout(r, ms));

const pickFreePort = (start = 9500) => new Promise(resolve => {
    let p = start;
    const attempt = () => {
        if (p > start + 80) return resolve(start);
        const s = net.createServer();
        s.once('error', () => { p++; attempt(); });
        s.once('listening', () => s.close(() => resolve(p)));
        s.listen(p, '127.0.0.1');
    };
    attempt();
});

const httpJson = (port, p) => new Promise((res, rej) => {
    http.get({ host: '127.0.0.1', port, path: p }, r => {
        let b = ''; r.on('data', c => b += c); r.on('end', () => { try { res(JSON.parse(b)); } catch (e) { rej(e); } });
    }).on('error', rej);
});

class Cdp {
    constructor(ws) { this.ws = ws; this.id = 0; this.pending = new Map(); }
    static async connect(url) {
        const ws = new WebSocket(url, { perMessageDeflate: false });
        await new Promise((res, rej) => { ws.once('open', res); ws.once('error', rej); });
        const c = new Cdp(ws);
        ws.on('message', d => {
            const m = JSON.parse(d.toString());
            if (m.id && c.pending.has(m.id)) {
                const { res, rej } = c.pending.get(m.id); c.pending.delete(m.id);
                m.error ? rej(new Error(JSON.stringify(m.error))) : res(m.result);
            }
        });
        return c;
    }
    send(method, params = {}) {
        const id = ++this.id;
        return new Promise((res, rej) => {
            this.pending.set(id, { res, rej });
            this.ws.send(JSON.stringify({ id, method, params }));
            setTimeout(() => { if (this.pending.has(id)) { this.pending.delete(id); rej(new Error(method + ' timeout')); } }, 30000);
        });
    }
    async eval(expression) {
        const r = await this.send('Runtime.evaluate', { expression, awaitPromise: true, returnByValue: true });
        if (r.exceptionDetails) throw new Error(r.exceptionDetails.text + ' ' + (r.exceptionDetails.exception?.description || ''));
        return r.result.value;
    }
}

// 页面内助手：全部通过真实 DOM 操作（Element Plus 用 click 监听，合成 click 有效）
const HELPERS = `
window.__u = {
  visible: (sel) => [...document.querySelectorAll(sel)].filter(e => e.offsetParent !== null),
  btn: (text) => [...document.querySelectorAll('button')].filter(b => b.offsetParent !== null)
      .find(b => (b.textContent || '').replace(/\\s+/g, '').includes(text)),
  // 打开「管理」下拉并点某菜单项
  menu: (item) => {
      const trigger = [...document.querySelectorAll('button')].filter(b => b.offsetParent !== null)
          .find(b => (b.textContent || '').trim().startsWith('管理'));
      if (!trigger) return 'no-trigger';
      trigger.click();
      const target = [...document.querySelectorAll('.el-dropdown-menu__item')]
          .find(i => (i.textContent || '').replace(/\\s+/g, '').includes(item));
      if (!target) return 'no-item';
      target.click();
      return 'clicked';
  },
  rows: () => [...document.querySelectorAll('.users-view .el-table__body-wrapper tbody tr')]
      .map(tr => [...tr.querySelectorAll('td')].map(td => (td.textContent || '').trim())),
  // 打开对话框：按按钮文字点击后等 dialog 出现
  dialogInputs: () => {
      const dlg = [...document.querySelectorAll('.el-dialog')].filter(d => d.offsetParent !== null).pop();
      return dlg ? [...dlg.querySelectorAll('input')] : [];
  },
  fill: (placeholderPart, value) => {
      const dlg = [...document.querySelectorAll('.el-dialog')].filter(d => d.offsetParent !== null).pop();
      if (!dlg) return 'no-dialog';
      const input = [...dlg.querySelectorAll('input')].find(i => (i.placeholder || '').includes(placeholderPart));
      if (!input) return 'no-input:' + placeholderPart;
      input.value = value;
      input.dispatchEvent(new Event('input', { bubbles: true }));
      input.dispatchEvent(new Event('change', { bubbles: true }));
      return 'ok';
  },
  dialogBtn: (text) => {
      const dlg = [...document.querySelectorAll('.el-dialog')].filter(d => d.offsetParent !== null).pop();
      if (!dlg) return 'no-dialog';
      const b = [...dlg.querySelectorAll('button')].find(x => (x.textContent || '').replace(/\\s+/g, '').includes(text));
      if (!b) return 'no-btn:' + text;
      b.click();
      return 'ok';
  },
  // 行内按钮：第 index 行（0 基）里文字匹配的按钮
  rowBtn: (username, text) => {
      const tr = [...document.querySelectorAll('.users-view .el-table__body-wrapper tbody tr')]
          .find(r => (r.textContent || '').includes(username));
      if (!tr) return 'no-row';
      const b = [...tr.querySelectorAll('button')].find(x => (x.textContent || '').replace(/\\s+/g, '').includes(text));
      if (!b) return 'no-btn';
      if (b.disabled) return 'disabled';
      b.click();
      return 'ok';
  },
  confirmBox: (text) => {
      const box = [...document.querySelectorAll('.el-message-box')].filter(d => d.offsetParent !== null).pop();
      if (!box) return 'no-box';
      const b = [...box.querySelectorAll('button')].find(x => (x.textContent || '').replace(/\\s+/g, '').includes(text));
      if (!b) return 'no-btn';
      b.click();
      return 'ok';
  },
  toasts: () => [...document.querySelectorAll('.el-message')].map(m => (m.textContent || '').trim()),
  search: (value) => {
      const input = [...document.querySelectorAll('input')].filter(i => i.offsetParent !== null)
          .find(i => (i.placeholder || '').includes('搜索用户名'));
      if (!input) return 'no-search';
      input.value = value;
      input.dispatchEvent(new Event('input', { bubbles: true }));
      return 'ok';
  },
  // 勾选/取消勾选某一行（Element Plus 表格选择列）
  toggleRow: (username) => {
      const tr = [...document.querySelectorAll('.users-view .el-table__body-wrapper tbody tr')]
          .find(r => (r.textContent || '').includes(username));
      if (!tr) return 'no-row';
      const box = tr.querySelector('.el-checkbox');
      if (!box) return 'no-checkbox';
      if (box.classList.contains('is-disabled')) return 'disabled';
      box.click();
      return 'ok';
  },
  selectedCount: () => ([...document.querySelectorAll('.users-view .el-table__body-wrapper tbody tr')]
      .filter(tr => tr.querySelector('.el-checkbox.is-checked')).length),
  statusOf: (username) => {
      const tr = [...document.querySelectorAll('.users-view .el-table__body-wrapper tbody tr')]
          .find(r => (r.textContent || '').includes(username));
      if (!tr) return 'no-row';
      const text = tr.textContent || '';
      if (text.includes('已禁用')) return 'disabled';
      if (text.includes('启用')) return 'enabled';
      return 'unknown';
  }
};
'ok'
`;

(async () => {
    if (!PORT) PORT = await pickFreePort();
    const edgeLog = path.join(os.tmpdir(), 'dsh-edge-users-' + process.pid + '.log');
    const browser = spawn(EDGE, ['--headless=new', `--remote-debugging-port=${PORT}`, `--user-data-dir=${USER_DATA}`,
        '--no-first-run', '--no-default-browser-check', '--disable-gpu', '--window-size=1600,1000', 'about:blank'],
        { stdio: ['ignore', 'ignore', 'pipe'] });
    browser.stderr.pipe(fs.createWriteStream(edgeLog));

    let target = null;
    for (let i = 0; i < 60 && !target; i++) {
        await sleep(500);
        try { target = (await httpJson(PORT, '/json/list')).find(t => t.type === 'page'); } catch (_) { /* retry */ }
    }
    if (!target) {
        console.error(`FAIL: 无法连接无头浏览器（端口 ${PORT}）`);
        try { console.error(fs.readFileSync(edgeLog, 'utf8').trim().split('\n').slice(-4).join('\n')); } catch (_) {}
        browser.kill(); process.exit(2);
    }
    const cdp = await Cdp.connect(target.webSocketDebuggerUrl);
    await cdp.send('Page.enable'); await cdp.send('Runtime.enable');

    const results = [];
    const check = (name, ok, detail) => {
        results.push({ name, ok });
        console.log(`  ${ok ? '✅ PASS' : '❌ FAIL'}  ${name}${detail ? ' — ' + detail : ''}`);
    };

    const stamp = Date.now().toString().slice(-6);
    const NEW_USER = 'e2e_u' + stamp;
    const NEW_DISPLAY = 'E2E 测试账号';
    const EDITED_DISPLAY = 'E2E 改名后';

    // 后台 API 助手（用 curl 之外的方式：页面内 fetch，避免 CORS 与 token 处理）
    await cdp.send('Page.navigate', { url: APP });
    await sleep(2500);
    const login = await cdp.eval(`(async () => {
        const r = await fetch('${API}/api/auth/login', { method:'POST', headers:{'Content-Type':'application/json'},
            body: JSON.stringify({ username: ${JSON.stringify(USER)}, password: ${JSON.stringify(PASSWORD)} }) });
        if (!r.ok) return 'HTTP ' + r.status;
        const d = await r.json();
        localStorage.setItem('token', d.token);
        return d.user.role;
    })()`);
    if (login !== 'ADMIN') { console.error('FAIL: 管理员登录失败 ' + login); browser.kill(); process.exit(2); }
    console.log('  登录角色:', login);

    await cdp.send('Page.navigate', { url: APP });
    await sleep(3000);
    await cdp.eval(HELPERS);

    // 1) 入口 → 列表
    const opened = await cdp.eval(`window.__u.menu('用户管理')`);
    await sleep(1800);
    const before = await cdp.eval(`window.__u.rows()`);
    check('管理菜单能打开用户管理视图并列出用户', opened === 'clicked' && before.length >= 3,
        `入口=${opened} 行数=${before.length}`);

    // 2) 新建
    await cdp.eval(`window.__u.btn('新建用户')?.click()`);
    await sleep(900);
    const fillName = await cdp.eval(`window.__u.fill('3–32 位字母', ${JSON.stringify(NEW_USER)})`);
    const fillDisplay = await cdp.eval(`window.__u.fill('姓名或称呼', ${JSON.stringify(NEW_DISPLAY)})`);
    const fillPwd = await cdp.eval(`window.__u.fill('至少 8 位', 'Test' + ${JSON.stringify(stamp)} + 'a1')`);
    const clickCreate = await cdp.eval(`window.__u.dialogBtn('创建')`);
    await sleep(2200);
    await cdp.eval(`window.__u.search(${JSON.stringify(NEW_USER)})`);
    await sleep(700);
    const afterCreate = await cdp.eval(`window.__u.rows()`);
    const createdRow = afterCreate.find(r => r.join(' ').includes(NEW_USER));
    check('新建用户成功并出现在列表中',
        fillName === 'ok' && fillDisplay === 'ok' && fillPwd === 'ok' && clickCreate === 'ok' && !!createdRow,
        `填充=${fillName}/${fillDisplay}/${fillPwd} 提交=${clickCreate} 行=${createdRow ? createdRow.slice(0, 4).join(' | ') : '未找到'}`);

    // 3) 编辑（改显示名）
    const editClick = await cdp.eval(`window.__u.rowBtn(${JSON.stringify(NEW_USER)}, '编辑')`);
    await sleep(900);
    await cdp.eval(`window.__u.fill('姓名或称呼', ${JSON.stringify(EDITED_DISPLAY)})`);
    const saveClick = await cdp.eval(`window.__u.dialogBtn('保存')`);
    await sleep(2200);
    const afterEdit = await cdp.eval(`window.__u.rows()`);
    check('编辑用户后列表显示新显示名',
        editClick === 'ok' && saveClick === 'ok' && afterEdit.some(r => r.join(' ').includes(EDITED_DISPLAY)),
        `编辑入口=${editClick} 保存=${saveClick}`);

    // 4) 重置密码
    const rpClick = await cdp.eval(`window.__u.rowBtn(${JSON.stringify(NEW_USER)}, '重置密码')`);
    await sleep(900);
    const rpFill = await cdp.eval(`window.__u.fill('至少 8 位', 'Reset' + ${JSON.stringify(stamp)} + 'a1')`);
    const rpConfirm = await cdp.eval(`window.__u.dialogBtn('确认重置')`);
    await sleep(1800);
    const rpToast = await cdp.eval(`window.__u.toasts()`);
    check('重置密码成功（旧登录态失效提示）',
        rpClick === 'ok' && rpFill === 'ok' && rpConfirm === 'ok' && rpToast.some(t => t.includes('密码已重置')),
        `入口=${rpClick} 填充=${rpFill} 确认=${rpConfirm} 提示=${JSON.stringify(rpToast.slice(-1))}`);

    // 5) 删除（含二次确认）
    const delClick = await cdp.eval(`window.__u.rowBtn(${JSON.stringify(NEW_USER)}, '删除')`);
    await sleep(900);
    const delConfirm = await cdp.eval(`window.__u.confirmBox('确认删除')`);
    await sleep(2200);
    const afterDelete = await cdp.eval(`window.__u.rows()`);
    check('删除用户成功（二次确认后从列表消失）',
        delClick === 'ok' && delConfirm === 'ok' && !afterDelete.some(r => r.join(' ').includes(NEW_USER)),
        `删除入口=${delClick} 确认=${delConfirm} 剩余行=${afterDelete.length}`);

    // 6) 自删保护：清掉搜索过滤后，当前登录账号的删除按钮应禁用
    await cdp.eval(`window.__u.search('')`);
    await sleep(700);
    const selfDisabled = await cdp.eval(`window.__u.rowBtn('admin', '删除')`);
    check('当前登录账号的删除按钮被禁用（自删保护）', selfDisabled === 'disabled', `返回=${selfDisabled}`);

    // 7) 后端审计四条记录齐全（审计行的 username 是操作者 admin，targetId 是被操作用户 id）
    const audit = await cdp.eval(`(async () => {
        const token = localStorage.getItem('token');
        const r = await fetch('${API}/api/audit?page=0&size=50', { headers: { Authorization: 'Bearer ' + token } });
        const d = await r.json();
        return (d.content || []).map(x => x.action + '|' + x.username + '|' + x.targetType + '|' + x.detail);
    })()`);
    const expect = ['USER_CREATE', 'USER_UPDATE', 'USER_PASSWORD_RESET', 'USER_DELETE'];
    const got = expect.filter(a => audit.some(x => x.startsWith(a + '|')));
    check('后端审计记录了创建/更新/重置/删除四类操作', got.length === 4,
        `命中=${JSON.stringify(got)}`);
    if (got.length !== 4) console.log('    最近审计:', JSON.stringify(audit.filter(x => x.startsWith('USER_')).slice(0, 6)));

    // ===== 批量操作 =====
    const U1 = 'e2e_b1_' + stamp;
    const U2 = 'e2e_b2_' + stamp;
    const created = await cdp.eval(`(async () => {
        const token = localStorage.getItem('token');
        const mk = async (username) => {
            const r = await fetch('${API}/api/users', { method:'POST',
                headers:{ 'Content-Type':'application/json', Authorization:'Bearer ' + token },
                body: JSON.stringify({ username, password: 'Batch' + ${JSON.stringify(stamp)} + 'a1',
                    displayName: '批量测试-' + username, role: 'VIEWER', enabled: true }) });
            return r.status;
        };
        await window.__u.search('');
        return [await mk(${JSON.stringify(U1)}), await mk(${JSON.stringify(U2)})];
    })()`);
    await sleep(1500);
    await cdp.eval(`window.__u.btn('刷新')?.click()`);
    await sleep(1500);

    const sel1 = await cdp.eval(`window.__u.toggleRow(${JSON.stringify(U1)})`);
    const sel2 = await cdp.eval(`window.__u.toggleRow(${JSON.stringify(U2)})`);
    const selCount = await cdp.eval(`window.__u.selectedCount()`);
    check('可勾选多个账号（已选计数同步）',
        created.every(s => s === 200 || s === 201) && sel1 === 'ok' && sel2 === 'ok' && selCount === 2,
        `创建=${JSON.stringify(created)} 勾选=${sel1}/${sel2} 已选=${selCount}`);

    const disClick = await cdp.eval(`window.__u.btn('批量禁用')?.click()`);
    await sleep(800);
    const disConfirm = await cdp.eval(`window.__u.confirmBox('确认禁用')`);
    await sleep(2200);
    const afterDisable = await cdp.eval(`({ a: window.__u.statusOf(${JSON.stringify(U1)}), b: window.__u.statusOf(${JSON.stringify(U2)}) })`);
    check('批量禁用生效（两个账号状态均为已禁用）',
        disConfirm === 'ok' && afterDisable.a === 'disabled' && afterDisable.b === 'disabled',
        `点击=${disClick === undefined ? 'ok' : disClick} 确认=${disConfirm} 状态=${JSON.stringify(afterDisable)}`);

    await cdp.eval(`window.__u.toggleRow(${JSON.stringify(U1)})`);
    await cdp.eval(`window.__u.toggleRow(${JSON.stringify(U2)})`);
    await cdp.eval(`window.__u.btn('批量启用')?.click()`);
    await sleep(800);
    await cdp.eval(`window.__u.confirmBox('确认启用')`);
    await sleep(2200);
    const afterEnable = await cdp.eval(`({ a: window.__u.statusOf(${JSON.stringify(U1)}), b: window.__u.statusOf(${JSON.stringify(U2)}) })`);
    check('批量启用生效（两个账号恢复启用）',
        afterEnable.a === 'enabled' && afterEnable.b === 'enabled', JSON.stringify(afterEnable));

    await cdp.eval(`window.__u.toggleRow(${JSON.stringify(U1)})`);
    await cdp.eval(`window.__u.toggleRow(${JSON.stringify(U2)})`);
    await cdp.eval(`window.__u.btn('批量删除')?.click()`);
    await sleep(800);
    const delNames = await cdp.eval(`(() => { const box = [...document.querySelectorAll('.el-message-box')].filter(d=>d.offsetParent!==null).pop();
        return box ? (box.textContent||'').includes(${JSON.stringify(U1)}) : false; })()`);
    const batchDelConfirm = await cdp.eval(`window.__u.confirmBox('确认删除')`);
    await sleep(2500);
    const afterBatchDelete = await cdp.eval(`({ a: window.__u.statusOf(${JSON.stringify(U1)}), b: window.__u.statusOf(${JSON.stringify(U2)}) })`);
    check('批量删除生效（确认框列出账号名，删除后两行均消失）',
        delNames === true && batchDelConfirm === 'ok' && afterBatchDelete.a === 'no-row' && afterBatchDelete.b === 'no-row',
        `确认框含账号名=${delNames} 确认=${batchDelConfirm} 结果=${JSON.stringify(afterBatchDelete)}`);

    // 批量操作审计
    const batchAudit = await cdp.eval(`(async () => {
        const token = localStorage.getItem('token');
        const r = await fetch('${API}/api/audit?page=0&size=50', { headers: { Authorization: 'Bearer ' + token } });
        const d = await r.json();
        return (d.content || []).map(x => x.action + '|' + (x.detail || ''));
    })()`);
    const batchGot = ['USER_BATCH_STATUS', 'USER_BATCH_DELETE'].filter(a => batchAudit.some(x => x.startsWith(a + '|')));
    check('批量操作写入审计（USER_BATCH_STATUS / USER_BATCH_DELETE）', batchGot.length === 2,
        `命中=${JSON.stringify(batchGot)}`);
    if (batchGot.length !== 2) console.log('    最近审计:', JSON.stringify(batchAudit.filter(x => x.startsWith('USER_')).slice(0, 6)));

    // 清理（幂等）：确保测试账号不残留
    const cleaned = await cdp.eval(`(async () => {
        const token = localStorage.getItem('token');
        const list = await (await fetch('${API}/api/users', { headers: { Authorization: 'Bearer ' + token } })).json();
        const targets = (list || []).filter(x => [${JSON.stringify(NEW_USER)}, ${JSON.stringify(U1)}, ${JSON.stringify(U2)}].includes(x.username));
        let n = 0;
        for (const u of targets) {
            const r = await fetch('${API}/api/users/' + u.id, { method: 'DELETE', headers: { Authorization: 'Bearer ' + token } });
            if (r.status === 204) n += 1;
        }
        return 'deleted ' + n + '/' + targets.length;
    })()`);
    console.log('  清理测试账号:', cleaned);

    const failed = results.filter(r => !r.ok).length;
    console.log(`\n  结果：${results.length - failed}/${results.length} 通过`);

    try { cdp.ws.close(); } catch (_) {}
    browser.kill();
    await sleep(400);
    process.exit(failed === 0 ? 0 : 1);
})().catch(e => { console.error('ERROR:', e.message); process.exit(3); });
