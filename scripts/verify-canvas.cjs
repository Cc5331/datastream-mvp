/**
 * 无头浏览器验证画布关键交互（Edge + CDP）。三组断言：
 *
 *   A. 拖拽落点：一次拖拽只产生一个控件
 *      （历史缺陷：setupDropHandler 重复绑定 drop 监听 → 一次拖拽 addNode 两次）
 *   B. 拖入的控件自带默认参数 + 提交前检查通过
 *      拖 CSV 输入 → 真指针拖拽连线到 CSV 输出 → 保存 → 读回 DAG：
 *      两个节点都应带 paramSchema 默认值（path 非空），且 GET /jobs/{id}/preflight 无错误
 *      （历史缺陷：节点 params 固定为空对象，面板看着有默认路径、检查却报「缺少必填参数: path」）
 *   C. 参数面板改动直接保存即可生效（无需先点「应用参数」）
 *      改面板里的路径 → 直接点保存 → 读回 DAG 应是新值
 *
 * 前置：后端 18080 + 前端 3000 已启动；演示账号可用；有 Edge；ws 模块可解析。
 * 用法：node scripts/verify-canvas.cjs
 * 环境变量：EDGE_PATH / VERIFY_APP / VERIFY_USER / VERIFY_PASSWORD / CDP_PORT / DSH_WS_NODE_MODULES
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
const USER = process.env.VERIFY_USER || 'admin';
const PASSWORD = process.env.VERIFY_PASSWORD || 'admin123';
const USER_DATA = path.join(os.tmpdir(), 'dsh-edge-verify-' + process.pid);
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

/** 动态挑空闲端口：本机保留端口区间较多（Docker/WSL 会登记），固定端口容易踩坑 */
function pickFreePort(start = 9500) {
    return new Promise(resolve => {
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
}

function httpJson(port, p) {
    return new Promise((resolve, reject) => {
        http.get({ host: '127.0.0.1', port, path: p }, res => {
            let body = '';
            res.on('data', c => body += c);
            res.on('end', () => { try { resolve(JSON.parse(body)); } catch (e) { reject(e); } });
        }).on('error', reject);
    });
}

class Cdp {
    constructor(ws) { this.ws = ws; this.id = 0; this.pending = new Map(); }

    static async connect(url) {
        const ws = new WebSocket(url, { perMessageDeflate: false, maxPayload: 256 * 1024 * 1024 });
        await new Promise((res, rej) => { ws.once('open', res); ws.once('error', rej); });
        const cdp = new Cdp(ws);
        ws.on('message', data => {
            const msg = JSON.parse(data.toString());
            if (msg.id && cdp.pending.has(msg.id)) {
                const { res, rej } = cdp.pending.get(msg.id);
                cdp.pending.delete(msg.id);
                msg.error ? rej(new Error(JSON.stringify(msg.error))) : res(msg.result);
            }
        });
        return cdp;
    }

    send(method, params = {}) {
        const id = ++this.id;
        return new Promise((res, rej) => {
            this.pending.set(id, { res, rej });
            this.ws.send(JSON.stringify({ id, method, params }));
            setTimeout(() => {
                if (this.pending.has(id)) { this.pending.delete(id); rej(new Error(method + ' timeout')); }
            }, 30000);
        });
    }

    async eval(expression) {
        const r = await this.send('Runtime.evaluate', { expression, awaitPromise: true, returnByValue: true });
        if (r.exceptionDetails) {
            throw new Error(r.exceptionDetails.text + ' ' + (r.exceptionDetails.exception?.description || ''));
        }
        return r.result.value;
    }

    /** 用真实指针事件拖拽（X6 的连线只认原生输入，合成 JS 事件无效） */
    async dragMouse(from, to, steps = 6) {
        await this.send('Input.dispatchMouseEvent', { type: 'mouseMoved', x: from.x, y: from.y, buttons: 0 });
        await this.send('Input.dispatchMouseEvent', { type: 'mousePressed', x: from.x, y: from.y, button: 'left', clickCount: 1, buttons: 1 });
        for (let i = 1; i <= steps; i++) {
            await this.send('Input.dispatchMouseEvent', {
                type: 'mouseMoved', button: 'left', buttons: 1,
                x: from.x + (to.x - from.x) * i / steps,
                y: from.y + (to.y - from.y) * i / steps
            });
            await sleep(60);
        }
        await this.send('Input.dispatchMouseEvent', { type: 'mouseReleased', x: to.x, y: to.y, button: 'left', clickCount: 1, buttons: 0 });
    }

    async click(x, y) {
        await this.send('Input.dispatchMouseEvent', { type: 'mouseMoved', x, y, buttons: 0 });
        await this.send('Input.dispatchMouseEvent', { type: 'mousePressed', x, y, button: 'left', clickCount: 1, buttons: 1 });
        await this.send('Input.dispatchMouseEvent', { type: 'mouseReleased', x, y, button: 'left', clickCount: 1, buttons: 0 });
    }
}

const PAGE_HELPERS = `
  window.__verify = {
    visibleButton: (text) => [...document.querySelectorAll('button')]
        .filter(b => b.offsetParent !== null)
        .find(b => (b.textContent || '').includes(text)),
    dropPayload: async (type) => {
        const controls = await (await fetch('/api/controls', {
            headers: { Authorization: 'Bearer ' + localStorage.getItem('token') } })).json();
        const ctrl = controls.find(c => c.type === type);
        return { type, name: ctrl.name, category: ctrl.category,
                 paramSchema: JSON.parse(ctrl.paramSchema || '{}'), flinkTemplate: ctrl.flinkTemplate };
    },
    drop: async (type, index) => {
        const el = document.getElementById('dag-canvas');
        const payload = await window.__verify.dropPayload(type);
        const dt = new DataTransfer();
        dt.setData('text/plain', JSON.stringify(payload));
        const rect = el.getBoundingClientRect();
        const opts = { bubbles: true, cancelable: true,
                       clientX: rect.left + 160 + index * 260, clientY: rect.top + 200, dataTransfer: dt };
        el.dispatchEvent(new DragEvent('dragover', opts));
        el.dispatchEvent(new DragEvent('drop', opts));
        return payload;
    },
    nodeIds: () => [...document.querySelectorAll('.x6-node')].map(n => n.getAttribute('data-cell-id')),
    edgeCount: () => document.querySelectorAll('.x6-edge').length,
    /** 节点右端口与目标节点中心的视口坐标（供 CDP 原生拖拽用） */
    portAndTarget: (fromId, toId) => {
        const from = document.querySelector('.x6-node[data-cell-id="' + fromId + '"]');
        const to = document.querySelector('.x6-node[data-cell-id="' + toId + '"]');
        const port = from.querySelector('[port-group="right"]');
        const a = port.getBoundingClientRect(), b = to.getBoundingClientRect();
        return { from: { x: a.left + a.width / 2, y: a.top + a.height / 2 },
                 to: { x: b.left + b.width / 2, y: b.top + b.height / 2 } };
    },
    nodeCenter: (id) => {
        const el = document.querySelector('.x6-node[data-cell-id="' + id + '"]');
        const r = el.getBoundingClientRect();
        return { x: r.left + r.width / 2, y: r.top + r.height / 2 };
    },
    /** 找到参数面板中当前值等于 oldValue 的可见输入框，写入新值并触发 v-model */
    setVisibleInputByValue: (oldValue, newValue) => {
        const input = [...document.querySelectorAll('input')]
            .filter(i => i.offsetParent !== null && i.value === oldValue)[0];
        if (!input) return false;
        input.value = newValue;
        input.dispatchEvent(new Event('input', { bubbles: true }));
        input.dispatchEvent(new Event('change', { bubbles: true }));
        return true;
    },
    latestJob: async () => {
        const token = localStorage.getItem('token');
        const jobs = await (await fetch('/api/jobs', { headers: { Authorization: 'Bearer ' + token } })).json();
        const newest = jobs.slice().sort((a, b) => b.id - a.id)[0];
        const detail = await (await fetch('/api/jobs/' + newest.id,
            { headers: { Authorization: 'Bearer ' + token } })).json();
        const preflight = await (await fetch('/api/jobs/' + newest.id + '/preflight',
            { headers: { Authorization: 'Bearer ' + token } })).json();
        const dag = JSON.parse(detail.dagJson);
        return {
            jobId: newest.id,
            nodes: dag.nodes.map(n => ({ id: n.id, type: n.type, path: n.params && n.params.path })),
            edges: (dag.edges || []).length,
            errors: (preflight.errors || []).map(e => e.message || e)
        };
    }
  };
  'ok'
`;

(async () => {
    if (!PORT) PORT = await pickFreePort();
    const edgeLog = path.join(os.tmpdir(), 'dsh-edge-verify-' + process.pid + '.log');
    const browser = spawn(EDGE, [
        '--headless=new', `--remote-debugging-port=${PORT}`, `--user-data-dir=${USER_DATA}`,
        '--no-first-run', '--no-default-browser-check', '--disable-gpu', '--window-size=1600,1000', 'about:blank'
    ], { stdio: ['ignore', 'ignore', 'pipe'] });
    browser.stderr.pipe(fs.createWriteStream(edgeLog));

    let target = null;
    for (let i = 0; i < 60 && !target; i++) {
        await sleep(500);
        try { target = (await httpJson(PORT, '/json/list')).find(t => t.type === 'page'); } catch (_) { /* retry */ }
    }
    if (!target) {
        console.error(`FAIL: 无法连接无头浏览器（端口 ${PORT}，30s 超时）`);
        try {
            console.error(fs.readFileSync(edgeLog, 'utf8').trim().split('\n').slice(-5).map(l => '    ' + l).join('\n'));
        } catch (_) { /* ignore */ }
        browser.kill();
        process.exit(2);
    }

    const cdp = await Cdp.connect(target.webSocketDebuggerUrl);
    await cdp.send('Page.enable');
    await cdp.send('Runtime.enable');

    const results = [];
    const check = (name, ok, detail) => {
        results.push({ name, ok });
        console.log(`  ${ok ? '✅ PASS' : '❌ FAIL'}  ${name}${detail ? ' — ' + detail : ''}`);
    };

    // ---- 登录并进入画布视图 ----
    await cdp.send('Page.navigate', { url: APP });
    await sleep(2500);
    const role = await cdp.eval(`(async () => {
        const r = await fetch('/api/auth/login', { method:'POST', headers:{'Content-Type':'application/json'},
            body: JSON.stringify({ username: ${JSON.stringify(USER)}, password: ${JSON.stringify(PASSWORD)} }) });
        if (!r.ok) return 'HTTP ' + r.status;
        const d = await r.json();
        localStorage.setItem('token', d.token);
        return d.user.role;
    })()`);
    if (role !== 'ADMIN' && role !== 'OPERATOR') { console.error('FAIL: 登录失败 ' + role); browser.kill(); process.exit(2); }
    console.log('  登录角色:', role);

    await cdp.send('Page.navigate', { url: APP });
    await sleep(3000);
    await cdp.eval(PAGE_HELPERS);   // 每次导航后都要重新注入
    await cdp.eval(`(() => { const b=[...document.querySelectorAll('button')].filter(x=>x.offsetParent!==null)
        .find(x=>(x.textContent||'').includes('画布设计')); if (b) b.click(); return 'ok'; })()`);
    await sleep(1500);
    await cdp.eval(`(() => { const b = window.__verify.visibleButton('清空'); if (b) b.click(); return 'ok'; })()`);
    await sleep(800);

    // ---- A. 一次拖拽 = 一个控件 ----
    const idsA = await cdp.eval(`(async () => {
        await window.__verify.drop('csv_input', 0);
        return new Promise(r => setTimeout(() => r(window.__verify.nodeIds()), 700));
    })()`);
    check('一次拖拽只产生一个控件', idsA.length === 1, `节点数=${idsA.length} ids=${JSON.stringify(idsA)}`);

    // ---- B. 拖入节点自带默认参数 + 连线 + 保存 + 提交前检查 ----
    await cdp.eval(`(() => { const b = window.__verify.visibleButton('清空'); if (b) b.click(); return 'ok'; })()`);
    await sleep(700);
    const ids = await cdp.eval(`(async () => {
        await window.__verify.drop('csv_input', 0);
        await new Promise(r => setTimeout(r, 500));
        await window.__verify.drop('csv_output', 1);
        await new Promise(r => setTimeout(r, 700));
        return window.__verify.nodeIds();
    })()`);
    const ports = await cdp.eval(`window.__verify.portAndTarget(${JSON.stringify(ids[0])}, ${JSON.stringify(ids[1])})`);
    await cdp.dragMouse(ports.from, ports.to);   // 原生指针拖拽连线
    await sleep(700);

    const afterConnect = await cdp.eval(`({ edges: window.__verify.edgeCount() })`);
    check('原生拖拽可从端口连出一条边', afterConnect.edges >= 1, `边数=${afterConnect.edges}`);

    await cdp.eval(`(() => { const b = window.__verify.visibleButton('保存'); if (b) b.click(); return 'ok'; })()`);
    await sleep(2200);
    const saved = await cdp.eval(`window.__verify.latestJob()`);
    const csvIn = saved.nodes.find(n => n.type === 'csv_input');
    const csvOut = saved.nodes.find(n => n.type === 'csv_output');
    check('落库的 CSV 输入节点带默认 path（不再报缺少必填参数）',
        !!(csvIn && csvIn.path && String(csvIn.path).trim()), `path=${JSON.stringify(csvIn && csvIn.path)}`);
    check('落库的 CSV 输出节点带默认 path',
        !!(csvOut && csvOut.path && String(csvOut.path).trim()), `path=${JSON.stringify(csvOut && csvOut.path)}`);
    check('提交前检查无错误（preflight errors 为空）', saved.errors.length === 0,
        `job=${saved.jobId} errors=${JSON.stringify(saved.errors)}`);

    // ---- C. 参数面板改动直接保存即生效（不用先点「应用参数」）----
    const center = await cdp.eval(`window.__verify.nodeCenter(${JSON.stringify(ids[0])})`);
    await cdp.click(center.x, center.y);           // 选中节点，右侧面板出现
    await sleep(600);
    const marker = 'output/verify_panel_edit.csv';
    const panelEdit = await cdp.eval(`(async () => {
        const before = ${JSON.stringify(csvIn && csvIn.path)};
        const ok = window.__verify.setVisibleInputByValue(before, ${JSON.stringify(marker)});
        if (!ok) return { ok: false };
        const b = window.__verify.visibleButton('保存');
        if (b) b.click();
        await new Promise(r => setTimeout(r, 2200));
        const job = await window.__verify.latestJob();
        const node = job.nodes.find(n => n.type === 'csv_input');
        return { ok: true, savedPath: node && node.path };
    })()`);
    check('面板里改参数后直接保存即落库（无需先点应用参数）',
        panelEdit.ok && panelEdit.savedPath === marker,
        panelEdit.ok ? `savedPath=${JSON.stringify(panelEdit.savedPath)}` : '未找到参数输入框（跳过）');

    // ---- 清理：删除本次验证创建的作业 ----
    const cleaned = await cdp.eval(`(async () => {
        const token = localStorage.getItem('token');
        const r = await fetch('/api/jobs/${saved.jobId}', { method:'DELETE', headers:{ Authorization:'Bearer ' + token } });
        return r.status;
    })()`);
    console.log(`  已清理验证用作业 job=${saved.jobId}（HTTP ${cleaned}）`);

    const failed = results.filter(r => !r.ok).length;
    console.log(`\n  结果：${results.length - failed}/${results.length} 通过`);

    try { cdp.ws.close(); } catch (_) { /* ignore */ }
    browser.kill();
    await sleep(500);
    process.exit(failed === 0 ? 0 : 1);
})().catch(e => { console.error('ERROR:', e.message); process.exit(3); });
