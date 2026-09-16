/**
 * 无头浏览器验证：拖拽一次控件，画布上应只出现一个节点。
 *
 * 为什么需要它：这是纯前端交互缺陷（事件被重复绑定），静态检查只能守住"代码形状"，
 * 真正能抓住"拖一次落两个"的只有跑一遍真实浏览器。2026-09-16 的缺陷就是用它复现的
 * （停用去重逻辑后该脚本报 FAIL / nodes=2，id 相差 4ms）。
 *
 * 前置：
 *   1) 后端 18080 + 前端 3000 已启动（scripts\start-backend.ps1 + node frontend/serve.js）
 *   2) 演示账号可用（默认 admin/admin123，可通过环境变量覆盖）
 *   3) 存在 ws 模块：优先用 node 解析到的 ws；否则用 DSH_WS_NODE_MODULES 指定其所在 node_modules
 *   4) 有 Edge（默认取 Program Files (x86)，可用 EDGE_PATH 覆盖）
 *
 * 用法：
 *   node scripts/verify-drag-single-node.cjs
 *   set EDGE_PATH=... & set VERIFY_USER=admin & set VERIFY_PASSWORD=admin123 & node scripts/verify-drag-single-node.cjs
 *
 * 退出码：0 = 单次拖拽只产生一个控件；1 = 出现多个（缺陷复现）；2/3 = 环境问题
 */
const { spawn } = require('child_process');
const http = require('http');
const path = require('path');
const os = require('os');

const EDGE = process.env.EDGE_PATH
    || 'C:\\Program Files (x86)\\Microsoft\\Edge\\Application\\msedge.exe';
const PORT = parseInt(process.env.CDP_PORT || '9333', 10);
const APP = process.env.VERIFY_APP || 'http://127.0.0.1:3000';
const USER = process.env.VERIFY_USER || 'admin';
const PASSWORD = process.env.VERIFY_PASSWORD || 'admin123';
const USER_DATA = path.join(os.tmpdir(), 'dsh-edge-verify');

function loadWs() {
    try { return require('ws'); } catch (_) { /* fallthrough */ }
    const extra = process.env.DSH_WS_NODE_MODULES;
    if (extra) return require(path.join(extra, 'ws'));
    console.error('ERROR: 找不到 ws 模块。请在装 ws 的环境运行，或用 DSH_WS_NODE_MODULES=<含 ws 的 node_modules> 指定。');
    process.exit(2);
}
const WebSocket = loadWs();

const sleep = (ms) => new Promise(r => setTimeout(r, ms));

function httpJson(pathname) {
    return new Promise((resolve, reject) => {
        http.get({ host: '127.0.0.1', port: PORT, path: pathname }, res => {
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
}

(async () => {
    const browser = spawn(EDGE, [
        '--headless=new', `--remote-debugging-port=${PORT}`, `--user-data-dir=${USER_DATA}`,
        '--no-first-run', '--no-default-browser-check', '--disable-gpu',
        '--window-size=1600,1000', 'about:blank'
    ], { stdio: 'ignore' });

    let target = null;
    for (let i = 0; i < 30 && !target; i++) {
        await sleep(500);
        try {
            const list = await httpJson('/json/list');
            target = (list || []).find(t => t.type === 'page');
        } catch (_) { /* 浏览器还没起来，重试 */ }
    }
    if (!target) { console.error('FAIL: 无法连接无头浏览器'); browser.kill(); process.exit(2); }

    const cdp = await Cdp.connect(target.webSocketDebuggerUrl);
    await cdp.send('Page.enable');
    await cdp.send('Runtime.enable');

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
    console.log('  登录结果:', role);
    if (role === 'HTTP 401' || role === 'HTTP 400') {
        console.error('FAIL: 登录失败（检查 VERIFY_USER / VERIFY_PASSWORD）');
        browser.kill(); process.exit(2);
    }

    // 重新加载，走完整的登录态初始化路径（onMounted 里会初始化画布）
    await cdp.send('Page.navigate', { url: APP });
    await sleep(3000);

    // 切到画布视图：这一步在修复前会再次绑定 drop 监听，是"拖一次落两个"的触发条件
    const switched = await cdp.eval(`(() => {
        const btn = [...document.querySelectorAll('button')].find(b => (b.textContent||'').includes('画布设计'));
        if (!btn) return 'no-button';
        btn.click();
        return 'clicked';
    })()`);
    console.log('  切到画布视图:', switched);
    await sleep(1500);

    const canvas = await cdp.eval(`(() => {
        const el = document.getElementById('dag-canvas');
        return el ? el.clientWidth + 'x' + el.clientHeight : 'no-canvas';
    })()`);
    console.log('  画布尺寸:', canvas);

    const result = await cdp.eval(`(() => {
        const el = document.getElementById('dag-canvas');
        const dt = new DataTransfer();
        dt.setData('text/plain', JSON.stringify({ type:'csv_input', name:'CSV 输入', category:'input',
            paramSchema: { type:'object', properties:{} }, flinkTemplate: '' }));
        const rect = el.getBoundingClientRect();
        const opts = { bubbles:true, cancelable:true, clientX: rect.left + 300, clientY: rect.top + 200, dataTransfer: dt };
        el.dispatchEvent(new DragEvent('dragover', opts));
        el.dispatchEvent(new DragEvent('drop', opts));
        return new Promise(res => setTimeout(() => res({
            nodes: document.querySelectorAll('.x6-node').length,
            ids: [...document.querySelectorAll('.x6-node')].map(n => n.getAttribute('data-cell-id'))
        }), 600));
    })()`);

    console.log('  拖拽一次后的节点数:', result.nodes);
    console.log('  节点 id:', JSON.stringify(result.ids));
    const ok = result.nodes === 1;
    console.log(ok ? '  ✅ PASS：一次拖拽只产生一个控件'
                   : `  ❌ FAIL：一次拖拽产生了 ${result.nodes} 个控件`);

    try { cdp.ws.close(); } catch (_) { /* ignore */ }
    browser.kill();
    await sleep(500);
    process.exit(ok ? 0 : 1);
})().catch(e => { console.error('ERROR:', e.message); process.exit(3); });
