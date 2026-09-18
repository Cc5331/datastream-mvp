/**
 * 截图核对：用户管理页「角色筛选下拉 + 批量操作栏」布局。
 * 用法：node scripts/shot-users-page.cjs [输出 png]
 */
const { spawn } = require('child_process');
const http = require('http');
const net = require('net');
const fs = require('fs');
const path = require('path');
const os = require('os');
const WebSocket = require(process.env.DSH_WS_NODE_MODULES
    ? path.join(process.env.DSH_WS_NODE_MODULES, 'ws') : 'ws');

const EDGE = process.env.EDGE_PATH || 'C:\\Program Files (x86)\\Microsoft\\Edge\\Application\\msedge.exe';
const APP = process.env.VERIFY_APP || 'http://127.0.0.1:3000';
const API = process.env.VERIFY_API || 'http://127.0.0.1:18080';
const OUT = process.argv[2] || path.join(__dirname, '..', 'output', '_video_work', 'users-page.png');
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
        const ws = new WebSocket(url, { perMessageDeflate: false, maxPayload: 64 * 1024 * 1024 });
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

(async () => {
    const PORT = await pickFreePort();
    const browser = spawn(EDGE, ['--headless=new', `--remote-debugging-port=${PORT}`,
        `--user-data-dir=${path.join(os.tmpdir(), 'shot-' + process.pid)}`, '--no-first-run',
        '--disable-gpu', '--window-size=1600,1000', 'about:blank'], { stdio: ['ignore', 'ignore', 'pipe'] });
    browser.stderr.pipe(fs.createWriteStream(path.join(os.tmpdir(), 'shot-edge.log')));

    let target = null;
    for (let i = 0; i < 60 && !target; i++) {
        await sleep(500);
        try { target = (await httpJson(PORT, '/json/list')).find(t => t.type === 'page'); } catch (_) {}
    }
    if (!target) { console.error('浏览器连接失败'); browser.kill(); process.exit(2); }
    const cdp = await Cdp.connect(target.webSocketDebuggerUrl);
    await cdp.send('Page.enable');
    await cdp.send('Runtime.enable');
    await cdp.send('Emulation.setDeviceMetricsOverride', { width: 1560, height: 900, deviceScaleFactor: 1, mobile: false });

    await cdp.send('Page.navigate', { url: APP });
    await sleep(2500);
    await cdp.eval(`(async () => {
        const r = await fetch('${API}/api/auth/login', { method:'POST', headers:{'Content-Type':'application/json'},
            body: JSON.stringify({ username:'admin', password:'admin123' }) });
        const d = await r.json();
        localStorage.setItem('token', d.token);
        return d.user.role;
    })()`);
    await cdp.send('Page.navigate', { url: APP });
    await sleep(3000);

    // 打开「管理 → 用户管理」
    await cdp.eval(`(() => {
        const t = [...document.querySelectorAll('button')].filter(b => b.offsetParent !== null)
            .find(b => (b.textContent || '').trim().startsWith('管理'));
        t.click();
        const item = [...document.querySelectorAll('.el-dropdown-menu__item')]
            .find(i => (i.textContent || '').includes('用户管理'));
        item.click();
        return 'ok';
    })()`);
    await sleep(2000);
    // 勾选两个账号，展示批量栏可用态
    await cdp.eval(`(() => {
        const boxes = [...document.querySelectorAll('.users-view .el-table__body-wrapper tbody tr .el-checkbox')]
            .filter(b => !b.classList.contains('is-disabled'));
        boxes.slice(0, 2).forEach(b => b.click());
        return boxes.length;
    })()`);
    await sleep(500);
    // 展开角色筛选下拉
    await cdp.eval(`(() => {
        const sel = [...document.querySelectorAll('.users-view .el-select')].filter(e => e.offsetParent !== null)[0];
        sel.querySelector('input')?.click();
        sel.click();
        return 'ok';
    })()`);
    await sleep(900);

    const shot = await cdp.send('Page.captureScreenshot', { format: 'png' });
    fs.mkdirSync(path.dirname(OUT), { recursive: true });
    fs.writeFileSync(OUT, Buffer.from(shot.data, 'base64'));
    const info = await cdp.eval(`(() => {
        const opts = [...document.querySelectorAll('.el-select-dropdown__item')].filter(e => e.offsetParent !== null)
            .map(e => (e.textContent || '').trim());
        const bar = document.querySelector('.users-batch-bar');
        const btns = bar ? [...bar.querySelectorAll('button')].map(b => (b.textContent || '').trim() + (b.disabled ? '(禁用)' : '')) : [];
        return { roleOptions: opts, batchButtons: btns, barVisible: !!bar && bar.offsetParent !== null };
    })()`);
    console.log('  角色下拉选项:', JSON.stringify(info.roleOptions));
    console.log('  批量栏可见:', info.barVisible, ' 按钮:', JSON.stringify(info.batchButtons));
    console.log('  截图:', OUT, Math.round(fs.statSync(OUT).size / 1024) + ' KB');

    try { cdp.ws.close(); } catch (_) {}
    browser.kill();
    process.exit(0);
})().catch(e => { console.error('ERROR:', e.message); process.exit(3); });
