/**
 * 通用页面截图/核对脚本（Edge + CDP）：打开顶栏某个菜单项对应的视图并截图。
 *
 * 用法：
 *   node scripts/shot-page.cjs "控件管理" [输出.png]
 *   node scripts/shot-page.cjs "用户管理" output/_video_work/users-page.png
 *   默认：菜单项「用户管理」，输出 output/_video_work/page.png
 *
 * 会打印：视图标题、表格行数、被断言元素存在性，便于无人值守核对布局。
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
const MENU = process.argv[2] || '用户管理';
const ROOT = path.resolve(__dirname, '..');
const OUT = process.argv[3] || path.join(ROOT, 'output', '_video_work', 'page.png');
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
    const role = await cdp.eval(`(async () => {
        const r = await fetch('${API}/api/auth/login', { method:'POST', headers:{'Content-Type':'application/json'},
            body: JSON.stringify({ username:'admin', password:'admin123' }) });
        if (!r.ok) return 'HTTP ' + r.status;
        const d = await r.json();
        localStorage.setItem('token', d.token);
        return d.user.role;
    })()`);
    if (role !== 'ADMIN') { console.error('登录失败: ' + role); browser.kill(); process.exit(2); }
    await cdp.send('Page.navigate', { url: APP });
    await sleep(3000);

    // 依次尝试每个顶栏下拉，点到包含目标菜单项的那个
    const opened = await cdp.eval(`(() => {
        const triggers = [...document.querySelectorAll('.el-dropdown')].filter(d => d.offsetParent !== null);
        for (const t of triggers) {
            const btn = t.querySelector('button');
            if (!btn) continue;
            btn.click();
            const item = [...document.querySelectorAll('.el-dropdown-menu__item')]
                .find(i => (i.textContent || '').replace(/\\s+/g, '').includes(${JSON.stringify(MENU)}));
            if (item) { item.click(); return 'clicked:' + (btn.textContent || '').trim(); }
        }
        return 'not-found';
    })()`);
    await sleep(2200);

    const info = await cdp.eval(`(() => {
        const view = [...document.querySelectorAll('.portal-view, .controls-view, .jobs-view, .monitor-view')]
            .find(v => v.offsetParent !== null);
        const heading = view ? (view.querySelector('h2')?.textContent || '').trim() : '';
        const rows = view ? view.querySelectorAll('.el-table__body-wrapper tbody tr').length : 0;
        const hint = view ? (view.querySelector('.controls-hint')?.textContent || '').replace(/\\s+/g, ' ').trim().slice(0, 120) : '';
        const stats = view ? (view.querySelector('.page-heading p')?.textContent || '').replace(/\\s+/g, ' ').trim() : '';
        return { viewClass: view ? view.className : 'none', heading, rows, stats, hint };
    })()`);

    const shot = await cdp.send('Page.captureScreenshot', { format: 'png' });
    fs.mkdirSync(path.dirname(OUT), { recursive: true });
    fs.writeFileSync(OUT, Buffer.from(shot.data, 'base64'));

    console.log('  菜单打开:', opened);
    console.log('  视图:', info.viewClass, '| 标题:', info.heading);
    console.log('  统计行:', info.stats);
    console.log('  表格行数:', info.rows);
    if (info.hint) console.log('  提示:', info.hint + '…');
    console.log('  截图:', OUT, Math.round(fs.statSync(OUT).size / 1024) + ' KB');

    try { cdp.ws.close(); } catch (_) {}
    browser.kill();
    process.exit(opened.startsWith('clicked') ? 0 : 1);
})().catch(e => { console.error('ERROR:', e.message); process.exit(3); });
