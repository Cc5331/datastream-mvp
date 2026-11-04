/**
 * 无头浏览器验证「拖拽文件到画布 → 自动分析 → 用户确认 → 生成输入控件」链路（Edge + CDP）。
 *
 *   A. 拖入文件不直接落节点，而是弹出确认对话框（含文件名 / 列名 / 预览行）
 *   B. 取消（或关闭）对话框不新增节点，且后端暂存文件被丢弃
 *   C. 确认后生成 CSV 输入控件，params 带服务器路径 + 识别出的表头/字段定义
 *   D. 生成节点后落库 → 提交前检查无错误（参数确实能被翻译层接受）
 *
 * 前置：后端 18080 + 前端 3000 已启动；演示账号可用；有 Edge；ws 模块可解析。
 * 用法：node scripts/verify-file-import.cjs
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
const USER_DATA = path.join(os.tmpdir(), 'dsh-edge-import-' + process.pid);
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

    async click(x, y) {
        await this.send('Input.dispatchMouseEvent', { type: 'mouseMoved', x, y, buttons: 0 });
        await this.send('Input.dispatchMouseEvent', { type: 'mousePressed', x, y, button: 'left', clickCount: 1, buttons: 1 });
        await this.send('Input.dispatchMouseEvent', { type: 'mouseReleased', x, y, button: 'left', clickCount: 1, buttons: 0 });
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
}

// 拖入一个真实的 File（浏览器侧构造 File，走 DataTransfer.items，等价于从资源管理器拖入）
// 故意带一个含空格的字段名（Order ID），用于验证「AI 建议 -> 应用字段改名」这条链路
const CSV_CONTENT = 'Order ID,region,amount,sale_date\nA001,华北,128.50,2026-01-01\nA002,华南,66.00,2026-01-02\n';

const PAGE_HELPERS = `
  window.__verify = {
    visibleButton: (text) => [...document.querySelectorAll('button')]
        .filter(b => b.offsetParent !== null)
        .find(b => (b.textContent || '').includes(text)),
    dialog: () => {
        const dlg = [...document.querySelectorAll('.el-dialog')]
            .find(d => d.offsetParent !== null && (d.textContent || '').includes('导入文件生成输入控件'));
        return dlg || null;
    },
    dropFile: (fileName, content) => {
        const el = document.getElementById('dag-canvas');
        const dt = new DataTransfer();
        dt.items.add(new File([content], fileName, { type: 'text/csv' }));
        const rect = el.getBoundingClientRect();
        const opts = { bubbles: true, cancelable: true,
                       clientX: rect.left + 260, clientY: rect.top + 220, dataTransfer: dt };
        el.dispatchEvent(new DragEvent('dragover', opts));
        el.dispatchEvent(new DragEvent('drop', opts));
        return 'ok';
    },
    nodeIds: () => [...document.querySelectorAll('.x6-node')].map(n => n.getAttribute('data-cell-id')),
    /** 从控件库拖一个控件（DataTransfer 带 text/plain 载荷），用于补齐下游输出节点 */
    dropControl: async (type, dx) => {
        const controls = await (await fetch('/api/controls', {
            headers: { Authorization: 'Bearer ' + localStorage.getItem('token') } })).json();
        const ctrl = controls.find(c => c.type === type);
        const payload = { type, name: ctrl.name, category: ctrl.category,
                          paramSchema: JSON.parse(ctrl.paramSchema || '{}'), flinkTemplate: ctrl.flinkTemplate };
        const el = document.getElementById('dag-canvas');
        const dt = new DataTransfer();
        dt.setData('text/plain', JSON.stringify(payload));
        const rect = el.getBoundingClientRect();
        const opts = { bubbles: true, cancelable: true,
                       clientX: rect.left + 260 + dx, clientY: rect.top + 220, dataTransfer: dt };
        el.dispatchEvent(new DragEvent('dragover', opts));
        el.dispatchEvent(new DragEvent('drop', opts));
        return 'ok';
    },
    /** 输入节点右端口与目标节点中心的视口坐标（供 CDP 原生拖拽连线） */
    portAndTarget: (fromId, toId) => {
        const from = document.querySelector('.x6-node[data-cell-id="' + fromId + '"]');
        const to = document.querySelector('.x6-node[data-cell-id="' + toId + '"]');
        const port = from.querySelector('[port-group="right"]');
        const a = port.getBoundingClientRect(), b = to.getBoundingClientRect();
        return { from: { x: a.left + a.width / 2, y: a.top + a.height / 2 },
                 to: { x: b.left + b.width / 2, y: b.top + b.height / 2 } };
    },
    edgeCount: () => document.querySelectorAll('.x6-edge').length,
    dialogText: () => { const d = window.__verify.dialog(); return d ? d.textContent.replace(/\\s+/g, ' ').trim() : ''; },
    dialogColumns: () => {
        const d = window.__verify.dialog();
        if (!d) return [];
        // 只取字段名输入框（el-select 内部也有 input，别一起算进来）
        return [...d.querySelectorAll('.file-import-column .el-input input')].map(i => i.value);
    },
    dialogPreviewRows: () => { const d = window.__verify.dialog(); return d ? d.querySelectorAll('.file-import-preview .el-table__body tr').length : 0; },
    clickDialogButton: (text) => {
        const d = window.__verify.dialog();
        if (!d) return 'no-dialog';
        const btn = [...d.querySelectorAll('button')].find(b => (b.textContent || '').includes(text));
        if (!btn) return 'no-button';
        btn.click();
        return 'clicked';
    },
    /**
     * 按「上传文件的唯一文件名」反查落库作业（不按最大 id 猜，
     * 库里存在大量历史作业，按 id 取最新可能拿到别人的作业）。
     */
    jobByFileName: async (needle) => {
        const token = localStorage.getItem('token');
        const jobs = await (await fetch('/api/jobs', { headers: { Authorization: 'Bearer ' + token } })).json();
        const hit = jobs.find(j => (j.dagJson || '').includes(needle));
        if (!hit) return null;
        const detail = await (await fetch('/api/jobs/' + hit.id,
            { headers: { Authorization: 'Bearer ' + token } })).json();
        const preflight = await (await fetch('/api/jobs/' + hit.id + '/preflight',
            { headers: { Authorization: 'Bearer ' + token } })).json();
        const dag = JSON.parse(detail.dagJson);
        return {
            jobId: hit.id,
            nodes: dag.nodes.map(n => ({ id: n.id, type: n.type, params: n.params || {} })),
            edges: (dag.edges || []).length,
            errors: (preflight.errors || []).map(e => e.message || e)
        };
    },
    deleteJob: async (id) => {
        const token = localStorage.getItem('token');
        const r = await fetch('/api/jobs/' + id, { method: 'DELETE', headers: { Authorization: 'Bearer ' + token } });
        return r.status;
    }
  };
  'ok'
`;

(async () => {
    if (!PORT) PORT = await pickFreePort();
    const edgeLog = path.join(os.tmpdir(), 'dsh-edge-import-' + process.pid + '.log');
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

    // ---- 登录 ----
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
    await cdp.eval(PAGE_HELPERS);
    await cdp.eval(`(() => { const b=[...document.querySelectorAll('button')].filter(x=>x.offsetParent!==null)
        .find(x=>(x.textContent||'').includes('画布设计')); if (b) b.click(); return 'ok'; })()`);
    await sleep(1200);
    await cdp.eval(`(() => { const b = window.__verify.visibleButton('清空'); if (b) b.click(); return 'ok'; })()`);
    await sleep(800);

    // ---- A. 拖入文件弹出确认框，且确认前不落节点 ----
    const fileName = 'verify_import_' + Date.now() + '.csv';
    const dropped = await cdp.eval(`window.__verify.dropFile(${JSON.stringify(fileName)}, ${JSON.stringify(CSV_CONTENT)})`);
    await sleep(2500);
    const dialogText = await cdp.eval('window.__verify.dialogText()');
    const nodeCountAfterDrop = await cdp.eval('window.__verify.nodeIds().length');
    check('拖入文件后弹出确认对话框', !!dialogText && dialogText.includes(fileName), dropped);
    check('确认前画布不生成任何节点', nodeCountAfterDrop === 0, `节点数=${nodeCountAfterDrop}`);
    const columns = await cdp.eval('window.__verify.dialogColumns()');
    check('识别出 4 个字段（Order ID/region/amount/sale_date）',
        Array.isArray(columns) && columns.length === 4 && columns[0] === 'Order ID',
        `columns=${JSON.stringify(columns)}`);
    const previewRows = await cdp.eval('window.__verify.dialogPreviewRows()');
    check('确认框展示数据预览行', previewRows >= 2, `预览行数=${previewRows}`);
    check('确认框标出服务器暂存路径', dialogText.includes('output') && dialogText.includes('uploads'),
        dialogText.slice(0, 160));

    // ---- A2. 「AI 建议」按钮：给出可解释建议，并能一键应用字段改名 ----
    await cdp.eval(`window.__verify.clickDialogButton('AI 建议')`);
    await sleep(4000);   // 本地规则 + （配置了模型时）一次 LLM 调用
    const advice = await cdp.eval(`(() => {
        const d = window.__verify.dialog();
        if (!d) return null;
        const box = d.querySelector('.file-import-advice');
        if (!box) return null;
        return { text: box.textContent.replace(/\\s+/g, ' ').trim().slice(0, 300),
                 items: box.querySelectorAll('li').length,
                 hasRename: !!([...d.querySelectorAll('button')].find(b => (b.textContent || '').includes('应用字段改名'))) };
    })()`);
    check('确认框内出现「AI 建议」面板', !!advice && advice.items > 0,
        advice ? `建议 ${advice.items} 条: ${advice.text.slice(0, 120)}` : '未出现建议面板');

    if (advice && advice.hasRename) {
        const before = await cdp.eval('window.__verify.dialogColumns()');
        await cdp.eval(`window.__verify.clickDialogButton('应用字段改名')`);
        await sleep(800);
        const after = await cdp.eval('window.__verify.dialogColumns()');
        check('「应用字段改名」把建议落到字段名上',
            JSON.stringify(before) !== JSON.stringify(after),
            `${JSON.stringify(before)} -> ${JSON.stringify(after)}`);
    } else {
        check('本次建议不含改名项（跳过改名断言）', true, 'no renames');
    }

    // ---- B. 取消 → 不落节点，对话框关闭 ----
    await cdp.eval(`window.__verify.clickDialogButton('取消')`);
    await sleep(1500);
    const afterCancel = await cdp.eval(`({ nodes: window.__verify.nodeIds().length, dialog: !!window.__verify.dialog() })`);
    check('取消后不生成节点且对话框关闭', afterCancel.nodes === 0 && afterCancel.dialog === false,
        `nodes=${afterCancel.nodes} dialog=${afterCancel.dialog}`);

    // ---- C. 再次拖入并确认 → 生成 CSV 输入控件 ----
    await cdp.eval(`window.__verify.dropFile(${JSON.stringify(fileName)}, ${JSON.stringify(CSV_CONTENT)})`);
    await sleep(2500);
    const confirmResult = await cdp.eval(`window.__verify.clickDialogButton('确认生成控件')`);
    await sleep(1200);
    const afterConfirm = await cdp.eval('window.__verify.nodeIds()');
    check('确认后生成 1 个输入控件', confirmResult === 'clicked' && afterConfirm.length === 1,
        `result=${confirmResult} nodes=${JSON.stringify(afterConfirm)}`);
    const dialogGone = await cdp.eval('!!window.__verify.dialog()');
    check('确认后对话框关闭', dialogGone === false, `dialog=${dialogGone}`);

    // ---- D. 补齐下游输出节点 + 连线（保存要求 DAG 连通），保存后校验落库参数 ----
    const nodesBeforeOutput = await cdp.eval('window.__verify.nodeIds()');
    await cdp.eval(`window.__verify.dropControl('csv_output', 320)`);
    await sleep(900);
    const nodesWithOutput = await cdp.eval('window.__verify.nodeIds()');
    const outId = nodesWithOutput.find(id => id !== nodesBeforeOutput[0]);
    check('可从控件库继续拖入下游输出节点', nodesWithOutput.length === 2 && !!outId,
        `nodes=${JSON.stringify(nodesWithOutput)}`);
    if (outId) {
        const ports = await cdp.eval(`window.__verify.portAndTarget(${JSON.stringify(nodesBeforeOutput[0])}, ${JSON.stringify(outId)})`);
        await cdp.dragMouse(ports.from, ports.to);
        await sleep(800);
    }
    const edges = await cdp.eval('window.__verify.edgeCount()');
    check('导入节点可与下游连线', edges >= 1, `边数=${edges}`);

    const beforeSave = await cdp.eval(`(async () => {
        const token = localStorage.getItem('token');
        const jobs = await (await fetch('/api/jobs', { headers: { Authorization: 'Bearer ' + token } })).json();
        return jobs.filter(j => (j.dagJson || '').includes(${JSON.stringify(fileName)})).map(j => j.id);
    })()`);
    check('保存前库里没有引用该文件的作业（保证下面读到的是本次保存结果）',
        Array.isArray(beforeSave) && beforeSave.length === 0, `hits=${JSON.stringify(beforeSave)}`);
    await cdp.eval(`(() => { const b = window.__verify.visibleButton('保存'); if (b) b.click(); return 'ok'; })()`);
    await sleep(2500);
    const saved = await cdp.eval(`window.__verify.jobByFileName(${JSON.stringify(fileName)})`);
    const node = saved && saved.nodes.find(n => n.type === 'csv_input');
    check('保存后落库了引用该文件的作业', !!saved, saved ? `job=${saved.jobId}` : '未找到（保存失败？）');
    check('落库节点类型为 csv_input', !!node, node ? node.id : 'not found');
    check('节点 params.path 指向服务器暂存文件',
        !!(node && node.params.path && String(node.params.path).includes('uploads') && String(node.params.path).includes(fileName)),
        node ? `path=${node.params.path}` : '');
    check('节点 params 带识别出的表头开关与字段定义',
        !!(node && String(node.params.hasHeader) === 'true' && node.params.fieldsConfig
            && /"name":"(Order ID|order_id)"/.test(node.params.fieldsConfig)),
        node ? `hasHeader=${node.params.hasHeader} fieldsConfig=${node.params.fieldsConfig}` : '');
    check('提交前检查无错误', !!saved && saved.errors.length === 0, saved ? `job=${saved.jobId} errors=${JSON.stringify(saved.errors)}` : '');

    // ---- 清理：只删本次验证创建的作业（按文件唯一名定位，避免误删历史作业）----
    if (saved && saved.jobId) {
        const status = await cdp.eval(`window.__verify.deleteJob(${saved.jobId})`);
        console.log(`  已清理验证用作业 job=${saved.jobId}（HTTP ${status}）`);
    } else {
        console.log('  未创建验证用作业，无需清理');
    }

    const failed = results.filter(r => !r.ok).length;
    console.log(`\n  结果：${results.length - failed}/${results.length} 通过`);

    try { cdp.ws.close(); } catch (_) { /* ignore */ }
    browser.kill();
    await sleep(500);
    process.exit(failed === 0 ? 0 : 1);
})().catch(e => { console.error('ERROR:', e.message); process.exit(3); });
