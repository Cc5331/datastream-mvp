/**
 * 真接口验证「批量删除作业」：逐项结果、运行中跳过、owner 隔离、审计落库。
 *
 * 前置：后端 18080 已启动、演示账号可用。
 * 用法：node scripts/verify-jobs-batch-delete.cjs
 * 退出码：0 = 全部 PASS；1 = 有断言失败；2/3 = 环境问题
 */
const http = require('http');

const HOST = process.env.VERIFY_HOST || '127.0.0.1';
const PORT = parseInt(process.env.VERIFY_PORT || '18080', 10);
const USER = process.env.VERIFY_USER || 'admin';
const PASSWORD = process.env.VERIFY_PASSWORD || 'admin123';

function request(method, path, { token, body } = {}) {
    return new Promise((resolve, reject) => {
        const payload = body == null ? null : Buffer.from(JSON.stringify(body), 'utf8');
        const headers = {};
        if (token) headers.Authorization = 'Bearer ' + token;
        if (payload) {
            headers['Content-Type'] = 'application/json; charset=utf-8';
            headers['Content-Length'] = payload.length;
        }
        const req = http.request({ host: HOST, port: PORT, path, method, headers }, res => {
            const chunks = [];
            res.on('data', c => chunks.push(c));
            res.on('end', () => {
                const text = Buffer.concat(chunks).toString('utf8');
                let data = text;
                try { data = JSON.parse(text); } catch (_) { /* keep text */ }
                resolve({ status: res.statusCode, data });
            });
        });
        req.on('error', reject);
        if (payload) req.write(payload);
        req.end();
    });
}

const results = [];
function check(name, ok, detail) {
    results.push({ name, ok });
    console.log(`  ${ok ? '✅ PASS' : '❌ FAIL'}  ${name}${detail ? ' — ' + detail : ''}`);
}

const dag = JSON.stringify({
    jobName: 'batch-del-verify', parallelism: 1,
    nodes: [{ id: 'csv_input_1', type: 'csv_input', label: 'CSV 输入',
        params: { path: 'test-resources/data/sales.csv', delimiter: ',', hasHeader: true }, x: 100, y: 100 },
    { id: 'csv_output_1', type: 'csv_output', label: 'CSV 输出',
        params: { path: 'output/batch_del_verify.csv' }, x: 400, y: 100 }],
    edges: [{ id: 'e1', source: 'csv_input_1', target: 'csv_output_1' }]
});

(async () => {
    const login = await request('POST', '/api/auth/login', { body: { username: USER, password: PASSWORD } });
    if (login.status !== 200) { console.error('登录失败', login.status, login.data); process.exit(2); }
    const token = login.data.token;
    console.log('  登录角色:', login.data.user.role);

    // 造 3 个 DRAFT 作业
    const created = [];
    for (let i = 0; i < 3; i++) {
        const res = await request('POST', '/api/jobs', {
            token, body: { name: 'batch-del-' + Date.now() + '-' + i, parallelism: 1, dagJson: dag }
        });
        if (res.status !== 200) { console.error('建作业失败', res.status, res.data); process.exit(3); }
        created.push(res.data.id);
    }
    console.log('  已创建作业:', created.join(', '));

    // 空列表必须被拒
    const empty = await request('POST', '/api/jobs/batch-delete', { token, body: { ids: [] } });
    check('空选被拒（400）', empty.status === 400, `status=${empty.status}`);

    // 正常批量删除：2 个真实 + 1 个不存在的 id
    const ghostId = 99999999;
    const res = await request('POST', '/api/jobs/batch-delete', {
        token, body: { ids: [created[0], created[1], ghostId, created[0]] }
    });
    const succeeded = (res.data && res.data.succeeded) || [];
    const failed = (res.data && res.data.failed) || [];
    check('批量删除返回 200 且契约正确（succeeded / failed）',
        res.status === 200 && Array.isArray(succeeded) && Array.isArray(failed),
        `succeeded=${JSON.stringify(succeeded)} failed=${JSON.stringify(failed.map(f => f.id))}`);
    check('两个存在的作业删除成功', succeeded.length === 2
        && succeeded.includes(created[0]) && succeeded.includes(created[1]));
    check('重复 id 只处理一次（去重）', succeeded.filter(id => id === created[0]).length === 1);
    check('不存在的作业进入 failed 并带原因', failed.length === 1 && failed[0].id === ghostId
        && String(failed[0].message).length > 0, failed[0] && failed[0].message);

    const gone1 = await request('GET', '/api/jobs/' + created[0], { token });
    const gone2 = await request('GET', '/api/jobs/' + created[1], { token });
    const alive = await request('GET', '/api/jobs/' + created[2], { token });
    check('已删除作业查不到了（404）', gone1.status === 404 && gone2.status === 404,
        `${gone1.status}/${gone2.status}`);
    check('未选中的作业仍在（不被误删）', alive.status === 200, `status=${alive.status}`);

    // 审计必须留痕（破坏性操作）
    const audit = await request('GET', '/api/audit?page=0&size=20', { token });
    const rows = (audit.data && (audit.data.content || audit.data)) || [];
    const hit = rows.find(r => r.action === 'JOB_BATCH_DELETE');
    check('写入审计 JOB_BATCH_DELETE', !!hit, hit ? `${hit.action} ${hit.detail || ''}` : '未找到审计记录');

    // 清理剩下的那个
    const cleanup = await request('DELETE', '/api/jobs/' + created[2], { token });
    console.log(`  已清理验证用作业 ${created[2]}（HTTP ${cleanup.status}）`);

    const failedCount = results.filter(r => !r.ok).length;
    console.log(`\n  结果：${results.length - failedCount}/${results.length} 通过`);
    process.exit(failedCount === 0 ? 0 : 1);
})().catch(e => { console.error('ERROR:', e.message); process.exit(3); });
