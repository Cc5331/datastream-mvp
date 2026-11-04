// 一次性验证脚本（真接口）：导入建议 / Kafka 字段推断 / 告警聚合。
// 用法：node scripts/verify-agent-features.cjs
const http = require('http');

const HOST = process.env.VERIFY_HOST || '127.0.0.1';
const PORT = parseInt(process.env.VERIFY_PORT || '18080', 10);
const USER = process.env.VERIFY_USER || 'admin';
const PASSWORD = process.env.VERIFY_PASSWORD || 'admin123';

function request(method, path, { token, body, contentType } = {}) {
    return new Promise((resolve, reject) => {
        const payload = body == null ? null
            : (Buffer.isBuffer(body) ? body : Buffer.from(typeof body === 'string' ? body : JSON.stringify(body), 'utf8'));
        const headers = {};
        if (token) headers.Authorization = 'Bearer ' + token;
        if (payload) headers['Content-Type'] = contentType || 'application/json; charset=utf-8';
        if (payload) headers['Content-Length'] = payload.length;
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

/** 手工拼 multipart，避免依赖第三方库 */
function multipart(fieldName, fileName, content, mime) {
    const boundary = '----verify' + Date.now();
    const head = `--${boundary}\r\nContent-Disposition: form-data; name="${fieldName}"; filename="${fileName}"\r\n`
        + `Content-Type: ${mime}\r\n\r\n`;
    const tail = `\r\n--${boundary}--\r\n`;
    return {
        boundary,
        buffer: Buffer.concat([Buffer.from(head, 'utf8'), Buffer.from(content, 'utf8'), Buffer.from(tail, 'utf8')])
    };
}

const results = [];
function check(name, ok, detail) {
    results.push({ name, ok });
    console.log(`  ${ok ? '✅ PASS' : '❌ FAIL'}  ${name}${detail ? ' — ' + detail : ''}`);
}

(async () => {
    const login = await request('POST', '/api/auth/login', { body: { username: USER, password: PASSWORD } });
    if (login.status !== 200) { console.error('登录失败', login.status, login.data); process.exit(2); }
    const token = login.data.token;
    console.log('  登录角色:', login.data.user.role);

    // ---------- ① 文件导入建议 ----------
    const csv = 'Order ID,amount,备注\n0001,12.5,\n0002,18,\n0003,7.5,\n';
    const form = multipart('file', 'verify_advice.csv', csv, 'text/csv');
    const analyzed = await request('POST', '/api/files/analyze', {
        token, body: form.buffer, contentType: 'multipart/form-data; boundary=' + form.boundary
    });
    check('上传+分析接口可用', analyzed.status === 200 && !!analyzed.data.path, `path=${analyzed.data.path}`);

    const advice = await request('POST', '/api/ai/inspect-file', {
        token, body: { path: analyzed.data.path, controlType: 'csv_input' }
    });
    const suggestions = (advice.data && advice.data.suggestions) || [];
    const kinds = suggestions.map(s => s.kind);
    check('导入建议接口返回建议', advice.status === 200 && suggestions.length > 0, `source=${advice.data.source} 条数=${suggestions.length}`);
    check('本地规则命中「编号列前导零」', kinds.includes('leading-zero'), kinds.join(','));
    check('本地规则命中「全空列」', kinds.includes('empty-column'), kinds.join(','));
    check('本地规则命中「字段名含特殊字符」', kinds.includes('name-style'), kinds.join(','));
    check('给出可应用的字段改名（Order ID -> order_id）',
        !!(advice.data.renames && advice.data.renames['Order ID'] === 'order_id'),
        JSON.stringify(advice.data.renames));
    check('中文建议内容未乱码', suggestions.some(s => String(s.title).includes('「Order ID」') || String(s.title).includes('全为空')),
        suggestions[0] && suggestions[0].title);

    await request('DELETE', '/api/files/uploaded?path=' + encodeURIComponent(analyzed.data.path), { token });

    // ---------- ①b 真实大文件的中文编码回归（2026-11-04 乱码事故）----------
    const fsp = require('fs');
    const pathMod = require('path');
    const ordersPath = pathMod.resolve(__dirname, '..', 'test-resources/data/orders.csv');
    if (fsp.existsSync(ordersPath)) {
        const ordersBytes = fsp.readFileSync(ordersPath);
        const ordersForm = multipart('file', 'orders.csv', ordersBytes, 'text/csv');
        const orders = await request('POST', '/api/files/analyze', {
            token, body: ordersForm.buffer, contentType: 'multipart/form-data; boundary=' + ordersForm.boundary
        });
        const rows = (orders.data && orders.data.rows) || [];
        const firstRow = JSON.stringify(rows[0] || []);
        check('真实 orders.csv 分析后中文正常（编码探测边界回归）',
            orders.status === 200 && orders.data.encoding === 'UTF-8'
            && !firstRow.includes('\uFFFD') && firstRow.includes('平板') && firstRow.includes('退款中'),
            `encoding=${orders.data && orders.data.encoding} row1=${firstRow}`);

        const preview = await request('GET',
            '/api/preview/file?path=' + encodeURIComponent(orders.data.path) + '&limit=3', { token });
        const previewText = JSON.stringify(preview.data.rows || []);
        check('数据预览接口对同一文件也不乱码',
            preview.status === 200 && !previewText.includes('\uFFFD') && previewText.includes('平板'),
            `encoding=${preview.data && preview.data.encoding}`);

        await request('DELETE', '/api/files/uploaded?path=' + encodeURIComponent(orders.data.path), { token });
    } else {
        check('真实 orders.csv 中文编码回归（数据文件缺失，跳过）', true, 'skipped');
    }

    // ---------- ② Kafka 字段推断 ----------
    const topics = await request('GET', '/api/kafka/topics', { token });
    const brokerUp = topics.status === 200 && Array.isArray(topics.data) && topics.data.length > 0;
    console.log(`  Kafka broker ${brokerUp ? '可用' : '不可用（只验证错误分支）'}`);
    const infer = await request('POST', '/api/kafka/infer-schema', {
        token, body: { topic: brokerUp ? topics.data[0].name : 'verify-not-exist-topic' }
    });
    if (brokerUp) {
        const columns = infer.data.columns || [];
        check('Kafka 推断：返回字段与 fieldsConfig',
            infer.status === 200 && columns.length > 0 && String(infer.data.fieldsConfig).startsWith('['),
            `topic=${infer.data.topic} 样本=${infer.data.sampled} 字段=${columns.map(c => c.name + ':' + c.type).join(',')}`);
    } else {
        check('Kafka 不可用时给出可读错误（不是 500）',
            infer.status === 400 && String(infer.data.error || '').includes('无法从 topic'),
            `status=${infer.status} error=${infer.data.error}`);
    }
    const emptyTopic = await request('POST', '/api/kafka/infer-schema', { token, body: { topic: ' ' } });
    check('空 topic 被拒（400）', emptyTopic.status === 400, `status=${emptyTopic.status}`);

    // ---------- ③ 告警聚合降噪 ----------
    const dag = (path) => JSON.stringify({
        jobName: 'agg-verify', parallelism: 1,
        nodes: [{ id: 'csv_input_1', type: 'csv_input', label: 'CSV 输入',
            params: { path, delimiter: ',', hasHeader: true }, x: 100, y: 100 },
        { id: 'csv_output_1', type: 'csv_output', label: 'CSV 输出',
            params: { path: 'output/agg_verify.csv' }, x: 400, y: 100 }],
        edges: [{ id: 'e1', source: 'csv_input_1', target: 'csv_output_1' }]
    });
    const missing = 'D:\\\\no-such-dir\\\\agg_missing_' + Date.now() + '.csv';
    const created = [];
    for (let i = 0; i < 2; i++) {
        const job = await request('POST', '/api/jobs', {
            token, body: { name: 'agg-verify-' + i, parallelism: 1, dagJson: dag(missing) }
        });
        if (job.status !== 200) { console.error('建作业失败', job.status, job.data); process.exit(3); }
        created.push(job.data.id);
    }
    const submits = [];
    for (const id of created) submits.push(await request('POST', `/api/jobs/${id}/submit`, { token }));
    console.log('  两次提交返回:', submits.map(s => s.status).join(', '));
    await new Promise(r => setTimeout(r, 2500));   // 等异步告警落库

    const alerts = await request('GET', '/api/alerts?page=0&size=20', { token });
    const list = (alerts.data && (alerts.data.content || alerts.data)) || [];
    // 只筛本次验证用的两个作业（历史告警里也有同样的报错文本，别误判）
    const marker = 'agg_missing_';
    const csvAlerts = list.filter(a => String(a.message || '').includes(marker));
    const merged = csvAlerts.find(a => (a.occurrences || 1) > 1);
    check('同根因两次失败只留一条告警记录', csvAlerts.length === 1, `条数=${csvAlerts.length}`);
    check('该告警被标记为合并（occurrences > 1）', !!merged, merged ? `occurrences=${merged.occurrences}` : '未找到合并记录');
    check('告警文本写明合并次数与涉及作业',
        !!(merged && String(merged.message).includes('同类告警已合并') && String(merged.message).includes('agg-verify')),
        merged ? merged.message.slice(0, 120) : '');
    check('告警记录带聚合签名', !!(merged && merged.signature), merged ? String(merged.signature).slice(0, 80) : '');

    // ---------- 清理 ----------
    for (const id of created) await request('DELETE', `/api/jobs/${id}`, { token });
    if (merged && merged.id) await request('DELETE', `/api/alerts/${merged.id}`, { token });
    console.log(`  已清理验证用作业 ${created.join(', ')}${merged ? ' 与告警 #' + merged.id : ''}`);

    const failed = results.filter(r => !r.ok).length;
    console.log(`\n  结果：${results.length - failed}/${results.length} 通过`);
    process.exit(failed === 0 ? 0 : 1);
})().catch(e => { console.error('ERROR:', e.message); process.exit(3); });
