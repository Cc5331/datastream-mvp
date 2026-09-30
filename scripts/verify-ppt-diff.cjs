// 独立验收：逐页提取 v9 / v10 文本并做行级 diff（唯一可信的"到底改了什么"证据）
const fs = require('fs'), zlib = require('zlib');
const A = process.argv[2], B = process.argv[3];

function readZip(b) {
  const eocd = b.lastIndexOf(Buffer.from([0x50, 0x4b, 0x05, 0x06]));
  const n = b.readUInt16LE(eocd + 10); let off = b.readUInt32LE(eocd + 16);
  const out = {};
  for (let i = 0; i < n; i++) {
    const nl = b.readUInt16LE(off + 28), el = b.readUInt16LE(off + 30), cl = b.readUInt16LE(off + 32);
    const name = b.toString('utf8', off + 46, off + 46 + nl);
    const lho = b.readUInt32LE(off + 42);
    const lnl = b.readUInt16LE(lho + 26), lel = b.readUInt16LE(lho + 28);
    const ds = lho + 30 + lnl + lel;
    out[name] = { method: b.readUInt16LE(off + 10), data: b.slice(ds, ds + b.readUInt32LE(off + 20)) };
    off += 46 + nl + el + cl;
  }
  return out;
}
const xmlOf = (e) => {
  let s = (e.method === 8 ? zlib.inflateRawSync(e.data) : e.data).toString('utf8');
  while (s.charCodeAt(0) === 0xFEFF) s = s.slice(1);   // 可能有多层 BOM（Node 写文件时会自带一个）
  return s;
};
const dec = (s) => s.replace(/&lt;/g, '<').replace(/&gt;/g, '>').replace(/&quot;/g, '"').replace(/&#34;/g, '"').replace(/&apos;/g, "'").replace(/&amp;/g, '&');
function paras(xml) {
  return (xml.match(/<a:p>[\s\S]*?<\/a:p>/g) || []).map(p =>
    dec([...p.matchAll(/<a:t>([\s\S]*?)<\/a:t>/g)].map(m => m[1]).join('')).replace(/\s+/g, ' ').trim()
  ).filter(s => s.length);
}

const za = readZip(fs.readFileSync(A)), zb = readZip(fs.readFileSync(B));
const keysA = Object.keys(za), keysB = new Set(Object.keys(zb));
console.log('条目数: v9=' + keysA.length + '  v10=' + Object.keys(zb).length + '  缺失=' + keysA.filter(n => !keysB.has(n)).length);

const slides = keysA.filter(k => /^ppt\/slides\/slide\d+\.xml$/.test(k))
  .sort((a, b) => parseInt(a.match(/(\d+)/)[1]) - parseInt(b.match(/(\d+)/)[1]));
let diffFiles = 0, diffParas = 0, structWarn = 0;
for (const nm of slides) {
  const pa = paras(xmlOf(za[nm]));
  const pb = paras(xmlOf(zb[nm]));
  const page = nm.match(/slide(\d+)/)[1];
  if (pa.length !== pb.length) { structWarn++; console.log('⚠ P' + page + ' 段落数变化 ' + pa.length + '→' + pb.length + '（脚本按最长对齐，仅提示）'); }
  const local = [];
  for (let i = 0; i < Math.max(pa.length, pb.length); i++) if ((pa[i] || '') !== (pb[i] || '')) local.push({ i, a: pa[i] || '', b: pb[i] || '' });
  if (local.length) {
    diffFiles++; diffParas += local.length;
    console.log('\n===== P' + page + ' 改动 ' + local.length + ' 段 =====');
    for (const d of local) { console.log('  - ' + d.a); console.log('  + ' + d.b); }
  }
}
const allB = slides.map(nm => paras(xmlOf(zb[nm])).join('\n')).join('\n');
console.log('\n--- 旧串残留检查（v10） ---');
for (const f of ['SQL Client → SQL Gateway → Flink REST', '8C32G', '5 秒内无缝切换', '5 秒内自动降级', '成功率 100%', '100% 走通'])
  console.log((allB.includes(f) ? '❌ 仍存在: ' : '✅ 已清除: ') + f);
console.log('--- 新串检查（v10） ---');
for (const m of ['SQL Gateway → SQL Client → Flink REST', '32vCPU/16G', '秒级无缝切换', '秒级自动降级', '提交流程不中断'])
  console.log((allB.includes(m) ? '✅ 已写入: ' : '❌ 缺失: ') + m);
console.log('\n汇总: 有改动页 ' + diffFiles + ' 页 / 共 ' + diffParas + ' 段' + (structWarn ? ' / 段落数警告 ' + structWarn + ' 页' : ' / 无结构变化'));
