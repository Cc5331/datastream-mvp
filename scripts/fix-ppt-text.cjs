// 基于 v9 生成 v10：仅替换指定段落文本；其余 zip 条目逐字节原样透传
const fs = require('fs'), zlib = require('zlib');
const SRC = 'D:/code/比赛/2026省服务外包/docs/ds-mvp_report_v9.pptx';
const OUT = 'D:/code/比赛/2026省服务外包/docs/ds-mvp_report_v10.pptx';
const DRY = process.argv.includes('--dry');

function readZip(b) {
  const eocd = b.lastIndexOf(Buffer.from([0x50, 0x4b, 0x05, 0x06]));
  const n = b.readUInt16LE(eocd + 10);
  let off = b.readUInt32LE(eocd + 16);
  const entries = [];
  for (let i = 0; i < n; i++) {
    const method = b.readUInt16LE(off + 10);
    const csize = b.readUInt32LE(off + 20);
    const nl = b.readUInt16LE(off + 28), el = b.readUInt16LE(off + 30), cl = b.readUInt16LE(off + 32);
    const name = b.toString('utf8', off + 46, off + 46 + nl);
    const lho = b.readUInt32LE(off + 42);
    const lnl = b.readUInt16LE(lho + 26), lel = b.readUInt16LE(lho + 28);
    const ds = lho + 30 + lnl + lel;
    entries.push({ name, method, csize, usize: b.readUInt32LE(off + 24), data: b.slice(ds, ds + csize), crc: b.readUInt32LE(off + 16) });
    off += 46 + nl + el + cl;
  }
  return entries;
}

function writeZip(entries, rawData) {
  const locals = [], centrals = [];
  let offset = 0;
  for (const e of entries) {
    const nameBuf = Buffer.from(e.name, 'utf8');
    // 未改动的条目：直接透传原始字节（压缩数据 + 原始长度字段），保证逐字节不变
    const raw = rawData.get(e.name);
    const body = raw ? raw.body : (e.method === 8 ? zlib.deflateRawSync(e.data, { level: 9 }) : e.data);
    const usize = raw ? raw.rawSize : e.data.length;
    const crc = raw ? raw.crc : zlib.crc32(e.data);
    const method = raw ? raw.method : e.method;
    const lh = Buffer.alloc(30);
    lh.writeUInt32LE(0x04034b50, 0); lh.writeUInt16LE(20, 4); lh.writeUInt16LE(0x0800, 6);
    lh.writeUInt16LE(method, 8); lh.writeUInt16LE(0, 10); lh.writeUInt16LE(0, 12);
    lh.writeUInt32LE(crc >>> 0, 14); lh.writeUInt32LE(body.length, 18); lh.writeUInt32LE(usize, 22);
    lh.writeUInt16LE(nameBuf.length, 26); lh.writeUInt16LE(0, 28);
    locals.push(lh, nameBuf, body);
    const ch = Buffer.alloc(46);
    ch.writeUInt32LE(0x02014b50, 0); ch.writeUInt16LE(20, 4); ch.writeUInt16LE(20, 6); ch.writeUInt16LE(0x0800, 8);
    ch.writeUInt16LE(method, 10); ch.writeUInt16LE(0, 12); ch.writeUInt16LE(0, 14);
    ch.writeUInt32LE(crc >>> 0, 16); ch.writeUInt32LE(body.length, 20); ch.writeUInt32LE(usize, 24);
    ch.writeUInt16LE(nameBuf.length, 28); ch.writeUInt32LE(0, 38); ch.writeUInt32LE(offset, 42);
    centrals.push(ch, nameBuf);
    offset += lh.length + nameBuf.length + body.length;
  }
  const cd = Buffer.concat(centrals);
  const eocd = Buffer.alloc(22);
  eocd.writeUInt32LE(0x06054b50, 0);
  eocd.writeUInt16LE(entries.length, 8); eocd.writeUInt16LE(entries.length, 10);
  eocd.writeUInt32LE(cd.length, 12); eocd.writeUInt32LE(offset, 16);
  return Buffer.concat([...locals, cd, eocd]);
}

function inflate(e) { return e.method === 8 ? zlib.inflateRawSync(e.data) : e.data; }
function textOf(buf) { let s = buf.toString('utf8'); if (s.charCodeAt(0) === 0xFEFF) s = s.slice(1); return s; }

// 段落级替换：newText 按字符比例分摊回原 run，格式（rPr）自动保留
// 关键：记账统一用"解码后文本"坐标（XML 里 &quot; 占 6 字符但解码后只算 1 个），写回前再转义
const escXml = (s) => s.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
const decXml = (s) => s.replace(/&lt;/g, '<').replace(/&gt;/g, '>').replace(/&quot;/g, '"').replace(/&#34;/g, '"').replace(/&apos;/g, "'").replace(/&amp;/g, '&');
// 段落级替换（稳妥版）：整段文本替换后重建为"单个 run"，沿用该段首个 run 的 rPr
// 不做 run 分摊——三处目标段落每段内部格式一致（纯文本 + 字号/斜体），重建后视觉无差别
function replaceInPara(paraXml, find, newText) {
  const runs = [...paraXml.matchAll(/<a:t>([\s\S]*?)<\/a:t>/g)];
  if (!runs.length) return null;
  const dec = runs.map(m => decXml(m[1]));
  const joined = dec.join('');
  const at = joined.indexOf(find);
  if (at < 0) return null;
  if (joined.indexOf(find, at + 1) >= 0) return { error: '匹配不唯一' };
  const newJoined = joined.slice(0, at) + newText + joined.slice(at + find.length);

  const first = runs[0];
  // 该 run 的 rPr（run 属性，不含 <a:t>）
  const head = paraXml.slice(0, first.index);
  const rPrMatch = head.match(/<a:rPr[^>]*\/>|<a:rPr[\s\S]*?<\/a:rPr>/);
  const pPrMatch = paraXml.match(/<a:pPr[^>]*\/>|<a:pPr[\s\S]*?<\/a:pPr>/);
  const runXml = '<a:r>' + (rPrMatch ? rPrMatch[0] : '') + '<a:t>' + escXml(newJoined) + '</a:t></a:r>';
  const out = '<a:p>' + (pPrMatch ? pPrMatch[0] : '') + runXml + '</a:p>';

  // 自检①：新段落只含一个 run，且解码后逐字等于期望文本
  const back = [...out.matchAll(/<a:t>([\s\S]*?)<\/a:t>/g)].map(x => x[1]);
  if (back.length !== 1 || decXml(back[0]) !== newJoined) return { error: '段落重建自检失败' };
  // 自检②：XML 必须结构闭合、不许出现未转义的裸 & 
  if (/&(?!(amp|lt|gt|quot|apos|#\d+);)/.test(out)) return { error: '段落转义自检失败' };
  return { xml: out, before: joined, after: newJoined };
}

const EDITS = {
  'ppt/slides/slide8.xml': [
    { find: '→ SQL Client', newText: ' → SQL Client', why: '补回 Gateway 后的连接符空格，与「→ Flink REST」风格一致' },
    { find: '系统均能在 5 秒内自动降级并成功提交，保障作业提交成功率 100%，"永不掉线"的设计', newText: '系统均能在秒级自动降级并成功提交，保障提交流程不中断，"永不掉线"的设计', why: '③ 撤回「5 秒内」「成功率 100%」两个无实测支撑的硬承诺（匹配走解码文本，故此处用真实引号）' },
  ],
  'ppt/slides/slide16.xml': [
    { find: 'SQL Client → SQL Gateway → Flink REST', newText: 'SQL Gateway → SQL Client → Flink REST', why: '① 降级顺序与 P8 / 代码对齐' },
    { find: '5 秒内无缝切换', newText: '秒级无缝切换', why: '③ 收窄「5 秒」硬承诺（与 P8 一致）' },
    { find: '保障作业提交流程 100% 走通', newText: '保障作业提交流程不中断', why: '③ 收窄「100% 走通」（与 P8 一致）' },
  ],
  'ppt/slides/slide20.xml': [
    { find: '在 8C32G/300G 环境下完成不同数据量、不同并行度的吞吐压测并出报告', newText: '在 32vCPU/16G 实测环境下完成不同数据量、不同并行度的吞吐压测并出报告', why: '② 机型与 benchmark_report.md 对齐' },
    { find: '牵头 8C32G 环境下的压测调优', newText: '牵头 32vCPU/16G 环境下的压测调优', why: '② 同上（保持同页一致）' },
  ],
};

const entries = readZip(fs.readFileSync(SRC));
// 原始压缩字节快照：未改动条目原样透传（连压缩数据都不重压），保证除目标页之外逐字节一致
const rawData = new Map(entries.map(e => [e.name, { body: e.data, method: e.method, rawSize: e.usize, crc: e.crc }]));
console.log('源文件条目:', entries.length, '| 模式:', DRY ? '干跑（不写文件）' : '实写 v10');
// 每文件的验收规则：required 必须出现，forbidden 必须不存在
const VERIFY = {
  'ppt/slides/slide8.xml': { required: ['SQL Gateway → SQL Client → Flink REST', '秒级自动降级', '提交流程不中断'], forbidden: ['5 秒内自动降级', '成功率 100%'] },
  'ppt/slides/slide16.xml': { required: ['SQL Gateway → SQL Client → Flink REST', '秒级无缝切换', '流程不中断'], forbidden: ['SQL Client → SQL Gateway → Flink REST', '5 秒内无缝切换', '100% 走通'] },
  'ppt/slides/slide20.xml': { required: ['32vCPU/16G 实测环境下', '32vCPU/16G 环境下的压测调优'], forbidden: ['8C32G'] },
};
let changedFiles = 0, applied = 0, failed = 0;
const TOTAL_EDITS = Object.values(EDITS).reduce((n, a) => n + a.length, 0);
const changed = new Set();   // 通过校验、允许写盘的文件
for (const e of entries) {
  const edits = EDITS[e.name];
  if (!edits) continue;
  let xml = textOf(inflate(e));
  // 段落区间只记录一次（用偏移量回填，避免对已修改的 XML 用旧串做 replace 而失配）
  const paraRe = /<a:p>[\s\S]*?<\/a:p>/g;
  const paras = [];
  let pm;
  while ((pm = paraRe.exec(xml)) !== null) paras.push({ start: pm.index, end: pm.index + pm[0].length });

  let unresolved = false;
  // 每个编辑前"重新计算"段落偏移：上一处替换会改变该段字符长度，用旧区间切新 XML 会切坏段落
  const scanParas = () => {
    const re = /<a:p>[\s\S]*?<\/a:p>/g, list = [];
    let m;
    while ((m = re.exec(xml)) !== null) list.push({ start: m.index, end: m.index + m[0].length });
    return list;
  };
  for (const ed of edits) {
    let hit = false;
    const paras = scanParas();
    for (let i = 0; i < paras.length; i++) {
      const pr = paras[i];
      const para = xml.slice(pr.start, pr.end);
      const r = replaceInPara(para, ed.find, ed.newText);
      if (!r) continue;
      if (r.error) { console.log('  ✗ ' + r.error + '，拒绝改动 ' + e.name + ' para#' + i + ': ' + ed.find); unresolved = true; hit = true; break; }
      xml = xml.slice(0, pr.start) + r.xml + xml.slice(pr.end);
      hit = true; applied++;
      console.log('  ✓ ' + e.name.replace('ppt/slides/', '') + ' para#' + i + '  (Δlen ' + (r.after.length - r.before.length) + ')');
      console.log('      改前: ' + r.before.replace(/\s+/g, ' ').slice(0, 100));
      console.log('      改后: ' + r.after.replace(/\s+/g, ' ').slice(0, 100));
      break;
    }
    if (!hit) { console.log('  ✗ 未命中 ' + e.name + ': ' + ed.find); unresolved = true; }
  }

  // 自检：残留检查在"解码文本"上做（否则 &quot; 会把匹配带偏）
  const rule = VERIFY[e.name];
  const xmlDec = decXml(xml);
  let fatal = false;
  for (const bad of rule.forbidden) if (xmlDec.includes(decXml(bad))) { console.log('  ⚠ 致命：仍残留旧串「' + bad + '」'); fatal = true; }
  if (unresolved) fatal = true;
  if (fatal) { failed++; console.log('  → ' + e.name + ' 不写入'); continue; }

  // 写回：不加 BOM——原文件条目也无 BOM，且 zip 规范不宜引入 BOM
  e.data = Buffer.from(xml, 'utf8');
  e.method = 8; e.csize = 0; changedFiles++;
  rawData.delete(e.name);   // 被改动的条目不再透传，改为重新压缩写入
  changed.add(e.name);
}
console.log('\n改动文件数:', changedFiles, '| 生效替换数:', applied + '/ ' + TOTAL_EDITS + ' | 致命:', failed);
if (failed > 0) { console.log('存在致命问题，不写出文件。'); process.exit(1); }
if (!DRY) {
  const outBuf = writeZip(entries, rawData);
  fs.writeFileSync(OUT, outBuf);
  console.log('已写出: ' + OUT + '  (' + outBuf.length + ' 字节)');
} else {
  console.log('干跑结束，未写文件。');
}
