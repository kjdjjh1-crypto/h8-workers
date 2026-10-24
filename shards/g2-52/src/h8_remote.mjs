// h8_remote.mjs — H8 class B 단위를 맥 밖(Kaggle·Modal·Colab)에서 받기·집계하고 결과만 가져와 원장에 합친다 (2026-10-24)
// pack:   원장에서 keys 단위 + 그 kline 단위만 담은 작은 원장, h8_run 코드 묶음, worker.py 를 작업 폴더로 만든다.
// export: (원격에서) 작은 원장의 결과 행과 raw/h8_out 을 result/ 로 모은다.
// lease / merge: 맥 원장에서 단위를 'remote' 로 빌려 주고, 돌아온 결과를 넣는다. 맥 실행기가 멈춘 동안에만 쓴다.
import fs from 'node:fs';
import path from 'node:path';
import { spawnSync } from 'node:child_process';
import { DatabaseSync } from 'node:sqlite';
import { ROOT, LEDGER, OUTDIR, DONE_STATES, openLedger, klineKeyOf } from './h8_run.mjs';

const SRC = path.join(ROOT, 'src');
const UNIT_COLS = ['key', 'market', 'dataset', 'class', 'symbol', 'period', 'bytes', 'checksum_sha256', 'sha256',
  'state', 'rows', 'quote_sum', 'c2', 'c3', 'held_reason', 'out_file', 'out_bytes', 'bundle', 'updated_at'];
const RESULT_COLS = ['state', 'sha256', 'rows', 'quote_sum', 'c2', 'c3', 'held_reason', 'out_file', 'out_bytes'];

function readKeys(p) {
  return [...new Set(fs.readFileSync(p, 'utf8').split('\n').map((s) => s.trim()).filter(Boolean))];
}

// h8_run.mjs 에서 './x.mjs' 로 닿는 파일만 (H7 등 다른 코드는 담지 않는다)
function moduleClosure(entry) {
  const seen = new Set();
  const walk = (f) => {
    if (seen.has(f)) return;
    seen.add(f);
    const text = fs.readFileSync(path.join(SRC, f), 'utf8');
    for (const m of text.matchAll(/['"]\.\/([A-Za-z0-9_]+\.mjs)['"]/g)) walk(m[1]);
  };
  walk(entry);
  return [...seen].sort();
}

// 2026-10-24 이전에 띄운 실행기는 원장 timeout 이 없다. 그 뒤에 띄운 실행기면 --live 로 실행 중에도 쓴다.
function runnerAlive() {
  if (process.argv.includes('--live')) return [];
  const r = spawnSync('pgrep', ['-f', 'src/h8_run.mjs'], { encoding: 'utf8' });
  return r.stdout.trim().split('\n').filter(Boolean);
}

export function pack({ keysFile, outDir }) {
  const keys = readKeys(keysFile);
  if (fs.existsSync(outDir) && fs.readdirSync(outDir).length) throw new Error(`out dir not empty: ${outDir}`);
  const src = new DatabaseSync(LEDGER, { readOnly: true });
  const get = src.prepare('SELECT * FROM units WHERE key=?');
  const units = [];
  const klines = new Map();
  for (const k of keys) {
    const u = get.get(k);
    if (!u) throw new Error(`unknown key ${k}`);
    if (u.class !== 'B' || !['planned', 'remote'].includes(u.state)) throw new Error(`not a leasable class B unit: ${k} class=${u.class} state=${u.state}`);
    units.push({ ...u, state: 'planned' });
    const kk = klineKeyOf(u);
    const ku = get.get(kk);
    if (ku) klines.set(kk, ku);
  }
  const events = src.prepare(`SELECT * FROM source_events WHERE key IN (${keys.map(() => '?').join(',')})`).all(...keys);
  src.close();

  fs.mkdirSync(path.join(outDir, 'src'), { recursive: true });
  fs.mkdirSync(path.join(outDir, 'manifest'), { recursive: true });
  for (const f of moduleClosure('h8_run.mjs').concat('h8_remote.mjs')) fs.copyFileSync(path.join(SRC, f), path.join(outDir, 'src', f));
  fs.copyFileSync(path.join(ROOT, 'manifest/h6x_source_gaps.json'), path.join(outDir, 'manifest/h6x_source_gaps.json'));
  fs.copyFileSync(path.join(ROOT, 'src/h8_remote_worker.py'), path.join(outDir, 'worker.py'));
  fs.writeFileSync(path.join(outDir, 'keys.txt'), keys.join('\n') + '\n');

  const db = openLedger(path.join(outDir, 'db/h8_ledger.db'));
  const ins = db.prepare(`INSERT INTO units(${UNIT_COLS.join(',')}) VALUES (${UNIT_COLS.map(() => '?').join(',')})`);
  db.exec('BEGIN');
  for (const u of [...units, ...klines.values()]) ins.run(...UNIT_COLS.map((c) => u[c] ?? null));
  const insE = db.prepare('INSERT INTO source_events(key,kind,detail,at) VALUES (?,?,?,?)');
  for (const e of events) insE.run(e.key, e.kind, e.detail, e.at);
  db.exec('COMMIT');
  db.exec('PRAGMA journal_mode=DELETE');
  db.close();
  const bytes = units.reduce((s, u) => s + u.bytes, 0);
  return { units: units.length, klines: klines.size, events: events.length, input_bytes: bytes };
}

// 원격에서: node src/h8_remote.mjs export --keys keys.txt --out result
export function exportResult({ keysFile, outDir }) {
  const keys = readKeys(keysFile);
  const db = new DatabaseSync(LEDGER, { readOnly: true });
  const get = db.prepare('SELECT * FROM units WHERE key=?');
  const rows = keys.map((k) => get.get(k)).filter(Boolean);
  const events = db.prepare(`SELECT * FROM source_events WHERE key IN (${keys.map(() => '?').join(',')})`).all(...keys);
  db.close();
  fs.mkdirSync(outDir, { recursive: true });
  for (const r of rows) {
    if (r.state !== 'done' || !r.out_file) continue;
    const dst = path.join(outDir, r.out_file);
    fs.mkdirSync(path.dirname(dst), { recursive: true });
    fs.copyFileSync(path.join(ROOT, r.out_file), dst);
  }
  fs.writeFileSync(path.join(outDir, 'rows.json'), JSON.stringify({ rows, events }, null, 0));
  const count = (s) => rows.filter((r) => r.state === s).length;
  return { keys: keys.length, done: count('done'), held: count('held'), other: rows.length - count('done') - count('held') };
}

export function lease({ keysFile, tag }) {
  const alive = runnerAlive();
  if (alive.length) throw new Error(`h8_run.mjs is running (pid ${alive.join(',')}); stop it first`);
  const keys = readKeys(keysFile);
  const db = openLedger();
  const syms = new Map();
  for (const k of keys) {
    const u = db.prepare('SELECT market,dataset,symbol,class,state FROM units WHERE key=?').get(k);
    if (!u || u.class !== 'B' || u.state !== 'planned') throw new Error(`not leasable: ${k} ${u?.class}/${u?.state}`);
    const g = `${u.market}|${u.dataset}|${u.symbol}`;
    syms.set(g, { ...u, mine: (syms.get(g)?.mine ?? 0) + 1 });
  }
  // 그룹(심볼)을 통째로 빌려 줘야 맥 실행기가 남은 반쪽을 따로 묶지 않는다
  for (const [g, u] of syms) {
    const n = db.prepare(`SELECT COUNT(*) n FROM units WHERE market=? AND dataset=? AND symbol=? AND class='B' AND state NOT IN (${DONE_STATES.map(() => '?').join(',')})`)
      .get(u.market, u.dataset, u.symbol, ...DONE_STATES).n;
    if (n !== u.mine) throw new Error(`partial group ${g}: ${u.mine} of ${n} pending units in keys`);
  }
  const now = new Date().toISOString();
  db.exec('BEGIN');
  for (const k of keys) {
    db.prepare("UPDATE units SET state='remote', updated_at=? WHERE key=? AND state='planned'").run(now, k);
    db.prepare('INSERT INTO source_events(key,kind,detail,at) VALUES (?,?,?,?)').run(k, 'remote_lease', tag, now);
  }
  db.exec('COMMIT');
  return { leased: keys.length, groups: syms.size, tag };
}

// fromDir = 원격 result/ (rows.json + raw/h8_out/...)
export function merge({ fromDir, tag }) {
  const alive = runnerAlive();
  if (alive.length) throw new Error(`h8_run.mjs is running (pid ${alive.join(',')}); stop it first`);
  const { rows, events } = JSON.parse(fs.readFileSync(path.join(fromDir, 'rows.json'), 'utf8'));
  const db = openLedger();
  const now = new Date().toISOString();
  const res = { done: 0, held: 0, released: 0, skipped: [] };
  db.exec('BEGIN');
  try {
    for (const r of rows) {
      const cur = db.prepare('SELECT state FROM units WHERE key=?').get(r.key);
      if (!cur || !['remote', 'planned'].includes(cur.state)) { res.skipped.push(`${r.key} ${cur?.state}`); continue; }
      if (r.state === 'done') {
        const from = path.join(fromDir, r.out_file);
        const to = path.join(ROOT, r.out_file);
        if (!r.out_file.startsWith(path.relative(ROOT, OUTDIR) + '/')) throw new Error(`bad out_file ${r.out_file}`);
        if (fs.statSync(from).size !== r.out_bytes) throw new Error(`size mismatch ${r.out_file}`);
        fs.mkdirSync(path.dirname(to), { recursive: true });
        fs.copyFileSync(from, to);
      }
      if (DONE_STATES.includes(r.state)) {
        db.prepare(`UPDATE units SET ${RESULT_COLS.map((c) => `${c}=?`).join(',')}, updated_at=? WHERE key=?`)
          .run(...RESULT_COLS.map((c) => r[c] ?? null), now, r.key);
        res[r.state === 'held' ? 'held' : 'done']++;
      } else {
        db.prepare("UPDATE units SET state='planned', updated_at=? WHERE key=?").run(now, r.key);
        res.released++;
      }
      db.prepare('INSERT INTO source_events(key,kind,detail,at) VALUES (?,?,?,?)').run(r.key, 'remote_merge', `${tag} ${r.state}`, now);
    }
    const known = db.prepare('SELECT 1 FROM source_events WHERE key=? AND kind=? AND detail=? AND at=?');
    for (const e of events) {
      if (e.kind === 'remote_lease' || known.get(e.key, e.kind, e.detail, e.at)) continue;
      db.prepare('INSERT INTO source_events(key,kind,detail,at) VALUES (?,?,?,?)').run(e.key, e.kind, e.detail, e.at);
    }
    db.exec('COMMIT');
  } catch (e) { db.exec('ROLLBACK'); throw e; }
  return res;
}

function argOf(name, dflt = null) {
  const i = process.argv.indexOf(name);
  return i >= 0 ? process.argv[i + 1] : dflt;
}

if (process.argv[1] && path.resolve(process.argv[1]) === path.join(SRC, 'h8_remote.mjs')) {
  const cmd = process.argv[2];
  const fns = {
    pack: () => pack({ keysFile: argOf('--keys'), outDir: argOf('--out') }),
    export: () => exportResult({ keysFile: argOf('--keys'), outDir: argOf('--out') }),
    lease: () => lease({ keysFile: argOf('--keys'), tag: argOf('--tag', 'remote') }),
    merge: () => merge({ fromDir: argOf('--from'), tag: argOf('--tag', 'remote') })
  };
  if (!fns[cmd]) { console.error('usage: node src/h8_remote.mjs pack|export|lease|merge --keys k.txt --out dir | --from dir [--tag t]'); process.exit(2); }
  try { console.log(JSON.stringify(fns[cmd]())); }
  catch (e) { console.error(e.message); process.exit(1); }
}
