// h8_run.mjs — H8 실행기: 원장(--init), S3 class A, S4 aggTrades, S5 trades, 묶음·올리기·C4 (지시서 H8, 2026-10-22)
// 고정: 단위 상태 이름(§2-2), 결과 파일 형식(§2-3), 판정 C1·CA·C2·C3·C4·K1(§2-4), 묶음·드라이브(§2-5), 처리 순서(§2-6).
// 쓰기: db/h8_ledger.db, raw/h8_stage/, raw/h8_out/, extent evi: evidence/h8, PROGRESS_H8.jsonl. tmp/ 는 tar 목록·임시 묶음만.
import fs from 'node:fs';
import path from 'node:path';
import zlib from 'node:zlib';
import crypto from 'node:crypto';
import readline from 'node:readline';
import { spawn, spawnSync } from 'node:child_process';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { DatabaseSync } from 'node:sqlite';
import { addDecimal, compareDecimal } from './parser.mjs';
import { judgeC2 } from './h6_of.mjs';
import { judgeC2X, OF_COLS, TF_COLS } from './h6x_trades.mjs';
import { aggInWorker } from './h6_agg_worker.mjs';
import { readKline1m } from './h6_kline.mjs';
import { wholeDupCheck, copiesCheck } from './h6_srcdup.mjs';
import { recomputeFromZip } from './h6_recheck.mjs';
import { recheckTradesZip } from './h6x_recheck.mjs';

export const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
export const MANIFEST = path.join(ROOT, 'manifest/h8_all.tsv');
export const LEDGER = path.join(ROOT, 'db/h8_ledger.db');
export const STAGE = path.join(ROOT, 'raw/h8_stage');
export const OUTDIR = path.join(ROOT, 'raw/h8_out');
export const TMP = path.join(ROOT, 'tmp');
export const STOP_FILE = path.join(ROOT, 'evidence/h8/STOP');
export const PROGRESS = path.join(ROOT, 'PROGRESS_H8.jsonl');
export const RANK_TSV = path.join(ROOT, 'manifest/h8_rank.tsv');
export const DRIVE_H8 = 'gdrive:TradeTerm-ticks/h8';
export const URL_BASE = 'https://data.binance.vision/';

const GAPS_FILE = path.join(ROOT, 'manifest/h6x_source_gaps.json');
const GAPS = JSON.parse(fs.readFileSync(GAPS_FILE, 'utf8'));
const GAP_BY_KEY = new Map();
for (const g of GAPS.gaps) {
  if (!GAP_BY_KEY.has(g.key)) GAP_BY_KEY.set(g.key, []);
  GAP_BY_KEY.get(g.key).push({ id: g.id, expect: g.expect, from: Date.parse(g.from), to: Date.parse(g.to) });
}
const WHOLE_DUP = new Map(GAPS.whole_dups.map((w) => [w.key, w]));
const PICK_COPY = new Map(GAPS.pick_copy.map((p) => [p.key, p]));

// ---------- 상태·상수 ----------
export const DONE_STATES = ['done', 'bundled', 'uploaded', 'local_deleted', 'held'];
export const PENDING_STATES = ['planned', 'downloaded', 'c1_ok'];
export const NOT_DONE_SQL = DONE_STATES.map(() => '?').join(',');
export function resumeSkip(state) { return DONE_STATES.includes(state); }

const S3_DATASET_ORDER = [
  ['spot', 'klines'], ['um', 'klines'], ['cm', 'klines'],
  ['um', 'markPriceKlines'], ['um', 'indexPriceKlines'], ['um', 'premiumIndexKlines'],
  ['cm', 'markPriceKlines'], ['cm', 'indexPriceKlines'], ['cm', 'premiumIndexKlines'],
  ['spot', 'fundingRate'], ['um', 'fundingRate'], ['cm', 'fundingRate'],
  ['spot', 'metrics'], ['um', 'metrics'], ['cm', 'metrics'],
  ['spot', 'bookDepth'], ['um', 'bookDepth'], ['cm', 'bookDepth'],
  ['option', 'BVOLIndex'], ['option', 'EOHSummary']
];
const KLINE_DATASETS = new Set(['klines', 'markPriceKlines', 'indexPriceKlines', 'premiumIndexKlines']);
function s3Rank(market, dataset) {
  const i = S3_DATASET_ORDER.findIndex(([m, d]) => m === market && d === dataset);
  return i < 0 ? 999 : i;
}

export const STEP_CAP_H = { S3: 138.4, S4: 163.2, S5: 251.3 }; // §C-5 (동시 수를 늘려도 안 바뀐다)
const RANGE_MIN = 8 << 20;   // §C-7: 8 MB 미만은 GET 한 번
const RANGE_CONN = 6;        // H6 와 같은 파일당 연결 수
// 원격 일꾼(Kaggle 등)은 디스크가 30 GiB보다 작다. h8_remote_worker.py 가 H8_DISK_FLOOR_GIB 로 낮춘다.
const DISK_FLOOR_KB = Number(process.env.H8_DISK_FLOOR_GIB ?? 30) * 1024 * 1024; // 기본 30 GiB
const UP_CONC = 4;           // 올리기 동시 4묶음 (대장님 지시 2026-10-22: 3→4)
const VERIFY_CONC = UP_CONC; // 검증(md5sum)은 올리기와 다른 줄에서 돈다
const RETRY_MS = [30000, 60000, 120000];
export const MAX_ATTEMPTS = 3;

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const nowIso = () => new Date().toISOString();
const log = (s) => console.log(s);

export function stopRequested(stopFile = STOP_FILE) { return fs.existsSync(stopFile); }

// 같은 파일이 3번 실패하면 held 로 둔다 (§0-3). 시험 7번이 이 함수를 쓴다.
export async function withRetries(fn, { attempts = MAX_ATTEMPTS, delays = RETRY_MS, onError = () => {} } = {}) {
  let lastErr = null;
  for (let i = 1; i <= attempts; i++) {
    try { return { ok: true, value: await fn(i), attempts: i }; }
    catch (e) {
      lastErr = e;
      onError(e, i);
      if (e && e.aborted) break;
      if (i < attempts) await sleep(delays[Math.min(i - 1, delays.length - 1)]);
    }
  }
  return { ok: false, error: lastErr, attempts };
}

// STOP 파일이 있으면 다음 단위로 가지 않는다 (시험 8번).
export async function forEachUnit(units, fn, { stopFile = STOP_FILE } = {}) {
  let n = 0;
  for (const u of units) {
    if (stopRequested(stopFile)) break;
    await fn(u, n);
    n++;
  }
  return n;
}

// ---------- 원장 ----------
export function openLedger(dbPath = LEDGER) {
  fs.mkdirSync(path.dirname(dbPath), { recursive: true });
  // timeout: h8_remote.mjs lease/merge 가 실행 중에 원장을 써도 SQLITE_BUSY 로 단위가 held 되지 않게 기다린다
  const db = new DatabaseSync(dbPath, { timeout: 15000 });
  db.exec('PRAGMA journal_mode=WAL');
  db.exec('PRAGMA synchronous=NORMAL');
  db.exec(`CREATE TABLE IF NOT EXISTS units(
  key TEXT PRIMARY KEY, market TEXT, dataset TEXT, class TEXT, symbol TEXT, period TEXT,
  bytes INTEGER, checksum_sha256 TEXT, sha256 TEXT,
  state TEXT, rows INTEGER, quote_sum TEXT,
  c2 TEXT, c3 TEXT, held_reason TEXT,
  out_file TEXT, out_bytes INTEGER, bundle TEXT, updated_at TEXT)`);
  db.exec(`CREATE TABLE IF NOT EXISTS bundles(
  bundle TEXT PRIMARY KEY, market TEXT, dataset TEXT, symbol TEXT, files INTEGER, bytes INTEGER,
  local_md5 TEXT, remote_md5 TEXT, drive_path TEXT, state TEXT, error TEXT, updated_at TEXT)`);
  db.exec(`CREATE TABLE IF NOT EXISTS source_events(key TEXT, kind TEXT, detail TEXT, at TEXT)`);
  db.exec(`CREATE TABLE IF NOT EXISTS meta(k TEXT PRIMARY KEY, v TEXT)`);
  db.exec('CREATE INDEX IF NOT EXISTS units_class_state ON units(class,state)');
  db.exec('CREATE INDEX IF NOT EXISTS units_group ON units(market,dataset,symbol,class,state)');
  db.exec('CREATE INDEX IF NOT EXISTS units_bundle ON units(bundle)');
  db.exec('CREATE INDEX IF NOT EXISTS events_key ON source_events(key)');
  return db;
}

function countUnits(db) {
  const total = db.prepare('SELECT COUNT(*) n FROM units').get().n;
  const perClass = Object.fromEntries(db.prepare('SELECT class,COUNT(*) n FROM units GROUP BY class ORDER BY class').all().map((r) => [r.class, r.n]));
  const perState = Object.fromEntries(db.prepare('SELECT state,COUNT(*) n FROM units GROUP BY state ORDER BY state').all().map((r) => [r.state, r.n]));
  const held = db.prepare("SELECT COUNT(*) n FROM units WHERE state='held'").get().n;
  const events = db.prepare('SELECT COUNT(*) n FROM source_events').get().n;
  return { total, perClass, perState, held, source_events: events };
}

function readNoChecksum(p) {
  if (!fs.existsSync(p)) return [];
  return fs.readFileSync(p, 'utf8').split('\n').filter((l) => l.trim()).map((l) => l.split('\t')[0]);
}

export async function initLedger({
  manifestPath = MANIFEST, dbPath = LEDGER,
  nochecksumPath = path.join(ROOT, 'evidence/h8/raw/S0-nochecksum.tsv'), logFn = log
} = {}) {
  const db = openLedger(dbPath);
  const have = db.prepare('SELECT COUNT(*) n FROM units').get().n;
  if (have > 0) return { existed: true, ...countUnits(db) };
  const held = new Set(readNoChecksum(nochecksumPath));
  const t0 = Date.now();
  const ins = db.prepare(`INSERT INTO units(key,market,dataset,class,symbol,period,bytes,state,held_reason,updated_at)
    VALUES (?,?,?,?,?,?,?,?,?,?)`);
  const ev = db.prepare('INSERT INTO source_events(key,kind,detail,at) VALUES (?,?,?,?)');
  const setMeta = db.prepare('INSERT OR REPLACE INTO meta(k,v) VALUES (?,?)');
  let lineNo = 0, rows = 0, heldInserted = 0;
  const at = nowIso();
  const seen = new Set();
  db.exec('BEGIN');
  try {
    const rl = readline.createInterface({ input: fs.createReadStream(manifestPath), crlfDelay: Infinity });
    for await (const line of rl) {
      lineNo++;
      if (lineNo === 1) continue;
      if (!line.trim()) continue;
      const [market, dataset, , symbol, period, cls, key, bytes] = line.split('\t');
      if (seen.has(key)) continue; // 방어: 같은 key 중복이면 첫 줄만
      seen.add(key);
      const isHeld = held.has(key);
      ins.run(key, market, dataset, cls, symbol, period, Number(bytes),
        isHeld ? 'held' : 'planned', isHeld ? 'no_checksum' : null, at);
      rows++;
      if (isHeld) { heldInserted++; ev.run(key, 'no_checksum', 'zip 은 있으나 .CHECKSUM 짝이 없다', at); }
    }
    setMeta.run('init_ts', at);
    setMeta.run('manifest_rows', String(rows));
    db.exec('COMMIT');
  } catch (e) {
    db.exec('ROLLBACK');
    throw e;
  }
  logFn(`init: rows=${rows} held=${heldInserted} elapsed=${((Date.now() - t0) / 1000).toFixed(1)}s`);
  return { existed: false, manifest_rows: rows, ...countUnits(db) };
}

function appendProgress(obj) {
  fs.appendFileSync(PROGRESS, JSON.stringify({ ts: nowIso(), ...obj }) + '\n');
}
export function progressLine(step, result, evidence) { appendProgress({ step, result, evidence }); }

function setUnit(db, key, fields) {
  const cols = Object.keys(fields);
  const sql = `UPDATE units SET ${cols.map((c) => `${c}=?`).join(',')}, updated_at=? WHERE key=?`;
  db.prepare(sql).run(...cols.map((c) => fields[c]), nowIso(), key);
}
function getUnit(db, key) { return db.prepare('SELECT * FROM units WHERE key=?').get(key); }

// 작업 폴더(STAGE·OUTDIR)를 외장 SSD로 옮겼다(2026-10-25). 본체와 작업 폴더 디스크 중 작은 쪽을 본다.
export function diskFreeKb(p = null) {
  const free = (q) => { const s = fs.statfsSync(q); return Math.floor((s.bavail * s.bsize) / 1024); };
  return p ? free(p) : Math.min(free(ROOT), free(STAGE), free(OUTDIR));
}

// ---------- 받기 ----------
export class HttpError extends Error {
  constructor(status, url) { super(`http_${status}`); this.status = status; this.url = url; }
}

async function fetchChunks(url, { range = null, signal = null, timeoutMs = 0, onChunk = null } = {}) {
  const headers = range ? { Range: `bytes=${range[0]}-${range[1]}` } : {};
  const ac = new AbortController();
  const relay = () => ac.abort(signal.reason);
  if (signal) {
    if (signal.aborted) throw signal.reason ?? new Error('aborted');
    signal.addEventListener('abort', relay, { once: true });
  }
  const timer = timeoutMs > 0 ? setTimeout(() => ac.abort(new Error('timeout')), timeoutMs) : null;
  try {
    const res = await fetch(url, { headers, signal: ac.signal, redirect: 'follow' });
    if (!res.ok) {
      try { await res.body?.cancel(); } catch {}
      throw new HttpError(res.status, url);
    }
    let n = 0;
    try {
      for await (const chunk of res.body) {
        n += chunk.length;
        if (onChunk) await onChunk(chunk, n - chunk.length);
      }
    } catch (e) { e.got = n; throw e; }
    return { status: res.status, bytes: n };
  } finally {
    if (timer) clearTimeout(timer);
    if (signal) signal.removeEventListener('abort', relay);
  }
}

function timeoutFor(bytes) {
  return Math.min(6 * 3600 * 1000, Math.max(60000, Math.ceil(((bytes || 1) / 50000) * 1000)));
}

// key 하나(zip)를 받는다. dest=null 이면 버린다(받기 측정). 요청별 상태 코드를 그대로 돌려준다.
export async function downloadKey(key, expectedBytes, { dest = null, signal = null } = {}) {
  const url = URL_BASE + key;
  const statuses = [];
  const t0 = Date.now();
  if (!(expectedBytes >= RANGE_MIN)) {
    let fh = null;
    if (dest) fh = await fs.promises.open(dest, 'w');
    try {
      const r = await fetchChunks(url, {
        signal, timeoutMs: timeoutFor(expectedBytes),
        onChunk: fh ? async (c) => { await fh.write(c); } : null
      });
      statuses.push(r.status);
      return { bytes: r.bytes, statuses, ms: Date.now() - t0 };
    } finally { if (fh) await fh.close(); }
  }
  const size = expectedBytes;
  const step = Math.ceil(size / RANGE_CONN);
  const parts = [];
  for (let k = 0; k < RANGE_CONN; k++) {
    const a = k * step, b = Math.min(size, a + step) - 1;
    if (a <= b) parts.push({ a, b });
  }
  let fh = null;
  if (dest) { fh = await fs.promises.open(dest, 'w'); await fh.truncate(size); }
  let got = 0;
  try {
    await Promise.all(parts.map(async ({ a, b }) => {
      try {
        const r = await fetchChunks(url, {
          range: [a, b], signal, timeoutMs: timeoutFor(b - a + 1),
          onChunk: fh ? async (c, off) => { await fh.write(c, 0, c.length, a + off); } : null
        });
        statuses.push(r.status);
        got += r.bytes;
        if (r.bytes !== b - a + 1) throw new Error(`range size ${r.bytes} != ${b - a + 1} ${key}`);
      } catch (e) {
        e.got = (e.got ?? 0) + got;
        throw e;
      }
    }));
  } finally { if (fh) await fh.close(); }
  return { bytes: got, statuses, ms: Date.now() - t0 };
}

export async function fetchChecksum(key, { signal = null } = {}) {
  let text = '';
  const r = await fetchChunks(URL_BASE + key + '.CHECKSUM', {
    signal, timeoutMs: 60000, onChunk: async (c) => { text += Buffer.from(c).toString('utf8'); }
  });
  const sha = text.trim().split(/\s+/)[0].toLowerCase();
  if (!/^[0-9a-f]{64}$/.test(sha)) throw new Error(`bad CHECKSUM content: ${text.slice(0, 40)}`);
  return { sha, statuses: [r.status] };
}

async function hashFile(p, algo) {
  const h = crypto.createHash(algo);
  for await (const chunk of fs.createReadStream(p)) h.update(chunk);
  return h.digest('hex');
}
export const sha256File = (p) => hashFile(p, 'sha256');
export const md5File = (p) => hashFile(p, 'md5');
export function md5FileSync(p) {
  const h = crypto.createHash('md5');
  const fd = fs.openSync(p, 'r');
  const buf = Buffer.alloc(8 * 1024 * 1024);
  try { for (;;) { const n = fs.readSync(fd, buf, 0, buf.length, null); if (n <= 0) break; h.update(buf.subarray(0, n)); } }
  finally { fs.closeSync(fd); }
  return h.digest('hex');
}

export function stageName(u) {
  return `${u.market}__${u.dataset}__${u.symbol}__${path.basename(u.key)}`;
}

// ---------- CA (class A 형식, 정보) ----------
function parseCaTime(raw) {
  const s = String(raw).trim();
  if (/^\d{13}$/.test(s)) return Number(s);
  if (/^\d{16}$/.test(s)) return Number(BigInt(s) / 1000n);
  const m = /^(\d{4})-(\d{2})-(\d{2})[ T](\d{2}):(\d{2}):(\d{2})/.exec(s);
  if (m) return Date.UTC(+m[1], +m[2] - 1, +m[3], +m[4], +m[5], +m[6]);
  return null;
}
function periodRangeMs(period) {
  if (/^\d{4}-\d{2}$/.test(period)) {
    const [y, m] = period.split('-').map(Number);
    return [Date.UTC(y, m - 1, 1), Date.UTC(m === 12 ? y + 1 : y, m === 12 ? 0 : m, 1)];
  }
  if (/^\d{4}-\d{2}-\d{2}$/.test(period)) {
    const [y, m, d] = period.split('-').map(Number);
    const a = Date.UTC(y, m - 1, d);
    return [a, a + 86400000];
  }
  return null;
}
const TIME_HEADERS = new Set(['open_time', 'calc_time', 'create_time', 'timestamp', 'date', 'time', 'funding_time', 'start_time']);

export function caCheckZip(zipPath, { period = '', dataset = '' } = {}) {
  return new Promise((resolve) => {
    const child = spawn('unzip', ['-p', zipPath], { stdio: ['ignore', 'pipe', 'pipe'] });
    const errChunks = [];
    child.stderr.on('data', (d) => errChunks.push(d));
    const rl = readline.createInterface({ input: child.stdout, crlfDelay: Infinity });
    const res = {
      lines: 0, rows: 0, cols: null, header: null, header_at: null, time_col: -1,
      late_header: 0, bad_cols: 0, bad_time: 0, not_ascending: 0, dup_time: 0,
      out_of_range: 0, kline_not_60s: 0, first_time: null, last_time: null, quote_sum: null
    };
    let timeIdx = -1, quoteIdx = -1, prev = null;
    const [pStart, pEnd] = periodRangeMs(period) ?? [null, null];
    rl.on('line', (line) => {
      res.lines++;
      if (!line.trim()) return;
      const cols = line.split(',');
      if (res.cols === null) res.cols = cols.length;
      else if (cols.length !== res.cols) res.bad_cols++;
      const first = cols[0].trim();
      const isNum = /^-?\d+(\.\d+)?$/.test(first);
      const isTimeStr = /^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}/.test(first);
      if (!isNum && !isTimeStr) {
        if (res.rows === 0) { res.header = line.trim(); res.header_at = res.lines; return; }
        res.late_header++;
        return;
      }
      if (res.rows === 0) {
        if (res.header) {
          const hs = res.header.split(',').map((s) => s.trim());
          for (let i = 0; i < hs.length; i++) if (TIME_HEADERS.has(hs[i])) { timeIdx = i; break; }
          quoteIdx = hs.indexOf('quote_volume');
        }
        if (timeIdx < 0) for (let i = 0; i < cols.length; i++) if (parseCaTime(cols[i]) !== null) { timeIdx = i; break; }
        res.time_col = timeIdx;
      }
      res.rows++;
      if (timeIdx >= 0 && timeIdx < cols.length) {
        const t = parseCaTime(cols[timeIdx]);
        if (t === null) res.bad_time++;
        else {
          if (res.first_time === null) res.first_time = t;
          res.last_time = t;
          if (prev !== null) {
            if (t < prev) res.not_ascending++;
            if (t === prev) res.dup_time++;
          }
          prev = t;
          if (pStart !== null && (t < pStart || t >= pEnd)) res.out_of_range++;
          if (dataset === 'klines' && t % 60000 !== 0) res.kline_not_60s++;
        }
      } else res.bad_time++;
      if (quoteIdx >= 0 && quoteIdx < cols.length) {
        const v = cols[quoteIdx].trim();
        if (/^-?\d+(\.\d+)?$/.test(v)) res.quote_sum = res.quote_sum === null ? v : addDecimal(res.quote_sum, v);
      }
    });
    const finish = (code) => {
      const problems = [];
      if (res.late_header) problems.push(`late_header=${res.late_header}`);
      if (res.bad_cols) problems.push(`bad_cols=${res.bad_cols}`);
      if (res.bad_time) problems.push(`bad_time=${res.bad_time}`);
      if (res.not_ascending) problems.push(`not_ascending=${res.not_ascending}`);
      if (res.out_of_range) problems.push(`out_of_range=${res.out_of_range}`);
      if (res.dup_time) problems.push(`dup_time=${res.dup_time}`);
      if (res.kline_not_60s) problems.push(`kline_not_60s=${res.kline_not_60s}`);
      if (code !== 0) problems.push(`unzip_exit=${code}`);
      resolve({ ...res, status: problems.length ? 'CA:' + problems.join(';') : 'CA:ok' });
    };
    let closed = false;
    rl.on('close', () => { closed = true; if (child.exitCode !== null) finish(child.exitCode); });
    child.on('close', (code) => finish(code ?? (errChunks.length ? 1 : 0)));
    child.on('error', () => resolve({ ...res, status: 'CA:unzip_error' }));
  });
}

// ---------- 결과 파일 (§2-3) ----------
export function resultPathOf(u) {
  const table = u.dataset === 'aggTrades' ? 'of_1m' : 'tf_1m';
  return path.join(OUTDIR, u.market, table, u.symbol, `${u.symbol}-${table}-${String(u.period).slice(0, 7)}.csv.gz`);
}
export function resultHeaderOf(u) {
  const table = u.dataset === 'aggTrades' ? 'of_1m' : 'tf_1m';
  const cols = table === 'of_1m' ? OF_COLS : TF_COLS;
  return cols.filter((c) => c !== 'exchange' && c !== 'symbol');
}
export async function writeResultGz({ file, header, rows }) {
  fs.mkdirSync(path.dirname(file), { recursive: true });
  const gz = zlib.createGzip();
  const out = fs.createWriteStream(file);
  gz.pipe(out);
  const finished = new Promise((res, rej) => { out.on('finish', res); out.on('error', rej); gz.on('error', rej); });
  gz.write(header.join(',') + '\n');
  for (const r of rows) gz.write(header.map((c) => String(r[c])).join(',') + '\n');
  gz.end();
  await finished;
  return fs.statSync(file).size;
}
export async function readResultGz(file) {
  const rl = readline.createInterface({ input: fs.createReadStream(file).pipe(zlib.createGunzip()), crlfDelay: Infinity });
  let header = null;
  const rows = [];
  for await (const line of rl) {
    if (!line) continue;
    if (!header) { header = line.split(','); continue; }
    const cells = line.split(',');
    const r = {};
    for (let i = 0; i < header.length; i++) r[header[i]] = cells[i];
    rows.push(r);
  }
  return { header, rows };
}

// ---------- 묶음 (§2-5) ----------
export function bundleNameOf({ market, dataset, symbol, year = null }) {
  return `${market}__${dataset}__${symbol}${year ? '__' + year : ''}.tar`;
}
export function drivePathOf({ market, dataset, bundle }) {
  return `${DRIVE_H8}/${market}/${dataset}/${bundle}`;
}
export function planBundles({ market, dataset, symbol, files }) {
  const total = files.reduce((s, f) => s + f.bytes, 0);
  if (total <= 4 * 1024 ** 3) {
    return [{ market, dataset, symbol, year: null, files, bytes: total, bundle: bundleNameOf({ market, dataset, symbol }) }];
  }
  const byYear = new Map();
  for (const f of files) {
    const y = String(f.period || '').slice(0, 4) || 'unknown';
    if (!byYear.has(y)) byYear.set(y, []);
    byYear.get(y).push(f);
  }
  return [...byYear.entries()].sort(([a], [b]) => (a < b ? -1 : 1)).map(([y, fs_]) => ({
    market, dataset, symbol, year: y, files: fs_,
    bytes: fs_.reduce((s, f) => s + f.bytes, 0),
    bundle: bundleNameOf({ market, dataset, symbol, year: y })
  }));
}

export function makeBundle({ files, tarPath, stageDir = STAGE, tmpDir = TMP }) {
  fs.mkdirSync(path.dirname(tarPath), { recursive: true });
  fs.mkdirSync(tmpDir, { recursive: true });
  const listFile = path.join(tmpDir, `h8-tar-${process.pid}-${Date.now()}.txt`);
  fs.writeFileSync(listFile, files.map((f) => f.name).join('\n') + '\n');
  fs.rmSync(tarPath, { force: true });
  try {
    const r = spawnSync('tar', ['-cf', tarPath, '-C', stageDir, '-T', listFile], { encoding: 'utf8' });
    if (r.status !== 0) throw new Error(`tar exit=${r.status} ${String(r.stderr).slice(0, 200)}`);
  } finally { fs.rmSync(listFile, { force: true }); }
  const bytes = fs.statSync(tarPath).size;
  return { tarPath, bytes, md5: md5FileSync(tarPath) };
}

function runCmd(cmd, args, { timeoutMs = 7200 * 1000 } = {}) {
  return new Promise((resolve, reject) => {
    const child = spawn(cmd, args, { stdio: ['ignore', 'pipe', 'pipe'] });
    let out = '', err = '';
    child.stdout.on('data', (d) => { out += d.toString('utf8'); });
    child.stderr.on('data', (d) => { err += d.toString('utf8'); });
    const timer = setTimeout(() => { try { child.kill('SIGTERM'); } catch {} }, timeoutMs);
    child.on('error', (e) => { clearTimeout(timer); reject(e); });
    child.on('close', (code) => {
      clearTimeout(timer);
      if (code !== 0) { const e = new Error(`${cmd} exit=${code} ${err.slice(0, 200)}`); e.code = code; reject(e); }
      else resolve(out);
    });
  });
}

async function remoteMd5(remotePath) {
  const out = await runCmd('rclone', ['md5sum', remotePath]);
  const line = out.split('\n').filter(Boolean)[0] ?? '';
  const v = line.split(/\s+/)[0]?.trim() ?? '';
  return /^[0-9a-f]{32}$/.test(v) ? v : null;
}

// ---------- 진행 ----------
class RunStats {
  constructor(step, totalPending) {
    this.step = step; this.t0 = Date.now(); this.done = 0; this.held = 0;
    this.total = totalPending; this.lastTick = Date.now(); this.lastDone = 0;
    this.heldStreak = 0; this.processed = 0; this.rateStreak = 0;
    this.c2ring = []; this.blocked = null;
  }
}

function startProgressTimer(stats) {
  const t = setInterval(() => {
    const hours = (Date.now() - stats.lastTick) / 3600000;
    const rate = hours > 0 ? (stats.done - stats.lastDone) / hours : 0;
    const left = Math.max(0, stats.total - stats.done - stats.held);
    const eta = rate > 0 ? left / rate : null;
    try {
      appendProgress({
        step: stats.step, result: 'progress',
        evidence: {
          done: stats.done, held: stats.held, left,
          units_per_h: Math.round(rate), eta_h: eta === null ? null : Number(eta.toFixed(1)),
          disk_gib: Number((diskFreeKb() / 1024 / 1024).toFixed(1))
        }
      });
    } catch (e) { log(`progress write fail: ${e.message}`); }
    stats.lastTick = Date.now(); stats.lastDone = stats.done;
  }, 3600 * 1000);
  t.unref?.();
  return t;
}

// ---------- 단위 처리 ----------
function classifyFailure(err, code) {
  const msg = String(err?.message ?? err);
  let reason = code;
  if (!reason) {
    if (err?.status === 403 || err?.status === 418 || err?.status === 429) reason = `rate_${err.status}`;
    else if (err?.status >= 500) reason = `http_${err.status}`;
    else if (/timeout|aborted|ECONN|fetch failed|socket/i.test(msg)) reason = 'network';
    else if (/C1 mismatch/.test(msg)) reason = 'c1_mismatch';
    else reason = 'error';
  }
  return { reason, detail: msg.slice(0, 300) };
}

async function downloadUnitFiles(u, { signal }) {
  const zip = path.join(STAGE, stageName(u));
  const cs = zip + '.CHECKSUM';
  const t0 = Date.now();
  const r = await withRetries(async () => {
    fs.rmSync(zip, { force: true });
    const d = await downloadKey(u.key, u.bytes, { dest: zip, signal });
    if (d.bytes !== u.bytes) throw new Error(`size ${d.bytes} != ${u.bytes}`);
    const c = await fetchChecksum(u.key, { signal });
    const got = await sha256File(zip);
    if (got !== c.sha) throw new Error(`C1 mismatch expected=${c.sha} got=${got}`);
    fs.writeFileSync(cs, `${c.sha}  ${path.basename(u.key)}\n`);
    return { sha: got, bytes: d.bytes };
  });
  if (!r.ok) return { ok: false, err: r.error, attempts: r.attempts, ms: Date.now() - t0 };
  return { ok: true, sha: r.value.sha, bytes: r.value.bytes, attempts: r.attempts, ms: Date.now() - t0 };
}

// class A: 받기 → C1 → CA → done (zip 은 묶음 C4 뒤에 지운다)
async function processClassAUnit(db, u, { signal, pre = null }) {
  const zip = path.join(STAGE, stageName(u));
  const d = pre ?? await downloadUnitFiles(u, { signal });
  if (!d.ok) return { held: true, ...classifyFailure(d.err) };
  setUnit(db, u.key, { state: 'downloaded', sha256: d.sha });
  const ca = await caCheckZip(zip, { period: u.period, dataset: u.dataset });
  const fields = { state: 'done', c2: ca.status, rows: ca.rows };
  if (KLINE_DATASETS.has(u.dataset) && ca.quote_sum !== null) fields.quote_sum = ca.quote_sum;
  setUnit(db, u.key, fields);
  return { done: true, ca: ca.status, rows: ca.rows };
}

// class B: 받기 → C1 → 집계 → C2(kline 다시 받기) → 결과 파일 → done → zip rm
// 자르기 바이트는 압축 푼 길이 기준이라 u.bytes(zip 크기)로 셈하지 않는다 (2026-10-23: 통째 중복은 아래 wholeDupCheck, N벌은 copiesCheck 값).
function aggOptsFor(u) {
  const o = {
    exchange: u.market === 'spot' ? 'binance' : 'binance-um',
    symbol: u.symbol, month: String(u.period).slice(0, 7)
  };
  if (u.dataset === 'trades') o.market = u.market;
  return o;
}

async function aggregateUnit(u, zip, extra = {}) {
  if (u.dataset === 'aggTrades') {
    const a = await aggInWorker('aggTrades', zip, { ...aggOptsFor(u), ...extra });
    return { rows: a.rows, out_of_range: a.out_of_range, time_reversed: a.time_reversed, placeholders: a.placeholders };
  }
  const a = await aggInWorker('trades', zip, { ...aggOptsFor(u), ...extra });
  return { rows: a.rows, out_of_range: a.out_of_range, time_reversed: a.time_reversed };
}

export function klineKeyOf(u) {
  const p = u.market === 'spot' ? 'spot' : 'futures/um';
  const m = String(u.period).slice(0, 7);
  return `data/${p}/monthly/klines/${u.symbol}/1m/${u.symbol}-1m-${m}.zip`;
}

async function ensureKlineZip(db, u, { signal }) {
  const key = klineKeyOf(u);
  const unit = getUnit(db, key);
  if (!unit || !unit.bytes) return { zip: null, sha: null };
  const zip = path.join(STAGE, 'kline__' + stageName(unit));
  const r = await withRetries(async () => {
    fs.rmSync(zip, { force: true });
    const d = await downloadKey(key, unit.bytes, { dest: zip, signal });
    if (d.bytes !== unit.bytes) throw new Error(`kline size ${d.bytes} != ${unit.bytes}`);
    const c = await fetchChecksum(key, { signal });
    const got = await sha256File(zip);
    if (got !== c.sha) throw new Error(`kline C1 mismatch ${got} != ${c.sha}`);
    return { sha: got };
  });
  if (!r.ok) throw r.error;
  return { zip, sha: r.value.sha };
}

async function processClassBUnit(db, u, { signal, c3 = false, pre = null, metered = false }) {
  const zip = path.join(STAGE, stageName(u));
  const d = pre ?? await downloadUnitFiles(u, { signal });
  if (!d.ok) return { held: true, ...classifyFailure(d.err) };
  setUnit(db, u.key, { state: 'downloaded', sha256: d.sha });

  let pickCut = {};
  const pick = PICK_COPY.get(u.key);
  if (pick) {
    const c = await copiesCheck(zip, pick.copies);
    const ok = c.ok && c.lines.every((n) => n === pick.copy_lines) && c.md5.every((m, i) => m === (i === pick.pick - 1 ? pick.pick_md5 : pick.other_md5));
    if (!ok) return { held: true, reason: 'agg_error', detail: `pick_copy mismatch total=${c.total} lines=${JSON.stringify(c.lines)}` };
    pickCut = { skipBytes: (pick.pick - 1) * c.copy_bytes, limitBytes: pick.pick === pick.copies ? null : c.copy_bytes };
  }
  let a = null, lastErr = null;
  for (let attempt = 1; attempt <= 2 && !a; attempt++) {
    try { a = await aggregateUnit(u, zip, pickCut); }
    catch (e) { lastErr = e; }
  }
  if (!a || a.time_reversed > 0) {
    const dup = await wholeDupCheck(zip).catch(() => ({ dup: false }));
    if (dup.dup) {
      const half = dup.half_bytes;
      try {
        a = await aggregateUnit(u, zip, { limitBytes: half });
        db.prepare('INSERT INTO source_events(key,kind,detail,at) VALUES (?,?,?,?)')
          .run(u.key, 'whole_dup', JSON.stringify({ half_bytes: half, half_lines: dup.half_lines }), nowIso());
      } catch (e) { return { held: true, reason: 'agg_error', detail: String(e.message).slice(0, 300) }; }
    } else if (!a) {
      return { held: true, reason: 'agg_error', detail: String(lastErr?.message ?? lastErr).slice(0, 300) };
    }
  }
  if (a.placeholders > 0) {
    db.prepare('INSERT INTO source_events(key,kind,detail,at) VALUES (?,?,?,?)')
      .run(u.key, 'placeholder_rows', String(a.placeholders), nowIso());
  }

  let c2 = { status: 'no_kline' };
  let klineZip = null;
  try {
    const k = await ensureKlineZip(db, u, { signal });
    klineZip = k.zip;
    const klUnit = getUnit(db, klineKeyOf(u));
    if (!k.zip || !klUnit || !klUnit.sha256) c2 = { status: 'no_kline' };
    else if (k.sha !== klUnit.sha256) c2 = { status: 'SHA_DIFF', detail: `kline ${k.sha} != 원장 ${klUnit.sha256}` };
    else {
      const kl = await readKline1m(k.zip);
      if (u.dataset === 'aggTrades') {
        const j = judgeC2({ rows: a.rows, kline: kl.byMinute, out_of_range: a.out_of_range, time_reversed: a.time_reversed, market: u.market });
        c2 = { status: j.status, judge: j };
      } else {
        const j = judgeC2X({ rows: a.rows, kline: kl.byMinute, out_of_range: a.out_of_range, time_reversed: a.time_reversed, known: GAP_BY_KEY.get(u.key) ?? [] });
        c2 = { status: j.status, judge: j };
      }
    }
  } catch (e) {
    c2 = { status: 'kline_error', detail: String(e.message).slice(0, 200) };
  }
  if (c2.status === 'SHA_DIFF') {
    fs.rmSync(zip, { force: true });
    if (klineZip) fs.rmSync(klineZip, { force: true });
    if (metered) bZipRemoved(u);
    return { held: true, reason: 'c2_kline_sha', detail: c2.detail };
  }

  const outFile = resultPathOf(u);
  const outBytes = await writeResultGz({ file: outFile, header: resultHeaderOf(u), rows: a.rows });
  setUnit(db, u.key, {
    state: 'done', rows: a.rows.length, c2: c2.status,
    out_file: path.relative(ROOT, outFile), out_bytes: outBytes
  });
  let c3res = null;
  if (c3) {
    try { c3res = await runC3(db, u, zip); }
    catch (e) { c3res = { ok: false, error: String(e.message).slice(0, 200) }; }
    setUnit(db, u.key, { c3: c3res.ok ? 'ok' : 'DIFF' });
  }
  fs.rmSync(zip, { force: true });
  if (klineZip) fs.rmSync(klineZip, { force: true });
  if (metered) bZipRemoved(u);
  return { done: true, c2: c2.status, rows: a.rows.length, judge: c2.judge, c3: c3res };
}

// ---------- C3 ----------
export function c3Selected(sha256, key, firstKeys) {
  if (sha256 && /^0[0-3]/.test(sha256)) return 'sha_prefix';
  if (firstKeys.has(key)) return 'first3';
  return null;
}
export function buildFirstKeys(db) {
  const set = new Set();
  for (const ds of ['aggTrades', 'trades']) {
    for (const m of ['spot', 'um']) {
      for (const r of db.prepare("SELECT key FROM units WHERE class='B' AND dataset=? AND market=? ORDER BY key LIMIT 3").all(ds, m)) set.add(r.key);
    }
  }
  return set;
}
async function runC3(db, u, zip) {
  const { rows } = await readResultGz(path.join(ROOT, getUnit(db, u.key).out_file));
  const byMin = new Map(rows.map((r) => [Number(r.minute), r]));
  if (u.dataset === 'aggTrades') {
    const pick = PICK_COPY.get(u.key);
    const r = await recomputeFromZip(zip, u.sha256, {
      exchange: u.market === 'spot' ? 'binance' : 'binance-um', symbol: u.symbol, month: String(u.period).slice(0, 7),
      skipLines: pick ? pick.copy_lines * (pick.pick - 1) : 0
    });
    const diffs = [];
    for (const rec of r.rows) {
      if (!rec) continue;
      const mine = byMin.get(rec.minute);
      if (!mine) { diffs.push({ minute: rec.minute, field: '*' }); continue; }
      for (const f of Object.keys(rec)) {
        if (f === 'exchange' || f === 'symbol') continue;
        const a = String(rec[f]), b = String(mine[f]);
        if (a !== b && compareDecimal(a, b) !== 0) diffs.push({ minute: rec.minute, field: f, recomputed: a, ours: b });
      }
    }
    return { ok: diffs.length === 0, minutes: r.minutes, diffs: diffs.slice(0, 10), total_diffs: diffs.length };
  }
  const minutes = [...byMin.keys()];
  // 통째 중복 원본은 집계처럼 앞 절반 줄만 재검산한다. 분이 3개 미만인 단위는 있는 분만 본다(2026-10-25).
  const wd = db.prepare("SELECT detail FROM source_events WHERE key=? AND kind='whole_dup' ORDER BY rowid DESC LIMIT 1").get(u.key);
  const maxLines = wd ? JSON.parse(wd.detail).half_lines : null;
  const r = await recheckTradesZip(zip, u.sha256, { market: u.market, maxLines }, minutes, (ms) => ms.map((m) => byMin.get(m)).filter(Boolean));
  const ok = r.diffs.length === 0 && r.minutes.length === Math.min(3, minutes.length);
  return { ok, minutes: r.minutes, diffs: r.diffs.slice(0, 10), total_diffs: r.diffs.length };
}

// ---------- 묶음 ----------
function groupReady(db, { market, dataset, symbol, cls }) {
  const row = db.prepare(`SELECT COUNT(*) n FROM units WHERE market=? AND dataset=? AND symbol=? AND class=?
    AND state NOT IN (${NOT_DONE_SQL})`).get(market, dataset, symbol, cls, ...DONE_STATES);
  return row.n === 0;
}

function unitsTsvFor(db, units) {
  const head = ['key', 'sha256', 'bytes', 'rows', 'c2', 'c3', 'held_reason', 'source_events'].join('\t');
  const ev = db.prepare('SELECT kind,COUNT(*) n FROM source_events WHERE key=? GROUP BY kind');
  const lines = [head];
  for (const u of units) {
    const events = ev.all(u.key).map((e) => `${e.kind}:${e.n}`).join(',');
    lines.push([u.key, u.sha256 ?? '', u.bytes, u.rows ?? '', u.c2 ?? '', u.c3 ?? '', u.held_reason ?? '', events].join('\t'));
  }
  return lines.join('\n') + '\n';
}

async function bundleGroup(db, { market, dataset, symbol, cls }, { uploadQueue, signal }) {
  const units = db.prepare('SELECT * FROM units WHERE market=? AND dataset=? AND symbol=? AND bundle IS NULL ORDER BY key')
    .all(market, dataset, symbol);
  if (!units.length) return;
  const files = [];
  if (cls === 'A') {
    for (const u of units) {
      if (u.state === 'held') continue;
      const zipName = stageName(u);
      const csName = zipName + '.CHECKSUM';
      for (const name of [zipName, csName]) {
        const p = path.join(STAGE, name);
        if (!fs.existsSync(p)) {
          // 재개 등으로 stage 가 비었으면 다시 받는다(원본 zip 만).
          const d = await downloadUnitFiles(u, { signal });
          if (!d.ok) throw new Error(`stage missing ${name} re-download fail`);
        }
        files.push({ name, bytes: fs.statSync(p).size, period: u.period, key: u.key });
      }
    }
    if (!files.length) return;
  } else {
    for (const u of units) {
      if (u.state === 'held' || !u.out_file) continue;
      const p = path.join(ROOT, u.out_file);
      if (!fs.existsSync(p)) throw new Error(`out missing ${u.out_file}`);
      files.push({ name: path.basename(p), bytes: fs.statSync(p).size, period: u.period, key: u.key, abs: p });
    }
    fs.mkdirSync(TMP, { recursive: true });
    const tmp = path.join(TMP, `h8-units-${process.pid}-${Date.now()}-${symbol}.tsv`);
    fs.writeFileSync(tmp, unitsTsvFor(db, units));
    files.push({ name: 'units.tsv', bytes: fs.statSync(tmp).size, period: '', key: '', tmp });
  }
  for (const plan of planBundles({ market, dataset, symbol, files })) uploadQueue.push({ plan, cls });
}

// 올리기 1단계: 묶기 + copyto. 검증은 verifyBundle 이 따로 맡는다(2026-10-22 최적화).
// 반환: {blocked} 이면 끝, {recopy} 면 copy 재시도, {verify} 면 검증 줄로.
async function copyBundle(db, job) {
  const { plan, cls, attempt = 1 } = job;
  const tarDir = path.join(TMP, 'h8_tar');
  fs.mkdirSync(tarDir, { recursive: true });
  const tarPath = path.join(tarDir, plan.bundle);
  let built;
  if (cls === 'A') {
    built = makeBundle({ files: plan.files, tarPath });
  } else {
    const stageTmp = path.join(tarDir, `b-${process.pid}-${Date.now()}`);
    fs.mkdirSync(stageTmp, { recursive: true });
    for (const f of plan.files) fs.copyFileSync(f.tmp ?? f.abs, path.join(stageTmp, f.name));
    built = makeBundle({ files: plan.files, tarPath, stageDir: stageTmp });
    fs.rmSync(stageTmp, { recursive: true, force: true });
  }
  const drivePath = drivePathOf(plan);
  db.prepare(`INSERT OR REPLACE INTO bundles(bundle,market,dataset,symbol,files,bytes,local_md5,remote_md5,drive_path,state,error,updated_at)
    VALUES (?,?,?,?,?,?,?,?,?,?,?,?)`).run(plan.bundle, plan.market, plan.dataset, plan.symbol, plan.files.length,
    built.bytes, built.md5, null, drivePath, 'built', null, nowIso());
  db.prepare(`UPDATE units SET state='bundled', bundle=?, updated_at=? WHERE market=? AND dataset=? AND symbol=? AND state='done'`)
    .run(plan.bundle, nowIso(), plan.market, plan.dataset, plan.symbol);

  try { await runCmd('rclone', ['copyto', tarPath, drivePath, '--retries', '5', '--low-level-retries', '10']); }
  catch (e) {
    db.prepare('UPDATE bundles SET error=?, updated_at=? WHERE bundle=?').run(`upload ${String(e.message).slice(0, 120)}`, nowIso(), plan.bundle);
    if (e.code === 75) return { blocked: 'rclone_guard', bundle: plan.bundle };
    if (attempt < 3) return { recopy: { plan, cls, attempt: attempt + 1 } };
    db.prepare('UPDATE bundles SET state=?, error=?, updated_at=? WHERE bundle=?').run('failed', 'c4_3_fail', nowIso(), plan.bundle);
    return { blocked: 'c4_3_fail', bundle: plan.bundle };
  }
  return { verify: { plan, cls, attempt, built, tarPath } };
}

// 올리기 2단계: 원격 md5 확인 → 맞으면 업로드 완료·로컬 삭제, 틀리면 copy 재시도.
async function verifyBundle(db, job) {
  const { plan, cls, attempt, built, tarPath } = job;
  const drivePath = drivePathOf(plan);
  const rh = await remoteMd5(drivePath).catch(() => null);
  if (rh && rh === built.md5) {
    db.prepare('UPDATE bundles SET state=?, remote_md5=?, error=NULL, updated_at=? WHERE bundle=?').run('uploaded', built.md5, nowIso(), plan.bundle);
    fs.rmSync(tarPath, { force: true });
    for (const f of plan.files) {
      const p = f.tmp ?? f.abs ?? path.join(STAGE, f.name);
      fs.rmSync(p, { force: true });
    }
    db.prepare(`UPDATE units SET state='local_deleted', updated_at=? WHERE market=? AND dataset=? AND symbol=? AND state IN ('bundled','uploaded')`)
      .run(nowIso(), plan.market, plan.dataset, plan.symbol);
    if (cls === 'A') meter.aBytes = Math.max(0, meter.aBytes - built.bytes);
    return { ok: true, bundle: plan.bundle, bytes: built.bytes };
  }
  db.prepare('UPDATE bundles SET error=?, updated_at=? WHERE bundle=?').run(`md5 mismatch ${rh}`, nowIso(), plan.bundle);
  if (attempt < 3) return { requeue: { plan, cls, attempt: attempt + 1 } };
  db.prepare('UPDATE bundles SET state=?, error=?, updated_at=? WHERE bundle=?').run('failed', 'c4_3_fail', nowIso(), plan.bundle);
  return { blocked: 'c4_3_fail', bundle: plan.bundle };
}

// ---------- 파이프라인 ----------
class AsyncQueue {
  constructor() { this.items = []; this.waiters = []; this.closed = false; }
  push(v) {
    if (this.closed) throw new Error('push after close');
    const w = this.waiters.shift();
    if (w) w({ value: v, done: false }); else this.items.push(v);
  }
  close() { this.closed = true; while (this.waiters.length) this.waiters.shift()({ done: true }); }
  next() {
    if (this.items.length) return Promise.resolve({ value: this.items.shift(), done: false });
    if (this.closed) return Promise.resolve({ done: true });
    return new Promise((r) => this.waiters.push(r));
  }
}

// 대기실 계량(§0-6). class B zip 은 집계 수+1·바이트 상한, class A 는 2 GiB.
const meter = { aBytes: 0, bCount: 0, inflightB: 0, bBytes: 0 };
export function resetMeter() { meter.aBytes = 0; meter.bCount = 0; meter.inflightB = 0; meter.bBytes = 0; }
export function readMeter() { return { ...meter }; }
// class B 받기 뒤 계량 정리: 성공은 bCount++, 실패는 대기실 바이트 반환.
export function bDownloadSettled(u, ok) {
  meter.inflightB--;
  if (ok) meter.bCount++;
  else meter.bBytes = Math.max(0, meter.bBytes - (u.bytes ?? 0));
}
// class B consumer 가 집계를 시작하면 대기 계수를 줄인다.
export function bConsumeOne() { meter.bCount--; }
// class B zip 이 지워지면 대기실 바이트를 반환한다.
export function bZipRemoved(u) { meter.bBytes = Math.max(0, meter.bBytes - (u.bytes ?? 0)); }

export async function reserveStage(cls, u, aggConns, stageGiB) {
  const bCap = (stageGiB ?? 6) * 1024 ** 3;
  for (;;) {
    if (stopRequested()) return false;
    if (cls === 'A') {
      if (meter.aBytes + u.bytes <= 2 * 1024 ** 3) { meter.aBytes += u.bytes; return true; }
    } else if (meter.bCount + meter.inflightB < aggConns + 1
        && (meter.bBytes + u.bytes <= bCap || meter.bBytes === 0)) {
      meter.inflightB++; meter.bBytes += u.bytes; return true;
    }
    await sleep(500);
  }
}

async function processGroup(db, group, stats, opts) {
  const { uploadQueue } = opts;
  const cls = group.cls;
  if (cls !== 'A' && cls !== 'B') throw new Error(`group cls missing ${group.market}/${group.dataset}/${group.symbol}`);
  // 원격에 맡긴 단위(state='remote')는 맥이 건드리지 않는다. 그룹 목록은 단계 시작 때 만들어져서 그 뒤 lease 된 단위가 섞일 수 있다.
  let units = db.prepare(`SELECT * FROM units WHERE market=? AND dataset=? AND symbol=? AND class=?
    AND state IN (${PENDING_STATES.map(() => '?').join(',')}) ORDER BY key`)
    .all(group.market, group.dataset, group.symbol, cls, ...PENDING_STATES);
  if (opts.only) units = units.filter((u) => opts.only.has(u.key));
  if (!units.length) return;

  const aggConns = Math.max(1, opts.agg);
  const dlConns = Math.max(1, opts.dl);
  const stageGiB = Math.max(0, opts.stageGib ?? 6);
  const ready = new AsyncQueue();
  let nextIdx = 0;

  const downloader = async () => {
    for (;;) {
      if (opts.signal?.aborted || stopRequested()) return;
      const i = nextIdx++;
      if (i >= units.length) return;
      const u = units[i];
      const okGate = await reserveStage(cls, u, aggConns, stageGiB);
      if (!okGate) { nextIdx = units.length; return; }
      let res;
      try { res = await downloadUnitFiles(u, { signal: opts.signal }); }
      catch (e) { res = { ok: false, err: e }; }
      if (cls === 'B') bDownloadSettled(u, res.ok);
      else if (!res.ok) meter.aBytes -= u.bytes;
      ready.push({ u, ...res });
    }
  };

  const consumer = async () => {
    for (;;) {
      const it = await ready.next();
      if (it.done) return;
      const { u, ok, err } = it.value;
      if (cls === 'B') bConsumeOne();
      if (!ok) { finishHeld(db, u, stats, classifyFailure(err)); continue; }
      let r;
      try {
        r = cls === 'A' ? await processClassAUnit(db, u, { signal: opts.signal, pre: it.value }) : await processClassBUnit(db, u, { signal: opts.signal, c3: opts.c3?.has(u.key), pre: it.value, metered: true });
      } catch (e) {
        r = { held: true, ...classifyFailure(e) };
      }
      if (r.held) finishHeld(db, u, stats, r);
      else {
        stats.done++; stats.heldStreak = 0; stats.processed++;
        log(`DONE ${u.key} ${JSON.stringify(cls === 'A' ? { ca: r.ca } : { c2: r.c2, rows: r.rows })}`);
        if (cls === 'B') {
          stats.c2ring.push(r.c2);
          if (stats.c2ring.length > 200) stats.c2ring.shift();
          if (stats.c2ring.length >= 200 && stats.c2ring.filter((s) => s === 'FAIL').length > 100) stats.blocked = 'c2x_fail_rate';
          if (r.c3 && !r.c3.ok) stats.blocked = `c3_diff ${u.key}`;
        }
      }
    }
  };

  const dls = Array.from({ length: dlConns }, () => downloader());
  const cons = Array.from({ length: aggConns }, () => consumer());
  await Promise.all(dls);
  ready.close();
  await Promise.all(cons);

  if (!opts.only && groupReady(db, { ...group, cls })) {
    const key = `${group.market}__${group.dataset}__${group.symbol}`;
    if (!opts.bundled.has(key)) {
      opts.bundled.add(key);
      await bundleGroup(db, { ...group, cls }, { uploadQueue, signal: opts.signal });
    }
  }
}

function finishHeld(db, u, stats, info) {
  const { reason, detail } = info;
  setUnit(db, u.key, { state: 'held', held_reason: reason });
  db.prepare('INSERT INTO source_events(key,kind,detail,at) VALUES (?,?,?,?)')
    .run(u.key, `held:${reason}`, String(detail ?? '').slice(0, 300), nowIso());
  stats.held++; stats.heldStreak++; stats.processed++;
  log(`HELD ${u.key} reason=${reason} ${String(detail ?? '').slice(0, 120)}`);
  if (['403', '418', '429'].some((c) => reason === `rate_${c}`)) stats.rateStreak++;
  else stats.rateStreak = 0;
  if (stats.heldStreak >= 20) stats.blocked = 'held_20';
  if (stats.processed >= 100 && stats.held / stats.processed > 0.02) stats.blocked = 'held_2pct';
  if (stats.rateStreak >= 3) stats.blocked = 'rate_limit_3';
}

// 그룹을 gpar 개까지 동시에 돈다. 새 그룹을 꺼내기 전에 blocked/STOP/벽시계/디스크 바닥을 검사한다(§opt13 1-1).
export async function runGroupPool(groups, {
  gpar = 1, stats, step, stepT0, capMs,
  processGroup, stopFile = STOP_FILE, logFn = log, progressLineFn = progressLine
}) {
  let gIdx = 0;
  const acquire = async () => {
    for (;;) {
      if (stats.blocked || stopRequested(stopFile)) return null;
      if (Date.now() - stepT0 > capMs) { stats.blocked = 'wallclock'; return null; }
      if (diskFreeKb() < DISK_FLOOR_KB) {
        progressLineFn(step, 'disk_wait', { disk_gib: Number((diskFreeKb() / 1024 / 1024).toFixed(1)) });
        const t0 = Date.now();
        while (diskFreeKb() < DISK_FLOOR_KB) {
          if (Date.now() - t0 > 3 * 3600 * 1000) { stats.blocked = 'disk'; break; }
          await sleep(30000);
        }
        if (stats.blocked) return null;
        continue;
      }
      break;
    }
    return groups[gIdx++] ?? null;
  };
  const workers = Array.from({ length: gpar }, async () => {
    for (;;) {
      const g = await acquire();
      if (!g) return;
      try {
        await processGroup(g);
      } catch (e) {
        logFn(`GROUP ERROR ${g.market}/${g.dataset}/${g.symbol} ${e.message}`);
        stats.blocked = `group_error ${String(e.message).slice(0, 100)}`;
      }
    }
  });
  await Promise.all(workers);
}

// ---------- 단계 ----------
function groupsForS3(db) {
  const rows = db.prepare(`SELECT market,dataset,symbol,COUNT(*) n FROM units
    WHERE class='A' AND state IN (${PENDING_STATES.map(() => '?').join(',')})
    GROUP BY market,dataset,symbol`).all(...PENDING_STATES);
  rows.sort((a, b) => s3Rank(a.market, a.dataset) - s3Rank(b.market, b.dataset)
    || (a.market < b.market ? -1 : a.market > b.market ? 1 : 0)
    || (a.symbol < b.symbol ? -1 : a.symbol > b.symbol ? 1 : 0));
  return rows.map((r) => ({ ...r, cls: 'A' }));
}

export async function buildRank(db) {
  const rows = db.prepare(`SELECT market,symbol,quote_sum FROM units
    WHERE dataset='klines' AND period >= '2025-10' AND period <= '2026-09' AND quote_sum IS NOT NULL`).all();
  const sum = new Map();
  for (const r of rows) {
    const k = `${r.market}|${r.symbol}`;
    sum.set(k, sum.has(k) ? addDecimal(sum.get(k), r.quote_sum) : r.quote_sum);
  }
  const lines = ['market\trank\tsymbol\tquote_sum'];
  for (const m of ['spot', 'um']) {
    const have = [...sum.entries()].filter(([k]) => k.startsWith(m + '|'))
      .map(([k, v]) => ({ symbol: k.split('|')[1], q: v }))
      .sort((a, b) => compareDecimal(b.q, a.q) || (a.symbol < b.symbol ? -1 : 1));
    const symbols = db.prepare("SELECT DISTINCT symbol FROM units WHERE class='B' AND market=?").all(m).map((r) => r.symbol);
    const haveSet = new Set(have.map((h) => h.symbol));
    const rest = symbols.filter((s) => !haveSet.has(s)).sort();
    let rank = 1;
    for (const h of have) lines.push(`${m}\t${rank++}\t${h.symbol}\t${h.q}`);
    for (const s of rest) lines.push(`${m}\t${rank++}\t${s}\t`);
  }
  fs.mkdirSync(path.dirname(RANK_TSV), { recursive: true });
  fs.writeFileSync(RANK_TSV, lines.join('\n') + '\n');
  return { rows: lines.length - 1 };
}

function readRank() {
  const out = new Map();
  if (!fs.existsSync(RANK_TSV)) return out;
  for (const l of fs.readFileSync(RANK_TSV, 'utf8').split('\n').filter(Boolean).slice(1)) {
    const [market, rank, symbol] = l.split('\t');
    out.set(`${market}|${symbol}`, Number(rank));
  }
  return out;
}

function groupsForB(db, dataset) {
  const rows = db.prepare(`SELECT market,symbol,COUNT(*) n FROM units
    WHERE class='B' AND dataset=? AND state IN (${PENDING_STATES.map(() => '?').join(',')})
    GROUP BY market,symbol`).all(dataset, ...PENDING_STATES);
  const rank = readRank();
  const order = (m) => (m === 'spot' ? 0 : 1);
  rows.sort((a, b) => order(a.market) - order(b.market)
    || (rank.get(`${a.market}|${a.symbol}`) ?? 1e9) - (rank.get(`${b.market}|${b.symbol}`) ?? 1e9)
    || (a.symbol < b.symbol ? -1 : 1));
  return rows.map((r) => ({ ...r, dataset, cls: 'B' }));
}

// 이미 다 끝났지만 묶음이 없는 그룹(재개 때)
function bundleOnlyGroups(db, cls, dataset) {
  const rows = db.prepare(`SELECT market,dataset,symbol FROM units
    WHERE class=? AND state='done' ${dataset ? 'AND dataset=?' : ''} AND bundle IS NULL
    GROUP BY market,dataset,symbol`).all(cls, ...(dataset ? [dataset] : []));
  // 아직 안 끝난 단위(remote, planned 등)가 남은 그룹은 반쪽만 묶이므로 뺀다.
  return rows.map((r) => ({ ...r, cls })).filter((g) => groupReady(db, g));
}

const sameNum = (a, b) => {
  if (String(a) === String(b)) return true;
  if (/^-?\d+(\.\d+)?$/.test(String(a)) && /^-?\d+(\.\d+)?$/.test(String(b))) return compareDecimal(String(a), String(b)) === 0;
  return false;
};

const K1_LO = Date.UTC(2026, 7, 1), K1_HI = Date.UTC(2026, 8, 1);
export async function k1Check(db, kind = null) {
  const ofdb = new DatabaseSync(path.join(ROOT, 'db/orderflow.db'), { readOnly: true });
  const series = ofdb.prepare("SELECT DISTINCT exchange,market,symbol,kind FROM units WHERE month='2026-08' ORDER BY kind,exchange,symbol").all();
  const results = [];
  for (const s of series) {
    if (kind && s.kind !== kind) continue;
    const dataset = s.kind;
    const table = s.kind === 'aggTrades' ? 'of_1m' : 'tf_1m';
    const prefix = s.market === 'spot' ? 'data/spot' : 'data/futures/um';
    const key = `${prefix}/monthly/${dataset}/${s.symbol}/${s.symbol}-${dataset}-2026-08.zip`;
    const u = getUnit(db, key);
    if (!u) { results.push({ key, ok: false, error: 'unit 없음' }); continue; }
    const p = u.out_file ? path.join(ROOT, u.out_file) : null;
    if (!p || !fs.existsSync(p)) { results.push({ key, ok: false, error: '결과 파일 없음' }); continue; }
    const mine = await readResultGz(p);
    const ref = ofdb.prepare(`SELECT * FROM ${table} WHERE exchange=? AND symbol=? AND minute>=? AND minute<?`).all(s.exchange, s.symbol, K1_LO, K1_HI);
    const byMin = new Map(ref.map((r) => [Number(r.minute), r]));
    const diffs = [];
    if (byMin.size !== mine.rows.length) diffs.push({ field: 'row_count', ours: mine.rows.length, ref: byMin.size });
    for (const r of mine.rows) {
      const o = byMin.get(Number(r.minute));
      if (!o) { diffs.push({ minute: r.minute, field: '*' }); continue; }
      for (const c of mine.header) {
        if (o[c] === undefined) continue;
        if (!sameNum(r[c], o[c])) diffs.push({ minute: r.minute, field: c, ours: r[c], ref: o[c] });
      }
    }
    results.push({ key, ok: diffs.length === 0, diffs: diffs.slice(0, 10), total_diffs: diffs.length, rows: mine.rows.length });
  }
  ofdb.close();
  return results;
}

async function ensureK1(db, opts) {
  const need = db.prepare(`SELECT key FROM units WHERE class='B' AND dataset='aggTrades' AND period='2026-08'
    AND state NOT IN (${NOT_DONE_SQL})`).all(...DONE_STATES);
  if (!need.length) {
    // 재개: 2026-08 결과는 올린 뒤 지워지므로, 이미 통과한 K1 기록이 있으면 그것을 쓴다(2026-10-23).
    const passed = fs.existsSync(PROGRESS) && fs.readFileSync(PROGRESS, 'utf8').split('\n')
      .some((l) => l.includes('"step":"S4","result":"k1_ok"') && l.includes('"bad":0'));
    if (passed) return { ok: true, units: 0, bad: [], prior: true };
    const k1 = await k1Check(db, 'aggTrades');
    const bad = k1.filter((r) => !r.ok);
    return { ok: bad.length === 0, units: k1.length, bad };
  }
  const stats = new RunStats('S4', need.length);
  const queue = new AsyncQueue();
  queue.close(); // 묶음 없음
  progressLine('S4', 'k1_start', { units: need.length });
  for (const { key } of need) {
    const u = getUnit(db, key);
    let r;
    try { r = await processClassBUnit(db, u, { signal: opts.signal }); }
    catch (e) { r = { held: true, ...classifyFailure(e) }; }
    if (r.held) finishHeld(db, u, stats, r);
  }
  const k1 = await k1Check(db, 'aggTrades');
  const bad = k1.filter((r) => !r.ok);
  progressLine('S4', bad.length ? 'k1_fail' : 'k1_ok', { units: k1.length, bad: bad.length, bad_keys: bad.slice(0, 5).map((b) => b.key) });
  return { ok: bad.length === 0, units: k1.length, bad };
}

async function runStep(step, opts) {
  const db = opts.db;
  mkdirs();
  const groups = step === 'S3' ? groupsForS3(db) : groupsForB(db, step === 'S4' ? 'aggTrades' : 'trades');
  const pending = groups.reduce((s, g) => s + g.n, 0);
  const stats = new RunStats(step, pending);
  opts.stats = stats;
  const timer = startProgressTimer(stats);
  const uploadQueue = new AsyncQueue();
  const verifyQueue = new AsyncQueue();
  const uploads = [];
  const failJob = (job, msg) => {
    const r = db.prepare(`UPDATE bundles SET state='failed',error=?,updated_at=? WHERE bundle=? AND state<>'uploaded'`)
      .run(String(msg).slice(0, 200), nowIso(), job.plan.bundle);
    if (r.changes === 0) return; // 이미 업로드된 묶음은 건드리지 않는다(중복 묶기 경합 방어)
    uploads.push({ blocked: 'bundle_error', error: String(msg).slice(0, 200) });
  };
  const copyWorkers = Array.from({ length: UP_CONC }, async () => {
    for (;;) {
      const it = await uploadQueue.next();
      if (it.done) return;
      try {
        const r = await copyBundle(db, it.value);
        if (r.blocked) uploads.push(r);
        else if (r.recopy) uploadQueue.push(r.recopy);
        else verifyQueue.push(r.verify);
      } catch (e) { failJob(it.value, e.message); }
    }
  });
  const verifyWorkers = Array.from({ length: VERIFY_CONC }, async () => {
    for (;;) {
      const it = await verifyQueue.next();
      if (it.done) return;
      try {
        const r = await verifyBundle(db, it.value);
        if (r.requeue) {
          if (uploadQueue.closed) {
            db.prepare('UPDATE bundles SET state=?, error=?, updated_at=? WHERE bundle=?').run('failed', 'c4_3_fail', nowIso(), it.value.plan.bundle);
            uploads.push({ blocked: 'c4_3_fail', bundle: it.value.plan.bundle });
          } else uploadQueue.push(r.requeue);
        } else uploads.push(r);
      } catch (e) { failJob(it.value, e.message); }
    }
  });

  progressLine(step, 'start', { groups: groups.length, units: pending, dl: opts.dl, agg: opts.agg, cap_h: STEP_CAP_H[step] });
  const bundled = new Set();
  const c3set = new Set();
  if (step !== 'S3') {
    const first = buildFirstKeys(db);
    const sample = db.prepare("SELECT key FROM units WHERE class='B' AND dataset=? AND substr(sha256,1,2) IN ('00','01','02','03')").all(step === 'S4' ? 'aggTrades' : 'trades');
    for (const r of sample) c3set.add(r.key);
    for (const k of first) c3set.add(k);
  }
  const stepT0 = Date.now();
  const capMs = STEP_CAP_H[step] * 3600 * 1000;

  // 재개: 끝났지만 묶음이 없는 그룹부터 묶는다. processGroup 이 같은 그룹을 또 묶지 않게 표시한다.
  for (const g of bundleOnlyGroups(db, step === 'S3' ? 'A' : 'B', step === 'S3' ? null : (step === 'S4' ? 'aggTrades' : 'trades'))) {
    if (stopRequested()) break;
    try { await bundleGroup(db, g, { uploadQueue, signal: opts.signal }); bundled.add(`${g.market}__${g.dataset}__${g.symbol}`); }
    catch (e) { log(`bundle-only error ${g.market}/${g.dataset}/${g.symbol} ${e.message}`); }
  }

  await runGroupPool(groups, {
    gpar: Math.max(1, opts.gpar ?? 1),
    stats, step, stepT0, capMs,
    processGroup: (g) => processGroup(db, g, stats, { ...opts, uploadQueue, bundled, c3: c3set })
  });
  uploadQueue.close();
  await Promise.all(copyWorkers);
  verifyQueue.close();
  await Promise.all(verifyWorkers);
  clearInterval(timer);

  const bad = uploads.find((u) => u.blocked);
  if (bad) stats.blocked = bad.blocked;

  progressLine(step, 'end', {
    done: stats.done, held: stats.held, elapsed_s: Math.round((Date.now() - stepT0) / 1000),
    blocked: stats.blocked ?? null, uploads_ok: uploads.filter((u) => u.ok).length
  });
  return { step, done: stats.done, held: stats.held, blocked: stats.blocked, uploads_ok: uploads.filter((u) => u.ok).length };
}

function mkdirs() {
  for (const d of [STAGE, OUTDIR, TMP, path.join(ROOT, 'evidence/h8')]) fs.mkdirSync(d, { recursive: true });
}

function stepPending(db, step) {
  const cls = step === 'S3' ? 'A' : 'B';
  const extra = step === 'S3' ? '' : ` AND dataset='${step === 'S4' ? 'aggTrades' : 'trades'}'`;
  return db.prepare(`SELECT COUNT(*) n FROM units WHERE class='${cls}'${extra}
    AND state NOT IN (${NOT_DONE_SQL})`).get(...DONE_STATES).n;
}

export async function runSteps(steps, opts) {
  const db = opts.db ?? openLedger();
  const out = [];
  resetMeter();
  try {
    for (const f of fs.readdirSync(STAGE)) meter.aBytes += fs.statSync(path.join(STAGE, f)).size;
  } catch {}
  for (const step of steps) {
    if (opts.signal?.aborted) break;
    const left = stepPending(db, step);
    if (left === 0) {
      progressLine(step, 'end', { note: 'already complete' });
      out.push({ step, done: 0, held: 0, blocked: null, note: 'already complete' });
      if (step === 'S3' && !fs.existsSync(RANK_TSV)) await buildRank(db);
      continue;
    }
    if (step === 'S4') {
      const k1 = await ensureK1(db, opts);
      if (!k1.ok) { out.push({ step, blocked: 'k1_mismatch', k1: k1.bad }); break; }
    }
    const r = await runStep(step, { ...opts, db });
    out.push(r);
    if (r.blocked) break;
    if (step === 'S3') {
      const leftA = db.prepare("SELECT COUNT(*) n FROM units WHERE class='A' AND state NOT IN ('local_deleted','held')").get().n;
      if (leftA > 0) { out.push({ step, blocked: 's3_left', left: leftA }); break; }
      await buildRank(db);
    }
  }
  return out;
}

// ---------- CLI ----------
const isMain = process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href;

function argOf(name, dflt = null) {
  const i = process.argv.indexOf(name);
  return i >= 0 ? process.argv[i + 1] : dflt;
}

if (isMain) {
  const init = process.argv.includes('--init');
  const stepsArg = argOf('--step');
  const only = argOf('--only');
  const dl = Number(argOf('--dl', '1'));
  const agg = Number(argOf('--agg', '1'));
  const gpar = Number(argOf('--gpar', '1'));
  const stageGib = Number(argOf('--stage-gib', '6'));
  (async () => {
    if (init) {
      const r = await initLedger();
      console.log(JSON.stringify(r, null, 2));
      const c = r.perClass ?? {};
      const ok = r.total === 1890091 && c.A === 1588068 && c.B === 273352 && c.C === 28671
        && r.held === 17 && r.source_events === 17 && r.perState.planned === 1890074;
      console.log(ok ? 'INIT_OK' : 'INIT_MISMATCH');
      process.exit(ok ? 0 : 9);
    }
    if (!stepsArg) { console.error('usage: node src/h8_run.mjs --init | --step S3,S4,S5 [--dl n] [--agg m] [--gpar p] [--stage-gib G] [--only keys.txt]'); process.exit(2); }
    const steps = stepsArg.split(',').map((s) => s.trim()).filter(Boolean);
    for (const s of steps) if (!['S3', 'S4', 'S5'].includes(s)) { console.error(`unknown step ${s}`); process.exit(2); }
    const ac = new AbortController();
    process.on('SIGINT', () => ac.abort(new Error('SIGINT')));
    process.on('SIGTERM', () => ac.abort(new Error('SIGTERM')));
    const db = openLedger();
    let onlySet = null;
    if (only) onlySet = new Set(fs.readFileSync(only, 'utf8').split('\n').map((s) => s.trim()).filter(Boolean));
    const res = await runSteps(steps, { db, dl, agg, gpar, stageGib, signal: ac.signal, only: onlySet });
    console.log(JSON.stringify(res, null, 2));
    process.exit(res.find((r) => r.blocked) ? 9 : 0);
  })().catch((e) => { console.error(e?.stack ?? String(e)); process.exit(1); });
}
