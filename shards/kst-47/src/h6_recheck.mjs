// H6 C3 독립 재계산 — 이 파일은 집계·소수·파서 모듈을 하나도 import하지 않는다. 줄 해석·시간 변환·소수 계산을 자기 코드로 한다.
// 소수는 10^8 배 정수 BigInt 방식(금액은 10^16 배). 시드는 zip sha256 앞 8자리 16진수라 재현 가능하다.
import { spawn } from 'node:child_process';
import { createInterface } from 'node:readline';

const SC = 8;            // price·qty 배수 자릿수
const SCQ = 16;          // 금액 배수 자릿수
const TH = [10000n * 10n ** 16n, 100000n * 10n ** 16n, 1000000n * 10n ** 16n];

function normTime(s) {
  const t = String(s).trim();
  if (!/^\d+$/.test(t)) throw new Error(`bad timestamp: ${t}`);
  if (t.length === 13) return Number(t);
  if (t.length === 16) return Number(BigInt(t) / 1000n);
  throw new Error(`bad timestamp length: ${t}`);
}

function scaledBig(s, scale) {
  let u = String(s).trim();
  let neg = false;
  if (u.charCodeAt(0) === 45) { neg = true; u = u.slice(1); }
  // 지수 표기 (예: 1e-05, 바이낸스 spot BCHUSDT-aggTrades-2020-06): 가수 정수 × 10^(scale - 소수자릿수 + 지수).
  // 나누어떨어지지 않으면 throw — 반올림 없음 (2026-10-21 대장님 지시)
  const ex = /^(\d+)(?:\.(\d+))?[eE]([+-]?\d+)$/.exec(u);
  if (ex) {
    const fr = ex[2] || '';
    const mant = BigInt(ex[1] + fr);
    const shift = scale - fr.length + Number(ex[3]);
    let v;
    if (shift >= 0) v = mant * 10n ** BigInt(shift);
    else {
      const div = 10n ** BigInt(-shift);
      if (mant % div !== 0n) throw new Error(`too many decimals: ${s}`);
      v = mant / div;
    }
    return neg ? -v : v;
  }
  const dot = u.indexOf('.');
  let whole, frac;
  if (dot < 0) { whole = u; frac = ''; }
  else { whole = u.slice(0, dot); frac = u.slice(dot + 1); }
  if (frac.length > scale) throw new Error(`too many decimals: ${s}`);
  const v = BigInt((whole || '0') + frac.padEnd(scale, '0'));
  return neg ? -v : v;
}

function fmt(n, scale) {
  const neg = n < 0n;
  const a = neg ? -n : n;
  const s = a.toString().padStart(scale + 1, '0');
  const frac = s.slice(-scale).replace(/0+$/, '');
  const whole = s.slice(0, s.length - scale);
  const body = whole + (frac ? '.' + frac : '');
  return neg && a !== 0n ? '-' + body : body;
}

function monthRange(month) {
  const [y, m] = month.split('-').map(Number);
  return [Date.UTC(y, m - 1, 1), Date.UTC(m === 12 ? y + 1 : y, m === 12 ? 0 : m, 1)];
}

function newAcc(minute) {
  return {
    minute,
    n_agg: 0,
    n_trades: 0,
    buy_qty: 0n,
    sell_qty: 0n,
    buy_quote: 0n,
    sell_quote: 0n,
    max_agg_quote: 0n,
    w: [{ n: 0, q: 0n }, { n: 0, q: 0n }, { n: 0, q: 0n }],
    s: [{ n: 0, q: 0n }, { n: 0, q: 0n }, { n: 0, q: 0n }],
    first_agg_id: null,
    last_agg_id: null,
    id_gaps: 0
  };
}

function toRow(a, exchange, symbol) {
  return {
    exchange,
    symbol,
    minute: a.minute,
    n_agg: a.n_agg,
    n_trades: a.n_trades,
    buy_qty: fmt(a.buy_qty, SC),
    sell_qty: fmt(a.sell_qty, SC),
    buy_quote: fmt(a.buy_quote, SCQ),
    sell_quote: fmt(a.sell_quote, SCQ),
    max_agg_quote: fmt(a.max_agg_quote, SCQ),
    buy_n_ge_10k: a.w[0].n,
    buy_quote_ge_10k: fmt(a.w[0].q, SCQ),
    sell_n_ge_10k: a.s[0].n,
    sell_quote_ge_10k: fmt(a.s[0].q, SCQ),
    buy_n_ge_100k: a.w[1].n,
    buy_quote_ge_100k: fmt(a.w[1].q, SCQ),
    sell_n_ge_100k: a.s[1].n,
    sell_quote_ge_100k: fmt(a.s[1].q, SCQ),
    buy_n_ge_1m: a.w[2].n,
    buy_quote_ge_1m: fmt(a.w[2].q, SCQ),
    sell_n_ge_1m: a.s[2].n,
    sell_quote_ge_1m: fmt(a.s[2].q, SCQ),
    first_agg_id: a.first_agg_id,
    last_agg_id: a.last_agg_id,
    id_gaps: a.id_gaps
  };
}

export const RECHECK_FIELDS = Object.keys(toRow(newAcc(0), 'x', 'y'));

// 줄 이터러블 → 분별 22칸 (자기 파서). counts.placeholders: 뺀 가짜 거래 줄 수
export async function aggregateIndependent(lines, { exchange, symbol, month }, counts = {}) {
  const [mStart, mEnd] = monthRange(month);
  counts.placeholders = 0;
  const byMinute = new Map();
  let prevTime = null;
  let prevLastId = null;
  let seenNonEmpty = false;
  let lineNo = 0;

  for await (const line of lines) {
    lineNo++;
    const trimmed = String(line).trim();
    if (!trimmed) continue;
    const cols = trimmed.split(',');
    const c0 = cols[0].trim();
    if (!/^\d+(\.\d+)?$/.test(c0)) {
      if (!seenNonEmpty) { seenNonEmpty = true; continue; } // 머리줄은 첫 비어있지 않은 줄에서만
      throw new Error(`unexpected header at line ${lineNo}: ${c0}`);
    }
    seenNonEmpty = true;

    const aggId = Number(c0);
    const price = scaledBig(cols[1].trim(), SC);
    const qty = scaledBig(cols[2].trim(), SC);
    const firstId = Number(cols[3].trim());
    const lastId = Number(cols[4].trim());
    const time = normTime(cols[5].trim());
    const buyerMaker = /^(true|1)$/i.test(cols[6].trim()) ? 1 : 0;

    // 가짜 거래 줄 (가격 0·수량 0·첫/끝 id -1): 집계·시간 역행·id 공백 판단에서 빼고 따로 센다. 규칙은 집계 쪽과 같고 코드는 따로 (2026-10-21 대장님 지시)
    if (firstId === -1 && lastId === -1 && price === 0n && qty === 0n) { counts.placeholders++; continue; }

    if (prevTime !== null && time < prevTime) throw new Error(`time reversed at line ${lineNo}`);
    prevTime = time;
    const hasGap = prevLastId !== null && firstId !== prevLastId + 1;
    prevLastId = lastId;

    if (time < mStart || time >= mEnd) continue;

    const minute = Math.floor(time / 60000) * 60000;
    let acc = byMinute.get(minute);
    if (acc === undefined) { acc = newAcc(minute); byMinute.set(minute, acc); }

    const quote = price * qty;
    acc.n_agg += 1;
    acc.n_trades += (lastId - firstId + 1);
    if (buyerMaker === 0) { acc.buy_qty += qty; acc.buy_quote += quote; }
    else { acc.sell_qty += qty; acc.sell_quote += quote; }
    const th = buyerMaker === 0 ? acc.w : acc.s;
    for (let i = 0; i < 3; i++) if (quote >= TH[i]) { th[i].n += 1; th[i].q += quote; }
    if (quote > acc.max_agg_quote) acc.max_agg_quote = quote;
    if (acc.first_agg_id === null || aggId < acc.first_agg_id) acc.first_agg_id = aggId;
    if (acc.last_agg_id === null || aggId > acc.last_agg_id) acc.last_agg_id = aggId;
    if (hasGap) acc.id_gaps += 1;
  }

  const out = new Map();
  for (const [m, a] of byMinute) out.set(m, toRow(a, exchange, symbol));
  return out;
}

export function pickMinutes(sha256, minuteKeys) {
  const seed = parseInt(String(sha256).slice(0, 8), 16) >>> 0;
  const sorted = [...minuteKeys].sort((a, b) => a - b);
  const n = sorted.length;
  const picked = [];
  let s = seed;
  let guard = 0;
  while (picked.length < 3 && guard < 1000000) {
    guard++;
    s = (Math.imul(s, 1103515245) + 12345) >>> 0;
    const idx = s % n;
    const m = sorted[idx];
    if (!picked.includes(m)) picked.push(m);
  }
  return { seed, minutes: picked.sort((a, b) => a - b) };
}

// zip을 다시 흘려 읽어 무작위 분 3개의 22칸을 재계산한다. maxLines: 통째 중복 원본은 앞 절반 줄 수까지만 (집계 쪽과 따로, 줄 수로 자른다)
// skipLines: N벌 원본에서 고른 벌 앞의 줄 수만큼 버린다
export async function recomputeFromZip(zipPath, sha256, { exchange, symbol, month, maxLines = null, skipLines = 0 }) {
  const counts = {};
  const all = await aggregateIndependent(unzipLines(zipPath, maxLines, skipLines), { exchange, symbol, month }, counts);
  const { seed, minutes } = pickMinutes(sha256, all.keys());
  return { seed, minutes, rows: minutes.map((m) => all.get(m)), total_minutes: all.size, placeholders: counts.placeholders };
}

export async function* unzipLines(zipPath, maxLines = null, skipLines = 0) {
  const child = spawn('unzip', ['-p', zipPath], { stdio: ['ignore', 'pipe', 'pipe'] });
  const errChunks = [];
  child.stderr.on('data', (d) => errChunks.push(d));
  const exitP = new Promise((res) => child.on('close', (code) => res(code)));
  const rl = createInterface({ input: child.stdout, crlfDelay: Infinity });
  let n = 0, cut = false, skipped = 0;
  try {
    for await (const line of rl) {
      if (skipped < skipLines) { skipped++; continue; }
      if (maxLines !== null && n >= maxLines) { cut = true; break; }
      n++;
      yield line;
    }
  } finally {
    rl.close();
    if (child.exitCode === null && child.signalCode === null) child.kill('SIGTERM');
  }
  const code = await exitP;
  if (cut) return;
  if (skipped < skipLines) throw new Error(`unzip -p ${zipPath} ended at ${skipped} lines, before skip ${skipLines}`);
  if (maxLines !== null) throw new Error(`unzip -p ${zipPath} ended at ${n} lines, before cut ${maxLines}`);
  if (code !== 0) {
    throw new Error(`unzip -p ${zipPath} exit=${code} ${Buffer.concat(errChunks).toString().slice(0, 200)}`);
  }
}

// DB 문자열 값을 같은 배율 BigInt로 바꿔 비교한다. 같은 배율이면 문자열도 같아야 한다.
export function scaledOfValue(value, scale) {
  return scaledBig(value, scale);
}

if (import.meta.url === `file://${process.argv[1]}`) {
  const args = process.argv.slice(2);
  const get = (f) => { const i = args.indexOf(f); return i >= 0 ? args[i + 1] : null; };
  const zip = get('--zip');
  const sha = get('--sha256');
  const month = get('--month');
  const exchange = get('--exchange') || 'binance';
  const symbol = get('--symbol') || 'BTCUSDT';
  if (!zip || !sha || !month) {
    console.log('usage: node src/h6_recheck.mjs --zip <path> --sha256 <hex> --month YYYY-MM [--exchange E] [--symbol S]');
    process.exit(64);
  }
  const r = await recomputeFromZip(zip, sha, { exchange, symbol, month });
  console.log(JSON.stringify(r));
}
