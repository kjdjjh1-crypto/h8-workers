// H6X C3X — trades 독립 재계산. 집계 모듈·parser·소수 모듈을 쓰지 않고 자기 코드로 줄을 해석한다.
// 소수는 10^8배(가격·수량)·10^16배(금액 = 가격×수량) 정수 BigInt. 무작위 분 3개(시드 = zip sha256 앞 8자리)의 키를 뺀 10칸을 다시 계산한다.
// 줄은 readline/split/regex 대신 unzip -p 바이트 스트림에서 바로 해석한다 (2026-10-24 W1: 같은 판정·같은 결과, CPU만 줄임).
// 판정 순서·오류 문구·결과 값은 기존과 같다: 머리줄 처리 → 칸 수 → 시간 → 분 판정 → is_buyer_maker → 소수.
import { spawn } from 'node:child_process';
import { createInterface } from 'node:readline';

const FIELDS = ['n_trades', 'n_buy', 'buy_qty', 'sell_qty', 'buy_quote', 'sell_quote',
  'max_trade_quote', 'first_trade_id', 'last_trade_id', 'id_gaps'];
const SCALE = { buy_qty: 8, sell_qty: 8, buy_quote: 16, sell_quote: 16, max_trade_quote: 16 };

// 소수 글(지수 표기 허용, 예: 1.1660955222E7) → 10^sc 배 BigInt. 가수 정수 × 10^(sc - 소수자릿수 + 지수)로 계산한다.
function scaled(s, sc) {
  const t = String(s).trim();
  const m = /^(\d+)(?:\.(\d+))?(?:[eE]([+-]?\d+))?$/.exec(t);
  if (!m) throw new Error(`bad number ${t}`);
  const frac = m[2] || '';
  const mant = BigInt(m[1] + frac);
  const shift = sc - frac.length + Number(m[3] || 0);
  if (shift >= 0) return mant * 10n ** BigInt(shift);
  const div = 10n ** BigInt(-shift);
  if (mant % div !== 0n) throw new Error(`more than ${sc} decimals: ${t}`);
  return mant / div;
}

function toMs(s) {
  const t = String(s).trim();
  if (!/^\d+$/.test(t)) throw new Error(`bad time ${t}`);
  if (t.length === 13) return Number(t);
  if (t.length === 16) return Number(BigInt(t) / 1000n);
  throw new Error(`bad time length ${t.length}`);
}

export function pickMinutes3(sha256, minuteKeys) {
  const seed = parseInt(String(sha256).slice(0, 8), 16) >>> 0;
  const sorted = [...minuteKeys].sort((a, b) => a - b);
  const n = sorted.length;
  const picked = [];
  let s = seed;
  let guard = 0;
  while (picked.length < Math.min(3, n) && guard < 1000000) {
    guard++;
    s = (Math.imul(s, 1103515245) + 12345) >>> 0;
    const m = sorted[s % n];
    if (!picked.includes(m)) picked.push(m);
  }
  return { seed, minutes: picked.sort((a, b) => a - b) };
}

// ---------- 줄 해석 코어 (버퍼 바이트 단위, 판정 순서·문구는 기존과 동일) ----------
const isWs = (c) => c === 32 || (c >= 9 && c <= 13); // 공백·탭·줄바꿈 계열 (String.trim 의 ASCII 부분)

function recNew(minuteKeys, market) {
  return {
    want: new Set(minuteKeys), acc: new Map(),
    ncols: market === 'spot' ? 7 : 6,
    prevId: null, seen: false, lineNo: 0
  };
}

function recLine(st, buf, s, e) {
  st.lineNo++;
  while (s < e && isWs(buf[s])) s++;
  while (e > s && isWs(buf[e - 1])) e--;
  if (s >= e) return;
  // 첫 칸(=id)과 칸 경계
  let i = s, commas = 0, c1 = -1, c2 = -1, c3 = -1, c4 = -1, c5 = -1;
  for (; i < e; i++) {
    if (buf[i] === 44) {
      if (commas === 0) c1 = i; else if (commas === 1) c2 = i; else if (commas === 2) c3 = i;
      else if (commas === 3) c4 = i; else if (commas === 4) c5 = i;
      commas++;
    }
  }
  const cols = commas + 1;
  let a = s, b = c1 < 0 ? e : c1;
  while (a < b && isWs(buf[a])) a++;
  while (b > a && isWs(buf[b - 1])) b--;
  let digits = b > a;
  for (let k = a; digits && k < b; k++) if ((buf[k] ^ 48) > 9) digits = false;
  if (!digits) {
    if (!st.seen) { st.seen = true; return; }
    throw new Error(`header in the middle at line ${st.lineNo}`);
  }
  st.seen = true;
  if (cols !== st.ncols) throw new Error(`expected ${st.ncols} cols at line ${st.lineNo}`);
  // id (16자리를 넘으면 Number 문자열 파싱으로 되돌아간다)
  let id = 0;
  const idLen = b - a;
  if (idLen > 15) id = Number(buf.toString('latin1', a, b));
  else for (let k = a; k < b; k++) id = id * 10 + (buf[k] - 48);
  const gap = st.prevId !== null && id !== st.prevId + 1;
  st.prevId = id;
  // 시간 (cols[4])
  const tA = c4 + 1, tB = c5;
  let ta = tA, tb = tB;
  while (ta < tb && isWs(buf[ta])) ta++;
  while (tb > ta && isWs(buf[tb - 1])) tb--;
  let tDigits = tb > ta;
  for (let k = ta; tDigits && k < tb; k++) if ((buf[k] ^ 48) > 9) tDigits = false;
  if (!tDigits) throw new Error(`bad time ${buf.toString('latin1', ta, tb)}`);
  const tn = tb - ta;
  let ms;
  if (tn === 13) {
    ms = 0;
    for (let k = ta; k < tb; k++) ms = ms * 10 + (buf[k] - 48);
  } else if (tn === 16) ms = Number(BigInt(buf.toString('latin1', ta, tb)) / 1000n);
  else throw new Error(`bad time length ${tn}`);
  const minute = Math.floor(ms / 60000) * 60000;
  if (!st.want.has(minute)) return;
  // is_buyer_maker (cols[5])
  let sa = c5 + 1, sb = e;
  if (st.ncols !== 6) {
    for (let k = c5 + 1; k < e; k++) if (buf[k] === 44) { sb = k; break; }
  }
  while (sa < sb && isWs(buf[sa])) sa++;
  while (sb > sa && isWs(buf[sb - 1])) sb--;
  const n5 = sb - sa;
  let sell;
  if (n5 === 4 && (buf[sa] | 32) === 116 && (buf[sa + 1] | 32) === 114 && (buf[sa + 2] | 32) === 117 && (buf[sa + 3] | 32) === 101) sell = true;
  else if (n5 === 5 && (buf[sa] | 32) === 102 && (buf[sa + 1] | 32) === 97 && (buf[sa + 2] | 32) === 108 && (buf[sa + 3] | 32) === 115 && (buf[sa + 4] | 32) === 101) sell = false;
  else throw new Error(`bad is_buyer_maker at line ${st.lineNo}`);
  // 소수 (cols[1] 가격, cols[2] 수량)
  const qty = scaled(buf.toString('latin1', c2 + 1, c3), 8);
  const quote = scaled(buf.toString('latin1', c1 + 1, c2), 8) * qty;
  let acc = st.acc.get(minute);
  if (!acc) {
    acc = { n_trades: 0n, n_buy: 0n, buy_qty: 0n, sell_qty: 0n, buy_quote: 0n, sell_quote: 0n,
      max_trade_quote: -1n, first_trade_id: BigInt(id), last_trade_id: 0n, id_gaps: 0n };
    st.acc.set(minute, acc);
  }
  acc.n_trades += 1n;
  acc.last_trade_id = BigInt(id);
  if (gap) acc.id_gaps += 1n;
  if (sell) { acc.sell_qty += qty; acc.sell_quote += quote; }
  else { acc.n_buy += 1n; acc.buy_qty += qty; acc.buy_quote += quote; }
  if (quote > acc.max_trade_quote) acc.max_trade_quote = quote;
}

function recResult(st, minutes) {
  return minutes.map((m) => ({ minute: m, ...(st.acc.get(m) || null) }));
}

// 줄 이터러블(문자열·버퍼) → 고른 분들의 10칸 (BigInt 값)
export async function recomputeMinutes(lines, minutes, { market }) {
  const st = recNew(minutes, market);
  for await (const raw of lines) {
    const b = typeof raw === 'string' ? Buffer.from(raw.trim(), 'latin1') : raw;
    recLine(st, b, 0, b.length);
  }
  return recResult(st, minutes);
}

// maxLines: 통째 중복 원본은 앞 절반 줄 수까지만 (집계 쪽과 따로, 줄 수로 자른다)
export async function* unzipLinesX(zipPath, maxLines = null) {
  const child = spawn('unzip', ['-p', zipPath], { stdio: ['ignore', 'pipe', 'pipe'] });
  const errChunks = [];
  child.stderr.on('data', (d) => errChunks.push(d));
  const exitP = new Promise((res) => child.on('close', (code) => res(code)));
  const rl = createInterface({ input: child.stdout, crlfDelay: Infinity });
  let n = 0, cut = false;
  try {
    for await (const line of rl) {
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
  if (maxLines !== null) throw new Error(`unzip -p ${zipPath} ended at ${n} lines, before cut ${maxLines}`);
  if (code !== 0) throw new Error(`unzip -p ${zipPath} exit=${code} ${Buffer.concat(errChunks).toString().slice(0, 200)}`);
}

// unzip -p 바이트 스트림 → 코어 (readline 없이 줄을 나눠 바로 해석한다. 자르기·예외 의미는 unzipLinesX 와 같다)
async function recomputeMinutesZip(zipPath, minutes, { market, maxLines = null }) {
  const st = recNew(minutes, market);
  const child = spawn('unzip', ['-p', zipPath], { stdio: ['ignore', 'pipe', 'pipe'] });
  const errChunks = [];
  child.stderr.on('data', (d) => errChunks.push(d));
  const exitP = new Promise((res) => child.on('close', (code) => res(code)));
  let left = null, n = 0, cut = maxLines === 0, failed = null;
  const handle = (buf, s, e) => {
    if (cut) return;
    n++;
    recLine(st, buf, s, e);
    if (maxLines !== null && n >= maxLines) { cut = true; child.kill('SIGTERM'); }
  };
  if (cut) {
    child.kill('SIGTERM');
    return recResult(st, minutes);
  }
  try {
    await new Promise((resolve, reject) => {
      child.on('error', (e) => { if (!failed) { failed = e; reject(e); } });
      child.stdout.on('data', (chunk) => {
        if (failed || cut) return;
        try {
          let start = 0;
          if (left !== null) {
            const nl = chunk.indexOf(10);
            if (nl < 0) { left = Buffer.concat([left, chunk]); return; }
            const joined = Buffer.concat([left, chunk.subarray(0, nl)]);
            left = null;
            handle(joined, 0, joined.length);
            start = nl + 1;
          }
          for (;;) {
            const nl = chunk.indexOf(10, start);
            if (nl < 0) break;
            handle(chunk, start, nl);
            start = nl + 1;
          }
          if (start < chunk.length) left = Buffer.from(chunk.subarray(start));
          if (cut) resolve();
        } catch (e) { failed = e; child.kill('SIGTERM'); reject(e); }
      });
      child.stdout.on('end', () => resolve());
    });
    if (left !== null && !cut && !failed) handle(left, 0, left.length);
    if (cut) return recResult(st, minutes);
    const code = await exitP;
    if (maxLines !== null) throw new Error(`unzip -p ${zipPath} ended at ${n} lines, before cut ${maxLines}`);
    if (code !== 0) throw new Error(`unzip -p ${zipPath} exit=${code} ${Buffer.concat(errChunks).toString().slice(0, 200)}`);
    return recResult(st, minutes);
  } finally {
    if (child.exitCode === null && child.signalCode === null) child.kill('SIGTERM');
  }
}

// DB 행(문자열·정수)과 재계산 행(BigInt)을 칸마다 비교한다
export function compareTf(recomputed, dbRows) {
  const byMin = new Map(dbRows.map((r) => [Number(r.minute), r]));
  const diffs = [];
  for (const c of recomputed) {
    const r = byMin.get(c.minute);
    if (!r || c.n_trades === undefined) { diffs.push({ minute: c.minute, field: '*', db: r || null }); continue; }
    for (const f of FIELDS) {
      const want = SCALE[f] ? scaled(r[f], SCALE[f]) : BigInt(r[f]);
      if (want !== c[f]) diffs.push({ minute: c.minute, field: f, db: String(r[f]), recomputed: c[f].toString() });
    }
  }
  return diffs;
}

export async function recheckTradesZip(zipPath, sha256, { market, maxLines = null }, minuteKeys, dbRowsFor) {
  const { seed, minutes } = pickMinutes3(sha256, minuteKeys);
  const rec = await recomputeMinutesZip(zipPath, minutes, { market, maxLines });
  const dbRows = dbRowsFor(minutes);
  const diffs = compareTf(rec, dbRows);
  return { seed, minutes, ok: minutes.length === 3 && diffs.length === 0, diffs };
}
