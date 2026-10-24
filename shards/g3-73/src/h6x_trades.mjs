// H6X trades — Binance trades(개별 체결) 월 파일을 1분 단위 10칸으로 정확히 집계하고, kline과 정확 대조(C2X)한다.
// 칸: 현물 id,price,qty,quote_qty,time,is_buyer_maker,is_best_match (머리줄 없음, True/False, time 13·16자리)
//     선물 id,price,qty,quote_qty,time,is_buyer_maker (머리줄 있음, true/false, time 13자리)
// 분 = floor(time_ms/60000)×60000. is_buyer_maker 거짓 → buy, 참 → sell. 금액 = price×qty 정확 곱을 더한다.
// (quote_qty 칸은 쓰지 않는다: 선물 2022-02 등은 소수 둘째 자리에서 잘려 있고 kline은 price×qty와 같다. 2026-10-20 대장님 지시.
//  quote_qty ≠ price×qty 인 줄 수는 quote_ne_pxq로 계속 센다.)
// 소수는 정수부 I + 소수 1~8자리 F + 9~16자리 G 세 Number로 더한다(올림 처리로 늘 정확). 16자리를 넘는 소수는 throw.
// unzip -p 스트림을 줄 문자열로 만들지 않고 바이트 단위로 해석한다(지시서 §2-3 readline의 속도 대체, 뜻은 같다).
import { spawn } from 'node:child_process';
import { normalizeTimestamp, addDecimal, compareDecimal } from './parser.mjs';
import { divDecimal } from './h6_decimal.mjs';
import { cutStdout } from './h6_srcdup.mjs';

const E8 = 1e8;
const POW10 = [1, 10, 100, 1e3, 1e4, 1e5, 1e6, 1e7, 1e8, 1e9, 1e10, 1e11, 1e12, 1e13, 1e14, 1e15, 1e16];
const MAX_SAFE = Number.MAX_SAFE_INTEGER;

export const TF_FIELDS = ['n_trades', 'n_buy', 'buy_qty', 'sell_qty', 'buy_quote', 'sell_quote',
  'max_trade_quote', 'first_trade_id', 'last_trade_id', 'id_gaps'];

export function monthRange(month) {
  const y = Number(month.slice(0, 4)), m = Number(month.slice(5, 7));
  return [Date.UTC(y, m - 1, 1), Date.UTC(y, m, 1)];
}

// (I, F, G) → decimal() 형식 문자열
function fmt3(I, F, G) {
  if (F === 0 && G === 0) return String(I);
  let frac = String(F).padStart(8, '0');
  if (G !== 0) frac += String(G).padStart(8, '0');
  let end = frac.length;
  while (end > 0 && frac.charCodeAt(end - 1) === 48) end--;
  return `${I}.${frac.slice(0, end)}`;
}

function newAcc() {
  return {
    n: 0, nb: 0,
    bqI: 0, bqF: 0, bqG: 0, sqI: 0, sqF: 0, sqG: 0,
    buI: 0, buF: 0, buG: 0, suI: 0, suF: 0, suG: 0,
    mxI: -1, mxF: 0, mxG: 0,
    fid: 0, lid: 0, gaps: 0
  };
}

// 소수 칸 하나의 해석 결과 (모듈 스칼라 — 줄마다 객체를 만들지 않는다. 값 의미는 D 와 같다)
let dI = 0, dF = 0, dG = 0, dM = 0, dX = 0, dOK = true;

// 지수 표기(선물 trades quote_qty, 예: 1.1660955222E7)를 보통 소수 글로 편다. 지수가 없으면 그대로 돌려준다.
function plainOf(t) {
  const m = /^(\d+)(?:\.(\d+))?[eE]([+-]?\d+)$/.exec(t);
  if (!m) return t;
  const digits = m[1] + (m[2] || '');
  const point = m[1].length + Number(m[3]);
  let whole, frac;
  if (point <= 0) { whole = '0'; frac = '0'.repeat(-point) + digits; }
  else if (point >= digits.length) { whole = digits + '0'.repeat(point - digits.length); frac = ''; }
  else { whole = digits.slice(0, point); frac = digits.slice(point); }
  whole = whole.replace(/^0+(?=\d)/, '');
  return frac ? `${whole}.${frac}` : whole;
}

// buf[s,e) 소수 → dI(정수부) dF(소수 1~8) dG(소수 9~16), dM·dX(끝 0을 뗀 가수와 소수 자릿수, 안전정수일 때만 dOK)
// 줄 하나마다 수만 번 도는 길이라 한 바이트 검사를 줄였다 (c^48 ≤ 9 이면 숫자 한 자리, 그 밖은 점·지수·오류).
function parseDec(buf, s, e) {
  let i = s, I = 0, nInt = 0, F = 0, G = 0, nf = 0, lnz = 0, frac = 0;
  for (; i < e; i++) {
    const c = buf[i];
    const d = c ^ 48;
    if (d <= 9) {
      if (!frac) { I = I * 10 + d; nInt++; }
      else {
        nf++;
        if (nf <= 8) F = F * 10 + d;
        else if (nf <= 16) G = G * 10 + d;
        else if (d !== 0) throw new Error('more than 16 decimals');
        if (d !== 0) lnz = nf;
      }
      continue;
    }
    if (c === 46) {
      if (frac) throw new Error(`bad decimal byte ${c}`);
      if (nInt === 0 || nInt > 15) throw new Error(`bad decimal integer part (${nInt} digits)`);
      frac = 1;
      continue;
    }
    if (c === 69 || c === 101) return parseExp(buf, s, e);
    throw new Error(`bad decimal byte ${c}`);
  }
  if (frac && nf === 0) throw new Error('empty fraction');
  if (nInt === 0 || nInt > 15) throw new Error(`bad decimal integer part (${nInt} digits)`);
  if (nf < 8) F *= POW10[8 - nf];
  if (nf > 8 && nf < 16) G *= POW10[16 - nf];
  dI = I; dF = F; dG = G;
  // 끝 0을 뗀 가수: 소수 8자리 이내이고 안전정수 범위일 때만 Number로 만든다.
  // 끝 0 개수 = 8 − lnz(lnz>0) 또는 8 상한(lnz=0)이라, 0 제거 루프 대신 나눗셈 한 번으로 같은 값을 낸다.
  if (lnz <= 8 && I < 90000000) {
    if (lnz === 0) { dM = I; dX = 0; }
    else { dM = (I * E8 + F) / POW10[8 - lnz]; dX = lnz; }
    dOK = true;
  } else dOK = false;
}

// 지수 표기 칸: 펴서 다시 해석한다 (드문 경우라 느린 길)
function parseExp(buf, s, e) {
  const t = plainOf(buf.toString('latin1', s, e));
  if (/[eE]/.test(t)) throw new Error(`bad decimal ${t}`);
  const b = Buffer.from(t, 'latin1');
  parseDec(b, 0, b.length);
}

// price×qty == quote 정확 비교, 가수가 크면 BigInt로 다시 본다
function decBig(buf, s, e) {
  const t = plainOf(buf.toString('latin1', s, e));
  const dot = t.indexOf('.');
  const frac = dot < 0 ? '' : t.slice(dot + 1);
  return [BigInt((dot < 0 ? t : t.slice(0, dot)) + frac), frac.length];
}
function pxqEqual(pm, px, qm, qx, um, ux, buf, sP, eP, sQ, eQ, sU, eU, fast) {
  if (fast) {
    let r = pm * qm;
    if (r <= MAX_SAFE) {
      let x = px + qx;
      if (r === 0) return um === 0;
      while (x > 0 && r % 10 === 0) { r /= 10; x--; }
      return r === um && x === ux;
    }
  }
  const [p, a] = decBig(buf, sP, eP), [q, b] = decBig(buf, sQ, eQ), [u, c] = decBig(buf, sU, eU);
  const prod = p * q, sc = a + b;
  return sc >= c ? prod === u * 10n ** BigInt(sc - c) : prod * 10n ** BigInt(c - sc) === u;
}

// price×qty 정확 곱 → dI·dF·dG. 가수가 크면 BigInt로 곱해 다시 해석한다 (소수 16자리를 넘으면 parseDec가 throw)
function pxq3(pm, px, qm, qx, buf, sP, eP, sQ, eQ, fast) {
  if (fast) {
    const r = pm * qm;
    if (r <= MAX_SAFE) {
      const sc = px + qx;
      const p = POW10[sc];
      const frac = r % p;
      dI = (r - frac) / p;
      if (sc <= 8) { dF = frac * POW10[8 - sc]; dG = 0; }
      else { const d = POW10[sc - 8]; const g = frac % d; dF = (frac - g) / d; dG = g * POW10[16 - sc]; }
      return;
    }
  }
  const [p, a] = decBig(buf, sP, eP), [q, b] = decBig(buf, sQ, eQ);
  const sc = a + b;
  let t = (p * q).toString();
  if (sc > 0) { t = t.padStart(sc + 1, '0'); t = `${t.slice(0, -sc)}.${t.slice(-sc)}`; }
  const tb = Buffer.from(t, 'latin1');
  parseDec(tb, 0, tb.length);
}

export class TradesAggregator {
  constructor({ exchange, symbol, month, market }) {
    if (market !== 'spot' && market !== 'um') throw new Error(`bad market ${market}`);
    this.exchange = exchange; this.symbol = symbol; this.month = month; this.market = market;
    this.ncols = market === 'spot' ? 7 : 6;
    [this.mStart, this.mEnd] = monthRange(month);
    this.byMinute = new Map();
    this.cur = null; this.curMinute = -1;
    this.prevTime = -1; this.prevId = -1;
    this.seenNonEmpty = false; this.header = null;
    this.lineNo = 0; this.lines = 0;
    this.lastTl = -1;
    this.out_of_range = 0; this.time_reversed = 0; this.quote_ne_pxq = 0; this.id_gaps_all = 0;
    this.firstTime = null; this.lastTime = null;
    this.tsChecked = new Set();
    this.left = null;
  }

  push(chunk) {
    let start = 0;
    if (this.left !== null) {
      const nl = chunk.indexOf(10);
      if (nl < 0) { this.left = Buffer.concat([this.left, chunk]); return; }
      const joined = Buffer.concat([this.left, chunk.subarray(0, nl)]);
      this.left = null;
      this.line(joined, 0, joined.length);
      start = nl + 1;
    }
    for (;;) {
      const nl = chunk.indexOf(10, start);
      if (nl < 0) break;
      this.line(chunk, start, nl);
      start = nl + 1;
    }
    if (start < chunk.length) this.left = Buffer.from(chunk.subarray(start));
  }

  end() {
    if (this.left !== null) { const b = this.left; this.left = null; this.line(b, 0, b.length); }
    return this.result();
  }

  line(buf, s, e) {
    this.lineNo++;
    if (e > s && buf[e - 1] === 13) e--;
    while (s < e && (buf[s] === 32 || buf[s] === 9)) s++;
    while (e > s && (buf[e - 1] === 32 || buf[e - 1] === 9)) e--;
    if (s >= e) return;
    const c0 = buf[s];
    if (c0 < 48 || c0 > 57) {
      if (!this.seenNonEmpty) { this.seenNonEmpty = true; this.header = buf.toString('latin1', s, e); return; }
      throw new Error(`unexpected header at line ${this.lineNo}: ${buf.toString('latin1', s, Math.min(e, s + 40))}`);
    }
    this.seenNonEmpty = true;

    // 칸 경계
    let i = s;
    let id = 0;
    for (; i < e; i++) { const c = buf[i]; if (c === 44) break; if (c < 48 || c > 57) throw new Error(`bad id at line ${this.lineNo}`); id = id * 10 + (c - 48); }
    if (i >= e) throw new Error(`short line ${this.lineNo}`);
    const sP = ++i; while (i < e && buf[i] !== 44) i++; const eP = i;
    const sQ = ++i; while (i < e && buf[i] !== 44) i++; const eQ = i;
    const sU = ++i; while (i < e && buf[i] !== 44) i++; const eU = i;
    const sT = ++i; while (i < e && buf[i] !== 44) i++; const eT = i;
    const sB = ++i; while (i < e && buf[i] !== 44) i++; const eB = i;
    let cols = 6;
    while (i < e) { if (buf[i] === 44) cols++; i++; }
    if (eB > e || sB >= e + 1) throw new Error(`short line ${this.lineNo}`);
    if (cols !== this.ncols) throw new Error(`expected ${this.ncols} columns, got ${cols} at line ${this.lineNo}`);

    // 시간
    let t = 0;
    for (let k = sT; k < eT; k++) { const c = buf[k]; if (c < 48 || c > 57) throw new Error(`bad time at line ${this.lineNo}`); t = t * 10 + (c - 48); }
    const tl = eT - sT;
    let ms;
    if (tl === 13) ms = t;
    else if (tl === 16) ms = (t - t % 1000) / 1000;
    else ms = normalizeTimestamp(buf.toString('latin1', sT, eT)); // 그 밖 길이는 parser가 판정(throw)
    if (tl !== this.lastTl) {
      this.lastTl = tl;
      if (!this.tsChecked.has(tl)) { // 길이별 첫 값은 parser.normalizeTimestamp와 같아야 한다
        this.tsChecked.add(tl);
        const ref = normalizeTimestamp(buf.toString('latin1', sT, eT));
        if (ref !== ms) throw new Error(`time conversion mismatch ${ref} != ${ms}`);
      }
    }

    // 쪽
    const b0 = buf[sB];
    let sell;
    if (b0 === 84 || b0 === 116) sell = true;        // T t
    else if (b0 === 70 || b0 === 102) sell = false;  // F f
    else throw new Error(`bad is_buyer_maker at line ${this.lineNo}`);

    this.lines++;
    if (this.prevTime >= 0 && ms < this.prevTime) this.time_reversed++;
    this.prevTime = ms;
    const gap = this.prevId >= 0 && id !== this.prevId + 1;
    if (gap) this.id_gaps_all++;
    this.prevId = id;
    if (this.firstTime === null) this.firstTime = ms;
    this.lastTime = ms;

    parseDec(buf, sP, eP); const pm = dM, px = dX, pok = dOK;
    parseDec(buf, sQ, eQ); const qI = dI, qF = dF, qG = dG, qm = dM, qx = dX, qok = dOK;
    parseDec(buf, sU, eU);
    if (!pxqEqual(pm, px, qm, qx, dM, dX, buf, sP, eP, sQ, eQ, sU, eU, pok && qok && dOK)) this.quote_ne_pxq++;
    pxq3(pm, px, qm, qx, buf, sP, eP, sQ, eQ, pok && qok); const uI = dI, uF = dF, uG = dG;

    if (ms < this.mStart || ms >= this.mEnd) { this.out_of_range++; return; }

    const minute = ms - ms % 60000;
    let a = this.cur;
    if (minute !== this.curMinute) {
      a = this.byMinute.get(minute);
      if (a === undefined) { a = newAcc(); a.fid = id; this.byMinute.set(minute, a); }
      this.cur = a; this.curMinute = minute;
    }
    a.n++;
    a.lid = id;
    if (gap) a.gaps++;
    if (sell) {
      a.sqG += qG; if (a.sqG >= E8) { a.sqG -= E8; a.sqF += 1; }
      a.sqF += qF; if (a.sqF >= E8) { a.sqF -= E8; a.sqI += 1; }
      a.sqI += qI;
      a.suG += uG; if (a.suG >= E8) { a.suG -= E8; a.suF += 1; }
      a.suF += uF; if (a.suF >= E8) { a.suF -= E8; a.suI += 1; }
      a.suI += uI;
    } else {
      a.nb++;
      a.bqG += qG; if (a.bqG >= E8) { a.bqG -= E8; a.bqF += 1; }
      a.bqF += qF; if (a.bqF >= E8) { a.bqF -= E8; a.bqI += 1; }
      a.bqI += qI;
      a.buG += uG; if (a.buG >= E8) { a.buG -= E8; a.buF += 1; }
      a.buF += uF; if (a.buF >= E8) { a.buF -= E8; a.buI += 1; }
      a.buI += uI;
    }
    if (uI > a.mxI || (uI === a.mxI && (uF > a.mxF || (uF === a.mxF && uG > a.mxG)))) {
      a.mxI = uI; a.mxF = uF; a.mxG = uG;
    }
  }

  result() {
    const minutes = [...this.byMinute.keys()].sort((x, y) => x - y);
    const rows = minutes.map((m) => {
      const a = this.byMinute.get(m);
      return {
        exchange: this.exchange, symbol: this.symbol, minute: m,
        n_trades: a.n, n_buy: a.nb,
        buy_qty: fmt3(a.bqI, a.bqF, a.bqG), sell_qty: fmt3(a.sqI, a.sqF, a.sqG),
        buy_quote: fmt3(a.buI, a.buF, a.buG), sell_quote: fmt3(a.suI, a.suF, a.suG),
        max_trade_quote: fmt3(a.mxI, a.mxF, a.mxG),
        first_trade_id: a.fid, last_trade_id: a.lid, id_gaps: a.gaps
      };
    });
    let gaps = 0;
    for (const r of rows) gaps += r.id_gaps;
    return {
      rows,
      lines: this.lines,
      header: this.header,
      out_of_range: this.out_of_range,
      time_reversed: this.time_reversed,
      quote_ne_pxq: this.quote_ne_pxq,
      id_gaps: gaps,
      id_gaps_all: this.id_gaps_all,
      first_time: this.firstTime,
      last_time: this.lastTime
    };
  }
}

// 버퍼 배열(시험용) → 집계
export function aggregateTradesBuffers(buffers, opts) {
  const agg = new TradesAggregator(opts);
  for (const b of buffers) agg.push(Buffer.isBuffer(b) ? b : Buffer.from(b));
  return agg.end();
}

// zip → unzip -p 스트림 → 집계 (opts.limitBytes: 통째 중복 원본은 앞 절반 바이트까지만, h6_srcdup.mjs)
export function aggregateTradesZip(zipPath, opts) {
  return new Promise((resolve, reject) => {
    const agg = new TradesAggregator(opts);
    const child = spawn('unzip', ['-p', zipPath], { stdio: ['ignore', 'pipe', 'pipe'] });
    const c = cutStdout(child, opts.limitBytes);
    const errChunks = [];
    let failed = null;
    child.stderr.on('data', (d) => errChunks.push(d));
    c.out.on('data', (chunk) => {
      if (failed) return;
      try { agg.push(chunk); } catch (e) { failed = e; child.kill('SIGTERM'); }
    });
    const outEnd = new Promise((r) => { c.out.once('end', r); c.out.once('close', r); });
    child.on('error', (e) => { if (!failed) failed = e; });
    child.on('close', async (code) => {
      if (failed) return reject(failed);
      const why = c.check(code);
      if (why) return reject(new Error(`unzip -p ${zipPath} ${why} ${Buffer.concat(errChunks).toString().slice(0, 200)}`));
      await outEnd;
      try { resolve(agg.end()); } catch (e) { reject(e); }
    });
  });
}

const ZERO_K = { count: '0', volume: '0', quote_volume: '0', taker_buy_volume: '0', taker_buy_quote: '0' };

function relDiff(a, b) {
  if (compareDecimal(b, '0') === 0) return compareDecimal(a, '0') === 0 ? '0' : '1';
  const c = compareDecimal(a, b);
  if (c === 0) return '0';
  const neg = (s) => (s.startsWith('-') ? s.slice(1) : '-' + s);
  const d = c > 0 ? addDecimal(a, neg(b)) : addDecimal(b, neg(a));
  return divDecimal(d, b, 10);
}

// C2X: 대상 분 = kline(count>0) ∪ tf_1m 분. 5칸 모두 정확히 같아야 한다. 허용 오차 없음.
// PASS = 어긋난 분 0 · out_of_range 0 · 시간 역행 0. 그 밖은 FAIL. 어긋난 분은 전부 돌려준다.
// known = 이 단위의 알려진 원본 결손 [{ id, from, to, expect }] (ms, from ≤ 분 < to, manifest/h6x_source_gaps.json).
// 어긋난 분이 결손 구간 안이고 방향이 맞으면(tf_short: 우리 거래 수 < kline, kline_short: 우리 > kline) known_gaps로 옮긴다.
// 남은 어긋난 분 0 · known_gaps 1 이상 · out_of_range 0 · 역행 0 → PASS_KNOWN.
export function judgeC2X({ rows, kline, out_of_range = 0, time_reversed = 0, known = [] }) {
  const tf = new Map();
  for (const r of rows) tf.set(r.minute, r);
  const sorted = [...new Set([...kline.keys(), ...tf.keys()])].sort((a, b) => a - b);
  const mismatches = [];
  const knownGaps = [];
  let tn = '0', tv = '0', tq = '0', ttb = '0', ttbq = '0';
  let kn = '0', kv = '0', kq = '0', ktb = '0', ktbq = '0';
  for (const m of sorted) {
    const k = kline.get(m) || ZERO_K;
    const o = tf.get(m);
    const on = o ? String(o.n_trades) : '0';
    const ov = o ? addDecimal(o.buy_qty, o.sell_qty) : '0';
    const oq = o ? addDecimal(o.buy_quote, o.sell_quote) : '0';
    const otb = o ? o.buy_qty : '0';
    const otbq = o ? o.buy_quote : '0';
    tn = addDecimal(tn, on); tv = addDecimal(tv, ov); tq = addDecimal(tq, oq); ttb = addDecimal(ttb, otb); ttbq = addDecimal(ttbq, otbq);
    kn = addDecimal(kn, k.count); kv = addDecimal(kv, k.volume); kq = addDecimal(kq, k.quote_volume);
    ktb = addDecimal(ktb, k.taker_buy_volume); ktbq = addDecimal(ktbq, k.taker_buy_quote);
    const bad = [];
    if (compareDecimal(on, k.count) !== 0) bad.push('n_trades');
    if (compareDecimal(ov, k.volume) !== 0) bad.push('volume');
    if (compareDecimal(oq, k.quote_volume) !== 0) bad.push('quote_volume');
    if (compareDecimal(otb, k.taker_buy_volume) !== 0) bad.push('taker_buy_volume');
    if (compareDecimal(otbq, k.taker_buy_quote) !== 0) bad.push('taker_buy_quote');
    if (bad.length) {
      const rec = {
        minute: m, fields: bad,
        tf: { n_trades: on, volume: ov, quote_volume: oq, taker_buy_volume: otb, taker_buy_quote: otbq },
        kline: { count: k.count, volume: k.volume, quote_volume: k.quote_volume, taker_buy_volume: k.taker_buy_volume, taker_buy_quote: k.taker_buy_quote }
      };
      const cmp = compareDecimal(on, k.count);
      const g = known.find((x) => m >= x.from && m < x.to && ((x.expect === 'tf_short' && cmp < 0) || (x.expect === 'kline_short' && cmp > 0)));
      if (g) knownGaps.push({ ...rec, known_gap: g.id });
      else mismatches.push(rec);
    }
  }
  const clean = mismatches.length === 0 && out_of_range === 0 && time_reversed === 0;
  const status = !clean ? 'FAIL' : knownGaps.length ? 'PASS_KNOWN' : 'PASS';
  return {
    status,
    target_minutes: sorted.length,
    mismatch_minutes: mismatches.length,
    mismatches,
    known_gap_minutes: knownGaps.length,
    known_gaps: knownGaps,
    rel_count: relDiff(tn, kn),
    rel_volume: relDiff(tv, kv),
    rel_quote: relDiff(tq, kq),
    tf_totals: { n_trades: tn, volume: tv, quote: tq, taker_buy_volume: ttb, taker_buy_quote: ttbq },
    kline_totals: { count: kn, volume: kv, quote: kq, taker_buy_volume: ktb, taker_buy_quote: ktbq }
  };
}

// 같은 시장 같은 분에서 2종목 이상 함께 틀리면 원본 사건 → 연속 FAIL 수에서 뺀다 (2026-10-21 대장님 지시).
// 어긋난 분이 전부 다른 종목도 틀린 분(peerMinutes)이고, 범위 밖·시간 역순이 없을 때만 원본 사건으로 본다.
export function sharedFailExplained({ minutes, out_of_range = 0, time_reversed = 0 }, peerMinutes) {
  const unique = minutes.filter((m) => !peerMinutes.has(m));
  return { explained: minutes.length > 0 && out_of_range === 0 && time_reversed === 0 && unique.length === 0, unique };
}

// tf_1m 행들의 월 합계 5개 (기준값 대조용)
export function monthTotals(rows) {
  let n = '0', v = '0', q = '0', tb = '0', tbq = '0';
  for (const r of rows) {
    n = addDecimal(n, String(r.n_trades));
    v = addDecimal(v, addDecimal(r.buy_qty, r.sell_qty));
    q = addDecimal(q, addDecimal(r.buy_quote, r.sell_quote));
    tb = addDecimal(tb, r.buy_qty);
    tbq = addDecimal(tbq, r.buy_quote);
  }
  return { n_trades: n, volume: v, quote: q, taker_buy_volume: tb, taker_buy_quote: tbq };
}

// ---------- h6_run 공용: 단위 행 쓰기와 기준값 비교 ----------
export const TF_COLS = ['exchange', 'symbol', 'minute', ...TF_FIELDS];
export const OF_COLS = ['exchange', 'symbol', 'minute', 'n_agg', 'n_trades', 'buy_qty', 'sell_qty', 'buy_quote', 'sell_quote', 'max_agg_quote',
  'buy_n_ge_10k', 'buy_quote_ge_10k', 'sell_n_ge_10k', 'sell_quote_ge_10k',
  'buy_n_ge_100k', 'buy_quote_ge_100k', 'sell_n_ge_100k', 'sell_quote_ge_100k',
  'buy_n_ge_1m', 'buy_quote_ge_1m', 'sell_n_ge_1m', 'sell_quote_ge_1m',
  'first_agg_id', 'last_agg_id', 'id_gaps'];

// 한 트랜잭션: 그 key의 c2_mismatch와 (exchange, symbol, 월 범위) 행을 지우고 다시 쓴다.
// C2 결과와 무관하게 행을 쓴다 (trades §2-3, aggTrades §2-4). mismatches는 주어진 것을 그대로 남긴다.
export function writeUnitRows(db, { kind, key, exchange, symbol, month }, rows, mismatches) {
  const table = kind === 'trades' ? 'tf_1m' : 'of_1m';
  const cols = kind === 'trades' ? TF_COLS : OF_COLS;
  const [ms, me] = monthRange(month);
  const ins = db.prepare(`INSERT OR REPLACE INTO ${table}(${cols.join(',')}) VALUES(${cols.map(() => '?').join(',')})`);
  const insM = db.prepare('INSERT OR REPLACE INTO c2_mismatch(key,minute,of_json,kline_json) VALUES(?,?,?,?)');
  db.exec('BEGIN');
  try {
    db.prepare('DELETE FROM c2_mismatch WHERE key=?').run(key);
    for (const m of mismatches) insM.run(key, m.minute, JSON.stringify(kind === 'trades' ? m.tf : m.of), JSON.stringify(m.kline));
    db.prepare(`DELETE FROM ${table} WHERE exchange=? AND symbol=? AND minute>=? AND minute<?`).run(exchange, symbol, ms, me);
    for (const r of rows) ins.run(...cols.map((c) => r[c]));
    db.exec('COMMIT');
  } catch (e) {
    db.exec('ROLLBACK');
    throw e;
  }
  return rows.length;
}

// 기준값(h6x_known.json trades_pilot 한 항목)과 단위 결과 비교 → 다른 항목 목록 (빈 배열이면 같음)
export function knownDiffX(pin, { sha256, c2, agg, totals }) {
  const d = [];
  const eq = (name, a, b) => { if (String(a) !== String(b)) d.push(`${name}: ${a} != ${b}`); };
  const eqDec = (name, a, b) => { if (a === undefined || compareDecimal(String(a), String(b)) !== 0) d.push(`${name}: ${a} != ${b}`); };
  eq('zip_sha256', sha256, pin.zip_sha256);
  eq('c2_status', c2.status, pin.c2_status);
  eq('target_minutes', c2.target_minutes, pin.target_minutes);
  eq('mismatch_minutes', c2.mismatch_minutes, pin.mismatch_minutes);
  eq('id_gaps', agg.id_gaps, pin.id_gaps);
  eq('quote_ne_pxq', agg.quote_ne_pxq, pin.quote_ne_pxq);
  for (const f of Object.keys(pin.totals)) eqDec(`totals.${f}`, totals[f], pin.totals[f]);
  if (pin.kline_totals) for (const f of Object.keys(pin.kline_totals)) eqDec(`kline_totals.${f}`, c2.kline_totals[f], pin.kline_totals[f]);
  return d;
}
