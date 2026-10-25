// H6 집계 — aggTrades 한 달 파일을 1분 단위 22칸으로 집계한다.
// 한 행의 분은 floor(time_ms/60000)×60000. 한 aggTrade의 금액 q = price×qty (정확값, 반올림 없음).
// 쪽: buyer_maker=0 → buy, 1 → sell. 큰 손 문턱 10000·100000·1000000 USDT(경계 포함).
import { parseAggTradeLine, addDecimal, absDiffDecimal, compareDecimal } from './parser.mjs';
import { divDecimal } from './h6_decimal.mjs';

export const SC_QTY = 8;   // qty 표시 자릿수
export const SC_QUOTE = 16; // 금액 표시 자릿수 (price 8자리 × qty 8자리)
const TH = [
  10000n * 10n ** 16n,
  100000n * 10n ** 16n,
  1000000n * 10n ** 16n
];

// 소수 문자열 → 값×10^8 BigInt
export function toScaled8(s) {
  let u = String(s);
  let neg = false;
  if (u.charCodeAt(0) === 45) { neg = true; u = u.slice(1); }
  const dot = u.indexOf('.');
  let whole, frac;
  if (dot < 0) { whole = u; frac = ''; }
  else { whole = u.slice(0, dot); frac = u.slice(dot + 1); }
  if (frac.length > SC_QTY) throw new Error(`more than ${SC_QTY} decimals: ${s}`);
  const v = BigInt((whole || '0') + frac.padEnd(SC_QTY, '0'));
  return neg ? -v : v;
}

// 값×10^scale BigInt → decimal()과 같은 문자열
export function fmtScaled(n, scale) {
  const neg = n < 0n;
  const a = neg ? -n : n;
  if (scale === 0) return (neg ? '-' : '') + a.toString();
  const s = a.toString().padStart(scale + 1, '0');
  const frac = s.slice(-scale).replace(/0+$/, '');
  const whole = s.slice(0, s.length - scale);
  return (neg ? '-' : '') + whole + (frac ? '.' + frac : '');
}

export function monthBounds(month) {
  const [y, m] = month.split('-').map(Number);
  const start = Date.UTC(y, m - 1, 1);
  const end = Date.UTC(m === 12 ? y + 1 : y, m === 12 ? 0 : m, 1);
  return [start, end];
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

// 22칸 행 (of_1m 컬럼과 같은 이름)
export function rowFromAcc(a, exchange, symbol) {
  return {
    exchange,
    symbol,
    minute: a.minute,
    n_agg: a.n_agg,
    n_trades: a.n_trades,
    buy_qty: fmtScaled(a.buy_qty, SC_QTY),
    sell_qty: fmtScaled(a.sell_qty, SC_QTY),
    buy_quote: fmtScaled(a.buy_quote, SC_QUOTE),
    sell_quote: fmtScaled(a.sell_quote, SC_QUOTE),
    max_agg_quote: fmtScaled(a.max_agg_quote, SC_QUOTE),
    buy_n_ge_10k: a.w[0].n,
    buy_quote_ge_10k: fmtScaled(a.w[0].q, SC_QUOTE),
    sell_n_ge_10k: a.s[0].n,
    sell_quote_ge_10k: fmtScaled(a.s[0].q, SC_QUOTE),
    buy_n_ge_100k: a.w[1].n,
    buy_quote_ge_100k: fmtScaled(a.w[1].q, SC_QUOTE),
    sell_n_ge_100k: a.s[1].n,
    sell_quote_ge_100k: fmtScaled(a.s[1].q, SC_QUOTE),
    buy_n_ge_1m: a.w[2].n,
    buy_quote_ge_1m: fmtScaled(a.w[2].q, SC_QUOTE),
    sell_n_ge_1m: a.s[2].n,
    sell_quote_ge_1m: fmtScaled(a.s[2].q, SC_QUOTE),
    first_agg_id: a.first_agg_id,
    last_agg_id: a.last_agg_id,
    id_gaps: a.id_gaps
  };
}

export const ROW_FIELDS = Object.keys(rowFromAcc(newAcc(0), 'x', 'y'));

// 줄 이터러블(동기·비동기) → 집계 결과
export async function aggregateLines(lines, { exchange, symbol, month }) {
  const [mStart, mEnd] = monthBounds(month);
  const byMinute = new Map();
  let prevTime = null;
  let prevLastId = null;
  let n_agg = 0, n_trades = 0, idGaps = 0, outOfRange = 0, reversed = 0, placeholders = 0;
  let rowsParsed = 0, lineNo = 0;
  const outExamples = [], revExamples = [], phExamples = [];

  let seenNonEmpty = false;

  for await (const line of lines) {
    lineNo++;
    const p = parseAggTradeLine(line, lineNo, !seenNonEmpty);
    if (p.type === 'empty') continue;
    seenNonEmpty = true;
    if (p.type === 'header') continue;
    const r = p.row;
    rowsParsed++;

    // 가짜 거래 줄 (가격 0·수량 0·첫/끝 거래 id -1, 바이낸스 spot BTCUSDT-aggTrades-2018-01): 집계·시간 역행·id 공백 판단에서 빼고 따로 센다 (2026-10-21 대장님 지시)
    if (r.first_id === -1 && r.last_id === -1 && toScaled8(r.price) === 0n && toScaled8(r.qty) === 0n) {
      placeholders++;
      if (phExamples.length < 5) phExamples.push({ line: lineNo, agg_id: r.agg_id, time: r.time });
      continue;
    }

    // 시간 역행
    if (prevTime !== null && r.time < prevTime) {
      reversed++;
      if (revExamples.length < 5) revExamples.push({ line: lineNo, prev_time: prevTime, time: r.time });
    }
    prevTime = r.time;

    // id 공백 (월 파일 안에서만, 이번 행의 분에 센다)
    const hasGap = prevLastId !== null && r.first_id !== prevLastId + 1;
    prevLastId = r.last_id;

    // 월 범위 밖
    if (r.time < mStart || r.time >= mEnd) {
      outOfRange++;
      if (outExamples.length < 5) outExamples.push({ line: lineNo, time: r.time });
      continue;
    }

    const minute = Math.floor(r.time / 60000) * 60000;
    let acc = byMinute.get(minute);
    if (acc === undefined) { acc = newAcc(minute); byMinute.set(minute, acc); }

    const p8 = toScaled8(r.price);
    const q8 = toScaled8(r.qty);
    const quote = p8 * q8;

    acc.n_agg++;
    n_agg++;
    const trades = r.last_id - r.first_id + 1;
    acc.n_trades += trades;
    n_trades += trades;

    if (quote > acc.max_agg_quote) acc.max_agg_quote = quote;

    const th = r.buyer_maker === 0 ? acc.w : acc.s;
    if (r.buyer_maker === 0) { acc.buy_qty += q8; acc.buy_quote += quote; }
    else { acc.sell_qty += q8; acc.sell_quote += quote; }

    for (let i = 0; i < 3; i++) {
      if (quote >= TH[i]) { th[i].n++; th[i].q += quote; }
    }

    if (acc.first_agg_id === null || r.agg_id < acc.first_agg_id) acc.first_agg_id = r.agg_id;
    if (acc.last_agg_id === null || r.agg_id > acc.last_agg_id) acc.last_agg_id = r.agg_id;

    if (hasGap) { acc.id_gaps++; idGaps++; }
  }

  return {
    byMinute,
    n_agg,
    n_trades,
    id_gaps_total: idGaps,
    out_of_range: outOfRange,
    time_reversed: reversed,
    placeholders,
    placeholder_examples: phExamples,
    rows_parsed: rowsParsed,
    lines_total: lineNo,
    out_examples: outExamples,
    rev_examples: revExamples
  };
}

export function rowsFromAgg(agg, exchange, symbol) {
  return [...agg.byMinute.values()]
    .sort((a, b) => a.minute - b.minute)
    .map((a) => rowFromAcc(a, exchange, symbol));
}

const ZERO_K = { count: '0', volume: '0', quote_volume: '0', taker_buy_volume: '0', taker_buy_quote: '0' };

// C2 — 로컬 kline 1m 대조. 대상 분은 kline count>0 인 분과 of_1m 분의 합집합.
// 판정: PASS(어긋난 분 0) / PASS_TOL(어긋난 분 비율 ≤ 0.005 이고 count·volume·quote 월합 상대오차 셋 다 ≤ 1e-4)
//       / 선물(um) 전용 PASS_SHIFT(월합 volume·quote·taker_buy·taker_buy_quote 상대오차 각 ≤ 1e-6, count ≤ 1e-3, disp ≤ 0.01) / FAIL
// disp = Σ|of−kline| volume(전체 분) ÷ (2 × kline 월합 volume) — 분 경계를 건너 이동한 물량 비율 (정정 지시서 §S3-fix-3).
// out_of_range>0 또는 time_reversed>0 이면 시장과 관계없이 FAIL. PASS_SHIFT 도 행을 쓰고 FAIL 계수에는 안 든다.
export function judgeC2({ rows, kline, out_of_range = 0, time_reversed = 0, market = null }) {
  const ofByMinute = new Map();
  for (const r of rows) ofByMinute.set(r.minute, r);

  const minutes = new Set([...kline.keys(), ...ofByMinute.keys()]);
  const sorted = [...minutes].sort((a, b) => a - b);

  let mismatch = 0;
  const mismatches = [];
  let of_count = '0', of_volume = '0', of_quote = '0';
  let kl_count = '0', kl_volume = '0', kl_quote = '0';
  let of_tbv = '0', of_tbq = '0', kl_tbv = '0', kl_tbq = '0';
  let sumAbsVol = '0';

  for (const m of sorted) {
    const k = kline.get(m) || ZERO_K;
    const o = ofByMinute.get(m);
    const ov = o ? addDecimal(o.buy_qty, o.sell_qty) : '0';
    const oq = o ? addDecimal(o.buy_quote, o.sell_quote) : '0';
    const otb = o ? o.buy_qty : '0';
    const otbq = o ? o.buy_quote : '0';
    const on = o ? String(o.n_trades) : '0';

    of_count = addDecimal(of_count, on);
    of_volume = addDecimal(of_volume, ov);
    of_quote = addDecimal(of_quote, oq);
    kl_count = addDecimal(kl_count, k.count);
    kl_volume = addDecimal(kl_volume, k.volume);
    kl_quote = addDecimal(kl_quote, k.quote_volume);
    of_tbv = addDecimal(of_tbv, otb);
    of_tbq = addDecimal(of_tbq, otbq);
    kl_tbv = addDecimal(kl_tbv, k.taker_buy_volume);
    kl_tbq = addDecimal(kl_tbq, k.taker_buy_quote);
    sumAbsVol = addDecimal(sumAbsVol, absDiffDecimal(ov, k.volume));

    const bad = [];
    if (compareDecimal(on, k.count) !== 0) bad.push('n_trades');
    if (compareDecimal(ov, k.volume) !== 0) bad.push('volume');
    if (compareDecimal(oq, k.quote_volume) !== 0) bad.push('quote_volume');
    if (compareDecimal(otb, k.taker_buy_volume) !== 0) bad.push('taker_buy_volume');
    if (compareDecimal(otbq, k.taker_buy_quote) !== 0) bad.push('taker_buy_quote');
    if (bad.length) {
      mismatch++;
      if (mismatches.length < 20) {
        mismatches.push({
          minute: m,
          fields: bad,
          of: { n_trades: on, volume: ov, quote_volume: oq, taker_buy_volume: otb, taker_buy_quote: otbq },
          kline: { count: k.count, volume: k.volume, quote_volume: k.quote_volume, taker_buy_volume: k.taker_buy_volume, taker_buy_quote: k.taker_buy_quote }
        });
      }
    }
  }

  const rel = (a, b) => {
    if (compareDecimal(b, '0') === 0) return compareDecimal(a, '0') === 0 ? '0' : '1';
    return divDecimal(aAbsDiff(a, b), b, 10);
  };
  const rel_count = rel(of_count, kl_count);
  const rel_volume = rel(of_volume, kl_volume);
  const rel_quote = rel(of_quote, kl_quote);
  const rel_taker_buy = rel(of_tbv, kl_tbv);
  const rel_taker_buy_quote = rel(of_tbq, kl_tbq);
  const disp = compareDecimal(kl_volume, '0') === 0
    ? (compareDecimal(sumAbsVol, '0') === 0 ? '0' : '1')
    : divDecimal(sumAbsVol, addDecimal(kl_volume, kl_volume), 10);

  const target = sorted.length;
  let status;
  if (out_of_range > 0 || time_reversed > 0) status = 'FAIL';
  else if (mismatch === 0) status = 'PASS';
  else {
    const withinRatio = target > 0 && BigInt(mismatch) * 200n <= BigInt(target);
    const withinRel = compareDecimal(rel_count, '0.0001') <= 0
      && compareDecimal(rel_volume, '0.0001') <= 0
      && compareDecimal(rel_quote, '0.0001') <= 0;
    if (withinRatio && withinRel) status = 'PASS_TOL';
    else if (market === 'um') {
      const withinShift = compareDecimal(rel_volume, '0.000001') <= 0
        && compareDecimal(rel_quote, '0.000001') <= 0
        && compareDecimal(rel_taker_buy, '0.000001') <= 0
        && compareDecimal(rel_taker_buy_quote, '0.000001') <= 0
        && compareDecimal(rel_count, '0.001') <= 0
        && compareDecimal(disp, '0.01') <= 0;
      status = withinShift ? 'PASS_SHIFT' : 'FAIL';
    } else status = 'FAIL';
  }

  return {
    status,
    target_minutes: target,
    mismatch_minutes: mismatch,
    mismatches,
    rel_count,
    rel_volume,
    rel_quote,
    rel_taker_buy,
    rel_taker_buy_quote,
    disp,
    of_totals: { n_trades: of_count, volume: of_volume, quote_volume: of_quote, taker_buy_volume: of_tbv, taker_buy_quote: of_tbq },
    kline_totals: { count: kl_count, volume: kl_volume, quote_volume: kl_quote, taker_buy_volume: kl_tbv, taker_buy_quote: kl_tbq }
  };
}

function aAbsDiff(a, b) {
  const c = compareDecimal(a, b);
  if (c === 0) return '0';
  const [ia, pa] = scaledParts(a);
  const [ib, pb] = scaledParts(b);
  const scale = Math.max(pa, pb);
  const d = ia * 10n ** BigInt(scale - pa) - ib * 10n ** BigInt(scale - pb);
  return fmtScaled(d < 0n ? -d : d, scale);
}

function scaledParts(v) {
  const s = String(v);
  const neg = s.startsWith('-');
  const t = neg ? s.slice(1) : s;
  const dot = t.indexOf('.');
  const whole = dot < 0 ? t : t.slice(0, dot);
  const frac = dot < 0 ? '' : t.slice(dot + 1);
  return [BigInt(whole + frac), frac.length];
}
