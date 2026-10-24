// Source: ~/TradeTerm-d3 commit 9531e5e src/market.mjs
// sha256(src/market.mjs @ 9531e5e) = 3873a9e7f44e5205260fbbcb999d96a8e1da1ed4f185e1c93da34c55a922619d
import crypto from 'node:crypto';
import fs from 'node:fs';

export function decimal(value) {
  let s = String(value);
  if (!/^-?\d+(\.\d+)?(e[+-]?\d+)?$/i.test(s)) throw new Error('Invalid decimal');
  const negative = s.startsWith('-'); if (negative) s = s.slice(1);
  const [base, exp = '0'] = s.toLowerCase().split('e');
  const [whole, fraction = ''] = base.split('.');
  const digits = whole + fraction, point = whole.length + Number(exp);
  s = point <= 0 ? '0.' + '0'.repeat(-point) + digits : point >= digits.length ? digits + '0'.repeat(point - digits.length) : digits.slice(0, point) + '.' + digits.slice(point);
  let [a, b = ''] = s.split('.'); a = a.replace(/^0+(?=\d)/, ''); b = b.replace(/0+$/, '');
  s = a + (b ? '.' + b : ''); return negative && s !== '0' ? '-' + s : s;
}

function parts(s) {
  const v = decimal(s), scale = (v.split('.')[1] || '').length;
  return [BigInt(v.replace('.', '')), scale];
}

function format(n, scale) {
  const sign = n < 0n ? '-' : '';
  const s = (n < 0n ? -n : n).toString().padStart(scale + 1, '0');
  return decimal(sign + (scale ? s.slice(0, -scale) + '.' + s.slice(-scale) : s));
}

export function addDecimal(a, b) {
  const [x, p] = parts(a), [y, q] = parts(b), scale = Math.max(p, q);
  return format(x * 10n ** BigInt(scale - p) + y * 10n ** BigInt(scale - q), scale);
}

export function subDecimal(a, b) {
  const [x, p] = parts(a), [y, q] = parts(b), scale = Math.max(p, q);
  return format(x * 10n ** BigInt(scale - p) - y * 10n ** BigInt(scale - q), scale);
}

export function absDiffDecimal(a, b) {
  const [x, p] = parts(a), [y, q] = parts(b), scale = Math.max(p, q);
  const diff = x * 10n ** BigInt(scale - p) - y * 10n ** BigInt(scale - q);
  return format(diff < 0n ? -diff : diff, scale);
}

export function compareDecimal(a, b) {
  const [x, p] = parts(a), [y, q] = parts(b), scale = Math.max(p, q);
  const lhs = x * 10n ** BigInt(scale - p);
  const rhs = y * 10n ** BigInt(scale - q);
  return lhs < rhs ? -1 : lhs > rhs ? 1 : 0;
}

export function normalizeTimestamp(raw) {
  const s = String(raw).trim();
  if (!/^\d+$/.test(s)) {
    throw new Error(`Invalid timestamp (non-digits): ${s}`);
  }
  if (s.length === 13) {
    return Number(s);
  }
  if (s.length === 16) {
    return Number(BigInt(s) / 1000n);
  }
  throw new Error(`Invalid timestamp length (${s.length}): ${s}`);
}

export function isNumericCell(cell) {
  return /^-?\d+(\.\d+)?$/.test(String(cell).trim());
}

export function parseKlineCsv(csvText, exchange, symbol, timeframe) {
  const lines = csvText.split(/\r?\n/);
  const rows = [];
  let nonEmptyIdx = 0;

  for (let i = 0; i < lines.length; i++) {
    const line = lines[i].trim();
    if (!line) continue;
    const cols = line.split(',');
    const firstCell = cols[0].trim();

    if (!isNumericCell(firstCell)) {
      if (nonEmptyIdx === 0) {
        nonEmptyIdx++;
        continue; // Skip header on first line
      }
      throw new Error(`Unexpected header or non-numeric first cell at line ${i + 1}: ${firstCell}`);
    }
    nonEmptyIdx++;

    if (cols.length < 8) {
      throw new Error(`Malformed kline row at line ${i + 1}: expected >=8 columns, got ${cols.length}`);
    }

    const time = normalizeTimestamp(firstCell);
    const closeTime = normalizeTimestamp(cols[6].trim());
    rows.push({
      exchange,
      symbol,
      timeframe,
      time,
      open: decimal(cols[1].trim()),
      high: decimal(cols[2].trim()),
      low: decimal(cols[3].trim()),
      close: decimal(cols[4].trim()),
      volume: decimal(cols[5].trim()),
      quote_volume: decimal(cols[7].trim()),
      close_time: closeTime,
      trade_count: Number(cols[8]?.trim() || 0)
    });
  }
  return rows;
}

export function parseFundingCsv(csvText, exchange, symbol) {
  const lines = csvText.split(/\r?\n/);
  const rows = [];
  let nonEmptyIdx = 0;

  for (let i = 0; i < lines.length; i++) {
    const line = lines[i].trim();
    if (!line) continue;
    const cols = line.split(',');
    const firstCell = cols[0].trim();

    if (!isNumericCell(firstCell)) {
      if (nonEmptyIdx === 0) {
        nonEmptyIdx++;
        continue;
      }
      throw new Error(`Unexpected header in funding CSV at line ${i + 1}: ${firstCell}`);
    }
    nonEmptyIdx++;

    const time = normalizeTimestamp(firstCell);
    const intervalHours = Number(cols[1].trim());
    const rate = decimal(cols[2].trim());
    rows.push({ exchange, symbol, time, interval_hours: intervalHours, rate });
  }
  return rows;
}

export function parseMetricsCsv(csvText, exchange, symbol) {
  const lines = csvText.split(/\r?\n/);
  const byTime = new Map();
  let nonEmptyIdx = 0;

  for (let i = 0; i < lines.length; i++) {
    const line = lines[i].trim();
    if (!line) continue;
    const cols = line.split(',');
    const firstCell = cols[0].trim();

    // Metrics first column is "YYYY-MM-DD HH:MM:SS" (starts with 4 digits) or header "create_time"
    if (!/^\d/.test(firstCell)) {
      if (nonEmptyIdx === 0) {
        nonEmptyIdx++;
        continue;
      }
      throw new Error(`Unexpected header in metrics CSV at line ${i + 1}: ${firstCell}`);
    }
    nonEmptyIdx++;

    let time;
    if (/^\d+$/.test(firstCell)) {
      time = normalizeTimestamp(firstCell);
    } else if (/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$/.test(firstCell)) {
      time = Date.parse(firstCell.replace(' ', 'T') + 'Z');
    } else {
      throw new Error(`Invalid metrics timestamp at line ${i + 1}: ${firstCell}`);
    }

    const normDec = v => {
      const t = (v ?? '').trim().replace(/^"+|"+$/g, '').trim();
      if (!t || t === 'NaN' || t === 'null' || t === 'Infinity' || t === '-Infinity') return '0';
      return decimal(t);
    };

    byTime.set(time, {
      exchange,
      symbol,
      time,
      create_time: firstCell,
      sum_open_interest: normDec(cols[2]),
      sum_open_interest_value: normDec(cols[3]),
      count_toptrader_long_short_ratio: normDec(cols[4]),
      sum_toptrader_long_short_ratio: normDec(cols[5]),
      count_long_short_ratio: normDec(cols[6]),
      sum_taker_long_short_vol_ratio: normDec(cols[7])
    });
  }
  return [...byTime.values()];
}

export function parseAggTradeLine(line, lineNumber, isFirstNonEmpty) {
  const trimmed = line.trim();
  if (!trimmed) return { type: 'empty' };
  const cols = trimmed.split(',');
  const firstCell = cols[0].trim();
  if (!isNumericCell(firstCell)) {
    if (isFirstNonEmpty) return { type: 'header' };
    throw new Error(`Unexpected header in aggTrades at line ${lineNumber}: ${firstCell}`);
  }
  const aggId = Number(firstCell);
  const price = decimal(cols[1].trim());
  const qty = decimal(cols[2].trim());
  const firstId = Number(cols[3].trim());
  const lastId = Number(cols[4].trim());
  const time = normalizeTimestamp(cols[5].trim());
  const buyerMaker = /^(true|1)$/i.test(cols[6].trim()) ? 1 : 0;
  return {
    type: 'row',
    row: { agg_id: aggId, price, qty, first_id: firstId, last_id: lastId, time, buyer_maker: buyerMaker }
  };
}

export function computeFileSha256(filePath) {
  const hash = crypto.createHash('sha256');
  const buf = fs.readFileSync(filePath);
  hash.update(buf);
  return { sha256: hash.digest('hex'), size: buf.length };
}

export function verifyZipChecksum(zipPath, checksumPath) {
  if (!fs.existsSync(zipPath)) throw new Error(`Missing zip file: ${zipPath}`);
  if (!fs.existsSync(checksumPath)) throw new Error(`Missing checksum file: ${checksumPath}`);
  const csText = fs.readFileSync(checksumPath, 'utf8').trim();
  const expectedSha = (csText.split(/\s+/)[0] || '').toLowerCase();
  if (!/^[0-9a-f]{64}$/.test(expectedSha)) {
    throw new Error(`Invalid checksum file content in ${checksumPath}`);
  }
  const { sha256, size } = computeFileSha256(zipPath);
  if (sha256.toLowerCase() !== expectedSha) {
    throw new Error(`Checksum mismatch for ${zipPath}: expected ${expectedSha}, got ${sha256}`);
  }
  return { sha256: expectedSha, size };
}
