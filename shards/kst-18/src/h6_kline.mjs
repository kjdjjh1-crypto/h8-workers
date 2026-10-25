// H6 kline 1m — 로컬 kline zip을 unzip -p 스트림으로 흘려 읽는다 (읽기 전용, 집계 대조용).
// 칸: open_time, open, high, low, close, volume, close_time, quote_volume, count, taker_buy_volume, taker_buy_quote_volume, ignore
// count = 0 인 분은 담지 않는다 (C2에서 "없는 분"과 같다).
import { spawn } from 'node:child_process';
import { createInterface } from 'node:readline';
import { normalizeTimestamp, isNumericCell, decimal } from './parser.mjs';

export function readKline1m(zipPath) {
  return new Promise((resolve, reject) => {
    const child = spawn('unzip', ['-p', zipPath], { stdio: ['ignore', 'pipe', 'pipe'] });
    const errChunks = [];
    child.stderr.on('data', (d) => errChunks.push(d));
    const rl = createInterface({ input: child.stdout, crlfDelay: Infinity });
    const byMinute = new Map();
    let lineNo = 0;
    let seenNonEmpty = false;
    let rows = 0;

    rl.on('line', (line) => {
      try {
      lineNo++;
      const trimmed = line.trim();
      if (!trimmed) return;
      const cols = trimmed.split(',');
      const first = cols[0].trim();
      if (!isNumericCell(first)) {
        if (!seenNonEmpty) { seenNonEmpty = true; return; } // 머리줄(선물)은 첫 비어있지 않은 줄에서만
        throw new Error(`Unexpected header in kline at line ${lineNo}: ${first}`);
      }
      seenNonEmpty = true;
      const count = Number(cols[8].trim());
      const openTime = normalizeTimestamp(first);
      if (count > 0) {
        const minute = Math.floor(openTime / 60000) * 60000;
        byMinute.set(minute, {
          count: String(count),
          volume: decimal(cols[5].trim()),
          quote_volume: decimal(cols[7].trim()),
          taker_buy_volume: decimal(cols[9].trim()),
          taker_buy_quote: decimal(cols[10].trim())
        });
        rows++;
      }
      } catch (e) {
        if (!childErr) childErr = e;
        rl.close();
        child.kill('SIGTERM');
      }
    });

    let done = false;
    let rlClosed = false;
    let childCode = null;
    let childErr = null;
    const finish = () => {
      if (done) return;
      done = true;
      if (childErr) reject(childErr);
      else if (childCode !== 0) {
        reject(new Error(`unzip -p ${zipPath} exit=${childCode} stderr=${Buffer.concat(errChunks).toString().slice(0, 300)}`));
      } else {
        resolve({ byMinute, lines_total: lineNo, rows });
      }
    };

    rl.on('close', () => { rlClosed = true; if (childCode !== null) finish(); });
    child.on('close', (code) => { childCode = code; if (rlClosed) finish(); });
    child.on('error', (e) => { childErr = e; finish(); });
  });
}
