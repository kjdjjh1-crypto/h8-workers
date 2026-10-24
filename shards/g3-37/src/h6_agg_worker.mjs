// 집계 일꾼 (worker_threads) — zip 하나의 무거운 줄 읽기·분 단위 합산을 본 스레드 밖에서 돌린다.
// 계산 함수는 본 스레드와 같은 것을 그대로 쓴다 (aggregateTradesZip / aggregateLines + rowsFromAgg).
// 판정·DB 쓰기·C3 재계산은 본 스레드에 남는다.
import { Worker, isMainThread, parentPort, workerData } from 'node:worker_threads';
import { spawn } from 'node:child_process';
import { createInterface } from 'node:readline';
import { aggregateLines, rowsFromAgg } from './h6_of.mjs';
import { aggregateTradesZip } from './h6x_trades.mjs';
import { cutStdout } from './h6_srcdup.mjs';

// limitBytes: 통째 중복 원본은 앞 절반 바이트까지만. skipBytes: N벌 원본에서 고른 벌 앞을 버린다 (h6_srcdup.mjs)
async function unzipStream(p, limitBytes, skipBytes) {
  const child = spawn('unzip', ['-p', p], { stdio: ['ignore', 'pipe', 'pipe'] });
  const c = cutStdout(child, limitBytes, skipBytes);
  const errChunks = [];
  child.stderr.on('data', (d) => errChunks.push(d));
  const exitP = new Promise((res) => child.on('close', (code) => res(code)));
  const rl = createInterface({ input: c.out, crlfDelay: Infinity });
  const iter = {
    [Symbol.asyncIterator]: () => (async function* () {
      try {
        for await (const line of rl) yield line;
      } finally {
        rl.close();
      }
    })()
  };
  const waitExit = async () => {
    const why = c.check(await exitP);
    if (why) throw new Error(`unzip -p ${p} ${why} ${Buffer.concat(errChunks).toString().slice(0, 200)}`);
  };
  iter.waitExit = waitExit;
  return iter;
}

// 같은 스레드에서 바로 계산 (시험에서 일꾼 결과와 맞대는 기준)
export async function runAggJob(kind, zipPath, opts) {
  if (kind === 'trades') return aggregateTradesZip(zipPath, opts);
  const lines = await unzipStream(zipPath, opts.limitBytes, opts.skipBytes);
  const agg = await aggregateLines(lines, { exchange: opts.exchange, symbol: opts.symbol, month: opts.month });
  await lines.waitExit();
  const { byMinute, ...rest } = agg;
  return { ...rest, rows: rowsFromAgg(agg, opts.exchange, opts.symbol) };
}

// 일꾼 스레드 하나를 띄워 계산하고 결과를 돌려받는다 (단위마다 새 일꾼, 끝나면 스스로 닫힌다)
export function aggInWorker(kind, zipPath, opts) {
  return new Promise((resolve, reject) => {
    const w = new Worker(new URL(import.meta.url), { workerData: { kind, zipPath, opts } });
    let settled = false;
    w.once('message', (m) => {
      settled = true;
      if (m.ok) resolve(m.result);
      else reject(new Error(m.message));
    });
    w.once('error', (e) => { if (!settled) { settled = true; reject(e); } });
    w.once('exit', (code) => { if (!settled) { settled = true; reject(new Error(`agg worker exit=${code} ${zipPath}`)); } });
  });
}

if (!isMainThread && workerData && workerData.kind) {
  const { kind, zipPath, opts } = workerData;
  runAggJob(kind, zipPath, opts).then(
    (result) => parentPort.postMessage({ ok: true, result }),
    (e) => parentPort.postMessage({ ok: false, message: e && e.message ? e.message : String(e) })
  );
}
