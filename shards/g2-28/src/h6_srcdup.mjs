// H6X 통째 중복 원본 — zip 안 CSV가 같은 내용을 두 번 이어 붙인 경우 (바이낸스 spot ETHUSDT-aggTrades-2021-12에서 확인).
// 뒷 절반이 앞 절반과 바이트까지 같을 때만 앞 절반을 원본으로 읽는다. 조금이라도 다르면 중복으로 보지 않는다. (2026-10-21 대장님 지시)
import { spawn } from 'node:child_process';
import { createHash } from 'node:crypto';
import { PassThrough } from 'node:stream';

// unzip -p 의 stdout에서 앞 skipBytes 바이트를 버리고, 그 뒤 limitBytes 바이트에서 자른다 (둘 다 없으면 그대로).
// 자른 뒤 unzip을 끝내므로, 종료 코드 판단은 check(code)로 한다: 끝까지 읽었거나 정확히 limitBytes에서 잘랐으면 null, 아니면 이유 글.
export function cutStdout(child, limitBytes, skipBytes = 0) {
  if (limitBytes == null && !skipBytes) return { out: child.stdout, check: (code) => (code === 0 ? null : `exit=${code}`) };
  const out = new PassThrough();
  let skip = skipBytes || 0;
  let left = limitBytes == null ? Infinity : limitBytes;
  let done = false;
  child.stdout.on('data', (b) => {
    if (done) return;
    if (skip > 0) {
      if (b.length <= skip) { skip -= b.length; return; }
      b = b.subarray(skip);
      skip = 0;
    }
    const part = b.length <= left ? b : b.subarray(0, left);
    left -= part.length;
    const more = out.write(part);
    if (left === 0) { done = true; out.end(); child.stdout.destroy(); child.kill('SIGTERM'); }
    else if (!more) { child.stdout.pause(); out.once('drain', () => child.stdout.resume()); }
  });
  child.stdout.on('end', () => { if (!done) out.end(); });
  return {
    out,
    check: (code) => {
      if (skip > 0) return `ended ${skip} bytes before skip ${skipBytes} (exit=${code})`;
      if (limitBytes == null) return code === 0 ? null : `exit=${code}`;
      return done ? null : `ended ${left} bytes before cut ${limitBytes} (exit=${code})`;
    }
  };
}

function streamZip(zipPath, onChunk) {
  return new Promise((resolve, reject) => {
    const child = spawn('unzip', ['-p', zipPath], { stdio: ['ignore', 'pipe', 'pipe'] });
    const errChunks = [];
    let total = 0;
    child.stderr.on('data', (d) => errChunks.push(d));
    child.stdout.on('data', (b) => { onChunk(b, total); total += b.length; });
    child.on('error', reject);
    child.on('close', (code) => {
      if (code !== 0) return reject(new Error(`unzip -p ${zipPath} exit=${code} ${Buffer.concat(errChunks).toString().slice(0, 200)}`));
      resolve(total);
    });
  });
}

function countNl(b) {
  let n = 0;
  for (let i = b.indexOf(10); i !== -1; i = b.indexOf(10, i + 1)) n++;
  return n;
}

// 두 번 흘려 읽는다: 1) 전체 바이트 수  2) 앞 절반·뒷 절반 md5, 앞 절반 줄 수, 앞 절반 마지막 바이트.
// dup = 전체가 짝수 바이트 && 앞 절반이 줄바꿈으로 끝남 && 두 절반 md5 같음 && 두 번 읽은 길이가 같음
export async function wholeDupCheck(zipPath) {
  const total = await streamZip(zipPath, () => {});
  if (total === 0 || total % 2 !== 0) return { dup: false, total };
  const half = total / 2;
  const ha = createHash('md5'), hb = createHash('md5');
  let halfLines = 0, lastOfFirst = -1;
  const total2 = await streamZip(zipPath, (b, pos) => {
    const end = pos + b.length;
    if (end <= half) { ha.update(b); halfLines += countNl(b); if (end === half) lastOfFirst = b[b.length - 1]; }
    else if (pos >= half) hb.update(b);
    else {
      const a = b.subarray(0, half - pos);
      ha.update(a); halfLines += countNl(a); lastOfFirst = a[a.length - 1];
      hb.update(b.subarray(half - pos));
    }
  });
  const [ma, mb] = [ha.digest('hex'), hb.digest('hex')];
  return { dup: total2 === total && lastOfFirst === 10 && ma === mb, total, half_bytes: half, half_lines: halfLines, half_md5: ma, second_md5: mb };
}

// N벌 원본 (spot ETHUSDT-aggTrades-2018-01은 5벌, 2026-10-21 대장님 지시로 한 벌만 읽는다).
// 두 번 흘려 읽는다: 1) 전체 바이트 수  2) 같은 크기로 나눈 벌마다 md5·줄 수·마지막 바이트.
// ok = 전체가 copies로 나누어떨어짐 && 벌마다 줄바꿈으로 끝남 && 두 번 읽은 길이가 같음 (md5·줄 수 비교는 부르는 쪽이 기록과 한다)
export async function copiesCheck(zipPath, copies) {
  const total = await streamZip(zipPath, () => {});
  if (total === 0 || total % copies !== 0) return { ok: false, total };
  const size = total / copies;
  const hs = Array.from({ length: copies }, () => createHash('md5'));
  const lines = new Array(copies).fill(0);
  const last = new Array(copies).fill(-1);
  const total2 = await streamZip(zipPath, (b, pos) => {
    let off = 0;
    while (off < b.length) {
      const i = Math.floor((pos + off) / size);
      const end = Math.min(b.length, (i + 1) * size - pos);
      const part = b.subarray(off, end);
      hs[i].update(part);
      lines[i] += countNl(part);
      last[i] = part[part.length - 1];
      off = end;
    }
  });
  return { ok: total2 === total && last.every((x) => x === 10), total, copy_bytes: size, lines, md5: hs.map((h) => h.digest('hex')) };
}
