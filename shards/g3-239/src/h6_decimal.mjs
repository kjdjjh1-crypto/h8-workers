// H6 소수 — BigInt 정확 계산. decimal()/addDecimal()/compareDecimal()은 parser.mjs에서 그대로 쓴다.
// 곱셈·나눗셈만 여기에 둔다. 저장 형식은 parser.mjs decimal()과 같게: 일반 표기, 끝자리 0 제거, 0은 "0", 반올림 없음.
import { decimal } from './parser.mjs';

// "123.45" → [12345n, 소수자리수, 음수여부]
function scaled(v) {
  const s = decimal(v);
  const neg = s.startsWith('-');
  const t = neg ? s.slice(1) : s;
  const dot = t.indexOf('.');
  const whole = dot < 0 ? t : t.slice(0, dot);
  const frac = dot < 0 ? '' : t.slice(dot + 1);
  return [BigInt(whole + frac), frac.length, neg];
}

// [절댓값 BigInt, 자릿수, 음수여부] → 문자열
function build(n, scale, neg) {
  const v = neg ? -n : n;
  if (scale === 0) return decimal((neg ? '-' : '') + n.toString());
  const s = n.toString().padStart(scale + 1, '0');
  const frac = s.slice(-scale).replace(/0+$/, '');
  const whole = s.slice(0, s.length - scale);
  return (neg && (n !== 0n) ? '-' : '') + whole + (frac ? '.' + frac : '');
}

// a × b, 정확값. price·qty는 소수 8자리 이하라 곱은 16자리 이하다.
export function mulDecimal(a, b) {
  const [x, px, nx] = scaled(a);
  const [y, py, ny] = scaled(b);
  return build(x * y, px + py, nx !== ny);
}

// a ÷ b 를 dp자리로 반올림(절반 올림, 절댓값 기준). 상대오차 계산 전용.
export function divDecimal(a, b, dp = 10) {
  const [x, px, nx] = scaled(a);
  const [y, py, ny] = scaled(b);
  if (y === 0n) throw new Error('divDecimal: division by zero');
  const num = x * 10n ** BigInt(py + dp);
  const den = y * 10n ** BigInt(px);
  let q = num / den;
  const r = num % den;
  if (r * 2n >= den) q += 1n;
  return build(q, dp, nx !== ny);
}
