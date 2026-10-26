# H13 wave2 자료 워커 — data.binance.vision 공개 파일을 받아 sha256(CHECKSUM) 확인 후 1분 단위로 줄여 result/에 쓴다.
import os, sys, json, gzip, io, zipfile, hashlib, subprocess, urllib.request, urllib.error, calendar, time
from concurrent.futures import ProcessPoolExecutor
import numpy as np, pandas as pd
BASE = 'https://data.binance.vision/data/'
TMP = os.path.abspath('tmp'); OUT = os.path.abspath('result')

def get(url, path):
    for k in range(5):
        try:
            with urllib.request.urlopen(url, timeout=120) as r, open(path, 'wb') as f:
                while True:
                    b = r.read(1 << 22)
                    if not b: break
                    f.write(b)
            return True
        except urllib.error.HTTPError as e:
            if e.code == 404: return False
            time.sleep(5 * (k + 1))
        except Exception:
            time.sleep(5 * (k + 1))
    raise RuntimeError('download failed ' + url)

def fetch(rel):
    """zip 경로 → (로컬 경로, sha256) 또는 None(404). CHECKSUM 불일치는 예외."""
    p = os.path.join(TMP, rel.replace('/', '_'))
    if not get(BASE + rel, p): return None
    h = hashlib.sha256()
    with open(p, 'rb') as f:
        for b in iter(lambda: f.read(1 << 22), b''): h.update(b)
    sha = h.hexdigest()
    c = p + '.CHECKSUM'
    if get(BASE + rel + '.CHECKSUM', c):
        want = open(c).read().split()[0]
        os.remove(c)
        if want != sha: os.remove(p); raise RuntimeError('checksum mismatch ' + rel)
    return p, sha

def read_zip_csv(p, names, usecols, dtype=None, chunks=None):
    z = zipfile.ZipFile(p); n = z.namelist()[0]
    with z.open(n) as f: first = f.readline().decode()
    head = 0 if not first[:1].isdigit() else None
    f = z.open(n)
    return pd.read_csv(f, header=head, names=names, usecols=usecols, dtype=dtype, chunksize=chunks, engine='c')

def days(ym):
    y, m = map(int, ym.split('-'))
    return [f'{ym}-{d:02d}' for d in range(1, calendar.monthrange(y, m)[1] + 1)]

def agg_trades(s, ym):
    got = fetch(f'futures/um/monthly/aggTrades/{s}/{s}-aggTrades-{ym}.zip')
    if not got: return None, []
    p, sha = got
    names = ['id', 'price', 'qty', 'fid', 'lid', 't', 'maker']
    parts = []
    for ch in read_zip_csv(p, names, ['price', 'qty', 't', 'maker'], chunks=5_000_000):
        t = ch['t'].to_numpy(np.int64)
        t = np.where(t > 10**14, t // 1000, t)
        q = ch['price'].to_numpy(np.float64) * ch['qty'].to_numpy(np.float64)
        mk = ch['maker'].astype(str).str.lower().isin(['true', '1']).to_numpy()
        buy = ~mk
        d = pd.DataFrame({'m': t // 60000 * 60000, 'bq': np.where(buy, q, 0), 'sq': np.where(buy, 0, q),
                          'b10': (buy & (q >= 1e4)).astype(np.int32), 's10': (~buy & (q >= 1e4)).astype(np.int32),
                          'b100': np.where(buy & (q >= 1e5), q, 0), 's100': np.where(~buy & (q >= 1e5), q, 0),
                          'mxb': np.where(buy, q, 0), 'mxs': np.where(buy, 0, q), 'n': 1})
        parts.append(d.groupby('m').agg({'bq': 'sum', 'sq': 'sum', 'b10': 'sum', 's10': 'sum', 'b100': 'sum', 's100': 'sum', 'mxb': 'max', 'mxs': 'max', 'n': 'sum'}))
    os.remove(p)
    g = pd.concat(parts).groupby(level=0).agg({'bq': 'sum', 'sq': 'sum', 'b10': 'sum', 's10': 'sum', 'b100': 'sum', 's100': 'sum', 'mxb': 'max', 'mxs': 'max', 'n': 'sum'})
    return g, [sha]

def premium(s, ym):
    got = fetch(f'futures/um/monthly/premiumIndexKlines/{s}/1m/{s}-1m-{ym}.zip')
    if not got: return None, []
    p, sha = got
    d = read_zip_csv(p, ['t', 'o', 'h', 'l', 'c', 'v', 'ct', 'q', 'n', 'tb', 'tq', 'x'], ['t', 'o', 'h', 'l', 'c'])
    os.remove(p)
    d['t'] = np.where(d['t'] > 10**14, d['t'] // 1000, d['t'])
    return d.set_index('t').sort_index(), [sha]

def book(s, ym):
    parts, shas = [], []
    for day in days(ym):
        got = fetch(f'futures/um/daily/bookDepth/{s}/{s}-bookDepth-{day}.zip')
        if not got: continue
        p, sha = got; shas.append(sha)
        d = read_zip_csv(p, ['ts', 'pct', 'depth', 'notional'], ['ts', 'pct', 'notional'])
        os.remove(p)
        d = d[d['pct'].isin([-1, 1, -2, 2, -5, 5])]
        d['m'] = (pd.to_datetime(d['ts'], utc=True) - pd.Timestamp(0, tz='UTC')) // pd.Timedelta(minutes=1) * 60000
        d = d.groupby(['m', 'pct'])['notional'].last().unstack('pct')
        parts.append(d)
    if not parts: return None, []
    g = pd.concat(parts).sort_index()
    g = g.rename(columns={-1: 'b1', 1: 'a1', -2: 'b2', 2: 'a2', -5: 'b5', 5: 'a5'})
    return g.reindex(columns=['b1', 'a1', 'b2', 'a2', 'b5', 'a5']), shas

def bvol(s, ym):
    parts, shas = [], []
    for day in days(ym):
        got = fetch(f'option/daily/BVOLIndex/{s}/{s}-BVOLIndex-{day}.zip')
        if not got: continue
        p, sha = got; shas.append(sha)
        d = read_zip_csv(p, ['t', 'sym', 'b', 'q', 'v'], ['t', 'v'])
        os.remove(p)
        d['m'] = np.where(d['t'] > 10**14, d['t'] // 1000, d['t']) // 60000 * 60000
        parts.append(d.groupby('m')['v'].last())
    if not parts: return None, []
    return pd.concat(parts).sort_index().to_frame('v'), shas

FN = {'aggTrades': agg_trades, 'premiumIndexKlines': premium, 'bookDepth': book, 'BVOLIndex': bvol}

def run(job):
    kind, s, ym = job
    t0 = time.time()
    try:
        g, shas = FN[kind](s, ym)
        if g is None: return {'kind': kind, 'sym': s, 'ym': ym, 'status': 'missing', 'sec': round(time.time() - t0)}
        d = os.path.join(OUT, kind, s); os.makedirs(d, exist_ok=True)
        out = os.path.join(d, ym + '.csv.gz')
        with gzip.open(out, 'wt', compresslevel=6) as f: g.to_csv(f, index_label='t', float_format='%.10g')
        return {'kind': kind, 'sym': s, 'ym': ym, 'status': 'ok', 'rows': int(len(g)), 'src_sha256': shas, 'sec': round(time.time() - t0)}
    except Exception as e:
        return {'kind': kind, 'sym': s, 'ym': ym, 'status': 'error', 'err': str(e)[:300], 'sec': round(time.time() - t0)}

if __name__ == '__main__':
    os.makedirs(TMP, exist_ok=True); os.makedirs(OUT, exist_ok=True)
    jobs = json.load(open('jobs.json'))
    with ProcessPoolExecutor(int(os.environ.get('PAR', '4'))) as ex, open(os.path.join(OUT, 'manifest.jsonl'), 'w') as mf:
        for r in ex.map(run, [tuple(j) for j in jobs]):
            mf.write(json.dumps(r) + '\n'); mf.flush()
            print(r['status'], r['kind'], r['sym'], r['ym'], r.get('sec'), flush=True)
