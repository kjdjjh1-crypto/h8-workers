# h8_remote_worker.py — 원격(Kaggle·Modal·Colab) 작업 폴더에서 실행한다. h8_remote.mjs pack 이 worker.py 로 넣는다.
# node 를 받고(sha256 확인), h8_run.mjs --only keys.txt 로 받기·집계만 하고, result/ 에 결과 행과 결과 파일을 모은다.
# 실패해도 0 으로 끝내고 result/status.json 에 적는다 (Modal 은 0 이 아니면 결과를 버린다).
import hashlib, json, os, shutil, stat, subprocess, sys, tarfile, time, urllib.request

NODE = "v26.12.0"
BASE = os.path.dirname(os.path.abspath(__file__))
RESULT = os.path.join(BASE, "result")
os.makedirs(RESULT, exist_ok=True)
status = {"node": NODE, "started": time.time()}


def save():
    status["ended"] = time.time()
    with open(os.path.join(RESULT, "status.json"), "w") as f:
        json.dump(status, f)


def node_bin():
    name = f"node-{NODE}-linux-x64"
    url = f"https://nodejs.org/dist/{NODE}/{name}.tar.xz"
    sums = urllib.request.urlopen(f"https://nodejs.org/dist/{NODE}/SHASUMS256.txt", timeout=60).read().decode()
    want = next(l.split()[0] for l in sums.splitlines() if l.endswith(f"{name}.tar.xz"))
    dst = os.path.join(BASE, "node.tar.xz")
    urllib.request.urlretrieve(url, dst)
    got = hashlib.sha256(open(dst, "rb").read()).hexdigest()
    if got != want:
        raise RuntimeError(f"node sha256 {got} != {want}")
    with tarfile.open(dst) as t:
        t.extractall(os.path.join(BASE, "node"))
    os.remove(dst)
    return os.path.join(BASE, "node", name, "bin", "node")


def unzip_shim(bindir):
    # unzip 이 없는 이미지(Modal debian_slim)용: 'unzip -p file' 만 흉내 낸다
    if shutil.which("unzip"):
        return
    p = os.path.join(bindir, "unzip")
    with open(p, "w") as f:
        f.write("#!" + sys.executable + "\n"
                "import sys, zipfile, shutil\n"
                "assert sys.argv[1] == '-p'\n"
                "z = zipfile.ZipFile(sys.argv[2])\n"
                "for n in z.namelist():\n"
                "    with z.open(n) as s: shutil.copyfileobj(s, sys.stdout.buffer, 1 << 20)\n")
    os.chmod(p, os.stat(p).st_mode | stat.S_IEXEC)
    status["unzip"] = "shim"


try:
    node = node_bin()
    bindir = os.path.join(BASE, "bin")
    os.makedirs(bindir, exist_ok=True)
    unzip_shim(bindir)
    env = dict(os.environ, PATH=bindir + os.pathsep + os.path.dirname(node) + os.pathsep + os.environ.get("PATH", ""))
    env.setdefault("H8_DISK_FLOOR_GIB", "4")  # 맥 기본 30 GiB 면 Kaggle 같은 작은 디스크에서 3시간 기다린다
    cpus = os.cpu_count() or 2
    status["cpus"] = cpus
    args = [node, "src/h8_run.mjs", "--step", os.environ.get("H8_STEP", "S5"), "--only", "keys.txt",
            "--dl", os.environ.get("H8_DL", "4"), "--agg", os.environ.get("H8_AGG", str(cpus)),
            "--gpar", os.environ.get("H8_GPAR", "2"), "--stage-gib", os.environ.get("H8_STAGE_GIB", "6")]
    status["args"] = args[1:]
    t0 = time.time()
    with open(os.path.join(RESULT, "run.log"), "w") as log:
        status["run_exit"] = subprocess.run(args, cwd=BASE, env=env, stdout=log, stderr=subprocess.STDOUT).returncode
    status["run_s"] = round(time.time() - t0, 1)
    ex = subprocess.run([node, "src/h8_remote.mjs", "export", "--keys", "keys.txt", "--out", RESULT],
                        cwd=BASE, env=env, capture_output=True, text=True)
    status["export"] = (ex.stdout or ex.stderr).strip()[-2000:]
except Exception as e:  # noqa: BLE001
    status["error"] = repr(e)[:2000]
save()
print("H8_REMOTE_STATUS=" + json.dumps({k: status.get(k) for k in ("run_exit", "run_s", "export", "error", "cpus")}))
