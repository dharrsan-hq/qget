#!/usr/bin/env python3
"""qget benchmark harness (PRD §14.1).

Runs qget and other downloaders against qget-testserver scenarios, verifies
every output by SHA-256, and writes a Markdown + CSV report.

Usage:  python3 -I bench/bench.py [--size-mib 1024] [--runs 3] [--quick]

Without root, network conditions are emulated by the test server
(per-connection rate caps and per-response latency) instead of tc netem.
"""

import argparse
import csv
import hashlib
import os
import platform
import resource
import shutil
import signal
import statistics
import subprocess
import sys
import tarfile
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
WORK = ROOT / "target" / "bench"
RESULTS = ROOT / "bench" / "results"


def sh(cmd, **kw):
    return subprocess.run(cmd, check=True, **kw)


def sha256(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while chunk := f.read(8 << 20):
            h.update(chunk)
    return h.hexdigest()


def tree_digest(path):
    """Order-independent digest of all regular files under path."""
    h = hashlib.sha256()
    for p in sorted(Path(path).rglob("*")):
        if p.is_file():
            h.update(str(p.relative_to(path)).encode())
            h.update(sha256(p).encode())
    return h.hexdigest()


def make_random(path, size):
    if path.exists() and path.stat().st_size == size:
        return
    with open(path, "wb") as f:
        left = size
        while left:
            n = min(left, 16 << 20)
            f.write(os.urandom(n))
            left -= n


def make_archive(dirpath, archive, size):
    """A snapshot-like tree: a few large semi-compressible files plus many small ones."""
    if archive.exists():
        return
    if dirpath.exists():
        shutil.rmtree(dirpath)
    (dirpath / "db").mkdir(parents=True)
    (dirpath / "small").mkdir()
    big = size // 4
    for i in range(3):
        with open(dirpath / "db" / f"part{i}.sst", "wb") as f:
            left = big
            while left:
                n = min(left, 4 << 20)
                # half random, half repetitive: roughly 2:1 compressible
                f.write(os.urandom(n // 2) + bytes([i]) * (n - n // 2))
                left -= n
    for i in range(2000):
        (dirpath / "small" / f"f{i}.json").write_bytes(
            (f'{{"id": {i}, "pad": "' + "x" * 300 + '"}\n').encode()
        )
    tar_path = archive.with_suffix("")
    with tarfile.open(tar_path, "w") as t:
        t.add(dirpath, arcname="snapshot")
    sh(["lz4", "-q", "-f", "-B7", str(tar_path), str(archive)])
    tar_path.unlink()


def start_server(binary, file, rate=None, latency=0, port=18080):
    args = [str(binary), str(file), "--listen", f"127.0.0.1:{port}", "--latency-ms", str(latency)]
    if rate:
        args += ["--rate-per-conn", str(rate)]
    p = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True)
    url = p.stdout.readline().strip()
    return p, url


def timed(cmd, shell=False):
    before = resource.getrusage(resource.RUSAGE_CHILDREN)
    t = time.perf_counter()
    r = subprocess.run(cmd, shell=shell, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
    wall = time.perf_counter() - t
    after = resource.getrusage(resource.RUSAGE_CHILDREN)
    cpu = (after.ru_utime - before.ru_utime) + (after.ru_stime - before.ru_stime)
    if r.returncode != 0:
        raise RuntimeError(f"{cmd}: exit {r.returncode}: {r.stderr.decode()[-400:]}")
    return wall, cpu


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--size-mib", type=int, default=1024)
    ap.add_argument("--runs", type=int, default=3)
    ap.add_argument("--quick", action="store_true", help="skip single-connection tools")
    a = ap.parse_args()

    WORK.mkdir(parents=True, exist_ok=True)
    RESULTS.mkdir(parents=True, exist_ok=True)
    sh(["cargo", "build", "--release", "-q", "-p", "qget-cli", "-p", "qget-testserver"], cwd=ROOT)
    qget = ROOT / "target/release/qget"
    server = ROOT / "target/release/qget-testserver"
    size = a.size_mib << 20
    data = WORK / f"rand-{a.size_mib}m.bin"
    make_random(data, size)
    want = sha256(data)
    archive = WORK / f"snapshot-{a.size_mib}m.tar.lz4"
    make_archive(WORK / "snapshot-src", archive, size)
    want_tree = tree_digest(WORK / "snapshot-src")

    out = WORK / "out"
    xdir = WORK / "x"

    def clean():
        for p in (out, xdir):
            if p.is_dir():
                shutil.rmtree(p)
            elif p.exists():
                p.unlink()
        for p in WORK.glob("out.*"):
            p.unlink()

    MB = 1_000_000
    file_scenarios = [
        ("per-conn cap 25 MB/s", 25 * MB, 0),
        ("per-conn cap 25 MB/s + 40 ms latency", 25 * MB, 40),
        ("unthrottled loopback", None, 0),
    ]
    tools = [
        ("qget", lambda u: [str(qget), "-q", "-O", str(out), u]),
        ("qget --max-connections 64", lambda u: [str(qget), "-q", "--max-connections", "64", "-O", str(out), u]),
        ("aria2c -x16 -s16 -k1M", lambda u: ["aria2c", "-q", "-x16", "-s16", "-k1M", "--file-allocation=falloc",
                                             "--allow-overwrite=true", "-d", str(WORK), "-o", "out", u]),
    ]
    if not a.quick:
        tools += [
            ("curl", lambda u: ["curl", "-s", "-o", str(out), u]),
            ("wget", lambda u: ["wget", "-q", "-O", str(out), u]),
        ]

    rows = []
    for name, rate, lat in file_scenarios:
        srv, url = start_server(server, data, rate, lat)
        try:
            for tool, cmd in tools:
                if rate is None and tool in ("curl", "wget") and a.quick:
                    continue
                walls, cpus = [], []
                for _ in range(a.runs):
                    clean()
                    w, c = timed(cmd(url))
                    if sha256(out) != want:
                        raise RuntimeError(f"{tool}: wrong output")
                    walls.append(w)
                    cpus.append(c)
                rows.append((name, tool, statistics.median(walls), max(walls), statistics.median(cpus)))
                print(f"{name:40} {tool:28} {statistics.median(walls):7.2f}s  cpu {statistics.median(cpus):6.2f}s", flush=True)
        finally:
            srv.send_signal(signal.SIGINT)
            srv.wait()

    # Stream-extract: download + extraction, end to end.
    srv, url = start_server(server, archive, 25 * MB, 0, port=18081)
    extract_tools = [
        ("qget -x (parallel, streaming)", f"{qget} -q -x {xdir} {url}"),
        ("aria2c -x16 then tar -I lz4", f"aria2c -q -x16 -s16 -k1M --file-allocation=falloc --allow-overwrite=true "
                                       f"-d {WORK} -o out {url} && mkdir -p {xdir} && tar -I lz4 -xf {out} -C {xdir}"),
        ("curl | lz4 -d | tar -x", f"mkdir -p {xdir} && curl -s {url} | lz4 -dc | tar -x -C {xdir}"),
    ]
    try:
        for tool, cmd in extract_tools:
            walls, cpus = [], []
            for _ in range(a.runs):
                clean()
                w, c = timed(cmd, shell=True)
                if tree_digest(xdir / "snapshot") != want_tree:
                    raise RuntimeError(f"{tool}: extracted tree differs")
                walls.append(w)
                cpus.append(c)
            label = f"extract {archive.stat().st_size >> 20} MiB .tar.lz4, per-conn cap 25 MB/s"
            rows.append((label, tool, statistics.median(walls), max(walls), statistics.median(cpus)))
            print(f"{label:40} {tool:28} {statistics.median(walls):7.2f}s  cpu {statistics.median(cpus):6.2f}s", flush=True)
    finally:
        srv.send_signal(signal.SIGINT)
        srv.wait()
    clean()

    stamp = time.strftime("%Y-%m-%d")
    host = platform.node() or "host"
    cpu_model = next((l.split(":", 1)[1].strip() for l in open("/proc/cpuinfo") if l.startswith("model name")), "?")
    base = RESULTS / f"{stamp}-{a.size_mib}m"
    with open(f"{base}.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["scenario", "tool", "median_s", "max_s", "median_cpu_s"])
        w.writerows(rows)
    with open(f"{base}.md", "w") as f:
        f.write(f"# qget benchmark {stamp}\n\n")
        f.write(f"- CPU: {cpu_model} ({os.cpu_count()} threads), kernel {platform.release()}\n")
        f.write(f"- File: {a.size_mib} MiB random; archive: {archive.stat().st_size >> 20} MiB .tar.lz4\n")
        f.write(f"- Runs per cell: {a.runs} (median shown); every output verified by SHA-256\n")
        f.write("- Network emulated by qget-testserver over loopback (no tc netem; page cache not dropped)\n\n")
        f.write("| Scenario | Tool | Median | Max | CPU (user+sys) |\n|---|---|---|---|---|\n")
        for s, t, m, mx, c in rows:
            f.write(f"| {s} | {t} | {m:.2f} s | {mx:.2f} s | {c:.2f} s |\n")
    print(f"\nwrote {base}.md and {base}.csv")


if __name__ == "__main__":
    sys.exit(main())
