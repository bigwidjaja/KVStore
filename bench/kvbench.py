"""Shared helpers for the benchmark scripts: start servers, run redis-benchmark, read INFO, and
record the test bed. Every number in bench/results comes through here.
"""

import csv
import io
import os
import platform
import shutil
import signal
import socket
import subprocess
import time

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
KV_BIN = os.environ.get("KV_SERVER_BIN", os.path.join(REPO, "build/release/kv_server"))


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def wait_port(port, proc, timeout=30):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if proc.poll() is not None:
            raise RuntimeError(f"server exited with {proc.returncode}")
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=1):
                return
        except OSError:
            time.sleep(0.05)
    raise RuntimeError("server did not start")


def taskset(cpus):
    return ["taskset", "-c", cpus] if cpus and shutil.which("taskset") else []


class Server:
    def __init__(self, argv, port):
        self.port = port
        self.proc = subprocess.Popen(argv, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
        wait_port(port, self.proc)

    def stop(self):
        if self.proc.poll() is None:
            self.proc.send_signal(signal.SIGTERM)
            self.proc.wait(timeout=60)
        self.proc.stderr.close()


def start_kv(*args, cpus=None):
    port = free_port()
    argv = taskset(cpus) + [KV_BIN, f"--port={port}", "--bind=127.0.0.1", *args]
    return Server(argv, port)


def start_redis(*args, cpus=None):
    port = free_port()
    argv = taskset(cpus) + ["redis-server", "--port", str(port), "--bind", "127.0.0.1",
                            "--save", "", "--appendonly", "no", *args]
    return Server(argv, port)


def redis_benchmark(port, *args, cpus=None):
    """Runs redis-benchmark --csv and returns {test: row} with numeric fields."""
    argv = taskset(cpus) + ["redis-benchmark", "-p", str(port), "--csv", *args]
    out = subprocess.run(argv, check=True, capture_output=True, text=True).stdout
    rows = {}
    for row in csv.DictReader(io.StringIO(out)):
        rows[row["test"]] = {k: (float(v) if k != "test" else v) for k, v in row.items()}
    return rows


def info(port, section=None):
    """INFO as a flat dict, read over a raw socket (no client library needed)."""
    with socket.create_connection(("127.0.0.1", port)) as s:
        cmd = ["INFO"] + ([section] if section else [])
        s.sendall(b"*%d\r\n" % len(cmd) + b"".join(b"$%d\r\n%s\r\n" % (len(c), c.encode()) for c in cmd))
        f = s.makefile("rb")
        header = f.readline()
        assert header.startswith(b"$"), header
        body = f.read(int(header[1:]) + 2).decode()
    out = {}
    for line in body.splitlines():
        if ":" in line and not line.startswith("#"):
            k, v = line.split(":", 1)
            try:
                out[k] = int(v)
            except ValueError:
                out[k] = v
    return out


def _cmd(argv):
    try:
        return subprocess.run(argv, capture_output=True, text=True, timeout=10).stdout.strip()
    except (OSError, subprocess.SubprocessError):
        return "unavailable"


def testbed():
    """The facts that must accompany every number (Phase 6, step 1)."""
    cpu = "unknown"
    try:
        with open("/proc/cpuinfo") as f:
            for line in f:
                if line.lower().startswith(("model name", "cpu part", "hardware")):
                    cpu = line.split(":", 1)[1].strip()
                    break
    except OSError:
        pass
    mem_kb = 0
    try:
        with open("/proc/meminfo") as f:
            mem_kb = int(f.readline().split()[1])
    except OSError:
        pass
    in_container = os.path.exists("/.dockerenv")
    lines = [
        f"date_utc: {time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())}",
        f"kernel: {platform.release()}",
        f"arch: {platform.machine()}",
        f"cpu: {cpu}",
        f"cpus_online: {os.cpu_count()}",
        f"mem_gib: {mem_kb / 1024 / 1024:.1f}",
        f"container: {'docker' if in_container else 'no'}",
        f"governor: {_cmd(['cat', '/sys/devices/system/cpu/cpu0/cpufreq/scaling_governor']) or 'unavailable'}",
        f"compiler: {_cmd(['c++', '--version']).splitlines()[0] if shutil.which('c++') else 'unknown'}",
        f"redis_server: {_cmd(['redis-server', '--version'])}",
        f"redis_benchmark: {_cmd(['redis-benchmark', '--version'])}",
        f"kv_commit: {_cmd(['git', '-C', REPO, '-c', 'safe.directory=*', 'rev-parse', '--short', 'HEAD'])}",
        f"kv_dirty: {'yes' if _cmd(['git', '-C', REPO, '-c', 'safe.directory=*', 'status', '--porcelain']) else 'no'}",
        "load_generator: same machine as the server" ,
    ]
    return "\n".join(lines) + "\n"


def write_csv(path, rows, fields):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in fields})
