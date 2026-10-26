"""Fixtures for the integration suite.

The server under test comes from KV_SERVER_BIN, with the backend from KV_BACKEND (epoll or
uring) and the thread count from KV_THREADS (default 4, so cross-shard paths always run).
Every server is stopped with SIGTERM and must exit 0 with no sanitizer report on stderr, so
leaks (LeakSanitizer at exit) and data races (TSan) fail the test that caused them.
"""

import os
import shutil
import signal
import socket
import subprocess
import tempfile
import time

import pytest
import redis

REPO = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
SERVER_BIN = os.environ.get("KV_SERVER_BIN", os.path.join(REPO, "build/debug/kv_server"))
BACKEND = os.environ.get("KV_BACKEND", "epoll")
THREADS = int(os.environ.get("KV_THREADS", "4"))

SANITIZER_MARKERS = (
    "ERROR: AddressSanitizer",
    "ERROR: LeakSanitizer",
    "WARNING: ThreadSanitizer",
    "runtime error:",  # UBSan
)


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def sanitizer_env():
    env = dict(os.environ)
    # GCC 13's libasan is 50-100x slower with stack-use-after-return detection on.
    env.setdefault("ASAN_OPTIONS", "detect_stack_use_after_return=0")
    env.setdefault("TSAN_OPTIONS", "halt_on_error=1 second_deadlock_stack=1")
    return env


class KvServer:
    def __init__(self, extra_args=(), threads=THREADS, backend=BACKEND):
        self.port = free_port()
        self.args = [SERVER_BIN, f"--port={self.port}", f"--threads={threads}",
                     f"--backend={backend}", "--bind=127.0.0.1", *extra_args]
        self.log = tempfile.NamedTemporaryFile(prefix="kv_server_", suffix=".log", delete=False)
        self.proc = subprocess.Popen(self.args, stdout=self.log, stderr=subprocess.STDOUT,
                                     env=sanitizer_env())
        self._wait_ready()
        self.backend = self.info("server")["io_backend"]

    def _wait_ready(self, timeout=30):
        deadline = time.time() + timeout
        while time.time() < deadline:
            if self.proc.poll() is not None:
                raise RuntimeError(f"server exited early:\n{self.output()}")
            try:
                with socket.create_connection(("127.0.0.1", self.port), timeout=1):
                    return
            except OSError:
                time.sleep(0.05)
        raise RuntimeError(f"server did not start:\n{self.output()}")

    def client(self, **kwargs):
        return redis.Redis(host="127.0.0.1", port=self.port, socket_timeout=60, **kwargs)

    def raw(self):
        return RespClient(self.port)

    def info(self, section=None):
        c = self.client()
        try:
            return c.info(section) if section else c.info()
        finally:
            c.close()

    def output(self):
        with open(self.log.name, "r", errors="replace") as f:
            return f.read()

    def rss_kb(self):
        with open(f"/proc/{self.proc.pid}/status") as f:
            for line in f:
                if line.startswith("VmRSS:"):
                    return int(line.split()[1])
        return 0

    def stop(self, check=True):
        if self.proc.poll() is None:
            self.proc.send_signal(signal.SIGTERM)
            try:
                self.proc.wait(timeout=60)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait()
                if check:
                    raise AssertionError(f"server hung on shutdown:\n{self.output()[-4000:]}")
        out = self.output()
        os.unlink(self.log.name)
        if check:
            bad = [m for m in SANITIZER_MARKERS if m in out]
            assert not bad, f"sanitizer report ({bad}):\n{out[-8000:]}"
            assert self.proc.returncode == 0, f"exit code {self.proc.returncode}:\n{out[-4000:]}"


class RespError(Exception):
    pass


class RespClient:
    """A tiny RESP2 client over a raw socket, for exact bytes and error texts."""

    def __init__(self, port, timeout=30):
        self.sock = socket.create_connection(("127.0.0.1", port), timeout=timeout)
        self.buf = b""

    def close(self):
        self.sock.close()

    def send(self, data: bytes):
        self.sock.sendall(data)

    @staticmethod
    def encode(*args):
        out = [b"*%d\r\n" % len(args)]
        for a in args:
            if isinstance(a, int):
                a = str(a)
            if isinstance(a, str):
                a = a.encode()
            out.append(b"$%d\r\n%s\r\n" % (len(a), a))
        return b"".join(out)

    def call(self, *args):
        self.send(self.encode(*args))
        return self.read()

    def _fill(self):
        chunk = self.sock.recv(1 << 20)
        if not chunk:
            raise ConnectionError("connection closed")
        self.buf += chunk

    def _line(self):
        while b"\r\n" not in self.buf:
            self._fill()
        line, self.buf = self.buf.split(b"\r\n", 1)
        return line

    def read(self):
        """Returns the next reply. Errors come back as RespError instances, not raised."""
        line = self._line()
        kind, rest = line[:1], line[1:]
        if kind == b"+":
            return rest.decode()
        if kind == b"-":
            return RespError(rest.decode())
        if kind == b":":
            return int(rest)
        if kind == b"$":
            n = int(rest)
            if n < 0:
                return None
            while len(self.buf) < n + 2:
                self._fill()
            data, self.buf = self.buf[:n], self.buf[n + 2:]
            return data
        if kind == b"*":
            n = int(rest)
            if n < 0:
                return None
            return [self.read() for _ in range(n)]
        raise AssertionError(f"bad reply line {line!r}")

    def closed_by_peer(self, timeout=5):
        self.sock.settimeout(timeout)
        try:
            return self.sock.recv(1) == b""
        except (ConnectionResetError, BrokenPipeError):
            return True


@pytest.fixture(scope="module")
def server():
    s = KvServer()
    if BACKEND == "uring" and s.backend != "uring":
        s.stop()
        pytest.skip("io_uring unavailable here; the server fell back to epoll")
    yield s
    s.stop()


@pytest.fixture
def make_server():
    """Factory for servers with custom flags, stopped (and checked) at test end."""
    started = []

    def make(*extra_args, **kwargs):
        s = KvServer(extra_args, **kwargs)
        started.append(s)
        return s

    yield make
    for s in started:
        s.stop()


@pytest.fixture
def r(server):
    c = server.client()
    c.flushall()
    yield c
    c.close()


@pytest.fixture
def raw(server):
    c = server.raw()
    assert c.call("FLUSHALL") == "OK"
    yield c
    c.close()


@pytest.fixture(scope="module")
def reference():
    """A real redis-server for differential tests, or skip if none is installed."""
    exe = shutil.which("redis-server")
    if exe is None:
        pytest.skip("redis-server not installed")
    port = free_port()
    proc = subprocess.Popen([exe, "--port", str(port), "--bind", "127.0.0.1", "--save", "",
                             "--appendonly", "no"], stdout=subprocess.DEVNULL,
                            stderr=subprocess.DEVNULL)
    deadline = time.time() + 10
    while time.time() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=1):
                break
        except OSError:
            time.sleep(0.05)
    yield port
    proc.terminate()
    proc.wait()
