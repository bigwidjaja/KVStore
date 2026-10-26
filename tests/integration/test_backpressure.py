"""Phase 2, steps 6 and 9: a reader that never reads, and partial writes."""

import socket
import time

from conftest import RespClient


def expect_replies(sock, reply, count):
    """Reads `count` copies of `reply` from `sock`, checking them as they stream in."""
    sock.setblocking(True)
    sock.settimeout(30)
    buf = bytearray()
    seen = 0
    while seen < count:
        chunk = sock.recv(1 << 20)
        assert chunk, f"server closed the connection after {seen} of {count} replies"
        buf += chunk
        whole = min(len(buf) // len(reply), count - seen)
        if whole:
            assert buf[:whole * len(reply)] == reply * whole, f"bad reply near #{seen}"
            del buf[:whole * len(reply)]
            seen += whole
    assert not buf, "extra bytes after the last reply"


def test_slow_reader_is_paused_and_others_keep_working(make_server):
    srv = make_server("--max_pipeline=1000")
    setup = srv.client()
    setup.set("blob", b"x" * 4096)

    # The slow client streams GETs and never reads the replies. A small send buffer keeps our
    # own kernel from soaking up the stream, so only the server's backpressure can stop it.
    # (Don't shrink SO_RCVBUF: on loopback a window far below the MSS stalls TCP itself.)
    slow = socket.create_connection(("127.0.0.1", srv.port))
    slow.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 16384)
    cmd = RespClient.encode("GET", "blob")
    chunk = cmd * 2000
    offset = 0      # Position inside `chunk`; the stream stays a whole number of commands.
    sent_bytes = 0
    slow.setblocking(False)
    rss_before = srv.rss_kb()
    processed_before = setup.info("stats")["total_commands_processed"]
    blocked_since = None
    deadline = time.time() + 30
    while time.time() < deadline and sent_bytes < 200_000 * len(cmd):
        try:
            n = slow.send(chunk[offset:])
            sent_bytes += n
            offset = (offset + n) % len(chunk)
            blocked_since = None
        except BlockingIOError:
            blocked_since = blocked_since or time.time()
            if time.time() - blocked_since > 1.0:
                break  # Stuck for a second: the server stopped reading from us.
            time.sleep(0.01)
    sent = sent_bytes // len(cmd)
    # How many requests the kernel accepted depends on TCP buffer autotuning; how many the
    # server executed is what backpressure bounds: ~1000 unsent replies plus what fits in the
    # socket buffers between us and the client.
    stats = setup.info("stats")
    processed = stats["total_commands_processed"] - processed_before
    assert processed < 20_000, processed
    assert processed < sent
    assert stats["backpressure_pauses"] >= 1
    # Unsent replies are bounded by --max_pipeline (1000 x 4 KB), not by what the client sent.
    assert srv.rss_kb() - rss_before < 200_000

    # Meanwhile everyone else is served normally.
    other = srv.client()
    for i in range(200):
        assert other.incr("other") == i + 1

    # Once the slow client reads, it gets every reply, intact and in order.
    expect_replies(slow, b"$4096\r\n" + b"x" * 4096 + b"\r\n", sent)
    slow.close()
    other.close()
    setup.close()


def test_partial_writes_with_tiny_sndbuf(make_server):
    # A 4 KB socket send buffer forces almost every reply write to be partial.
    srv = make_server("--sndbuf=4096")
    c = srv.raw()
    value = bytes(i % 253 for i in range(3 << 20))
    assert c.call("SET", "v", value) == "OK"
    assert c.call("GET", "v") == value

    # Many medium replies in one pipeline.
    pieces = [bytes([i % 256]) * 10_000 for i in range(300)]
    c.send(b"".join(RespClient.encode("SET", f"p{i}", p) for i, p in enumerate(pieces)))
    for _ in pieces:
        assert c.read() == "OK"
    c.send(b"".join(RespClient.encode("GET", f"p{i}") for i in range(len(pieces))))
    for p in pieces:
        assert c.read() == p
    assert c.call("MGET", *[f"p{i}" for i in range(len(pieces))]) == pieces
    c.close()


def test_client_disconnects_with_unsent_replies(make_server):
    srv = make_server("--sndbuf=4096")
    setup = srv.client()
    setup.set("blob", b"y" * 65536)
    for _ in range(20):
        s = socket.create_connection(("127.0.0.1", srv.port))
        s.sendall(RespClient.encode("GET", "blob") * 200)
        s.close()  # Leave with megabytes of replies still queued.
    assert setup.get("blob") == b"y" * 65536
    deadline = time.time() + 10
    # Ask on our own connection: a fresh one would count itself.
    while setup.info("clients")["connected_clients"] > 1 and time.time() < deadline:
        time.sleep(0.05)
    assert setup.info("clients")["connected_clients"] == 1
    setup.close()


def test_byte_cap_pauses_reading(make_server):
    # Large replies hit --max_unsent_bytes long before --max_pipeline.
    srv = make_server("--max_unsent_bytes=2097152", "--max_pipeline=1000000")
    setup = srv.client()
    blob = b"z" * 65536
    setup.set("blob", blob)
    slow = socket.create_connection(("127.0.0.1", srv.port))
    slow.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 16384)
    slow.setblocking(False)
    processed_before = setup.info("stats")["total_commands_processed"]
    cmd = RespClient.encode("GET", "blob")
    chunk = cmd * 500
    offset = sent_bytes = 0
    blocked_since = None
    deadline = time.time() + 30
    while time.time() < deadline:
        try:
            n = slow.send(chunk[offset:])
            sent_bytes += n
            offset = (offset + n) % len(chunk)
            blocked_since = None
        except BlockingIOError:
            blocked_since = blocked_since or time.time()
            if time.time() - blocked_since > 1.0:
                break
            time.sleep(0.01)
    sent = sent_bytes // len(cmd)
    # 2 MB of unsent replies is ~32 GETs; the socket buffers hold a few hundred more. Without
    # the byte cap the server would execute everything the client streams for 30 s.
    stats = setup.info("stats")
    processed = stats["total_commands_processed"] - processed_before
    assert processed < 2_000, processed
    assert stats["backpressure_pauses"] >= 1
    # (No RSS check here: each 64 KB reply is copied and freed a few times, and ASan's
    # quarantine keeps up to 256 MB of freed memory resident.)

    expect_replies(slow, b"$65536\r\n" + blob + b"\r\n", sent)
    slow.close()
    setup.close()
