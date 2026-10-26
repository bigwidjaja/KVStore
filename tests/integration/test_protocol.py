"""Wire-level behavior: split writes, inline commands, protocol errors, huge arguments."""

import time

from conftest import RespClient, RespError


def test_byte_by_byte(server):
    c = server.raw()
    payload = (RespClient.encode("SET", "slow", "drip") + RespClient.encode("GET", "slow") +
               b"PING\r\n")
    for i in range(len(payload)):
        c.send(payload[i:i + 1])
        time.sleep(0.0005)
    assert c.read() == "OK"
    assert c.read() == b"drip"
    assert c.read() == "PONG"
    c.close()


def test_inline_commands(server):
    c = server.raw()
    c.send(b"SET greeting \"hello world\"\r\nGET greeting\r\nPING\nECHO 'it\\'s'\r\n\r\n")
    assert c.read() == "OK"
    assert c.read() == b"hello world"
    assert c.read() == "PONG"
    assert c.read() == b"it's"
    c.close()


def test_protocol_error_closes_connection(server):
    for payload, text in [
        (b"*1\r\n$3\r\nfooXX", "ERR Protocol error: expected CRLF after bulk string"),
        (b"*abc\r\n", "ERR Protocol error: invalid multibulk length"),
        (b"*1\r\n$-1\r\n", "ERR Protocol error: invalid bulk length"),
        (b"*1\r\n+x\r\n", "ERR Protocol error: expected '$', got '+'"),
        (b"SET a \"b\r\n", "ERR Protocol error: unbalanced quotes in request"),
        (b"x" * 70000, "ERR Protocol error: too big inline request"),
    ]:
        c = server.raw()
        c.send(RespClient.encode("PING"))
        c.send(payload)
        assert c.read() == "PONG"
        reply = c.read()
        assert isinstance(reply, RespError) and str(reply) == text, payload[:30]
        assert c.closed_by_peer()
        c.close()


def test_replies_before_error_are_delivered(server):
    c = server.raw()
    c.send(RespClient.encode("SET", "e", "1") + RespClient.encode("INCR", "e") + b"*x\r\n")
    assert c.read() == "OK"
    assert c.read() == 2
    assert isinstance(c.read(), RespError)
    assert c.closed_by_peer()
    c.close()


def test_eight_megabyte_value(server):
    c = server.raw()
    value = bytes(i % 251 for i in range(8 << 20))
    assert c.call("SET", "huge", value) == "OK"
    assert c.call("GET", "huge") == value
    assert c.call("STRLEN", "huge") == len(value)
    c.close()


def test_many_arguments(server):
    c = server.raw()
    keys = [f"many:{i}" for i in range(20000)]
    args = ["MSET"]
    for k in keys:
        args += [k, k]
    assert c.call(*args) == "OK"
    assert c.call("EXISTS", *keys) == len(keys)
    got = c.call("MGET", *keys)
    assert got == [k.encode() for k in keys]
    assert c.call("DEL", *keys) == len(keys)
    c.close()


def test_half_close_still_gets_replies(server):
    # A client that writes its commands and shuts down its sending side, like `nc -q`.
    import socket
    s = socket.create_connection(("127.0.0.1", server.port))
    s.sendall(RespClient.encode("SET", "hc", "1") + RespClient.encode("GET", "hc"))
    s.shutdown(socket.SHUT_WR)
    data = b""
    s.settimeout(10)
    while True:
        chunk = s.recv(4096)
        if not chunk:
            break
        data += chunk
    s.close()
    assert data == b"+OK\r\n$1\r\n1\r\n"
