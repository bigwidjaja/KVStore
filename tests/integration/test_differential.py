"""Runs the same command scripts against a real redis-server and against us, and requires the
same replies, byte for byte, including error texts. Skipped when redis-server isn't installed.

Scripts avoid anything that legitimately differs: wall-clock values, INFO, COMMAND output,
behavior of commands we don't implement, and SELECT of databases other than 0 (we have one).
"""

import pytest

from conftest import RespClient, RespError

SCRIPTS = {
    "strings": [
        ("SET", "a", "1"), ("GET", "a"), ("GET", "nope"), ("SET", "a", "2", "NX"),
        ("SET", "a", "3", "XX"), ("SET", "b", "3", "XX"), ("SET", "a", "4", "GET"),
        ("SET", "c", "1", "NX", "GET"), ("SET", "c", "2", "NX", "GET"), ("GET", "c"),
        ("SETNX", "d", "x"), ("SETNX", "d", "y"), ("GETSET", "d", "z"), ("GETDEL", "d"),
        ("GETDEL", "d"), ("APPEND", "e", "ab"), ("APPEND", "e", "cd"), ("STRLEN", "e"),
        ("STRLEN", "zz"), ("TYPE", "e"), ("TYPE", "zz"), ("SET", "", ""), ("GET", ""),
    ],
    "counters": [
        ("INCR", "n"), ("INCRBY", "n", "10"), ("DECR", "n"), ("DECRBY", "n", "-5"),
        ("INCRBY", "n", "-100"), ("SET", "n", "007"), ("INCR", "n"), ("SET", "n", "1e3"),
        ("INCR", "n"), ("SET", "n", "9223372036854775806"), ("INCR", "n"), ("INCR", "n"),
        ("SET", "n", "-9223372036854775807"), ("DECR", "n"), ("DECR", "n"),
        ("INCRBY", "n", "abc"), ("INCRBY", "m", "9223372036854775807"), ("INCR", "m"),
        ("SET", "s", "hello"), ("INCR", "s"), ("DECRBY", "s", "1"),
    ],
    "multikey": [
        ("MSET", "k1", "v1", "k2", "v2", "k3", "v3"), ("MGET", "k1", "k2", "k3", "k4"),
        ("MGET", "k1", "k1"), ("EXISTS", "k1", "k2", "k4", "k1"), ("DEL", "k1", "k4", "k1"),
        ("UNLINK", "k2"), ("MGET", "k1", "k2", "k3"), ("MSET", "k1"), ("MSET", "k1", "v", "k2"),
        ("DBSIZE",), ("FLUSHALL",), ("DBSIZE",), ("MGET", "x"),
    ],
    "ttl": [
        ("SET", "t", "v"), ("TTL", "t"), ("PTTL", "t"), ("TTL", "nope"), ("EXPIRE", "t", "100"),
        ("TTL", "t"), ("EXPIRE", "t", "50", "GT"), ("EXPIRE", "t", "500", "GT"), ("TTL", "t"),
        ("EXPIRE", "t", "1000", "LT"), ("EXPIRE", "t", "10", "LT"), ("TTL", "t"),
        ("EXPIRE", "t", "20", "NX"), ("EXPIRE", "t", "20", "XX"), ("TTL", "t"),
        ("PERSIST", "t"), ("PERSIST", "t"), ("TTL", "t"), ("EXPIRE", "t", "10", "NX", "XX"),
        ("EXPIRE", "t", "10", "GT", "LT"), ("EXPIRE", "t", "10", "BAD"), ("EXPIRE", "t", "x"),
        ("EXPIRE", "t", "9223372036854775807"), ("EXPIRE", "t", "-9223372036854775808"),
        ("EXPIRE", "missing", "10"), ("EXPIRE", "t", "-5"), ("EXISTS", "t"),
        ("SET", "u", "v", "EX", "100"), ("TTL", "u"), ("SET", "u", "w", "KEEPTTL"), ("TTL", "u"),
        ("SET", "u", "w"), ("TTL", "u"), ("SETEX", "u", "30", "v"), ("TTL", "u"),
        ("PSETEX", "u", "30000", "v"), ("TTL", "u"), ("SETEX", "u", "0", "v"),
        ("SETEX", "u", "-1", "v"), ("PSETEX", "u", "0", "v"), ("SET", "u", "v", "EX", "0"),
        ("SET", "u", "v", "PX", "abc"), ("SET", "u", "v", "EX", "10", "PX", "10"),
        ("SET", "u", "v", "KEEPTTL", "EX", "10"), ("SET", "u", "v", "NX", "XX"),
        ("EXPIRETIME", "nope"), ("PEXPIRETIME", "nope"), ("SET", "w", "v"), ("EXPIRETIME", "w"),
        ("EXPIREAT", "w", "1"), ("EXISTS", "w"),
    ],
    "errors": [
        ("GET",), ("GET", "a", "b"), ("SET", "a"), ("NOSUCHCMD", "x", "y"), ("nosuchcmd",),
        ("INCR",), ("DEL",), ("EXISTS",), ("MGET",), ("TTL",), ("PING", "a", "b"),
        ("ECHO",), ("SELECT", "abc"), ("SELECT", "0"), ("PING",), ("PING", "m"),
        ("ECHO", "hello"), ("FLUSHALL", "bogus"), ("DBSIZE", "x"),
    ],
}


def run(port, script):
    c = RespClient(port)
    try:
        out = []
        for cmd in script:
            reply = c.call(*cmd)
            out.append(("error", str(reply)) if isinstance(reply, RespError) else reply)
        return out
    finally:
        c.close()


@pytest.mark.parametrize("name", sorted(SCRIPTS))
def test_matches_redis(server, reference, name):
    RespClient(reference).call("FLUSHALL")
    RespClient(server.port).call("FLUSHALL")
    script = SCRIPTS[name]
    expected = run(reference, script)
    actual = run(server.port, script)
    for cmd, want, got in zip(script, expected, actual):
        assert got == want, f"{cmd}: redis says {want!r}, we say {got!r}"


def test_protocol_errors_match_redis(server, reference):
    cases = [
        b"*abc\r\n", b"*1\r\n$abc\r\n", b"*1\r\n$-1\r\n", b"*1\r\n+foo\r\n",
        b"SET a \"b\r\n", b"SET a \"b\"c\r\n",
    ]
    for payload in cases:
        replies = []
        for port in (reference, server.port):
            c = RespClient(port)
            c.send(payload)
            reply = c.read()
            replies.append(str(reply))
            assert c.closed_by_peer(), f"{payload!r}: connection should close"
            c.close()
        assert replies[0] == replies[1], f"{payload!r}: redis {replies[0]!r}, we {replies[1]!r}"
