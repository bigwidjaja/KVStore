"""TTL commands, lazy expiry and active expiry (Phase 3)."""

import time

import pytest

from conftest import RespError


def err(reply):
    assert isinstance(reply, RespError), f"expected an error, got {reply!r}"
    return str(reply)


def test_ttl_states(raw):
    assert raw.call("TTL", "missing") == -2
    assert raw.call("PTTL", "missing") == -2
    raw.call("SET", "k", "v")
    assert raw.call("TTL", "k") == -1
    assert raw.call("PTTL", "k") == -1
    assert raw.call("EXPIRE", "k", "100") == 1
    assert raw.call("TTL", "k") == 100
    assert 99000 < raw.call("PTTL", "k") <= 100000


def test_expire_variants(raw):
    raw.call("SET", "k", "v")
    assert raw.call("PEXPIRE", "k", "50000") == 1
    assert 49 <= raw.call("TTL", "k") <= 50
    now = int(time.time())
    assert raw.call("EXPIREAT", "k", str(now + 1000)) == 1
    assert 998 <= raw.call("TTL", "k") <= 1000
    assert raw.call("EXPIRETIME", "k") == now + 1000
    assert raw.call("PEXPIREAT", "k", str((now + 2000) * 1000)) == 1
    assert raw.call("PEXPIRETIME", "k") == (now + 2000) * 1000
    assert raw.call("EXPIRE", "missing", "10") == 0
    assert raw.call("EXPIRETIME", "missing") == -2
    raw.call("SET", "p", "v")
    assert raw.call("EXPIRETIME", "p") == -1


def test_expire_in_the_past_deletes(raw):
    raw.call("SET", "k", "v")
    assert raw.call("EXPIRE", "k", "-1") == 1
    assert raw.call("EXISTS", "k") == 0
    raw.call("SET", "k", "v")
    assert raw.call("EXPIREAT", "k", "1") == 1
    assert raw.call("GET", "k") is None
    raw.call("SET", "k", "v")
    assert raw.call("PEXPIRE", "k", "0") == 1
    assert raw.call("EXISTS", "k") == 0


def test_expire_options(raw):
    raw.call("SET", "k", "v")
    assert raw.call("EXPIRE", "k", "100", "XX") == 0  # No TTL yet.
    assert raw.call("EXPIRE", "k", "100", "GT") == 0  # No TTL counts as infinite.
    assert raw.call("EXPIRE", "k", "100", "NX") == 1
    assert raw.call("EXPIRE", "k", "200", "NX") == 0
    assert raw.call("EXPIRE", "k", "50", "GT") == 0
    assert raw.call("EXPIRE", "k", "200", "GT") == 1
    assert raw.call("EXPIRE", "k", "300", "LT") == 0
    assert raw.call("EXPIRE", "k", "10", "LT") == 1
    assert raw.call("EXPIRE", "k", "20", "xx") == 1
    assert raw.call("TTL", "k") == 20
    raw.call("SET", "n", "v")
    assert raw.call("EXPIRE", "n", "10", "LT") == 1  # Infinite is greater than anything.


@pytest.mark.parametrize("args,message", [
    (("EXPIRE", "k", "10", "NX", "XX"),
     "ERR NX and XX, GT or LT options at the same time are not compatible"),
    (("EXPIRE", "k", "10", "NX", "GT"),
     "ERR NX and XX, GT or LT options at the same time are not compatible"),
    (("EXPIRE", "k", "10", "GT", "LT"), "ERR GT and LT options at the same time are not compatible"),
    (("EXPIRE", "k", "10", "FOO"), "ERR Unsupported option FOO"),
    (("EXPIRE", "k", "abc"), "ERR value is not an integer or out of range"),
    (("EXPIRE", "k", "9223372036854775807"), "ERR invalid expire time in 'expire' command"),
    (("PEXPIRE", "k", "9223372036854775807"), "ERR invalid expire time in 'pexpire' command"),
    (("EXPIRE", "k"), "ERR wrong number of arguments for 'expire' command"),
])
def test_expire_errors(raw, args, message):
    raw.call("SET", "k", "v")
    assert err(raw.call(*args)) == message


def test_persist(raw):
    raw.call("SET", "k", "v", "EX", "100")
    assert raw.call("PERSIST", "k") == 1
    assert raw.call("TTL", "k") == -1
    assert raw.call("PERSIST", "k") == 0
    assert raw.call("PERSIST", "missing") == 0


def test_lazy_expiry(raw):
    raw.call("SET", "k", "v", "PX", "50")
    assert raw.call("GET", "k") == b"v"
    time.sleep(0.1)
    assert raw.call("GET", "k") is None
    assert raw.call("TTL", "k") == -2
    assert raw.call("EXISTS", "k") == 0


def test_set_overwrites_ttl_and_keepttl(raw):
    raw.call("SET", "k", "v", "EX", "100")
    raw.call("SET", "k", "v2")
    assert raw.call("TTL", "k") == -1
    raw.call("SET", "k", "v", "EX", "100")
    raw.call("MSET", "k", "v3")
    assert raw.call("TTL", "k") == -1
    raw.call("SET", "k", "v", "EX", "100")
    raw.call("APPEND", "k", "x")
    assert raw.call("TTL", "k") > 0


def test_active_expiry_removes_untouched_keys(server, r):
    # 2,000 keys with a 100 ms TTL, never read again. The 100 Hz heartbeat has to find them.
    pipe = r.pipeline(transaction=False)
    for i in range(2000):
        pipe.set(f"short:{i}", "v", px=100)
    for i in range(500):
        pipe.set(f"long:{i}", "v")
    pipe.execute()
    assert r.info("keyspace")["db0"]["expires"] == 2000
    deadline = time.time() + 10
    while time.time() < deadline:
        ks = r.info("keyspace").get("db0", {"keys": 0, "expires": 0})
        if ks["expires"] == 0:
            break
        time.sleep(0.1)
    assert ks["expires"] == 0
    assert ks["keys"] == 500
    assert r.info("stats")["expired_keys"] >= 2000
