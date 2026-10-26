"""Every supported command, with replies and error texts as Redis 7 gives them."""

import pytest

from conftest import RespError


def err(reply):
    assert isinstance(reply, RespError), f"expected an error, got {reply!r}"
    return str(reply)


def test_ping_echo(raw):
    assert raw.call("PING") == "PONG"
    assert raw.call("ping", "hello") == b"hello"
    assert err(raw.call("PING", "a", "b")) == "ERR wrong number of arguments for 'ping' command"
    assert raw.call("ECHO", "x y") == b"x y"
    assert err(raw.call("ECHO")) == "ERR wrong number of arguments for 'echo' command"


def test_set_get(r):
    assert r.set("k", "v") is True
    assert r.get("k") == b"v"
    assert r.get("missing") is None
    assert r.set("k", "") is True
    assert r.get("k") == b""
    big = b"x" * 100_000
    r.set("big", big)
    assert r.get("big") == big
    binary = bytes(range(256)) * 4
    r.set(b"\x00bin\r\n", binary)
    assert r.get(b"\x00bin\r\n") == binary


def test_set_options(raw):
    assert raw.call("SET", "k", "1", "NX") == "OK"
    assert raw.call("SET", "k", "2", "NX") is None
    assert raw.call("GET", "k") == b"1"
    assert raw.call("SET", "k", "3", "XX") == "OK"
    assert raw.call("SET", "absent", "3", "XX") is None
    assert raw.call("SET", "k", "4", "GET") == b"3"
    assert raw.call("SET", "new", "1", "GET") is None
    assert raw.call("SET", "k", "5", "NX", "GET") == b"4"
    assert raw.call("GET", "k") == b"4"
    assert raw.call("SET", "k", "v", "EX", "100") == "OK"
    assert 99 <= raw.call("TTL", "k") <= 100
    assert raw.call("SET", "k", "v", "PX", "100000") == "OK"
    assert 99000 <= raw.call("PTTL", "k") <= 100000
    assert raw.call("SET", "k", "v2", "KEEPTTL") == "OK"
    assert raw.call("PTTL", "k") > 90000
    assert raw.call("SET", "k", "v3") == "OK"
    assert raw.call("TTL", "k") == -1
    assert raw.call("SET", "k", "v", "EXAT", "99999999999") == "OK"
    assert raw.call("TTL", "k") > 1_000_000
    assert raw.call("SET", "k", "v", "pxat", "99999999999000") == "OK"
    assert raw.call("TTL", "k") > 1_000_000


@pytest.mark.parametrize("args,message", [
    (("SET", "k", "v", "NX", "XX"), "ERR syntax error"),
    (("SET", "k", "v", "EX"), "ERR syntax error"),
    (("SET", "k", "v", "EX", "10", "PX", "10"), "ERR syntax error"),
    (("SET", "k", "v", "KEEPTTL", "EX", "10"), "ERR syntax error"),
    (("SET", "k", "v", "BOGUS"), "ERR syntax error"),
    (("SET", "k", "v", "EX", "abc"), "ERR value is not an integer or out of range"),
    (("SET", "k", "v", "EX", "0"), "ERR invalid expire time in 'set' command"),
    (("SET", "k", "v", "PX", "-5"), "ERR invalid expire time in 'set' command"),
    (("SET", "k", "v", "EX", "9223372036854775807"), "ERR invalid expire time in 'set' command"),
    (("SET", "k"), "ERR wrong number of arguments for 'set' command"),
])
def test_set_errors(raw, args, message):
    assert err(raw.call(*args)) == message


def test_setnx_setex_psetex(raw):
    assert raw.call("SETNX", "k", "1") == 1
    assert raw.call("SETNX", "k", "2") == 0
    assert raw.call("GET", "k") == b"1"
    assert raw.call("SETEX", "e", "100", "v") == "OK"
    assert 99 <= raw.call("TTL", "e") <= 100
    assert raw.call("PSETEX", "p", "100000", "v") == "OK"
    assert raw.call("PTTL", "p") > 90000
    assert err(raw.call("SETEX", "e", "0", "v")) == "ERR invalid expire time in 'setex' command"
    assert err(raw.call("PSETEX", "e", "-1", "v")) == "ERR invalid expire time in 'psetex' command"
    assert err(raw.call("SETEX", "e", "x", "v")) == "ERR value is not an integer or out of range"


def test_getset_getdel(r):
    assert r.getset("k", "a") is None
    r.expire("k", 100)
    assert r.getset("k", "b") == b"a"
    assert r.ttl("k") == -1  # GETSET drops the TTL.
    assert r.getdel("k") == b"b"
    assert r.getdel("k") is None
    assert r.exists("k") == 0


def test_append_strlen(r):
    assert r.append("k", "hello") == 5
    assert r.append("k", " world") == 11
    assert r.get("k") == b"hello world"
    assert r.strlen("k") == 11
    assert r.strlen("missing") == 0


def test_incr_family(raw):
    assert raw.call("INCR", "n") == 1
    assert raw.call("INCRBY", "n", "41") == 42
    assert raw.call("DECR", "n") == 41
    assert raw.call("DECRBY", "n", "50") == -9
    assert raw.call("GET", "n") == b"-9"
    raw.call("SET", "n", "9223372036854775807")
    assert err(raw.call("INCR", "n")) == "ERR increment or decrement would overflow"
    raw.call("SET", "n", "-9223372036854775808")
    assert err(raw.call("DECR", "n")) == "ERR increment or decrement would overflow"
    assert err(raw.call("DECRBY", "x", "-9223372036854775808")) == "ERR decrement would overflow"
    raw.call("SET", "s", "abc")
    assert err(raw.call("INCR", "s")) == "ERR value is not an integer or out of range"
    raw.call("SET", "s", " 1")
    assert err(raw.call("INCR", "s")) == "ERR value is not an integer or out of range"
    assert err(raw.call("INCRBY", "n", "1.5")) == "ERR value is not an integer or out of range"
    raw.call("SET", "t", "5", "EX", "100")
    raw.call("INCR", "t")
    assert raw.call("TTL", "t") > 0  # INCR keeps the TTL.


def test_del_exists_unlink(raw):
    raw.call("MSET", "a", "1", "b", "2", "c", "3")
    assert raw.call("EXISTS", "a", "b", "zz") == 2
    assert raw.call("EXISTS", "a", "a", "a") == 3
    assert raw.call("DEL", "a", "zz", "a") == 1
    assert raw.call("UNLINK", "b", "c") == 2
    assert raw.call("EXISTS", "a", "b", "c") == 0
    assert err(raw.call("DEL")) == "ERR wrong number of arguments for 'del' command"


def test_type(raw):
    raw.call("SET", "k", "v")
    assert raw.call("TYPE", "k") == "string"
    assert raw.call("TYPE", "nope") == "none"


def test_dbsize_flushall(r):
    for i in range(500):
        r.set(f"key:{i}", i)
    assert r.dbsize() == 500
    assert r.flushall() is True
    assert r.dbsize() == 0
    r.set("a", 1)
    assert r.flushdb() is True
    assert r.dbsize() == 0


def test_flushall_options(raw):
    assert raw.call("FLUSHALL", "ASYNC") == "OK"
    assert raw.call("FLUSHALL", "sync") == "OK"
    assert err(raw.call("FLUSHALL", "nope")) == "ERR syntax error"


def test_unknown_command(raw):
    assert err(raw.call("FOO")) == "ERR unknown command 'FOO', with args beginning with: "
    assert (err(raw.call("foo", "a", "b c")) ==
            "ERR unknown command 'foo', with args beginning with: 'a' 'b c' ")


@pytest.mark.parametrize("cmd,argc", [
    ("get", 1), ("get", 3), ("incr", 3), ("ttl", 1), ("mget", 1), ("exists", 1),
    ("expire", 2), ("strlen", 3), ("append", 2), ("dbsize", 2),
])
def test_wrong_arity(raw, cmd, argc):
    args = [cmd] + ["x"] * (argc - 1)
    assert err(raw.call(*args)) == f"ERR wrong number of arguments for '{cmd}' command"


def test_commands_are_case_insensitive(raw):
    assert raw.call("sEt", "k", "v") == "OK"
    assert raw.call("GeT", "k") == b"v"


def test_select(raw):
    assert raw.call("SELECT", "0") == "OK"
    assert err(raw.call("SELECT", "1")) == "ERR DB index is out of range"
    assert err(raw.call("SELECT", "x")) == "ERR value is not an integer or out of range"


def test_command_stub(raw):
    assert raw.call("COMMAND") == []
    assert raw.call("COMMAND", "DOCS") == []
    assert raw.call("COMMAND", "COUNT") > 30
    assert b"get" in raw.call("COMMAND", "LIST")


def test_config_get(raw):
    assert raw.call("CONFIG", "GET", "save") == [b"save", b""]
    assert raw.call("CONFIG", "GET", "appendonly") == [b"appendonly", b"no"]
    assert raw.call("CONFIG", "GET", "nonexistent") == []
    assert err(raw.call("CONFIG", "FOO")) == "ERR unknown subcommand 'FOO'. Try CONFIG HELP."


def test_client(raw):
    assert raw.call("CLIENT", "GETNAME") is None
    assert raw.call("CLIENT", "SETNAME", "worker-1") == "OK"
    assert raw.call("CLIENT", "GETNAME") == b"worker-1"
    assert isinstance(raw.call("CLIENT", "ID"), int)
    assert raw.call("CLIENT", "SETINFO", "LIB-NAME", "test") == "OK"


def test_time(raw):
    secs, usecs = raw.call("TIME")
    assert int(secs) > 1_600_000_000
    assert 0 <= int(usecs) < 1_000_000


def test_info(server, r):
    r.set("a", 1)
    r.set("b", 2, ex=100)
    info = server.info()
    assert info["io_backend"] == server.backend
    assert info["num_threads"] >= 1
    assert info["connected_clients"] >= 1
    assert info["db0"]["keys"] == 2
    assert info["db0"]["expires"] == 1
    keyspace = r.info("keyspace")
    assert set(keyspace) == {"db0"}
    syscalls = r.info("syscalls")
    assert syscalls["syscall_total"] > 0


def test_quit(server):
    c = server.raw()
    assert c.call("QUIT") == "OK"
    assert c.closed_by_peer()
    c.close()


def test_redis_py_client_basics(r):
    """The plain redis-py API, as an application would use it."""
    pipe = r.pipeline(transaction=False)
    pipe.set("x", 1).incr("x").get("x").delete("x").exists("x")
    assert pipe.execute() == [True, 2, b"2", 1, 0]
    assert r.mset({"m1": "a", "m2": "b"}) is True
    assert r.mget("m1", "m2", "m3") == [b"a", b"b", None]
