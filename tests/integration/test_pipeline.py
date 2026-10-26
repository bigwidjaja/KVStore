"""Pipelining: many commands per read, replies in order even when shards answer out of order."""

import random


def test_ten_thousand_pipelined_commands(r):
    pipe = r.pipeline(transaction=False)
    n = 10_000
    for i in range(n):
        pipe.set(f"key:{i}", f"value:{i}")
    for i in range(n):
        pipe.get(f"key:{i}")
    replies = pipe.execute()
    assert replies[:n] == [True] * n
    assert replies[n:] == [f"value:{i}".encode() for i in range(n)]
    assert r.dbsize() == n


def test_order_with_mixed_shards_and_local_commands(r):
    # Interleave keys on different shards with coordinator-only commands (PING, ECHO): every
    # reply must still come back in request order.
    pipe = r.pipeline(transaction=False)
    expected = []
    for i in range(3000):
        k = f"k{i % 97}"
        kind = i % 6
        if kind == 0:
            pipe.incr(k)
            expected.append(("incr", k))
        elif kind == 1:
            pipe.echo(f"e{i}")
            expected.append(("echo", f"e{i}".encode()))
        elif kind == 2:
            pipe.get(k)
            expected.append(("get", k))
        elif kind == 3:
            pipe.ping()
            expected.append(("ping", True))
        elif kind == 4:
            pipe.mget(k, f"k{(i + 1) % 97}", "missing")
            expected.append(("mget", (k, f"k{(i + 1) % 97}")))
        else:
            pipe.exists(k, "missing", k)
            expected.append(("exists", k))
    replies = pipe.execute()

    counts = {}
    for (kind, arg), reply in zip(expected, replies):
        if kind == "incr":
            counts[arg] = counts.get(arg, 0) + 1
            assert reply == counts[arg]
        elif kind == "get":
            want = counts.get(arg)
            assert reply == (str(want).encode() if want is not None else None)
        elif kind == "mget":
            a, b = arg
            wa, wb = counts.get(a), counts.get(b)
            assert reply == [str(wa).encode() if wa else None, str(wb).encode() if wb else None, None]
        elif kind == "exists":
            assert reply == (2 if arg in counts else 0)
        else:
            assert reply == arg


def test_same_key_sequence_is_ordered(r):
    # Per-key order must hold across a pipeline even when the key lives on another shard.
    pipe = r.pipeline(transaction=False)
    for i in range(2000):
        pipe.append("log", f"{i},")
    pipe.get("log")
    replies = pipe.execute()
    assert replies[-1] == "".join(f"{i}," for i in range(2000)).encode()


def test_large_values_in_a_pipeline(r):
    rng = random.Random(1)
    values = {f"big:{i}": bytes(rng.getrandbits(8) for _ in range(rng.randint(1, 300_000)))
              for i in range(20)}
    pipe = r.pipeline(transaction=False)
    for k, v in values.items():
        pipe.set(k, v)
    for k in values:
        pipe.get(k)
    replies = pipe.execute()
    assert replies[len(values):] == list(values.values())
    assert r.mget(list(values)) == list(values.values())


def test_flushall_is_ordered_with_other_commands(r):
    pipe = r.pipeline(transaction=False)
    for i in range(200):
        pipe.set(f"x{i}", i)
    pipe.flushall()
    for i in range(200):
        pipe.get(f"x{i}")
    pipe.set("after", 1)
    pipe.dbsize()
    replies = pipe.execute()
    assert replies[201:401] == [None] * 200
    assert replies[-1] == 1
