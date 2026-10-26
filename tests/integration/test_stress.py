"""Phase 4, step 13: many clients sending random pipelined operations, each checked against a
model, plus clients killed mid-flight. Any lost, duplicated or reordered reply fails the test.

Every client owns its own keys (so its model is exact), but the key names are spread over all
shards, so every pipeline mixes local and remote ops. KV_STRESS_CLIENTS and KV_STRESS_ROUNDS
scale the run.
"""

import asyncio
import os
import random
import socket
import struct
import time

from conftest import RespClient
from test_concurrency import read_reply

CLIENTS = int(os.environ.get("KV_STRESS_CLIENTS", "1000"))
ROUNDS = int(os.environ.get("KV_STRESS_ROUNDS", "15"))


def random_op(rng, prefix, model):
    keys = [f"{prefix}:k{j}" for j in range(12)]
    counters = [f"{prefix}:n{j}" for j in range(3)]
    kind = rng.randrange(10)
    k = rng.choice(keys)
    if kind == 0:
        v = os.urandom(rng.randrange(0, 40))
        model[k] = v
        return ("SET", k, v), "OK"
    if kind == 1:
        return ("GET", k), model.get(k)
    if kind == 2:
        k2 = rng.choice(keys)
        n = sum(1 for x in {k, k2} if x in model)
        model.pop(k, None)
        model.pop(k2, None)
        return ("DEL", k, k2), n
    if kind == 3:
        v = os.urandom(rng.randrange(1, 20))
        model[k] = model.get(k, b"") + v
        return ("APPEND", k, v), len(model[k])
    if kind == 4:
        k2, k3 = rng.choice(keys), rng.choice(keys)
        return ("EXISTS", k, k2, k3), sum(1 for x in (k, k2, k3) if x in model)
    if kind == 5:
        pairs = {rng.choice(keys): os.urandom(rng.randrange(0, 30)) for _ in range(rng.randrange(1, 5))}
        args = ["MSET"]
        for pk, pv in pairs.items():
            args += [pk, pv]
            model[pk] = pv
        return tuple(args), "OK"
    if kind == 6:
        ks = [rng.choice(keys) for _ in range(rng.randrange(1, 6))]
        return ("MGET", *ks), [model.get(x) for x in ks]
    if kind == 7:
        c = rng.choice(counters)
        model[c] = str(int(model.get(c, b"0")) + 1).encode()
        return ("INCR", c), int(model[c])
    if kind == 8:
        return ("STRLEN", k), len(model.get(k, b""))
    v = os.urandom(rng.randrange(0, 10))
    old = model.get(k)
    model[k] = v
    return ("GETSET", k, v), old


async def client(port, idx, killer):
    rng = random.Random(idx)
    prefix = f"c{idx}"
    model = {}
    reader, writer = await asyncio.open_connection("127.0.0.1", port)
    for rnd in range(ROUNDS):
        ops = [random_op(rng, prefix, model) for _ in range(rng.randrange(1, 40))]
        writer.write(b"".join(RespClient.encode(*cmd) for cmd, _ in ops))
        await writer.drain()
        if killer and rnd == ROUNDS // 2:
            # Abort with replies still in flight: RST, no goodbye.
            sock = writer.get_extra_info("socket")
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))
            writer.transport.abort()
            return 0
        for cmd, want in ops:
            got = await read_reply(reader)
            assert got == want, f"client {idx} round {rnd}: {cmd[0]} -> {got!r}, expected {want!r}"
    # Final check of the whole namespace in one MGET.
    keys = sorted(model)
    if keys:
        writer.write(RespClient.encode("MGET", *keys))
        await writer.drain()
        assert await read_reply(reader) == [model[k] for k in keys]
    writer.close()
    await writer.wait_closed()
    return len(model)


async def run_all(port):
    killers = {i for i in range(CLIENTS) if i % 10 == 3}
    return await asyncio.gather(*(client(port, i, i in killers) for i in range(CLIENTS)))


def test_random_pipelines_against_model(server, r):
    before = r.info("stats")["total_commands_processed"]
    asyncio.run(run_all(server.port))
    processed = r.info("stats")["total_commands_processed"] - before
    # Roughly CLIENTS * ROUNDS * 20 ops; make sure the run really did that much work.
    assert processed > CLIENTS * ROUNDS * 10, processed
    # Killed clients must not take the server down or leave connections behind.
    assert r.ping()
    deadline = time.time() + 15
    while r.info("clients")["connected_clients"] > 1 and time.time() < deadline:
        time.sleep(0.1)
    assert r.info("clients")["connected_clients"] == 1


def test_kill_storm(server, r):
    """Clients that connect, fire cross-shard pipelines and vanish, over and over."""
    payload = b"".join(RespClient.encode("MSET", f"ks{i}", "v" * 100, f"kt{i}", "w") +
                       RespClient.encode("MGET", f"ks{i}", f"kt{i}", f"ku{i}") +
                       RespClient.encode("INFO") for i in range(50))
    for _ in range(300):
        s = socket.create_connection(("127.0.0.1", server.port))
        s.sendall(payload)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))
        s.close()
    assert r.ping()
    deadline = time.time() + 15
    while r.info("clients")["connected_clients"] > 1 and time.time() < deadline:
        time.sleep(0.1)
    assert r.info("clients")["connected_clients"] == 1
