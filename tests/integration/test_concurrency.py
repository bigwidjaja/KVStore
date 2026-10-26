"""Phase 2, step 9: a thousand concurrent clients."""

import asyncio

from conftest import RespClient


async def read_reply(reader):
    line = await reader.readline()
    kind, rest = line[:1], line[1:-2]
    if kind in (b"+", b"-"):
        return rest.decode()
    if kind == b":":
        return int(rest)
    if kind == b"$":
        n = int(rest)
        if n < 0:
            return None
        data = await reader.readexactly(n + 2)
        return data[:-2]
    if kind == b"*":
        return [await read_reply(reader) for _ in range(int(rest))]
    raise AssertionError(f"bad line {line!r}")


async def one_client(port, i, rounds):
    reader, writer = await asyncio.open_connection("127.0.0.1", port)
    for j in range(rounds):
        writer.write(RespClient.encode("SET", f"c{i}:{j}", f"{i}-{j}") +
                     RespClient.encode("GET", f"c{i}:{j}") +
                     RespClient.encode("INCR", "shared"))
        await writer.drain()
        assert await read_reply(reader) == "OK"
        assert await read_reply(reader) == f"{i}-{j}".encode()
        assert isinstance(await read_reply(reader), int)
    writer.close()
    await writer.wait_closed()


async def all_clients(port, n, rounds):
    # Connect everyone first so all n are open at the same time, then run the rounds.
    await asyncio.gather(*(one_client(port, i, rounds) for i in range(n)))


def test_thousand_clients(server, r):
    n, rounds = 1000, 5
    asyncio.run(all_clients(server.port, n, rounds))
    assert int(r.get("shared")) == n * rounds
    assert r.dbsize() == n * rounds + 1
    assert server.info("stats")["total_connections_received"] >= n


def test_thousand_idle_connections_then_traffic(server, r):
    async def run():
        conns = [await asyncio.open_connection("127.0.0.1", server.port) for _ in range(1000)]
        assert server.info("clients")["connected_clients"] >= 1000
        for k, (reader, writer) in enumerate(conns):
            writer.write(RespClient.encode("INCR", "idle"))
        for reader, writer in conns:
            await writer.drain()
            assert isinstance(await read_reply(reader), int)
        for reader, writer in conns:
            writer.close()
        await asyncio.gather(*(w.wait_closed() for _, w in conns))

    asyncio.run(run())
    assert int(r.get("idle")) == 1000
