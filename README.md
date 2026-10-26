# KVStore

A Redis-compatible cache server in C++20 with one thread per core. Each thread owns one shard of the keys and talks to the others only through bounded lock-free queues, so there are no locks on the data path. Two I/O backends, epoll and io_uring, sit behind one interface. Modeled on [Dragonfly](https://github.com/dragonflydb/dragonfly)'s design and written from scratch; see [CREDITS.md](CREDITS.md) for what came from where.

- One thread does 2.1M SET/s on epoll and 2.4M on io_uring, where Redis 7.0.15 does 1.37M on the same core. io_uring makes 82 to 85% fewer syscalls per request than epoll. Dev-VM numbers with the client on the same machine; see [BENCHMARKS.md](BENCHMARKS.md).
- Unmodified `redis-cli`, `redis-py` and `redis-benchmark` work. Replies and error texts are checked byte for byte against a real `redis-server`.

## Build and test

Linux only (kernel 6.1+ for io_uring). On a Mac, `tools/dev/shell.sh <command>` runs any command below in a Linux container.

```
cmake --preset release && cmake --build --preset release
cmake --preset debug && cmake --build --preset debug    # ASan + UBSan; also tsan and fuzz presets
ctest --preset debug                                    # unit tests, then the integration suite on both backends
```

Ubuntu 24.04 packages: `g++ cmake ninja-build pkg-config liburing-dev libxxhash-dev libabsl-dev libgtest-dev libgmock-dev libbenchmark-dev`, plus `python3-pytest python3-redis redis-server` for the integration suite.

```
KV_BACKEND=uring KV_SERVER_BIN=build/debug/kv_server python3 -m pytest -q tests/integration
cmake --preset fuzz && cmake --build --preset fuzz --target resp_parser_fuzz
build/fuzz/tests/resp_parser_fuzz -max_total_time=600 tests/fuzz/seeds
```

## Run

```
build/release/kv_server --port 6379 --threads 8 --pin   # --backend=uring|epoll, --help for every flag
redis-cli -p 6379
```

## Commands

`GET SET SETNX SETEX PSETEX GETSET GETDEL APPEND STRLEN INCR DECR INCRBY DECRBY MGET MSET DEL UNLINK EXISTS TYPE EXPIRE PEXPIRE EXPIREAT PEXPIREAT TTL PTTL EXPIRETIME PEXPIRETIME PERSIST DBSIZE FLUSHALL FLUSHDB PING ECHO QUIT SELECT COMMAND CONFIG CLIENT TIME INFO`

`SET` takes `NX XX GET EX PX EXAT PXAT KEEPTTL`, and `EXPIRE` takes `NX XX GT LT`. Unlike Redis, multi-key commands are not atomic across shards, and there is one database.

## Benchmarks

```
python3 bench/phase2_baseline.py                  # one thread vs Redis
python3 bench/phase3_storage.py                   # bytes per key, insert latency while growing, table throughput
python3 bench/phase4_scaling.py --backend epoll   # 1, 2, 4 and 8 threads
python3 bench/phase5_syscalls.py                  # syscalls per request, epoll vs io_uring
```

Each script writes CSVs and a `testbed.txt` (machine, kernel, versions, commit) into `bench/results/`.

## Layout

```
src/net      reactor interface, epoll and io_uring backends
src/proto    RESP parser, reply buffers
src/core     compact strings, segmented hash table, per-shard keyspace and expiry
src/shard    shard, key router, MPSC queue
src/server   connections, worker threads, command handlers
tests/       GoogleTest unit tests, pytest integration suite, libFuzzer target
bench/       microbenchmarks and benchmark scripts
```

[docs/DESIGN.md](docs/DESIGN.md) covers the request path and maps each design choice taken from Dragonfly to the file that implements it.
