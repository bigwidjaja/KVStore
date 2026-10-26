# Design

A Redis-compatible, thread-per-core cache server in C++20 that recreates Dragonfly's networking
and sharding core. This document maps each design choice borrowed from Dragonfly (DC-01 to
DC-20) to where it lives in this repo, and records every place we deviated from the project
plan and why.

## Architecture

Each worker thread owns one reactor (epoll or io_uring), the connections it accepted, one
inbox, and one shard of the key space. Threads share nothing on the data path; the only traffic
between them is messages through inboxes.

```
          client                          client
            |                               |
   +--------v------------+        +---------v-----------+
   | worker 0            |        | worker 1            |
   |  reactor (I/O)      |        |  reactor (I/O)      |
   |  connections ---+   |        |                     |
   |  shard 0 <-local|   |  batch |  shard 1            |
   |  inbox <--------+---+--------+-> inbox             |
   |        <--------+---+--------+-- (replies)         |
   +---------------------+        +---------------------+
```

A command on a key the connection's own thread owns runs on the spot and writes its reply
straight into the output buffer. A key owned elsewhere costs two messages: the batch to the
owner's inbox, and the same batch, now holding replies, back to the origin thread, which writes
the replies to the client in request order.

The request path, end to end:

1. The reactor delivers bytes into the connection's read buffer (`Connection::OnRecv`).
2. The incremental parser yields commands (`RespParser::Parse`).
3. Each command takes the next reply slot. `Dispatch` routes it: coordinator-only commands
   (PING, CLIENT, ...) and keys owned locally run immediately; everything else is appended to a
   per-target-shard `ShardBatch`.
4. At the end of the read batch, every open batch leaves as one message per shard
   (`Worker::SendOpenBatches`), and the owner is woken only if it is asleep.
5. The owner runs the ops against its `DbSlice`, writes RESP replies into the batch, and sends
   the batch back.
6. The origin fills the slots; the completed prefix of slots is flushed with one `writev`.

## Source map

| Path | What |
| --- | --- |
| `src/base/` | Logging, Redis-compatible integer parsing, glob matching, clocks |
| `src/proto/resp_parser.*` | Incremental RESP2 parser, inline commands |
| `src/proto/reply_builder.*` | `RespWriter` (serialization) and `ReplyBuffer` (8 KB chunks + iovecs) |
| `src/core/compact_string.*` | 18-byte `CompactStr` / `CompactKey` with inline TTL |
| `src/core/dash_table.*` | Segmented hash table (directory, 56+4 buckets x 14 slots) |
| `src/core/db_slice.*` | One shard's key space: lazy and active expiry, TTL bookkeeping |
| `src/shard/` | `Shard`, router (`ShardOf`), `MpscQueue`, `InlineTask` |
| `src/net/reactor.h` | The backend-neutral interface: `Reactor`, `StreamSocket`, `StreamHandler` |
| `src/net/epoll_reactor.cpp` | Edge-triggered epoll backend |
| `src/net/uring_reactor.cpp` | io_uring backend, kernel feature probe |
| `src/server/connection.*` | Per-connection state machine, reply slots, backpressure |
| `src/server/worker.*` | Per-thread loop: inbox, wakeups, outboxes, batching, connection table |
| `src/server/*_family.cpp` | Command handlers |
| `src/server/server.*`, `main.cpp` | Startup, listeners, shutdown |

## Design choices taken from Dragonfly

Sources are Dragonfly commit `d8cd94b` and helio `40b7b20`. Each choice is credited in the code
with a `Design after Dragonfly (DC-NN)` comment where it is implemented.

### Threading and routing

| ID | Choice | Dragonfly source | Our adaptation | Where |
| --- | --- | --- | --- | --- |
| DC-01 | Shared-nothing shards: one owner thread per shard, messages only | `docs/df-share-nothing.md`; `num_shards` in `main_service.cc` | Same. No mutex anywhere on the data path. Shards = threads. | `shard/shard.h`, `server/worker.h` |
| DC-02 | Every thread does I/O and owns a shard; connection stays on one thread; `TCP_NODELAY` | `Listener::PickConnectionProactor` in `dragonfly_listener.cc` | Each worker has its own `SO_REUSEPORT` listener and the kernel spreads connections. `TCP_NODELAY` on every accepted socket. | `server/server.cpp` (`Start`), `worker.cpp` (`OnAccept`) |
| DC-03 | Pin threads to cores | `proactor_pool.cc` | `--pin`: worker i on the i-th CPU in the affinity mask, via `pthread_setaffinity_np`. | `worker.cpp` (`ThreadMain`) |
| DC-04 | `XXH64(key) % shards`, `{tag}` hashing | `Shard()` in `sharding.cc` | Same, Redis Cluster hash-tag rule. The table inside a shard uses XXH3, so shard bits and fingerprint bits are independent. | `shard/router.*` |
| DC-05 | Per-shard bounded lock-free queue of small inline closures; blocking `Add` on full | `base/mpmc_bounded_queue.h`, `FiberQueue::Tasklet`, `kQueueLen` | Vyukov's bounded queue with a single consumer (no CAS on pop), cache-line padded cells. `InlineTask` stores closures up to 32 bytes inline; bigger ones don't compile. We can't block, so a full inbox leaves the message in a per-target outbox retried every loop turn, preserving order. Capacity `--queue_len` (256). | `shard/mpsc_queue.h`, `shard/inline_task.h`, `worker.cpp` (`SendTo`, `FlushOutboxes`) |
| DC-06 | Wake a target only when it announced it is going to sleep; spin briefly first | `WakeupIfNeeded` in `proactor_base.h` | One atomic word per thread (`tq_seq_`), as in helio: producers add 2 per message; the consumer CASes its loop-start snapshot to the wait value 1 before sleeping, which fails if anything arrived. Only a producer that sees 1 wakes it. `--wake_always` turns the optimization off for measurement. | `worker.cpp` (`OnLoopTurn`, `BeforeSleep`, `NotifyPeer`) |
| DC-07 | Single-shard fast path; inline execution when already on the owner | `docs/transaction.md` | Local key and nothing queued ahead of it: run now and write the reply straight into the output buffer. Otherwise one hop out, one back. | `connection.cpp` (`Dispatch`) |

### Protocol and connections

| ID | Choice | Dragonfly source | Our adaptation | Where |
| --- | --- | --- | --- | --- |
| DC-08 | Incremental RESP state machine; 65,536 args, 2 GiB bulk | `resp_srv_parser.h`; `max_multi_bulk_len`, `max_bulk_len` | Same states and limits, plus inline commands with `sdssplitargs` quoting. On `INPUT_PENDING` the parser copies what it still needs and reports all input consumed (see deviations). Error texts match Redis 7. | `proto/resp_parser.*` |
| DC-09 | Replies into an 8 KB buffer + 16 inline iovecs, one `writev`, batch mode while pipelining, large values by reference | `SinkReplyBuilder` in `reply_builder.h` | 8 KB chunks, `absl::InlinedVector<iovec, 16>`, one flush per read batch. Replies of 4 KB+ that already own a string (from another shard) are moved in and get their own iovec. Two buffers per connection so nothing an in-flight write points at changes. | `proto/reply_builder.*`, `connection.cpp` (`Flush`) |
| DC-10 | Squash pipelined commands per shard into one hop (32 per batch); pipeline caps 10,000 commands / 128 MB | `SquashPipeline`, `multi_command_squasher.h`; `pipeline_queue_limit`, `pipeline_buffer_limit` | One `ShardBatch` per target shard per read batch, capped at 32 ops or 1 MB of args. Stop reading from a connection with 10,000 unsent replies, 128 MB of unsent reply bytes, or 256 commands waiting on other shards (`--max_pipeline`, `--max_unsent_bytes`, `--max_inflight`). | `server/shard_batch.h`, `worker.cpp` (`BatchFor`), `connection.cpp` (`Backpressured`) |
| DC-11 | Bounded read time per connection (200 µs); read buffer capped at 32 KB | `max_busy_read_usec`, `max_client_iobuf_len` | At most `--max_cmds_per_turn` (256) commands per connection per loop turn, then it yields and resumes next turn. The budget is per turn, not per read, because io_uring multishot delivers many reads per turn. Read buffer starts at 4 KB and doubles when a read fills it, up to 32 KB. | `connection.cpp` (`ProcessInput`, `ReadBuffer`) |

### I/O backends

| ID | Choice | Dragonfly source | Our adaptation | Where |
| --- | --- | --- | --- | --- |
| DC-12 | Edge-triggered epoll, registered once with `IN|OUT|ET|RDHUP`, 128 events per wait | `kEventMask` in `epoll_socket.cc`, `kEvBatchSize` | Same mask and batch size. Partial writes resume on `EPOLLOUT`. | `net/epoll_reactor.cpp` |
| DC-13 | Ring with `SINGLE_ISSUER|DEFER_TASKRUN|COOP_TASKRUN|SUBMIT_ALL`, no SQPOLL; submit when SQ full or once per loop; batched reaping; `user_data` indexes a free list | `UringProactor::Init`, `GetSubmitEntry`, `ReapCompletions` | Same flags. Exactly one `io_uring_submit_and_get_events` (or `_and_wait` when sleeping) per loop turn; completions reaped 128 at a time with `io_uring_peek_batch_cqe`. `user_data` = slot index plus a generation, so a stale cancel can't hit a reused slot. | `net/uring_reactor.cpp` |
| DC-14 | Multishot recv over a provided buffer ring, re-armed when the kernel stops it | `BufRingGroup`, `uring_socket.cc` | Same, as an option over the basic one-recv-at-a-time path. Data is copied into the connection's buffer and the ring buffer is returned at once. Re-armed on `ENOBUFS`; cancelled while the connection is paused. | `net/uring_reactor.cpp` (`UringSocket::OnRecvMulti`) |
| DC-15 | io_uring by default, epoll fallback | `dfly_main.cc`, `pool.cc` | `--backend=uring|epoll` behind one `Reactor` interface. Startup probes the ring flags, opcodes, buffer rings and `MSG_RING`, and falls back (with a log line) when something is missing. | `net/reactor.h`, `server.cpp` |

### Storage and expiry

| ID | Choice | Dragonfly source | Our adaptation | Where |
| --- | --- | --- | --- | --- |
| DC-16 | Dashtable: directory of segments, 56 regular + 4 stash buckets x 14 slots; home, neighbor, then stash; split one segment at a time | `docs/dashtable.md`, `dash.h`, `dash_internal.h` | Same shape. Additions: 1-byte fingerprint per slot, a per-bucket count of entries that overflowed to the stash (skip the stash when 0), and splits that move each entry to the same bucket/slot of the new segment so a split can never fail. | `core/dash_table.*` |
| DC-17 | Compact keys: up to 16 bytes inline; a key with a TTL holds its expiry | `CompactObj`, `CompactKey::SetExpireTime` | 18-byte handle, 16 bytes inline. A TTL takes the second 8 payload bytes, so keys up to 8 bytes stay inline with a TTL; longer ones move to a heap block that starts with a 4-byte length. Lazy expiry on every lookup. | `core/compact_string.*`, `core/db_slice.cpp` |
| DC-18 | 100 Hz heartbeat runs an expiry step from a saved cursor; next budget scales with the expired share (5 to 200); clean a segment before splitting it | `EngineShard::Heartbeat` (`kTtlDeleteLimit`), `expire_cursor` | Same cursor (it encodes a hash-space position, so it survives splits) and same budget rule, counted in keys with a TTL examined. `InsertNew` sweeps a full segment for expired entries before splitting. | `core/db_slice.cpp` (`Heartbeat`), `dash_table.cpp` (`RemoveExpired`) |
| DC-20 | VLL transactions | `docs/transaction.md`, `transaction.cc` | Not implemented (Phase 7 stretch). | |

### Benchmarking

| ID | Choice | Dragonfly source | Our adaptation | Where |
| --- | --- | --- | --- | --- |
| DC-19 | memtier from a separate machine, `-c 20 -t <threads> -d 256 --distinct-client-seed --test-time 100` | `README.md`, `dfly_bench.cc` | Phase 6. Phases 2 to 5 use `redis-benchmark` scripts in `bench/`. | `bench/` |

## Deviations from the plan, and why

**No blocking server or `unordered_map` storage in the tree.** Phase 1 steps 1 and 7 were
stepping stones the plan itself replaces (the blocking loop in Phase 2, the map in Phase 3). The
Phase 1 checks (redis-cli interactive, `redis-benchmark -c 1`, parser tests, fuzzing) run against
the final server.

**The parser owns partial commands (DC-08, DC-11).** Dragonfly's parser caches state and lets the
caller drop consumed bytes; growing the read buffer for large bulk strings is the caller's job.
Ours copies any not-yet-finished command (completed args plus the partial one) into its own
storage when input runs out, and reports everything consumed. A command that arrives in one
read is still parsed with zero copies. The win: the connection's read buffer never holds a
partial command, so it never has to grow past 32 KB or be compacted, and parsed argument views
are valid until the buffer is refilled. The cost: commands that span reads are copied once.

**Short reads count as drained (DC-12).** With edge-triggered epoll the rule is "read until
EAGAIN". For stream sockets, `epoll(7)` notes that a read returning fewer bytes than requested
also shows the socket is drained, so we skip the extra `recv` that would only return EAGAIN,
unless the peer hung up (then we read to EOF, since no new edge will come). This saves one
syscall per event and makes the epoll side of the Phase 5 comparison stronger, not weaker.

**The wakeup word is a counter, not a flag (DC-06).** The plan says "one atomic flag per
thread". A flag needs a Dekker-style store-then-load on both sides, which requires sequentially
consistent fences that ThreadSanitizer can't reason about. helio's scheme (every producer bumps
the counter; the consumer CASes its snapshot to "waiting") orders everything through one
atomic's modification order, needs no fences, and is what Dragonfly ships.

**A full inbox never blocks (DC-05).** Dragonfly's `Add` blocks the fiber. We have no fibers, so
the sender keeps the message in a per-target outbox and retries each loop turn. While an outbox
is non-empty, that thread polls without sleeping. Every thread drains its own inbox each turn, so
two threads with full inboxes can't deadlock.

**Multi-key commands are split per key.** MGET, MSET, DEL, UNLINK and EXISTS become one op per
key (plus a combiner: array, sum, or OK) and ride in the same per-shard batches as everything
else, so a pipeline of them still costs one message per shard. They are **not atomic across
shards**: another client can observe an MSET half applied. Redis runs them atomically. Same for
FLUSHALL and DBSIZE, which run once per shard.

**The byte cap on unsent replies is soft (DC-10).** A reply from another shard is only counted
once it lands, and we can't know a GET's size before running it. So a connection also stops
reading while `--max_inflight` (256) of its commands are waiting on other shards; that bounds the
overshoot of `--max_unsent_bytes` to about 256 replies, however slow the other shards are. The
reply-count cap is exact. With
io_uring multishot recv there is a second kind of slack: requests the kernel already placed in
provided buffers when a pause starts are still delivered (up to `--uring_bufs` x
`--uring_buf_size` per thread) and wait, unexecuted, in the read buffer.

**One database.** `SELECT 0` works; other indexes return `ERR DB index is out of range`.

**Closing an io_uring socket waits for the kernel.** With a basic recv the kernel writes into
the connection's buffer asynchronously, so a connection is only freed after `shutdown()` plus
`IORING_OP_ASYNC_CANCEL` (fd, all) and every outstanding completion has arrived. Replies that
arrive from other shards after a client left find a different generation in the connection
table and are dropped (Phase 4, step 10).

**Batches make a round trip.** A `ShardBatch` is allocated by the origin thread, executed by the
owner (which appends replies into it), sent back, and freed by the origin. One allocation per
batch, freed on the thread that made it, which keeps per-thread allocator caches happy.

**Syscall accounting is built in.** Each thread counts the syscalls it makes by kind
(`INFO syscalls`). Phase 5 divides the deltas by commands processed and cross-checks against
`strace -c -f`. For io_uring only `io_uring_enter` counts; socket I/O done by the kernel inside
the ring is not a syscall. Wakeups with `IORING_OP_MSG_RING` ride on the sender's next
`io_uring_enter` and cost no syscall of their own; with eventfd they cost a `write`.

**Unmodified clients.** `COMMAND` is a stub (empty `DOCS`, real `COUNT` and `LIST`) so `redis-cli`
starts; `CONFIG GET` answers the handful of parameters clients probe on connect; `CLIENT
SETNAME/GETNAME/ID/SETINFO` work. `INFO` reports `redis_version:7.2.0` because client libraries
gate features on it.

**Sanitizer settings.** Tests run ASan with `detect_stack_use_after_return=0`: GCC 13's runtime
makes the fake stack 50 to 100 times slower, turning a one-second test into minutes. Everything
else in ASan/UBSan/LSan stays on. Set `ASAN_OPTIONS=detect_stack_use_after_return=1` for a slow,
thorough run.

## Testing map

| Plan step | Test |
| --- | --- |
| 1.3 split-point parser tests | `tests/unit/resp_parser_test.cpp` (every split, every pair of splits, one byte at a time, malformed input) |
| 1.4 fuzzing | `tests/fuzz/resp_parser_fuzz.cpp` (also checks whole-vs-chunked parses agree); weekly in `.github/workflows/fuzz.yml` |
| 1.8, 3 TTL behavior vs Redis | `tests/integration/test_basic.py`, `test_ttl.py`, `test_differential.py` (byte-for-byte against a real `redis-server`) |
| 2.9 1,000 clients, pipelining 10,000, slow reader, partial writes | `test_concurrency.py`, `test_pipeline.py`, `test_backpressure.py`; reactor-level partial writes in `tests/unit/reactor_test.cpp` |
| 3.8 differential table test (10M ops) | `tests/unit/table_diff_test.cpp` |
| 3.9 microbenchmarks | `bench/micro/` via `bench/phase3_storage.py` |
| 4.5 queue stress (8 x 10M) and throughput | `tests/unit/mpsc_queue_test.cpp`, `bench/micro/queue_bench.cpp` |
| 4.11 multi-key fan-out | `test_multikey.py` |
| 4.13 random pipelines vs a model, kills mid-flight | `test_stress.py` |
| 5 everything on both backends | `reactor_test` is parameterized over epoll, uring basic and uring multishot; CI runs the integration suite per backend and preset |

Every integration server is stopped with SIGTERM and must exit 0 with no sanitizer report, so a
leak (LeakSanitizer at exit) or data race (TSan) fails the test that caused it.
