#pragma once

#include <array>
#include <cstdint>

#include "net/reactor.h"

namespace kv {

// Per-thread counters. Each worker owns one and bumps plain integers: there are no shared
// atomics on the hot path. INFO collects them by messaging every thread (Phase 4, step 12).
#define KV_THREAD_STATS(X) \
  X(connections_received)  \
  X(connected_clients)     \
  X(commands_processed)    \
  X(net_input_bytes)       \
  X(net_output_bytes)      \
  X(local_ops)             \
  X(remote_ops_sent)       \
  X(remote_ops_executed)   \
  X(batches_sent)          \
  X(outbox_retries)        \
  X(wakeups_sent)          \
  X(loop_turns)            \
  X(sleeps)                \
  X(backpressure_pauses)   \
  X(fairness_yields)       \
  X(protocol_errors)

struct ThreadStats {
#define KV_DECLARE_STAT(name) uint64_t name = 0;
  KV_THREAD_STATS(KV_DECLARE_STAT)
#undef KV_DECLARE_STAT
};

// Shard-level values reported by INFO.
#define KV_SHARD_STATS(X) \
  X(keys)                 \
  X(expires)              \
  X(expired_keys)         \
  X(keyspace_hits)        \
  X(keyspace_misses)      \
  X(table_segments)       \
  X(table_splits)         \
  X(table_bytes)

// Everything INFO sums across threads, flattened into one array so a thread's share can
// travel back to the coordinator as plain bytes.
enum InfoField : unsigned {
#define KV_INFO_THREAD(name) kInfoThread_##name,
#define KV_INFO_SHARD(name) kInfoShard_##name,
#define KV_INFO_SYSCALL(name) kInfoSyscall_##name,
  KV_THREAD_STATS(KV_INFO_THREAD) KV_SHARD_STATS(KV_INFO_SHARD) KV_SYSCALL_KINDS(KV_INFO_SYSCALL)
#undef KV_INFO_THREAD
#undef KV_INFO_SHARD
#undef KV_INFO_SYSCALL
  kInfoFieldCount
};

using InfoCounters = std::array<uint64_t, kInfoFieldCount>;

}  // namespace kv
