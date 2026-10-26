#pragma once

#include <cstdint>
#include <string>

namespace kv {

struct Config {
  std::string bind = "0.0.0.0";
  uint16_t port = 6379;
  uint32_t threads = 0;  // 0: one per available CPU.
  bool pin = false;      // DC-03: pin worker i to the i-th available CPU.

  // DC-15: "uring" falls back to epoll, with a log line, if the kernel can't do what we need.
  std::string backend = "uring";
  bool uring_multishot = true;          // DC-14: multishot recv with a provided buffer ring.
  std::string uring_wake = "msg_ring";  // "msg_ring" or "eventfd".
  uint32_t uring_bufs = 512;
  uint32_t uring_buf_size = 4096;

  // DC-06 experiment: wake the target thread on every message, not only when it sleeps.
  bool wake_always = false;
  uint32_t queue_len = 256;  // DC-05: per-thread inbox capacity (power of two).

  uint32_t max_pipeline = 10000;               // DC-10: unsent replies before we stop reading.
  uint64_t max_unsent_bytes = 128ull << 20;    // DC-10: unsent reply bytes before we stop reading.
  uint32_t max_cmds_per_turn = 256;            // DC-11: commands per connection per loop turn.
  uint32_t max_inflight = 256;                 // Commands per connection waiting on other shards.
  uint64_t max_bulk_len = 2ull << 30;          // DC-08
  int sndbuf = 0;                              // SO_SNDBUF for accepted sockets; 0 keeps the default.
  uint32_t hz = 100;                           // DC-18: heartbeat frequency for active expiry.

  std::string log_level = "info";
};

// Parses --name=value, --name value, and bare --flag for booleans. On error returns false and
// sets *err.
bool ParseFlags(int argc, char** argv, Config* config, std::string* err);
std::string FlagsUsage();

}  // namespace kv
