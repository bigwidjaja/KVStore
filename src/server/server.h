#pragma once

#include <atomic>
#include <cstdint>
#include <latch>
#include <memory>
#include <string>
#include <vector>

#include "net/reactor.h"
#include "server/command_table.h"
#include "server/config.h"

namespace kv {

class Worker;

struct StartupLatches {
  std::latch* reactors_ready;  // Every worker has created its reactor.
  std::latch* started;         // Every worker is listening.
  std::latch* stopped;         // Every worker left its loop (plus the stopping thread).
};

// Owns the worker threads. Start() returns once every worker is listening.
class Server {
 public:
  explicit Server(Config config);
  ~Server();

  bool Start(std::string* err);
  // Closes every connection and joins the workers. Call from a non-worker thread.
  void Stop();

  const Config& config() const { return config_; }
  const CommandTable& commands() const { return commands_; }
  const net::ReactorOptions& reactor_options() const { return reactor_opts_; }
  uint16_t port() const { return port_; }
  uint32_t num_threads() const { return num_threads_; }
  Worker& worker(uint32_t i) { return *workers_[i]; }
  const char* backend_name() const;
  int64_t start_time_ms() const { return start_ms_; }
  int CpuFor(uint32_t worker) const { return cpus_[worker % cpus_.size()]; }
  uint64_t NextClientId() { return next_client_id_.fetch_add(1, std::memory_order_relaxed); }

 private:
  Config config_;
  CommandTable commands_;
  net::ReactorOptions reactor_opts_;
  uint32_t num_threads_ = 0;
  uint16_t port_ = 0;
  int64_t start_ms_ = 0;
  std::vector<int> cpus_;
  std::atomic<uint64_t> next_client_id_{1};
  std::unique_ptr<std::latch> reactors_ready_, started_, stopped_;
  std::vector<std::unique_ptr<Worker>> workers_;
  bool running_ = false;
};

}  // namespace kv
