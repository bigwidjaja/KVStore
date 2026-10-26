#pragma once

#include <atomic>
#include <cstdint>
#include <deque>
#include <latch>
#include <memory>
#include <span>
#include <string>
#include <thread>
#include <vector>

#include "net/reactor.h"
#include "net/socket_util.h"
#include "server/server.h"
#include "server/shard_batch.h"
#include "server/stats.h"
#include "shard/inline_task.h"
#include "shard/mpsc_queue.h"
#include "shard/shard.h"

namespace kv {

class CommandTable;
class Connection;
class Server;
struct CommandSpec;
struct Config;
struct StartupLatches;

// One pinned thread: a reactor, the connections it accepted, an inbox, and one shard.
//
// Design after Dragonfly (DC-01, DC-02): every thread does I/O and owns a shard, and threads
// interact only through messages. A connection stays on the thread that accepted it and acts
// as the coordinator for its commands.
class Worker final : public net::LoopDelegate {
 public:
  Worker(Server* server, uint32_t index, net::Fd listener);
  ~Worker() override;

  Worker(const Worker&) = delete;
  Worker& operator=(const Worker&) = delete;

  // Starts the thread. It creates its reactor, waits until every worker has one (so peers
  // can be woken), starts listening, then counts down `latches.started`.
  void Start(const StartupLatches& latches);
  void Join();
  // Any thread: closes every connection, then leaves the loop.
  void RequestStop();

  // The worker running on the calling thread.
  static Worker* Current() { return tlocal_; }

  uint32_t index() const { return index_; }
  uint32_t num_shards() const;
  Server& server() { return *server_; }
  const Config& config() const;
  const CommandTable& commands() const;
  Shard& shard() { return shard_; }
  ThreadStats& stats() { return stats_; }
  int64_t now_ms() const { return now_ms_; }
  // Increments once per loop turn; lets per-turn budgets reset (DC-11).
  uint64_t turn() const { return turn_; }

  // Runs one op on this thread's shard, writing its RESP reply to *out. Returns the op's
  // integer result (for Combine::kSum commands).
  int64_t RunLocal(const CommandSpec& cmd, std::span<const std::string_view> args, std::string* out);

  // The batch for `target` that the connection being processed is filling (DC-10).
  ShardBatch& BatchFor(uint32_t target, const Connection& conn);
  // Sends every open batch, one message per target shard.
  void SendOpenBatches();

  // Runs conn.ResumeProcessing() on the next loop turn (DC-11 fairness).
  void ScheduleResume(Connection& conn);
  // Called from Connection::OnClosed(); destroys the connection.
  void ReleaseConnection(uint32_t id);

  // This thread's share of INFO.
  void CollectInfo(InfoCounters* out);

 private:
  struct ConnSlot {
    uint32_t gen = 0;
    std::unique_ptr<Connection> conn;
  };

  // net::LoopDelegate
  bool OnLoopTurn() override;
  bool BeforeSleep() override;
  void AfterWake() override;

  void ThreadMain(StartupLatches latches);
  int64_t RunOp(const CommandSpec& cmd, std::span<const std::string_view> args, std::string* out);
  void OnAccept(int fd);
  Connection* FindConnection(uint32_t id, uint32_t gen);
  void BeginShutdown();

  void SendTo(uint32_t target, InlineTask task);
  void FlushOutboxes();
  void NotifyPeer(uint32_t target);
  void SendBatch(uint32_t target);
  void ExecuteBatch(std::unique_ptr<ShardBatch> batch);
  void OnBatchReturned(std::unique_ptr<ShardBatch> batch);
  void RunResumes();

  static inline thread_local Worker* tlocal_ = nullptr;

  Server* server_;
  const uint32_t index_;
  net::Fd listener_;
  std::thread thread_;
  std::unique_ptr<net::Reactor> reactor_;
  Shard shard_;
  ThreadStats stats_;
  int64_t now_ms_ = 0;
  uint64_t turn_ = 0;

  // Inbox for closures from other threads (DC-05), and the wakeup word (DC-06).
  MpscQueue<InlineTask> inbox_;
  alignas(64) std::atomic<uint64_t> tq_seq_{0};
  uint64_t tq_snapshot_ = 0;
  std::atomic<bool> stop_requested_{false};
  bool shutting_down_ = false;

  // Messages whose target inbox was full, kept per target in send order and retried every
  // loop turn. We can't block, so this is our backpressure (DC-05).
  std::vector<std::deque<InlineTask>> outbox_;
  size_t outbox_pending_ = 0;

  // Batches being filled for the connection currently being processed.
  std::vector<std::unique_ptr<ShardBatch>> open_batches_;
  std::vector<uint32_t> open_targets_;
  std::vector<std::string_view> exec_args_;

  std::vector<ConnSlot> conns_;
  std::vector<uint32_t> free_ids_;
  size_t live_conns_ = 0;
  std::vector<std::pair<uint32_t, uint32_t>> resume_;  // (id, gen)
};

}  // namespace kv
