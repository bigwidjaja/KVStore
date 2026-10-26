#pragma once

#include <cstdint>
#include <deque>
#include <memory>
#include <span>
#include <string>
#include <vector>

#include "net/reactor.h"
#include "proto/reply_builder.h"
#include "proto/resp_parser.h"
#include "server/stats.h"

namespace kv {

struct CommandSpec;
struct ShardBatch;
class Worker;

// The receive buffer of one connection.
//
// Design after Dragonfly (DC-11): the read buffer starts small and is capped at 32 KB
// (max_client_iobuf_len). Ours doubles whenever a read fills it, up to 32 KB. It never needs
// to hold a partial command (the parser keeps those), so it only grows past the cap if the
// io_uring multishot path delivers data while the connection is paused.
class ReadBuffer {
 public:
  static constexpr size_t kInitialSize = 4096;
  static constexpr size_t kMaxSize = 32 * 1024;
  static constexpr size_t kMinFree = 1024;

  std::span<char> Writable();
  void Commit(size_t n);
  std::span<const char> Readable() const { return {buf_.get() + begin_, end_ - begin_}; }
  void Consume(size_t n) { begin_ += n; }
  bool Empty() const { return begin_ == end_; }
  void Clear() { begin_ = end_ = 0; }
  size_t capacity() const { return cap_; }

 private:
  void Resize(size_t cap);

  std::unique_ptr<char[]> buf_;
  size_t cap_ = 0;
  size_t begin_ = 0;
  size_t end_ = 0;
  bool grow_ = false;  // The last read filled all free space.
};

// One client connection, driven by its worker's reactor. It never moves between threads.
//
// Each parsed command takes the next reply slot. Commands on keys this thread owns run on the
// spot; others are copied into a batch for the owning shard (DC-07, DC-10). Replies can come
// back out of order, so only the completed prefix of slots is written out (Phase 4, step 8).
//
// Design after Dragonfly: Connection::IoLoop, HandleRecvSocket and DispatchSingle in
// src/facade/dragonfly_connection.cc, rewritten as callbacks because we have no fibers.
class Connection final : public net::StreamHandler {
 public:
  Connection(Worker* worker, uint32_t id, uint32_t gen, uint64_t client_id);
  ~Connection() override;

  Connection(const Connection&) = delete;
  Connection& operator=(const Connection&) = delete;

  void Start(net::StreamSocket* socket);

  uint32_t id() const { return id_; }
  uint32_t gen() const { return gen_; }
  uint64_t client_id() const { return client_id_; }
  const std::string& name() const { return name_; }
  void set_name(std::string name) { name_ = std::move(name); }

  // QUIT and protocol errors: stop reading, flush what is queued, then close.
  void CloseAfterReply() { close_after_reply_ = true; }
  // Closes now. Replies still in flight from other shards are dropped when they land.
  void Close();
  bool closing() const { return closing_; }

  // Worker hooks.
  void ResumeProcessing();                    // Continue a backlog left by the fairness cap.
  void OnRemoteResults(const ShardBatch& batch);
  bool resume_queued = false;

  // net::StreamHandler
  std::span<char> RecvBuffer() override { return rbuf_.Writable(); }
  void OnRecv(size_t n) override;
  bool WantsRecv() const override;
  void OnEof() override;
  void OnSendDone() override;
  void OnError(int err) override;
  void OnClosed() override;

 private:
  // A reply slot for one command, completed when all of its ops are back.
  struct PendingReply {
    const CommandSpec* cmd = nullptr;
    uint32_t parts_left = 0;
    bool done = false;
    int64_t sum = 0;
    std::string reply;
    std::vector<std::string> items;        // Combine::kArray, in key order.
    std::unique_ptr<InfoCounters> info;    // Combine::kInfo.
    std::string info_section;
  };

  void ProcessInput();
  void Dispatch(const CmdArgs& args);
  void DispatchPerKey(const CommandSpec& cmd, const CmdArgs& args);
  void DispatchAllShards(const CommandSpec& cmd, const CmdArgs& args);
  void RunOrSend(uint32_t shard, uint64_t seq, const CommandSpec& cmd, uint32_t part,
                 std::span<const std::string_view> args);

  // Where an immediately available reply goes: straight into the output buffer when no
  // earlier reply is pending, otherwise into a new, already completed slot.
  std::string* InOrderTarget();
  uint64_t OpenSlot(const CommandSpec* cmd, uint32_t parts);
  PendingReply& Slot(uint64_t seq) { return slots_[seq - base_seq_]; }
  void CompletePart(uint64_t seq, uint32_t part, int64_t integer, std::string_view reply);
  void Finalize(PendingReply& slot);
  void DrainCompleted();

  void Flush();
  void AfterProgress(bool from_recv);
  bool Backpressured() const;
  bool ReadyToClose() const;

  ReplyBuffer& building() { return out_[cur_]; }
  ReplyBuffer& sending() { return out_[cur_ ^ 1]; }

  Worker* worker_;
  net::StreamSocket* socket_ = nullptr;
  const uint32_t id_;
  const uint32_t gen_;
  const uint64_t client_id_;
  std::string name_;

  ReadBuffer rbuf_;
  RespParser parser_;
  CmdArgs args_;

  // Two output buffers: one being filled, one owned by the socket while a send is in flight.
  ReplyBuffer out_[2];
  unsigned cur_ = 0;
  bool send_in_flight_ = false;
  size_t replies_building_ = 0;
  size_t replies_sending_ = 0;
  size_t bytes_sending_ = 0;

  std::deque<PendingReply> slots_;
  uint64_t base_seq_ = 0;  // Sequence number of slots_.front().
  uint32_t unresolved_ = 0;  // Slots still waiting for ops.
  std::string scratch_;    // Reply of a local op that is folded into a slot.

  uint64_t budget_turn_ = 0;     // Loop turn the command budget below belongs to.
  uint32_t handled_in_turn_ = 0;  // Commands handled in that turn (DC-11).

  bool closing_ = false;
  bool close_after_reply_ = false;
  bool peer_closed_ = false;
  bool paused_ = false;  // Counted once per backpressure episode.
};

}  // namespace kv
