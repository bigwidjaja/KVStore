#include "server/connection.h"

#include <cstring>

#include "base/log.h"
#include "server/command_table.h"
#include "server/config.h"
#include "server/server_family.h"
#include "server/shard_batch.h"
#include "server/worker.h"
#include "shard/router.h"

namespace kv {

std::span<char> ReadBuffer::Writable() {
  if (begin_ == end_) begin_ = end_ = 0;
  if (cap_ == 0) Resize(kInitialSize);
  if (grow_ && cap_ < kMaxSize) Resize(cap_ * 2);
  grow_ = false;
  if (cap_ - end_ < kMinFree && begin_ > 0) {
    std::memmove(buf_.get(), buf_.get() + begin_, end_ - begin_);
    end_ -= begin_;
    begin_ = 0;
  }
  if (cap_ - end_ < kMinFree) Resize(cap_ * 2);
  return {buf_.get() + end_, cap_ - end_};
}

void ReadBuffer::Commit(size_t n) {
  if (n == cap_ - end_) grow_ = true;
  end_ += n;
}

void ReadBuffer::Resize(size_t cap) {
  auto bigger = std::make_unique<char[]>(cap);
  if (end_ > begin_) std::memcpy(bigger.get(), buf_.get() + begin_, end_ - begin_);
  end_ -= begin_;
  begin_ = 0;
  buf_ = std::move(bigger);
  cap_ = cap;
}

namespace {

RespParser::Limits ParserLimits(const Config& cfg) {
  RespParser::Limits limits;
  limits.max_bulk_len = cfg.max_bulk_len;
  return limits;
}

}  // namespace

Connection::Connection(Worker* worker, uint32_t id, uint32_t gen, uint64_t client_id)
    : worker_(worker),
      id_(id),
      gen_(gen),
      client_id_(client_id),
      parser_(ParserLimits(worker->config())) {}

Connection::~Connection() = default;

void Connection::Start(net::StreamSocket* socket) {
  socket_ = socket;
  socket_->ResumeRecv();
}

bool Connection::Backpressured() const {
  // DC-10: stop reading from a connection whose replies aren't leaving (a slow or stalled
  // reader), so its pipeline can't grow without bound.
  const Config& cfg = worker_->config();
  size_t unsent = slots_.size() + replies_building_ + replies_sending_;
  size_t unsent_bytes = bytes_sending_ + out_[cur_].Bytes();
  // A remote reply's size is unknown until it lands, so the byte cap alone can be overshot by
  // whatever is in flight. Capping in-flight commands bounds that overshoot.
  return unsent >= cfg.max_pipeline || unsent_bytes >= cfg.max_unsent_bytes ||
         unresolved_ >= cfg.max_inflight;
}

bool Connection::WantsRecv() const {
  return !closing_ && !close_after_reply_ && !peer_closed_ && rbuf_.Empty() && !Backpressured();
}

bool Connection::ReadyToClose() const {
  bool done_reading = close_after_reply_ || (peer_closed_ && rbuf_.Empty());
  return done_reading && slots_.empty() && out_[cur_].Empty() && !send_in_flight_;
}

void Connection::OnRecv(size_t n) {
  rbuf_.Commit(n);
  worker_->stats().net_input_bytes += n;
  ProcessInput();
  AfterProgress(true);
}

void Connection::ResumeProcessing() {
  if (closing_) return;
  ProcessInput();
  AfterProgress(false);
}

void Connection::ProcessInput() {
  const uint32_t max_cmds = worker_->config().max_cmds_per_turn;
  // The budget is per loop turn, not per call: io_uring multishot recv can call OnRecv many
  // times in one turn, and each call must not get a fresh budget.
  if (budget_turn_ != worker_->turn()) {
    budget_turn_ = worker_->turn();
    handled_in_turn_ = 0;
  }
  while (!closing_ && !close_after_reply_ && !rbuf_.Empty()) {
    if (Backpressured()) {
      if (!paused_) ++worker_->stats().backpressure_pauses;
      paused_ = true;
      break;
    }
    // DC-11: bound the work one connection does per loop turn so others aren't starved.
    if (handled_in_turn_ >= max_cmds) {
      ++worker_->stats().fairness_yields;
      break;
    }
    size_t consumed = 0;
    RespParser::Result res = parser_.Parse(rbuf_.Readable(), &consumed, &args_);
    rbuf_.Consume(consumed);
    if (res == RespParser::Result::kInputPending) break;
    if (res != RespParser::Result::kOk) {
      ++worker_->stats().protocol_errors;
      RespWriter(InOrderTarget()).Error(parser_.ErrorText(res));
      rbuf_.Clear();
      close_after_reply_ = true;
      break;
    }
    if (args_.empty()) continue;
    Dispatch(args_);
    ++handled_in_turn_;
  }
  // DC-10: everything this read batch sends to a shard leaves as one message per shard.
  worker_->SendOpenBatches();
  DrainCompleted();
  Flush();
}

std::string* Connection::InOrderTarget() {
  if (slots_.empty()) {
    ++replies_building_;
    return building().Tail();
  }
  PendingReply& slot = Slot(OpenSlot(nullptr, 0));
  slot.done = true;
  return &slot.reply;
}

uint64_t Connection::OpenSlot(const CommandSpec* cmd, uint32_t parts) {
  PendingReply& slot = slots_.emplace_back();
  slot.cmd = cmd;
  slot.parts_left = parts;
  if (parts > 0) ++unresolved_;
  return base_seq_ + slots_.size() - 1;
}

void Connection::Dispatch(const CmdArgs& args) {
  ++worker_->stats().commands_processed;
  const CommandSpec* cmd = worker_->commands().Find(args[0]);
  if (cmd == nullptr) {
    RespWriter(InOrderTarget()).Error(UnknownCommandError(args));
    return;
  }
  if (!CommandTable::ArityOk(*cmd, args.size())) {
    RespWriter(InOrderTarget()).Error(WrongArgsError(cmd->name));
    return;
  }
  if (cmd->check != nullptr) {
    std::string err = cmd->check(args);
    if (!err.empty()) {
      RespWriter(InOrderTarget()).Error(err);
      return;
    }
  }

  switch (cmd->route) {
    case Route::kCoordinator: {
      CoordContext ctx{*this, *worker_, args, RespWriter(InOrderTarget())};
      cmd->coord_fn(ctx);
      return;
    }
    case Route::kSingleKey: {
      uint32_t shard = ShardOf(args[1], worker_->num_shards());
      if (shard == worker_->index() && slots_.empty()) {
        // DC-07 fast path: the key is ours and nothing is queued ahead of it, so run it now
        // and write the reply straight into the output buffer.
        worker_->RunLocal(*cmd, args, building().Tail());
        ++replies_building_;
        return;
      }
      RunOrSend(shard, OpenSlot(cmd, 1), *cmd, 0, args);
      return;
    }
    case Route::kPerKey:
      return DispatchPerKey(*cmd, args);
    case Route::kAllShards:
      return DispatchAllShards(*cmd, args);
  }
}

// Multi-key commands become one op per key, grouped into per-shard batches like any other op.
// They are not atomic across shards, which differs from Redis (see docs/DESIGN.md).
void Connection::DispatchPerKey(const CommandSpec& cmd, const CmdArgs& args) {
  const size_t step = cmd.key_step;
  const uint32_t nkeys = static_cast<uint32_t>((args.size() - 1) / step);
  uint64_t seq = OpenSlot(&cmd, nkeys);
  if (cmd.combine == Combine::kArray) Slot(seq).items.resize(nkeys);
  std::string_view part[3] = {args[0]};
  for (uint32_t i = 0; i < nkeys; ++i) {
    size_t k = 1 + i * step;
    for (size_t j = 0; j < step; ++j) part[1 + j] = args[k + j];
    RunOrSend(ShardOf(args[k], worker_->num_shards()), seq, cmd, i,
              std::span<const std::string_view>(part, 1 + step));
  }
}

void Connection::DispatchAllShards(const CommandSpec& cmd, const CmdArgs& args) {
  const uint32_t n = worker_->num_shards();
  uint64_t seq = OpenSlot(&cmd, n);
  if (cmd.combine == Combine::kInfo) {
    PendingReply& slot = Slot(seq);
    slot.info = std::make_unique<InfoCounters>();
    slot.info->fill(0);
    if (args.size() > 1) slot.info_section = std::string(args[1]);
  }
  for (uint32_t s = 0; s < n; ++s) RunOrSend(s, seq, cmd, s, args);
}

void Connection::RunOrSend(uint32_t shard, uint64_t seq, const CommandSpec& cmd, uint32_t part,
                           std::span<const std::string_view> args) {
  if (shard == worker_->index()) {
    scratch_.clear();
    int64_t integer = worker_->RunLocal(cmd, args, &scratch_);
    CompletePart(seq, part, integer, scratch_);
    return;
  }
  worker_->BatchFor(shard, *this).AddOp(seq, &cmd, part, args);
  ++worker_->stats().remote_ops_sent;
}

void Connection::CompletePart(uint64_t seq, uint32_t part, int64_t integer, std::string_view reply) {
  PendingReply& slot = Slot(seq);
  switch (slot.cmd->combine) {
    case Combine::kReply:
      slot.reply.assign(reply);
      break;
    case Combine::kSum:
      slot.sum += integer;
      break;
    case Combine::kArray:
      slot.items[part].assign(reply);
      break;
    case Combine::kOk:
      break;
    case Combine::kInfo: {
      InfoCounters part_counters;
      KV_CHECK(reply.size() == sizeof(part_counters));
      std::memcpy(part_counters.data(), reply.data(), sizeof(part_counters));
      for (size_t i = 0; i < part_counters.size(); ++i) (*slot.info)[i] += part_counters[i];
      break;
    }
  }
  if (--slot.parts_left == 0) Finalize(slot);
}

void Connection::Finalize(PendingReply& slot) {
  RespWriter w(&slot.reply);
  switch (slot.cmd->combine) {
    case Combine::kReply:
      break;
    case Combine::kSum:
      w.Integer(slot.sum);
      break;
    case Combine::kArray:
      w.ArrayHeader(slot.items.size());
      for (const std::string& item : slot.items) w.Raw(item);
      slot.items.clear();
      break;
    case Combine::kOk:
      w.Ok();
      break;
    case Combine::kInfo:
      w.Bulk(FormatInfo(*slot.info, slot.info_section, *worker_));
      slot.info.reset();
      break;
  }
  slot.done = true;
  --unresolved_;
}

void Connection::DrainCompleted() {
  while (!slots_.empty() && slots_.front().done) {
    building().Append(std::move(slots_.front().reply));
    ++replies_building_;
    slots_.pop_front();
    ++base_seq_;
  }
}

void Connection::OnRemoteResults(const ShardBatch& batch) {
  if (closing_) return;
  for (const ShardOp& op : batch.ops) CompletePart(op.seq, op.part, op.integer, batch.Reply(op));
  DrainCompleted();
  Flush();
  AfterProgress(false);
}

// DC-09: everything queued goes out in one writev. While a send is in flight new replies
// collect in the other buffer and leave when it completes.
void Connection::Flush() {
  while (!closing_ && !send_in_flight_ && !building().Empty()) {
    cur_ ^= 1;
    ReplyBuffer& out = sending();
    replies_sending_ = replies_building_;
    replies_building_ = 0;
    bytes_sending_ = out.Bytes();
    worker_->stats().net_output_bytes += bytes_sending_;
    switch (socket_->Send(out.Iovecs())) {
      case net::StreamSocket::SendResult::kPending:
        send_in_flight_ = true;
        return;
      case net::StreamSocket::SendResult::kError:
        Close();
        return;
      case net::StreamSocket::SendResult::kDone:
        out.Clear();
        replies_sending_ = 0;
        bytes_sending_ = 0;
        break;
    }
  }
}

void Connection::OnSendDone() {
  send_in_flight_ = false;
  sending().Clear();
  replies_sending_ = 0;
  bytes_sending_ = 0;
  Flush();
  AfterProgress(false);
}

void Connection::AfterProgress(bool from_recv) {
  if (closing_) return;
  if (ReadyToClose()) return Close();
  if (Backpressured()) return;
  paused_ = false;
  if (!rbuf_.Empty()) {
    // Parsed input is waiting (fairness cap or a pause that just ended): continue next turn.
    if (!close_after_reply_) worker_->ScheduleResume(*this);
    return;
  }
  // Inside OnRecv the socket keeps reading on its own; elsewhere it may need a nudge.
  if (!from_recv && WantsRecv()) socket_->ResumeRecv();
}

void Connection::OnEof() {
  peer_closed_ = true;
  AfterProgress(false);
}

void Connection::OnError(int err) {
  KV_LOG_DEBUG("connection %u: socket error %d", id_, err);
  Close();
}

void Connection::Close() {
  if (closing_) return;
  closing_ = true;
  socket_->Close();
}

void Connection::OnClosed() {
  // Must be the last thing we do: this deletes the connection.
  worker_->ReleaseConnection(id_);
}

}  // namespace kv
