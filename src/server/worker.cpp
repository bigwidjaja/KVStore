#include "server/worker.h"

#include <pthread.h>
#include <sched.h>

#include <cstdio>

#include "base/log.h"
#include "base/time.h"
#include "server/command_table.h"
#include "server/config.h"
#include "server/connection.h"
#include "server/server.h"

namespace kv {
namespace {

// DC-06: tq_seq_ holds this value while the thread is (about to be) blocked in its poll.
// Producers add 2 per message, so the value is never 1 again until the thread re-announces.
constexpr uint64_t kWaitState = 1;

constexpr unsigned kMaxInboxPerTurn = 512;   // Bounds message work per turn so I/O keeps up.
constexpr unsigned kSpinBeforeSleep = 64;    // DC-06: spin briefly before sleeping.
constexpr size_t kMaxOpsPerBatch = 32;       // DC-10: Dragonfly squashes up to 32 per hop.
constexpr size_t kMaxBatchArgBytes = 1 << 20;

}  // namespace

Worker::Worker(Server* server, uint32_t index, net::Fd listener)
    : server_(server),
      index_(index),
      listener_(std::move(listener)),
      shard_(index),
      inbox_(server->config().queue_len),
      outbox_(server->num_threads()),
      open_batches_(server->num_threads()) {}

Worker::~Worker() {
  Join();
  reactor_.reset();
}

uint32_t Worker::num_shards() const { return server_->num_threads(); }
const Config& Worker::config() const { return server_->config(); }
const CommandTable& Worker::commands() const { return server_->commands(); }

void Worker::Start(const StartupLatches& latches) {
  thread_ = std::thread(&Worker::ThreadMain, this, latches);
}

void Worker::Join() {
  if (thread_.joinable()) thread_.join();
}

void Worker::RequestStop() {
  stop_requested_.store(true, std::memory_order_release);
  reactor_->WakeFromAnyThread();
}

void Worker::ThreadMain(StartupLatches latches) {
  tlocal_ = this;
  Shard::SetLocal(&shard_);
  char name[16];
  snprintf(name, sizeof(name), "w%u", index_);
  SetThreadLogName(name);
  pthread_setname_np(pthread_self(), name);

  if (config().pin) {
    // DC-03: one thread per core.
    int cpu = server_->CpuFor(index_);
    cpu_set_t set;
    CPU_ZERO(&set);
    CPU_SET(cpu, &set);
    if (int err = pthread_setaffinity_np(pthread_self(), sizeof(set), &set); err != 0) {
      KV_LOG_WARN("pthread_setaffinity_np(cpu %d): %s", cpu, strerror(err));
    }
  }

  // SINGLE_ISSUER rings must be created by the thread that submits to them.
  reactor_ = net::CreateReactor(server_->reactor_options());
  // Peers may wake us as soon as they run, so wait until every reactor exists.
  latches.reactors_ready->arrive_and_wait();

  now_ms_ = UnixTimeMs();
  reactor_->AddTimer(std::chrono::milliseconds(std::max<uint32_t>(1, 1000 / config().hz)), [this] {
    now_ms_ = UnixTimeMs();
    shard_.db().Heartbeat(now_ms_);  // DC-18: active expiry.
  });
  reactor_->Listen(listener_.release(), [this](int fd) { OnAccept(fd); });
  latches.started->count_down();

  reactor_->Run(this);

  // Other threads may still message us until every loop has ended, so the reactor (which
  // they wake) must outlive them all.
  latches.stopped->arrive_and_wait();
  reactor_.reset();
  tlocal_ = nullptr;
  Shard::SetLocal(nullptr);
}

void Worker::OnAccept(int fd) {
  if (shutting_down_) {
    net::CloseFd(fd);
    return;
  }
  net::SetNoDelay(fd);  // DC-02: TCP_NODELAY on by default.
  if (config().sndbuf > 0) net::SetSendBuffer(fd, config().sndbuf);

  uint32_t id;
  if (!free_ids_.empty()) {
    id = free_ids_.back();
    free_ids_.pop_back();
  } else {
    id = static_cast<uint32_t>(conns_.size());
    conns_.emplace_back();
  }
  ConnSlot& slot = conns_[id];
  slot.conn = std::make_unique<Connection>(this, id, slot.gen, server_->NextClientId());
  Connection* conn = slot.conn.get();
  ++live_conns_;
  ++stats_.connections_received;
  stats_.connected_clients = live_conns_;
  conn->Start(reactor_->AddStream(fd, conn));
}

Connection* Worker::FindConnection(uint32_t id, uint32_t gen) {
  // Phase 4, step 10: replies name their connection by (id, generation), never by pointer,
  // so one that lands after its client left finds nothing.
  if (id >= conns_.size()) return nullptr;
  ConnSlot& slot = conns_[id];
  return slot.gen == gen ? slot.conn.get() : nullptr;
}

void Worker::ReleaseConnection(uint32_t id) {
  ConnSlot& slot = conns_[id];
  ++slot.gen;
  free_ids_.push_back(id);
  --live_conns_;
  stats_.connected_clients = live_conns_;
  slot.conn.reset();
}

void Worker::BeginShutdown() {
  shutting_down_ = true;
  for (ConnSlot& slot : conns_) {
    if (slot.conn) slot.conn->Close();
  }
}

bool Worker::OnLoopTurn() {
  ++stats_.loop_turns;
  ++turn_;
  now_ms_ = UnixTimeMs();
  // DC-06: remember the wakeup word before draining. If a producer pushes after this point,
  // its increment makes the CAS in BeforeSleep() fail, so we can't sleep through its message.
  tq_snapshot_ = tq_seq_.load(std::memory_order_acquire);
  if (!shutting_down_ && stop_requested_.load(std::memory_order_acquire)) BeginShutdown();

  for (unsigned i = 0; i < kMaxInboxPerTurn; ++i) {
    InlineTask task;
    if (!inbox_.TryPop(task)) break;
    task();
  }
  if (outbox_pending_ > 0) FlushOutboxes();
  RunResumes();

  if (shutting_down_ && live_conns_ == 0) {
    reactor_->Stop();
    return false;
  }
  return outbox_pending_ > 0 || !resume_.empty() || !inbox_.Empty();
}

bool Worker::BeforeSleep() {
  for (unsigned i = 0; i < kSpinBeforeSleep; ++i) {
    if (!inbox_.Empty()) return false;
    CpuRelax();
  }
  uint64_t expected = tq_snapshot_;
  if (!tq_seq_.compare_exchange_strong(expected, kWaitState, std::memory_order_acq_rel)) {
    return false;  // A message arrived since the snapshot.
  }
  ++stats_.sleeps;
  return true;
}

void Worker::AfterWake() {
  // Leave the wait state so producers stop sending wakeups we don't need.
  uint64_t expected = kWaitState;
  tq_seq_.compare_exchange_strong(expected, 0, std::memory_order_acq_rel);
}

// Design after Dragonfly (DC-06): ProactorBase::WakeupIfNeeded. Only a producer that finds the
// target in the wait state pays for a wakeup; a busy target picks the message up on its next
// loop turn for free.
void Worker::NotifyPeer(uint32_t target) {
  Worker& peer = server_->worker(target);
  uint64_t prev = peer.tq_seq_.fetch_add(2, std::memory_order_acq_rel);
  if (prev == kWaitState || config().wake_always) {
    reactor_->WakeRemote(*peer.reactor_);
    ++stats_.wakeups_sent;
  }
}

void Worker::SendTo(uint32_t target, InlineTask task) {
  std::deque<InlineTask>& pending = outbox_[target];
  // Anything already waiting for this target goes first, to keep per-target order.
  if (pending.empty() && server_->worker(target).inbox_.TryPush(task)) {
    NotifyPeer(target);
    return;
  }
  pending.push_back(std::move(task));
  ++outbox_pending_;
  ++stats_.outbox_retries;
}

void Worker::FlushOutboxes() {
  for (uint32_t t = 0; t < outbox_.size(); ++t) {
    std::deque<InlineTask>& pending = outbox_[t];
    bool pushed = false;
    while (!pending.empty() && server_->worker(t).inbox_.TryPush(pending.front())) {
      pending.pop_front();
      --outbox_pending_;
      pushed = true;
    }
    if (pushed) NotifyPeer(t);
  }
}

int64_t Worker::RunOp(const CommandSpec& cmd, std::span<const std::string_view> args,
                      std::string* out) {
  OpContext ctx{shard_, now_ms_, args, RespWriter(out)};
  cmd.shard_fn(ctx);
  return ctx.integer;
}

int64_t Worker::RunLocal(const CommandSpec& cmd, std::span<const std::string_view> args,
                         std::string* out) {
  ++stats_.local_ops;
  return RunOp(cmd, args, out);
}

ShardBatch& Worker::BatchFor(uint32_t target, const Connection& conn) {
  std::unique_ptr<ShardBatch>& batch = open_batches_[target];
  if (batch && (batch->ops.size() >= kMaxOpsPerBatch || batch->arg_bytes.size() >= kMaxBatchArgBytes)) {
    SendBatch(target);
  }
  if (!batch) {
    batch = std::make_unique<ShardBatch>();
    batch->origin = index_;
    batch->target = target;
    batch->conn_id = conn.id();
    batch->conn_gen = conn.gen();
    open_targets_.push_back(target);
  }
  KV_DCHECK(batch->conn_id == conn.id() && batch->conn_gen == conn.gen());
  return *batch;
}

void Worker::SendOpenBatches() {
  for (uint32_t target : open_targets_) {
    if (open_batches_[target]) SendBatch(target);
  }
  open_targets_.clear();
}

void Worker::SendBatch(uint32_t target) {
  ++stats_.batches_sent;
  SendTo(target, [batch = std::move(open_batches_[target])]() mutable {
    Worker::Current()->ExecuteBatch(std::move(batch));
  });
}

void Worker::ExecuteBatch(std::unique_ptr<ShardBatch> batch) {
  for (ShardOp& op : batch->ops) {
    exec_args_.clear();
    for (uint32_t i = 0; i < op.num_args; ++i) exec_args_.push_back(batch->Arg(op.first_arg + i));
    op.reply_begin = batch->replies.size();
    op.integer = RunOp(*op.cmd, exec_args_, &batch->replies);
    op.reply_end = batch->replies.size();
  }
  stats_.remote_ops_executed += batch->ops.size();
  // The reply rides back in the same object, which its origin thread then frees.
  uint32_t origin = batch->origin;
  SendTo(origin, [batch = std::move(batch)]() mutable {
    Worker::Current()->OnBatchReturned(std::move(batch));
  });
}

void Worker::OnBatchReturned(std::unique_ptr<ShardBatch> batch) {
  Connection* conn = FindConnection(batch->conn_id, batch->conn_gen);
  if (conn != nullptr && !conn->closing()) conn->OnRemoteResults(*batch);
}

void Worker::ScheduleResume(Connection& conn) {
  if (conn.resume_queued) return;
  conn.resume_queued = true;
  resume_.emplace_back(conn.id(), conn.gen());
}

void Worker::RunResumes() {
  if (resume_.empty()) return;
  std::vector<std::pair<uint32_t, uint32_t>> batch;
  batch.swap(resume_);
  for (auto [id, gen] : batch) {
    Connection* conn = FindConnection(id, gen);
    if (conn == nullptr) continue;
    conn->resume_queued = false;
    conn->ResumeProcessing();
  }
}

void Worker::CollectInfo(InfoCounters* out) {
  InfoCounters& v = *out;
#define KV_COPY_STAT(name) v[kInfoThread_##name] = stats_.name;
  KV_THREAD_STATS(KV_COPY_STAT)
#undef KV_COPY_STAT
  const DbSlice& db = shard_.db();
  v[kInfoShard_keys] = db.size();
  v[kInfoShard_expires] = db.expires();
  v[kInfoShard_expired_keys] = db.stats().expired_keys;
  v[kInfoShard_keyspace_hits] = db.stats().keyspace_hits;
  v[kInfoShard_keyspace_misses] = db.stats().keyspace_misses;
  v[kInfoShard_table_segments] = db.table().segment_count();
  v[kInfoShard_table_splits] = db.table().split_count();
  v[kInfoShard_table_bytes] = db.table().TableBytes();
  const net::SyscallCounters& sc = net::ThreadSyscalls();
#define KV_COPY_SYSCALL(name) v[kInfoSyscall_##name] = sc.name;
  KV_SYSCALL_KINDS(KV_COPY_SYSCALL)
#undef KV_COPY_SYSCALL
}

}  // namespace kv
