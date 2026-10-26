// io_uring backend.
//
// Design after Dragonfly (DC-13, DC-14): helio/util/fibers/uring_proactor.cc and
// uring_socket.cc.
//  - One ring per thread with SINGLE_ISSUER | DEFER_TASKRUN | COOP_TASKRUN | SUBMIT_ALL, and
//    no SQPOLL.
//  - Submission is lazy: SQEs pile up during a loop turn and go to the kernel in the one
//    io_uring_enter the loop makes per turn (or earlier if the SQ fills). Completions are
//    reaped in batches.
//  - user_data is an index into a free list of operation slots, so a completion maps straight
//    back to the object that issued it.
//  - Optional multishot recv drawing from a provided buffer ring (kernel 5.19+), re-armed
//    when the kernel stops it, usually after running out of buffers.

#include <liburing.h>
#include <sys/eventfd.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <climits>
#include <cstdlib>
#include <cstring>
#include <unordered_set>
#include <vector>

#include "base/log.h"
#include "net/reactor.h"
#include "net/reactor_internal.h"
#include "net/socket_util.h"

namespace kv::net {
namespace {

// DC-13: UringProactor::Init flags.
constexpr unsigned kSetupFlags = IORING_SETUP_SINGLE_ISSUER | IORING_SETUP_DEFER_TASKRUN |
                                 IORING_SETUP_COOP_TASKRUN | IORING_SETUP_SUBMIT_ALL;
constexpr unsigned kCqeBatch = 128;
constexpr uint16_t kBufGroup = 0;

// user_data values that don't refer to a slot.
constexpr uint64_t kTagIgnore = ~uint64_t{0};       // Cancels, MSG_RING sends.
constexpr uint64_t kTagMsgWake = ~uint64_t{0} - 1;  // Arrives on a ring woken by MSG_RING.

enum class OpKind : uint8_t { kFree, kAccept, kRecv, kRecvMulti, kSend, kTimer, kWakeRead };

class UringReactor;

class UringSocket final : public StreamSocket {
 public:
  UringSocket(UringReactor* reactor, int fd, StreamHandler* handler)
      : reactor_(reactor), fd_(fd), handler_(handler) {}

  SendResult Send(std::span<const iovec> iov) override;
  void ResumeRecv() override { StartRecv(); }
  void Close() override;
  int fd() const override { return fd_; }

  void StartRecv();
  void OnRecv(int res);
  void OnRecvMulti(int res, uint32_t flags);
  void OnSend(int res);
  void FinishClose() { handler_->OnClosed(); }

 private:
  void SubmitSend();
  void CancelRecv();
  void Deliver(const char* data, size_t len);
  void MaybeFinishClose();

  UringReactor* reactor_;
  int fd_;
  StreamHandler* handler_;
  unsigned inflight_ = 0;  // Operations the kernel still owns. The fd closes at zero.
  bool recv_armed_ = false;
  bool cancel_sent_ = false;
  bool send_inflight_ = false;
  bool closing_ = false;
  bool finished_ = false;
  uint64_t recv_user_data_ = 0;
  std::vector<iovec> iov_;
  size_t iov_idx_ = 0;
};

struct UringListener {
  Fd fd;
  Reactor::AcceptCallback cb;
};

struct UringTimer {
  __kernel_timespec ts;
  std::function<void()> cb;
};

class UringReactor final : public Reactor {
 public:
  explicit UringReactor(const ReactorOptions& opts) : msg_ring_wake_(opts.msg_ring_wake) {
    io_uring_params params{};
    params.flags = kSetupFlags | IORING_SETUP_CQSIZE;
    params.cq_entries = opts.ring_entries * 8;
    int r = io_uring_queue_init_params(opts.ring_entries, &ring_, &params);
    if (r < 0) Fatal("io_uring_queue_init_params: %s", strerror(-r));

    wake_fd_.reset(eventfd(0, EFD_CLOEXEC));
    KV_CHECK(wake_fd_);
    ArmWakeRead();

    if (opts.multishot_recv) SetupBufRing(opts.buf_count, opts.buf_size);
  }

  ~UringReactor() override {
    RunDeferred();
    // The owner closes every stream before stopping, so no recv can still target its memory.
    if (buf_ring_ != nullptr) io_uring_free_buf_ring(&ring_, buf_ring_, buf_count_, kBufGroup);
    io_uring_queue_exit(&ring_);
    std::free(buf_mem_);
    for (UringSocket* s : sockets_) {
      ::close(s->fd());
      delete s;
    }
  }

  const char* Name() const override { return "uring"; }

  void Run(LoopDelegate* delegate) override {
    io_uring_cqe* cqes[kCqeBatch];
    while (!stop_) {
      bool busy = delegate->OnLoopTurn();
      if (stop_) break;
      busy = busy || !closed_.empty();
      bool sleep = !busy && delegate->BeforeSleep();
      // DC-13: the one io_uring_enter per loop turn. It submits everything queued since the
      // last turn and, because of DEFER_TASKRUN, is also what runs completions.
      ++ThreadSyscalls().io_uring_enter;
      int r = sleep ? io_uring_submit_and_wait(&ring_, 1) : io_uring_submit_and_get_events(&ring_);
      if (sleep) delegate->AfterWake();
      if (r < 0 && r != -EINTR && r != -EAGAIN && r != -EBUSY && r != -ETIME) {
        KV_LOG_WARN("io_uring_enter: %s", strerror(-r));
      }
      while (true) {
        unsigned n = io_uring_peek_batch_cqe(&ring_, cqes, kCqeBatch);
        if (n == 0) break;
        for (unsigned i = 0; i < n; ++i) {
          Dispatch(cqes[i]->user_data, cqes[i]->res, cqes[i]->flags);
        }
        io_uring_cq_advance(&ring_, n);
      }
      RunDeferred();
    }
  }

  void Stop() override { stop_ = true; }

  void Listen(int listen_fd, AcceptCallback on_accept) override {
    listeners_.push_back(std::make_unique<UringListener>(UringListener{Fd(listen_fd), std::move(on_accept)}));
    ArmAccept(listeners_.back().get());
  }

  StreamSocket* AddStream(int fd, StreamHandler* handler) override {
    auto* s = new UringSocket(this, fd, handler);
    sockets_.insert(s);
    return s;
  }

  void AddTimer(std::chrono::milliseconds period, std::function<void()> callback) override {
    auto timer = std::make_unique<UringTimer>();
    timer->ts.tv_sec = period.count() / 1000;
    timer->ts.tv_nsec = (period.count() % 1000) * 1000000;
    timer->cb = std::move(callback);
    ArmTimer(timer.get());
    timers_.push_back(std::move(timer));
  }

  void WakeRemote(Reactor& target) override {
    auto& t = static_cast<UringReactor&>(target);
    if (!msg_ring_wake_) {
      t.WakeFromAnyThread();
      return;
    }
    // Post a CQE straight into the target's ring. The SQE rides along with this thread's next
    // io_uring_enter, so the wakeup costs no syscall of its own.
    io_uring_sqe* sqe = GetSqe();
    io_uring_prep_msg_ring(sqe, t.ring_.ring_fd, 0, kTagMsgWake, 0);
    io_uring_sqe_set_data64(sqe, kTagIgnore);
    sqe->flags |= IOSQE_CQE_SKIP_SUCCESS;
  }

  void WakeFromAnyThread() override {
    uint64_t one = 1;
    ++ThreadSyscalls().eventfd_write;
    (void)!::write(wake_fd_.get(), &one, sizeof(one));
  }

  io_uring_sqe* GetSqe() {
    io_uring_sqe* sqe = io_uring_get_sqe(&ring_);
    if (sqe == nullptr) {
      // SQ full: submit early (DC-13 submits only when full or once per loop).
      ++ThreadSyscalls().io_uring_enter;
      io_uring_submit(&ring_);
      sqe = io_uring_get_sqe(&ring_);
      KV_CHECK(sqe != nullptr);
    }
    return sqe;
  }

  // The low 32 bits of user_data index the slot; the high bits hold the slot's generation, so
  // a cancel aimed at an old operation can't hit a newer one that reused the slot.
  uint64_t AllocSlot(OpKind kind, void* target) {
    uint32_t idx;
    if (free_head_ != kNoSlot) {
      idx = free_head_;
      free_head_ = slots_[idx].next_free;
    } else {
      idx = static_cast<uint32_t>(slots_.size());
      slots_.emplace_back();
    }
    Slot& s = slots_[idx];
    s.kind = kind;
    s.target = target;
    return (static_cast<uint64_t>(s.gen) << 32) | idx;
  }

  bool multishot() const { return buf_ring_ != nullptr; }
  const char* BufAddr(unsigned bid) const { return buf_mem_ + size_t{bid} * buf_size_; }

  void RecycleBuffer(unsigned bid) {
    io_uring_buf_ring_add(buf_ring_, buf_mem_ + size_t{bid} * buf_size_, buf_size_, bid,
                          io_uring_buf_ring_mask(buf_count_), 0);
    io_uring_buf_ring_advance(buf_ring_, 1);
  }

  void QueueClosed(UringSocket* s) { closed_.push_back(s); }

 private:
  static constexpr uint32_t kNoSlot = ~uint32_t{0};

  struct Slot {
    OpKind kind = OpKind::kFree;
    uint32_t gen = 0;
    uint32_t next_free = kNoSlot;
    void* target = nullptr;
  };

  void FreeSlot(uint32_t idx) {
    Slot& s = slots_[idx];
    s.kind = OpKind::kFree;
    s.target = nullptr;
    ++s.gen;
    s.next_free = free_head_;
    free_head_ = idx;
  }

  void SetupBufRing(unsigned count, unsigned size) {
    int err = 0;
    buf_ring_ = io_uring_setup_buf_ring(&ring_, count, kBufGroup, 0, &err);
    if (buf_ring_ == nullptr) {
      KV_LOG_WARN("io_uring_setup_buf_ring failed (%s); using one recv per read", strerror(-err));
      return;
    }
    buf_count_ = count;
    buf_size_ = size;
    buf_mem_ = static_cast<char*>(std::aligned_alloc(4096, size_t{count} * size));
    KV_CHECK(buf_mem_ != nullptr);
    for (unsigned i = 0; i < count; ++i) {
      io_uring_buf_ring_add(buf_ring_, buf_mem_ + size_t{i} * size, size, static_cast<uint16_t>(i),
                            io_uring_buf_ring_mask(count), static_cast<int>(i));
    }
    io_uring_buf_ring_advance(buf_ring_, static_cast<int>(count));
  }

  void ArmAccept(UringListener* l) {
    io_uring_sqe* sqe = GetSqe();
    io_uring_prep_multishot_accept(sqe, l->fd.get(), nullptr, nullptr, SOCK_CLOEXEC);
    io_uring_sqe_set_data64(sqe, AllocSlot(OpKind::kAccept, l));
  }

  void ArmTimer(UringTimer* t) {
    io_uring_sqe* sqe = GetSqe();
    io_uring_prep_timeout(sqe, &t->ts, 0, 0);
    io_uring_sqe_set_data64(sqe, AllocSlot(OpKind::kTimer, t));
  }

  void ArmWakeRead() {
    io_uring_sqe* sqe = GetSqe();
    io_uring_prep_read(sqe, wake_fd_.get(), &wake_buf_, sizeof(wake_buf_), 0);
    io_uring_sqe_set_data64(sqe, AllocSlot(OpKind::kWakeRead, nullptr));
  }

  void Dispatch(uint64_t user_data, int res, uint32_t flags) {
    if (user_data == kTagIgnore || user_data == kTagMsgWake) return;
    uint32_t idx = static_cast<uint32_t>(user_data);
    KV_DCHECK(idx < slots_.size());
    Slot& slot = slots_[idx];
    OpKind kind = slot.kind;
    void* target = slot.target;
    bool more = flags & IORING_CQE_F_MORE;
    if (!more) FreeSlot(idx);

    switch (kind) {
      case OpKind::kAccept: {
        auto* l = static_cast<UringListener*>(target);
        if (res >= 0) {
          l->cb(res);
        } else if (res != -ECANCELED) {
          KV_LOG_WARN("accept: %s", strerror(-res));
        }
        if (!more && !stop_) ArmAccept(l);
        break;
      }
      case OpKind::kRecv:
        static_cast<UringSocket*>(target)->OnRecv(res);
        break;
      case OpKind::kRecvMulti:
        static_cast<UringSocket*>(target)->OnRecvMulti(res, flags);
        break;
      case OpKind::kSend:
        static_cast<UringSocket*>(target)->OnSend(res);
        break;
      case OpKind::kTimer: {
        auto* t = static_cast<UringTimer*>(target);
        if (res == -ETIME || res == 0) t->cb();
        if (!stop_) ArmTimer(t);
        break;
      }
      case OpKind::kWakeRead:
        if (!stop_) ArmWakeRead();
        break;
      case OpKind::kFree:
        KV_LOG_ERROR("completion for a free slot %u (res=%d)", idx, res);
        break;
    }
  }

  void RunDeferred() {
    while (!closed_.empty()) {
      std::vector<UringSocket*> closed;
      closed.swap(closed_);
      for (UringSocket* s : closed) {
        sockets_.erase(s);
        s->FinishClose();
        delete s;
      }
    }
  }

  io_uring ring_{};
  bool stop_ = false;
  bool msg_ring_wake_;
  Fd wake_fd_;
  uint64_t wake_buf_ = 0;

  std::vector<Slot> slots_;
  uint32_t free_head_ = kNoSlot;

  io_uring_buf_ring* buf_ring_ = nullptr;
  char* buf_mem_ = nullptr;
  unsigned buf_count_ = 0;
  unsigned buf_size_ = 0;

  std::vector<std::unique_ptr<UringListener>> listeners_;
  std::vector<std::unique_ptr<UringTimer>> timers_;
  std::unordered_set<UringSocket*> sockets_;
  std::vector<UringSocket*> closed_;
};

void UringSocket::StartRecv() {
  if (closing_ || recv_armed_ || !handler_->WantsRecv()) return;
  io_uring_sqe* sqe = reactor_->GetSqe();
  if (reactor_->multishot()) {
    io_uring_prep_recv_multishot(sqe, fd_, nullptr, 0, 0);
    sqe->flags |= IOSQE_BUFFER_SELECT;
    sqe->buf_group = kBufGroup;
    recv_user_data_ = reactor_->AllocSlot(OpKind::kRecvMulti, this);
  } else {
    std::span<char> buf = handler_->RecvBuffer();
    io_uring_prep_recv(sqe, fd_, buf.data(), buf.size(), 0);
    recv_user_data_ = reactor_->AllocSlot(OpKind::kRecv, this);
  }
  io_uring_sqe_set_data64(sqe, recv_user_data_);
  recv_armed_ = true;
  cancel_sent_ = false;
  ++inflight_;
}

void UringSocket::OnRecv(int res) {
  recv_armed_ = false;
  --inflight_;
  if (closing_) return MaybeFinishClose();
  if (res > 0) {
    handler_->OnRecv(static_cast<size_t>(res));
    StartRecv();
  } else if (res == 0) {
    handler_->OnEof();
  } else if (res == -EINTR || res == -EAGAIN || res == -ECANCELED) {
    StartRecv();
  } else {
    handler_->OnError(-res);
  }
}

void UringSocket::Deliver(const char* data, size_t len) {
  while (len > 0 && !closing_) {
    std::span<char> buf = handler_->RecvBuffer();
    size_t n = std::min(len, buf.size());
    std::memcpy(buf.data(), data, n);
    handler_->OnRecv(n);
    data += n;
    len -= n;
  }
}

void UringSocket::OnRecvMulti(int res, uint32_t flags) {
  bool more = flags & IORING_CQE_F_MORE;
  if (!more) {
    recv_armed_ = false;
    --inflight_;
  }
  if (res > 0) {
    unsigned bid = flags >> IORING_CQE_BUFFER_SHIFT;
    // Copy into the connection's buffer and hand the provided buffer straight back.
    Deliver(reactor_->BufAddr(bid), static_cast<size_t>(res));
    reactor_->RecycleBuffer(bid);
  }
  if (closing_) return MaybeFinishClose();

  if (res > 0) {
    if (!more) {
      StartRecv();
    } else if (!handler_->WantsRecv()) {
      // Backpressure: the kernel would keep delivering, so stop the multishot. Data that
      // lands before the cancel takes effect is still buffered.
      CancelRecv();
    }
  } else if (res == 0) {
    handler_->OnEof();
  } else if (res == -ENOBUFS || res == -ECANCELED || res == -EINTR || res == -EAGAIN) {
    // DC-14: the kernel stops a multishot recv when the ring runs dry; re-arm it.
    StartRecv();
  } else {
    handler_->OnError(-res);
  }
}

void UringSocket::CancelRecv() {
  if (!recv_armed_ || cancel_sent_) return;
  io_uring_sqe* sqe = reactor_->GetSqe();
  io_uring_prep_cancel64(sqe, recv_user_data_, 0);
  io_uring_sqe_set_data64(sqe, kTagIgnore);
  cancel_sent_ = true;
}

StreamSocket::SendResult UringSocket::Send(std::span<const iovec> iov) {
  KV_DCHECK(!send_inflight_);
  if (iov.empty()) return SendResult::kDone;
  iov_.assign(iov.begin(), iov.end());
  iov_idx_ = 0;
  SubmitSend();
  return SendResult::kPending;
}

void UringSocket::SubmitSend() {
  io_uring_sqe* sqe = reactor_->GetSqe();
  unsigned count = static_cast<unsigned>(std::min<size_t>(iov_.size() - iov_idx_, IOV_MAX));
  io_uring_prep_writev(sqe, fd_, &iov_[iov_idx_], count, 0);
  io_uring_sqe_set_data64(sqe, reactor_->AllocSlot(OpKind::kSend, this));
  send_inflight_ = true;
  ++inflight_;
}

void UringSocket::OnSend(int res) {
  send_inflight_ = false;
  --inflight_;
  if (closing_) return MaybeFinishClose();
  if (res < 0) {
    if (res == -EINTR || res == -EAGAIN) {
      SubmitSend();
    } else {
      handler_->OnError(-res);
    }
    return;
  }
  size_t left = static_cast<size_t>(res);
  while (left > 0 && iov_idx_ < iov_.size()) {
    iovec& v = iov_[iov_idx_];
    if (left >= v.iov_len) {
      left -= v.iov_len;
      ++iov_idx_;
    } else {
      v.iov_base = static_cast<char*>(v.iov_base) + left;
      v.iov_len -= left;
      left = 0;
    }
  }
  if (iov_idx_ < iov_.size()) {
    SubmitSend();  // Partial write: send the rest.
  } else {
    iov_.clear();
    handler_->OnSendDone();
  }
}

void UringSocket::Close() {
  if (closing_) return;
  closing_ = true;
  // shutdown() makes a pending recv complete and a blocked send fail; the cancel covers
  // anything still queued. The fd itself is closed only when the kernel is done with it.
  ++ThreadSyscalls().shutdown;
  ::shutdown(fd_, SHUT_RDWR);
  if (inflight_ > 0) {
    io_uring_sqe* sqe = reactor_->GetSqe();
    io_uring_prep_cancel_fd(sqe, fd_, IORING_ASYNC_CANCEL_ALL);
    io_uring_sqe_set_data64(sqe, kTagIgnore);
  }
  MaybeFinishClose();
}

void UringSocket::MaybeFinishClose() {
  if (!closing_ || inflight_ > 0 || finished_) return;
  finished_ = true;
  CloseFd(fd_);
  reactor_->QueueClosed(this);
}

}  // namespace

std::unique_ptr<Reactor> CreateUringReactor(const ReactorOptions& options) {
  return std::make_unique<UringReactor>(options);
}

UringSupport ProbeUring() {
  UringSupport s;
  io_uring ring{};
  io_uring_params params{};
  params.flags = kSetupFlags;
  int r = io_uring_queue_init_params(8, &ring, &params);
  if (r < 0) {
    s.why = std::string("io_uring_setup(SINGLE_ISSUER|DEFER_TASKRUN|COOP_TASKRUN|SUBMIT_ALL): ") +
            strerror(-r);
    if (r == -EPERM || r == -ENOSYS) s.why += " (blocked by seccomp or kernel.io_uring_disabled?)";
    if (r == -EINVAL) s.why += " (kernel older than 6.1?)";
    return s;
  }
  io_uring_probe* probe = io_uring_get_probe_ring(&ring);
  if (probe == nullptr) {
    s.why = "io_uring_get_probe_ring failed";
    io_uring_queue_exit(&ring);
    return s;
  }
  const int kRequired[] = {IORING_OP_ACCEPT, IORING_OP_RECV,  IORING_OP_WRITEV,
                           IORING_OP_READ,   IORING_OP_TIMEOUT, IORING_OP_ASYNC_CANCEL};
  for (int op : kRequired) {
    if (!io_uring_opcode_supported(probe, op)) {
      s.why = "io_uring opcode " + std::to_string(op) + " not supported";
      io_uring_free_probe(probe);
      io_uring_queue_exit(&ring);
      return s;
    }
  }
  s.msg_ring = io_uring_opcode_supported(probe, IORING_OP_MSG_RING);
  io_uring_free_probe(probe);

  int err = 0;
  io_uring_buf_ring* br = io_uring_setup_buf_ring(&ring, 8, kBufGroup, 0, &err);
  if (br != nullptr) {
    s.buf_ring = true;
    io_uring_free_buf_ring(&ring, br, 8, kBufGroup);
  }
  io_uring_queue_exit(&ring);
  s.usable = true;
  return s;
}

}  // namespace kv::net
