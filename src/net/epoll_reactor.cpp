// Edge-triggered epoll backend.
//
// Design after Dragonfly (DC-12): helio/util/fibers/epoll_proactor.cc and epoll_socket.cc.
// Every socket is registered once with EPOLLIN | EPOLLOUT | EPOLLET | EPOLLRDHUP and never
// re-armed; events are reaped 128 at a time; the loop polls without blocking while work is
// pending.

#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <sys/socket.h>
#include <sys/timerfd.h>
#include <unistd.h>

#include <cerrno>
#include <climits>
#include <cstring>
#include <unordered_set>
#include <vector>

#include "base/log.h"
#include "net/reactor.h"
#include "net/reactor_internal.h"
#include "net/socket_util.h"

namespace kv::net {
namespace {

constexpr int kEventBatch = 128;  // DC-12: kEvBatchSize
constexpr uint32_t kStreamEvents = EPOLLIN | EPOLLOUT | EPOLLET | EPOLLRDHUP;  // DC-12: kEventMask

class EpollReactor;

// Whatever an epoll_event's data.ptr points at.
class EpollItem {
 public:
  virtual ~EpollItem() = default;
  virtual void OnEvent(uint32_t events) = 0;
};

class EpollSocket final : public StreamSocket, public EpollItem {
 public:
  EpollSocket(EpollReactor* reactor, int fd, StreamHandler* handler)
      : reactor_(reactor), fd_(fd), handler_(handler) {}

  SendResult Send(std::span<const iovec> iov) override;
  void ResumeRecv() override;
  void Close() override;
  int fd() const override { return fd_; }

  void OnEvent(uint32_t events) override;
  void ReadLoop();
  void FinishClose() { handler_->OnClosed(); }
  bool closed() const { return closed_; }

  bool resume_queued = false;

 private:
  SendResult WriteSome();

  EpollReactor* reactor_;
  int fd_;
  StreamHandler* handler_;
  bool readable_ = false;  // Saw an EPOLLIN edge and haven't drained the socket since.
  bool peer_hup_ = false;  // Saw EPOLLRDHUP or EPOLLHUP: keep reading until EOF.
  bool closed_ = false;
  bool send_pending_ = false;
  int send_err_ = 0;
  std::vector<iovec> iov_;
  size_t iov_idx_ = 0;
};

class EpollListener final : public EpollItem {
 public:
  EpollListener(int fd, Reactor::AcceptCallback cb) : fd_(fd), cb_(std::move(cb)) {}

  void OnEvent(uint32_t) override {
    while (true) {
      ++ThreadSyscalls().accept;
      int fd = ::accept4(fd_.get(), nullptr, nullptr, SOCK_NONBLOCK | SOCK_CLOEXEC);
      if (fd >= 0) {
        cb_(fd);
        continue;
      }
      if (errno == EINTR || errno == ECONNABORTED) continue;
      if (errno != EAGAIN && errno != EWOULDBLOCK) {
        KV_LOG_WARN("accept4: %s", strerror(errno));
      }
      return;
    }
  }

  int fd() const { return fd_.get(); }

 private:
  Fd fd_;
  Reactor::AcceptCallback cb_;
};

class EpollTimer final : public EpollItem {
 public:
  EpollTimer(int fd, std::function<void()> cb) : fd_(fd), cb_(std::move(cb)) {}

  void OnEvent(uint32_t) override {
    uint64_t expirations;
    ++ThreadSyscalls().timerfd_read;
    if (::read(fd_.get(), &expirations, sizeof(expirations)) == sizeof(expirations)) cb_();
  }

 private:
  Fd fd_;
  std::function<void()> cb_;
};

class EpollWake final : public EpollItem {
 public:
  explicit EpollWake(int fd) : fd_(fd) {}
  void OnEvent(uint32_t) override {
    uint64_t value;
    ++ThreadSyscalls().eventfd_read;
    (void)!::read(fd_, &value, sizeof(value));
  }

 private:
  int fd_;
};

class EpollReactor final : public Reactor {
 public:
  EpollReactor() {
    epfd_.reset(epoll_create1(EPOLL_CLOEXEC));
    KV_CHECK(epfd_);
    wake_fd_.reset(eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC));
    KV_CHECK(wake_fd_);
    auto wake = std::make_unique<EpollWake>(wake_fd_.get());
    Register(wake_fd_.get(), EPOLLIN | EPOLLET, wake.get());
    items_.push_back(std::move(wake));
  }

  ~EpollReactor() override {
    RunDeferred();
    for (EpollSocket* s : sockets_) {
      if (!s->closed()) ::close(s->fd());
      delete s;
    }
  }

  const char* Name() const override { return "epoll"; }

  void Run(LoopDelegate* delegate) override {
    epoll_event events[kEventBatch];
    while (!stop_) {
      bool busy = delegate->OnLoopTurn();
      if (stop_) break;
      busy = busy || !resume_.empty() || !closed_.empty();
      bool sleep = !busy && delegate->BeforeSleep();
      ++ThreadSyscalls().epoll_wait;
      int n = epoll_wait(epfd_.get(), events, kEventBatch, sleep ? -1 : 0);
      if (sleep) delegate->AfterWake();
      if (n < 0) {
        KV_CHECK(errno == EINTR);
        n = 0;
      }
      for (int i = 0; i < n; ++i) {
        static_cast<EpollItem*>(events[i].data.ptr)->OnEvent(events[i].events);
      }
      RunDeferred();
    }
  }

  void Stop() override { stop_ = true; }

  void Listen(int listen_fd, AcceptCallback on_accept) override {
    auto listener = std::make_unique<EpollListener>(listen_fd, std::move(on_accept));
    Register(listen_fd, EPOLLIN | EPOLLET, listener.get());
    // Connections may already be waiting; an edge-triggered registration reports them, but
    // draining here keeps the behavior obvious.
    listener->OnEvent(EPOLLIN);
    items_.push_back(std::move(listener));
  }

  StreamSocket* AddStream(int fd, StreamHandler* handler) override {
    auto* s = new EpollSocket(this, fd, handler);
    sockets_.insert(s);
    Register(fd, kStreamEvents, s);
    return s;
  }

  void AddTimer(std::chrono::milliseconds period, std::function<void()> callback) override {
    int fd = timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK | TFD_CLOEXEC);
    KV_CHECK(fd >= 0);
    itimerspec spec{};
    spec.it_interval.tv_sec = period.count() / 1000;
    spec.it_interval.tv_nsec = (period.count() % 1000) * 1000000;
    spec.it_value = spec.it_interval;
    KV_CHECK(timerfd_settime(fd, 0, &spec, nullptr) == 0);
    auto timer = std::make_unique<EpollTimer>(fd, std::move(callback));
    Register(fd, EPOLLIN | EPOLLET, timer.get());
    items_.push_back(std::move(timer));
  }

  void WakeRemote(Reactor& target) override { target.WakeFromAnyThread(); }

  void WakeFromAnyThread() override {
    uint64_t one = 1;
    ++ThreadSyscalls().eventfd_write;
    (void)!::write(wake_fd_.get(), &one, sizeof(one));
  }

  void Register(int fd, uint32_t events, EpollItem* item) {
    epoll_event ev{};
    ev.events = events;
    ev.data.ptr = item;
    ++ThreadSyscalls().epoll_ctl;
    KV_CHECK(epoll_ctl(epfd_.get(), EPOLL_CTL_ADD, fd, &ev) == 0);
  }

  void Unregister(int fd) {
    ++ThreadSyscalls().epoll_ctl;
    epoll_ctl(epfd_.get(), EPOLL_CTL_DEL, fd, nullptr);
  }

  void QueueResume(EpollSocket* s) { resume_.push_back(s); }
  void QueueClosed(EpollSocket* s) { closed_.push_back(s); }

 private:
  // Socket work that must not run inside the caller's stack: read passes requested by
  // ResumeRecv(), and OnClosed() notifications. Closed sockets are deleted only here, after
  // the event batch, so a stale data.ptr from the same epoll_wait can never dangle.
  void RunDeferred() {
    while (!resume_.empty() || !closed_.empty()) {
      std::vector<EpollSocket*> resume;
      resume.swap(resume_);
      for (EpollSocket* s : resume) {
        s->resume_queued = false;
        if (!s->closed()) s->ReadLoop();
      }
      std::vector<EpollSocket*> closed;
      closed.swap(closed_);
      for (EpollSocket* s : closed) {
        sockets_.erase(s);
        s->FinishClose();
        delete s;
      }
    }
  }

  Fd epfd_;
  Fd wake_fd_;
  bool stop_ = false;
  std::vector<std::unique_ptr<EpollItem>> items_;
  std::unordered_set<EpollSocket*> sockets_;
  std::vector<EpollSocket*> resume_;
  std::vector<EpollSocket*> closed_;
};

void EpollSocket::OnEvent(uint32_t events) {
  if (closed_) return;
  if (events & EPOLLERR) {
    int err = 0;
    socklen_t len = sizeof(err);
    getsockopt(fd_, SOL_SOCKET, SO_ERROR, &err, &len);
    handler_->OnError(err != 0 ? err : EIO);
    return;
  }
  if ((events & EPOLLOUT) && send_pending_) {
    SendResult r = WriteSome();
    if (r == SendResult::kDone) {
      handler_->OnSendDone();
    } else if (r == SendResult::kError) {
      handler_->OnError(send_err_);
      return;
    }
    if (closed_) return;
  }
  if (events & (EPOLLRDHUP | EPOLLHUP)) peer_hup_ = true;
  if (events & (EPOLLIN | EPOLLRDHUP | EPOLLHUP)) {
    readable_ = true;
    ReadLoop();
  }
}

void EpollSocket::ReadLoop() {
  while (readable_ && !closed_ && handler_->WantsRecv()) {
    std::span<char> buf = handler_->RecvBuffer();
    ++ThreadSyscalls().recv;
    ssize_t n = ::recv(fd_, buf.data(), buf.size(), 0);
    if (n > 0) {
      // A short read means the socket is drained (epoll(7): for stream sockets, reading fewer
      // bytes than asked for shows the read space is exhausted), so skip the extra recv that
      // would only return EAGAIN. New data will raise a new edge. If the peer hung up we keep
      // reading until recv returns 0, since no further edge will tell us about the EOF.
      if (static_cast<size_t>(n) < buf.size() && !peer_hup_) readable_ = false;
      handler_->OnRecv(static_cast<size_t>(n));
      continue;
    }
    if (n == 0) {
      readable_ = false;
      handler_->OnEof();
      return;
    }
    if (errno == EINTR) continue;
    if (errno == EAGAIN || errno == EWOULDBLOCK) {
      readable_ = false;
      return;
    }
    handler_->OnError(errno);
    return;
  }
}

StreamSocket::SendResult EpollSocket::Send(std::span<const iovec> iov) {
  KV_DCHECK(!send_pending_);
  iov_.assign(iov.begin(), iov.end());
  iov_idx_ = 0;
  return WriteSome();
}

StreamSocket::SendResult EpollSocket::WriteSome() {
  while (iov_idx_ < iov_.size()) {
    int count = static_cast<int>(std::min<size_t>(iov_.size() - iov_idx_, IOV_MAX));
    ++ThreadSyscalls().writev;
    ssize_t n = ::writev(fd_, &iov_[iov_idx_], count);
    if (n < 0) {
      if (errno == EINTR) continue;
      if (errno == EAGAIN || errno == EWOULDBLOCK) {
        // Partial write: EPOLLOUT fires once there is room again (registered once, DC-12).
        send_pending_ = true;
        return SendResult::kPending;
      }
      send_err_ = errno;
      send_pending_ = false;
      return SendResult::kError;
    }
    size_t left = static_cast<size_t>(n);
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
  }
  send_pending_ = false;
  iov_.clear();
  return SendResult::kDone;
}

void EpollSocket::ResumeRecv() {
  if (closed_ || !readable_ || resume_queued) return;
  resume_queued = true;
  reactor_->QueueResume(this);
}

void EpollSocket::Close() {
  if (closed_) return;
  closed_ = true;
  reactor_->Unregister(fd_);
  CloseFd(fd_);
  reactor_->QueueClosed(this);
}

}  // namespace

std::unique_ptr<Reactor> CreateEpollReactor() { return std::make_unique<EpollReactor>(); }

}  // namespace kv::net
