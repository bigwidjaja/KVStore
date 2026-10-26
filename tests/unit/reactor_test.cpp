// Runs every reactor configuration through the same checks: timers, cross-thread wakeups, and
// a stream echo that forces partial writes. io_uring cases skip themselves when the kernel (or
// a seccomp profile) blocks io_uring.

#include "net/reactor.h"

#include <fcntl.h>
#include <gtest/gtest.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <cstring>
#include <string>
#include <thread>
#include <vector>

#include "base/time.h"

namespace kv::net {
namespace {

struct Variant {
  const char* name;
  ReactorOptions options;
};

std::vector<Variant> Variants() {
  ReactorOptions epoll;
  epoll.backend = Backend::kEpoll;
  ReactorOptions basic;
  basic.backend = Backend::kUring;
  basic.multishot_recv = false;
  basic.msg_ring_wake = false;
  ReactorOptions multishot = basic;
  multishot.multishot_recv = true;
  multishot.msg_ring_wake = true;
  multishot.buf_count = 16;  // Few buffers, so the ring runs dry and multishot gets re-armed.
  return {{"epoll", epoll}, {"uring_basic", basic}, {"uring_multishot", multishot}};
}

void PrintTo(const Variant& v, std::ostream* os) { *os << v.name; }

class ReactorTest : public ::testing::TestWithParam<Variant> {
 protected:
  void SetUp() override {
    if (GetParam().options.backend == Backend::kUring) {
      UringSupport s = ProbeUring();
      if (!s.usable) GTEST_SKIP() << "io_uring unavailable: " << s.why;
      if (GetParam().options.multishot_recv && !s.buf_ring) GTEST_SKIP() << "no buffer rings";
      if (GetParam().options.msg_ring_wake && !s.msg_ring) GTEST_SKIP() << "no MSG_RING";
    }
  }
};

// Sleeps whenever it can, and stops the loop once `stop` is set.
class SleepyDelegate : public LoopDelegate {
 public:
  explicit SleepyDelegate(Reactor* r) : reactor_(r) {}
  bool OnLoopTurn() override {
    ++turns;
    if (stop.load(std::memory_order_acquire)) reactor_->Stop();
    return false;
  }
  bool BeforeSleep() override { return !stop.load(std::memory_order_acquire); }
  void AfterWake() override { ++wakes; }

  std::atomic<bool> stop{false};
  std::atomic<int> turns{0};
  std::atomic<int> wakes{0};

 private:
  Reactor* reactor_;
};

TEST_P(ReactorTest, TimerFires) {
  auto reactor = CreateReactor(GetParam().options);
  SleepyDelegate d(reactor.get());
  int ticks = 0;
  reactor->AddTimer(std::chrono::milliseconds(5), [&] {
    if (++ticks == 10) d.stop = true;
  });
  uint64_t start = MonotonicNs();
  reactor->Run(&d);
  double ms = double(MonotonicNs() - start) / 1e6;
  EXPECT_EQ(ticks, 10);
  EXPECT_GE(ms, 40);
  EXPECT_LT(ms, 2000);
}

TEST_P(ReactorTest, WakeFromAnyThread) {
  auto reactor = CreateReactor(GetParam().options);
  SleepyDelegate d(reactor.get());
  std::thread waker([&] {
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    d.stop = true;
    reactor->WakeFromAnyThread();
  });
  uint64_t start = MonotonicNs();
  reactor->Run(&d);  // No timers: only the wakeup can end this.
  waker.join();
  EXPECT_LT(double(MonotonicNs() - start) / 1e6, 2000);
  EXPECT_GE(d.wakes.load(), 1);
}

TEST_P(ReactorTest, WakeRemote) {
  // Two reactors on two threads; A wakes B from inside A's loop (MSG_RING for the multishot
  // variant, eventfd otherwise).
  std::unique_ptr<Reactor> a, b;
  std::atomic<int> ready{0};
  SleepyDelegate* db_ptr = nullptr;
  std::atomic<bool> b_done{false};

  std::thread tb([&] {
    b = CreateReactor(GetParam().options);
    SleepyDelegate db(b.get());
    db_ptr = &db;
    ready.fetch_add(1);
    b->Run(&db);
    b_done = true;
    while (ready.load() != 3) std::this_thread::yield();  // Keep B alive until A is done.
    b.reset();  // A ring is torn down by the thread that created it.
  });
  while (ready.load() != 1) std::this_thread::yield();

  a = CreateReactor(GetParam().options);
  SleepyDelegate da(a.get());
  int ticks = 0;
  a->AddTimer(std::chrono::milliseconds(20), [&] {
    ++ticks;
    if (ticks == 1) {
      db_ptr->stop = true;
      a->WakeRemote(*b);
    }
    if (b_done.load() || ticks > 100) da.stop = true;
  });
  a->Run(&da);
  EXPECT_TRUE(b_done.load());
  EXPECT_LT(ticks, 10);
  ready = 3;
  tb.join();
}

// Echoes everything it receives, with bounded buffering.
class EchoHandler : public StreamHandler {
 public:
  explicit EchoHandler(Reactor* r) : reactor_(r), buf_(16384) {}

  std::span<char> RecvBuffer() override { return {buf_.data(), buf_.size()}; }
  void OnRecv(size_t n) override {
    received += n;
    pending_.append(buf_.data(), n);
    MaybeSend();
  }
  bool WantsRecv() const override { return pending_.size() < (1 << 20); }
  void OnEof() override {
    eof_ = true;
    MaybeClose();
  }
  void OnSendDone() override {
    sending_ = false;
    inflight_.clear();
    MaybeSend();
    if (WantsRecv()) socket->ResumeRecv();
    MaybeClose();
  }
  void OnError(int err) override {
    error = err;
    socket->Close();
  }
  void OnClosed() override {
    closed = true;
    reactor_->Stop();
  }

  StreamSocket* socket = nullptr;
  size_t received = 0;
  int error = 0;
  bool closed = false;
  int partial_sends = 0;

 private:
  void MaybeSend() {
    while (!sending_ && !pending_.empty()) {
      inflight_.swap(pending_);
      pending_.clear();
      iovec iov{inflight_.data(), inflight_.size()};
      switch (socket->Send(std::span<const iovec>(&iov, 1))) {
        case StreamSocket::SendResult::kDone:
          inflight_.clear();
          break;
        case StreamSocket::SendResult::kPending:
          sending_ = true;
          ++partial_sends;
          return;
        case StreamSocket::SendResult::kError:
          socket->Close();
          return;
      }
    }
  }
  void MaybeClose() {
    if (eof_ && pending_.empty() && !sending_) socket->Close();
  }

  Reactor* reactor_;
  std::vector<char> buf_;
  std::string pending_, inflight_;
  bool sending_ = false;
  bool eof_ = false;
};

TEST_P(ReactorTest, StreamEchoWithPartialWrites) {
  int fds[2];
  ASSERT_EQ(socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, fds), 0);
  int small = 4096;
  setsockopt(fds[0], SOL_SOCKET, SO_SNDBUF, &small, sizeof(small));
  if (GetParam().options.backend == Backend::kEpoll) {
    // The epoll backend expects nonblocking sockets, as accept4 gives it.
    int flags = fcntl(fds[0], F_GETFL);
    fcntl(fds[0], F_SETFL, flags | O_NONBLOCK);
  }

  std::string data(4 << 20, '\0');
  for (size_t i = 0; i < data.size(); ++i) data[i] = static_cast<char>(i * 131 + (i >> 12));

  auto reactor = CreateReactor(GetParam().options);
  SleepyDelegate d(reactor.get());
  EchoHandler echo(reactor.get());
  echo.socket = reactor->AddStream(fds[0], &echo);
  echo.socket->ResumeRecv();

  std::string back;
  std::thread client([&] {
    std::thread writer([&] {
      size_t off = 0;
      while (off < data.size()) {
        ssize_t n = write(fds[1], data.data() + off, std::min<size_t>(65536, data.size() - off));
        ASSERT_GT(n, 0);
        off += static_cast<size_t>(n);
      }
      shutdown(fds[1], SHUT_WR);
    });
    char buf[8192];
    while (true) {
      ssize_t n = read(fds[1], buf, sizeof(buf));
      if (n <= 0) break;
      back.append(buf, static_cast<size_t>(n));
      if (back.size() % (1 << 18) < sizeof(buf)) std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    writer.join();
  });
  reactor->Run(&d);
  client.join();
  close(fds[1]);

  EXPECT_EQ(echo.error, 0);
  EXPECT_TRUE(echo.closed);
  EXPECT_EQ(echo.received, data.size());
  EXPECT_GT(echo.partial_sends, 0);
  ASSERT_EQ(back.size(), data.size());
  EXPECT_TRUE(back == data);
}

INSTANTIATE_TEST_SUITE_P(Backends, ReactorTest, ::testing::ValuesIn(Variants()),
                         [](const auto& info) { return std::string(info.param.name); });

}  // namespace
}  // namespace kv::net
