#pragma once

#include <sys/uio.h>

#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <span>
#include <string>

namespace kv::net {

// System calls made by the calling thread, by kind. Each reactor thread counts its own (no
// shared atomics), and INFO adds them up so syscalls per request can be compared between
// backends (Phase 5). For io_uring, socket I/O done by the kernel on our behalf is not a
// syscall; only io_uring_enter is.
#define KV_SYSCALL_KINDS(X) \
  X(epoll_wait)             \
  X(epoll_ctl)              \
  X(io_uring_enter)         \
  X(recv)                   \
  X(writev)                 \
  X(accept)                 \
  X(eventfd_read)           \
  X(eventfd_write)          \
  X(timerfd_read)           \
  X(setsockopt)             \
  X(shutdown)               \
  X(close)

struct SyscallCounters {
#define KV_DECLARE_COUNTER(name) uint64_t name = 0;
  KV_SYSCALL_KINDS(KV_DECLARE_COUNTER)
#undef KV_DECLARE_COUNTER
};

SyscallCounters& ThreadSyscalls();

// Receives events for one connected stream socket. All calls happen on the reactor thread.
class StreamHandler {
 public:
  virtual ~StreamHandler() = default;

  // Where the next received bytes should go. Must return a non-empty span. With a basic
  // io_uring recv the kernel writes here asynchronously, so the memory must stay put until
  // the matching OnRecv().
  virtual std::span<char> RecvBuffer() = 0;
  // `n` > 0 bytes were written at the start of the last RecvBuffer().
  virtual void OnRecv(size_t n) = 0;
  // False pauses receiving (fairness or backpressure); call StreamSocket::ResumeRecv() once it
  // becomes true again. A few more OnRecv() calls can still arrive after it turns false.
  virtual bool WantsRecv() const = 0;
  // The peer closed its side. No more OnRecv() calls follow.
  virtual void OnEof() = 0;
  // The last Send() that returned kPending has finished.
  virtual void OnSendDone() = 0;
  // The socket failed. The handler should Close() it.
  virtual void OnError(int err) = 0;
  // After Close(): every in-flight operation is done and the fd is closed. No callback follows,
  // and the socket object is deleted right after this returns.
  virtual void OnClosed() = 0;
};

class StreamSocket {
 public:
  enum class SendResult { kDone, kPending, kError };

  virtual ~StreamSocket() = default;

  // Sends every byte described by `iov`; partial writes are resumed internally. kDone means
  // it all went out synchronously. kPending means OnSendDone() follows; until then the memory
  // `iov` points at must stay unchanged. Only one send may be outstanding.
  virtual SendResult Send(std::span<const iovec> iov) = 0;

  // Re-arms receiving after WantsRecv() went from false to true. Cheap and idempotent.
  virtual void ResumeRecv() = 0;

  // Starts closing. OnClosed() follows from the reactor loop, never from inside this call.
  virtual void Close() = 0;

  virtual int fd() const = 0;
};

// Hooks the reactor calls around polling. Implemented by the worker that owns the reactor.
class LoopDelegate {
 public:
  virtual ~LoopDelegate() = default;
  // Once per loop turn, before polling. Returns true if work remains, in which case the
  // reactor polls without blocking.
  virtual bool OnLoopTurn() = 0;
  // Called before a blocking poll. Returns false to skip blocking because work just arrived.
  virtual bool BeforeSleep() = 0;
  // Called after a blocking poll returns.
  virtual void AfterWake() = 0;
};

// One event loop, owned by one thread.
//
// Design after Dragonfly (DC-15): helio's ProactorBase with epoll and io_uring implementations
// chosen at startup. Everything above this interface is shared by both backends, so nothing
// outside src/net may call epoll or io_uring directly.
class Reactor {
 public:
  using AcceptCallback = std::function<void(int fd)>;

  virtual ~Reactor() = default;

  virtual const char* Name() const = 0;

  // Runs until Stop() is called from this thread.
  virtual void Run(LoopDelegate* delegate) = 0;
  virtual void Stop() = 0;

  // Accepts connections on `listen_fd` (which the reactor now owns) and hands each new fd to
  // `on_accept`.
  virtual void Listen(int listen_fd, AcceptCallback on_accept) = 0;

  // Registers a connected socket. The reactor owns the returned object until OnClosed().
  virtual StreamSocket* AddStream(int fd, StreamHandler* handler) = 0;

  virtual void AddTimer(std::chrono::milliseconds period, std::function<void()> callback) = 0;

  // Wakes `target`, another thread's reactor that may be blocked in its poll. Called on this
  // reactor's thread, so a backend can use its own resources (io_uring's MSG_RING) to do it.
  virtual void WakeRemote(Reactor& target) = 0;

  // Wakes this reactor from any thread, e.g. for shutdown.
  virtual void WakeFromAnyThread() = 0;
};

enum class Backend { kEpoll, kUring };

struct ReactorOptions {
  Backend backend = Backend::kEpoll;
  // io_uring only.
  bool multishot_recv = true;  // DC-14: multishot recv over a provided buffer ring.
  bool msg_ring_wake = true;   // Wake other rings with IORING_OP_MSG_RING instead of eventfd.
  unsigned ring_entries = 1024;
  unsigned buf_count = 512;
  unsigned buf_size = 4096;
};

std::unique_ptr<Reactor> CreateReactor(const ReactorOptions& options);

// What this kernel's io_uring supports, checked once at startup.
struct UringSupport {
  bool usable = false;  // Ring setup with our flags works and the basic opcodes exist.
  bool msg_ring = false;
  bool buf_ring = false;
  std::string why;  // When !usable.
};

UringSupport ProbeUring();

}  // namespace kv::net
