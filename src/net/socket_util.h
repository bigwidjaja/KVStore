#pragma once

#include <cstdint>
#include <string>

namespace kv::net {

// Owns a file descriptor and closes it on destruction, so error paths can't leak it.
class Fd {
 public:
  Fd() = default;
  explicit Fd(int fd) : fd_(fd) {}
  ~Fd() { reset(); }

  Fd(Fd&& other) noexcept : fd_(other.release()) {}
  Fd& operator=(Fd&& other) noexcept {
    if (this != &other) reset(other.release());
    return *this;
  }
  Fd(const Fd&) = delete;
  Fd& operator=(const Fd&) = delete;

  int get() const { return fd_; }
  explicit operator bool() const { return fd_ >= 0; }

  int release() {
    int fd = fd_;
    fd_ = -1;
    return fd;
  }
  void reset(int fd = -1);

 private:
  int fd_ = -1;
};

// A nonblocking TCP listening socket with SO_REUSEADDR and SO_REUSEPORT, so every worker
// thread can bind its own socket to the same port (DC-02). Port 0 picks a free port.
// Returns an empty Fd and sets *err on failure.
Fd CreateListener(const std::string& bind_addr, uint16_t port, int backlog, std::string* err);

uint16_t LocalPort(int fd);

// Counted in ThreadSyscalls(). Failures are ignored; these are best effort.
void SetNoDelay(int fd);
void SetSendBuffer(int fd, int bytes);
void CloseFd(int fd);

}  // namespace kv::net
