#include "net/socket_util.h"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>

#include "net/reactor.h"

namespace kv::net {

SyscallCounters& ThreadSyscalls() {
  static thread_local SyscallCounters counters;
  return counters;
}

void Fd::reset(int fd) {
  if (fd_ >= 0) ::close(fd_);
  fd_ = fd;
}

Fd CreateListener(const std::string& bind_addr, uint16_t port, int backlog, std::string* err) {
  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_port = htons(port);
  if (inet_pton(AF_INET, bind_addr.c_str(), &addr.sin_addr) != 1) {
    *err = "invalid bind address: " + bind_addr;
    return Fd();
  }

  Fd fd(::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0));
  if (!fd) {
    *err = std::string("socket: ") + strerror(errno);
    return Fd();
  }
  int one = 1;
  if (setsockopt(fd.get(), SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one)) != 0 ||
      setsockopt(fd.get(), SOL_SOCKET, SO_REUSEPORT, &one, sizeof(one)) != 0) {
    *err = std::string("setsockopt: ") + strerror(errno);
    return Fd();
  }
  if (::bind(fd.get(), reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
    *err = "bind " + bind_addr + ":" + std::to_string(port) + ": " + strerror(errno);
    return Fd();
  }
  if (::listen(fd.get(), backlog) != 0) {
    *err = std::string("listen: ") + strerror(errno);
    return Fd();
  }
  return fd;
}

uint16_t LocalPort(int fd) {
  sockaddr_in addr{};
  socklen_t len = sizeof(addr);
  if (getsockname(fd, reinterpret_cast<sockaddr*>(&addr), &len) != 0) return 0;
  return ntohs(addr.sin_port);
}

void SetNoDelay(int fd) {
  int one = 1;
  ++ThreadSyscalls().setsockopt;
  setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
}

void SetSendBuffer(int fd, int bytes) {
  ++ThreadSyscalls().setsockopt;
  setsockopt(fd, SOL_SOCKET, SO_SNDBUF, &bytes, sizeof(bytes));
}

void CloseFd(int fd) {
  ++ThreadSyscalls().close;
  ::close(fd);
}

}  // namespace kv::net
