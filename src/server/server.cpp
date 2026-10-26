#include "server/server.h"

#include <sched.h>
#include <sys/socket.h>

#include "base/log.h"
#include "base/time.h"
#include "net/socket_util.h"
#include "server/worker.h"

namespace kv {

Server::Server(Config config) : config_(std::move(config)) {}

Server::~Server() { Stop(); }

const char* Server::backend_name() const {
  return reactor_opts_.backend == net::Backend::kUring ? "uring" : "epoll";
}

bool Server::Start(std::string* err) {
  LogLevel level;
  if (ParseLogLevel(config_.log_level.c_str(), &level)) SetLogLevel(level);

  cpu_set_t set;
  CPU_ZERO(&set);
  if (sched_getaffinity(0, sizeof(set), &set) == 0) {
    for (int cpu = 0; cpu < CPU_SETSIZE; ++cpu) {
      if (CPU_ISSET(cpu, &set)) cpus_.push_back(cpu);
    }
  }
  if (cpus_.empty()) cpus_.push_back(0);
  num_threads_ = config_.threads != 0 ? config_.threads : static_cast<uint32_t>(cpus_.size());

  // DC-15: io_uring by default, epoll when the kernel can't run our ring setup.
  reactor_opts_.backend = net::Backend::kEpoll;
  if (config_.backend == "uring") {
    net::UringSupport support = net::ProbeUring();
    if (!support.usable) {
      KV_LOG_WARN("io_uring unavailable, falling back to epoll: %s", support.why.c_str());
    } else {
      reactor_opts_.backend = net::Backend::kUring;
      reactor_opts_.multishot_recv = config_.uring_multishot && support.buf_ring;
      if (config_.uring_multishot && !support.buf_ring) {
        KV_LOG_WARN("provided buffer rings unavailable; using one recv per read");
      }
      reactor_opts_.msg_ring_wake = config_.uring_wake == "msg_ring" && support.msg_ring;
      if (config_.uring_wake == "msg_ring" && !support.msg_ring) {
        KV_LOG_WARN("IORING_OP_MSG_RING unavailable; waking threads with eventfd");
      }
      reactor_opts_.buf_count = config_.uring_bufs;
      reactor_opts_.buf_size = config_.uring_buf_size;
    }
  }

  // DC-02 (adapted): every worker gets its own SO_REUSEPORT listener and the kernel spreads
  // connections across them. The first binds the requested port (0 picks one); the rest
  // reuse whatever port it got.
  std::vector<net::Fd> listeners;
  for (uint32_t i = 0; i < num_threads_; ++i) {
    net::Fd fd = net::CreateListener(config_.bind, i == 0 ? config_.port : port_, SOMAXCONN, err);
    if (!fd) return false;
    if (i == 0) port_ = net::LocalPort(fd.get());
    listeners.push_back(std::move(fd));
  }

  start_ms_ = UnixTimeMs();
  reactors_ready_ = std::make_unique<std::latch>(num_threads_);
  started_ = std::make_unique<std::latch>(num_threads_);
  stopped_ = std::make_unique<std::latch>(num_threads_ + 1);
  for (uint32_t i = 0; i < num_threads_; ++i) {
    workers_.push_back(std::make_unique<Worker>(this, i, std::move(listeners[i])));
  }
  StartupLatches latches{reactors_ready_.get(), started_.get(), stopped_.get()};
  for (auto& w : workers_) w->Start(latches);
  started_->wait();
  running_ = true;

  KV_LOG_INFO("Ready to accept connections on port %u (threads=%u backend=%s%s%s pin=%s)", port_,
              num_threads_, backend_name(),
              reactor_opts_.backend == net::Backend::kUring
                  ? (reactor_opts_.multishot_recv ? " recv=multishot" : " recv=basic")
                  : "",
              reactor_opts_.backend == net::Backend::kUring
                  ? (reactor_opts_.msg_ring_wake ? " wake=msg_ring" : " wake=eventfd")
                  : "",
              config_.pin ? "on" : "off");
  return true;
}

void Server::Stop() {
  if (!running_) return;
  running_ = false;
  for (auto& w : workers_) w->RequestStop();
  // Our count on `stopped_` lets workers tear down their reactors only after every
  // RequestStop() above has finished waking them.
  stopped_->count_down();
  for (auto& w : workers_) w->Join();
  workers_.clear();
}

}  // namespace kv
