#include "server/config.h"

#include <functional>
#include <string_view>
#include <vector>

#include "base/strings.h"

namespace kv {
namespace {

struct Flag {
  const char* name;
  const char* help;
  bool is_bool;
  std::function<bool(std::string_view)> set;
};

template <typename T>
std::function<bool(std::string_view)> IntSetter(T* field, int64_t min, int64_t max) {
  return [field, min, max](std::string_view v) {
    int64_t n;
    if (!ParseInt64(v, &n) || n < min || n > max) return false;
    *field = static_cast<T>(n);
    return true;
  };
}

std::function<bool(std::string_view)> BoolSetter(bool* field) {
  return [field](std::string_view v) {
    if (v == "true" || v == "1" || v == "yes") {
      *field = true;
    } else if (v == "false" || v == "0" || v == "no") {
      *field = false;
    } else {
      return false;
    }
    return true;
  };
}

std::function<bool(std::string_view)> ChoiceSetter(std::string* field,
                                                   std::vector<std::string> choices) {
  return [field, choices](std::string_view v) {
    for (const auto& c : choices) {
      if (v == c) {
        *field = c;
        return true;
      }
    }
    return false;
  };
}

std::vector<Flag> MakeFlags(Config* c) {
  return {
      {"bind", "IPv4 address to listen on", false,
       [c](std::string_view v) {
         c->bind = std::string(v);
         return true;
       }},
      {"port", "TCP port; 0 picks a free one", false, IntSetter(&c->port, 0, 65535)},
      {"threads", "worker threads, one shard each; 0 = one per CPU", false,
       IntSetter(&c->threads, 0, 1024)},
      {"pin", "pin each worker thread to its own CPU", true, BoolSetter(&c->pin)},
      {"backend", "I/O backend: uring or epoll", false, ChoiceSetter(&c->backend, {"uring", "epoll"})},
      {"uring_multishot", "io_uring: multishot recv with a provided buffer ring", true,
       BoolSetter(&c->uring_multishot)},
      {"uring_wake", "io_uring: how to wake other threads, msg_ring or eventfd", false,
       ChoiceSetter(&c->uring_wake, {"msg_ring", "eventfd"})},
      {"uring_bufs", "io_uring: provided buffers per thread (power of two)", false,
       IntSetter(&c->uring_bufs, 2, 32768)},
      {"uring_buf_size", "io_uring: size of each provided buffer", false,
       IntSetter(&c->uring_buf_size, 512, 1 << 20)},
      {"wake_always", "wake the target thread on every message (DC-06 experiment)", true,
       BoolSetter(&c->wake_always)},
      {"queue_len", "per-thread inbox capacity (power of two)", false,
       IntSetter(&c->queue_len, 2, 1 << 20)},
      {"max_pipeline", "unsent replies per connection before reading pauses", false,
       IntSetter(&c->max_pipeline, 1, 100000000)},
      {"max_unsent_bytes", "unsent reply bytes per connection before reading pauses", false,
       IntSetter(&c->max_unsent_bytes, 1024, int64_t{1} << 40)},
      {"max_cmds_per_turn", "commands handled per connection per loop turn", false,
       IntSetter(&c->max_cmds_per_turn, 1, 1000000)},
      {"max_inflight", "commands per connection waiting on other shards before reading pauses",
       false, IntSetter(&c->max_inflight, 1, 1000000)},
      {"max_bulk_len", "largest accepted bulk string", false,
       IntSetter(&c->max_bulk_len, 1, int64_t{4} << 30)},
      {"sndbuf", "SO_SNDBUF for client sockets; 0 = kernel default", false,
       IntSetter(&c->sndbuf, 0, 1 << 30)},
      {"hz", "heartbeat frequency for active expiry", false, IntSetter(&c->hz, 1, 1000)},
      {"log_level", "debug, info, warning or error", false,
       ChoiceSetter(&c->log_level, {"debug", "info", "warning", "error"})},
  };
}

}  // namespace

bool ParseFlags(int argc, char** argv, Config* config, std::string* err) {
  std::vector<Flag> flags = MakeFlags(config);
  for (int i = 1; i < argc; ++i) {
    std::string_view arg = argv[i];
    if (arg.substr(0, 2) != "--") {
      *err = "unexpected argument: " + std::string(arg);
      return false;
    }
    arg.remove_prefix(2);
    std::string_view name = arg, value;
    bool has_value = false;
    if (size_t eq = arg.find('='); eq != std::string_view::npos) {
      name = arg.substr(0, eq);
      value = arg.substr(eq + 1);
      has_value = true;
    }
    const Flag* flag = nullptr;
    for (const Flag& f : flags) {
      if (name == f.name) flag = &f;
    }
    if (flag == nullptr) {
      *err = "unknown flag: --" + std::string(name);
      return false;
    }
    if (!has_value) {
      if (flag->is_bool) {
        value = "true";
      } else if (i + 1 < argc) {
        value = argv[++i];
      } else {
        *err = "missing value for --" + std::string(name);
        return false;
      }
    }
    if (!flag->set(value)) {
      *err = "bad value for --" + std::string(name) + ": " + std::string(value);
      return false;
    }
  }
  if (config->queue_len & (config->queue_len - 1)) {
    *err = "--queue_len must be a power of two";
    return false;
  }
  if (config->uring_bufs & (config->uring_bufs - 1)) {
    *err = "--uring_bufs must be a power of two";
    return false;
  }
  return true;
}

std::string FlagsUsage() {
  Config defaults;
  std::string out = "Usage: kv_server [flags]\n";
  for (const Flag& f : MakeFlags(&defaults)) {
    out += "  --";
    out += f.name;
    out += std::string(20 - std::min<size_t>(19, std::string_view(f.name).size()), ' ');
    out += f.help;
    out += '\n';
  }
  return out;
}

}  // namespace kv
