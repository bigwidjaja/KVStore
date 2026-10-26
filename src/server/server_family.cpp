// Connection and server commands: PING, ECHO, QUIT, SELECT, COMMAND, CONFIG, CLIENT, TIME, INFO.
// Everything except INFO runs on the connection's own thread.

#include "server/server_family.h"

#include <cstring>

#include "base/strings.h"
#include "base/time.h"
#include "server/command_table.h"
#include "server/connection.h"
#include "server/server.h"
#include "server/worker.h"

namespace kv {
namespace {

void Ping(CoordContext& c) {
  if (c.args.size() > 2) return c.out.Error(WrongArgsError("ping"));
  if (c.args.size() == 2) return c.out.Bulk(c.args[1]);
  c.out.SimpleString("PONG");
}

void Echo(CoordContext& c) { c.out.Bulk(c.args[1]); }

void Quit(CoordContext& c) {
  c.out.Ok();
  c.conn.CloseAfterReply();
}

void Select(CoordContext& c) {
  int64_t db;
  if (!ParseInt64(c.args[1], &db)) return c.out.Error(kNotIntegerErr);
  if (db != 0) return c.out.Error("ERR DB index is out of range");
  c.out.Ok();
}

// A minimal COMMAND so redis-cli starts cleanly. It does not describe each command.
void Command(CoordContext& c) {
  const CommandTable& table = c.worker.commands();
  if (c.args.size() == 1) return c.out.ArrayHeader(0);
  std::string_view sub = c.args[1];
  if (EqualsIgnoreCase(sub, "count")) return c.out.Integer(static_cast<int64_t>(table.size()));
  if (EqualsIgnoreCase(sub, "list")) {
    auto all = table.All();
    c.out.ArrayHeader(all.size());
    for (const CommandSpec* spec : all) c.out.Bulk(spec->name);
    return;
  }
  if (EqualsIgnoreCase(sub, "docs") || EqualsIgnoreCase(sub, "info")) return c.out.ArrayHeader(0);
  c.out.Error(UnknownSubcommandError(sub, "command"));
}

// CONFIG GET reports a few read-only values that clients probe on connect (redis-benchmark
// asks for "save" and "appendonly"). CONFIG SET is not supported.
void ConfigCmd(CoordContext& c) {
  std::string_view sub = c.args[1];
  if (EqualsIgnoreCase(sub, "get")) {
    if (c.args.size() < 3) return c.out.Error(WrongArgsError("config|get"));
    const ::kv::Config& cfg = c.worker.server().config();
    const std::pair<std::string, std::string> params[] = {
        {"save", ""},
        {"appendonly", "no"},
        {"databases", "1"},
        {"maxmemory", "0"},
        {"bind", cfg.bind},
        {"port", std::to_string(c.worker.server().port())},
        {"io-threads", std::to_string(c.worker.num_shards())},
    };
    std::vector<const std::pair<std::string, std::string>*> matches;
    for (const auto& p : params) {
      for (size_t i = 2; i < c.args.size(); ++i) {
        if (GlobMatch(c.args[i], p.first, true)) {
          matches.push_back(&p);
          break;
        }
      }
    }
    c.out.ArrayHeader(matches.size() * 2);
    for (const auto* p : matches) {
      c.out.Bulk(p->first);
      c.out.Bulk(p->second);
    }
    return;
  }
  if (EqualsIgnoreCase(sub, "resetstat")) return c.out.Ok();
  if (EqualsIgnoreCase(sub, "set")) return c.out.Error("ERR CONFIG SET is not supported");
  c.out.Error(UnknownSubcommandError(sub, "config"));
}

void Client(CoordContext& c) {
  std::string_view sub = c.args[1];
  if (EqualsIgnoreCase(sub, "setname")) {
    if (c.args.size() != 3) return c.out.Error(WrongArgsError("client|setname"));
    for (char ch : c.args[2]) {
      if (ch < '!' || ch > '~') {
        return c.out.Error("ERR Client names cannot contain spaces, newlines or special characters.");
      }
    }
    c.conn.set_name(std::string(c.args[2]));
    return c.out.Ok();
  }
  if (EqualsIgnoreCase(sub, "getname")) {
    if (c.conn.name().empty()) return c.out.NullBulk();
    return c.out.Bulk(c.conn.name());
  }
  if (EqualsIgnoreCase(sub, "id")) return c.out.Integer(static_cast<int64_t>(c.conn.client_id()));
  if (EqualsIgnoreCase(sub, "setinfo")) {
    if (c.args.size() != 4) return c.out.Error(WrongArgsError("client|setinfo"));
    return c.out.Ok();
  }
  c.out.Error(UnknownSubcommandError(sub, "client"));
}

void Time(CoordContext& c) {
  int64_t us = UnixTimeUs();
  c.out.ArrayHeader(2);
  c.out.Bulk(std::to_string(us / 1000000));
  c.out.Bulk(std::to_string(us % 1000000));
}

// INFO runs on every shard; each thread returns its counters as raw bytes and the coordinator
// sums them (Combine::kInfo).
void InfoPart(OpContext& c) {
  InfoCounters counters{};
  Worker::Current()->CollectInfo(&counters);
  c.out.Raw(std::string_view(reinterpret_cast<const char*>(counters.data()), sizeof(counters)));
}

const char* const kThreadStatNames[] = {
#define KV_NAME(name) #name,
    KV_THREAD_STATS(KV_NAME)
#undef KV_NAME
};

const char* const kSyscallNames[] = {
#define KV_NAME(name) #name,
    KV_SYSCALL_KINDS(KV_NAME)
#undef KV_NAME
};

}  // namespace

std::string FormatInfo(const InfoCounters& v, std::string_view section, Worker& worker) {
  const Server& server = worker.server();
  bool all = section.empty() || EqualsIgnoreCase(section, "all") ||
             EqualsIgnoreCase(section, "default") || EqualsIgnoreCase(section, "everything");
  auto want = [&](std::string_view name) { return all || EqualsIgnoreCase(section, name); };

  std::string out;
  auto line = [&](std::string_view key, const std::string& value) {
    out.append(key);
    out.push_back(':');
    out.append(value);
    out.append("\r\n");
  };

  if (want("server")) {
    out += "# Server\r\n";
    line("redis_version", "7.2.0");
    line("kv_version", "0.1.0");
    line("redis_mode", "standalone");
    line("io_backend", server.backend_name());
    line("num_threads", std::to_string(server.num_threads()));
    line("tcp_port", std::to_string(server.port()));
    line("uptime_in_seconds", std::to_string((UnixTimeMs() - server.start_time_ms()) / 1000));
    out += "\r\n";
  }
  if (want("clients")) {
    out += "# Clients\r\n";
    line("connected_clients", std::to_string(v[kInfoThread_connected_clients]));
    out += "\r\n";
  }
  if (want("stats")) {
    out += "# Stats\r\n";
    line("total_connections_received", std::to_string(v[kInfoThread_connections_received]));
    line("total_commands_processed", std::to_string(v[kInfoThread_commands_processed]));
    line("expired_keys", std::to_string(v[kInfoShard_expired_keys]));
    line("keyspace_hits", std::to_string(v[kInfoShard_keyspace_hits]));
    line("keyspace_misses", std::to_string(v[kInfoShard_keyspace_misses]));
    line("table_segments", std::to_string(v[kInfoShard_table_segments]));
    line("table_splits", std::to_string(v[kInfoShard_table_splits]));
    line("table_bytes", std::to_string(v[kInfoShard_table_bytes]));
    for (unsigned i = 0; i < std::size(kThreadStatNames); ++i) {
      if (i == kInfoThread_connected_clients || i == kInfoThread_connections_received ||
          i == kInfoThread_commands_processed) {
        continue;
      }
      line(kThreadStatNames[i], std::to_string(v[i]));
    }
    out += "\r\n";
  }
  if (want("syscalls")) {
    // Calls made by our code, counted per thread. See docs/DESIGN.md for how they compare with
    // strace.
    out += "# Syscalls\r\n";
    uint64_t total = 0;
    for (unsigned i = 0; i < std::size(kSyscallNames); ++i) {
      uint64_t n = v[kInfoSyscall_epoll_wait + i];
      total += n;
      line(std::string("syscall_") + kSyscallNames[i], std::to_string(n));
    }
    line("syscall_total", std::to_string(total));
    out += "\r\n";
  }
  if (want("keyspace")) {
    out += "# Keyspace\r\n";
    if (v[kInfoShard_keys] > 0) {
      line("db0", "keys=" + std::to_string(v[kInfoShard_keys]) +
                      ",expires=" + std::to_string(v[kInfoShard_expires]) + ",avg_ttl=0");
    }
    out += "\r\n";
  }
  if (!out.empty()) out.resize(out.size() - 2);  // Drop the last section's blank line.
  return out;
}

void RegisterServerFamily(CommandTable* t) {
  t->Add({.name = "ping", .arity = -1, .route = Route::kCoordinator, .coord_fn = Ping});
  t->Add({.name = "echo", .arity = 2, .route = Route::kCoordinator, .coord_fn = Echo});
  t->Add({.name = "quit", .arity = -1, .route = Route::kCoordinator, .coord_fn = Quit});
  t->Add({.name = "select", .arity = 2, .route = Route::kCoordinator, .coord_fn = Select});
  t->Add({.name = "command", .arity = -1, .route = Route::kCoordinator, .coord_fn = Command});
  t->Add({.name = "config", .arity = -2, .route = Route::kCoordinator, .coord_fn = ConfigCmd});
  t->Add({.name = "client", .arity = -2, .route = Route::kCoordinator, .coord_fn = Client});
  t->Add({.name = "time", .arity = 1, .route = Route::kCoordinator, .coord_fn = Time});
  t->Add({.name = "info",
          .arity = -1,
          .route = Route::kAllShards,
          .combine = Combine::kInfo,
          .shard_fn = InfoPart});
}

}  // namespace kv
