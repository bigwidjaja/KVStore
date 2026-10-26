#pragma once

#include <cstdint>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "proto/reply_builder.h"

namespace kv {

class Connection;
class Shard;
class Worker;

// Where a command runs.
enum class Route : uint8_t {
  kCoordinator,  // No keys: runs on the connection's own thread (PING, CLIENT, ...).
  kSingleKey,    // args[1] is the key: runs on the shard that owns it (DC-07).
  kPerKey,       // Several keys: split into one op per key, fanned out and gathered.
  kAllShards,    // Runs once on every shard (DBSIZE, FLUSHALL, INFO).
};

// How the coordinator turns per-op results into the client's reply.
enum class Combine : uint8_t {
  kReply,  // The single op's RESP reply is the reply.
  kSum,    // Integer: sum of the ops' integers (DEL, EXISTS, DBSIZE).
  kArray,  // Array of the ops' replies in key order (MGET).
  kOk,     // +OK once every op is done (MSET, FLUSHALL).
  kInfo,   // INFO: sums the threads' counters and formats them.
};

// Runs on the shard that owns the key(s). For kPerKey commands `args` is {name, key} or
// {name, key, value}. Writes a RESP reply to `out`, or sets `integer` for kSum commands.
struct OpContext {
  Shard& shard;
  int64_t now_ms;
  std::span<const std::string_view> args;
  RespWriter out;
  int64_t integer = 0;
};

// Runs on the connection's thread.
struct CoordContext {
  Connection& conn;
  Worker& worker;
  std::span<const std::string_view> args;
  RespWriter out;
};

using ShardFn = void (*)(OpContext&);
using CoordFn = void (*)(CoordContext&);
// Runs on the coordinator before a command is routed. Returns an error text, or "" to proceed.
using CheckFn = std::string (*)(std::span<const std::string_view> args);

struct CommandSpec {
  std::string name;  // Lowercase.
  int arity;         // Redis convention: N means exactly N args, -N means at least N.
  Route route;
  Combine combine = Combine::kReply;
  unsigned key_step = 1;  // kPerKey: 1 for "k1 k2 ...", 2 for "k1 v1 k2 v2 ...".
  ShardFn shard_fn = nullptr;
  CoordFn coord_fn = nullptr;
  CheckFn check = nullptr;
};

class CommandTable {
 public:
  CommandTable();

  // Case-insensitive. Returned pointers stay valid for the table's lifetime.
  const CommandSpec* Find(std::string_view name) const;
  static bool ArityOk(const CommandSpec& cmd, size_t argc);

  void Add(CommandSpec spec);
  size_t size() const { return specs_.size(); }
  std::vector<const CommandSpec*> All() const;

 private:
  std::vector<std::unique_ptr<CommandSpec>> specs_;
  absl::flat_hash_map<std::string, const CommandSpec*> by_name_;
};

// Registration, one function per command family.
void RegisterStringFamily(CommandTable* table);
void RegisterGenericFamily(CommandTable* table);
void RegisterServerFamily(CommandTable* table);

// Redis error texts shared by several commands.
inline constexpr std::string_view kSyntaxErr = "ERR syntax error";
inline constexpr std::string_view kNotIntegerErr = "ERR value is not an integer or out of range";
inline constexpr std::string_view kOverflowErr = "ERR increment or decrement would overflow";

std::string WrongArgsError(std::string_view cmd);
std::string InvalidExpireError(std::string_view cmd);
std::string UnknownCommandError(std::span<const std::string_view> args);
std::string UnknownSubcommandError(std::string_view sub, std::string_view cmd);

}  // namespace kv
