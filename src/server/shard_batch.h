#pragma once

#include <cstddef>
#include <cstdint>
#include <span>
#include <string>
#include <string_view>
#include <vector>

namespace kv {

struct CommandSpec;

// One command, or one key of a multi-key command, to run on another shard.
struct ShardOp {
  uint64_t seq;       // Reply slot in the origin connection.
  const CommandSpec* cmd;
  uint32_t part;      // Index of the key within a multi-key command, or of the shard for kAllShards.
  uint32_t first_arg;
  uint32_t num_args;
  // Filled in by the owning shard.
  int64_t integer = 0;
  size_t reply_begin = 0;
  size_t reply_end = 0;
};

// Everything one connection sends one shard from a single read batch.
//
// Design after Dragonfly (DC-10): MultiCommandSquasher and Connection::SquashPipeline group
// pipelined commands by shard and send each group in one hop. A batch makes a round trip: it
// is built on the origin thread, executed by the owner (which writes the replies into it),
// sent back, and freed by the thread that allocated it. Arguments are copied in because the
// connection's read buffer is reused as soon as the batch leaves.
struct ShardBatch {
  uint32_t origin = 0;
  uint32_t target = 0;
  uint32_t conn_id = 0;
  uint32_t conn_gen = 0;
  std::vector<ShardOp> ops;
  std::string arg_bytes;
  std::vector<size_t> arg_ends;  // Argument i is arg_bytes[arg_ends[i-1], arg_ends[i]).
  std::string replies;

  void AddOp(uint64_t seq, const CommandSpec* cmd, uint32_t part,
             std::span<const std::string_view> args) {
    ops.push_back(ShardOp{seq, cmd, part, static_cast<uint32_t>(arg_ends.size()),
                          static_cast<uint32_t>(args.size())});
    for (std::string_view a : args) {
      arg_bytes.append(a);
      arg_ends.push_back(arg_bytes.size());
    }
  }

  std::string_view Arg(size_t i) const {
    size_t begin = i == 0 ? 0 : arg_ends[i - 1];
    return std::string_view(arg_bytes).substr(begin, arg_ends[i] - begin);
  }

  std::string_view Reply(const ShardOp& op) const {
    return std::string_view(replies).substr(op.reply_begin, op.reply_end - op.reply_begin);
  }
};

}  // namespace kv
