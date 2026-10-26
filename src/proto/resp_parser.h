#pragma once

#include <cstddef>
#include <cstdint>
#include <span>
#include <string>
#include <string_view>
#include <vector>

namespace kv {

// Arguments of one command, including the command name at index 0.
using CmdArgs = std::vector<std::string_view>;

// Incremental RESP2 request parser.
//
// Design after Dragonfly (DC-08): src/facade/resp_srv_parser.h. The parser is an explicit
// state machine (array length, argument type, bulk length, bulk data, trailing CRLF) that
// survives split reads: when input runs out mid-command it returns kInputPending with its
// state cached, so the caller can drop the bytes it has consumed.
//
// Our adaptation: on kInputPending the parser copies what it still needs (finished arguments
// of the current command, plus the partial one) into its own storage and reports the whole
// input as consumed. The caller's read buffer never has to hold a partial command, so it
// can stay small (DC-11). A command that arrives in one piece is parsed with zero copies.
//
// Inline commands ("PING\r\n", as typed into telnet) are also accepted, split with the same
// quoting rules as Redis' sdssplitargs.
class RespParser {
 public:
  enum class Result : uint8_t {
    kOk,            // One complete command is in `out`. It can be empty; callers skip it.
    kInputPending,  // All input was consumed; call again with more bytes.
    kInvalidMultibulkLength,
    kInvalidBulkLength,
    kExpectedDollar,
    kMissingCrlf,
    kTooBigInline,
    kUnbalancedQuotes,
  };

  struct Limits {
    size_t max_args = 65536;                // DC-08: max_multi_bulk_len
    size_t max_bulk_len = size_t{2} << 30;  // DC-08: max_bulk_len, 2 GiB
    size_t max_inline_len = 64 * 1024;      // Redis' PROTO_INLINE_MAX_SIZE
  };

  RespParser() = default;
  explicit RespParser(const Limits& limits) : limits_(limits) {}

  // Parses at most one command from `in`.
  //  kOk:           *consumed is the number of bytes this command used. `out` holds its
  //                 arguments, valid until the next Parse() call and only while `in` is intact.
  //  kInputPending: *consumed == in.size().
  //  errors:        reply with ErrorText() and close the connection. The parser must be Reset()
  //                 before reuse.
  Result Parse(std::span<const char> in, size_t* consumed, CmdArgs* out);

  // Redis-compatible error text for an error result, without the leading '-'.
  std::string ErrorText(Result r) const;

  void Reset();

  // True while a command is partially parsed.
  bool InProgress() const { return state_ != State::kStart || !line_.empty(); }

 private:
  enum class State : uint8_t { kStart, kArrayLen, kArgType, kBulkLen, kBulk, kBulkCr, kBulkLf, kInline };
  enum class Line : uint8_t { kComplete, kIncomplete, kBad };

  struct Ref {
    const char* data;
    size_t len;
  };

  Line ReadLine(const char* p, size_t n, size_t* pos, size_t max_len, std::string_view* line);
  void StashRefs();
  Result Pending(size_t n, size_t* consumed);
  Result Finish(size_t pos, size_t* consumed, CmdArgs* out);
  Result Fail(Result r);

  Limits limits_;
  State state_ = State::kStart;
  bool finished_ = false;      // Last call returned kOk; owned_ must be dropped first.
  bool bulk_owned_ = false;    // The bulk string in progress lives in owned_.back().
  int64_t args_left_ = 0;
  size_t bulk_len_ = 0;
  char bad_char_ = 0;
  std::string line_;               // Partial header or inline line carried across calls.
  std::vector<std::string> owned_;  // Arguments copied out of earlier inputs.
  std::vector<Ref> refs_;           // Arguments that still point into the current input.
};

}  // namespace kv
