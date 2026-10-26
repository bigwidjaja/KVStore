// libFuzzer target for the RESP parser (Phase 1, step 4).
//
// Besides crashing under ASan/UBSan, the fuzzer checks one property: parsing the input whole
// and parsing it in chunks (sizes taken from the first input byte, as a connection receiving
// split reads would) must give exactly the same commands and the same final result.
//
//   cmake --preset fuzz && cmake --build --preset fuzz
//   build/fuzz/tests/resp_parser_fuzz -max_total_time=600 tests/fuzz/seeds

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

#include "proto/resp_parser.h"

namespace {

using kv::CmdArgs;
using kv::RespParser;

struct Run {
  std::vector<std::vector<std::string>> commands;
  RespParser::Result last = RespParser::Result::kInputPending;
};

RespParser::Limits FuzzLimits() {
  RespParser::Limits limits;
  limits.max_args = 1024;
  limits.max_bulk_len = 1 << 20;
  limits.max_inline_len = 4096;
  return limits;
}

Run Parse(const char* data, size_t size, size_t chunk) {
  RespParser parser(FuzzLimits());
  CmdArgs args;
  Run run;
  std::string buf;  // Plays the connection's read buffer.
  size_t pos = 0;
  while (pos < size) {
    size_t n = std::min(chunk, size - pos);
    buf.append(data + pos, n);
    pos += n;
    size_t off = 0;
    while (off < buf.size()) {
      size_t consumed = 0;
      RespParser::Result r =
          parser.Parse(std::span<const char>(buf.data() + off, buf.size() - off), &consumed, &args);
      if (consumed > buf.size() - off) __builtin_trap();
      off += consumed;
      run.last = r;
      if (r == RespParser::Result::kOk) {
        if (!args.empty()) run.commands.emplace_back(args.begin(), args.end());
        continue;
      }
      if (r == RespParser::Result::kInputPending) {
        if (off != buf.size()) __builtin_trap();  // Pending must consume everything.
        break;
      }
      return run;  // Protocol error: the connection would close here.
    }
    buf.erase(0, off);
  }
  return run;
}

}  // namespace

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
  if (size < 1) return 0;
  size_t chunk = 1 + data[0] % 97;
  const char* body = reinterpret_cast<const char*>(data + 1);
  size_t len = size - 1;

  Run whole = Parse(body, len, len == 0 ? 1 : len);
  Run split = Parse(body, len, chunk);
  if (whole.last != split.last || whole.commands != split.commands) __builtin_trap();
  return 0;
}
