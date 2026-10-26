#include "proto/resp_parser.h"

#include <gtest/gtest.h>

#include <string>
#include <vector>

namespace kv {
namespace {

using Command = std::vector<std::string>;
using Result = RespParser::Result;

struct Outcome {
  std::vector<Command> commands;
  Result last = Result::kInputPending;
};

// Feeds `chunks` to one parser in order, the way a connection would: each chunk is parsed
// until the parser asks for more input.
Outcome ParseChunks(const std::vector<std::string>& chunks, RespParser::Limits limits = {}) {
  RespParser parser(limits);
  Outcome out;
  CmdArgs args;
  for (const std::string& chunk : chunks) {
    size_t pos = 0;
    while (true) {
      size_t consumed = 0;
      Result r = parser.Parse(std::span<const char>(chunk.data() + pos, chunk.size() - pos),
                              &consumed, &args);
      pos += consumed;
      out.last = r;
      if (r == Result::kOk) {
        if (!args.empty()) out.commands.emplace_back(args.begin(), args.end());
        if (pos == chunk.size()) break;
        continue;
      }
      if (r == Result::kInputPending) {
        EXPECT_EQ(pos, chunk.size());
        break;
      }
      return out;  // Error.
    }
  }
  return out;
}

Outcome ParseWhole(const std::string& s) { return ParseChunks({s}); }

std::string Multibulk(const Command& cmd) {
  std::string out = "*" + std::to_string(cmd.size()) + "\r\n";
  for (const std::string& a : cmd) out += "$" + std::to_string(a.size()) + "\r\n" + a + "\r\n";
  return out;
}

struct Case {
  std::string input;
  std::vector<Command> expected;
};

std::vector<Case> Cases() {
  std::string binary("a\r\nb\0c\r\n", 8);
  return {
      {Multibulk({"PING"}), {{"PING"}}},
      {Multibulk({"SET", "key", "value"}), {{"SET", "key", "value"}}},
      {Multibulk({"SET", "k", ""}), {{"SET", "k", ""}}},
      {Multibulk({"SET", "bin", binary}), {{"SET", "bin", binary}}},
      {Multibulk({"GET", "a"}) + Multibulk({"GET", "b"}) + Multibulk({"DEL", "a", "b", "c"}),
       {{"GET", "a"}, {"GET", "b"}, {"DEL", "a", "b", "c"}}},
      {"*0\r\n" + Multibulk({"PING"}), {{"PING"}}},
      {"*-1\r\n" + Multibulk({"PING"}), {{"PING"}}},
      {"PING\r\n", {{"PING"}}},
      {"PING\n", {{"PING"}}},
      {"\r\n\r\nPING\r\n", {{"PING"}}},
      {"SET a \"b c\"\r\nGET  a\r\n", {{"SET", "a", "b c"}, {"GET", "a"}}},
      {"SET a \"\\x41\\n\\\"\" 'it\\'s'\r\n", {{"SET", "a", "A\n\"", "it's"}}},
      {"ECHO hi\r\n" + Multibulk({"ECHO", "there"}), {{"ECHO", "hi"}, {"ECHO", "there"}}},
      // Inline lines end at a NUL, as in Redis. Found by the fuzzer (it used to loop forever).
      {std::string("SET a b\0junk\r\n\0\r\nPING\r\n", 23), {{"SET", "a", "b"}, {"PING"}}},
  };
}

TEST(RespParser, WholeInput) {
  for (const Case& c : Cases()) {
    Outcome out = ParseWhole(c.input);
    EXPECT_EQ(out.commands, c.expected) << c.input;
    EXPECT_TRUE(out.last == Result::kOk || out.last == Result::kInputPending);
  }
}

TEST(RespParser, SplitAtEveryByte) {
  for (const Case& c : Cases()) {
    for (size_t split = 0; split <= c.input.size(); ++split) {
      Outcome out = ParseChunks({c.input.substr(0, split), c.input.substr(split)});
      EXPECT_EQ(out.commands, c.expected) << "split at " << split << " of " << c.input;
    }
  }
}

TEST(RespParser, SplitAtEveryPairOfBytes) {
  for (const Case& c : Cases()) {
    if (c.input.size() > 60) continue;
    for (size_t a = 0; a <= c.input.size(); ++a) {
      for (size_t b = a; b <= c.input.size(); ++b) {
        Outcome out = ParseChunks(
            {c.input.substr(0, a), c.input.substr(a, b - a), c.input.substr(b)});
        EXPECT_EQ(out.commands, c.expected) << a << "," << b << " of " << c.input;
      }
    }
  }
}

TEST(RespParser, OneByteAtATime) {
  for (const Case& c : Cases()) {
    std::vector<std::string> chunks;
    for (char ch : c.input) chunks.emplace_back(1, ch);
    EXPECT_EQ(ParseChunks(chunks).commands, c.expected) << c.input;
  }
}

TEST(RespParser, LargeBulkAcrossManyReads) {
  std::string value(1 << 20, 'x');
  for (size_t i = 0; i < value.size(); i += 4099) value[i] = static_cast<char>('a' + i % 26);
  std::string input = Multibulk({"SET", "big", value}) + Multibulk({"GET", "big"});
  std::vector<std::string> chunks;
  for (size_t i = 0; i < input.size(); i += 1000) chunks.push_back(input.substr(i, 1000));
  Outcome out = ParseChunks(chunks);
  ASSERT_EQ(out.commands.size(), 2u);
  EXPECT_EQ(out.commands[0][2], value);
  EXPECT_EQ(out.commands[1], (Command{"GET", "big"}));
}

TEST(RespParser, MaxArgsAccepted) {
  Command cmd(65536, "a");
  cmd[0] = "DEL";
  Outcome out = ParseWhole(Multibulk(cmd));
  ASSERT_EQ(out.commands.size(), 1u);
  EXPECT_EQ(out.commands[0].size(), 65536u);
}

TEST(RespParser, ArgsPointIntoInputWhenWhole) {
  std::string input = Multibulk({"SET", "key", "value"});
  RespParser parser;
  CmdArgs args;
  size_t consumed;
  ASSERT_EQ(parser.Parse(input, &consumed, &args), Result::kOk);
  EXPECT_EQ(consumed, input.size());
  ASSERT_EQ(args.size(), 3u);
  // Zero-copy: the views point into the caller's buffer.
  EXPECT_GE(args[2].data(), input.data());
  EXPECT_LT(args[2].data(), input.data() + input.size());
}

struct BadCase {
  std::string input;
  Result error;
  std::string text;
};

TEST(RespParser, MalformedInput) {
  const BadCase cases[] = {
      {"*abc\r\n", Result::kInvalidMultibulkLength, "ERR Protocol error: invalid multibulk length"},
      {"*1x\r\n", Result::kInvalidMultibulkLength, "ERR Protocol error: invalid multibulk length"},
      {"*01\r\n", Result::kInvalidMultibulkLength, "ERR Protocol error: invalid multibulk length"},
      {"*65537\r\n", Result::kInvalidMultibulkLength, "ERR Protocol error: invalid multibulk length"},
      {"*99999999999999999999999\r\n", Result::kInvalidMultibulkLength,
       "ERR Protocol error: invalid multibulk length"},
      {"*1\n", Result::kInvalidMultibulkLength, "ERR Protocol error: invalid multibulk length"},
      {"*" + std::string(100, '1'), Result::kInvalidMultibulkLength,
       "ERR Protocol error: invalid multibulk length"},
      {"*1\r\n$-1\r\n", Result::kInvalidBulkLength, "ERR Protocol error: invalid bulk length"},
      {"*1\r\n$-5\r\n", Result::kInvalidBulkLength, "ERR Protocol error: invalid bulk length"},
      {"*1\r\n$abc\r\n", Result::kInvalidBulkLength, "ERR Protocol error: invalid bulk length"},
      {"*1\r\n$3\nfoo\r\n", Result::kInvalidBulkLength, "ERR Protocol error: invalid bulk length"},
      {"*1\r\n$3000000000\r\n", Result::kInvalidBulkLength, "ERR Protocol error: invalid bulk length"},
      {"*1\r\n+foo\r\n", Result::kExpectedDollar, "ERR Protocol error: expected '$', got '+'"},
      {"*2\r\n$3\r\nfoo\r\n:1\r\n", Result::kExpectedDollar, "ERR Protocol error: expected '$', got ':'"},
      {"*1\r\n$3\r\nfooXX", Result::kMissingCrlf, "ERR Protocol error: expected CRLF after bulk string"},
      {"*1\r\n$3\r\nfoo\rX", Result::kMissingCrlf, "ERR Protocol error: expected CRLF after bulk string"},
      {"SET a \"b\r\n", Result::kUnbalancedQuotes, "ERR Protocol error: unbalanced quotes in request"},
      {"SET a \"b\"c\r\n", Result::kUnbalancedQuotes, "ERR Protocol error: unbalanced quotes in request"},
      {"SET a 'b\r\n", Result::kUnbalancedQuotes, "ERR Protocol error: unbalanced quotes in request"},
      {std::string(70000, 'a'), Result::kTooBigInline, "ERR Protocol error: too big inline request"},
  };
  for (const BadCase& c : cases) {
    // Whole, and one byte at a time: errors must not depend on how input is split.
    RespParser parser;
    CmdArgs args;
    size_t consumed;
    Result r = parser.Parse(c.input, &consumed, &args);
    EXPECT_EQ(r, c.error) << c.input.substr(0, 40);
    EXPECT_EQ(parser.ErrorText(r), c.text);

    std::vector<std::string> chunks;
    for (char ch : c.input) chunks.emplace_back(1, ch);
    EXPECT_EQ(ParseChunks(chunks).last, c.error) << "bytewise: " << c.input.substr(0, 40);
  }
}

TEST(RespParser, BulkLimitIsConfigurable) {
  RespParser::Limits limits;
  limits.max_bulk_len = 10;
  EXPECT_EQ(ParseChunks({Multibulk({"SET", "k", std::string(10, 'v')})}, limits).commands.size(), 1u);
  EXPECT_EQ(ParseChunks({Multibulk({"SET", "k", std::string(11, 'v')})}, limits).last,
            Result::kInvalidBulkLength);
}

TEST(RespParser, ResetRecoversAfterError) {
  RespParser parser;
  CmdArgs args;
  size_t consumed;
  std::string bad = "*1\r\n+x\r\n";
  ASSERT_EQ(parser.Parse(bad, &consumed, &args), Result::kExpectedDollar);
  parser.Reset();
  std::string good = Multibulk({"PING"});
  ASSERT_EQ(parser.Parse(good, &consumed, &args), Result::kOk);
  EXPECT_EQ(args.size(), 1u);
}

}  // namespace
}  // namespace kv
