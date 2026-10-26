#include "proto/resp_parser.h"

#include <algorithm>
#include <cstring>

#include "base/strings.h"

namespace kv {
namespace {

// Longest legal "*<n>" or "$<n>" header: 20 digits plus a sign. Anything longer is invalid,
// which also bounds how much a client can make us buffer before the first argument.
constexpr size_t kMaxCountLine = 32;

// Upper bound for reserving a large bulk string up front. A client can claim 2 GiB and send
// nothing, so we only trust the claimed length this far and grow normally after that.
constexpr size_t kMaxBulkReserve = 1 << 20;

bool IsHexDigit(char c) {
  return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F');
}

int HexValue(char c) {
  if (c >= '0' && c <= '9') return c - '0';
  if (c >= 'a' && c <= 'f') return c - 'a' + 10;
  return c - 'A' + 10;
}

bool IsSpace(char c) { return c == ' ' || c == '\n' || c == '\r' || c == '\t' || c == '\v' || c == '\f'; }

// Splits an inline command line the way Redis' sdssplitargs does: whitespace separates
// arguments; "double quotes" support \n \r \t \b \a \\ \" and \xHH escapes; 'single quotes'
// support \'. A closing quote must be followed by whitespace or the end of the line.
bool SplitInline(std::string_view line, std::vector<std::string>* out) {
  // sdssplitargs works on C strings, so a NUL ends the line. (Found by the fuzzer: treating
  // NUL as a separator without consuming it looped forever.)
  line = line.substr(0, line.find('\0'));
  size_t i = 0;
  const size_t n = line.size();
  while (true) {
    while (i < n && IsSpace(line[i])) ++i;
    if (i == n) return true;

    std::string cur;
    bool in_dq = false, in_sq = false, done = false;
    while (!done) {
      if (in_dq) {
        if (i == n) return false;
        if (line[i] == '\\' && i + 3 < n && line[i + 1] == 'x' && IsHexDigit(line[i + 2]) &&
            IsHexDigit(line[i + 3])) {
          cur.push_back(static_cast<char>(HexValue(line[i + 2]) * 16 + HexValue(line[i + 3])));
          i += 4;
        } else if (line[i] == '\\' && i + 1 < n) {
          char c = line[i + 1];
          switch (c) {
            case 'n': c = '\n'; break;
            case 'r': c = '\r'; break;
            case 't': c = '\t'; break;
            case 'b': c = '\b'; break;
            case 'a': c = '\a'; break;
            default: break;
          }
          cur.push_back(c);
          i += 2;
        } else if (line[i] == '"') {
          if (i + 1 < n && !IsSpace(line[i + 1])) return false;
          ++i;
          done = true;
        } else {
          cur.push_back(line[i++]);
        }
      } else if (in_sq) {
        if (i == n) return false;
        if (line[i] == '\\' && i + 1 < n && line[i + 1] == '\'') {
          cur.push_back('\'');
          i += 2;
        } else if (line[i] == '\'') {
          if (i + 1 < n && !IsSpace(line[i + 1])) return false;
          ++i;
          done = true;
        } else {
          cur.push_back(line[i++]);
        }
      } else if (i == n || IsSpace(line[i])) {
        done = true;
      } else if (line[i] == '"') {
        in_dq = true;
        ++i;
      } else if (line[i] == '\'') {
        in_sq = true;
        ++i;
      } else {
        cur.push_back(line[i++]);
      }
    }
    out->push_back(std::move(cur));
  }
}

}  // namespace

void RespParser::Reset() {
  state_ = State::kStart;
  finished_ = false;
  bulk_owned_ = false;
  args_left_ = 0;
  bulk_len_ = 0;
  line_.clear();
  owned_.clear();
  refs_.clear();
}

RespParser::Line RespParser::ReadLine(const char* p, size_t n, size_t* pos, size_t max_len,
                                      std::string_view* line) {
  const char* start = p + *pos;
  size_t avail = n - *pos;
  const char* nl = static_cast<const char*>(memchr(start, '\n', avail));
  if (nl == nullptr) {
    if (line_.size() + avail > max_len + 1) return Line::kBad;
    line_.append(start, avail);
    *pos = n;
    return Line::kIncomplete;
  }
  size_t len = static_cast<size_t>(nl - start);
  *pos += len + 1;
  std::string_view piece(start, len);
  if (!line_.empty()) {
    line_.append(piece);
    piece = line_;
  }
  if (piece.empty() || piece.back() != '\r') return Line::kBad;
  piece.remove_suffix(1);
  if (piece.size() > max_len) return Line::kBad;
  *line = piece;
  return Line::kComplete;
}

// Moves arguments that point into the caller's buffer into owned storage, keeping order.
void RespParser::StashRefs() {
  for (const Ref& r : refs_) owned_.emplace_back(r.data, r.len);
  refs_.clear();
}

RespParser::Result RespParser::Pending(size_t n, size_t* consumed) {
  StashRefs();
  *consumed = n;
  return Result::kInputPending;
}

RespParser::Result RespParser::Finish(size_t pos, size_t* consumed, CmdArgs* out) {
  // owned_ holds every argument that arrived in an earlier call, so it always comes first.
  out->reserve(owned_.size() + refs_.size());
  for (const std::string& s : owned_) out->emplace_back(s);
  for (const Ref& r : refs_) out->emplace_back(r.data, r.len);
  *consumed = pos;
  state_ = State::kStart;
  finished_ = true;
  return Result::kOk;
}

RespParser::Result RespParser::Fail(Result r) {
  state_ = State::kStart;
  return r;
}

RespParser::Result RespParser::Parse(std::span<const char> in, size_t* consumed, CmdArgs* out) {
  if (finished_) {
    owned_.clear();
    refs_.clear();
    finished_ = false;
  }
  out->clear();

  const char* p = in.data();
  const size_t n = in.size();
  size_t pos = 0;

  while (true) {
    switch (state_) {
      case State::kStart:
        if (pos == n) return Pending(n, consumed);
        if (p[pos] == '*') {
          ++pos;
          state_ = State::kArrayLen;
        } else {
          state_ = State::kInline;
        }
        break;

      case State::kArrayLen: {
        std::string_view line;
        Line l = ReadLine(p, n, &pos, kMaxCountLine, &line);
        if (l == Line::kIncomplete) return Pending(n, consumed);
        int64_t len = 0;
        bool ok = l == Line::kComplete && ParseInt64(line, &len) &&
                  len <= static_cast<int64_t>(limits_.max_args);
        line_.clear();
        if (!ok) return Fail(Result::kInvalidMultibulkLength);
        if (len <= 0) return Finish(pos, consumed, out);  // "*0" and "*-1" are no-ops, as in Redis.
        args_left_ = len;
        state_ = State::kArgType;
        break;
      }

      case State::kArgType:
        if (pos == n) return Pending(n, consumed);
        if (p[pos] != '$') {
          bad_char_ = p[pos];
          return Fail(Result::kExpectedDollar);
        }
        ++pos;
        state_ = State::kBulkLen;
        break;

      case State::kBulkLen: {
        std::string_view line;
        Line l = ReadLine(p, n, &pos, kMaxCountLine, &line);
        if (l == Line::kIncomplete) return Pending(n, consumed);
        int64_t len = 0;
        bool ok = l == Line::kComplete && ParseInt64(line, &len) && len >= 0 &&
                  static_cast<uint64_t>(len) <= limits_.max_bulk_len;
        line_.clear();
        if (!ok) return Fail(Result::kInvalidBulkLength);
        bulk_len_ = static_cast<size_t>(len);
        bulk_owned_ = false;
        state_ = State::kBulk;
        break;
      }

      case State::kBulk:
        if (bulk_owned_) {
          std::string& s = owned_.back();
          size_t take = std::min(bulk_len_ - s.size(), n - pos);
          s.append(p + pos, take);
          pos += take;
          if (s.size() < bulk_len_) return Pending(n, consumed);
        } else if (n - pos >= bulk_len_) {
          refs_.push_back({p + pos, bulk_len_});
          pos += bulk_len_;
        } else {
          // The bulk string spans reads. Stash earlier arguments first to keep their order.
          StashRefs();
          std::string& s = owned_.emplace_back();
          s.reserve(std::min(bulk_len_, kMaxBulkReserve));
          s.append(p + pos, n - pos);
          bulk_owned_ = true;
          return Pending(n, consumed);
        }
        bulk_owned_ = false;
        state_ = State::kBulkCr;
        break;

      case State::kBulkCr:
        if (pos == n) return Pending(n, consumed);
        if (p[pos] != '\r') return Fail(Result::kMissingCrlf);
        ++pos;
        state_ = State::kBulkLf;
        break;

      case State::kBulkLf:
        if (pos == n) return Pending(n, consumed);
        if (p[pos] != '\n') return Fail(Result::kMissingCrlf);
        ++pos;
        if (--args_left_ == 0) return Finish(pos, consumed, out);
        state_ = State::kArgType;
        break;

      case State::kInline: {
        const char* start = p + pos;
        size_t avail = n - pos;
        const char* nl = static_cast<const char*>(memchr(start, '\n', avail));
        if (nl == nullptr) {
          if (line_.size() + avail > limits_.max_inline_len) return Fail(Result::kTooBigInline);
          line_.append(start, avail);
          return Pending(n, consumed);
        }
        std::string_view line(start, static_cast<size_t>(nl - start));
        pos += line.size() + 1;
        if (!line_.empty()) {
          line_.append(line);
          line = line_;
        }
        if (line.size() > limits_.max_inline_len) return Fail(Result::kTooBigInline);
        if (!line.empty() && line.back() == '\r') line.remove_suffix(1);
        bool ok = SplitInline(line, &owned_);
        line_.clear();
        if (!ok) return Fail(Result::kUnbalancedQuotes);
        if (owned_.size() > limits_.max_args) return Fail(Result::kInvalidMultibulkLength);
        return Finish(pos, consumed, out);
      }
    }
  }
}

std::string RespParser::ErrorText(Result r) const {
  switch (r) {
    case Result::kInvalidMultibulkLength:
      return "ERR Protocol error: invalid multibulk length";
    case Result::kInvalidBulkLength:
      return "ERR Protocol error: invalid bulk length";
    case Result::kExpectedDollar:
      return std::string("ERR Protocol error: expected '$', got '") + bad_char_ + "'";
    case Result::kMissingCrlf:
      return "ERR Protocol error: expected CRLF after bulk string";
    case Result::kTooBigInline:
      return "ERR Protocol error: too big inline request";
    case Result::kUnbalancedQuotes:
      return "ERR Protocol error: unbalanced quotes in request";
    case Result::kOk:
    case Result::kInputPending:
      break;
  }
  return "ERR Protocol error";
}

}  // namespace kv
