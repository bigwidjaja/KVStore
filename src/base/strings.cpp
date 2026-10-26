#include "base/strings.h"

#include <cctype>
#include <limits>
#include <utility>

namespace kv {

bool ParseInt64(std::string_view s, int64_t* out) {
  if (s.empty() || s.size() > 20) return false;
  if (s.size() == 1 && s[0] == '0') {
    *out = 0;
    return true;
  }
  size_t i = 0;
  bool negative = false;
  if (s[0] == '-') {
    negative = true;
    i = 1;
    if (s.size() == 1) return false;
  }
  // First digit must be 1-9: this rejects "-0", "00" and "007" as Redis does.
  if (s[i] < '1' || s[i] > '9') return false;
  uint64_t v = 0;
  for (; i < s.size(); ++i) {
    char c = s[i];
    if (c < '0' || c > '9') return false;
    uint64_t digit = static_cast<uint64_t>(c - '0');
    if (v > (std::numeric_limits<uint64_t>::max() - digit) / 10) return false;
    v = v * 10 + digit;
  }
  if (negative) {
    constexpr uint64_t kMinMagnitude = static_cast<uint64_t>(std::numeric_limits<int64_t>::max()) + 1;
    if (v > kMinMagnitude) return false;
    *out = v == kMinMagnitude ? std::numeric_limits<int64_t>::min() : -static_cast<int64_t>(v);
  } else {
    if (v > static_cast<uint64_t>(std::numeric_limits<int64_t>::max())) return false;
    *out = static_cast<int64_t>(v);
  }
  return true;
}

bool EqualsIgnoreCase(std::string_view a, std::string_view b) {
  if (a.size() != b.size()) return false;
  for (size_t i = 0; i < a.size(); ++i) {
    if (std::tolower(static_cast<unsigned char>(a[i])) !=
        std::tolower(static_cast<unsigned char>(b[i])))
      return false;
  }
  return true;
}

void AppendLower(std::string_view s, std::string* out) {
  for (char c : s) out->push_back(static_cast<char>(std::tolower(static_cast<unsigned char>(c))));
}

namespace {

char Fold(char c, bool nocase) {
  return nocase ? static_cast<char>(std::tolower(static_cast<unsigned char>(c))) : c;
}

bool GlobMatchImpl(const char* p, size_t plen, const char* s, size_t slen, bool nocase, int depth) {
  if (depth > 1000) return false;  // Same guard against pathological patterns as Redis.
  while (plen > 0) {
    switch (*p) {
      case '*': {
        while (plen > 1 && p[1] == '*') {
          ++p;
          --plen;
        }
        if (plen == 1) return true;
        for (size_t i = 0; i <= slen; ++i) {
          if (GlobMatchImpl(p + 1, plen - 1, s + i, slen - i, nocase, depth + 1)) return true;
        }
        return false;
      }
      case '?':
        if (slen == 0) return false;
        ++s;
        --slen;
        break;
      case '[': {
        if (slen == 0) return false;
        ++p;
        --plen;
        bool negate = plen > 0 && *p == '^';
        if (negate) {
          ++p;
          --plen;
        }
        bool match = false;
        while (plen > 0 && *p != ']') {
          if (*p == '\\' && plen >= 2) {
            ++p;
            --plen;
            if (Fold(*p, nocase) == Fold(*s, nocase)) match = true;
          } else if (plen >= 3 && p[1] == '-' && p[2] != ']') {
            char lo = Fold(p[0], nocase), hi = Fold(p[2], nocase), c = Fold(*s, nocase);
            if (lo > hi) std::swap(lo, hi);
            if (c >= lo && c <= hi) match = true;
            p += 2;
            plen -= 2;
          } else if (Fold(*p, nocase) == Fold(*s, nocase)) {
            match = true;
          }
          ++p;
          --plen;
        }
        if (negate) match = !match;
        if (!match) return false;
        ++s;
        --slen;
        if (plen == 0) return slen == 0;  // Unterminated '[': treat as end of pattern.
        break;
      }
      case '\\':
        if (plen >= 2) {
          ++p;
          --plen;
        }
        [[fallthrough]];
      default:
        if (slen == 0 || Fold(*p, nocase) != Fold(*s, nocase)) return false;
        ++s;
        --slen;
        break;
    }
    ++p;
    --plen;
  }
  return slen == 0;
}

}  // namespace

bool GlobMatch(std::string_view pattern, std::string_view s, bool nocase) {
  return GlobMatchImpl(pattern.data(), pattern.size(), s.data(), s.size(), nocase, 0);
}

}  // namespace kv
