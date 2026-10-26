#pragma once

#include <cstdint>
#include <limits>
#include <string_view>

#include "base/strings.h"

namespace kv {

enum class ExpireParse { kOk, kNotInteger, kInvalid };

// Expire argument of SET, SETEX and PSETEX, checked the way Redis'
// getExpireMillisecondsOrReply does: must be a positive integer, and converting to absolute
// milliseconds must not overflow.
inline ExpireParse ParseSetExpire(std::string_view arg, bool seconds, bool relative,
                                  int64_t now_ms, int64_t* at_ms) {
  int64_t v;
  if (!ParseInt64(arg, &v)) return ExpireParse::kNotInteger;
  if (v <= 0 || (seconds && v > std::numeric_limits<int64_t>::max() / 1000)) {
    return ExpireParse::kInvalid;
  }
  if (seconds) v *= 1000;
  if (relative) {
    if (v > std::numeric_limits<int64_t>::max() - now_ms) return ExpireParse::kInvalid;
    v += now_ms;
  }
  *at_ms = v;
  return ExpireParse::kOk;
}

}  // namespace kv
