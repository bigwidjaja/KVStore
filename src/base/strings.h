#pragma once

#include <cstdint>
#include <string>
#include <string_view>

namespace kv {

// Parses a signed 64-bit integer with the same rules as Redis' string2ll: an optional '-',
// no '+', no leading zeros, no whitespace, and no overflow. Used for protocol lengths and for
// every integer argument, so error behavior matches Redis.
bool ParseInt64(std::string_view s, int64_t* out);

bool EqualsIgnoreCase(std::string_view a, std::string_view b);
void AppendLower(std::string_view s, std::string* out);

// Glob-style match supporting '*', '?', '[...]' and '\' escapes, like Redis' stringmatchlen.
bool GlobMatch(std::string_view pattern, std::string_view s, bool nocase = false);

}  // namespace kv
