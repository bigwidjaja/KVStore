#include "shard/router.h"

#define XXH_INLINE_ALL
#include <xxhash.h>

namespace kv {

std::string_view RoutingKey(std::string_view key) {
  size_t open = key.find('{');
  if (open == std::string_view::npos) return key;
  size_t close = key.find('}', open + 1);
  if (close == std::string_view::npos || close == open + 1) return key;
  return key.substr(open + 1, close - open - 1);
}

uint32_t ShardOf(std::string_view key, uint32_t num_shards) {
  if (num_shards == 1) return 0;
  std::string_view k = RoutingKey(key);
  return static_cast<uint32_t>(XXH64(k.data(), k.size(), 0) % num_shards);
}

}  // namespace kv
