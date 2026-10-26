#pragma once

#include <cstdint>
#include <string_view>

namespace kv {

// The part of `key` that decides its shard. Same rule as Redis Cluster hash tags: if the key
// has a '{' followed later by a '}' with at least one byte between them, only those bytes are
// hashed, so "{user1}:name" and "{user1}:email" land on the same shard. Otherwise the whole key.
std::string_view RoutingKey(std::string_view key);

// Design after Dragonfly (DC-04): Shard() in src/server/sharding.cc, XXH64(key) % shard_num
// with lock tags. The table inside a shard uses a different hash (XXH3), so the low bits that
// pick the shard don't leave every key in a shard with the same fingerprint bits.
uint32_t ShardOf(std::string_view key, uint32_t num_shards);

}  // namespace kv
