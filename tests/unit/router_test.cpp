#include "shard/router.h"

#include <gtest/gtest.h>

#include <string>
#include <vector>

namespace kv {
namespace {

TEST(Router, HashTags) {
  EXPECT_EQ(RoutingKey("{user1}:name"), "user1");
  EXPECT_EQ(RoutingKey("x{user1}y{z}"), "user1");
  EXPECT_EQ(RoutingKey("plain"), "plain");
  EXPECT_EQ(RoutingKey("{}empty"), "{}empty");  // Empty tag: hash the whole key.
  EXPECT_EQ(RoutingKey("open{only"), "open{only");
  EXPECT_EQ(RoutingKey("}{a}"), "a");
}

TEST(Router, SameTagSameShard) {
  for (uint32_t n : {2u, 3u, 8u, 16u}) {
    EXPECT_EQ(ShardOf("{user1}:name", n), ShardOf("{user1}:email", n));
    EXPECT_EQ(ShardOf("{user1}:name", n), ShardOf("user1", n));
  }
}

TEST(Router, SpreadsKeys) {
  constexpr uint32_t kShards = 8;
  std::vector<int> counts(kShards, 0);
  const int n = 80000;
  for (int i = 0; i < n; ++i) ++counts[ShardOf("key:" + std::to_string(i), kShards)];
  for (int c : counts) {
    EXPECT_GT(c, n / kShards * 0.9);
    EXPECT_LT(c, n / kShards * 1.1);
  }
  EXPECT_EQ(ShardOf("anything", 1), 0u);
}

}  // namespace
}  // namespace kv
