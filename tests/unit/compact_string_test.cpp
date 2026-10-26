#include "core/compact_string.h"

#include <gtest/gtest.h>

#include <string>
#include <vector>

namespace kv {
namespace {

std::string Str(size_t len, char seed = 'a') {
  std::string s(len, ' ');
  for (size_t i = 0; i < len; ++i) s[i] = static_cast<char>(seed + i % 23);
  return s;
}

TEST(CompactStr, InlineUpTo16Bytes) {
  for (size_t len = 0; len <= 40; ++len) {
    CompactValue v;
    std::string s = Str(len);
    v.Assign(s);
    EXPECT_EQ(v.view(), s);
    EXPECT_EQ(v.size(), len);
    EXPECT_EQ(v.IsInline(), len <= 16) << len;
    EXPECT_EQ(v.HeapBytes(), len <= 16 ? 0 : len + 4);
  }
}

TEST(CompactStr, ReassignAcrossForms) {
  CompactValue v;
  for (size_t len : {3, 30, 16, 17, 0, 100, 5}) {
    std::string s = Str(len, 'k');
    v.Assign(s);
    EXPECT_EQ(v.view(), s);
  }
}

TEST(CompactStr, AssignFromOwnBytes) {
  CompactValue v;
  v.Assign("abcdefgh");
  v.Assign(v.view().substr(2));
  EXPECT_EQ(v.view(), "cdefgh");
  v.Assign(Str(50));
  v.Assign(v.view().substr(10, 20));
  EXPECT_EQ(v.view(), Str(50).substr(10, 20));
}

TEST(CompactStr, MoveTransfersOwnership) {
  CompactValue a;
  a.Assign(Str(64));
  CompactValue b(std::move(a));
  EXPECT_TRUE(a.empty());
  EXPECT_EQ(b.view(), Str(64));
  CompactValue c;
  c.Assign("short");
  c = std::move(b);
  EXPECT_EQ(c.view(), Str(64));
}

TEST(CompactKey, ExpiryKeepsBytes) {
  for (size_t len = 0; len <= 40; ++len) {
    CompactKey k;
    std::string s = Str(len, 'K');
    k.Assign(s);
    EXPECT_FALSE(k.HasExpiry());
    EXPECT_EQ(k.ExpireAtMs(), 0);

    k.SetExpireAtMs(1700000000123);
    EXPECT_TRUE(k.HasExpiry());
    EXPECT_EQ(k.ExpireAtMs(), 1700000000123);
    EXPECT_EQ(k.view(), s);
    // With a TTL only 8 bytes fit inline.
    EXPECT_EQ(k.IsInline(), len <= 8) << len;
    EXPECT_TRUE(k.IsExpired(1700000000123));
    EXPECT_FALSE(k.IsExpired(1700000000122));

    k.SetExpireAtMs(42);
    EXPECT_EQ(k.ExpireAtMs(), 42);
    EXPECT_EQ(k.view(), s);

    k.SetExpireAtMs(0);
    EXPECT_FALSE(k.HasExpiry());
    EXPECT_EQ(k.view(), s);
    EXPECT_EQ(k.IsInline(), len <= 16) << len;
  }
}

TEST(CompactKey, SizeIs18Bytes) {
  EXPECT_EQ(sizeof(CompactKey), 18u);
  EXPECT_EQ(sizeof(CompactValue), 18u);
}

}  // namespace
}  // namespace kv
