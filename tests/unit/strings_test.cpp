#include "base/strings.h"

#include <gtest/gtest.h>

#include <limits>

namespace kv {
namespace {

TEST(ParseInt64, AcceptsWhatRedisAccepts) {
  int64_t v;
  ASSERT_TRUE(ParseInt64("0", &v));
  EXPECT_EQ(v, 0);
  ASSERT_TRUE(ParseInt64("123", &v));
  EXPECT_EQ(v, 123);
  ASSERT_TRUE(ParseInt64("-42", &v));
  EXPECT_EQ(v, -42);
  ASSERT_TRUE(ParseInt64("9223372036854775807", &v));
  EXPECT_EQ(v, std::numeric_limits<int64_t>::max());
  ASSERT_TRUE(ParseInt64("-9223372036854775808", &v));
  EXPECT_EQ(v, std::numeric_limits<int64_t>::min());
}

TEST(ParseInt64, RejectsWhatRedisRejects) {
  int64_t v;
  for (const char* s : {"", "-", "+1", "01", "-0", "00", " 1", "1 ", "1.5", "abc", "1e3",
                        "9223372036854775808", "-9223372036854775809",
                        "99999999999999999999999"}) {
    EXPECT_FALSE(ParseInt64(s, &v)) << s;
  }
}

TEST(GlobMatch, Patterns) {
  EXPECT_TRUE(GlobMatch("*", "anything"));
  EXPECT_TRUE(GlobMatch("h?llo", "hello"));
  EXPECT_TRUE(GlobMatch("h*o", "hello"));
  EXPECT_FALSE(GlobMatch("h*x", "hello"));
  EXPECT_TRUE(GlobMatch("h[ae]llo", "hallo"));
  EXPECT_FALSE(GlobMatch("h[^e]llo", "hello"));
  EXPECT_TRUE(GlobMatch("h[a-f]llo", "hello"));
  EXPECT_TRUE(GlobMatch("a\\*b", "a*b"));
  EXPECT_FALSE(GlobMatch("a\\*b", "axb"));
  EXPECT_TRUE(GlobMatch("SAVE", "save", /*nocase=*/true));
  EXPECT_FALSE(GlobMatch("SAVE", "save"));
}

TEST(EqualsIgnoreCase, Basics) {
  EXPECT_TRUE(EqualsIgnoreCase("GeT", "get"));
  EXPECT_FALSE(EqualsIgnoreCase("get", "gets"));
}

}  // namespace
}  // namespace kv
