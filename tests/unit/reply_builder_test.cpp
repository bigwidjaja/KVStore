#include "proto/reply_builder.h"

#include <gtest/gtest.h>

#include <limits>

namespace kv {
namespace {

std::string Joined(ReplyBuffer& buf) {
  std::string out;
  for (const iovec& v : buf.Iovecs()) out.append(static_cast<const char*>(v.iov_base), v.iov_len);
  return out;
}

TEST(RespWriter, AllTypes) {
  std::string s;
  RespWriter w(&s);
  w.SimpleString("OK");
  w.Error("ERR bad\r\nthing");
  w.Integer(-42);
  w.Integer(std::numeric_limits<int64_t>::min());
  w.Bulk("hello");
  w.Bulk("");
  w.NullBulk();
  w.ArrayHeader(2);
  w.NullArray();
  EXPECT_EQ(s,
            "+OK\r\n"
            "-ERR bad  thing\r\n"
            ":-42\r\n"
            ":-9223372036854775808\r\n"
            "$5\r\nhello\r\n"
            "$0\r\n\r\n"
            "$-1\r\n"
            "*2\r\n"
            "*-1\r\n");
}

TEST(ReplyBuffer, SmallRepliesShareOneIovec) {
  ReplyBuffer buf;
  EXPECT_TRUE(buf.Empty());
  for (int i = 0; i < 100; ++i) RespWriter(buf.Tail()).Ok();
  EXPECT_EQ(buf.Iovecs().size(), 1u);
  EXPECT_EQ(buf.Bytes(), 500u);
}

TEST(ReplyBuffer, ChunksAt8K) {
  ReplyBuffer buf;
  std::string reply(1000, 'x');
  for (int i = 0; i < 20; ++i) buf.Append(std::string_view(reply));
  EXPECT_EQ(buf.Bytes(), 20000u);
  EXPECT_GE(buf.Iovecs().size(), 2u);
  EXPECT_EQ(Joined(buf), std::string(20000, 'x'));
}

TEST(ReplyBuffer, LargeRepliesAreReferencedNotCopied) {
  ReplyBuffer buf;
  RespWriter(buf.Tail()).Ok();
  std::string big(100000, 'b');
  const char* data = big.data();
  buf.Append(std::move(big));
  RespWriter(buf.Tail()).Integer(7);
  auto iov = buf.Iovecs();
  ASSERT_EQ(iov.size(), 3u);
  EXPECT_EQ(iov[1].iov_base, data);  // The same allocation, moved in.
  EXPECT_EQ(Joined(buf), "+OK\r\n" + std::string(100000, 'b') + ":7\r\n");
}

TEST(ReplyBuffer, ClearKeepsWorking) {
  ReplyBuffer buf;
  buf.Append(std::string(50000, 'z'));
  RespWriter(buf.Tail()).Ok();
  buf.Clear();
  EXPECT_TRUE(buf.Empty());
  EXPECT_EQ(buf.Iovecs().size(), 0u);
  RespWriter(buf.Tail()).Integer(1);
  EXPECT_EQ(Joined(buf), ":1\r\n");
}

}  // namespace
}  // namespace kv
