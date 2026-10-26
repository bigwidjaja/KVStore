#include "shard/inline_task.h"

#include <gtest/gtest.h>

#include <memory>

namespace kv {
namespace {

TEST(InlineTask, RunsAndMoves) {
  int calls = 0;
  InlineTask t = [&calls] { ++calls; };
  t();
  InlineTask moved = std::move(t);
  EXPECT_FALSE(t);
  moved();
  EXPECT_EQ(calls, 2);
}

TEST(InlineTask, OwnsMoveOnlyCaptures) {
  auto counter = std::make_shared<int>(0);
  {
    auto payload = std::make_unique<std::shared_ptr<int>>(counter);
    InlineTask t = [p = std::move(payload)]() mutable { ++**p; };
    EXPECT_EQ(counter.use_count(), 2);
    t();
    InlineTask other;
    other = std::move(t);
    other();
  }
  EXPECT_EQ(*counter, 2);
  EXPECT_EQ(counter.use_count(), 1);  // The capture was destroyed exactly once.
}

TEST(InlineTask, ResetDestroysCapture) {
  auto counter = std::make_shared<int>(0);
  InlineTask t = [c = counter] { ++*c; };
  EXPECT_EQ(counter.use_count(), 2);
  t.Reset();
  EXPECT_EQ(counter.use_count(), 1);
}

}  // namespace
}  // namespace kv
