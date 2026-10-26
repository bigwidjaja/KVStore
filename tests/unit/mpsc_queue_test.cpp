// Phase 4, step 5: the queue is tested on its own before anything is wired to it. The stress
// test runs 8 producers x 10M messages by default (KV_QUEUE_STRESS_MSGS overrides the
// per-producer count) and is meant to run under ThreadSanitizer.

#include "shard/mpsc_queue.h"

#include <gtest/gtest.h>

#include <atomic>
#include <cstdlib>
#include <thread>
#include <vector>

#include "base/time.h"
#include "shard/inline_task.h"

namespace kv {
namespace {

TEST(MpscQueue, FifoAndFull) {
  MpscQueue<int> q(4);
  EXPECT_TRUE(q.Empty());
  for (int i = 0; i < 4; ++i) {
    int v = i;
    ASSERT_TRUE(q.TryPush(v));
  }
  int extra = 99;
  EXPECT_FALSE(q.TryPush(extra));
  EXPECT_EQ(extra, 99);  // Untouched on failure.
  int out;
  for (int i = 0; i < 4; ++i) {
    ASSERT_TRUE(q.TryPop(out));
    EXPECT_EQ(out, i);
  }
  EXPECT_FALSE(q.TryPop(out));
  EXPECT_TRUE(q.Empty());
}

TEST(MpscQueue, WrapsAround) {
  MpscQueue<int> q(8);
  int out;
  for (int i = 0; i < 1000; ++i) {
    int v = i;
    ASSERT_TRUE(q.TryPush(v));
    if (i % 3 == 2) {
      while (q.TryPop(out)) {
      }
    }
  }
}

TEST(MpscQueue, FailedPushKeepsTask) {
  MpscQueue<InlineTask> q(2);
  int ran = 0;
  for (int i = 0; i < 2; ++i) {
    InlineTask t = [&ran] { ++ran; };
    ASSERT_TRUE(q.TryPush(t));
  }
  InlineTask kept = [&ran] { ran += 10; };
  ASSERT_FALSE(q.TryPush(kept));
  ASSERT_TRUE(kept);
  InlineTask t;
  while (q.TryPop(t)) t();
  kept();
  EXPECT_EQ(ran, 12);
}

TEST(MpscQueue, DestructorDropsRemainingItems) {
  auto counter = std::make_shared<int>(0);
  {
    MpscQueue<InlineTask> q(8);
    for (int i = 0; i < 5; ++i) {
      InlineTask t = [c = counter] { ++*c; };
      ASSERT_TRUE(q.TryPush(t));
    }
    EXPECT_EQ(counter.use_count(), 6);
  }
  EXPECT_EQ(counter.use_count(), 1);
}

uint64_t MessagesPerProducer() {
  if (const char* env = std::getenv("KV_QUEUE_STRESS_MSGS")) return std::strtoull(env, nullptr, 10);
  return 10'000'000;
}

TEST(MpscQueueStress, EightProducersKeepCountAndOrder) {
  constexpr unsigned kProducers = 8;
  const uint64_t per_producer = MessagesPerProducer();
  // Small capacity on purpose: producers hit "full" constantly and must retry.
  MpscQueue<uint64_t> q(256);
  std::atomic<bool> go{false};

  std::vector<std::thread> producers;
  for (unsigned p = 0; p < kProducers; ++p) {
    producers.emplace_back([&, p] {
      while (!go.load(std::memory_order_acquire)) CpuRelax();
      for (uint64_t i = 0; i < per_producer; ++i) {
        uint64_t msg = (uint64_t{p} << 56) | i;  // Producer id in the top byte.
        while (!q.TryPush(msg)) CpuRelax();
      }
    });
  }

  std::vector<uint64_t> next(kProducers, 0);
  uint64_t received = 0;
  const uint64_t total = per_producer * kProducers;
  go.store(true, std::memory_order_release);
  uint64_t start = MonotonicNs();
  uint64_t msg;
  while (received < total) {
    if (!q.TryPop(msg)) {
      CpuRelax();
      continue;
    }
    unsigned p = static_cast<unsigned>(msg >> 56);
    uint64_t seq = msg & ((uint64_t{1} << 56) - 1);
    ASSERT_LT(p, kProducers);
    ASSERT_EQ(seq, next[p]) << "producer " << p << " out of order";
    ++next[p];
    ++received;
  }
  double secs = double(MonotonicNs() - start) / 1e9;
  for (auto& t : producers) t.join();

  EXPECT_FALSE(q.TryPop(msg));
  for (unsigned p = 0; p < kProducers; ++p) EXPECT_EQ(next[p], per_producer);
  RecordProperty("messages", std::to_string(total));
  RecordProperty("msgs_per_sec", std::to_string(static_cast<uint64_t>(double(total) / secs)));
}

}  // namespace
}  // namespace kv
