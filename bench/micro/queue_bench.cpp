// Phase 4, step 5: messages per second through one MpscQueue<InlineTask>, with 1 to 8
// producers and the benchmark thread as the consumer.

#include <benchmark/benchmark.h>

#include <atomic>
#include <thread>
#include <vector>

#include "base/time.h"
#include "shard/inline_task.h"
#include "shard/mpsc_queue.h"

namespace kv {
namespace {

void BM_MpscThroughput(benchmark::State& state) {
  const int producers = static_cast<int>(state.range(0));
  const size_t capacity = static_cast<size_t>(state.range(1));
  constexpr uint64_t kPerProducer = 1'000'000;
  for (auto _ : state) {
    MpscQueue<InlineTask> q(capacity);
    std::atomic<bool> go{false};
    uint64_t sum = 0;
    std::vector<std::thread> threads;
    for (int p = 0; p < producers; ++p) {
      threads.emplace_back([&] {
        while (!go.load(std::memory_order_acquire)) CpuRelax();
        for (uint64_t i = 0; i < kPerProducer; ++i) {
          InlineTask t = [&sum] { ++sum; };
          while (!q.TryPush(t)) CpuRelax();
        }
      });
    }
    go.store(true, std::memory_order_release);
    InlineTask t;
    for (uint64_t got = 0; got < kPerProducer * producers;) {
      if (q.TryPop(t)) {
        t();
        ++got;
      } else {
        CpuRelax();
      }
    }
    for (auto& th : threads) th.join();
    benchmark::DoNotOptimize(sum);
  }
  state.SetItemsProcessed(state.iterations() * static_cast<int64_t>(kPerProducer) * producers);
}

BENCHMARK(BM_MpscThroughput)
    ->ArgsProduct({{1, 2, 4, 7}, {256, 4096}})
    ->ArgNames({"producers", "capacity"})
    ->UseRealTime()
    ->Unit(benchmark::kMillisecond);

void BM_PushPopSameThread(benchmark::State& state) {
  MpscQueue<InlineTask> q(256);
  int x = 0;
  InlineTask out;
  for (auto _ : state) {
    InlineTask t = [&x] { ++x; };
    q.TryPush(t);
    q.TryPop(out);
    out();
  }
  benchmark::DoNotOptimize(x);
  state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_PushPopSameThread);

}  // namespace
}  // namespace kv

BENCHMARK_MAIN();
