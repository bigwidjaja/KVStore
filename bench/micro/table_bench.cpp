// Phase 3, step 9: insert and lookup throughput for our DashTable vs std::unordered_map and
// absl::flat_hash_map, all mapping 16-byte keys to 16-byte values.

#include <benchmark/benchmark.h>

#include <random>
#include <vector>

#include "table_adapters.h"

namespace kv::bench {
namespace {

std::vector<std::string> Keys(size_t n) {
  std::vector<std::string> keys;
  keys.reserve(n);
  for (size_t i = 0; i < n; ++i) keys.push_back(BenchKey(i));
  return keys;
}

const std::string kValue = "value-0123456789";

template <typename Table>
void BM_Insert(benchmark::State& state) {
  const size_t n = static_cast<size_t>(state.range(0));
  std::vector<std::string> keys = Keys(n);
  for (auto _ : state) {
    Table t;
    for (const std::string& k : keys) t.Insert(k, kValue);
    benchmark::DoNotOptimize(t.size());
  }
  state.SetItemsProcessed(state.iterations() * static_cast<int64_t>(n));
}

template <typename Table>
void BM_LookupHit(benchmark::State& state) {
  const size_t n = static_cast<size_t>(state.range(0));
  std::vector<std::string> keys = Keys(n);
  Table t;
  for (const std::string& k : keys) t.Insert(k, kValue);
  std::vector<uint32_t> order(1 << 20);
  std::mt19937 rng(1);
  for (auto& o : order) o = rng() % n;
  size_t i = 0;
  for (auto _ : state) {
    benchmark::DoNotOptimize(t.Lookup(keys[order[i++ & (order.size() - 1)]]));
  }
  state.SetItemsProcessed(state.iterations());
}

template <typename Table>
void BM_LookupMiss(benchmark::State& state) {
  const size_t n = static_cast<size_t>(state.range(0));
  Table t;
  for (size_t k = 0; k < n; ++k) t.Insert(BenchKey(k), kValue);
  std::vector<std::string> misses = Keys(1 << 16);
  for (auto& m : misses) m[0] = 'm';
  size_t i = 0;
  for (auto _ : state) benchmark::DoNotOptimize(t.Lookup(misses[i++ & 0xFFFF]));
  state.SetItemsProcessed(state.iterations());
}

#define KV_TABLE_BENCH(fn)                                                             \
  BENCHMARK(fn<DashAdapter>)->Name(#fn "/dash")->Arg(100000)->Arg(1000000);            \
  BENCHMARK(fn<StdAdapter>)->Name(#fn "/std_unordered_map")->Arg(100000)->Arg(1000000); \
  BENCHMARK(fn<AbslAdapter>)->Name(#fn "/absl_flat_hash_map")->Arg(100000)->Arg(1000000)

KV_TABLE_BENCH(BM_Insert)->Unit(benchmark::kMillisecond);
KV_TABLE_BENCH(BM_LookupHit);
KV_TABLE_BENCH(BM_LookupMiss);

}  // namespace
}  // namespace kv::bench

BENCHMARK_MAIN();
