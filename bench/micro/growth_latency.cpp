// Phase 3, step 9: worst-case single-insert latency while a table grows.
//
// Inserts N keys (default 10M) one at a time into each table and times every insert. A
// rehash-everything table shows a spike each time it doubles; a segmented table only ever
// splits one segment. Writes two CSVs:
//   <out>/growth_latency.csv  table,inserted,window_max_us   (max per window of inserts)
//   <out>/growth_summary.csv  table,n,p50_ns,p99_ns,p999_ns,p9999_ns,max_us,total_ms
//
// Usage: growth_latency [n] [out_dir]

#include <time.h>

#include <algorithm>
#include <cstdio>
#include <cstdlib>
#include <string>
#include <vector>

#include "table_adapters.h"

namespace kv::bench {
namespace {

uint64_t NowNs() {
  timespec ts;
  clock_gettime(CLOCK_MONOTONIC, &ts);
  return static_cast<uint64_t>(ts.tv_sec) * 1000000000ull + static_cast<uint64_t>(ts.tv_nsec);
}

template <typename Table>
void Run(size_t n, size_t window, FILE* series, FILE* summary) {
  std::vector<std::string> keys;
  keys.reserve(n);
  for (size_t i = 0; i < n; ++i) keys.push_back(BenchKey(i * 2654435761u));
  const std::string value = "value-0123456789";

  std::vector<uint32_t> lat(n);  // ns, saturating at ~4 s.
  Table t;
  uint64_t start = NowNs();
  for (size_t i = 0; i < n; ++i) {
    uint64_t t0 = NowNs();
    t.Insert(keys[i], value);
    uint64_t d = NowNs() - t0;
    lat[i] = static_cast<uint32_t>(std::min<uint64_t>(d, UINT32_MAX));
  }
  double total_ms = double(NowNs() - start) / 1e6;

  for (size_t i = 0; i < n; i += window) {
    uint32_t m = *std::max_element(lat.begin() + i, lat.begin() + std::min(n, i + window));
    fprintf(series, "%s,%zu,%.3f\n", Table::kName, std::min(n, i + window), m / 1000.0);
  }
  uint32_t max_ns = *std::max_element(lat.begin(), lat.end());
  std::sort(lat.begin(), lat.end());
  auto pct = [&](double p) { return lat[std::min(n - 1, static_cast<size_t>(p * n))]; };
  fprintf(summary, "%s,%zu,%u,%u,%u,%u,%.1f,%.1f\n", Table::kName, n, pct(0.5), pct(0.99),
          pct(0.999), pct(0.9999), max_ns / 1000.0, total_ms);
  fprintf(stderr, "%-20s n=%zu p99=%uns p99.99=%uns max=%.1fus total=%.0fms\n", Table::kName, n,
          pct(0.99), pct(0.9999), max_ns / 1000.0, total_ms);
}

}  // namespace
}  // namespace kv::bench

int main(int argc, char** argv) {
  size_t n = argc > 1 ? std::strtoull(argv[1], nullptr, 10) : 10'000'000;
  std::string out = argc > 2 ? argv[2] : ".";
  FILE* series = fopen((out + "/growth_latency.csv").c_str(), "w");
  FILE* summary = fopen((out + "/growth_summary.csv").c_str(), "w");
  if (!series || !summary) {
    perror("fopen");
    return 1;
  }
  fprintf(series, "table,inserted,window_max_us\n");
  fprintf(summary, "table,n,p50_ns,p99_ns,p999_ns,p9999_ns,max_us,total_ms\n");
  size_t window = std::max<size_t>(1, n / 2000);
  kv::bench::Run<kv::bench::DashAdapter>(n, window, series, summary);
  kv::bench::Run<kv::bench::StdAdapter>(n, window, series, summary);
  kv::bench::Run<kv::bench::AbslAdapter>(n, window, series, summary);
  fclose(series);
  fclose(summary);
  return 0;
}
