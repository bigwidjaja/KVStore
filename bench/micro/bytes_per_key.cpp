// Phase 3, step 1: bytes per key at 1M keys, measured from the allocator (mallinfo2), for
//  - the key handle alone: CompactKey vs std::string, at several key lengths, with and
//    without a TTL;
//  - whole tables of 16-byte keys and 16-byte values.
//
// Usage: bytes_per_key [n] > bytes_per_key.csv

#include <malloc.h>

#include <cstdio>
#include <cstdlib>
#include <memory>
#include <string>
#include <vector>

#include "core/compact_string.h"
#include "table_adapters.h"

namespace kv::bench {
namespace {

// Bytes handed out by malloc, including large blocks it served with mmap.
size_t HeapInUse() {
  malloc_trim(0);
  struct mallinfo2 mi = mallinfo2();
  return mi.uordblks + mi.hblkhd;
}

std::string KeyOfLength(size_t i, size_t len) {
  std::string k = std::to_string(i);
  if (k.size() < len) k.insert(0, len - k.size(), 'k');
  return k.substr(0, len);
}

void Handles(size_t n, size_t len, bool ttl) {
  {
    size_t before = HeapInUse();
    auto keys = std::make_unique<std::string[]>(n);
    for (size_t i = 0; i < n; ++i) keys[i] = KeyOfLength(i, len);
    // A std::string has no room for a TTL; model "TTL next to the key" as 8 more bytes.
    size_t extra = ttl ? n * sizeof(int64_t) : 0;
    double per = double(HeapInUse() - before + extra) / double(n);
    printf("key_handle,std_string,%zu,%s,%.1f\n", len, ttl ? "ttl" : "no_ttl", per);
  }
  {
    size_t before = HeapInUse();
    auto keys = std::make_unique<CompactKey[]>(n);
    for (size_t i = 0; i < n; ++i) {
      keys[i].Assign(KeyOfLength(i, len));
      if (ttl) keys[i].SetExpireAtMs(1700000000000 + static_cast<int64_t>(i));
    }
    double per = double(HeapInUse() - before) / double(n);
    printf("key_handle,compact_key,%zu,%s,%.1f\n", len, ttl ? "ttl" : "no_ttl", per);
  }
}

template <typename Table>
void WholeTable(size_t n) {
  size_t before = HeapInUse();
  auto t = std::make_unique<Table>();
  for (size_t i = 0; i < n; ++i) t->Insert(BenchKey(i), "value-0123456789");
  double per = double(HeapInUse() - before) / double(n);
  printf("table,%s,16,no_ttl,%.1f\n", Table::kName, per);
}

}  // namespace
}  // namespace kv::bench

int main(int argc, char** argv) {
  size_t n = argc > 1 ? std::strtoull(argv[1], nullptr, 10) : 1'000'000;
  printf("kind,impl,key_len,ttl,bytes_per_key\n");
  for (size_t len : {8, 16, 24, 40}) {
    kv::bench::Handles(n, len, false);
    kv::bench::Handles(n, len, true);
  }
  kv::bench::WholeTable<kv::bench::DashAdapter>(n);
  kv::bench::WholeTable<kv::bench::StdAdapter>(n);
  kv::bench::WholeTable<kv::bench::AbslAdapter>(n);
  return 0;
}
