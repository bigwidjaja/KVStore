#pragma once

// The three tables behind one tiny interface, so every benchmark treats them the same way:
// string keys to string values, insert-or-assign and lookup.

#include <string>
#include <string_view>
#include <unordered_map>

#include "absl/container/flat_hash_map.h"
#include "core/dash_table.h"

namespace kv::bench {

struct DashAdapter {
  static constexpr const char* kName = "dash";
  DashTable table;

  void Insert(std::string_view key, std::string_view value) {
    uint64_t h = DashTable::Hash(key);
    DashTable::Iterator it = table.Find(key, h);
    if (!it) it = table.InsertNew(key, h, 0, nullptr);
    it->value.Assign(value);
  }
  bool Lookup(std::string_view key) { return bool(table.Find(key, DashTable::Hash(key))); }
  size_t size() const { return table.size(); }
};

struct StdAdapter {
  static constexpr const char* kName = "std_unordered_map";
  std::unordered_map<std::string, std::string> table;

  void Insert(std::string_view key, std::string_view value) {
    table.insert_or_assign(std::string(key), std::string(value));
  }
  bool Lookup(std::string_view key) { return table.find(std::string(key)) != table.end(); }
  size_t size() const { return table.size(); }
};

struct AbslAdapter {
  static constexpr const char* kName = "absl_flat_hash_map";
  absl::flat_hash_map<std::string, std::string> table;

  void Insert(std::string_view key, std::string_view value) {
    table.insert_or_assign(std::string(key), std::string(value));
  }
  bool Lookup(std::string_view key) {
    return table.find(absl::string_view(key.data(), key.size())) != table.end();
  }
  size_t size() const { return table.size(); }
};

// "key:00001234"-style keys: 16 bytes, like typical cache keys and redis-benchmark's.
inline std::string BenchKey(uint64_t i) {
  char buf[32];
  int n = snprintf(buf, sizeof(buf), "key:%012llu", static_cast<unsigned long long>(i));
  return std::string(buf, static_cast<size_t>(n));
}

}  // namespace kv::bench
