#pragma once

#include <cstddef>
#include <cstdint>
#include <string_view>
#include <utility>

#include "core/dash_table.h"

namespace kv {

// One shard's key space: the table plus TTL bookkeeping. This is the interface the command
// handlers use; nothing above it touches the table directly. Single-threaded by design: only
// the owning shard thread ever calls it (DC-01).
class DbSlice {
 public:
  using Iterator = DashTable::Iterator;

  // DC-18: active expiry budget, in keys with a TTL examined per heartbeat.
  static constexpr size_t kMinExpireBudget = 5;
  static constexpr size_t kMaxExpireBudget = 200;

  struct ExpireStepStats {
    size_t examined = 0;  // Keys with a TTL that were checked.
    size_t deleted = 0;
  };

  struct Stats {
    uint64_t expired_keys = 0;  // Lazy, active, and before-split expiry combined.
    uint64_t keyspace_hits = 0;
    uint64_t keyspace_misses = 0;
  };

  DbSlice() = default;
  DbSlice(const DbSlice&) = delete;
  DbSlice& operator=(const DbSlice&) = delete;

  // Returns the live entry for `key`. An entry whose TTL has passed is deleted on the spot
  // and reported as missing (lazy expiry).
  Iterator Find(std::string_view key, int64_t now_ms);

  // Find() that also counts a keyspace hit or miss, for read commands.
  Iterator FindRead(std::string_view key, int64_t now_ms);

  // Returns the live entry, inserting an empty one with no TTL if needed. The bool is true
  // when the entry was inserted.
  std::pair<Iterator, bool> FindOrInsert(std::string_view key, int64_t now_ms);

  // Deletes `key` if it is live. Returns true if something was deleted.
  bool Delete(std::string_view key, int64_t now_ms);
  void Delete(Iterator it);

  // Sets the absolute expiry time in Unix ms; 0 removes the TTL.
  void SetExpire(Iterator it, int64_t at_ms);

  size_t size() const { return table_.size(); }
  size_t expires() const { return expires_; }
  void Clear();

  // Examines keys with a TTL from the saved cursor until `budget` of them were checked or a
  // full pass ended, deleting expired ones.
  ExpireStepStats DeleteExpiredStep(int64_t now_ms, size_t budget);

  // Runs one active-expiry step and adapts the next step's budget (DC-18). Called at 100 Hz.
  void Heartbeat(int64_t now_ms);
  size_t expire_budget() const { return expire_budget_; }

  const Stats& stats() const { return stats_; }
  const DashTable& table() const { return table_; }

 private:
  void EraseExpired(Iterator it, uint64_t hash);

  DashTable table_;
  size_t expires_ = 0;  // Live keys that carry a TTL.
  uint64_t expire_cursor_ = 0;
  size_t expire_budget_ = kMinExpireBudget;
  Stats stats_;
};

}  // namespace kv
