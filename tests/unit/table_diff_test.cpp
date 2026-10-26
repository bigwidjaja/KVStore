// Phase 3, step 8: random insert, lookup, erase and expire operations against DbSlice, checked
// against std::unordered_map as the reference model. 10M operations by default; set
// KV_DIFF_OPS to change that.

#include <gtest/gtest.h>

#include <cstdlib>
#include <random>
#include <string>
#include <unordered_map>
#include <vector>

#include "core/db_slice.h"

namespace kv {
namespace {

struct ModelEntry {
  std::string value;
  int64_t expire_at = 0;  // 0: no TTL.
};

size_t OpsFromEnv() {
  if (const char* env = std::getenv("KV_DIFF_OPS")) return std::strtoull(env, nullptr, 10);
  return 10'000'000;
}

class Model {
 public:
  const ModelEntry* Find(const std::string& key, int64_t now) {
    auto it = map_.find(key);
    if (it == map_.end()) return nullptr;
    if (it->second.expire_at != 0 && it->second.expire_at <= now) {
      map_.erase(it);
      return nullptr;
    }
    return &it->second;
  }
  ModelEntry& Upsert(const std::string& key, int64_t now) {
    Find(key, now);
    return map_[key];
  }
  bool Erase(const std::string& key, int64_t now) {
    if (!Find(key, now)) return false;
    map_.erase(key);
    return true;
  }
  size_t LiveCount(int64_t now) const {
    size_t n = 0;
    for (auto& [k, e] : map_) n += e.expire_at == 0 || e.expire_at > now;
    return n;
  }
  const std::unordered_map<std::string, ModelEntry>& map() const { return map_; }

 private:
  std::unordered_map<std::string, ModelEntry> map_;
};

std::string RandomKey(std::mt19937_64& rng, const std::vector<std::string>& pool) {
  return pool[rng() % pool.size()];
}

TEST(TableDiff, RandomOpsMatchModel) {
  const size_t ops = OpsFromEnv();
  std::mt19937_64 rng(12345);

  // Keys of every compact form: <=8 bytes, 9..16 bytes, and heap-sized.
  std::vector<std::string> pool;
  for (size_t i = 0; i < 200000; ++i) {
    std::string k;
    switch (i % 3) {
      case 0: k = std::to_string(i); break;
      case 1: k = "user:" + std::to_string(i * 7); break;
      default: k = "session:abcdefghijklmnop:" + std::to_string(i); break;
    }
    pool.push_back(std::move(k));
  }

  DbSlice db;
  Model model;
  int64_t now = 1'000'000;

  for (size_t op = 0; op < ops; ++op) {
    if (op % 1000 == 0) now += static_cast<int64_t>(rng() % 50);  // Time moves forward.
    if (op % 97 == 0) db.Heartbeat(now);                       // Active expiry in between.

    std::string key = RandomKey(rng, pool);
    unsigned kind = static_cast<unsigned>(rng() % 100);
    if (kind < 35) {  // SET
      std::string value(rng() % 32, static_cast<char>('a' + rng() % 26));
      auto it = db.FindOrInsert(key, now).first;
      it->value.Assign(value);
      db.SetExpire(it, 0);
      ModelEntry& m = model.Upsert(key, now);
      m.value = value;
      m.expire_at = 0;
    } else if (kind < 70) {  // GET
      auto it = db.Find(key, now);
      const ModelEntry* m = model.Find(key, now);
      ASSERT_EQ(bool(it), m != nullptr) << "op " << op << " key " << key;
      if (m) {
        ASSERT_EQ(it->value.view(), m->value) << "op " << op;
      }
    } else if (kind < 82) {  // DEL
      ASSERT_EQ(db.Delete(key, now), model.Erase(key, now)) << "op " << op;
    } else if (kind < 95) {  // EXPIRE: mostly short TTLs so keys really do expire.
      int64_t at = now + 1 + static_cast<int64_t>(rng() % 2000);
      auto it = db.Find(key, now);
      const ModelEntry* m = model.Find(key, now);
      ASSERT_EQ(bool(it), m != nullptr) << "op " << op;
      if (it) {
        db.SetExpire(it, at);
        model.Upsert(key, now).expire_at = at;
        ASSERT_EQ(it->key.ExpireAtMs(), at);
      }
    } else {  // PERSIST
      auto it = db.Find(key, now);
      const ModelEntry* m = model.Find(key, now);
      ASSERT_EQ(bool(it), m != nullptr) << "op " << op;
      if (it) {
        ASSERT_EQ(it->key.ExpireAtMs(), m->expire_at);
        db.SetExpire(it, 0);
        model.Upsert(key, now).expire_at = 0;
      }
    }
  }

  // Full comparison: every live model key is in the table, and the live counts agree.
  size_t live_in_table = 0;
  uint64_t cursor = 0;
  const DashTable& table = db.table();
  do {
    cursor = const_cast<DashTable&>(table).Traverse(cursor, [&](DashTable::Iterator it) {
      if (!it->key.IsExpired(now)) ++live_in_table;
    });
  } while (cursor != 0);
  EXPECT_EQ(live_in_table, model.LiveCount(now));
  for (const auto& [k, e] : model.map()) {
    if (e.expire_at != 0 && e.expire_at <= now) continue;
    auto it = db.Find(k, now);
    ASSERT_TRUE(it) << k;
    EXPECT_EQ(it->value.view(), e.value);
    EXPECT_EQ(it->key.ExpireAtMs(), e.expire_at);
  }
  // The TTL counter must not drift.
  size_t with_ttl = 0;
  cursor = 0;
  do {
    cursor = const_cast<DashTable&>(table).Traverse(cursor, [&](DashTable::Iterator it) {
      with_ttl += it->key.HasExpiry();
    });
  } while (cursor != 0);
  EXPECT_EQ(with_ttl, db.expires());
  RecordProperty("ops", std::to_string(ops));
  RecordProperty("segments", std::to_string(table.segment_count()));
}

}  // namespace
}  // namespace kv
