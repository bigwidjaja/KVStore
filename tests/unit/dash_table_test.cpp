#include "core/dash_table.h"

#include <gtest/gtest.h>

#include <random>
#include <string>
#include <unordered_map>
#include <unordered_set>

namespace kv {
namespace {

std::string Key(size_t i) { return "key:" + std::to_string(i); }

DashTable::Iterator Insert(DashTable& t, const std::string& key, const std::string& value,
                           int64_t now = 0) {
  uint64_t h = DashTable::Hash(key);
  EXPECT_FALSE(t.Find(key, h));
  auto it = t.InsertNew(key, h, now, nullptr);
  it->value.Assign(value);
  return it;
}

TEST(DashTable, InsertFindErase) {
  DashTable t;
  Insert(t, "a", "1");
  Insert(t, "b", "2");
  EXPECT_EQ(t.size(), 2u);
  auto it = t.Find("a", DashTable::Hash("a"));
  ASSERT_TRUE(it);
  EXPECT_EQ(it->value.view(), "1");
  t.Erase(it, DashTable::Hash("a"));
  EXPECT_FALSE(t.Find("a", DashTable::Hash("a")));
  EXPECT_TRUE(t.Find("b", DashTable::Hash("b")));
  EXPECT_EQ(t.size(), 1u);
}

TEST(DashTable, GrowsBySplittingSegments) {
  DashTable t;
  const size_t n = 300000;
  for (size_t i = 0; i < n; ++i) Insert(t, Key(i), std::to_string(i));
  EXPECT_EQ(t.size(), n);
  EXPECT_GT(t.segment_count(), n / DashTable::kSegmentCapacity);
  EXPECT_EQ(t.split_count(), t.segment_count() - 1);
  for (size_t i = 0; i < n; ++i) {
    auto it = t.Find(Key(i), DashTable::Hash(Key(i)));
    ASSERT_TRUE(it) << i;
    ASSERT_EQ(it->value.view(), std::to_string(i));
  }
  // Load factor stays reasonable.
  double load = double(n) / double(t.segment_count() * DashTable::kSegmentCapacity);
  EXPECT_GT(load, 0.5);
}

TEST(DashTable, EraseHalfThenReinsert) {
  DashTable t;
  const size_t n = 100000;
  for (size_t i = 0; i < n; ++i) Insert(t, Key(i), "v");
  for (size_t i = 0; i < n; i += 2) {
    auto it = t.Find(Key(i), DashTable::Hash(Key(i)));
    ASSERT_TRUE(it);
    t.Erase(it, DashTable::Hash(Key(i)));
  }
  EXPECT_EQ(t.size(), n / 2);
  for (size_t i = 0; i < n; ++i) {
    EXPECT_EQ(bool(t.Find(Key(i), DashTable::Hash(Key(i)))), i % 2 == 1);
  }
  size_t segments = t.segment_count();
  for (size_t i = 0; i < n; i += 2) Insert(t, Key(i), "w");
  EXPECT_EQ(t.segment_count(), segments);  // Freed slots, including stash slots, get reused.
  EXPECT_EQ(t.size(), n);
}

TEST(DashTable, TraverseVisitsEveryEntryOnce) {
  DashTable t;
  const size_t n = 50000;
  for (size_t i = 0; i < n; ++i) Insert(t, Key(i), "v");
  std::unordered_set<std::string> seen;
  uint64_t cursor = 0;
  do {
    cursor = t.Traverse(cursor, [&](DashTable::Iterator it) {
      EXPECT_TRUE(seen.insert(std::string(it->key.view())).second);
    });
  } while (cursor != 0);
  EXPECT_EQ(seen.size(), n);
}

TEST(DashTable, TraverseCanErase) {
  DashTable t;
  for (size_t i = 0; i < 20000; ++i) Insert(t, Key(i), "v");
  uint64_t cursor = 0;
  do {
    cursor = t.Traverse(cursor, [&](DashTable::Iterator it) {
      t.Erase(it, DashTable::Hash(it->key.view()));
    });
  } while (cursor != 0);
  EXPECT_EQ(t.size(), 0u);
}

TEST(DashTable, CursorSurvivesSplits) {
  DashTable t;
  for (size_t i = 0; i < 5000; ++i) Insert(t, Key(i), "v");
  std::unordered_set<std::string> seen;
  uint64_t cursor = 0;
  size_t next = 5000;
  do {
    cursor = t.Traverse(cursor, [&](DashTable::Iterator it) { seen.insert(std::string(it->key.view())); });
    for (int j = 0; j < 30; ++j) Insert(t, Key(next++), "v");  // Splits happen mid-pass.
  } while (cursor != 0);
  // Every key that existed when the pass started was visited at least once.
  for (size_t i = 0; i < 5000; ++i) EXPECT_TRUE(seen.count(Key(i))) << i;
}

TEST(DashTable, CleansExpiredBeforeSplitting) {
  DashTable t;
  // Fill with keys that expire at t=100 until the table has split once (two segments).
  for (size_t i = 0; t.segment_count() < 2; ++i) {
    auto it = Insert(t, Key(i), "v", /*now=*/0);
    it->key.SetExpireAtMs(100);
  }
  // At t=1000 they are all expired. 1000 fresh keys fit in two segments (1680 slots) only if
  // full segments are swept instead of split.
  size_t removed = 0;
  for (size_t j = 0; j < 1000; ++j) {
    std::string k = "fresh:" + std::to_string(j);
    t.InsertNew(k, DashTable::Hash(k), 1000, &removed);
  }
  EXPECT_GT(removed, 0u);
  EXPECT_EQ(t.segment_count(), 2u);
  for (size_t j = 0; j < 1000; ++j) {
    std::string k = "fresh:" + std::to_string(j);
    EXPECT_TRUE(t.Find(k, DashTable::Hash(k)));
  }
}

TEST(DashTable, LongAndShortKeys) {
  DashTable t;
  std::unordered_map<std::string, std::string> model;
  std::mt19937_64 rng(7);
  for (int i = 0; i < 20000; ++i) {
    std::string k(rng() % 64, 'x');
    for (char& c : k) c = static_cast<char>('a' + rng() % 26);
    if (model.count(k)) continue;
    std::string v(rng() % 40, 'v');
    model[k] = v;
    Insert(t, k, v);
  }
  for (auto& [k, v] : model) {
    auto it = t.Find(k, DashTable::Hash(k));
    ASSERT_TRUE(it);
    EXPECT_EQ(it->value.view(), v);
  }
}

}  // namespace
}  // namespace kv
