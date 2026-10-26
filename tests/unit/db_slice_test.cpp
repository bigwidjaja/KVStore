#include "core/db_slice.h"

#include <gtest/gtest.h>

#include <string>

namespace kv {
namespace {

void Set(DbSlice& db, const std::string& key, const std::string& value, int64_t now,
         int64_t expire_at = 0) {
  auto it = db.FindOrInsert(key, now).first;
  it->value.Assign(value);
  db.SetExpire(it, expire_at);
}

TEST(DbSlice, LazyExpiry) {
  DbSlice db;
  Set(db, "k", "v", 1000, 2000);
  EXPECT_EQ(db.expires(), 1u);
  EXPECT_TRUE(db.Find("k", 1999));
  EXPECT_FALSE(db.Find("k", 2000));  // Deleted on lookup.
  EXPECT_EQ(db.size(), 0u);
  EXPECT_EQ(db.expires(), 0u);
  EXPECT_EQ(db.stats().expired_keys, 1u);
}

TEST(DbSlice, FindOrInsertReplacesExpired) {
  DbSlice db;
  Set(db, "k", "old", 0, 10);
  auto [it, inserted] = db.FindOrInsert("k", 20);
  EXPECT_TRUE(inserted);
  EXPECT_EQ(it->value.view(), "");
  EXPECT_FALSE(it->key.HasExpiry());
  EXPECT_EQ(db.expires(), 0u);
}

TEST(DbSlice, DeleteSkipsExpired) {
  DbSlice db;
  Set(db, "a", "v", 0, 10);
  Set(db, "b", "v", 0);
  EXPECT_FALSE(db.Delete("a", 50));  // Expired counts as missing.
  EXPECT_TRUE(db.Delete("b", 50));
  EXPECT_FALSE(db.Delete("b", 50));
  EXPECT_EQ(db.size(), 0u);
}

TEST(DbSlice, ExpiresCountTracksTtlChanges) {
  DbSlice db;
  Set(db, "k", "v", 0);
  auto it = db.Find("k", 0);
  db.SetExpire(it, 100);
  db.SetExpire(it, 200);
  EXPECT_EQ(db.expires(), 1u);
  db.SetExpire(it, 0);
  EXPECT_EQ(db.expires(), 0u);
}

TEST(DbSlice, ActiveExpiryRemovesEverythingEventually) {
  DbSlice db;
  for (int i = 0; i < 10000; ++i) Set(db, "k" + std::to_string(i), "v", 0, i % 2 ? 100 : 0);
  EXPECT_EQ(db.expires(), 5000u);
  int steps = 0;
  while (db.expires() > 0 && steps < 100000) {
    db.Heartbeat(1000);
    ++steps;
  }
  EXPECT_EQ(db.expires(), 0u);
  EXPECT_EQ(db.size(), 5000u);
  EXPECT_EQ(db.stats().expired_keys, 5000u);
}

TEST(DbSlice, ExpireBudgetAdapts) {
  DbSlice db;
  EXPECT_EQ(db.expire_budget(), DbSlice::kMinExpireBudget);
  // Every key with a TTL has expired: the budget climbs toward the ceiling.
  for (int i = 0; i < 50000; ++i) Set(db, "k" + std::to_string(i), "v", 0, 100);
  db.Heartbeat(1000);
  EXPECT_EQ(db.expire_budget(), DbSlice::kMaxExpireBudget);
  // None of the TTLs have passed: the budget drops back to the floor.
  DbSlice fresh;
  for (int i = 0; i < 50000; ++i) Set(fresh, "k" + std::to_string(i), "v", 0, 1000000);
  for (int i = 0; i < 5; ++i) fresh.Heartbeat(1000);
  EXPECT_EQ(fresh.expire_budget(), DbSlice::kMinExpireBudget);
}

TEST(DbSlice, StepRespectsBudget) {
  DbSlice db;
  for (int i = 0; i < 10000; ++i) Set(db, "k" + std::to_string(i), "v", 0, 100);
  DbSlice::ExpireStepStats st = db.DeleteExpiredStep(1000, 5);
  EXPECT_GE(st.examined, 5u);
  EXPECT_LT(st.examined, 5u + 14u * 2);  // Stops within a bucket or two of the budget.
  EXPECT_EQ(st.deleted, st.examined);
}

}  // namespace
}  // namespace kv
