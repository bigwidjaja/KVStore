#include "core/db_slice.h"

#include <algorithm>

namespace kv {

void DbSlice::EraseExpired(Iterator it, uint64_t hash) {
  --expires_;
  ++stats_.expired_keys;
  table_.Erase(it, hash);
}

DbSlice::Iterator DbSlice::Find(std::string_view key, int64_t now_ms) {
  uint64_t hash = DashTable::Hash(key);
  Iterator it = table_.Find(key, hash);
  if (it && it->key.IsExpired(now_ms)) {
    EraseExpired(it, hash);
    return Iterator();
  }
  return it;
}

DbSlice::Iterator DbSlice::FindRead(std::string_view key, int64_t now_ms) {
  Iterator it = Find(key, now_ms);
  if (it) {
    ++stats_.keyspace_hits;
  } else {
    ++stats_.keyspace_misses;
  }
  return it;
}

std::pair<DbSlice::Iterator, bool> DbSlice::FindOrInsert(std::string_view key, int64_t now_ms) {
  uint64_t hash = DashTable::Hash(key);
  Iterator it = table_.Find(key, hash);
  if (it) {
    if (!it->key.IsExpired(now_ms)) return {it, false};
    EraseExpired(it, hash);
  }
  size_t removed = 0;
  it = table_.InsertNew(key, hash, now_ms, &removed);
  expires_ -= removed;
  stats_.expired_keys += removed;
  return {it, true};
}

bool DbSlice::Delete(std::string_view key, int64_t now_ms) {
  uint64_t hash = DashTable::Hash(key);
  Iterator it = table_.Find(key, hash);
  if (!it) return false;
  if (it->key.IsExpired(now_ms)) {
    EraseExpired(it, hash);
    return false;
  }
  if (it->key.HasExpiry()) --expires_;
  table_.Erase(it, hash);
  return true;
}

void DbSlice::Delete(Iterator it) {
  if (it->key.HasExpiry()) --expires_;
  table_.Erase(it, DashTable::Hash(it->key.view()));
}

void DbSlice::SetExpire(Iterator it, int64_t at_ms) {
  bool had = it->key.HasExpiry();
  it->key.SetExpireAtMs(at_ms);
  bool has = at_ms != 0;
  if (had && !has) --expires_;
  if (!had && has) ++expires_;
}

void DbSlice::Clear() {
  table_.Clear();
  expires_ = 0;
  expire_cursor_ = 0;
  expire_budget_ = kMinExpireBudget;
}

DbSlice::ExpireStepStats DbSlice::DeleteExpiredStep(int64_t now_ms, size_t budget) {
  ExpireStepStats st;
  // Most buckets may hold only keys without a TTL, which cost a mask scan each. Bound the
  // buckets visited too, so one step never walks the whole table.
  size_t buckets_left = std::max<size_t>(budget, 1) * 4;
  do {
    expire_cursor_ = table_.Traverse(expire_cursor_, [&](Iterator it) {
      if (!it->key.HasExpiry()) return;
      ++st.examined;
      if (it->key.IsExpired(now_ms)) {
        EraseExpired(it, DashTable::Hash(it->key.view()));
        ++st.deleted;
      }
    });
  } while (expire_cursor_ != 0 && st.examined < budget && --buckets_left > 0);
  return st;
}

void DbSlice::Heartbeat(int64_t now_ms) {
  if (expires_ == 0) return;
  ExpireStepStats st = DeleteExpiredStep(now_ms, expire_budget_);
  if (st.examined == 0) return;
  // DC-18: EngineShard::Heartbeat scales the next step with the share of examined keys that
  // had expired, from a floor of 5 up to kTtlDeleteLimit (200).
  size_t next = kMaxExpireBudget * st.deleted / st.examined;
  expire_budget_ = std::clamp(next, kMinExpireBudget, kMaxExpireBudget);
}

}  // namespace kv
