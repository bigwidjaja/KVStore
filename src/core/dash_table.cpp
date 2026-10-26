#include "core/dash_table.h"

#define XXH_INLINE_ALL
#include <xxhash.h>

#include "base/log.h"

namespace kv {

DashTable::DashTable() {
  segments_.push_back(std::make_unique<Segment>(0));
  dir_.push_back(segments_.back().get());
}

DashTable::~DashTable() { Clear(); }

uint64_t DashTable::Hash(std::string_view key) {
  return XXH3_64bits(key.data(), key.size());
}

int DashTable::FindInBucket(Bucket& b, uint8_t fp, std::string_view key) {
  uint32_t mask = b.busy;
  while (mask != 0) {
    unsigned slot = static_cast<unsigned>(__builtin_ctz(mask));
    mask &= mask - 1;
    if (b.fp[slot] == fp && b.entry(slot)->key.Equals(key)) return static_cast<int>(slot);
  }
  return -1;
}

int DashTable::FreeSlot(const Bucket& b) {
  uint32_t free_mask = ~static_cast<uint32_t>(b.busy) & ((1u << kSlotsPerBucket) - 1);
  return free_mask == 0 ? -1 : __builtin_ctz(free_mask);
}

DashTable::Iterator DashTable::Find(std::string_view key, uint64_t hash) const {
  Segment* seg = dir_[DirIndex(hash)];
  uint8_t fp = Fingerprint(hash);
  unsigned home = HomeBucket(hash);
  unsigned neighbor = (home + 1) % kRegularBuckets;

  for (unsigned bi : {home, neighbor}) {
    int slot = FindInBucket(seg->buckets[bi], fp, key);
    if (slot >= 0) {
      return Iterator(seg->buckets[bi].entry(slot), seg, static_cast<uint8_t>(bi),
                      static_cast<uint8_t>(slot));
    }
  }
  if (seg->buckets[home].overflow != 0) {
    for (unsigned bi = kRegularBuckets; bi < kTotalBuckets; ++bi) {
      int slot = FindInBucket(seg->buckets[bi], fp, key);
      if (slot >= 0) {
        return Iterator(seg->buckets[bi].entry(slot), seg, static_cast<uint8_t>(bi),
                        static_cast<uint8_t>(slot));
      }
    }
  }
  return Iterator();
}

DashTable::Iterator DashTable::TryPlace(Segment* seg, std::string_view key, uint64_t hash) {
  unsigned home = HomeBucket(hash);
  unsigned neighbor = (home + 1) % kRegularBuckets;

  unsigned bucket = kTotalBuckets;
  int slot = FreeSlot(seg->buckets[home]);
  if (slot >= 0) {
    bucket = home;
  } else if ((slot = FreeSlot(seg->buckets[neighbor])) >= 0) {
    bucket = neighbor;
  } else {
    for (unsigned bi = kRegularBuckets; bi < kTotalBuckets; ++bi) {
      if ((slot = FreeSlot(seg->buckets[bi])) >= 0) {
        bucket = bi;
        uint8_t& overflow = seg->buckets[home].overflow;
        if (overflow != 0xFF) ++overflow;  // Saturates: then we always probe the stash.
        break;
      }
    }
  }
  if (bucket == kTotalBuckets) return Iterator();

  Bucket& b = seg->buckets[bucket];
  Entry* e = new (b.raw(static_cast<unsigned>(slot))) Entry();
  e->key.Assign(key);
  b.fp[slot] = Fingerprint(hash);
  b.busy |= static_cast<uint16_t>(1u << slot);
  ++seg->size;
  return Iterator(e, seg, static_cast<uint8_t>(bucket), static_cast<uint8_t>(slot));
}

void DashTable::DestroyEntry(Segment* seg, unsigned bucket, unsigned slot) {
  Bucket& b = seg->buckets[bucket];
  b.entry(slot)->~Entry();
  b.busy &= static_cast<uint16_t>(~(1u << slot));
  --seg->size;
}

void DashTable::Erase(Iterator it, uint64_t hash) {
  Segment* seg = it.seg_;
  if (it.bucket_ >= kRegularBuckets) {
    uint8_t& overflow = seg->buckets[HomeBucket(hash)].overflow;
    if (overflow != 0 && overflow != 0xFF) --overflow;
  }
  DestroyEntry(seg, it.bucket_, it.slot_);
  --size_;
}

size_t DashTable::RemoveExpired(Segment* seg, int64_t now_ms) {
  size_t removed = 0;
  for (unsigned bi = 0; bi < kTotalBuckets; ++bi) {
    Bucket& b = seg->buckets[bi];
    uint32_t mask = b.busy;
    while (mask != 0) {
      unsigned slot = static_cast<unsigned>(__builtin_ctz(mask));
      mask &= mask - 1;
      Entry* e = b.entry(slot);
      if (!e->key.IsExpired(now_ms)) continue;
      uint64_t h = Hash(e->key.view());
      Erase(Iterator(e, seg, static_cast<uint8_t>(bi), static_cast<uint8_t>(slot)), h);
      ++removed;
    }
  }
  return removed;
}

DashTable::Iterator DashTable::InsertNew(std::string_view key, uint64_t hash, int64_t now_ms,
                                         size_t* expired_removed) {
  bool cleaned = false;
  while (true) {
    Segment* seg = dir_[DirIndex(hash)];
    Iterator it = TryPlace(seg, key, hash);
    if (it) {
      ++size_;
      return it;
    }
    // DC-18: sweep the full segment for expired entries before paying for a split.
    if (!cleaned) {
      cleaned = true;
      size_t removed = RemoveExpired(seg, now_ms);
      if (expired_removed != nullptr) *expired_removed += removed;
      if (removed != 0) continue;
    }
    Split(seg, hash);
    cleaned = false;
  }
}

void DashTable::Split(Segment* seg, uint64_t hash) {
  const uint8_t depth = seg->local_depth;
  KV_CHECK(depth < 32);
  if (depth == global_depth_) {
    // Double the directory: entry i becomes entries 2i and 2i+1.
    std::vector<Segment*> bigger(dir_.size() * 2);
    for (size_t i = 0; i < dir_.size(); ++i) bigger[2 * i] = bigger[2 * i + 1] = dir_[i];
    dir_.swap(bigger);
    ++global_depth_;
  }

  segments_.push_back(std::make_unique<Segment>(depth + 1));
  Segment* fresh = segments_.back().get();
  seg->local_depth = depth + 1;

  // Entries whose next hash bit is 1 move to the new segment, to the same bucket and slot.
  // Their old placement was valid and bucket indexes are the same in both segments, so this
  // never fails, unlike reinserting them one by one.
  const unsigned bit = 63 - depth;
  for (unsigned bi = 0; bi < kTotalBuckets; ++bi) {
    Bucket& from = seg->buckets[bi];
    Bucket& to = fresh->buckets[bi];
    uint32_t mask = from.busy;
    while (mask != 0) {
      unsigned slot = static_cast<unsigned>(__builtin_ctz(mask));
      mask &= mask - 1;
      Entry* e = from.entry(slot);
      uint64_t h = Hash(e->key.view());
      if (((h >> bit) & 1) == 0) continue;

      new (to.raw(slot)) Entry(std::move(*e));
      to.fp[slot] = from.fp[slot];
      to.busy |= static_cast<uint16_t>(1u << slot);
      ++fresh->size;
      DestroyEntry(seg, bi, slot);

      if (bi >= kRegularBuckets) {
        unsigned home = HomeBucket(h);
        uint8_t& old_overflow = seg->buckets[home].overflow;
        if (old_overflow != 0 && old_overflow != 0xFF) --old_overflow;
        uint8_t& new_overflow = fresh->buckets[home].overflow;
        if (new_overflow != 0xFF) ++new_overflow;
      }
    }
  }

  // The segment covers a block of 2^(global - depth) directory entries; the upper half of
  // that block now points at the new segment.
  size_t idx = DirIndex(hash);
  size_t span = size_t{1} << (global_depth_ - depth);
  size_t start = idx & ~(span - 1);
  for (size_t i = start + span / 2; i < start + span; ++i) dir_[i] = fresh;
  ++splits_;
}

void DashTable::Clear() {
  for (auto& seg : segments_) {
    for (unsigned bi = 0; bi < kTotalBuckets; ++bi) {
      Bucket& b = seg->buckets[bi];
      uint32_t mask = b.busy;
      while (mask != 0) {
        unsigned slot = static_cast<unsigned>(__builtin_ctz(mask));
        mask &= mask - 1;
        b.entry(slot)->~Entry();
      }
    }
  }
  segments_.clear();
  dir_.clear();
  segments_.push_back(std::make_unique<Segment>(0));
  dir_.push_back(segments_.back().get());
  global_depth_ = 0;
  size_ = 0;
}

size_t DashTable::TableBytes() const {
  return segments_.size() * sizeof(Segment) + dir_.capacity() * sizeof(Segment*);
}

}  // namespace kv
