#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <new>
#include <string_view>
#include <utility>
#include <vector>

#include "core/compact_string.h"

namespace kv {

// A segmented open-addressing hash table from string keys to string values.
//
// Design after Dragonfly (DC-16): docs/dashtable.md and src/core/dash.h, which build on the
// Dash paper. A directory of 2^global_depth pointers maps the top bits of a key's hash to a
// fixed-size segment (extendible hashing). A segment has 56 regular buckets and 4 stash
// buckets of 14 slots each, 840 entries in all. An entry lives in its home bucket, the
// neighbor bucket, or a stash bucket. When a segment is full, only that segment splits, so
// growing never rehashes the whole table and there is no Redis-style 2N rehash spike.
//
// Our additions: a 1-byte fingerprint per slot so most probes never touch the key (as in
// Swiss tables), and a per-bucket count of entries that overflowed into the stash so lookups
// skip the stash when it can't hold the key. Splits move each entry to the same bucket and
// slot in the new segment, which can never fail.
//
// Before splitting a full segment, entries whose TTL has passed are removed (DC-18), which
// sometimes makes the split unnecessary.
class DashTable {
 public:
  static constexpr unsigned kSlotsPerBucket = 14;
  static constexpr unsigned kRegularBuckets = 56;
  static constexpr unsigned kStashBuckets = 4;
  static constexpr unsigned kTotalBuckets = kRegularBuckets + kStashBuckets;
  static constexpr unsigned kSegmentCapacity = kTotalBuckets * kSlotsPerBucket;

  struct Entry {
    CompactKey key;
    CompactValue value;
  };

 private:
  struct Bucket {
    uint16_t busy = 0;     // Bit i is set when slot i holds an entry.
    uint8_t overflow = 0;  // Regular buckets: entries homed here that live in the stash.
    uint8_t fp[kSlotsPerBucket];
    alignas(Entry) unsigned char storage[kSlotsPerBucket * sizeof(Entry)];

    Entry* entry(unsigned slot) {
      return std::launder(reinterpret_cast<Entry*>(storage + slot * sizeof(Entry)));
    }
    void* raw(unsigned slot) { return storage + slot * sizeof(Entry); }
  };

  struct Segment {
    explicit Segment(uint8_t depth) : local_depth(depth) {}
    uint8_t local_depth;
    uint32_t size = 0;
    Bucket buckets[kTotalBuckets];
  };

 public:
  // Points at one entry. Invalidated by any insert (a split may move entries) or erase.
  class Iterator {
   public:
    Iterator() = default;
    Entry* operator->() const { return entry_; }
    Entry& operator*() const { return *entry_; }
    explicit operator bool() const { return entry_ != nullptr; }

   private:
    friend class DashTable;
    Iterator(Entry* e, Segment* seg, uint8_t bucket, uint8_t slot)
        : entry_(e), seg_(seg), bucket_(bucket), slot_(slot) {}

    Entry* entry_ = nullptr;
    Segment* seg_ = nullptr;
    uint8_t bucket_ = 0;
    uint8_t slot_ = 0;
  };

  DashTable();
  ~DashTable();
  DashTable(const DashTable&) = delete;
  DashTable& operator=(const DashTable&) = delete;

  static uint64_t Hash(std::string_view key);

  Iterator Find(std::string_view key, uint64_t hash) const;

  // Inserts `key` with an empty value. The key must not be present. If the target segment is
  // full, expired entries in it are removed first (counted in *expired_removed), then the
  // segment splits if still needed.
  Iterator InsertNew(std::string_view key, uint64_t hash, int64_t now_ms, size_t* expired_removed);

  // Erases the entry. `hash` must be Hash(key) of that entry.
  void Erase(Iterator it, uint64_t hash);

  void Clear();

  size_t size() const { return size_; }
  size_t segment_count() const { return segments_.size(); }
  unsigned global_depth() const { return global_depth_; }
  size_t split_count() const { return splits_; }
  // Memory held by segments and the directory; excludes heap blocks of long strings.
  size_t TableBytes() const;

  // Visits every entry of one bucket, starting a pass at cursor 0. Returns the cursor for the
  // next call, or 0 once a full pass is done. `fn(Iterator)` may erase the entry it is given.
  // The cursor stays meaningful across splits (it encodes a hash-space position, not a
  // directory index), though entries can be visited twice or missed in a pass that races a
  // split. That is fine for active expiry.
  template <typename Fn>
  uint64_t Traverse(uint64_t cursor, Fn&& fn);

 private:
  size_t DirIndex(uint64_t hash) const {
    return global_depth_ == 0 ? 0 : static_cast<size_t>(hash >> (64 - global_depth_));
  }
  static unsigned HomeBucket(uint64_t hash) {
    return static_cast<unsigned>((hash >> 8) % kRegularBuckets);
  }
  static uint8_t Fingerprint(uint64_t hash) { return static_cast<uint8_t>(hash); }

  static int FindInBucket(Bucket& b, uint8_t fp, std::string_view key);
  static int FreeSlot(const Bucket& b);
  // Places a new entry in segment `seg`, or returns a null iterator if the segment is full.
  static Iterator TryPlace(Segment* seg, std::string_view key, uint64_t hash);
  static void DestroyEntry(Segment* seg, unsigned bucket, unsigned slot);
  size_t RemoveExpired(Segment* seg, int64_t now_ms);
  void Split(Segment* seg, uint64_t hash);

  std::vector<Segment*> dir_;
  std::vector<std::unique_ptr<Segment>> segments_;
  unsigned global_depth_ = 0;
  size_t size_ = 0;
  size_t splits_ = 0;
};

template <typename Fn>
uint64_t DashTable::Traverse(uint64_t cursor, Fn&& fn) {
  // Cursor layout: bits [8, 40) are the top 32 bits of the hash space where the segment
  // starts; bits [0, 8) are the bucket inside it.
  uint32_t pos = static_cast<uint32_t>(cursor >> 8);
  unsigned bucket = static_cast<unsigned>(cursor & 0xFF);
  if (bucket >= kTotalBuckets) bucket = 0;

  size_t idx = global_depth_ == 0 ? 0 : static_cast<size_t>(pos >> (32 - global_depth_));
  Segment* seg = dir_[idx];
  Bucket& b = seg->buckets[bucket];
  uint32_t mask = b.busy;
  while (mask != 0) {
    unsigned slot = static_cast<unsigned>(__builtin_ctz(mask));
    mask &= mask - 1;
    fn(Iterator(b.entry(slot), seg, static_cast<uint8_t>(bucket), static_cast<uint8_t>(slot)));
  }

  if (++bucket < kTotalBuckets) return (static_cast<uint64_t>(pos) << 8) | bucket;

  // Move to the first directory index past the range this segment covers.
  size_t span = size_t{1} << (global_depth_ - seg->local_depth);
  size_t next = (idx & ~(span - 1)) + span;
  if (next >= dir_.size()) return 0;
  uint32_t next_pos = static_cast<uint32_t>(next << (32 - global_depth_));
  return static_cast<uint64_t>(next_pos) << 8;
}

}  // namespace kv
