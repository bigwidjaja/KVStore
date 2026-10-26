#pragma once

#include <cstdint>
#include <cstring>
#include <string_view>

namespace kv {

// An 18-byte string handle: up to 16 bytes stored inline, longer strings in a heap block that
// starts with a 4-byte length.
//
// Design after Dragonfly (DC-17): CompactObj and CompactKey in src/core/compact_object.h.
// The second half of the payload can hold an "extra" 64-bit word instead of string bytes.
// CompactKey uses it for the expiry time, so a key with a TTL needs no separate expire table:
// keys up to 8 bytes keep their bytes inline next to the TTL, longer ones move to the heap.
//
//   no extra, inline   [ bytes (<=16)          ] len  flags
//   no extra, heap     [ ptr | unused          ] 0xFF flags
//   extra, inline      [ bytes (<=8) | extra   ] len  flags|kHasExtra
//   extra, heap        [ ptr         | extra   ] 0xFF flags|kHasExtra
//
// Alignment is 1 so the table can pack entries tightly; pointers and the extra word are read
// and written with memcpy.
class CompactStr {
 public:
  static constexpr size_t kInlineCap = 16;
  static constexpr size_t kInlineCapWithExtra = 8;

  CompactStr() = default;
  ~CompactStr() { Reset(); }

  CompactStr(const CompactStr&) = delete;
  CompactStr& operator=(const CompactStr&) = delete;

  // A move is a byte copy that hands over the heap block.
  CompactStr(CompactStr&& other) noexcept {
    std::memcpy(static_cast<void*>(this), &other, sizeof(CompactStr));
    other.MarkEmpty();
  }
  CompactStr& operator=(CompactStr&& other) noexcept {
    if (this != &other) {
      Reset();
      std::memcpy(static_cast<void*>(this), &other, sizeof(CompactStr));
      other.MarkEmpty();
    }
    return *this;
  }

  std::string_view view() const {
    if (len_ != kHeapTag) return {payload_, len_};
    const char* block = HeapBlock();
    uint32_t len;
    std::memcpy(&len, block, sizeof(len));
    return {block + sizeof(uint32_t), len};
  }

  size_t size() const { return view().size(); }
  bool empty() const { return len_ == 0; }
  bool IsInline() const { return len_ != kHeapTag; }
  bool Equals(std::string_view s) const { return view() == s; }

  // Replaces the string. Keeps the extra word, if any.
  void Assign(std::string_view s);

  // Frees the string and clears the extra word.
  void Reset();

  // Bytes held in the heap block, 0 when inline.
  size_t HeapBytes() const { return len_ == kHeapTag ? sizeof(uint32_t) + size() : 0; }

 protected:
  bool has_extra() const { return flags_ & kHasExtra; }
  int64_t extra() const {
    int64_t v;
    std::memcpy(&v, payload_ + 8, sizeof(v));
    return v;
  }
  // Switches to the layout with an extra word; may move the string to the heap.
  void SetExtra(int64_t v);
  // Drops the extra word; may move the string back inline.
  void ClearExtra();

 private:
  static constexpr uint8_t kHeapTag = 0xFF;
  static constexpr uint8_t kHasExtra = 1;

  size_t InlineCap() const { return has_extra() ? kInlineCapWithExtra : kInlineCap; }
  char* HeapBlock() const {
    char* p;
    std::memcpy(&p, payload_, sizeof(p));
    return p;
  }
  void SetHeapBlock(char* p) { std::memcpy(payload_, &p, sizeof(p)); }
  void MarkEmpty() {
    len_ = 0;
    flags_ = 0;
  }
  // Stores `s` in the current layout. The previous string must already be released.
  void Store(std::string_view s);

  char payload_[16];
  uint8_t len_ = 0;
  uint8_t flags_ = 0;
};

static_assert(sizeof(CompactStr) == 18);
static_assert(alignof(CompactStr) == 1);

// A key with an optional absolute expiry time in Unix milliseconds.
class CompactKey : public CompactStr {
 public:
  bool HasExpiry() const { return has_extra(); }
  // 0 when the key has no TTL.
  int64_t ExpireAtMs() const { return has_extra() ? extra() : 0; }
  void SetExpireAtMs(int64_t at_ms) {
    if (at_ms == 0) {
      ClearExtra();
    } else {
      SetExtra(at_ms);
    }
  }
  bool IsExpired(int64_t now_ms) const { return has_extra() && extra() <= now_ms; }
};

static_assert(sizeof(CompactKey) == 18);

using CompactValue = CompactStr;

}  // namespace kv
