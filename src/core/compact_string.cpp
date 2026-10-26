#include "core/compact_string.h"

#include <cstdlib>
#include <new>

namespace kv {
namespace {

char* AllocBlock(std::string_view s) {
  auto* block = static_cast<char*>(std::malloc(sizeof(uint32_t) + s.size()));
  if (block == nullptr) throw std::bad_alloc();
  uint32_t len = static_cast<uint32_t>(s.size());
  std::memcpy(block, &len, sizeof(len));
  if (!s.empty()) std::memcpy(block + sizeof(uint32_t), s.data(), s.size());
  return block;
}

}  // namespace

void CompactStr::Store(std::string_view s) {
  if (s.size() <= InlineCap()) {
    if (!s.empty()) std::memcpy(payload_, s.data(), s.size());
    len_ = static_cast<uint8_t>(s.size());
  } else {
    SetHeapBlock(AllocBlock(s));
    len_ = kHeapTag;
  }
}

void CompactStr::Assign(std::string_view s) {
  char* old = len_ == kHeapTag ? HeapBlock() : nullptr;
  // `s` may point into our own storage, so store the new copy before freeing the old one.
  if (len_ != kHeapTag && s.data() >= payload_ && s.data() < payload_ + sizeof(payload_)) {
    char tmp[kInlineCap];
    std::memcpy(tmp, s.data(), s.size());
    Store(std::string_view(tmp, s.size()));
  } else {
    Store(s);
  }
  std::free(old);
}

void CompactStr::Reset() {
  if (len_ == kHeapTag) std::free(HeapBlock());
  MarkEmpty();
}

void CompactStr::SetExtra(int64_t v) {
  if (!has_extra() && len_ != kHeapTag && len_ > kInlineCapWithExtra) {
    // The bytes past offset 8 are about to hold the extra word: move the string to the heap.
    SetHeapBlock(AllocBlock(std::string_view(payload_, len_)));
    len_ = kHeapTag;
  }
  flags_ |= kHasExtra;
  std::memcpy(payload_ + 8, &v, sizeof(v));
}

void CompactStr::ClearExtra() {
  if (!has_extra()) return;
  flags_ &= static_cast<uint8_t>(~kHasExtra);
  if (len_ == kHeapTag && size() <= kInlineCap) {
    char* block = HeapBlock();
    std::string_view s = view();
    char tmp[kInlineCap];
    std::memcpy(tmp, s.data(), s.size());
    std::memcpy(payload_, tmp, s.size());
    len_ = static_cast<uint8_t>(s.size());
    std::free(block);
  }
}

}  // namespace kv
