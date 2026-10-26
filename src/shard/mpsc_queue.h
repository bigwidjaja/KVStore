#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <new>
#include <utility>

#include "base/log.h"

namespace kv {

// Bounded lock-free queue with many producers and a single consumer.
//
// Design after Dragonfly (DC-05): helio/base/mpmc_bounded_queue.h, which is Dmitry Vyukov's
// bounded MPMC queue (1024cores.net). Each cell carries a sequence number: a producer claims
// a position with one CAS on the tail, writes the item, then publishes it by storing
// seq = pos + 1. The consumer takes the cell at the head once seq says it is published and
// recycles it by storing seq = pos + capacity.
//
// With one consumer the dequeue side needs no CAS, so the head is a plain counter. Head, tail
// and every cell sit on separate cache lines so producers and the consumer don't false-share.
// Items from one producer come out in the order that producer pushed them.
template <typename T>
class MpscQueue {
 public:
  explicit MpscQueue(size_t capacity) : mask_(capacity - 1), cells_(new Cell[capacity]) {
    KV_CHECK(capacity >= 2 && (capacity & (capacity - 1)) == 0);
    for (size_t i = 0; i < capacity; ++i) cells_[i].seq.store(i, std::memory_order_relaxed);
  }

  ~MpscQueue() {
    T item;
    while (TryPop(item)) {
    }
  }

  MpscQueue(const MpscQueue&) = delete;
  MpscQueue& operator=(const MpscQueue&) = delete;

  size_t capacity() const { return mask_ + 1; }

  // Any thread. Moves from `item` only when it returns true; when the queue is full the item
  // is left untouched so the caller can keep it and retry.
  bool TryPush(T& item) {
    Cell* cell;
    size_t pos = tail_.load(std::memory_order_relaxed);
    while (true) {
      cell = &cells_[pos & mask_];
      size_t seq = cell->seq.load(std::memory_order_acquire);
      intptr_t diff = static_cast<intptr_t>(seq) - static_cast<intptr_t>(pos);
      if (diff == 0) {
        if (tail_.compare_exchange_weak(pos, pos + 1, std::memory_order_relaxed)) break;
      } else if (diff < 0) {
        return false;  // Full: the cell still holds an item from one lap ago.
      } else {
        pos = tail_.load(std::memory_order_relaxed);
      }
    }
    new (cell->storage) T(std::move(item));
    cell->seq.store(pos + 1, std::memory_order_release);
    return true;
  }

  // Consumer thread only.
  bool TryPop(T& out) {
    Cell& cell = cells_[head_ & mask_];
    size_t seq = cell.seq.load(std::memory_order_acquire);
    if (static_cast<intptr_t>(seq) - static_cast<intptr_t>(head_ + 1) < 0) return false;
    T* item = std::launder(reinterpret_cast<T*>(cell.storage));
    out = std::move(*item);
    item->~T();
    cell.seq.store(head_ + mask_ + 1, std::memory_order_release);
    ++head_;
    return true;
  }

  // Consumer thread only. False positives are impossible; a push racing with this call may
  // be missed, which the wakeup protocol accounts for.
  bool Empty() const {
    const Cell& cell = cells_[head_ & mask_];
    size_t seq = cell.seq.load(std::memory_order_acquire);
    return static_cast<intptr_t>(seq) - static_cast<intptr_t>(head_ + 1) < 0;
  }

 private:
  static constexpr size_t kCacheLine = 64;

  struct alignas(kCacheLine) Cell {
    std::atomic<size_t> seq;
    alignas(T) unsigned char storage[sizeof(T)];
  };

  const size_t mask_;
  std::unique_ptr<Cell[]> cells_;
  alignas(kCacheLine) std::atomic<size_t> tail_{0};  // Shared by producers.
  alignas(kCacheLine) size_t head_ = 0;              // Owned by the consumer.
};

}  // namespace kv
