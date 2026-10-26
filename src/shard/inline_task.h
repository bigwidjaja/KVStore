#pragma once

#include <cstddef>
#include <new>
#include <type_traits>
#include <utility>

namespace kv {

// A move-only `void()` callable stored inline, so passing a message between threads never
// allocates.
//
// Design after Dragonfly (DC-05): FiberQueue's Tasklet in
// helio/util/fibers/fiberqueue_threadpool.h keeps closures up to 32 bytes inline. Closures
// that don't fit are a compile error here rather than a silent heap allocation: capture a
// pointer (usually a std::unique_ptr to the payload) instead.
class InlineTask {
 public:
  static constexpr size_t kCapacity = 32;

  InlineTask() = default;

  template <typename F, typename Fn = std::decay_t<F>,
            typename = std::enable_if_t<!std::is_same_v<Fn, InlineTask>>>
  InlineTask(F&& f) {  // NOLINT: implicit on purpose, so lambdas convert.
    static_assert(sizeof(Fn) <= kCapacity, "closure too large for InlineTask");
    static_assert(alignof(Fn) <= alignof(std::max_align_t), "closure over-aligned");
    static_assert(std::is_nothrow_move_constructible_v<Fn>, "closure must be nothrow-movable");
    new (storage_) Fn(std::forward<F>(f));
    ops_ = &kOps<Fn>;
  }

  InlineTask(InlineTask&& other) noexcept { MoveFrom(other); }
  InlineTask& operator=(InlineTask&& other) noexcept {
    if (this != &other) {
      Reset();
      MoveFrom(other);
    }
    return *this;
  }
  InlineTask(const InlineTask&) = delete;
  InlineTask& operator=(const InlineTask&) = delete;

  ~InlineTask() { Reset(); }

  void operator()() { ops_->invoke(storage_); }
  explicit operator bool() const { return ops_ != nullptr; }

  void Reset() {
    if (ops_ != nullptr) {
      ops_->destroy(storage_);
      ops_ = nullptr;
    }
  }

 private:
  struct Ops {
    void (*invoke)(void*);
    void (*relocate)(void* dst, void* src);  // Move-construct into dst, destroy src.
    void (*destroy)(void*);
  };

  template <typename Fn>
  static constexpr Ops kOps = {
      [](void* p) { (*std::launder(static_cast<Fn*>(p)))(); },
      [](void* dst, void* src) {
        Fn* from = std::launder(static_cast<Fn*>(src));
        new (dst) Fn(std::move(*from));
        from->~Fn();
      },
      [](void* p) { std::launder(static_cast<Fn*>(p))->~Fn(); },
  };

  void MoveFrom(InlineTask& other) {
    if (other.ops_ != nullptr) {
      other.ops_->relocate(storage_, other.storage_);
      ops_ = other.ops_;
      other.ops_ = nullptr;
    }
  }

  alignas(std::max_align_t) unsigned char storage_[kCapacity];
  const Ops* ops_ = nullptr;
};

}  // namespace kv
