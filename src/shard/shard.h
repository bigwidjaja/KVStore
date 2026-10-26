#pragma once

#include <cstdint>

#include "core/db_slice.h"

namespace kv {

// One shard of the key space, owned outright by one thread.
//
// Design after Dragonfly (DC-01): EngineShard in src/server/engine_shard.cc. Each shard is
// touched only by its owning thread; other threads reach it by sending that thread a message.
// There is no lock anywhere on this path.
class Shard {
 public:
  explicit Shard(uint32_t id) : id_(id) {}

  uint32_t id() const { return id_; }
  DbSlice& db() { return db_; }
  const DbSlice& db() const { return db_; }

  // The shard owned by the calling thread, or nullptr on threads that own none.
  static Shard* Local() { return tlocal_; }
  static void SetLocal(Shard* shard) { tlocal_ = shard; }

 private:
  static inline thread_local Shard* tlocal_ = nullptr;

  uint32_t id_;
  DbSlice db_;
};

}  // namespace kv
