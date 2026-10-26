// Keyspace commands: DEL, EXISTS, TYPE, the EXPIRE and TTL families, DBSIZE, FLUSHALL.

#include <limits>

#include "base/strings.h"
#include "server/command_table.h"
#include "shard/shard.h"

namespace kv {
namespace {

// DEL, UNLINK and EXISTS run one op per key and the coordinator sums the results.
// Note: across shards these are not atomic, unlike Redis (see docs/DESIGN.md).
void DelPart(OpContext& c) { c.integer = c.shard.db().Delete(c.args[1], c.now_ms) ? 1 : 0; }

void ExistsPart(OpContext& c) { c.integer = c.shard.db().Find(c.args[1], c.now_ms) ? 1 : 0; }

void Type(OpContext& c) {
  c.out.SimpleString(c.shard.db().Find(c.args[1], c.now_ms) ? "string" : "none");
}

enum ExpireFlags : unsigned { kNX = 1, kXX = 2, kGT = 4, kLT = 8 };

// EXPIRE / PEXPIRE / EXPIREAT / PEXPIREAT key time [NX | XX | GT | LT]
// Follows Redis 7's expireGenericCommand, including the order of its checks: options first,
// then the integer, then overflow, then the key.
void ExpireGeneric(OpContext& c, bool seconds, bool relative, std::string_view name) {
  unsigned flags = 0;
  for (size_t j = 3; j < c.args.size(); ++j) {
    std::string_view opt = c.args[j];
    if (EqualsIgnoreCase(opt, "nx")) {
      flags |= kNX;
    } else if (EqualsIgnoreCase(opt, "xx")) {
      flags |= kXX;
    } else if (EqualsIgnoreCase(opt, "gt")) {
      flags |= kGT;
    } else if (EqualsIgnoreCase(opt, "lt")) {
      flags |= kLT;
    } else {
      return c.out.Error("ERR Unsupported option " + std::string(opt));
    }
  }
  if ((flags & kNX) && (flags & (kXX | kGT | kLT))) {
    return c.out.Error("ERR NX and XX, GT or LT options at the same time are not compatible");
  }
  if ((flags & kGT) && (flags & kLT)) {
    return c.out.Error("ERR GT and LT options at the same time are not compatible");
  }

  int64_t when;
  if (!ParseInt64(c.args[2], &when)) return c.out.Error(kNotIntegerErr);
  if (seconds) {
    if (when > std::numeric_limits<int64_t>::max() / 1000 ||
        when < std::numeric_limits<int64_t>::min() / 1000) {
      return c.out.Error(InvalidExpireError(name));
    }
    when *= 1000;
  }
  int64_t base = relative ? c.now_ms : 0;
  if (when > std::numeric_limits<int64_t>::max() - base) return c.out.Error(InvalidExpireError(name));
  when += base;

  DbSlice& db = c.shard.db();
  auto it = db.Find(c.args[1], c.now_ms);
  if (!it) return c.out.Integer(0);

  int64_t current = it->key.ExpireAtMs();  // 0 means no TTL ("infinite").
  if ((flags & kNX) && current != 0) return c.out.Integer(0);
  if ((flags & kXX) && current == 0) return c.out.Integer(0);
  if ((flags & kGT) && (current == 0 || when <= current)) return c.out.Integer(0);
  if ((flags & kLT) && current != 0 && when >= current) return c.out.Integer(0);

  if (when <= c.now_ms) {
    db.Delete(it);  // Already in the past: Redis deletes the key right away.
  } else {
    db.SetExpire(it, when);
  }
  c.out.Integer(1);
}

void Expire(OpContext& c) { ExpireGeneric(c, true, true, "expire"); }
void PExpire(OpContext& c) { ExpireGeneric(c, false, true, "pexpire"); }
void ExpireAt(OpContext& c) { ExpireGeneric(c, true, false, "expireat"); }
void PExpireAt(OpContext& c) { ExpireGeneric(c, false, false, "pexpireat"); }

// TTL and PTTL: -2 for a missing key, -1 for no TTL. TTL rounds to the nearest second.
void TtlGeneric(OpContext& c, bool ms) {
  auto it = c.shard.db().Find(c.args[1], c.now_ms);
  if (!it) return c.out.Integer(-2);
  int64_t at = it->key.ExpireAtMs();
  if (at == 0) return c.out.Integer(-1);
  int64_t ttl = std::max<int64_t>(at - c.now_ms, 0);
  c.out.Integer(ms ? ttl : (ttl + 500) / 1000);
}

void Ttl(OpContext& c) { TtlGeneric(c, false); }
void PTtl(OpContext& c) { TtlGeneric(c, true); }

void ExpireTimeGeneric(OpContext& c, bool ms) {
  auto it = c.shard.db().Find(c.args[1], c.now_ms);
  if (!it) return c.out.Integer(-2);
  int64_t at = it->key.ExpireAtMs();
  if (at == 0) return c.out.Integer(-1);
  c.out.Integer(ms ? at : at / 1000);
}

void ExpireTime(OpContext& c) { ExpireTimeGeneric(c, false); }
void PExpireTime(OpContext& c) { ExpireTimeGeneric(c, true); }

void Persist(OpContext& c) {
  DbSlice& db = c.shard.db();
  auto it = db.Find(c.args[1], c.now_ms);
  if (!it || !it->key.HasExpiry()) return c.out.Integer(0);
  db.SetExpire(it, 0);
  c.out.Integer(1);
}

void DbSizePart(OpContext& c) { c.integer = static_cast<int64_t>(c.shard.db().size()); }

void FlushPart(OpContext& c) { c.shard.db().Clear(); }

std::string CheckFlush(std::span<const std::string_view> args) {
  if (args.size() == 1) return {};
  if (args.size() == 2 && (EqualsIgnoreCase(args[1], "sync") || EqualsIgnoreCase(args[1], "async"))) {
    return {};
  }
  return std::string(kSyntaxErr);
}

}  // namespace

void RegisterGenericFamily(CommandTable* t) {
  t->Add({.name = "del", .arity = -2, .route = Route::kPerKey, .combine = Combine::kSum, .shard_fn = DelPart});
  t->Add({.name = "unlink", .arity = -2, .route = Route::kPerKey, .combine = Combine::kSum, .shard_fn = DelPart});
  t->Add({.name = "exists", .arity = -2, .route = Route::kPerKey, .combine = Combine::kSum, .shard_fn = ExistsPart});
  t->Add({.name = "type", .arity = 2, .route = Route::kSingleKey, .shard_fn = Type});
  t->Add({.name = "expire", .arity = -3, .route = Route::kSingleKey, .shard_fn = Expire});
  t->Add({.name = "pexpire", .arity = -3, .route = Route::kSingleKey, .shard_fn = PExpire});
  t->Add({.name = "expireat", .arity = -3, .route = Route::kSingleKey, .shard_fn = ExpireAt});
  t->Add({.name = "pexpireat", .arity = -3, .route = Route::kSingleKey, .shard_fn = PExpireAt});
  t->Add({.name = "ttl", .arity = 2, .route = Route::kSingleKey, .shard_fn = Ttl});
  t->Add({.name = "pttl", .arity = 2, .route = Route::kSingleKey, .shard_fn = PTtl});
  t->Add({.name = "expiretime", .arity = 2, .route = Route::kSingleKey, .shard_fn = ExpireTime});
  t->Add({.name = "pexpiretime", .arity = 2, .route = Route::kSingleKey, .shard_fn = PExpireTime});
  t->Add({.name = "persist", .arity = 2, .route = Route::kSingleKey, .shard_fn = Persist});
  t->Add({.name = "dbsize", .arity = 1, .route = Route::kAllShards, .combine = Combine::kSum, .shard_fn = DbSizePart});
  t->Add({.name = "flushall", .arity = -1, .route = Route::kAllShards, .combine = Combine::kOk, .shard_fn = FlushPart, .check = CheckFlush});
  t->Add({.name = "flushdb", .arity = -1, .route = Route::kAllShards, .combine = Combine::kOk, .shard_fn = FlushPart, .check = CheckFlush});
}

}  // namespace kv
