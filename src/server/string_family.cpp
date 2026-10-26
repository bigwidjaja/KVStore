// String commands. Every handler here runs on the shard that owns its key.

#include <charconv>
#include <limits>
#include <string>

#include "base/strings.h"
#include "server/command_table.h"
#include "server/family_util.h"
#include "shard/shard.h"

namespace kv {
namespace {

void Get(OpContext& c) {
  auto it = c.shard.db().FindRead(c.args[1], c.now_ms);
  if (!it) return c.out.NullBulk();
  c.out.Bulk(it->value.view());
}

// SET key value [NX | XX] [GET] [EX seconds | PX ms | EXAT ts | PXAT ts-ms | KEEPTTL]
// Option parsing follows Redis' parseExtendedStringArgumentsOrReply.
void Set(OpContext& c) {
  enum : unsigned {
    kNX = 1,
    kXX = 2,
    kGet = 4,
    kKeepTtl = 8,
    kEX = 16,
    kPX = 32,
    kEXAT = 64,
    kPXAT = 128,
  };
  constexpr unsigned kAnyExpire = kEX | kPX | kEXAT | kPXAT;

  unsigned flags = 0;
  std::string_view expire_arg;
  for (size_t j = 3; j < c.args.size(); ++j) {
    std::string_view opt = c.args[j];
    bool has_next = j + 1 < c.args.size();
    auto expire_opt = [&](unsigned flag) {
      if ((flags & kKeepTtl) || (flags & (kAnyExpire & ~flag)) || !has_next) return false;
      flags |= flag;
      expire_arg = c.args[++j];
      return true;
    };
    if (EqualsIgnoreCase(opt, "nx") && !(flags & kXX)) {
      flags |= kNX;
    } else if (EqualsIgnoreCase(opt, "xx") && !(flags & kNX)) {
      flags |= kXX;
    } else if (EqualsIgnoreCase(opt, "get")) {
      flags |= kGet;
    } else if (EqualsIgnoreCase(opt, "keepttl") && !(flags & kAnyExpire)) {
      flags |= kKeepTtl;
    } else if (EqualsIgnoreCase(opt, "ex") && expire_opt(kEX)) {
    } else if (EqualsIgnoreCase(opt, "px") && expire_opt(kPX)) {
    } else if (EqualsIgnoreCase(opt, "exat") && expire_opt(kEXAT)) {
    } else if (EqualsIgnoreCase(opt, "pxat") && expire_opt(kPXAT)) {
    } else {
      return c.out.Error(kSyntaxErr);
    }
  }

  int64_t expire_at = 0;
  if (flags & kAnyExpire) {
    bool seconds = flags & (kEX | kEXAT);
    bool relative = flags & (kEX | kPX);
    switch (ParseSetExpire(expire_arg, seconds, relative, c.now_ms, &expire_at)) {
      case ExpireParse::kOk:
        break;
      case ExpireParse::kNotInteger:
        return c.out.Error(kNotIntegerErr);
      case ExpireParse::kInvalid:
        return c.out.Error(InvalidExpireError("set"));
    }
  }

  DbSlice& db = c.shard.db();
  auto it = db.Find(c.args[1], c.now_ms);
  if (flags & kGet) {
    if (it) {
      c.out.Bulk(it->value.view());
    } else {
      c.out.NullBulk();
    }
  }
  if (((flags & kNX) && it) || ((flags & kXX) && !it)) {
    if (!(flags & kGet)) c.out.NullBulk();
    return;
  }
  if (!it) it = db.FindOrInsert(c.args[1], c.now_ms).first;
  it->value.Assign(c.args[2]);
  if (expire_at != 0) {
    db.SetExpire(it, expire_at);
  } else if (!(flags & kKeepTtl)) {
    db.SetExpire(it, 0);
  }
  if (!(flags & kGet)) c.out.Ok();
}

void SetNx(OpContext& c) {
  auto [it, inserted] = c.shard.db().FindOrInsert(c.args[1], c.now_ms);
  if (inserted) it->value.Assign(c.args[2]);
  c.out.Integer(inserted ? 1 : 0);
}

// SETEX key seconds value / PSETEX key milliseconds value
void SetExGeneric(OpContext& c, bool seconds, std::string_view name) {
  int64_t expire_at;
  switch (ParseSetExpire(c.args[2], seconds, true, c.now_ms, &expire_at)) {
    case ExpireParse::kOk:
      break;
    case ExpireParse::kNotInteger:
      return c.out.Error(kNotIntegerErr);
    case ExpireParse::kInvalid:
      return c.out.Error(InvalidExpireError(name));
  }
  DbSlice& db = c.shard.db();
  auto it = db.FindOrInsert(c.args[1], c.now_ms).first;
  it->value.Assign(c.args[3]);
  db.SetExpire(it, expire_at);
  c.out.Ok();
}

void SetEx(OpContext& c) { SetExGeneric(c, true, "setex"); }
void PSetEx(OpContext& c) { SetExGeneric(c, false, "psetex"); }

void GetSet(OpContext& c) {
  DbSlice& db = c.shard.db();
  auto [it, inserted] = db.FindOrInsert(c.args[1], c.now_ms);
  if (inserted) {
    c.out.NullBulk();
  } else {
    c.out.Bulk(it->value.view());
  }
  it->value.Assign(c.args[2]);
  db.SetExpire(it, 0);
}

void GetDel(OpContext& c) {
  DbSlice& db = c.shard.db();
  auto it = db.FindRead(c.args[1], c.now_ms);
  if (!it) return c.out.NullBulk();
  c.out.Bulk(it->value.view());
  db.Delete(it);
}

void Append(OpContext& c) {
  auto [it, inserted] = c.shard.db().FindOrInsert(c.args[1], c.now_ms);
  std::string joined(it->value.view());
  joined.append(c.args[2]);
  it->value.Assign(joined);
  c.out.Integer(static_cast<int64_t>(joined.size()));
}

void StrLen(OpContext& c) {
  auto it = c.shard.db().Find(c.args[1], c.now_ms);
  c.out.Integer(it ? static_cast<int64_t>(it->value.size()) : 0);
}

void IncrBy(OpContext& c, int64_t delta) {
  auto [it, inserted] = c.shard.db().FindOrInsert(c.args[1], c.now_ms);
  int64_t cur = 0;
  if (!inserted && !ParseInt64(it->value.view(), &cur)) return c.out.Error(kNotIntegerErr);
  if ((delta < 0 && cur < 0 && delta < std::numeric_limits<int64_t>::min() - cur) ||
      (delta > 0 && cur > 0 && delta > std::numeric_limits<int64_t>::max() - cur)) {
    return c.out.Error(kOverflowErr);
  }
  cur += delta;
  char buf[24];
  auto [end, ec] = std::to_chars(buf, buf + sizeof(buf), cur);
  it->value.Assign(std::string_view(buf, static_cast<size_t>(end - buf)));
  c.out.Integer(cur);
}

void Incr(OpContext& c) { IncrBy(c, 1); }
void Decr(OpContext& c) { IncrBy(c, -1); }

void IncrByCmd(OpContext& c) {
  int64_t delta;
  if (!ParseInt64(c.args[2], &delta)) return c.out.Error(kNotIntegerErr);
  IncrBy(c, delta);
}

void DecrByCmd(OpContext& c) {
  int64_t delta;
  if (!ParseInt64(c.args[2], &delta)) return c.out.Error(kNotIntegerErr);
  if (delta == std::numeric_limits<int64_t>::min()) return c.out.Error("ERR decrement would overflow");
  IncrBy(c, -delta);
}

// MGET and MSET run one op per key: {name, key} and {name, key, value}.
void MGetPart(OpContext& c) { Get(c); }

void MSetPart(OpContext& c) {
  DbSlice& db = c.shard.db();
  auto it = db.FindOrInsert(c.args[1], c.now_ms).first;
  it->value.Assign(c.args[2]);
  db.SetExpire(it, 0);
}

std::string CheckMSet(std::span<const std::string_view> args) {
  return (args.size() - 1) % 2 == 0 ? std::string() : WrongArgsError("mset");
}

}  // namespace

void RegisterStringFamily(CommandTable* t) {
  t->Add({.name = "get", .arity = 2, .route = Route::kSingleKey, .shard_fn = Get});
  t->Add({.name = "set", .arity = -3, .route = Route::kSingleKey, .shard_fn = Set});
  t->Add({.name = "setnx", .arity = 3, .route = Route::kSingleKey, .shard_fn = SetNx});
  t->Add({.name = "setex", .arity = 4, .route = Route::kSingleKey, .shard_fn = SetEx});
  t->Add({.name = "psetex", .arity = 4, .route = Route::kSingleKey, .shard_fn = PSetEx});
  t->Add({.name = "getset", .arity = 3, .route = Route::kSingleKey, .shard_fn = GetSet});
  t->Add({.name = "getdel", .arity = 2, .route = Route::kSingleKey, .shard_fn = GetDel});
  t->Add({.name = "append", .arity = 3, .route = Route::kSingleKey, .shard_fn = Append});
  t->Add({.name = "strlen", .arity = 2, .route = Route::kSingleKey, .shard_fn = StrLen});
  t->Add({.name = "incr", .arity = 2, .route = Route::kSingleKey, .shard_fn = Incr});
  t->Add({.name = "decr", .arity = 2, .route = Route::kSingleKey, .shard_fn = Decr});
  t->Add({.name = "incrby", .arity = 3, .route = Route::kSingleKey, .shard_fn = IncrByCmd});
  t->Add({.name = "decrby", .arity = 3, .route = Route::kSingleKey, .shard_fn = DecrByCmd});
  t->Add({.name = "mget",
          .arity = -2,
          .route = Route::kPerKey,
          .combine = Combine::kArray,
          .shard_fn = MGetPart});
  t->Add({.name = "mset",
          .arity = -3,
          .route = Route::kPerKey,
          .combine = Combine::kOk,
          .key_step = 2,
          .shard_fn = MSetPart,
          .check = CheckMSet});
}

}  // namespace kv
