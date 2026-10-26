#pragma once

// Minimal printf-style logging to stderr, plus CHECK macros.

namespace kv {

enum class LogLevel { kDebug = 0, kInfo = 1, kWarning = 2, kError = 3 };

void SetLogLevel(LogLevel level);
bool ParseLogLevel(const char* name, LogLevel* out);
LogLevel GetLogLevel();

// Name shown in log lines from the calling thread, e.g. "w3" for worker 3.
void SetThreadLogName(const char* name);

[[gnu::format(printf, 2, 3)]] void Log(LogLevel level, const char* fmt, ...);
[[noreturn, gnu::format(printf, 1, 2)]] void Fatal(const char* fmt, ...);

}  // namespace kv

#define KV_LOG_DEBUG(...)                                                \
  do {                                                                   \
    if (::kv::GetLogLevel() <= ::kv::LogLevel::kDebug)                   \
      ::kv::Log(::kv::LogLevel::kDebug, __VA_ARGS__);                    \
  } while (0)
#define KV_LOG_INFO(...) ::kv::Log(::kv::LogLevel::kInfo, __VA_ARGS__)
#define KV_LOG_WARN(...) ::kv::Log(::kv::LogLevel::kWarning, __VA_ARGS__)
#define KV_LOG_ERROR(...) ::kv::Log(::kv::LogLevel::kError, __VA_ARGS__)

#define KV_CHECK(cond)                                                          \
  do {                                                                          \
    if (__builtin_expect(!(cond), 0))                                           \
      ::kv::Fatal("Check failed at %s:%d: %s", __FILE__, __LINE__, #cond);      \
  } while (0)

#ifdef NDEBUG
#define KV_DCHECK(cond) \
  do {                  \
  } while (0)
#else
#define KV_DCHECK(cond) KV_CHECK(cond)
#endif
