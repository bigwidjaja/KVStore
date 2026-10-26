#include "base/log.h"

#include <algorithm>
#include <atomic>
#include <cstdarg>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>

namespace kv {
namespace {

std::atomic<LogLevel> g_level{LogLevel::kInfo};
thread_local char tl_name[16] = "main";

const char kLevelChar[] = {'D', 'I', 'W', 'E'};

void VLog(LogLevel level, const char* fmt, va_list ap) {
  timespec ts;
  clock_gettime(CLOCK_REALTIME, &ts);
  tm tm_buf;
  localtime_r(&ts.tv_sec, &tm_buf);

  char line[1024];
  int n = snprintf(line, sizeof(line), "%c%02d%02d %02d:%02d:%02d.%06ld %-4s ",
                   kLevelChar[static_cast<int>(level)], tm_buf.tm_mon + 1, tm_buf.tm_mday,
                   tm_buf.tm_hour, tm_buf.tm_min, tm_buf.tm_sec, ts.tv_nsec / 1000, tl_name);
  if (n < 0) return;
  int m = vsnprintf(line + n, sizeof(line) - n - 1, fmt, ap);
  if (m < 0) return;
  size_t len = std::min(sizeof(line) - 2, static_cast<size_t>(n + m));
  line[len] = '\n';
  // One write per line keeps lines from different threads from interleaving.
  fwrite(line, 1, len + 1, stderr);
}

}  // namespace

void SetLogLevel(LogLevel level) { g_level.store(level, std::memory_order_relaxed); }

LogLevel GetLogLevel() { return g_level.load(std::memory_order_relaxed); }

bool ParseLogLevel(const char* name, LogLevel* out) {
  struct {
    const char* name;
    LogLevel level;
  } const kNames[] = {{"debug", LogLevel::kDebug},
                      {"info", LogLevel::kInfo},
                      {"warning", LogLevel::kWarning},
                      {"error", LogLevel::kError}};
  for (const auto& entry : kNames) {
    if (strcmp(entry.name, name) == 0) {
      *out = entry.level;
      return true;
    }
  }
  return false;
}

void SetThreadLogName(const char* name) {
  strncpy(tl_name, name, sizeof(tl_name) - 1);
  tl_name[sizeof(tl_name) - 1] = '\0';
}

void Log(LogLevel level, const char* fmt, ...) {
  if (level < GetLogLevel()) return;
  va_list ap;
  va_start(ap, fmt);
  VLog(level, fmt, ap);
  va_end(ap);
}

void Fatal(const char* fmt, ...) {
  va_list ap;
  va_start(ap, fmt);
  VLog(LogLevel::kError, fmt, ap);
  va_end(ap);
  fflush(stderr);
  abort();
}

}  // namespace kv
