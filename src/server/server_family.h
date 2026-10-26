#pragma once

#include <string>
#include <string_view>

#include "server/stats.h"

namespace kv {

class Worker;

// Builds the INFO text from counters summed over every thread. `section` is the optional
// argument ("" means the default set).
std::string FormatInfo(const InfoCounters& counters, std::string_view section, Worker& worker);

}  // namespace kv
