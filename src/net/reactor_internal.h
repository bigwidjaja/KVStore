#pragma once

#include <memory>

#include "net/reactor.h"

namespace kv::net {

std::unique_ptr<Reactor> CreateEpollReactor();
std::unique_ptr<Reactor> CreateUringReactor(const ReactorOptions& options);

}  // namespace kv::net
