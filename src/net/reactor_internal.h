#pragma once

#include <memory>

#include "net/reactor.h"

namespace kv::net {

std::unique_ptr<Reactor> CreateEpollReactor();

}  // namespace kv::net
