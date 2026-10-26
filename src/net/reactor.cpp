#include "net/reactor.h"

#include "net/reactor_internal.h"

namespace kv::net {

std::unique_ptr<Reactor> CreateReactor(const ReactorOptions& options) {
  switch (options.backend) {
    case Backend::kEpoll:
      return CreateEpollReactor();
  }
  return nullptr;
}

}  // namespace kv::net
