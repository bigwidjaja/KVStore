#include <signal.h>
#include <sys/resource.h>

#include <cstdio>
#include <string>

#include "base/log.h"
#include "server/config.h"
#include "server/server.h"

namespace {

// Lift the open-file limit to its hard maximum so thousands of clients can connect.
void RaiseFdLimit() {
  rlimit lim;
  if (getrlimit(RLIMIT_NOFILE, &lim) == 0 && lim.rlim_cur < lim.rlim_max) {
    lim.rlim_cur = lim.rlim_max;
    setrlimit(RLIMIT_NOFILE, &lim);
  }
}

}  // namespace

int main(int argc, char** argv) {
  kv::Config config;
  std::string err;
  if (argc > 1 && (std::string(argv[1]) == "--help" || std::string(argv[1]) == "-h")) {
    fputs(kv::FlagsUsage().c_str(), stdout);
    return 0;
  }
  if (!kv::ParseFlags(argc, argv, &config, &err)) {
    fprintf(stderr, "%s\n\n%s", err.c_str(), kv::FlagsUsage().c_str());
    return 2;
  }

  signal(SIGPIPE, SIG_IGN);
  // Block the shutdown signals in every thread (workers inherit this mask) and wait for them
  // here, so no worker is ever interrupted by a handler.
  sigset_t signals;
  sigemptyset(&signals);
  sigaddset(&signals, SIGINT);
  sigaddset(&signals, SIGTERM);
  pthread_sigmask(SIG_BLOCK, &signals, nullptr);
  RaiseFdLimit();

  kv::Server server(config);
  if (!server.Start(&err)) {
    KV_LOG_ERROR("startup failed: %s", err.c_str());
    return 1;
  }

  int sig = 0;
  sigwait(&signals, &sig);
  KV_LOG_INFO("Received %s, shutting down", sig == SIGINT ? "SIGINT" : "SIGTERM");
  server.Stop();
  KV_LOG_INFO("Bye");
  return 0;
}
