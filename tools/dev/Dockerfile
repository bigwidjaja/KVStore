# Linux dev environment for building and testing on a Mac (or anywhere).
# Real benchmark numbers still come from dedicated Linux hosts; see the plan, Phase 0 and Phase 6.
FROM ubuntu:24.04

ARG DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends \
      build-essential g++-14 clang-18 libclang-rt-18-dev lld-18 \
      cmake ninja-build pkg-config git ca-certificates \
      liburing-dev libxxhash-dev libabsl-dev libgtest-dev libgmock-dev libbenchmark-dev \
      gdb strace procps psmisc iproute2 netcat-openbsd \
      python3 python3-pytest python3-redis python3-matplotlib \
      redis-server redis-tools \
    && rm -rf /var/lib/apt/lists/*

RUN update-alternatives --install /usr/bin/clang clang /usr/bin/clang-18 100 \
 && update-alternatives --install /usr/bin/clang++ clang++ /usr/bin/clang++-18 100

WORKDIR /src
