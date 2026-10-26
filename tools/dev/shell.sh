#!/usr/bin/env bash
# Runs a command (or an interactive shell) in the Linux dev container, with the repo mounted
# at /src. io_uring needs seccomp off inside Docker; strace needs SYS_PTRACE.
#
#   tools/dev/shell.sh                                   # interactive shell
#   tools/dev/shell.sh cmake --preset debug              # one command
set -euo pipefail

repo="$(cd "$(dirname "$0")/../.." && pwd)"
image="${KV_DEV_IMAGE:-kvstore-dev}"

if [ -z "$(docker images -q "$image" 2>/dev/null)" ]; then
  docker build -t "$image" -f "$repo/tools/dev/Dockerfile" "$repo/tools/dev"
fi

tty_flags=()
if [ -t 0 ] && [ -t 1 ]; then tty_flags=(-it); fi

exec docker run --rm ${tty_flags[@]+"${tty_flags[@]}"} \
  --security-opt seccomp=unconfined \
  --cap-add SYS_PTRACE --cap-add SYS_NICE \
  --ulimit nofile=65536:65536 \
  -v "$repo:/src" -w /src \
  "$image" "${@:-bash}"
