#!/usr/bin/env python3
"""Phase 5, step 10: syscalls per request, epoll vs io_uring, under the same load.

Two independent measurements:
  1. The server's own per-thread syscall counters (INFO syscalls), read before and after the
     load, divided by the number of commands processed in between. Cheap, so the load runs
     at full speed.
  2. strace -c -f attached to the server for one shorter run per backend, as a cross-check
     that the counters see every syscall. strace slows the server down a lot, which changes
     how much work each loop turn batches, so its absolute numbers are not the headline.

Writes bench/results/phase5/{syscalls.csv,strace_*.txt,testbed.txt}.
"""

import argparse
import os
import shutil
import signal
import subprocess
import time

import kvbench

CONFIGS = [
    ("epoll", ["--backend=epoll"]),
    ("uring_basic_eventfd", ["--backend=uring", "--uring_multishot=false", "--uring_wake=eventfd"]),
    ("uring_multishot_eventfd", ["--backend=uring", "--uring_multishot=true", "--uring_wake=eventfd"]),
    ("uring_multishot_msgring", ["--backend=uring", "--uring_multishot=true", "--uring_wake=msg_ring"]),
]

WORKLOADS = [
    ("P1_c50", ["-c", "50", "-P", "1"]),
    ("P16_c50", ["-c", "50", "-P", "16"]),
]


def syscall_counters(port):
    s = kvbench.info(port, "syscalls")
    return {k[len("syscall_"):]: v for k, v in s.items() if k.startswith("syscall_")}


def run_once(name, flags, workload, threads, requests):
    srv = kvbench.start_kv(f"--threads={threads}", *flags)
    try:
        backend = kvbench.info(srv.port, "server")["io_backend"]
        if flags[0] == "--backend=uring" and backend != "uring":
            raise SystemExit("io_uring unavailable; run with seccomp disabled (see tools/dev/shell.sh)")
        before = syscall_counters(srv.port)
        cmds_before = kvbench.info(srv.port, "stats")["total_commands_processed"]
        result = kvbench.redis_benchmark(srv.port, *workload, "-t", "set,get", "-n", str(requests),
                                         "-r", "1000000", "--threads", "2")
        after = syscall_counters(srv.port)
        # The two INFO calls after the run count themselves as commands; subtract them.
        cmds = kvbench.info(srv.port, "stats")["total_commands_processed"] - cmds_before - 2
    finally:
        srv.stop()
    delta = {k: after[k] - before.get(k, 0) for k in after}
    row = {"config": name, "commands": cmds,
           "rps_set": result["SET"]["rps"], "rps_get": result["GET"]["rps"],
           "syscalls_per_req": delta["total"] / cmds}
    for k, v in delta.items():
        if k != "total":
            row[f"{k}_per_req"] = v / cmds
    return row


def strace_run(name, flags, threads, requests, out):
    srv = kvbench.start_kv(f"--threads={threads}", *flags)
    path = os.path.join(out, f"strace_{name}.txt")
    try:
        tracer = subprocess.Popen(["strace", "-c", "-f", "-o", path, "-p", str(srv.proc.pid)],
                                  stderr=subprocess.DEVNULL)
        time.sleep(1)
        cmds_before = kvbench.info(srv.port, "stats")["total_commands_processed"]
        kvbench.redis_benchmark(srv.port, "-c", "50", "-P", "16", "-t", "set,get", "-n",
                                str(requests), "-r", "1000000")
        cmds = kvbench.info(srv.port, "stats")["total_commands_processed"] - cmds_before - 1
        tracer.send_signal(signal.SIGINT)
        tracer.wait(timeout=60)
    finally:
        srv.stop()
    total = 0
    with open(path) as f:
        for line in f:
            parts = line.split()
            if len(parts) >= 5 and parts[-1] == "total":
                total = int(parts[3])
    with open(path, "a") as f:
        f.write(f"\n# commands during trace: {cmds}\n# syscalls per request: {total / cmds:.4f}\n")
    return {"config": name, "commands": cmds, "strace_total": total,
            "strace_per_req": total / cmds}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--threads", type=int, default=4)
    ap.add_argument("--requests", type=int, default=2_000_000)
    ap.add_argument("--strace-requests", type=int, default=200_000)
    ap.add_argument("--out", default=os.path.join(kvbench.REPO, "bench/results/phase5"))
    args = ap.parse_args()
    os.makedirs(args.out, exist_ok=True)

    rows = []
    for wname, workload in WORKLOADS:
        for name, flags in CONFIGS:
            row = run_once(name, flags, workload, args.threads, args.requests)
            row["workload"] = wname
            rows.append(row)
            print(f"{wname:8} {name:24} syscalls/req={row['syscalls_per_req']:.4f} "
                  f"SET={row['rps_set']:>11,.0f} GET={row['rps_get']:>11,.0f}")
    fields = ["workload", "config", "commands", "rps_set", "rps_get", "syscalls_per_req"]
    fields += sorted({k for r in rows for k in r if k.endswith("_per_req") and k not in fields})
    kvbench.write_csv(os.path.join(args.out, "syscalls.csv"), rows, fields)

    if shutil.which("strace"):
        checks = [strace_run(n, f, args.threads, args.strace_requests, args.out)
                  for n, f in (CONFIGS[0], CONFIGS[3])]
        kvbench.write_csv(os.path.join(args.out, "strace_check.csv"), checks,
                          ["config", "commands", "strace_total", "strace_per_req"])
        for c in checks:
            print(f"strace {c['config']:24} syscalls/req={c['strace_per_req']:.4f}")

    with open(os.path.join(args.out, "testbed.txt"), "w") as f:
        f.write(kvbench.testbed())
        f.write(f"server threads: {args.threads}; redis-benchmark -t set,get -r 1000000 --threads 2\n")
        f.write("method: INFO syscalls deltas / commands processed; strace -c -f cross-check\n")


if __name__ == "__main__":
    main()
