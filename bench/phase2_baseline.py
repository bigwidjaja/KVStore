#!/usr/bin/env python3
"""Phase 2, step 10: the first single-thread baseline against Redis on the same box.

    redis-benchmark -c 50 -P 16 -t set,get, against Redis and against kv_server with one
    thread on each backend. Server pinned to CPU 0, client to the other CPUs.

Writes bench/results/phase2/{baseline.csv,testbed.txt}.
"""

import argparse
import os

import kvbench


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--requests", type=int, default=2_000_000)
    ap.add_argument("--out", default=os.path.join(kvbench.REPO, "bench/results/phase2"))
    ap.add_argument("--repeats", type=int, default=3)
    args = ap.parse_args()

    ncpu = os.cpu_count() or 2
    server_cpu, client_cpus = "0", f"1-{ncpu - 1}"
    bench_args = ["-c", "50", "-P", "16", "-t", "set,get", "-n", str(args.requests),
                  "-r", "1000000"]

    targets = [
        ("redis", lambda: kvbench.start_redis(cpus=server_cpu)),
        ("kv_epoll", lambda: kvbench.start_kv("--threads=1", "--backend=epoll", cpus=server_cpu)),
        ("kv_uring", lambda: kvbench.start_kv("--threads=1", "--backend=uring", cpus=server_cpu)),
    ]
    rows = []
    for name, start in targets:
        for rep in range(args.repeats):
            srv = start()
            try:
                result = kvbench.redis_benchmark(srv.port, *bench_args, cpus=client_cpus)
            finally:
                srv.stop()
            for test, r in result.items():
                rows.append({"server": name, "repeat": rep, "test": test, "rps": r["rps"],
                             "p50_ms": r["p50_latency_ms"], "p99_ms": r["p99_latency_ms"],
                             "max_ms": r["max_latency_ms"]})
                print(f"{name:9} {test:4} rep={rep} rps={r['rps']:>12,.0f} "
                      f"p50={r['p50_latency_ms']}ms p99={r['p99_latency_ms']}ms")

    kvbench.write_csv(os.path.join(args.out, "baseline.csv"), rows,
                      ["server", "repeat", "test", "rps", "p50_ms", "p99_ms", "max_ms"])
    with open(os.path.join(args.out, "testbed.txt"), "w") as f:
        f.write(kvbench.testbed())
        f.write("command: redis-benchmark " + " ".join(bench_args) + "\n")
        f.write(f"pinning: server on CPU {server_cpu}, client on CPUs {client_cpus}\n")


if __name__ == "__main__":
    main()
