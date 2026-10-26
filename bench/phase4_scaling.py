#!/usr/bin/env python3
"""Phase 4: throughput at 1, 2, 4 and 8 worker threads.

Random keys (-r) so requests spread over every shard. Server threads are pinned (--pin);
redis-benchmark runs multi-threaded so the client isn't the bottleneck.

Writes bench/results/phase4/{scaling.csv,testbed.txt}.
"""

import argparse
import os

import kvbench


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--threads", default="1,2,4,8")
    ap.add_argument("--backend", default="epoll")
    ap.add_argument("--requests", type=int, default=4_000_000)
    ap.add_argument("--client-threads", type=int, default=4)
    ap.add_argument("--pipeline", type=int, default=16)
    ap.add_argument("--repeats", type=int, default=3)
    ap.add_argument("--out", default=os.path.join(kvbench.REPO, "bench/results/phase4"))
    args = ap.parse_args()

    bench_args = ["-c", "200", "-P", str(args.pipeline), "-t", "set,get", "-n",
                  str(args.requests), "-r", "1000000", "--threads", str(args.client_threads)]
    rows = []
    for t in [int(x) for x in args.threads.split(",")]:
        for rep in range(args.repeats):
            srv = kvbench.start_kv(f"--threads={t}", f"--backend={args.backend}", "--pin")
            try:
                result = kvbench.redis_benchmark(srv.port, *bench_args)
                stats = kvbench.info(srv.port, "stats")
            finally:
                srv.stop()
            for test, r in result.items():
                rows.append({"threads": t, "backend": args.backend, "repeat": rep, "test": test,
                             "rps": r["rps"], "p50_ms": r["p50_latency_ms"],
                             "p99_ms": r["p99_latency_ms"],
                             "remote_ops": stats.get("remote_ops_sent", 0),
                             "batches": stats.get("batches_sent", 0),
                             "wakeups": stats.get("wakeups_sent", 0)})
                print(f"threads={t} {test:4} rep={rep} rps={r['rps']:>12,.0f} "
                      f"p50={r['p50_latency_ms']}ms p99={r['p99_latency_ms']}ms")

    kvbench.write_csv(os.path.join(args.out, f"scaling_{args.backend}.csv"), rows,
                      ["threads", "backend", "repeat", "test", "rps", "p50_ms", "p99_ms",
                       "remote_ops", "batches", "wakeups"])
    with open(os.path.join(args.out, "testbed.txt"), "w") as f:
        f.write(kvbench.testbed())
        f.write("command: redis-benchmark " + " ".join(bench_args) + "\n")
        f.write("server: --pin (worker i on CPU i); client unpinned\n")


if __name__ == "__main__":
    main()
