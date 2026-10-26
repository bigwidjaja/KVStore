#!/usr/bin/env python3
"""Phase 3, steps 1 and 9: bytes per key, worst-case insert latency during growth, and table
throughput, for our DashTable against std::unordered_map and absl::flat_hash_map.

Runs the binaries from the release build and writes bench/results/phase3/:
  bytes_per_key.csv, growth_latency.csv, growth_summary.csv, table_bench.json,
  growth_latency.png, testbed.txt
"""

import argparse
import csv
import os
import subprocess

import kvbench


def plot(out):
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    series = {}
    with open(os.path.join(out, "growth_latency.csv")) as f:
        for row in csv.DictReader(f):
            series.setdefault(row["table"], ([], []))
            series[row["table"]][0].append(int(row["inserted"]) / 1e6)
            series[row["table"]][1].append(float(row["window_max_us"]))

    labels = {"dash": "DashTable (ours)", "std_unordered_map": "std::unordered_map",
              "absl_flat_hash_map": "absl::flat_hash_map"}
    colors = {"dash": "#2a7ab0", "std_unordered_map": "#d1495b", "absl_flat_hash_map": "#8e8e8e"}
    fig, ax = plt.subplots(figsize=(9, 4.5), dpi=150)
    for name in ("std_unordered_map", "absl_flat_hash_map", "dash"):
        if name in series:
            x, y = series[name]
            ax.plot(x, y, label=labels[name], color=colors[name], linewidth=1.2)
    ax.set_yscale("log")
    ax.set_xlabel("keys inserted (millions)")
    ax.set_ylabel("slowest single insert in window (µs, log)")
    ax.set_title("Insert latency while the table grows")
    ax.grid(True, which="both", alpha=0.25)
    ax.legend(frameon=False)
    fig.tight_layout()
    fig.savefig(os.path.join(out, "growth_latency.png"))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--keys", type=int, default=10_000_000)
    ap.add_argument("--build", default=os.path.join(kvbench.REPO, "build/release"))
    ap.add_argument("--out", default=os.path.join(kvbench.REPO, "bench/results/phase3"))
    args = ap.parse_args()
    os.makedirs(args.out, exist_ok=True)
    bench = os.path.join(args.build, "bench")

    with open(os.path.join(args.out, "bytes_per_key.csv"), "w") as f:
        subprocess.run([os.path.join(bench, "bytes_per_key"), "1000000"], stdout=f, check=True)
    subprocess.run(["taskset", "-c", "0", os.path.join(bench, "growth_latency"), str(args.keys),
                    args.out], check=True)
    subprocess.run(["taskset", "-c", "0", os.path.join(bench, "table_bench"),
                    "--benchmark_out=" + os.path.join(args.out, "table_bench.json"),
                    "--benchmark_out_format=json", "--benchmark_repetitions=3",
                    "--benchmark_report_aggregates_only=true"], check=True)
    plot(args.out)
    with open(os.path.join(args.out, "testbed.txt"), "w") as f:
        f.write(kvbench.testbed())
        f.write(f"growth_latency keys: {args.keys}; pinned to CPU 0\n")


if __name__ == "__main__":
    main()
