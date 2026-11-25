#!/usr/bin/env python3
"""Plots the latency distributions written by rtbus_latency.

For each message size, one curve per transport: the fraction of messages slower than each
latency (a complementary CDF). Both axes are logarithmic, so the tail from p99 to p99.9 is as
readable as the median.

Usage: plot_latency.py latency.csv latency.png
"""

import csv
import sys
from collections import defaultdict

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

COLORS = {"unix-socket": "tab:red", "rtbus-sleep": "tab:blue", "rtbus-busy-poll": "tab:green"}


def size_label(size):
    if size >= 1 << 20:
        return f"{size >> 20} MB"
    if size >= 1 << 10:
        return f"{size >> 10} KB"
    return f"{size} B"


def read_samples(path):
    samples = defaultdict(list)
    with open(path, newline="") as file:
        for row in csv.DictReader(file):
            key = (int(row["message_bytes"]), row["transport"])
            samples[key].append(int(row["latency_ns"]) / 1000.0)
    return samples


def plot(samples, output):
    sizes = sorted({size for size, _ in samples})
    figure, axes = plt.subplots(1, len(sizes), figsize=(4 * len(sizes), 4), sharey=True)
    for axis, size in zip(axes, sizes):
        for transport, color in COLORS.items():
            latencies = sorted(samples.get((size, transport), []))
            if not latencies:
                continue
            count = len(latencies)
            slower = [(count - i) / count for i in range(count)]
            axis.step(latencies, slower, where="post", color=color, label=transport)
        axis.set_xscale("log")
        axis.set_yscale("log")
        axis.set_title(size_label(size))
        axis.set_xlabel("one-way latency (µs)")
        axis.grid(True, which="both", alpha=0.3)
    axes[0].set_ylabel("fraction of messages slower")
    axes[0].legend()
    figure.tight_layout()
    figure.savefig(output, dpi=120)


def main():
    if len(sys.argv) != 3:
        sys.exit(__doc__)
    plot(read_samples(sys.argv[1]), sys.argv[2])


if __name__ == "__main__":
    main()
