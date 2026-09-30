#!/usr/bin/env python3
"""Plot Phase 6 benchmark results (tiers.jsonl) -> docs/benchmarks.png.

    uv run --with matplotlib python3 benchmarks/plot_results.py [tiers.jsonl]

Left panel : target input vs achieved production vs consumption (throughput)
Right panel: event-time -> result-time freshness (p50/p95) + end-of-tier lag
"""

import json
import sys
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

PROJECT = Path(__file__).resolve().parent.parent


def newest_tiers() -> Path:
    candidates = sorted(PROJECT.glob("benchmarks/results/*/tiers.jsonl"))
    if not candidates:
        sys.exit("no tiers.jsonl found — run: make benchmark")
    return candidates[-1]


def main() -> None:
    src = Path(sys.argv[1]) if len(sys.argv) > 1 else newest_tiers()
    tiers = sorted(
        (json.loads(line) for line in open(src) if line.strip()),
        key=lambda r: r["target_rate"],
    )
    if not tiers:
        sys.exit(f"{src} is empty")

    x = [t["target_rate"] for t in tiers]
    produced = [t["produced_per_s"] for t in tiers]
    consumed = [
        max((d["rate"] for d in t["consumed"].values()), default=0) for t in tiers
    ]
    # Latency = stream lag in seconds behind (lag_at_end / achieved rate).
    # Per-tier result-freshness was too sparse to plot (result writes cluster
    # at window close) — reported once over the whole run in docs/benchmarks.md.
    lag_s = [
        t["lag_at_end"] / max(t["produced_per_s"], 1.0) for t in tiers
    ]
    drain = [t["drain_s"] for t in tiers]

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12.5, 4.6))

    ax1.plot(x, x, "--", color="#888888", lw=1.2, label="target input")
    ax1.plot(x, produced, "o-", color="#2563eb", lw=2, label="produced → Kafka")
    ax1.plot(x, consumed, "s-", color="#16a34a", lw=2, label="consumed (busiest query)")
    ax1.set_xlabel("target events/s")
    ax1.set_ylabel("events/s")
    ax1.set_title("Throughput: input vs pipeline")
    ax1.grid(alpha=0.3)
    ax1.legend(loc="center left")

    ax2.plot(x, lag_s, "o-", color="#dc2626", lw=2, label="stream lag @ tier end")
    ax2.set_xlabel("target events/s")
    ax2.set_ylabel("seconds behind (worst query)", color="#dc2626")
    ax2.set_title("Backpressure: lag & drain")
    ax2.grid(alpha=0.3)
    ax2.margins(y=0.2)

    ax3 = ax2.twinx()
    ax3.bar(x, drain, width=[r * 0.14 for r in x], alpha=0.25, color="#2563eb",
            label="drain to catch up")
    ax3.set_ylabel("drain seconds", color="#2563eb")

    h1, l1 = ax2.get_legend_handles_labels()
    h2, l2 = ax3.get_legend_handles_labels()
    ax2.legend(h1 + h2, l1 + l2, loc="upper left")

    fig.suptitle(
        "Gaming Intelligence Platform — Phase 6 benchmark "
        f"(drain: {', '.join(str(d) + 's' for d in drain)})",
        fontsize=11,
    )
    fig.tight_layout()
    out = PROJECT / "docs" / "benchmarks.png"
    fig.savefig(out, dpi=130)
    print(f"wrote {out}  (from {src})")


if __name__ == "__main__":
    main()
