#!/usr/bin/env python3
"""Generate benchmark bar charts from benchmark_results.json."""

import json
import sys
from pathlib import Path
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import numpy as np

QUERIES = [f"q{i}" for i in range(1, 13)]
QUERY_LABELS = [f"Q{i}" for i in range(1, 13)]

ENGINE_STYLE = {
    "chgeos": {"color": "#2563eb", "label": "chgeos"},
    "duckdb":  {"color": "#f59e0b", "label": "DuckDB"},
    "sedona":  {"color": "#16a34a", "label": "Sedona"},
    "pycanopy": {"color": "#dc2626", "label": "PyCanopy"},
}

# Placeholder bar length for timeout/not-run entries (rendered differently)
PLACEHOLDER = 1e-3


def load(path: Path) -> dict:
    with open(path) as f:
        return json.load(f)


def engine_data(results: list, engine: str, scale: int) -> tuple[list, list]:
    """Return (times, statuses) for all 12 queries for one engine/scale combo."""
    for run in results:
        if run["engine"] == engine and run["scale_factor"] == scale:
            by_q = {r["query"]: r for r in run["results"]}
            times, statuses = [], []
            for q in QUERIES:
                entry = by_q.get(q, {})
                t = entry.get("time_seconds")
                s = entry.get("status", "not_run")
                times.append(t if t is not None else PLACEHOLDER)
                statuses.append(s)
            return times, statuses
    return [PLACEHOLDER] * 12, ["not_run"] * 12


def plot_scale(results: list, scale: int, out_path: Path) -> None:
    engines = ["chgeos", "duckdb", "sedona", "pycanopy"]
    n = len(QUERIES)
    height = 0.8 / len(engines)
    y = np.arange(n)

    fig, ax = plt.subplots(figsize=(11, 9))

    all_real = []
    bar_groups = {}
    for eng in engines:
        times, statuses = engine_data(results, eng, scale)
        bar_groups[eng] = (times, statuses)
        all_real.extend(t for t, s in zip(times, statuses) if s == "success")

    # X axis: log scale; floor at a sensible minimum
    x_min = max(0.01, min(all_real) * 0.7) if all_real else 0.01
    x_max = max(all_real) * 4 if all_real else 1000

    for i, eng in enumerate(engines):
        times, statuses = bar_groups[eng]
        color = ENGINE_STYLE[eng]["color"]
        # First engine on top within each group, so legend order reads downward.
        offset = ((len(engines) - 1) / 2 - i) * height
        bars = ax.barh(y + offset, times, height, color=color,
                       alpha=0.85, label=ENGINE_STYLE[eng]["label"],
                       zorder=3)

        for bar, t, s in zip(bars, times, statuses):
            if s == "timeout":
                # Hatched bar running to the right edge of the plot
                bar.set_hatch("////")
                bar.set_edgecolor("white")
                bar.set_width(x_max * 0.9)
                # Label inside the bar end; outside would clip at the axis edge.
                ax.text(x_max * 0.87, bar.get_y() + bar.get_height() / 2,
                        "T/O", ha="right", va="center", fontsize=7,
                        color="white", fontweight="bold")
            elif s == "not_run":
                bar.set_width(0)

    ax.set_xscale("log")
    ax.set_xlim(x_min, x_max)
    ax.set_yticks(y)
    ax.set_yticklabels(QUERY_LABELS)
    ax.invert_yaxis()  # Q1 at the top
    ax.set_ylabel("Query", fontsize=12)
    ax.set_xlabel("Time (seconds, log scale)", fontsize=12)
    sf_label = "SF1 — 6M rows" if scale == 1 else "SF10 — 60M rows"
    ax.set_title(f"Spatial Benchmark — {sf_label}", fontsize=14, fontweight="bold")
    ax.xaxis.grid(True, which="both", linestyle="--", alpha=0.4, zorder=0)
    ax.set_axisbelow(True)

    # Legend: engines + timeout note
    handles = [mpatches.Patch(color=ENGINE_STYLE[e]["color"],
                               label=ENGINE_STYLE[e]["label"]) for e in engines]
    timeout_patch = mpatches.Patch(facecolor="grey", hatch="////",
                                   edgecolor="white", label="T/O = timeout / not run")
    handles.append(timeout_patch)
    # Fastest queries sit at the top, so the upper-right corner stays clear.
    ax.legend(handles=handles, loc="upper right", fontsize=10)

    fig.tight_layout()
    fig.savefig(out_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"Written: {out_path}")


def main():
    repo = Path(__file__).parent.parent
    data_path = repo / "benchmark_results.json"
    data = load(data_path)
    results = data["results"]

    plot_scale(results, scale=1,  out_path=repo / "sf1.png")
    plot_scale(results, scale=10, out_path=repo / "sf10.png")


if __name__ == "__main__":
    main()
