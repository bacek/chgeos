#!/usr/bin/env python3
"""Plot chgeos query times over time from the git history of benchmark_results.json.

One panel per query: speedup relative to the first measurement (log axis), SF1 and SF10
as two lines. Each point is the
last committed measurement of that day. Vertical lines mark the points where the
numbers stop being comparable (new machine, corrected queries, new data files).
"""

import json
import subprocess
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter, LogLocator, NullFormatter

ROOT = Path(__file__).resolve().parent.parent
RESULTS = "benchmark_results.json"
OUT = ROOT / "history.png"

QUERIES = [f"q{i}" for i in range(1, 13)]
TITLES = {
    "q1": "Q1 point-in-radius", "q2": "Q2 trips in county", "q3": "Q3 monthly bbox stats",
    "q4": "Q4 zone distribution", "q5": "Q5 convex hull area", "q6": "Q6 zone stats",
    "q7": "Q7 detour ratio", "q8": "Q8 pickups per building", "q9": "Q9 building IoU",
    "q10": "Q10 zone duration", "q11": "Q11 cross-zone trips", "q12": "Q12 kNN buildings",
}
# First date at which numbers are no longer comparable with the previous point.
BREAKS = {
    "2026-05-06": "new machine",
    "2026-08-06": "canonical queries",
    "2026-10-04": "upstream data files",
}
SCALES = {1: ("SF1", "#93c5fd", "--"), 10: ("SF10", "#2563eb", "-")}


def git(*args: str) -> str:
    return subprocess.run(["git", "-C", str(ROOT), *args], check=True,
                          capture_output=True, text=True).stdout


def load_snapshots() -> list[tuple[str, dict]]:
    """Return [(date, {(scale, query): seconds})], last commit per day, oldest first."""
    by_date: dict[str, dict] = {}
    for line in git("log", "--reverse", "--format=%h %ad", "--date=short", "--", RESULTS).splitlines():
        sha, date = line.split()
        data = json.loads(git("show", f"{sha}:{RESULTS}"))
        times = {}
        for run in data["results"]:
            if run["engine"] != "chgeos":
                continue
            scale = int(float(run["scale_factor"]))
            for r in run["results"]:
                if r.get("status") == "success" and r.get("time_seconds") is not None:
                    times[(scale, r["query"])] = r["time_seconds"]
        by_date[date] = times
    return list(by_date.items())


def main() -> None:
    snapshots = load_snapshots()
    dates = [d for d, _ in snapshots]
    xs = range(len(dates))

    fig, axes = plt.subplots(3, 4, figsize=(16, 10), sharex=True)
    for ax, q in zip(axes.flat, QUERIES):
        finals = []
        for scale, (label, color, style) in SCALES.items():
            ys = [t.get((scale, q)) for _, t in snapshots]
            pts = [(x, y) for x, y in zip(xs, ys) if y is not None]
            if not pts:
                continue
            base = pts[0][1]
            ax.plot([x for x, _ in pts], [base / y for _, y in pts], style,
                    marker="o", ms=4, color=color, label=label)
            finals.append(f"{label} {base / pts[-1][1]:.1f}×")
        ax.axhline(1, color="grey", lw=0.8)
        ax.set_title(f"{TITLES[q]}\n" + ", ".join(finals), fontsize=10)
        ax.set_yscale("log")
        ax.yaxis.set_major_locator(LogLocator(base=10, subs=(1, 2, 5)))
        ax.yaxis.set_major_formatter(FuncFormatter(lambda v, _: f"{v:g}×"))
        ax.yaxis.set_minor_formatter(NullFormatter())
        ax.grid(True, which="both", alpha=0.25)
        for i, d in enumerate(dates):
            if d in BREAKS:
                ax.axvline(i - 0.5, color="grey", lw=0.8, ls=":")

    for ax in axes[-1]:
        ax.set_xticks(list(xs))
        ax.set_xticklabels([d[5:] for d in dates], rotation=45, fontsize=8)
    for ax in axes[:, 0]:
        ax.set_ylabel("speedup vs first run")
    axes[0][0].legend(fontsize=8)

    notes = "   ".join(f"{d[5:]}: {why}" for d, why in BREAKS.items())
    fig.suptitle("chgeos speedup over time, relative to first measurement (higher is better)", fontsize=14)
    fig.text(0.5, 0.005, f"Dotted lines: numbers not directly comparable across them — {notes}",
             ha="center", fontsize=9, color="grey")
    fig.tight_layout(rect=(0, 0.02, 1, 0.97))
    fig.savefig(OUT, dpi=110)
    print(f"Written: {OUT}")


if __name__ == "__main__":
    main()
