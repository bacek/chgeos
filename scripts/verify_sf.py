#!/usr/bin/env python3
"""Verify chgeos SF query results against the committed spatialbench answers.

Runs each benchmark query once (the same SQL bench_sf.py times) and compares the
result against the ground truth in the sedona-spatialbench repo:

    <spatialbench>/benchmark/answers/<sf>/q<n>.parquet

Comparison semantics come from that answer schema (see its README) and mirror
spatialbench's own verify_results.py:

  * columns compared by position — engines name columns differently
    (avg_duration vs avg_duration_seconds)
  * integer keys / counts, timestamps and strings: exact
  * floating-point metrics: rtol=1e-6, atol=1e-9; duration columns (*_seconds)
    get atol=1e-3, because SedonaDB truncates interval averages to milliseconds
  * a within-tolerance tie confined to the final row of a capped LIMIT query is
    tolerated: two rows can tie on the float ordering metric and swap identity

Usage:
    python3 scripts/verify_sf.py --ch /path/to/clickhouse --sf sf1
        [--port 19000] [--timeout 300] [--native]
        [--wire-protocol col|mp|buffers|cb]
        [--query Q1] [--queries Q1,Q7]
        [--answers-dir <spatialbench>/benchmark/answers/sf1]
        [--dump-dir DIR]        # write each result CSV for inspection

Exit status is non-zero if any query mismatches its answer or fails to run.
"""

import argparse
import io
import os
import re
import subprocess
import sys

import numpy as np
import pandas as pd

from bench_sf import QUERIES, _apply_suffix, build_table_vars

DEFAULT_ANSWERS_ROOT = os.path.expanduser(
    "~/src/sedona-spatialbench/benchmark/answers"
)

# Queries bounded by ORDER BY <float metric> ... LIMIT, so two rows can tie on
# the metric within tolerance and swap the final row's identity. Q8 is excluded
# deliberately: it orders by an integer count with an integer-key tiebreaker, so
# its ordering is fully deterministic and must match exactly.
LIMIT_QUERIES = {"q1", "q5", "q7", "q9", "q10", "q12"}
LIMIT_CAP = 100

DURATION_SUFFIX = "_seconds"
DURATION_ATOL = 1e-3

WIRE_SUFFIX = {"col": "", "mp": "_mp", "buffers": "_buffers", "cb": "_cb"}


def parse_args():
    p = argparse.ArgumentParser(
        description="Verify chgeos SF results against the committed spatialbench answers."
    )
    p.add_argument("--ch", default=None, help="ClickHouse binary (default: $CLICKHOUSE_BIN or PATH)")
    p.add_argument("--sf", default="sf1",
                   help="Scale factor: sf1 (default), sf10, sf100, ... (answers must exist)")
    p.add_argument("--port", type=int, default=int(os.environ.get("CH_PORT", 19000)))
    p.add_argument("--timeout", type=int, default=int(os.environ.get("BENCH_TIMEOUT", 300)))
    p.add_argument("--native", action="store_true", help="read MergeTree tables instead of parquet")
    p.add_argument("--wire-protocol", default="col", choices=list(WIRE_SUFFIX))
    p.add_argument("--settings", default=None, help="extra SETTINGS appended to each query")
    p.add_argument("--query", action="append", dest="query", metavar="QUERY",
                   help="verify only this query; repeatable")
    p.add_argument("--queries", default=None, help="comma-separated shorthand, e.g. Q1,Q7")
    p.add_argument("--answers-dir", default=None,
                   help=f"answers directory (default: {DEFAULT_ANSWERS_ROOT}/<sf>)")
    p.add_argument("--dump-dir", default=None, help="write each result CSV here")
    p.add_argument("--rtol", type=float, default=1e-6)
    p.add_argument("--atol", type=float, default=1e-9)
    return p.parse_args()


def column_kind(dtype) -> str:
    """Classify an answer-parquet column dtype; it decides the comparison."""
    if pd.api.types.is_float_dtype(dtype):
        return "float"
    if pd.api.types.is_integer_dtype(dtype):
        return "int"
    if pd.api.types.is_datetime64_any_dtype(dtype):
        return "datetime"
    return "exact"


def compare(answer: pd.DataFrame, result: pd.DataFrame, rtol: float, atol: float) -> list[dict]:
    """Positional comparison; the answer's per-column dtype decides semantics.

    Only float columns use tolerance, so key 1451371 vs 1451372.0 is a mismatch
    rather than a near-tie.
    """
    if answer.shape[1] != result.shape[1]:
        return [{
            "kind": "shape", "first_row": -1, "count": 0,
            "message": (f"column count differs: answer {answer.shape[1]} "
                        f"{list(answer.columns)} vs chgeos {result.shape[1]} {list(result.columns)}"),
        }]

    issues = []
    if len(answer) != len(result):
        issues.append({
            "kind": "shape", "first_row": -1, "count": abs(len(answer) - len(result)),
            "message": f"row count differs: answer {len(answer)} vs chgeos {len(result)}",
        })

    n = min(len(answer), len(result))
    for i in range(answer.shape[1]):
        name = str(answer.columns[i])
        kind = column_kind(answer.dtypes.iloc[i])
        a = answer.iloc[:n, i].reset_index(drop=True)
        b = result.iloc[:n, i].reset_index(drop=True)

        if kind == "float":
            col_atol = max(atol, DURATION_ATOL) if name.endswith(DURATION_SUFFIX) else atol
            va = pd.to_numeric(a, errors="coerce").to_numpy(dtype=float)
            vb = pd.to_numeric(b, errors="coerce").to_numpy(dtype=float)
            bad = ~np.isclose(va, vb, rtol=rtol, atol=col_atol, equal_nan=True)
        elif kind == "int":
            va = pd.to_numeric(a, errors="coerce")
            vb = pd.to_numeric(b, errors="coerce")
            bad = ((va != vb) & ~(va.isna() & vb.isna())).to_numpy()
        elif kind == "datetime":
            va = pd.to_datetime(a, errors="coerce")
            vb = pd.to_datetime(b, errors="coerce")
            bad = ((va != vb) & ~(va.isna() & vb.isna())).to_numpy()
        else:
            sa = a.astype("string").fillna("<NA>")
            sb = b.astype("string").fillna("<NA>")
            bad = (sa != sb).to_numpy()

        count = int(bad.sum())
        if count:
            j = int(np.where(bad)[0][0])
            issues.append({
                "kind": kind, "first_row": j, "count": count, "name": name,
                "message": (f"col {i} ('{name}', {kind}): {count}/{n} mismatch "
                            f"(first at row {j}: {a.iloc[j]!r} vs {b.iloc[j]!r})"),
            })
    return issues


def is_boundary_tie(query: str, issues: list[dict], answer: pd.DataFrame) -> bool:
    """True only for a legitimate LIMIT-boundary tie: an eligible LIMIT query at
    the cap whose only discrepancy is the final row's identity, with every float
    ordering metric still tying within tolerance."""
    if not issues or query not in LIMIT_QUERIES or len(answer) != LIMIT_CAP:
        return False
    last = len(answer) - 1
    for iss in issues:
        if iss["kind"] == "shape":
            return False
        if iss["first_row"] != last or iss["count"] != 1:
            return False
        # a numeric ordering metric that differs is a wrong answer, not a tie
        if iss["kind"] == "float":
            return False
    return True


def build_query(tpl, table_vars, settings_clause, settings5_clause, suffix):
    sql = tpl.format(**table_vars, FUEL=settings_clause,
                     FUEL5=settings5_clause, FUEL_SORT=settings_clause)
    return _apply_suffix(sql, suffix)


def run_csv(ch, port, sql, timeout):
    """Run a query, returning (DataFrame, error). CSVWithNames, nulls as empty."""
    proc = subprocess.run(
        [ch, "client", f"--port={port}", "--format=CSVWithNames", "-q", sql],
        capture_output=True, timeout=timeout + 60,
    )
    if proc.returncode != 0:
        err = proc.stderr.decode("utf-8", errors="replace").strip().splitlines()
        return None, (err[-1] if err else f"exit {proc.returncode}")
    text = proc.stdout.decode("utf-8", errors="replace")
    return pd.read_csv(io.StringIO(text)), None


def main() -> int:
    args = parse_args()

    ch = args.ch or os.environ.get("CLICKHOUSE_BIN", "")
    if not ch:
        import shutil
        ch = shutil.which("clickhouse") or ""
    if not ch or not os.access(ch, os.X_OK):
        print("ERROR: ClickHouse binary not found; pass via --ch or put on PATH", file=sys.stderr)
        return 2

    if not re.fullmatch(r"sf\d+", args.sf):
        print(f"ERROR: scale factor must look like sf1, sf10, sf100, ...; got '{args.sf}'", file=sys.stderr)
        return 2

    answers_dir = args.answers_dir or os.path.join(DEFAULT_ANSWERS_ROOT, args.sf)
    if not os.path.isdir(answers_dir):
        print(f"ERROR: no answers directory at {answers_dir}", file=sys.stderr)
        return 2

    query_filter: set[str] = set()
    if args.query:
        query_filter.update(q.strip().upper() for q in args.query)
    if args.queries:
        query_filter.update(q.strip().upper() for q in args.queries.split(","))

    if args.dump_dir:
        os.makedirs(args.dump_dir, exist_ok=True)

    # Same settings bench_sf.py uses, so the verified SQL is the timed SQL.
    def settings(*extra: str) -> str:
        parts = [
            "webassembly_udf_max_fuel=0",
            f"max_execution_time={args.timeout}",
            "max_bytes_ratio_before_external_group_by=0",
            "max_bytes_ratio_before_external_sort=0",
            # NULLs print as an empty CSV field, matching the answers' encoding
            "format_csv_null_representation=''",
            *extra,
        ]
        if args.settings:
            parts.append(args.settings)
        return "SETTINGS " + ", ".join(parts)

    fuel = settings()
    fuel5 = settings("query_plan_execute_functions_after_sorting=0")
    table_vars = build_table_vars(args.sf, args.native)
    suffix = WIRE_SUFFIX[args.wire_protocol]

    print()
    print(f"Verifying {args.sf} ({'native' if args.native else 'parquet'}, "
          f"wire-protocol {args.wire_protocol}) against {answers_dir}")
    print()
    print("| %-6s | %-8s | %s" % ("Query", "Verdict", "Detail"))
    print("|--------|----------|--------")

    failures = []
    checked = 0

    for label, tpl in QUERIES:
        if query_filter and label not in query_filter:
            continue
        qname = label.lower()
        answer_pq = os.path.join(answers_dir, f"{qname}.parquet")
        if not os.path.isfile(answer_pq):
            print("| %-6s | %-8s | no committed answer" % (label, "SKIP"))
            continue

        sql = build_query(tpl, table_vars, fuel, fuel5, suffix)
        try:
            result, err = run_csv(ch, args.port, sql, args.timeout)
        except subprocess.TimeoutExpired:
            result, err = None, "client timed out"
        if err is not None:
            print("| %-6s | %-8s | %s" % (label, "ERROR", err))
            failures.append((label, err))
            continue

        if args.dump_dir:
            result.to_csv(os.path.join(args.dump_dir, f"chgeos_{qname}_result.csv"), index=False)

        answer = pd.read_parquet(answer_pq)
        issues = compare(answer, result, args.rtol, args.atol)
        checked += 1

        if not issues:
            print("| %-6s | %-8s | %d rows" % (label, "PASS", len(answer)))
        elif is_boundary_tie(qname, issues, answer):
            print("| %-6s | %-8s | %d rows, LIMIT-boundary tie tolerated" % (label, "PASS", len(answer)))
        else:
            detail = "; ".join(i["message"] for i in issues[:3])
            print("| %-6s | %-8s | %s" % (label, "FAIL", detail))
            failures.append((label, detail))

    print()
    if failures:
        print(f"FAILED: {len(failures)} of {checked + len(failures)} quer(ies) did not match:",
              file=sys.stderr)
        for label, detail in failures:
            print(f"  {label}: {detail}", file=sys.stderr)
        return 1
    if checked == 0:
        print("ERROR: nothing was verified", file=sys.stderr)
        return 2
    print(f"All {checked} verified quer(ies) match the committed answers.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
