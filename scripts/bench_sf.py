#!/usr/bin/env python3
"""Geospatial benchmark suite for ClickHouse.

Usage:
    python3 scripts/bench_sf.py [path/to/clickhouse] [sf1|sf10] [--native] [--settings "key=val, key2=val2"] [QUERY]

SF (optional): scale factor — sf1 (default) or sf10.
--native: read from native MergeTree tables (sf1.trip etc.) instead of parquet.
          Run scripts/import_sf.sh once beforehand to populate them.
--settings: comma-separated "key=value" pairs appended to SETTINGS clause of each query.
QUERY (optional): run only the named query, e.g. Q1, Q7
--json: write results to benchmark_results.json (JSON Lines, one BenchmarkSuite per line)
--output: output file path for --json (default: <sf>/benchmark_results.json)

Without --native, run scripts/link_bench_data.sh once to set up parquet symlinks.
"""

import argparse
import json
import re
import socket
import subprocess
import sys
import os
from datetime import datetime, timezone

# ---------------------------------------------------------------------------
# Queries
# ---------------------------------------------------------------------------

QUERIES = [
    (
        "Q1",
        """\
SELECT t_tripkey, st_x(t_pickuploc), st_y(t_pickuploc), t_pickuptime,
    st_distance(t_pickuploc, st_geomfromtext('POINT (-111.7610 34.8697)')) AS distance_to_center
 FROM {TRIP}
 WHERE st_dwithin(t_pickuploc, st_geomfromtext('POINT (-111.7610 34.8697)'), 0.45)
 ORDER BY distance_to_center ASC, t_tripkey ASC
 {FUEL}""",
    ),
    (
        "Q2",
        """\
SELECT count() AS trip_count
 FROM {TRIP} t
 WHERE st_intersects(t.t_pickuploc,
     (SELECT z_boundary FROM {ZONE} WHERE z_name = 'Coconino County' LIMIT 1))
 {FUEL}""",
    ),
    (
        "Q3",
        """\
SELECT toStartOfMonth(t_pickuptime) AS pickup_month,
    count(t_tripkey) AS total_trips,
    avg(t_distance) AS avg_distance,
    avg(t_fare) AS avg_fare
 FROM {TRIP}
 WHERE st_dwithin(t_pickuploc,
     st_geomfromtext('POLYGON((-111.9060 34.7347,-111.6160 34.7347,-111.6160 35.0047,-111.9060 35.0047,-111.9060 34.7347))'),
     0.045)
 GROUP BY pickup_month
 ORDER BY pickup_month
 {FUEL}""",
    ),
    (
        "Q4",
        """\
SELECT z.z_zonekey, z.z_name, count() AS trip_count
 FROM {ZONE} z
 JOIN (SELECT t_pickuploc FROM {TRIP} ORDER BY t_tip DESC, t_tripkey ASC LIMIT 1000) top_trips
   ON st_within(top_trips.t_pickuploc, z.z_boundary)
 GROUP BY z.z_zonekey, z.z_name
 ORDER BY trip_count DESC, z.z_zonekey ASC
 {FUEL}""",
    ),
    (
        "Q5",
        """\
SELECT c.c_custkey, c.c_name AS customer_name,
    toStartOfMonth(t.t_pickuptime) AS pickup_month,
    st_area(st_convexhull(st_collect_agg(t.t_dropoffloc))) AS monthly_travel_hull_area,
    count() AS dropoff_count
 FROM {TRIP} t
 JOIN {CUSTOMER} c ON t.t_custkey = c.c_custkey
 GROUP BY c.c_custkey, c.c_name, pickup_month
 HAVING dropoff_count > 5
 ORDER BY dropoff_count DESC, c.c_custkey ASC
 {FUEL5}""",
    ),
    (
        "Q6",
        """\
SELECT z.z_zonekey, z.z_name,
    count(t.t_tripkey) AS total_pickups,
    avg(t.t_totalamount) AS avg_amount,
    avg(t.t_dropofftime - t.t_pickuptime) AS avg_duration
 FROM {TRIP} t
 JOIN (
      SELECT z_zonekey, z_name, z_boundary
      FROM {ZONE}
      WHERE st_intersects(st_geomfromtext('POLYGON((-112.2110 34.4197,-111.3110 34.4197,-111.3110 35.3197,-112.2110 35.3197,-112.2110 34.4197))'), z_boundary)
  ) z ON st_within(t.t_pickuploc, z.z_boundary)
 GROUP BY z.z_zonekey, z.z_name
 ORDER BY total_pickups DESC, z.z_zonekey ASC
 {FUEL}""",
    ),
    (
        "Q7",
        """\
WITH trip_lengths AS (
     SELECT t_tripkey,
         t_distance AS reported_distance_m,
         st_length(st_makeline(t_pickuploc, t_dropoffloc)) / 0.000009 AS line_distance_m
     FROM {TRIP}
 )
 SELECT t_tripkey, reported_distance_m, line_distance_m,
     reported_distance_m / nullIf(line_distance_m, 0) AS detour_ratio
 FROM trip_lengths
 ORDER BY detour_ratio DESC NULLS LAST, reported_distance_m DESC, t_tripkey ASC
 {FUEL}""",
    ),
    (
        "Q8",
        """\
SELECT b.b_buildingkey, b.b_name, count() AS nearby_pickup_count
 FROM {TRIP} t
 JOIN {BUILDING} b ON st_dwithin(t.t_pickuploc, b.b_boundary, 0.0045)
 GROUP BY b.b_buildingkey, b.b_name
 ORDER BY nearby_pickup_count DESC, b.b_buildingkey ASC
 {FUEL}""",
    ),
    (
        "Q9",
        """\
WITH b1 AS (SELECT b_buildingkey AS id, b_boundary AS geom FROM {BUILDING}),
     b2 AS (SELECT b_buildingkey AS id, b_boundary AS geom FROM {BUILDING}),
     pairs AS (
         SELECT b1.id AS building_1, b2.id AS building_2,
             st_area(b1.geom) AS area1, st_area(b2.geom) AS area2,
             st_area(st_intersection(b1.geom, b2.geom)) AS overlap_area
         FROM b1 JOIN b2 ON b1.id < b2.id AND st_intersects(b1.geom, b2.geom)
     )
 SELECT building_1, building_2, area1, area2, overlap_area,
      CASE WHEN overlap_area = 0 THEN 0.0
           WHEN (area1 + area2 - overlap_area) = 0 THEN 1.0
           ELSE overlap_area / (area1 + area2 - overlap_area) END AS iou
 FROM pairs
 ORDER BY iou DESC, building_1 ASC, building_2 ASC
 {FUEL}""",
    ),
    (
        "Q10",
        """\
SELECT z.z_zonekey, z.z_name AS pickup_zone,
    avg(t.t_dropofftime - t.t_pickuptime) AS avg_duration,
    avg(t.t_distance) AS avg_distance,
    count(t.t_tripkey) AS num_trips
 FROM {ZONE} z
 LEFT JOIN {TRIP} t ON st_within(t.t_pickuploc, z.z_boundary)
 GROUP BY z.z_zonekey, z.z_name
 ORDER BY avg_duration DESC NULLS LAST, z.z_zonekey ASC
 {FUEL}""",
    ),
    (
        "Q11",
        """\
SELECT count() AS cross_zone_trip_count
 FROM {ZONE} pickup_zone
 JOIN {TRIP} t            ON st_within(t.t_pickuploc,  pickup_zone.z_boundary)
 JOIN {ZONE} dropoff_zone ON st_within(t.t_dropoffloc, dropoff_zone.z_boundary)
 WHERE pickup_zone.z_zonekey != dropoff_zone.z_zonekey
 {FUEL}""",
    ),
    (
        "Q12",
        """\
WITH
     all_bldg AS (
         SELECT groupArray(b_boundary) AS wkbs,
                groupArray(b_buildingkey) AS keys,
                groupArray(b_name) AS names
         FROM {BUILDING}
     ),
     knn_expanded AS (
         SELECT t.t_tripkey, t.t_pickuploc,
                nb.1 AS idx,
                nb.2 AS dist
         FROM {TRIP} t
         ARRAY JOIN st_knn(t.t_pickuploc,
                           (SELECT wkbs FROM all_bldg), 5) AS nb
     )
 SELECT t_tripkey, t_pickuploc,
        (SELECT keys FROM all_bldg)[idx + 1]  AS b_buildingkey,
        (SELECT names FROM all_bldg)[idx + 1] AS building_name,
        dist AS distance_to_building
 FROM knn_expanded
 ORDER BY distance_to_building ASC, b_buildingkey ASC
 {FUEL}""",
    ),
]

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def parse_args():
    parser = argparse.ArgumentParser(
        description="Geospatial benchmark suite for ClickHouse.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""\
Usage:
  python3 scripts/bench_sf.py --ch clickhouse --sf sf1 --query Q1

--ch: path to ClickHouse binary (default: clickhouse on PATH)
--sf: scale factor — sf1 (default) or sf10
--native: read from native MergeTree tables instead of parquet
--settings: extra SETTINGS appended to each query
--query: run only this query (e.g. Q1, Q7)
--queries: comma-separated queries (e.g. Q1,Q7)
--json: write results to benchmark_results.json (JSON Lines)
--output: output file path for --json
""",
    )
    parser.add_argument("--ch", default=None)
    parser.add_argument("--sf", default="sf1")
    parser.add_argument("--port", type=int, default=int(os.environ.get("CH_PORT", 19000)))
    parser.add_argument("--timeout", type=int, default=int(os.environ.get("BENCH_TIMEOUT", 120)))
    parser.add_argument("--runs", type=int, default=int(os.environ.get("BENCH_RUNS", 5)))
    parser.add_argument("--native", action="store_true")
    parser.add_argument("--settings", default=None)
    parser.add_argument("--query", default=None,
                        help="Run only this query (e.g. Q1, Q7)")
    parser.add_argument("--queries", default=None,
                        help="Comma-separated list of queries to run (e.g. Q1,Q7)")
    parser.add_argument("--json", action="store_true",
                        help="Write results to benchmark_results.json (JSON Lines)")
    parser.add_argument("--output", default=None,
                        help="Output file path for --json (default: benchmark_results.json in --sf dir)")
    return parser.parse_args()


def run_query(ch, port, query):
    """Run a single query, returning (stdout_text, stderr_text, returncode)."""
    result = subprocess.run(
        [ch, "client", f"--port={port}", "--time", "-q", query],
        capture_output=True, text=True, errors="replace",
    )
    return result.stdout, result.stderr, result.returncode


def extract_ms(stderr, timeout):
    """Extract timing from stderr, return ms (int) or timeout_ms."""
    timeout_ms = timeout * 1000
    for line in reversed(stderr.strip().splitlines()):
        m = re.fullmatch(r"\d+(\.\d+)?", line.strip())
        if m:
            secs = float(m.group(0))
            return round(secs * 1000)
    return timeout_ms


def status_label(ms, rc, timeout):
    """Return 'OK', 'ERROR', or 'TIMEOUT'."""
    threshold = timeout * 1000 - 500
    if rc != 0 and ms < threshold:
        return "ERROR"
    if ms >= threshold:
        return "TIMEOUT"
    return "OK"


def get_git_sha():
    """Return short git SHA or 'unknown'."""
    try:
        r = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            capture_output=True, text=True, cwd=os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        )
        return r.stdout.strip() if r.returncode == 0 else "unknown"
    except Exception:
        return "unknown"


def write_json_line(output_path, suite_dict):
    """Append one BenchmarkSuite dict as a JSON line."""
    with open(output_path, "a") as f:
        json.dump(suite_dict, f)
        f.write("\n")


def run_query_once(ch, port, query, timeout):
    """Run a query and return (ms, rows_or_none, status)."""
    stdout, stderr, rc = run_query(ch, port, query)
    ms = extract_ms(stderr, timeout)
    st = status_label(ms, rc, timeout)
    rows = None
    if st == "OK" and ms < timeout * 1000 - 500:
        lines = stdout.strip().splitlines()
        if lines and lines[0].strip():
            rows = len(lines)
    return ms, rows, st


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    args = parse_args()

    ch = args.ch or os.environ.get("CLICKHOUSE_BIN", "")
    if not ch:
        import shutil
        ch = shutil.which("clickhouse") or ""
    if not ch or not os.access(ch, os.X_OK):
        print("ERROR: ClickHouse binary not found; pass via --ch or put on PATH", file=sys.stderr)
        sys.exit(1)

    sf = args.sf
    if sf not in ("sf1", "sf10"):
        print(f"ERROR: scale factor must be sf1 or sf10, got '{sf}'", file=sys.stderr)
        sys.exit(1)

    native = args.native
    runs = args.runs
    port = args.port
    timeout = args.timeout
    extra_settings = args.settings
    query_filter = args.query

    # Parse --queries (comma-separated, Sedona-compatible)
    if args.queries:
        query_filter = ",".join(q.strip().upper() for q in args.queries.split(","))

    json_flag = args.json
    output_path = args.output if args.output else (
        f"{args.sf}/benchmark_results.json" if json_flag else None
    )

    fuel = (
        "SETTINGS webassembly_udf_max_fuel=0, max_execution_time="
        f"{timeout}"
        f"{', ' + extra_settings if extra_settings else ''}"
    )
    fuel5 = (
        "SETTINGS webassembly_udf_max_fuel=0, max_execution_time="
        f"{timeout}, query_plan_execute_functions_after_sorting=0"
        f"{', ' + extra_settings if extra_settings else ''}"
    )

    table_vars = {}
    if native:
        table_vars = {
            "TRIP": f"{sf}.trip",
            "ZONE": f"{sf}.zone",
            "BUILDING": f"{sf}.building",
            "CUSTOMER": f"{sf}.customer",
        }
    else:
        table_vars = {
            "TRIP": f"file('{sf}/trip.parquet', Parquet)",
            "ZONE": f"file('{sf}/zone.parquet', Parquet)",
            "BUILDING": f"file('{sf}/building.parquet', Parquet)",
            "CUSTOMER": f"file('{sf}/customer.parquet', Parquet)",
        }

    format_label = "native" if native else "parquet"
    print()
    print(f"Scale factor: {sf}  format: {format_label}  ({runs} runs each)")

    # trip count
    result = subprocess.run(
        [ch, "client", f"--port={port}", "-q",
         f"SELECT count() FROM {table_vars['TRIP']}"],
        capture_output=True, text=True,
    )
    trip_count = result.stdout.strip() if result.returncode == 0 else "?"
    print(f"trip rows: {trip_count}")
    print()

    # header
    print("| %-6s | %8s | %8s | %8s | %10s |" % ("Query", "min", "avg", "max", "rows"))
    print("|--------|----------|----------|----------|------------|")

    # Collect results for JSON output
    results = []

    for label, tpl in QUERIES:
        if query_filter and label != query_filter:
            continue

        times = []
        rows = None
        errored = False
        timed_out = False

        for i in range(runs):
            if i == 0:
                # First run: capture rows
                stdout, stderr, rc = run_query(ch, port, tpl.format(**{**table_vars, "FUEL": fuel, "FUEL5": fuel5}))
                ms = extract_ms(stderr, timeout)
                st = status_label(ms, rc, timeout)

                if st == "ERROR":
                    errored = True
                    break

                if st == "OK" and ms < timeout * 1000 - 500:
                    lines = stdout.strip().splitlines()
                    if lines and lines[0].strip():
                        rows = len(lines)

                if ms >= timeout * 1000 - 500:
                    timed_out = True
                    break

                times.append(ms)
            else:
                # Subsequent runs: timing only
                _, stderr, rc = run_query(ch, port, tpl.format(**{**table_vars, "FUEL": fuel, "FUEL5": fuel5}))
                ms = extract_ms(stderr, timeout)
                st = status_label(ms, rc, timeout)

                if st == "ERROR":
                    errored = True
                    break
                if st == "TIMEOUT":
                    timed_out = True
                    break

                times.append(ms)

        if errored:
            print("| %-6s | %8s | %8s | %8s | %10s |" % (label, "ERROR", "ERROR", "ERROR", rows or "?"))
            results.append({"query": label, "time_seconds": None, "row_count": None,
                            "status": "error", "error_message": "ERROR"})
        elif timed_out:
            print("| %-6s | %8s | %8s | %8s | %10s |" % (label, "TIMEOUT", "TIMEOUT", "TIMEOUT", rows or "?"))
            results.append({"query": label, "time_seconds": float(timeout), "row_count": None,
                            "status": "timeout", "error_message": f"Timeout after {timeout}s"})
        else:
            avg = sum(times) // len(times)
            mn = min(times)
            mx = max(times)
            print("| %-6s | %8sms | %8sms | %8sms | %10s |" % (label, mn, avg, mx, rows))
            results.append({"query": label, "time_seconds": round(avg / 1000, 2), "row_count": rows,
                            "status": "success", "error_message": None})

    # Write JSON output
    if json_flag and output_path:
        git_sha = get_git_sha()
        suite = {
            "engine": "chgeos",
            "version": git_sha,
            "scale_factor": float(sf.replace("sf", "")),
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "total_time": round(sum(r["time_seconds"] for r in results if r["time_seconds"]), 2),
            "results": results,
        }
        write_json_line(output_path, suite)
        print(f"\nResults written to {output_path}")


if __name__ == "__main__":
    main()
