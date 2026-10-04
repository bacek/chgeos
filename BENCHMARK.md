# chgeos Benchmark Results

Comparison of chgeos (ClickHouse + GEOS WASM UDFs) against DuckDB spatial extension,
Apache Sedona (SedonaDB) and PyCanopy on the spatial benchmark suite.

**Hardware:** AMD Ryzen 9 5900X, 128 GB RAM  
**Dataset:** synthetic taxi trip data from https://github.com/apache/sedona-spatialbench — SF1 = 6M trips, SF10 = 60M trips  
**Timeout:** 120 s (all engines)  
**chgeos version:** 2026-10-04 (CH `95fbe4909e6`)  
**DuckDB version:** 1.5.6  
**SedonaDB version:** 0.4.1  
**PyCanopy version:** 0.4.1 (Polars 1.44.2)  
**Competitors measured:** 2026-10-04, same box and split Parquet files, via sedona-spatialbench `run_benchmark.py`  
**Runs:** 5 per query (average reported)  
**Winner:** fastest engine; margins under 5% are reported as a tie

---

## SF1 — 6 Million Trip Rows

| Query | Description                        | chgeos   | DuckDB   | Sedona  | PyCanopy | Winner   |
|-------|------------------------------------|----------|----------|---------|----------|----------|
| Q1    | Point-in-radius filter             | 0.07 s   | 0.12 s   | 0.53 s  | 0.36 s   | chgeos   |
| Q2    | Count trips in county polygon      | 0.08 s   | 0.19 s   | 1.34 s  | 0.35 s   | chgeos   |
| Q3    | Monthly stats in bbox+buffer       | 0.05 s   | 0.17 s   | 0.62 s  | 0.39 s   | chgeos   |
| Q4    | Zone distribution (top-1000 tips)  | 0.68 s   | 0.70 s   | 2.53 s  | 2.42 s   | Tie      |
| Q5    | Convex hull area per customer/month| 0.46 s   | 0.83 s   | 3.83 s  | 1.62 s   | chgeos   |
| Q6    | Zone stats for bbox-intersect zones| 0.77 s   | 0.88 s   | 2.32 s  | 4.52 s   | chgeos   |
| Q7    | Detour ratio (all trips)           | 0.14 s   | 0.28 s   | 0.78 s  | 2.50 s   | chgeos   |
| Q8    | Nearby pickups per building        | 0.05 s   | 0.48 s   | 0.34 s  | 2.20 s   | chgeos   |
| Q9    | Building conflation via IoU        | 0.02 s   | 0.03 s   | 0.13 s  | 0.02 s   | chgeos   |
| Q10   | Zone avg duration/distance         | 2.58 s   | 110.10 s | 6.20 s  | 4.18 s   | chgeos   |
| Q11   | Cross-zone trip count              | 4.60 s   | TIMEOUT  | 10.39 s | 5.97 s   | chgeos   |
| Q12   | 5 nearest buildings per trip (kNN) | 2.44 s   | TIMEOUT  | 3.97 s  | 3.31 s   | chgeos   |

**SF1 wins — chgeos: 11, DuckDB: 0, Sedona: 0, PyCanopy: 0, Ties: 1**

SF1 Q4 was re-run warm on an idle box (10 runs, min/avg/max 0.66/0.68/0.71 s); the suite run's
1.61 s average was one cold read of `zone.parquet`. Q4 is dominated by that zone read, which is the
same file at every scale factor, so SF1 and SF10 Q4 land close together.

SF1 Q10/Q11 are from a freshly restarted server (CH `4cae932a453`, 5 runs: Q10 2.49/2.58/2.66 s,
Q11 5.89/6.03/6.24 s). On a server that has already run the SF10 suite they come out ~20% / ~7%
slower (3.19 s / 6.48 s measured back to back, same build): the slowdown is server state left by
earlier heavy queries, not the code.

**The SF1 tally understates DuckDB, and the cause is our Parquet files.** `sf1/trip.parquet`
holds 6M rows in only 4 row groups. DuckDB caps a scan pipeline's thread count at the number
of row groups in the file — the row group is its atomic unit of scan parallelism, with no
sub-splitting — so DuckDB ran these queries on at most 4 of the machine's 24 threads. At SF10
the same file layout gives it 31 row groups and full parallelism, which is most of why its
SF1→SF10 curve looks sublinear. ClickHouse has no equivalent limit: its Parquet reader splits
row groups into subgroups and is at full parallelism at both scales. Several SF1 rows,
particularly the sub-second Q1–Q3, are therefore partly measuring DuckDB's row-group cap
rather than chgeos being faster, and DuckDB would likely take some of them on files with
smaller row groups. We cannot say how many: two scale factors are not enough to separate the
thread cap from DuckDB's fixed per-query overhead (~0.15 s, versus roughly zero for chgeos),
and the two models do not predict the same SF10 times. Until the data is regenerated with
smaller row groups, **treat SF10 as the primary comparison.** The SF10 Q7 gap is not
explained by this — see the Q7 note below.

![SF1 benchmark](sf1.png)

---

## SF10 — 60 Million Trip Rows

| Query | Description                        | chgeos    | DuckDB   | Sedona   | PyCanopy | Winner   |
|-------|------------------------------------|-----------|----------|----------|----------|----------|
| Q1    | Point-in-radius filter             | 0.31 s    | 0.41 s   | 1.45 s   | 3.19 s   | chgeos   |
| Q2    | Count trips in county polygon      | 0.38 s    | 0.47 s   | 2.29 s   | 3.02 s   | chgeos   |
| Q3    | Monthly stats in bbox+buffer       | 0.33 s    | 0.50 s   | 2.20 s   | 3.34 s   | chgeos   |
| Q4    | Zone distribution (top-1000 tips)  | 1.14 s    | 0.98 s   | 4.48 s   | 6.48 s   | DuckDB   |
| Q5    | Convex hull area per customer/month| 5.24 s    | 6.28 s   | 25.94 s  | 21.68 s  | chgeos   |
| Q6    | Zone stats for bbox-intersect zones| 1.59 s    | 1.55 s   | 4.58 s   | 12.47 s  | Tie      |
| Q7    | Detour ratio (all trips)           | 0.96 s    | 0.80 s   | 2.95 s   | 23.65 s  | DuckDB   |
| Q8    | Nearby pickups per building        | 0.50 s    | 1.52 s   | 1.87 s   | 16.11 s  | chgeos   |
| Q9    | Building conflation via IoU        | 0.04 s    | 0.14 s   | 0.25 s   | 0.05 s   | chgeos   |
| Q10   | Zone avg duration/distance         | 14.05 s   | TIMEOUT  | 18.03 s  | 31.21 s  | chgeos   |
| Q11   | Cross-zone trip count              | 28.21 s   | TIMEOUT  | 36.39 s  | 34.27 s  | chgeos   |
| Q12   | 5 nearest buildings per trip (kNN) | 31.67 s   | TIMEOUT  | 41.47 s  | 36.23 s  | chgeos   |

**SF10 wins — chgeos: 9, DuckDB: 2, Sedona: 0, PyCanopy: 0, Ties: 1**

chgeos SF10 was re-measured on 2026-10-04 at CH `95fbe4909e6` (COLUMNAR_V1 wire,
5 runs, averages, load average ~19 from unrelated processes, so treat sub-2-second rows as
±10%). DuckDB, SedonaDB and PyCanopy were all re-measured the same day on the same split files
(5 runs, averages, 120 s timeout).

Q10 and Q11 moved from losses to wins (Q10 30.0 → 14.0 s, Q11 45.6 → 26.9 s), all from
host-side work in `SpatialRTreeJoin`, not WASM:
- predicate evaluation groups candidates by left-row runs, resolves column pointers once per
  probe block and copies only the columns the predicate reads (`77700fc9f0a`, `95fbe4909e6`);
  the large-row path for zones with millions of candidate trips was still looking every column
  up by name per candidate, which was ~31% of Q10's straggler tail;
- the per-core R-tree sub-trees are now spatially disjoint (kd bisection on box centres) for
  build sides up to 10M rows (`08e9f5beba7`), so a probe only descends the trees it can touch.
  Q11's second join (31M rows probing the zone tree) is where this pays: CPU −24%. Above 10M
  rows (the 60M-trip build of Q10/Q11's first join) the same partitioning lost parallelism and
  is not used.

What moved since 2026-10-03: a host-side bounding-box prefilter answers bbox-disjoint
rows of constant-argument spatial predicates without entering WASM (Q1 −29%, Q3 −30%,
Q2/Q6 −7..12%), and an occupancy grid in `SpatialRTreeJoin` skips probe rows whose box
touches no build-side cell (Q8 1.45 → 0.51 s). Q4 and Q6 are now Parquet-read bound
(Q6 = zone scan 0.86 s + trip scan 0.88 s; the join itself is ~0). Q7 is the remaining
DuckDB lead: Parquet read alone is ~0.73 s, the rest is copying WKB into WASM.
The 2026-08-07 description that follows is kept for history.

Both chgeos columns were re-measured on 2026-08-07 on an idle machine at commit
`79063df`, and every query was checked against the spatialbench reference answers
first (SF1 and SF10 both 12/12). Only Q7 moved by more than the noise floor:
0.45 → 0.21 s at SF1 and 3.95 → 1.86 s at SF10, both from the flat-geometry chain
path described in the Q7 note. Every other query landed within its own run-to-run
width of the 2026-08-06 measurement, including Q5, which changed aggregation
strategy — see the Q5 note. Repeat spreads on this run were at most 1.2× between
the fastest and slowest of the 5 runs, so the sub-second rows are tighter than
usual; they are still Parquet-I/O bound and still drift ±25% across machine
states. The Q3, Q8 and Q11 ties are all under 5%, and Q3 and Q11 flipped from a
DuckDB and a PyCanopy win purely on that margin.
Competitor columns are re-measured on 2026-10-04 (DuckDB 1.5.6, SedonaDB 0.4.1, PyCanopy 0.4.1).
Treat the sub-2-second SF10 rows as indicative, not as a ranking.

![SF10 benchmark](sf10.png)

### chgeos over time

![chgeos query times over time](history.png)

Generated by `scripts/plot_history.py` from the git history of `benchmark_results.json`
(last measurement per day), shown as speedup relative to each query's first measurement.
Values below 1× on Q4 and Q6 come from data and machine changes, not from chgeos: both
queries read the full zone file (now 156k worldwide zones), and DuckDB slowed by the same
factor on Q4 over the same period (SF1 1.71×, SF10 1.26×), so the chgeos/DuckDB ratio is
unchanged. Dotted lines mark points where
numbers are not directly comparable: a new machine (05-06), the corrected canonical queries (08-06, which changed
Q12's meaning) and the switch to the upstream split data files (10-04).

---

## Notes

**Q5 (convex hull per customer):** The `query_plan_execute_functions_after_sorting=0`
hint is required to keep the WASM convex hull running on parallel threads before the
ORDER BY merge. Without it, ClickHouse defers the function to the single-threaded
post-sort stage, causing ~7× slowdown. Q5 also needs `max_bytes_ratio_before_external_group_by=0`
at SF10: it accumulates ~15 GiB across 3.1M groups, and the default ratio spills to disk at
0.5 × `max_server_memory_usage`, costing ~3.5 s. chgeos now leads at both scales (0.46 s vs DuckDB 0.83 s at SF1,
5.24 s vs 6.28 s at SF10); PyCanopy is 21.7 s and SedonaDB 25.9 s at SF10.

`st_collect_agg` can be registered either as a plain WASM UDF fed by `groupArray`, or as a
native aggregate via `SETTINGS is_aggregate = 1`, which accumulates rows into arena nodes
and skips building the intermediate array. Measured head to head on this query, the
`groupArray` form wins at both scales — 0.79 s vs 1.09 s at SF1 and 8.95 s vs 10.82 s at
SF10 — for 0.1–0.5 GiB more peak memory, so `st_collect_agg` dropped the setting in commit
`79063df`. The other four `*_agg` functions keep it.

**Q7 (detour ratio):** Scans all rows computing `st_length(st_makeline(...))` with no
spatial join. WasmChainFusionPass fuses `st_makeline → st_length` into a single WASM
call, eliminating the intermediate WKB round-trip. That is enough to beat SedonaDB (0.78 s /
2.95 s) and PyCanopy (2.50 s / 23.65 s) at both scales, and the flat path below it
also takes SF1 from DuckDB — 0.14 s vs 0.28 s. DuckDB still wins SF10, 0.80 s vs
0.96 s. DuckDB 1.5.2 needed 6.53 s / 68.3 s here, so most of that engine's position is a
25× / 73× improvement on its own side, not a chgeos regression. The remaining SF10 gap is
the largest against chgeos in the suite, and unlike the SF1 rows above it is not an
artifact of the file layout.

Reading the DuckDB sources explains the remainder: DuckDB does not use GEOS for this
query at all. Its `sgl` geometry layer treats a deserialized POINT as a pointer into the
Parquet blob with no copy and no allocation, builds the `ST_MakeLine` result in a stack
buffer, and computes `ST_Length` as a plain loop over the vertex array. GEOS is a separate
module reserved for real topology (`ST_Intersects`, `ST_Within`, `ST_Contains`). chgeos
instead parses WKB into a GEOS geometry tree for every row.

Most of that cost turned out to be ours rather than GEOS's, and it came off in two steps.
First, `read_wkb` was constructing a `GeometryFactory` and a `WKBReader` on every call, and
Q7 calls it twice per row — 12M reader constructions at SF1, 120M at SF10. Hoisting both to
statics over the shared default factory (commit `ea6c42c`) cut Q7 by 29% / 33%.

Second, the chain machinery grew a non-GEOS currency. Alongside `Geometry`, a chain can pass
a `FlatBatch` between stages: one contiguous `xy` array with ring and row offset arrays, no
per-row allocation and no geometry tree. `st_makeline` builds one straight from the two WKB
points and `st_length` sums vertex runs out of it, so the fused Q7 chain now runs end to end
without entering GEOS. A stage joins the flat path only if it can produce bit-identical
output, NaN handling and accumulation order included; anything that cannot declines the block
and GEOS runs it, which is why the reference answers are unchanged. That took Q7 from 0.45 s
to 0.21 s at SF1 and 3.95 s to 1.86 s at SF10.

Marginal cost per million rows is now 0.031 s for chgeos against 0.012 s for DuckDB, a 2.5×
gap — down from 5.2× after the hoist and 7.9× before it, and approaching the 1.2–2.0× seen
on the filter-dominated Q1–Q3. What remains is the WKB parse itself, which the flat path
still performs per row while DuckDB reads points in place out of the Parquet blob.

**Q9 (building IoU):** Self-join of ~20K buildings. SpatialRTreeJoin evaluates
non-spatial ON conditions (e.g. `b1.id < b2.id`) as a pre-filter before the spatial
predicate, cutting candidate pairs dramatically. All engines land at or below 0.25 s, near the
resolution of these measurements; chgeos is 0.02 s / 0.04 s against PyCanopy 0.02 s / 0.05 s.

**Q10 at SF10:** chgeos 14.0 s, SedonaDB 18.0 s, PyCanopy 31.2 s, DuckDB TIMEOUT. The remaining
cost is the 60M-trip R-tree build (~5 s) plus a probe tail: a few zone blocks with millions of
candidate trips each run longer than the rest on one thread.

**Q11 (cross-zone trips):** chgeos runs the plan PyCanopy hand-codes in Polars
(`bench/spatial_bench/queries/q11.py` in the PyCanopy repo): resolve each trip's pickup zone and
dropoff zone with two independent spatial joins, then match the two results on `t_tripkey`.
The spatialbench SQL text instead chains the dropoff join onto the pickup join's output; the
answer is identical (verified against the reference at SF1 and SF10). With the chained text
chgeos measured ~6.0 s at SF1 (a tie with PyCanopy) and 26.9 s at SF10; with the two-join
plan it is 4.60 s and 28.2 s (5 runs, fresh server), against PyCanopy 5.97 s / 34.3 s and
SedonaDB 10.4 s / 36.4 s. DuckDB runs the chained SQL and times out at both scales.

**Q12 (kNN):** WASM `st_knn` uses a static 2-D centroid k-d tree for candidate selection,
then refines the surviving candidates to an exact point-to-geometry distance. The tree
alone reports centroid distance, which is not what `ST_Distance` means — before the
refinement landed, Q12 disagreed with the reference answers. Refinement reads coordinates
flattened once at index build, so it needs no GEOS parse per row, and Q12 got
*faster*: 10.9 s → 2.0 s at SF1, 103.8 s → 27.3 s at SF10. chgeos now leads the query at
both scales; DuckDB times out at both, SedonaDB is 3.97 s / 41.5 s and PyCanopy 3.31 s / 36.2 s.
