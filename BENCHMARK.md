# chgeos Benchmark Results

Comparison of chgeos (ClickHouse + GEOS WASM UDFs) against DuckDB spatial extension,
Apache Sedona (SedonaDB) and PyCanopy on the spatial benchmark suite.

**Hardware:** AMD Ryzen 9 5900X, 128 GB RAM  
**Dataset:** synthetic taxi trip data from https://github.com/apache/sedona-spatialbench — SF1 = 6M trips, SF10 = 60M trips  
**Timeout:** 120 s (all engines)  
**chgeos version:** 2026-08-07  
**DuckDB version:** 1.5.5  
**Sedona version:** 0.3.0  
**PyCanopy:** measured 2026-07-29  
**Runs:** 5 per query (fastest reported)  
**Winner:** fastest engine; margins under 5% are reported as a tie

---

## SF1 — 6 Million Trip Rows

| Query | Description                        | chgeos   | DuckDB   | Sedona  | PyCanopy | Winner   |
|-------|------------------------------------|----------|----------|---------|----------|----------|
| Q1    | Point-in-radius filter             | 0.08 s   | 0.15 s   | 0.43 s  | 0.81 s   | chgeos   |
| Q2    | Count trips in county polygon      | 0.08 s   | 0.22 s   | 1.13 s  | 1.68 s   | chgeos   |
| Q3    | Monthly stats in bbox+buffer       | 0.08 s   | 0.17 s   | 0.50 s  | 0.64 s   | chgeos   |
| Q4    | Zone distribution (top-1000 tips)  | 0.61 s   | 0.94 s   | 0.91 s  | 4.78 s   | chgeos   |
| Q5    | Convex hull area per customer/month| 0.79 s   | 0.69 s   | 2.00 s  | 1.16 s   | DuckDB   |
| Q6    | Zone stats for bbox-intersect zones| 0.78 s   | 0.82 s   | 0.87 s  | 2.88 s   | Tie      |
| Q7    | Detour ratio (all trips)           | 0.21 s   | 0.26 s   | 2.35 s  | 1.22 s   | chgeos   |
| Q8    | Nearby pickups per building        | 0.16 s   | 0.44 s   | 0.35 s  | 0.25 s   | chgeos   |
| Q9    | Building conflation via IoU        | 0.03 s   | 0.03 s   | 0.24 s  | 0.03 s   | Tie      |
| Q10   | Zone avg duration/distance         | 3.92 s   | 99.97 s  | 4.87 s  | 5.70 s   | chgeos   |
| Q11   | Cross-zone trip count              | 5.58 s   | TIMEOUT  | 7.82 s  | 5.84 s   | Tie      |
| Q12   | 5 nearest buildings per trip (kNN) | 2.02 s   | TIMEOUT  | 18.07 s | 5.71 s   | chgeos   |

**SF1 wins — chgeos: 8, DuckDB: 1, Sedona: 0, PyCanopy: 0, Ties: 3**

Q6 and Q11 are scored as ties on measurement grounds rather than by the 5% rule.
Q6 is 6% apart, but chgeos alone spans 0.78–0.89 s across repeat runs, so the
margin is inside its own width. Q11 is 4.5% apart, and PyCanopy's 5.84 s is a
2026-07-29 measurement being compared against a 2026-08-07 chgeos number.

Q7 changed hands since the previous revision: the flat-geometry chain path took it
from 0.45 s to 0.21 s, past DuckDB's 0.26 s. See the Q7 note below.

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
| Q1    | Point-in-radius filter             | 0.52 s    | 0.38 s   | 0.94 s   | 18.93 s  | DuckDB   |
| Q2    | Count trips in county polygon      | 0.67 s    | 0.54 s   | 1.64 s   | 12.81 s  | DuckDB   |
| Q3    | Monthly stats in bbox+buffer       | 0.50 s    | 0.52 s   | 1.43 s   | 14.59 s  | Tie      |
| Q4    | Zone distribution (top-1000 tips)  | 1.10 s    | 0.98 s   | 1.86 s   | 19.80 s  | DuckDB   |
| Q5    | Convex hull area per customer/month| 8.95 s    | 6.11 s   | 42.43 s  | 21.93 s  | DuckDB   |
| Q6    | Zone stats for bbox-intersect zones| 1.77 s    | 1.60 s   | 2.86 s   | 7.74 s   | DuckDB   |
| Q7    | Detour ratio (all trips)           | 1.86 s    | 0.93 s   | 42.28 s  | 14.39 s  | DuckDB   |
| Q8    | Nearby pickups per building        | 1.69 s    | 1.77 s   | 2.02 s   | 2.69 s   | Tie      |
| Q9    | Building conflation via IoU        | 0.10 s    | 0.13 s   | 0.37 s   | 0.06 s   | PyCanopy |
| Q10   | Zone avg duration/distance         | 28.25 s   | TIMEOUT  | 17.02 s  | 27.05 s  | Sedona   |
| Q11   | Cross-zone trip count              | 40.96 s   | TIMEOUT  | TIMEOUT  | 42.97 s  | Tie      |
| Q12   | 5 nearest buildings per trip (kNN) | 27.29 s   | TIMEOUT  | TIMEOUT  | 98.60 s  | chgeos   |

**SF10 wins — chgeos: 1, DuckDB: 6, Sedona: 1, PyCanopy: 1, Ties: 3**

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
Both DuckDB columns were re-measured on 2026-08-06 with DuckDB 1.5.5, which is far
faster than the 1.5.2 numbers this file used to carry (SF10 Q7 68.31 s → 0.93 s,
SF1 Q7 6.53 s → 0.26 s, SF1 Q10 TIMEOUT → 99.97 s). Sedona is from 2026-05-06 and
PyCanopy from 2026-07-29, and neither has been re-measured since.
Treat the sub-2-second SF10 rows as indicative, not as a ranking.

![SF10 benchmark](sf10.png)

---

## Notes

**Q5 (convex hull per customer):** The `query_plan_execute_functions_after_sorting=0`
hint is required to keep the WASM convex hull running on parallel threads before the
ORDER BY merge. Without it, ClickHouse defers the function to the single-threaded
post-sort stage, causing ~7× slowdown. Q5 also needs `max_bytes_ratio_before_external_group_by=0`
at SF10: it accumulates ~15 GiB across 3.1M groups, and the default ratio spills to disk at
0.5 × `max_server_memory_usage`, costing ~3.5 s. DuckDB leads at both scales (0.69 s vs
chgeos 0.79 s at SF1, 6.11 s vs 8.95 s at SF10); PyCanopy is 22 s at SF10 and Sedona 4×
slower (42 s).

`st_collect_agg` can be registered either as a plain WASM UDF fed by `groupArray`, or as a
native aggregate via `SETTINGS is_aggregate = 1`, which accumulates rows into arena nodes
and skips building the intermediate array. Measured head to head on this query, the
`groupArray` form wins at both scales — 0.79 s vs 1.09 s at SF1 and 8.95 s vs 10.82 s at
SF10 — for 0.1–0.5 GiB more peak memory, so `st_collect_agg` dropped the setting in commit
`79063df`. The other four `*_agg` functions keep it.

**Q7 (detour ratio):** Scans all rows computing `st_length(st_makeline(...))` with no
spatial join. WasmChainFusionPass fuses `st_makeline → st_length` into a single WASM
call, eliminating the intermediate WKB round-trip. That is enough to beat Sedona (2.35 s /
42.3 s) and PyCanopy (1.22 s / 14.4 s) at both scales, and since the flat path below it
also takes SF1 from DuckDB 1.5.5 — 0.21 s vs 0.26 s. DuckDB still wins SF10, 0.93 s vs
1.86 s. DuckDB 1.5.2 needed 6.53 s / 68.3 s here, so most of that engine's position is a
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
predicate, cutting candidate pairs dramatically. At SF1 chgeos, DuckDB and PyCanopy all
land at 0.03 s, which is below the resolution of these measurements — scored as a tie.
At SF10 chgeos beats DuckDB and Sedona but PyCanopy is faster (0.06 s vs 0.10 s).

**Q10 at SF10:** Sedona wins (17 s vs chgeos 28 s and PyCanopy 27 s, DuckDB TIMEOUT).
Sedona's DataFusion task-parallel build/probe model handles the large trip build side
more efficiently. DuckDB cannot complete within 120 s.

**Q11 at SF10:** Sedona times out because it materializes the intermediate
trip×pickup_zone result before applying the second zone join, causing memory explosion
at SF10. chgeos handles both zone joins in a single `SpatialRTreeDoubleJoin` pass
(41.0 s vs TIMEOUT for both DuckDB and Sedona). PyCanopy also completes at 43.0 s, but
that figure is from 2026-07-29 and the 5% margin is inside the noise — scored a tie.

**Q12 (kNN):** WASM `st_knn` uses a static 2-D centroid k-d tree for candidate selection,
then refines the surviving candidates to an exact point-to-geometry distance. The tree
alone reports centroid distance, which is not what `ST_Distance` means — before the
refinement landed, Q12 disagreed with the reference answers. Refinement reads coordinates
flattened once at index build, so it needs no GEOS parse per row, and Q12 got
*faster*: 10.9 s → 2.0 s at SF1, 103.8 s → 27.3 s at SF10. chgeos now leads the query at
both scales; DuckDB and Sedona still time out at SF10, and PyCanopy is 5.7 s / 98.6 s.
