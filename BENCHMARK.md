# chgeos Benchmark Results

Comparison of chgeos (ClickHouse + GEOS WASM UDFs) against DuckDB spatial extension, Apache
Sedona (SedonaDB) and PyCanopy on the spatial benchmark suite.

**Hardware:** AMD Ryzen 9 5900X, 128 GB RAM  
**Dataset:** synthetic taxi trip data from https://github.com/apache/sedona-spatialbench — SF1 = 6M trips, SF10 = 60M trips  
**Timeout:** 120 s (all engines)  
**chgeos version:** 2026-10-05 (CH `d2d24230309`, chgeos `f1fc30a`)  
**DuckDB version:** 1.5.6  
**SedonaDB version:** 0.4.1  
**PyCanopy version:** 0.4.1 (Polars 1.44.2)  
**Competitors measured:** 2026-10-04/05, same box and split Parquet files, via sedona-spatialbench `run_benchmark.py`, straight after the chgeos runs on an otherwise idle machine  
**Runs:** chgeos 3 per query, competitors 5 per query (average reported)  
**Winner:** fastest engine; margins under 5% are reported as a tie

---

## SF1 — 6 Million Trip Rows

| Query | Description                        | chgeos   | DuckDB   | Sedona  | PyCanopy | Winner   |
|-------|------------------------------------|----------|----------|---------|----------|----------|
| Q1    | Point-in-radius filter             | 0.05 s   | 0.12 s   | 0.15 s  | 0.28 s   | chgeos   |
| Q2    | Count trips in county polygon      | 0.06 s   | 0.18 s   | 0.68 s  | 0.23 s   | chgeos   |
| Q3    | Monthly stats in bbox+buffer       | 0.06 s   | 0.17 s   | 0.28 s  | 0.29 s   | chgeos   |
| Q4    | Zone distribution (top-1000 tips)  | 0.66 s   | 0.66 s   | 0.72 s  | 1.48 s   | Tie      |
| Q5    | Convex hull area per customer/month| 0.38 s   | 0.67 s   | 2.63 s  | 0.74 s   | chgeos   |
| Q6    | Zone stats for bbox-intersect zones| 0.63 s   | 0.83 s   | 0.70 s  | 1.50 s   | chgeos   |
| Q7    | Detour ratio (all trips)           | 0.13 s   | 0.27 s   | 0.53 s  | 0.53 s   | chgeos   |
| Q8    | Nearby pickups per building        | 0.04 s   | 0.45 s   | 0.16 s  | 0.27 s   | chgeos   |
| Q9    | Building conflation via IoU        | 0.02 s   | 0.03 s   | 0.03 s  | 0.03 s   | chgeos   |
| Q10   | Zone avg duration/distance         | 1.81 s   | 103.21 s | 2.44 s  | 2.20 s   | chgeos   |
| Q11   | Cross-zone trip count              | 3.40 s   | TIMEOUT  | 3.99 s  | 2.47 s   | PyCanopy |
| Q12   | 5 nearest buildings per trip (kNN) | 1.61 s   | TIMEOUT  | 3.19 s  | 1.74 s   | chgeos   |

**SF1 wins — chgeos: 10, DuckDB: 0, Sedona: 0, PyCanopy: 1, Ties: 1**

**The SF1 results understate DuckDB.** The SF1 trip data (`sf1/trip/*.parquet`, 2 files from
upstream) has only 4 row groups. DuckDB scans one row group per thread, so it ran these queries
on at most 4 of the machine's 24 threads. At SF10 the 15 trip files have 45 row groups, enough
to use every thread. ClickHouse splits row groups further and uses all threads at both scales.
So some SF1 wins, especially the sub-second Q1–Q3, partly reflect DuckDB's thread limit rather
than chgeos being faster. We cannot tell how much, because DuckDB also has a fixed cost of
about 0.15 s per query. **Treat SF10 as the main comparison.** The SF10 Q7 result is not
affected by this; see the Q7 note.

![SF1 benchmark](sf1.png)

---

## SF10 — 60 Million Trip Rows

| Query | Description                        | chgeos    | DuckDB   | Sedona   | PyCanopy | Winner   |
|-------|------------------------------------|-----------|----------|----------|----------|----------|
| Q1    | Point-in-radius filter             | 0.27 s    | 0.32 s   | 0.62 s   | 2.23 s   | chgeos   |
| Q2    | Count trips in county polygon      | 0.31 s    | 0.47 s   | 1.36 s   | 2.02 s   | chgeos   |
| Q3    | Monthly stats in bbox+buffer       | 0.27 s    | 0.44 s   | 1.04 s   | 2.89 s   | chgeos   |
| Q4    | Zone distribution (top-1000 tips)  | 0.97 s    | 1.02 s   | 1.77 s   | 5.39 s   | chgeos   |
| Q5    | Convex hull area per customer/month| 4.86 s    | 6.54 s   | 20.87 s  | 8.63 s   | chgeos   |
| Q6    | Zone stats for bbox-intersect zones| 1.50 s    | 1.51 s   | 2.05 s   | 6.37 s   | Tie      |
| Q7    | Detour ratio (all trips)           | 0.91 s    | 0.81 s   | 1.92 s   | 7.07 s   | DuckDB   |
| Q8    | Nearby pickups per building        | 0.46 s    | 1.50 s   | 1.37 s   | 3.32 s   | chgeos   |
| Q9    | Building conflation via IoU        | 0.03 s    | 0.13 s   | 0.05 s   | 0.06 s   | chgeos   |
| Q10   | Zone avg duration/distance         | 8.00 s    | TIMEOUT  | 14.32 s  | 21.09 s  | chgeos   |
| Q11   | Cross-zone trip count              | 14.32 s   | TIMEOUT  | 29.98 s  | 15.86 s  | chgeos   |
| Q12   | 5 nearest buildings per trip (kNN) | 19.16 s   | TIMEOUT  | 36.37 s  | 19.45 s  | Tie      |

**SF10 wins — chgeos: 9, DuckDB: 1, Sedona: 0, PyCanopy: 0, Ties: 2**

chgeos was measured on 2026-10-05 at CH `d2d24230309`, 3 runs per query. The machine was busy
(load about 17), so if anything chgeos looks slower than it is. All 12 queries return the
correct answers at both scales. The other engines were measured on 2026-10-04/05 on an idle
machine, 5 runs per query, with a 120 s timeout.

What changed for Q10–Q12 since 2026-10-04 (at SF10, Q10 went from 14.1 to 8.0 s, Q11 from 25.7
to 14.3 s and Q12 from 32.5 to 19.2 s):
- the spatial join builds its index faster (2.4 → 1.3 s for 60M trips), spreads the heaviest
  zones across idle threads, and copies matching rows more efficiently;
- the nearest-building search reuses its index between batches and checks the closest
  candidates first, stopping as soon as the answer is certain.

![SF10 benchmark](sf10.png)

### chgeos over time

![chgeos query times over time](history.png)

The chart shows each query's speedup since its first measurement, using the last measurement of
each day from the git history of `benchmark_results.json` (`scripts/plot_history.py`). Q4 and
Q6 fall below 1× because the zone file grew to 156k zones worldwide, not because chgeos got
slower: DuckDB slowed by the same amount on Q4. Dotted lines mark breaks where numbers are not
directly comparable: a new machine (05-06), corrected queries (08-06, which changed what Q12
measures) and the switch to upstream's split data files (10-04).

---

## Notes

**Q4 and Q6:** both read the whole zone file, the same file at every scale. Most of their time
is spent reading Parquet, not on spatial work, so the three fastest engines finish close
together.

**Q5 (convex hull per customer):** two settings matter here.
`query_plan_execute_functions_after_sorting=0` keeps the hull calculation on parallel threads;
without it ClickHouse runs it after the sort, on one thread, about 7× slower. At SF10 the query
also needs `max_bytes_ratio_before_external_group_by=0`, because its 3.1M groups use ~15 GiB
and the default setting spills them to disk. `st_collect_agg` runs as a plain function over
`groupArray`; it was faster that way than as a native aggregate.

**Q7 (detour ratio):** every trip computes `st_length(st_makeline(...))`; there is no join.
chgeos fuses the two functions into one WASM call and runs them on plain coordinate arrays
instead of building a GEOS geometry per row. This beats every engine at SF1. DuckDB still wins
SF10 (0.81 s vs 0.91 s): it reads points straight out of the Parquet bytes, while chgeos still
parses each WKB value once.

**Q9 (building IoU):** a self-join of about 20K buildings. The non-spatial part of the join
condition (`b1.id < b2.id`) is checked before the spatial test, which removes most pairs. All
engines finish in a fraction of a second, close to the limit of what these runs can measure.

**Q10:** chgeos wins at both scales; DuckDB times out at SF10.

**Q11 (cross-zone trips):** chgeos uses the same plan as PyCanopy
(`bench/spatial_bench/queries/q11.py`): one spatial join for the pickup zone and one for the
dropoff zone, matched on `t_tripkey`. The spatialbench SQL nests the second join inside the
first; the answer is the same. PyCanopy still wins SF1 (2.47 s vs 3.40 s); chgeos wins SF10.
DuckDB times out.

**Q12 (kNN):** WASM `st_knn` finds candidates with a k-d tree over building centres, then
measures the exact distance to each candidate's outline. chgeos wins SF1 and ties PyCanopy at
SF10; DuckDB times out.