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

## SF1: 6 million trip rows

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

SF1 wins: chgeos 10, DuckDB 0, Sedona 0, PyCanopy 1, ties 1.

The SF1 results make DuckDB look slower than it is. The SF1 trip data (`sf1/trip/*.parquet`,
2 files from upstream) has only 4 row groups. DuckDB reads one row group per thread, so it ran
these queries on 4 of the 24 threads of the machine at most. At SF10, the 15 trip files have 45
row groups, which is enough for all threads. ClickHouse splits row groups into smaller parts
and uses all threads at both scales. Thus some SF1 wins, mainly Q1 to Q3, come partly from the
DuckDB thread limit. We cannot measure how much, because DuckDB also has a fixed cost of about
0.15 s for each query. Use SF10 as the main comparison. This limit does not change the SF10 Q7
result (see the Q7 note).

![SF1 benchmark](sf1.png)

---

## SF10: 60 million trip rows

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

SF10 wins: chgeos 9, DuckDB 1, Sedona 0, PyCanopy 0, ties 2.

We measured chgeos on 2026-10-05 at CH `d2d24230309`, with 3 runs for each query. Other work
ran on the machine at the same time (load about 17), so the chgeos times can be a little too
high. All 12 queries return the correct answers at both scales. We measured the other engines
on 2026-10-04 and 2026-10-05 on an idle machine, with 5 runs for each query and a 120 s
timeout.

Q10 to Q12 became faster after 2026-10-04. At SF10, Q10 went from 14.1 s to 8.0 s, Q11 from
25.7 s to 14.3 s, and Q12 from 32.5 s to 19.2 s. There are two changes:

- The spatial join builds its index faster (2.4 s to 1.3 s for 60M trips). It moves the work
  of the largest zones to idle threads, and it copies matched rows faster.
- The nearest-building search keeps its index between batches. It tests the closest candidates
  first and stops when the answer is known.

![SF10 benchmark](sf10.png)

### chgeos over time

![chgeos query times over time](history.png)

The chart shows the speedup of each query since its first measurement. It uses the last
measurement of each day from the git history of `benchmark_results.json`
(`scripts/plot_history.py`). Q4 and Q6 are below 1× because the zone file grew to 156k zones
worldwide. chgeos did not become slower: DuckDB became slower by the same amount on Q4. Dotted
lines mark points where the numbers before and after are not directly comparable:

- 05-06: a new machine.
- 08-06: corrected queries. This changed what Q12 measures.
- 10-04: the change to the split data files from upstream.

---

## Notes

### Q4 and Q6

Both queries read the full zone file, which is the same file at every scale. Most of their time
goes to reading Parquet, not to spatial work. Thus the three fastest engines finish close
together.

### Q5 (convex hull for each customer)

Two settings are important for this query:

- `query_plan_execute_functions_after_sorting=0` keeps the hull calculation on parallel
  threads. Without it, ClickHouse runs the calculation after the sort, on one thread, and the
  query is about 7× slower.
- At SF10, the query also needs `max_bytes_ratio_before_external_group_by=0`. Its 3.1M groups
  use about 15 GiB, and with the default setting ClickHouse writes them to disk.

`st_collect_agg` runs as a normal function on the result of `groupArray`. This was faster than
a native aggregate function.

### Q7 (detour ratio)

The query computes `st_length(st_makeline(...))` for every trip. It has no join. chgeos joins
the two functions into one WASM call, and works on plain coordinate arrays. It does not build a
GEOS geometry for each row. chgeos is the fastest engine at SF1. DuckDB is faster at SF10
(0.81 s against 0.91 s). DuckDB reads the points directly from the Parquet bytes, but chgeos
still parses each WKB value one time.

### Q9 (building IoU)

The query joins about 20K buildings with themselves. The join tests the non-spatial condition
(`b1.id < b2.id`) before the spatial test, and this removes most pairs. All engines finish in
less than a second, near the limit of what these runs can measure.

### Q10

chgeos is the fastest engine at both scales. DuckDB stops at the timeout at SF10.

### Q11 (trips between zones)

chgeos uses the same plan as PyCanopy (`bench/spatial_bench/queries/q11.py`). One spatial join
finds the pickup zone, a second join finds the dropoff zone, and the results are matched on
`t_tripkey`. The spatialbench SQL puts the second join inside the first. The answer is the
same. PyCanopy is faster at SF1 (2.47 s against 3.40 s). chgeos is faster at SF10. DuckDB stops
at the timeout.

### Q12 (kNN)

The WASM function `st_knn` finds candidates with a k-d tree of building centres. Then it
measures the exact distance to the outline of each candidate. chgeos is the fastest engine at
SF1 and ties with PyCanopy at SF10. DuckDB stops at the timeout.
