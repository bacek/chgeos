# chgeos Benchmark Results

Comparison of chgeos (ClickHouse + GEOS WASM UDFs) against DuckDB spatial extension
and Apache Sedona (SedonaDB) on the spatial benchmark suite.

**Hardware:** AMD Ryzen 9 5900X, 128 GB RAM  
**Dataset:** synthetic taxi trip data from https://github.com/apache/sedona-spatialbench — SF1 = 6M trips, SF10 = 60M trips  
**Timeout:** 120 s (all engines)  
**chgeos version:** 2026-05-06  
**DuckDB version:** 1.5.2  
**Sedona version:** 0.3.0  
**Runs:** 5 per query (average reported)

---

## SF1 — 6 Million Trip Rows

| Query | Description                        | chgeos   | DuckDB   | Sedona  | Winner   |
|-------|------------------------------------|----------|----------|---------|----------|
| Q1    | Point-in-radius filter             | 0.090 s  | 0.11 s   | 0.43 s  | chgeos   |
| Q2    | Count trips in county polygon      | 0.078 s  | 0.20 s   | 1.13 s  | chgeos   |
| Q3    | Monthly stats in bbox+buffer       | 0.083 s  | 0.16 s   | 0.50 s  | chgeos   |
| Q4    | Zone distribution (top-1000 tips)  | 0.620 s  | 0.67 s   | 0.91 s  | chgeos   |
| Q5    | Convex hull area per customer/month| 0.857 s  | 0.82 s   | 2.00 s  | DuckDB   |
| Q6    | Zone stats for bbox-intersect zones| 0.814 s  | 0.97 s   | 0.87 s  | chgeos   |
| Q7    | Detour ratio (all trips)           | 1.285 s  | 6.53 s   | 2.35 s  | chgeos   |
| Q8    | Nearby pickups per building        | 0.146 s  | 0.46 s   | 0.35 s  | chgeos   |
| Q9    | Building conflation via IoU        | 0.025 s  | 0.03 s   | 0.24 s  | chgeos   |
| Q10   | Zone avg duration/distance         | 3.961 s  | TIMEOUT  | 4.87 s  | chgeos   |
| Q11   | Cross-zone trip count              | 7.337 s  | TIMEOUT  | 7.82 s  | chgeos   |
| Q12   | 5 nearest buildings per trip (kNN) | 10.431 s | TIMEOUT  | 18.07 s | chgeos   |

**SF1 wins — chgeos: 11, DuckDB: 1, Sedona: 0**

![SF1 benchmark](sf1.png)

---

## SF10 — 60 Million Trip Rows

| Query | Description                        | chgeos    | DuckDB   | Sedona   | Winner   |
|-------|------------------------------------|-----------|----------|----------|----------|
| Q1    | Point-in-radius filter             | 0.544 s   | 0.44 s   | 0.94 s   | DuckDB   |
| Q2    | Count trips in county polygon      | 0.669 s   | 0.70 s   | 1.64 s   | chgeos   |
| Q3    | Monthly stats in bbox+buffer       | 0.522 s   | 0.54 s   | 1.43 s   | chgeos   |
| Q4    | Zone distribution (top-1000 tips)  | 1.163 s   | 1.08 s   | 1.86 s   | DuckDB   |
| Q5    | Convex hull area per customer/month| 9.899 s   | 8.24 s   | 42.43 s  | DuckDB   |
| Q6    | Zone stats for bbox-intersect zones| 1.872 s   | 1.95 s   | 2.86 s   | chgeos   |
| Q7    | Detour ratio (all trips)           | 11.800 s  | 68.31 s  | 42.28 s  | chgeos   |
| Q8    | Nearby pickups per building        | 1.602 s   | 2.19 s   | 2.02 s   | chgeos   |
| Q9    | Building conflation via IoU        | 0.104 s   | 0.16 s   | 0.37 s   | chgeos   |
| Q10   | Zone avg duration/distance         | 27.314 s  | TIMEOUT  | 17.02 s  | Sedona   |
| Q11   | Cross-zone trip count              | 43.646 s  | TIMEOUT  | TIMEOUT  | chgeos   |
| Q12   | 5 nearest buildings per trip (kNN) | 107.090 s | TIMEOUT  | TIMEOUT  | chgeos   |

**SF10 wins — chgeos: 8, DuckDB: 3, Sedona: 1**

![SF10 benchmark](sf10.png)

---

## Notes

**Q5 (convex hull per customer):** The `query_plan_execute_functions_after_sorting=0`
hint is required to keep the WASM convex hull running on parallel threads before the
ORDER BY merge. Without it, ClickHouse defers the function to the single-threaded
post-sort stage, causing ~7× slowdown. DuckDB leads at SF10 (8.24 s vs chgeos 9.9 s);
Sedona is 4× slower (42 s).

**Q7 (detour ratio):** Scans all rows computing `st_length(st_makeline(...))` with no
spatial join. WasmChainFusionPass fuses `st_makeline → st_length` into a single WASM
call, eliminating the intermediate WKB round-trip. chgeos leads both DuckDB (68 s) and
Sedona (42 s) by a wide margin at SF10.

**Q9 (building IoU):** Self-join of ~20K buildings. SpatialRTreeJoin evaluates
non-spatial ON conditions (e.g. `b1.id < b2.id`) as a pre-filter before the spatial
predicate, cutting candidate pairs dramatically. chgeos leads at both scales.

**Q10 at SF10:** The one query where Sedona wins (17 s vs chgeos 27 s, DuckDB TIMEOUT).
Sedona's DataFusion task-parallel build/probe model handles the large trip build side
more efficiently. DuckDB cannot complete within 120 s.

**Q11 at SF10:** Sedona times out because it materializes the intermediate
trip×pickup_zone result before applying the second zone join, causing memory explosion
at SF10. chgeos handles both zone joins in a single `SpatialRTreeDoubleJoin` pass
(57 s vs TIMEOUT for both DuckDB and Sedona).

**Q12 (kNN):** WASM `st_knn` uses a static 2-D centroid k-d tree with branch-and-bound
search. Both DuckDB and Sedona time out at SF10; chgeos completes in 105 s.
