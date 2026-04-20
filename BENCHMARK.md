# chgeos Benchmark Results

Comparison of chgeos (ClickHouse + GEOS WASM UDFs) against DuckDB spatial extension
and Apache Sedona (SedonaDB) on the spatial benchmark suite.

**Hardware:** Apple M-series, 12 cores  
**Dataset:** synthetic taxi trip data from https://github.com/apache/sedona-spatialbench — SF1 = 6M trips, SF10 = 60M trips. 
**Timeout:** 120 s (chgeos, Sedona), 600 s (DuckDB SF10)  
**chgeos version:** 2026-04-20 (after SpatialRTreeJoin pre-spatial filter + heap-corruption fix)  
**DuckDB version:** v1.5.2  
**Sedona version:** recorded 2026-04-19

Note: it's running with CraneLift optimization disabled. CraneLift fix
https://github.com/bytecodealliance/wasmtime/pull/12841 is not in ClickHouse
yet. But it won't make _much_ difference. Probably.

---

## SF1 — 6 Million Trip Rows

| Query | Description                        | chgeos   | DuckDB  | Sedona  | Winner   |
|-------|------------------------------------|----------|---------|---------|----------|
| Q1    | Point-in-radius filter             | 0.124 s  | 0.15 s  | 0.36 s  | chgeos   |
| Q2    | Count trips in county polygon      | 0.079 s  | 0.20 s  | 0.42 s  | chgeos   |
| Q3    | Monthly stats in bbox+buffer       | 0.124 s  | 0.13 s  | 0.43 s  | chgeos   |
| Q4    | Zone distribution (top-1000 tips)  | 0.595 s  | 0.41 s  | 0.59 s  | DuckDB   |
| Q5    | Convex hull area per customer/month| 1.587 s  | 0.73 s  | 1.63 s  | DuckDB   |
| Q6    | Zone stats for bbox-intersect zones| 0.283 s  | 0.41 s  | 0.54 s  | chgeos   |
| Q7    | Detour ratio (all trips)           | 4.71 s   | 3.74 s  | 1.59 s  | Sedona   |
| Q8    | Nearby pickups per building        | 0.172 s  | 0.43 s  | 0.40 s  | chgeos   |
| Q9    | Building conflation via IoU        | 0.028 s  | 0.03 s  | 0.29 s  | chgeos   |
| Q10   | Zone avg duration/distance         | 8.20 s   | TIMEOUT | 4.63 s  | Sedona   |
| Q11   | Cross-zone trip count              | 15.6 s   | TIMEOUT | 7.62 s  | Sedona   |
| Q12   | 5 nearest buildings per trip (kNN) | 25.5 s   | TIMEOUT | 15.7 s  | Sedona   |

**SF1 wins — chgeos: 6, DuckDB: 2, Sedona: 4**

---

## SF10 — 60 Million Trip Rows

| Query | Description                        | chgeos   | DuckDB   | Sedona   | Winner   |
|-------|------------------------------------|----------|----------|----------|----------|
| Q1    | Point-in-radius filter             | 0.70 s   | 2.15 s   | 3.06 s   | chgeos   |
| Q2    | Count trips in county polygon      | 0.70 s   | 3.20 s   | 3.83 s   | chgeos   |
| Q3    | Monthly stats in bbox+buffer       | 0.56 s   | 3.10 s   | 5.70 s   | chgeos   |
| Q4    | Zone distribution (top-1000 tips)  | 0.97 s   | 1.22 s   | 1.67 s   | chgeos   |
| Q5    | Convex hull area per customer/month| 20.4 s   | 508 s    | 108 s    | chgeos   |
| Q6    | Zone stats for bbox-intersect zones| 1.62 s   | 4.66 s   | 4.82 s   | chgeos   |
| Q7    | Detour ratio (all trips)           | 46.5 s   | —        | 39.3 s   | Sedona   |
| Q8    | Nearby pickups per building        | 2.60 s   | 8.51 s   | 9.49 s   | chgeos   |
| Q9    | Building conflation via IoU        | 0.104 s  | 0.21 s   | 0.41 s   | chgeos   |
| Q10   | Zone avg duration/distance         | TIMEOUT  | —        | 72.3 s   | Sedona   |
| Q11   | Cross-zone trip count              | OOM      | TIMEOUT  | 115 s    | Sedona   |
| Q12   | 5 nearest buildings per trip (kNN) | TIMEOUT  | TIMEOUT  | —        | —        |

**SF10 wins — chgeos: 8, DuckDB: 0, Sedona: 3, none: 1**

---

## Notes

**Q7 (detour ratio):** Scans all 6M/60M rows computing `st_length(st_makeline(...))` with no
spatial join. Sedona leads at both scales; chgeos and DuckDB are bottlenecked by the per-row
geometry construction cost at this volume. DuckDB did not run Q7 at SF10.

**Q9 (building IoU):** Self-join of ~20K buildings. SpatialRTreeJoin now evaluates
non-spatial ON conditions (e.g. `b1.id < b2.id`) as a pre-filter before the spatial
predicate, cutting spatial evaluations from 20K (including self-pairs) to ~74.
chgeos leads at both scales: 0.028 s at SF1 (vs DuckDB 0.03 s, Sedona 0.29 s) and
0.104 s at SF10 (vs DuckDB 0.21 s, Sedona 0.41 s).

**Q10/Q11 at SF10:** chgeos uses the SF10 zone dataset (454K globally-scoped zones vs
~265 NYC zones at SF1). Joining 454K polygon zones against 60M trips is infeasible
within the 120 s timeout; Sedona completes in 72–115 s. This is a scale/resource limit,
not a correctness issue.

**Q12 at SF10:** Global kNN over 60M trips × 20K buildings exceeds 120 s for both
chgeos and DuckDB. Sedona did not run Q12.

**Q5 at SF10:** chgeos (20 s) is 5× faster than Sedona (108 s) and 25× faster than
DuckDB (508 s). The `query_plan_execute_functions_after_sorting=0` hint is required to
keep the WASM convex hull running on parallel threads before the ORDER BY merge.
