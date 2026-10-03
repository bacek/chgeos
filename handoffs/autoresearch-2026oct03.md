# Autoresearch 2026-10-03 — close DuckDB gap on SF10 Q1–Q9

Branch: `autoresearch-2026oct03` (no push). Scratch: `.scratch/ar/`.

## Rules (from user)
- No cheating: no ad-hoc fused/chained functions keyed on query shape (like the old CH-side st_makeline>st_length hack).
- Improving generic chain machinery inside chgeos is OK (flat tiers, tiling).
- Query rewrites OK if semantics + outputs preserved (verify_sf must pass); no data-specific constants.
- tg allowed (expected low value).
- Split layout only.

## Step 0 — baselines (same box, same session)

chgeos: `bench_sf.py --sf sf10 --runs 5`, cb wire, HEAD `d77a822`.
DuckDB: spatialbench `run_benchmark.py --engines duckdb --runs 5`, HF split files.
DuckDB 1.5.5 vs 1.5.6 (spatial ext `04270fe`) — 1.5.6 is the reference.

| Q | chgeos avg ms | DuckDB 1.5.5 | DuckDB 1.5.6 | ratio vs 1.5.6 | gap ms |
|---|---|---|---|---|---|
| Q1 | 535 | 700 | 330 | 1.62x | +205 |
| Q2 | 762 | 560 | 480 | 1.59x | +282 |
| Q3 | 550 | 570 | 460 | 1.20x | +90 |
| Q4 | 1071 | 1420 | 1070 | 1.00x | 0 |
| Q5 | 5644 | 8300 | 7370 | **0.77x win** | −1726 |
| Q6 | 1970 | 1730 | 1620 | 1.22x | +350 |
| Q7 | 1816 | 950 | 860 | 2.11x | +956 |
| Q8 | 1433 | 1630 | 1530 | **0.94x win** | −97 |
| Q9 | 99 | 140 | 140 | **0.71x win** | −41 |

Gap ranking (abs): Q7 956 > Q6 350 > Q2 282 > Q1 205 > Q3 90.
Raw: `.scratch/ar/base_ch.txt`, `base_duck.txt` (1.5.5), `base_duck156.txt`.

## Log

### 1. Q2 attribution (query_log ProfileEvents, SF10, avg/run)
| Q | wall | CPU-s | WASM total | guest | ser |
|---|---|---|---|---|---|
| Q1 | 547 | 6.1 | 3.0 | 2.1 | 0.76 |
| Q2 | 769 | 13.6 | 11.7 | 8.1 | 3.3 |
| Q3 | 562 | 6.3 | 3.2 | 2.3 | 0.77 |
| Q6 | 1848 | 26.5 | 6.4 | 4.3 | 2.0 |
| Q7 | 1724 | 33.4 | 22.8 | 13.3 | 7.9 |
| Q8 | 1469 | 31.5 | 2.0 | 0.6 | 0.5 |
Q2: only 8,990 of 60M points fall in the Coconino bbox (polygon 169 KB, 10.6k verts),
yet guest re-parsed + re-indexed the polygon every batch. Tiny-polygon control: 573ms / guest 3.0.

### 2. Const-polygon cache in point fast path — LANDED
`ConstAreaLocator` (columnar.hpp): single-entry, byte-equality-keyed cache of parsed
polygon + IPIAL; parse deferred until first point passes bbox; bind once per call.
- Q2 762 → 650 ms (−15%), Q10/Q11 unchanged (45.5s vs 45.3s old), verify Q1-Q11 PASS.
- **Bug found on the way**: first version bound (memcmp of full WKB) per *row* → Q11 >400s,
  CPU ~8% (stragglers). Always A/B Q10/Q11 when touching the point path.
- Also: WASM build was broken since tg was added (tg.c static_asserts assume 64-bit);
  tg removed from the WASM target (unused there).
- Q11 CPU avg 67-73% even on old guest = known SpatialRTreeJoin straggler tail.
- Tooling: `.scratch/ar/run_cpu.sh <tag> <queries> [runs]` = bench + CPU% sampling.
