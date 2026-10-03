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

### 3. Q2 SQL bbox-prefilter rewrite — DEAD
Bbox from st_envelope(poly) via readWKBPolygon, prefilter with readWKBPoint().1/.2 BETWEEN,
then st_intersects on survivors. Correct (5499) but 1052 ms vs 650 — native readWKBPoint over
60M rows costs more than the WASM bbox reject. Q2 residual: guest 4.0 + ser 3.0 CPU-s;
ser is mostly the 169 KB const resent each call (CH-side const caching = bigger project).

### 4. Q7 rewrite st_length(st_makeline(a,b)) -> st_distance(a,b) — NO GAIN (not applied)
Bit-identical output. With GEOS st_distance: 3.1s (worse). Added generic ColWkbPairOp
point-point fast path for st_distance (bit-exact vs GEOS, 2000-pair test): rewrite then
ties original (~1.87s). => Q7 geometry is not the bottleneck (scan/WKB read/marshal/sort are).
st_distance fast path kept as a generic improvement.

### 5. Q6 probe — no lever in SpatialRTreeJoin
processors_profile: JoiningTransform 12.25 busy-s/24 streams, File 7.7 s. Right side (filtered
zones) < 200k entries → single sub-tree, so a global-bbox early reject = the rtree root check.
Not built.

### 6. WASM guest per-row floor (calibration, SF10 60M rows)
| shape | wall | guest CPU-s | ser CPU-s |
|---|---|---|---|
| st_x(p) | 802 | 4.0 | 3.8 |
| st_distance(p,q) fast path | 1489 | 7.9 | 8.6 |
| st_length(st_makeline(p,q)) chain | 1425 | 13.1 | 7.3 |
st_x wrapper: **native 2.7 ns/row; WASM single-thread 20 ns/row (flat across batch 1k..1M);
WASM at 24 threads 67 ns/row.** => ~7x codegen + ~3x contention (bandwidth-bound, cf. memory:
box saturates ~6.5 GB/s memcpy). Per-call overhead is not the issue. The remaining Q1/Q2/Q7 gap
is the copy-in/copy-out architecture (zero-copy is what DuckDB has) — out of scope for this run.
Possible small follow-up: devirtualise ColWkbScalarOp/ColWkbPairOp (template param instead of fn
pointer) to cut the single-thread 20 ns; unlikely to matter at 24 threads.

## Final same-session (after e0810df + 9d77f48), SF10, 5 runs, verify Q1-Q11 PASS
| Q | chgeos before | chgeos after | DuckDB 1.5.6 | ratio after |
|---|---|---|---|---|
| Q1 | 535 | 534 | 580* | 0.92x |
| Q2 | 762 | 632 | 480 | 1.32x |
| Q3 | 550 | 565 | 480 | 1.18x |
| Q4 | 1071 | 1134 | 1140 | 0.99x |
| Q5 | 5644 | 5906 | 7150 | 0.83x |
| Q6 | 1970 | 2039 | 1620 | 1.26x |
| Q7 | 1816 | 1820 | 840 | 2.17x |
| Q8 | 1433 | 1456 | 1510 | 0.96x |
| Q9 | 99 | 99 | 130 | 0.76x |
*DuckDB Q1 is its first query (cold); earlier run gave 330.
chgeos CPU avg 84.5% during the suite. Only real move: Q2 −17%. Others within run-to-run drift.

## OVERNIGHT (started after user sign-off)

### 7. Per-query settings — LANDED (>=10% bar)
Sweep (.scratch/ar/sweep, sweep2; bracketing base runs agree within 2%). Kept:
| Q | base | setting | after | Δ |
|---|---|---|---|---|
| Q1 | ~498 | parquet block 32k | 423 | −15% |
| Q2 | ~615 | max_threads=8, max_parsing_threads=8, block 32k | 361 | −41% |
| Q3 | ~512 | block 32k | 449 | −12% |
| Q7 | ~1800 | max_threads=12, block 8k | ~1450 (noisy 1288–1999) | −19% |
| Q9 | ~95 | block 8k | 46 | −52% |
Q5 keeps query_plan_execute_functions_after_sorting=0. Rejected: Q4 mt8 (−2% confirm), anything for
Q5/Q6/Q8 (block 8k makes Q5 +19%, Q6 +27%; mt8 makes Q8 +60%). Uniform block size is dead (as before).
Implemented as QUERY_SETTINGS dict in bench_sf.py, shared by verify_sf.py. verify Q1-Q9 PASS.
Note: DuckDB Q1 330 / Q2 480 / Q3 460 / Q7 840 / Q9 140 → Q1, Q2, Q3(~), Q9 now at/under DuckDB.

## 8. Host bbox prefilter for const-arg spatial predicates — DEAD

CH COLUMNAR_V1 executeImpl: one const geometry arg → per-row wkbBBox, bbox-disjoint rows get per-function `bbox_disjoint_result`, only candidates go to guest (bail if >50% candidates).
Interleaved A/B (on/off/on/off, 5 runs, same binary, CPU 63–75%), on vs off ms:
Q1 440/432 vs 435/444 · Q2 359/361 vs 369/362 · Q3 453/444 vs 445/434 · Q4 1136/1080 vs 1101/1073 · Q6 2229*/1934 vs 1926/1911 · Q8 1487/1462 vs 1459/1466 (* noisy arm).
Zero gain: guest already rejects cheaply by bbox; cost is reading/marshalling the column, not the predicate.
Code: CH stash "wasm bbox_disjoint_result prefilter (dead, 2026-10-03)"; create.sql reverted.
Side bug (unfixed): `st_disjoint_cb` has is_spatial_predicate=1, but disjoint is TRUE for disjoint bboxes → unsafe for RTree join / Parquet GeoFilter. Flag should be cleared.

## Morning pick-list

Keep (already committed on autoresearch-2026oct03):
1. Const-polygon cache (e0810df): Q2 762→632, Q10/Q11 flat.
2. st_distance point-point WKB fast path (9d77f48): bit-exact, Q7 flat.
3. Per-query settings (5a30d3b): Q1 423 / Q2 361 / Q3 449 / Q7 ~1450 / Q9 46 ms. Q1/Q2/Q9 at or under DuckDB 1.5.6.

Decide:
4. Clear `is_spatial_predicate` on `st_disjoint_cb`. This is a correctness fix, with no perf impact expected.
5. Rebuild the CH binary. The running binary still contains the stashed prefilter code; it is inert because the default is -1.

Remaining gaps vs DuckDB 1.5.6: Q7 ~1.7x and Q6 ~1.2x. Both are bound by copy-in/out and the Parquet reader. Settings, fusion, the prefilter and wire tweaks are all exhausted.
The only structural lever left is zero-copy WASM input: map CH column buffers into guest memory. That is a large design change.

## 9. Q7 anatomy + max_parsing_threads=4 — LANDED

Probe decomposition (SF10, mt12, 8k blocks): no-loc 195ms/2.8 CPU-s; + read both WKB cols 990ms/13.6; full Q7 1471ms/25.0.
DuckDB read-only equivalent 610-710ms/~12 CPU-s → CH reader CPU same, not the gap.
Read-only Q7 does NOT scale with max_threads (t1 1022ms, t12 969ms): Parquet decode runs in the parsing pool.
Full Q7 CPU doubles t1→t12 (11.7→23.8 CPU-s) = contention inflation; perf: memcpy 18%, snappy 11%, kernel ~20%, guest JIT 27%.
Fewer parsing threads cut contention: probe pt4/8 full Q7 1137ms vs 1392 default, CPU 13-15 vs 24.
Clean bench sweep (bracketed): Q7 base 1255/1318 → pt4 1017 (−21%), pt8 1055, pt16 1095. Q1/Q3 lose with any pt cap; Q4/Q5/Q6/Q8/Q9 flat.
Landed: Q7 QUERY_SETTINGS += max_parsing_threads=4. verify Q7 PASS.
Q7 vs DuckDB 1.5.6 (840-860): ~1.2x now (was 1.7x).
Tooling notes: perf report on CH binary spawned addr2line (20 GB, swap full) → poisoned one sweep; flat reports only.

## 10. Prefilter retry with Nullable support — DEAD (again)

Root cause of attempt 1 being inert on Q6: z_boundary is Nullable(String), prefilter required ColumnString. Extended to Nullable varying arg + Nullable(UInt8) result.
Standalone Q6 build probe (count of zones intersecting the const polygon): 1096 → 865ms (−21%), same 18 rows.
Full suite interleaved A/B (on/off/on/off, 3 runs, CPU 74-80%): Q6 1880/1824 vs 1798/1817 — no gain; nothing else moves.
The build-side saving doesn't reach Q6 wall time. Code: CH stash "wasm bbox prefilter + Nullable (dead x2)".
Noted: Q7 now 915-925ms in these runs (with max_parsing_threads=4) vs DuckDB 840-860 → ~1.08x.

## 11. Switch bench default wire cb → col — LANDED

Found by accident: probe with plain (COLUMNAR_V1) function names ran Q1 in 279ms vs 441ms for the _cb (BUFFERED_V1 + ColumnBinary) names the bench uses.
Isolated to st_dwithin: col 330ms / 3.6 user-s vs cb 575ms / 8.5 user-s (cb: 2.7 s ColumnBinary serialization + slower guest); st_x at parity.
Guest code identical (same prep ops / fast paths both macros) → cost is the BUFFERED_V1 host marshal. (COLUMNAR path does not emit Wasm* ProfileEvents — those counters are BUFFERED-only.)
Interleaved bench col/cb/col/cb (3 runs): Q1 283/276 vs 440/420 (−35%), Q3 287/274 vs 446/421 (−35%), Q2 −9%, Q6 −8%, Q5 −5%, Q4 +6% (1127/1079 vs 1082/1019), Q7/Q8/Q9 parity. Q10/Q11 1-run parity (29.1/44.2 vs 28.7/44.0 s).
verify_sf (col) Q1-Q9 PASS. Default in bench_sf.py now --wire-protocol col.
Contradicts the Sep-12 "col vs cb parity" memory: that predates this session's col-side fast paths? No — guest is shared; most likely the cb host serializer regressed or Q1/Q3 const-arg handling differs; worth a CH-side look if cb must stay.
vs DuckDB 1.5.6: Q1 ~280 vs 330 (win), Q3 ~280 vs 460 (win), Q2 ~325 vs 480 (win).

## 12. Same-session head-to-head (col wire, all landed changes) — 2026-10-03 night, box load 20-25

| Q | chgeos a/b ms | DuckDB 1.5.6 | |
|---|---|---|---|
| Q1 | 295/314 | 360 | win 1.18x |
| Q2 | 342/344 | 480 | win 1.40x |
| Q3 | 324/299 | 520 | win 1.67x |
| Q4 | 1164/1150 | 1520 | win 1.31x |
| Q5 | 5796/5916 | 8500 | win 1.45x |
| Q6 | 1769/1677 | 1740 | parity |
| Q7 | 980/1025 | 850 | loss 1.18x |
| Q8 | 1447/1427 | 1530 | win 1.06x |
| Q9 | 47/43 | 140 | win 3x |
7 wins, 1 parity, 1 loss. Remaining targets: Q7, Q6.

## 13. Host bbox prefilter — LANDED (sections 8 and 10 were measurement errors)

Sections 8/10 called it dead. Both A/Bs ran on the **cb** wire (then the bench default); the prefilter lives in the COLUMNAR_V1 path only, so it never fired. The later "col beats cb by 35%" (section 11) was col **plus** prefilter: the server still had create_prefilter.sql loaded (reverted in git, never reloaded).
Clean col A/B (pre/no/pre/no, 3 runs): Q1 309/282 vs 425/418 (−29%), Q3 306/290 vs 432/426 (−30%), Q6 1728/1725 vs 1975/1959 (−12%), Q2 336/331 vs 362/356 (−7%), Q4 +3%, rest flat.
verify Q1-Q11 PASS. CH commit on autoresearch-2026oct03; create.sql adds bbox_disjoint_result to the 35 spatial predicates (=1 for st_disjoint_cb).
Harness trap found on the way: reload.sh's INSERT reads stdin in background jobs → hangs → functions vanish; use </dev/null.

## 14. SpatialRTreeJoin occupancy grid — WIN (CH 5cfbb7b5a04)

Hypothesis: Q8 probes 60M rows for 38.5k matches; each probe walks all sub-trees (~400ns/row). A 1024² occupancy
grid over build-side bboxes rejects most probes in O(1).
Guards: skip grid if right side >200k entries (Q4/Q6-style huge build sides); abort if >4M cells marked.
Interleaved A/B (env toggle, same binary, removed before commit), SF10 col, 3 runs median:

| arm    | Q4   | Q6   | Q8   | Q9 |
|--------|------|------|------|----|
| grid   | 1130 | 1735 | 538  | 48 |
| nogrid | 1125 | 1789 | 1447 | 41 |
| grid   | 1173 | 1750 | 552  | 42 |
| nogrid | 1129 | 1792 | 1459 | 45 |

Q8 −62%; Q4/Q6 flat. Earlier "Q4/Q6 +15-20%" reading was external load (root python3, load 20).
verify_sf SF10 Q1–Q11: 11/11 PASS.

## 15. Head-to-head after grid (2026-10-04 07:00, box load 7→29, external noise)

| Q | chgeos a/b avg ms | DuckDB 1.5.6 | |
|---|---|---|---|
| Q1 | 398/357 | 370 | parity |
| Q2 | 465/371 | 530 | win 1.1-1.4x |
| Q3 | 423/344 | 490 | win 1.2-1.4x |
| Q4 | 1246/1198 | 1220 | parity |
| Q5 | 6294/5570 | 8850 | win 1.4-1.6x |
| Q6 | 1906/1823 | 2010 | win 1.1x |
| Q7 | 1073/1051 | 960 | loss 1.1x |
| Q8 | 573/549 | 1640 | **win 3.0x** |
| Q9 | 47/42 | 130 | win 3x |

Q6 anatomy (probe.sh): zone build subquery alone 855ms/10 CPU-s + trip probe-side scan alone 877ms/16.6 CPU-s = whole Q6;
join itself ~0. Both reader-bound. Late materialization (PREWHERE on pickuploc) saves only 8% (810 vs 877):
matching rows are scattered, pages still decode → spatial runtime-filter pushdown is not worth building.
Q7 native rewrite `L2Distance(readWKBPoint(a), readWKBPoint(b))` is bit-identical but only −11% (1390→1235 probe);
read-bound; not adopted (bypasses the library for a marginal gain).

## 16. bbox prefilter on cb (buffered) path (CH, after 5cfbb7b5a04)

tryBboxPrefilter only wrapped run_columnar; buffered `execute()` never called it. Now both paths do.
verify_sf SF10 Q1-Q11 on cb: 11/11 PASS. Interleaved cb/col/cb/col, 3 runs, load ~15:
cb Q1 305/309 Q2 359/349 Q3 325/318 Q4 1087/1130 Q6 1730/1699 Q8 586/590
col Q1 306/293 Q2 345/358 Q3 318/311 Q4 1130/1114 Q6 1706/1725 Q8 539/543
cb now at parity with col on Q1-Q6 (was Q1 ~425, Q3 ~430 without prefilter). Q8 cb +8% (join path, unrelated).
