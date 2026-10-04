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

## 17. Q4 anatomy + two dead levers (2026-10-04 08:00, load ~3)

Q4 = top-1000 trip build (probe.sh 185-208 ms) + zone probe scan (805 ms warm) ≈ 1.0-1.17 s; join itself ~0.
- Zone-scan knob sweep (5 runs each): mpt16 866, mpt24 860, block 8192 823, block 4096+mpt16 812,
  max_streams_for_file_reading=5 849, max_threads=48 887, default 805 → all flat/worse. DEAD.
- Build without t_pickuploc is 91 ms, so reading pickuploc for 60M rows costs ~100 ms. Two-phase rewrite
  (`PREWHERE t_tripkey IN (top-1000 keys)`, equivalent since t_tripkey is unique) = 178 vs 185 ms, CPU 2.98 vs 1.79 s. DEAD:
  late materialization doesn't skip pages because the top-1000 rows are scattered across all row groups.
Q4 at ~1.0-1.17 s vs DuckDB 1.08 is read-bound parity; nothing left on the chgeos/join side.

## 18. Q7 anatomy at bench settings (2026-10-04 08:00, load ~7)

probe.sh, exact bench settings (mt12, mpt4, block 8192), interleaved x2:
| variant | ms | CPU-s |
|---|---|---|
| full (st_length(st_makeline)) | 1019 / 990 | 11.9 / 11.7 |
| read-only (length(a)+length(b)) | 725 / 716 | 4.9 / 4.8 |
| native L2Distance(readWKBPoint) | 926 / 914 | 8.6 / 8.6 |
WASM adds ~280 ms / 7 CPU-s over the read. Even the pure-native expression adds ~200 ms / 3.7 CPU-s and stays above
DuckDB's 0.84 s. So the Q7 gap is the CH read (0.72 s) plus per-row column materialization, not GEOS. The WASM-specific part is ~90 ms.
Closing it needs zero-copy guest access to CH column buffers (big design). Not started.
perf -p <server> only samples idle threads (logger/sleep): server-attached perf is useless here; use clickhouse-local children.
Next target: Q11 (45.6 s vs PyCanopy 43.0 s) — §"Q11 serial tail": ~13 s at 1-2 cores from SpatialRTreeJoin stragglers.

## 19. Q11 straggler track — anatomy + hypothesis 3 (probe split) DEAD

Q11 SF10 = two chained SpatialRTreeJoins (not the fused double join): join1 = zones (left, 455k rows) probing a tree of 60M trips;
join2 = join1 output (31M rows) probing the zone tree. processors_profile_log (baseline 46.9 s):
join1 24 streams busy 344 s, per-stream 6.1-25.0 s (median 13.1); join2 busy 407 s, 7.9-30.1 s (median 16.5), input 351k-3.0M rows/stream.
Total join busy 751 s ≈ 31 s on 24 cores vs 47 s wall. Large-row mode (kLargeHit=4M) is never hit here: no single zone has 4M trips.
- Parquet block 1024 / 8192 for the whole query: 77 s / 67 s (trip read gets slower). DEAD.
- SplitChunksTransform (cut probe side into ≤N-row chunks before the join's existing resize), interleaved off/8192/1024 ×2, load 5→14:
  Q11 wall 45.3/43.6/44.5 then 51.3/55.3/56.8; CPU-s 600/691/747 then 643/749/826; Q10 33.0/29.5/27.5 then 30.5/33.6/34.8.
  CPU +15-28% (smaller chunks → less per-zone grouping in evaluateAndEmit), wall noise-level. DEAD; stashed in CH as "spatial probe split (dead, 2026-10-04)".
Hypothesis (2) kLargeHit is moot for Q11 (never reached). Next: why per-stream cost varies 4× with equal row counts — cost is per-zone polygon complexity.

## 20. Q11 straggler — hypothesis 1 (parallel group eval) INCONCLUSIVE, reverse orientation DEAD

- Reorder Q11 so trips probe zone trees in both joins (`FROM trip JOIN zone JOIN zone`): >200 s, killed. Zone-probe orientation is right.
- Parallel predicate-group evaluation inside evaluateAndEmit (shared ThreadPool, threshold 100k candidates, env A/B; first version
  used a function-static pool which blocked server shutdown >60 s → restart refused; fixed by a per-join member pool):
  Q11 wall off/par8 pairs: 45.9/42.5, 46.1/42.9, 44.8/42.6, 44.4/45.3, 44.1/48.7 (last with processors log); par16 43.2, 45.3.
  CPU-s +12% (600→680). Tail: join1 max 25.3→34.3 s (worse), join2 max 28.9→20.5 s (better). Load 8-15 throughout (external).
  Net: no reliable wall win, consistent CPU cost. Stashed in CH as "spatial parallel group eval (inconclusive, 2026-10-04)".
- Hypothesis 2 (kLargeHit) moot: never reached on Q11. Hypothesis 3 (probe split) DEAD (§19).
Track status: Q11 stays ~44-46 s vs PyCanopy 43.0 s. Remaining idea: fused SpatialRTreeDoubleJoin for this shape (join1 tree on trips is
built at FillingRightJoinSide busy 82 s) — needs plan change, not attempted.

## 21. Q10/Q11 goal — host overhead in SpatialRTreeJoin::evaluateAndEmit (CH 77700fc9f0a) — WIN

joinBlock trace (send_logs_level=trace) on SF10 Q10: 405 s join total, only 47.6 s WASM (12%). Host bookkeeping dominated.
Three fixes, verified individually by single runs (load 4-8):
1. Left-run grouping: candidates arrive in ascending left_row order; when average run >= 4, group by runs instead of building
   `unordered_map<right_row, vector>` over every candidate (millions for zone-probes-trips) and then a second left map.
   Q10 34.5 -> 21.9 s (join total 405 -> 220 s).
2. Column pointers resolved once per joinBlock instead of `getByName()` per copied row (pred-block build + output emission).
   Q10 21.9 (flat), Q11 45 -> 37.3 s.
3. Copy only predicate-required columns into the predicate input block (was: every right column per candidate).
   Q10 -> 18.2 s (join 142 s).
bench_sf 3 runs: Q10 19.1/19.3/19.6 s, Q11 35.1/36.5/37.3 s. verify_sf SF10 Q1-Q11 11/11 PASS.
vs competitors: Q11 36.5 < PyCanopy 43.0 (met). Q10 19.3 vs Sedona 17.0 (still -12%).
Q10 profile now: evaluateAndEmit self 12%, ColumnString::insertFrom 5%, rtree pack 6.6%, joinBlock 6%.

### 22. Parallel group eval re-tested on 77700fc — DEAD

Re-applied stash on top of the host-overhead fix. Interleaved A/B (AB_PAR_GROUPS 1/8/1/8), SF10, load 7-13:

| arm | Q10 | Q11 | Q11 CPU-s |
|---|---|---|---|
| par=1 | 19.3 / 18.0 s | 42.1 / 35.2 s | 487 / 493 |
| par=8 | 17.6 / 19.6 s | 34.5 / 34.8 s | 565 / 578 |

Round 2 (cleaner): Q11 −1%, Q10 +9%; CPU +16%. Round-1 par=1 Q11 is an outlier. Not a win; re-stashed as "spatial parallel group eval (dead on 77700fc, 2026-10-04)". Q10 remaining gap (~18.9 vs Sedona 17.0) is not group-eval parallelism.

### 23. kd-partitioned sub-trees — WIN on Q11 (CH commit below)

runPostBuildPhase built 24 sub-trees from contiguous slices of arrival-order entries → every tree spans the whole extent, every probe walks all 24. Now kd bisection (nth_element on box centres, alternating axes, recursive threads) gives spatially disjoint trees.

Uncapped (3 rounds): Q11 30.4s vs 37.6s head, CPU 355 vs 490; **Q10 +15%** (20.9 vs 18.3s) — serial top-level nth_element on the 60M-trip build. Capped at 10M entries (trip build keeps contiguous slices), 2 rounds:

| arm | Q10 | Q11 | Q11 CPU-s |
|---|---|---|---|
| head | 18.1 / 18.2 | 34.7 / 43.3 | 490 / 502 |
| kd ≤10M | 17.6 / 18.1 | 31.1 / 26.6 | 379 / 370 |
| kd uncapped | 20.8 / 20.7 | 30.1 / 32.6 | 361 / 359 |

Committed the capped variant. verify_sf SF10 Q1–Q11 all PASS. Q10 still ~18 s vs Sedona 17.0 — next: parallel top-level partition (sample median + parallel std::partition) so the 60M build also gets disjoint trees without the serial cost.

### 24. Parallel grid-scatter for the >10M build — DEAD

Sampled 4×6 quantile grid, per-thread count+scatter into disjoint cells, one tree per cell (no serial nth_element). Env A/B on 08e9f5beba7, 3 rounds:

| arm | Q10 | Q11 | Q10 CPU-s |
|---|---|---|---|
| off | 20.3 / 17.9 / 18.0 | 29.9 / 28.1 / 28.7 | 168 / 153 / 153 |
| scatter | 21.1 / 20.1 / 20.2 | 30.9 / 29.5 / 31.3 | 143 / 142 / 144 |

Q10 CPU −7% but wall +2 s, same as uncapped kd. So the Q10 loss with spatially disjoint trip trees is not nth_element build cost; disjoint trees cut work but lose parallelism (hypothesis: a dense-Manhattan zone's candidates now come from 1–2 trees, so per-probe latency concentrates on hot zones and the straggler tail grows). Reverted. Also confirms 08e9f5beba7 Q11 ≈ 28–30 s.

### 25. Large-row path name lookups — WIN, Q10 goal met

Q10 anatomy (08e9f5beba7): 19.5 s wall / 160 CPU-s (~8 of 24 cores); JoiningTransform max 13.7 s vs avg 6.85 s; hottest joinBlock 9.06 s total vs 2.99 s WASM. System-wide perf of the tail window: Block::findPositionByName 11.7% + name-index hash find 9.7% + getPositionByName 5.3% + getByName 4.5% ≈ 31%. Source: processLargeRow (kLargeHit rows) still did getByName per candidate per column and per output row per column, and copied every left/right column into pred_block — 77700fc only fixed evaluateAndEmit.

Fix: reuse right_columns_by_name / out_left_columns / out_right_columns, and only predicate_required columns into pred_block. Interleaved A/B, 3 rounds, load 8–11:

| arm | Q10 | Q11 | Q10 CPU-s |
|---|---|---|---|
| 08e9f5beba7 | 18.3 / 17.4 / 18.7 | 27.5 / 28.8 / 27.3 | 153 / 152 / 151 |
| fix | 14.3 / 14.4 / 14.0 | 26.6 / 25.7 / 26.3 | 147 / 150 / 148 |

Q10 −22% → ~14.2 s, beats Sedona 17.0. Q11 −5% → ~26 s vs PyCanopy 43.0. verify_sf SF10 Q1–Q11 all PASS. Both Q10/Q11 competitor targets met.

### 26. kd cap 10M → 1M for SF1 (CH 4cae932a453)

SF1 Q10/Q11 had drifted from the morning baseline (2.62/5.86 s) to 3.27/6.41 s in the 5-run suite. SF1's 6M-trip build fell under the 10M kd cap. Env A/B on SF1, 2 rounds × 3 runs, load 4–10:

| cap | Q10 | Q11 |
|---|---|---|
| 10M | 2.54 / 2.65 | 6.07 / 6.11 |
| 1M | 2.60 / 2.45 | 5.90 / 5.94 |

Q11 −3%, back within 1–2% of morning; Q10 noise (the 3.27 suite figure was noise, A/B shows ~2.5–2.6 either way). Zone tree (same file at all SFs) stays kd-partitioned, SF10 trip build was already above the cap. SF10 check at 4cae932a453 (load 12): Q10 14.11 s, Q11 28.06 s (vs 14.05/26.87 suite at load 19 — inside noise). verify_sf SF1 Q10/Q11 and SF10 Q8–Q11 PASS.
