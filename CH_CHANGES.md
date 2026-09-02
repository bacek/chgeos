# ClickHouse Changes vs Upstream

Four independent areas: WASM runtime extensions, a columnar call ABI
(ColumnBinary), a spatial-predicate join engine, and spatial pruning at the
storage layer.

---

## 1. WebAssembly UDF Runtime

The upstream WASM UDF system (row-at-a-time MsgPack ABI) was extended in several ways.

**Aggregate functions (UDAFs).** A new `is_aggregate = 1` setting in `CREATE FUNCTION`
registers a WASM export as an aggregate. The runtime calls `addBatchSinglePlace` to push
batches of rows into the accumulator, then serializes/deserializes state across merge
boundaries. MsgPack encoding is used for UDAF state transport.

~~**DETERMINISTIC constant folding.** Adding `DETERMINISTIC` to `CREATE FUNCTION` opts a
WASM UDF into CH's constant-folding pipeline. Three cooperating changes were needed: the
function registration records the flag, the analyzer propagates it through the function
node, and the query resolver evaluates constant WASM calls at planning time rather than
execution time. This allows expressions like `st_geomfromtext('POINT(0 0)')` to be
evaluated once and reused.~~ ([PR #100005](https://github.com/ClickHouse/ClickHouse/pull/100005), merged)

**Dynamic block splitting.** Before calling a WASM UDF, the runtime now checks how much
linear memory is available in the WASM instance and splits the input block if needed to
stay within the instance's 4 GB address space. This prevents OOM kills for wide or
high-cardinality input batches. ([PR #116552](https://github.com/ClickHouse/ClickHouse/pull/116552), open)

~~**system.functions visibility.** WASM UDFs now appear in `system.functions` with their
full argument list and return type, matching the behaviour of built-in functions.~~
([PR #101053](https://github.com/ClickHouse/ClickHouse/pull/101053), merged)

~~**Cranelift SIGILL on aarch64-apple-darwin.** A Cranelift E-Graph compiler bug triggered
SIGILL; the workaround disables the E-Graph optimization pass for that target. Fixed
upstream independently (our [PR #103487](https://github.com/ClickHouse/ClickHouse/pull/103487) was closed in favour of the upstream fix).~~

**Buffer data pointer enforcement.** A buffer declaring a non-zero size must not point at
linear-memory offset 0; `WasmMemoryManagerV01::getMemoryView` now throws `WASM_ERROR`
instead of reading whatever sits there. Shipped in [PR #116548](https://github.com/ClickHouse/ClickHouse/pull/116548)
("Document the WASM UDF buffer data pointer requirement"), open.

---

## 2. ColumnBinary Wire Format

A call ABI that replaces row-at-a-time MsgPack for bulk predicate and scalar
evaluation. The wire frame is defined in `ColumnBinaryWire.h` and is registered
user-facing as the **`ColumnBinary` serialization format** under the regular
`ABI BUFFERED_V1` path (FormatFactory, `ColumnBinaryInputFormat` /
`ColumnBinaryOutputFormat`). chgeos registers every function via
`serialization_format = 'ColumnBinary'`; the dedicated legacy columnar-ABI
registration branch in `UserDefinedWebAssembly.cpp` is no longer used by chgeos
and is a removal candidate on the fork side.
Upstream: [PR #104424](https://github.com/ClickHouse/ClickHouse/pull/104424) ("Add the `ColumnBinary` format"), open.

**Motivation.** The MsgPack path makes one host↔WASM boundary crossing per row. At 6M
rows per query this creates measurable overhead from serialization and repeated boundary
crossings. The columnar format sends all N rows in a single call as a typed column buffer.

**Format.** Each argument is a length-prefixed typed buffer. Supported column types:
fixed-width scalars (bool, int32, int64, float64), variable-length bytes (WKB geometry,
strings), and nullable variants of each. Arrays are supported via `COL_COMPLEX` for
aggregate return types.

**Constant column optimization.** A `COL_IS_CONST` flag marks columns that have the same
value for every row in the batch. The WASM side reads one value and broadcasts it rather
than reading N identical copies. For spatial predicates, this triggers `PreparedGeometry`
construction on the constant side, amortizing the GEOS index build cost over the whole
batch.

**Extracted and tested.** The format is defined in `ColumnBinaryWire.h` with a standalone
unit test suite (`gtest_column_binary`), separate from the WASM execution machinery.

---

## 3. Spatial Predicate Join

A new `IJoin` implementation (`SpatialRTreeJoin`) that uses an R-tree index on the right
side to accelerate spatial predicate joins, bypassing the O(N×M) cross-join that CH would
otherwise produce.

### Build phase

On first call, `addBlockToJoin` extracts the geometry (WKB) from the designated right-side
column, computes its bounding box, and inserts the bbox into a Boost.Geometry R-tree. For
distance predicates (`st_dwithin`), the bbox is expanded by the distance argument before
insertion. The right-side blocks themselves are retained in memory for result projection.
Build is serialized by a mutex; probe is fully parallel.

### Probe phase

`joinBlock` is called once per left-side block (concurrently across CH's pipeline threads).
For each left row, the R-tree is queried for right-side bboxes that intersect the expanded
left bbox, producing a candidate set. Candidates are grouped by whichever side has fewer
distinct geometries; that side is wrapped as `ColumnConst` so the WASM predicate call
builds a GEOS `PreparedGeometry` once per group. Only candidates that pass the precise
spatial predicate are written to the output block.

The output block is built by inserting only matched rows directly — not by allocating the
full candidate set and filtering it afterward. At SF10 scale (60M trips, 0.0045° distance
predicate) the candidate set per call can reach millions of rows; building it in full
before filtering caused GB-scale intermediate allocations and malloc heap corruption under
concurrent probe threads.

### Query rewriting

Spatial predicate joins are typically written as comma joins with a WHERE clause
(`FROM trip, building WHERE st_dwithin(...)`). A new analyzer pass
(`SpatialPredicateJoinPass`) detects this pattern and rewrites it into explicit
`JOIN ... ON` form. The planner then routes the join to `SpatialRTreeJoin` instead of a
hash or cross join, and `spatial_expand_arg` metadata tells the join which argument index
carries the distance parameter for bbox expansion.

### LEFT JOIN

Unmatched left rows are tracked during the probe pass and appended to the output with NULL
right columns afterward, satisfying LEFT JOIN semantics.

---

## 4. Spatial Pruning

Three storage layers gained the ability to skip data based on a spatial predicate's
bounding box before reading any rows.

### Shared infrastructure

~~A new virtual method `IFunctionBase::isSpatialPredicate()` identifies spatial predicates
without hardcoding function names. It propagates through the function adaptor chain so
wrapped variants (e.g., `FunctionVariantAdaptor`) are also recognized. All spatial
functions (`st_within`, `st_intersects`, `st_dwithin`, etc.) return `true`. The pruning
layers call this to decide whether a filter qualifies for bbox-based skipping.~~

~~A shared `GeoBbox` type in `Common/GeoBbox.h` provides the bounding-box accumulator
reused by all three pruning layers.~~

### GeoParquet

~~GeoParquet files that follow the `covering.bbox` convention store the bounding box of each
geometry as separate `xmin/ymin/xmax/ymax` columns. CH now reads these at two granularities:~~

~~- **Row-group level.** Column statistics (`min`/`max`) for the `covering.bbox` columns are
  read from the Parquet file footer. Row groups whose aggregate bbox doesn't intersect the
  query's spatial filter are skipped entirely, before any data pages are decompressed.~~

~~- **Page level.** If the file has a Parquet column index, the per-page min/max bounds for
  the `covering.bbox` columns are used to skip individual data pages within a row group.~~

~~The spatial filter is extracted from the query plan via the `KeyCondition` hyperrectangle
pipeline, so any function that implements `isSpatialPredicate()` participates
automatically. New `ProfileEvents` track the number of row groups and pages skipped.~~
([PR #104435](https://github.com/ClickHouse/ClickHouse/pull/104435), "Geoparquet rowgroup pruning", merged)

### Iceberg

Iceberg manifest files can record per-data-file bounds under a `covering.bbox` naming
convention. Two changes were made:

- **Write path.** When writing Iceberg data files, the writer computes and records the
  geometry column's bounding box in the manifest entry.

- **Read path.** `ManifestFilesPruning` compares the recorded bbox against the spatial
  predicate's bounding box and excludes manifest entries (and their data files) that
  cannot intersect, before any data files are opened.

### MergeTree skip index

A new index type `spatial_bbox` can be declared on a geometry column (stored as WKB) in a
MergeTree table. At index build time, the granule's bounding box is accumulated over all
rows. At query time, granules whose recorded bbox doesn't intersect the spatial filter's
geometry are skipped, reducing the number of rows read by the storage engine.
([PR #104437](https://github.com/ClickHouse/ClickHouse/pull/104437), "Add spatial_bbox skip index for MergeTree geometry columns", open)

---

## 5. WASM Function Chain Fusion

A new query plan pass (`WasmChainFusionPass`) detects consecutive WASM scalar UDF calls
where the output of one function feeds directly into the input of the next, and fuses them
into a single host↔WASM boundary crossing.

**Motivation.** Each WASM call boundary requires serializing the output of one function
into a ClickHouse column (WKB bytes for geometry), then deserializing it as input to the
next. For geometry-heavy pipelines (e.g. `st_length(st_makeline(a, b))`) this doubles the
WKB encoding work and allocates an intermediate column that is immediately consumed.

**Mechanism.** Functions declare that they support chaining by implementing
`can_chain_execute(names, n)` in WASM — returning 1 means the WASM module can execute the
named sequence in a single invocation without exporting intermediate results. The pass
detects linear chains in the expression DAG where all intermediate results are single-use
WASM outputs, replaces the chain with a single `WasmChain` function node that carries the
fused export name, and registers a corresponding WASM binding via the chain ABI.

**Results.** Q7 (`st_length(st_makeline(...))` over 6M rows): 2.5 s → fused from 4.5 s
(−44 %). Q5 (`st_area(st_convexhull(...))` per group): 1.54 s → fused from 1.84 s (−16 %).
