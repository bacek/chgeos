# ClickHouse changes compared to upstream

chgeos uses a ClickHouse fork with changes in five areas:

1. Extensions to the WebAssembly (WASM) UDF runtime.
2. A new columnar call interface, COLUMNAR_V1.
3. A join engine for spatial predicates.
4. Spatial pruning at the storage layer.
5. Fusion of chained WASM function calls.

The struck-through items below are merged upstream.

---

## 1. WebAssembly UDF runtime

Upstream ClickHouse has two WASM call interfaces. `ROW_DIRECT` calls the module one time per
row. `BUFFERED_V1` sends a full block in one call, serialized in a row format such as MsgPack.
The fork extends the runtime as follows.

Aggregate functions (UDAFs). The setting `is_aggregate = 1` in `CREATE FUNCTION` registers a
WASM export as an aggregate function. The runtime calls `addBatchSinglePlace` to send batches of
rows into the accumulator. It serializes the state between merge steps with MsgPack.

~~**DETERMINISTIC constant folding.** Adding `DETERMINISTIC` to `CREATE FUNCTION` opts a
WASM UDF into CH's constant-folding pipeline. Three cooperating changes were needed: the
function registration records the flag, the analyzer propagates it through the function
node, and the query resolver evaluates constant WASM calls at planning time rather than
execution time. This allows expressions like `st_geomfromtext('POINT(0 0)')` to be
evaluated once and reused.~~ ([PR #100005](https://github.com/ClickHouse/ClickHouse/pull/100005), merged)

~~**Dynamic block splitting.** Before calling a WASM UDF, the runtime now checks how much
linear memory is available in the WASM instance and splits the input block if needed to
stay within the instance's 4 GB address space. This prevents OOM kills for wide or
high-cardinality input batches.~~
([PR #116552](https://github.com/ClickHouse/ClickHouse/pull/116552), "Split WASM UDF input blocks by estimated memory size", merged 2026-09-07)

Whole-batch input measurement. This change follows the block splitting above. The runtime
chooses the batch size from the measured size of a complete candidate batch. It does not add
the sizes of single rows. Thus the fixed cost of each block (`LowCardinality` dictionaries and
the structure prefixes of `Dynamic` and `Variant` columns) counts one time per batch, and not
one time per row. ([PR #118989](https://github.com/ClickHouse/ClickHouse/pull/118989), open)

~~**system.functions visibility.** WASM UDFs now appear in `system.functions` with their
full argument list and return type, matching the behaviour of built-in functions.~~
([PR #101053](https://github.com/ClickHouse/ClickHouse/pull/101053), merged)

~~**Cranelift SIGILL on aarch64-apple-darwin.** A Cranelift E-Graph compiler bug triggered
SIGILL; the workaround disables the E-Graph optimization pass for that target. Fixed
upstream independently (our [PR #103487](https://github.com/ClickHouse/ClickHouse/pull/103487) was closed in favour of the upstream fix).~~

~~**Buffer data pointer enforcement.** A buffer declaring a non-zero size must not point at
linear-memory offset 0; `WasmMemoryManagerV01::getMemoryView` now throws `WASM_ERROR`
instead of reading whatever sits there.~~ Shipped in [PR #116548](https://github.com/ClickHouse/ClickHouse/pull/116548)
("Document the WASM UDF buffer data pointer requirement"), merged 2026-09-07.

---

## 2. COLUMNAR_V1 wire format

COLUMNAR_V1 is a call interface (ABI) for scalar functions and predicates. Like `BUFFERED_V1`,
it sends a full block of rows in one call. The difference is the encoding. `BUFFERED_V1` with
MsgPack encodes the block row by row, and the WASM side decodes every value. COLUMNAR_V1 sends
each column as one typed buffer, in the same layout as the ClickHouse column in memory.

`src/Formats/ColumnBinaryWire.h` defines the frame. ClickHouse can use the frame in two ways:

- As the `ColumnBinary` serialization format on the normal `ABI BUFFERED_V1` path, through
  `FormatFactory`, `ColumnBinaryInputFormat` and `ColumnBinaryOutputFormat`.
- Through the separate `ABI COLUMNAR_V1` registration in `UserDefinedWebAssembly.cpp`.

chgeos registers both, so you can compare them in benchmarks. Upstream:
[PR #104424](https://github.com/ClickHouse/ClickHouse/pull/104424) ("Add the `ColumnBinary`
format"), open.

A frame starts with a 16-byte header: magic `CBIN`, version, row count and column count. The
reader rejects an unknown magic or version. Then there is one 40-byte descriptor for each
column, with the type and the offsets of the null map, the offsets array and the data. The
column types are:

- `COL_BYTES`: variable-length bytes (strings and WKB geometry).
- `COL_FIXED8` to `COL_FIXED64`: fixed-width scalars. They are copied with one `memcpy`.
- `COL_COMPLEX`: `Array`, `Tuple` and `Map`, with a recursive layout.
- `COL_VARIANT`: `Variant`, with a discriminator for each row.
- `COL_FIXEDN`: other fixed widths, such as `UUID`, `Int128` and `FixedString(N)`.
- `COL_LOWCARD`: `LowCardinality`, as a dictionary and an index array.

Two flags change a type. `COL_IS_NULLABLE` adds a null map with one byte for each row.
`COL_IS_CONST` marks a constant column, and the frame stores only one row of it. The WASM side
reads the value one time and uses it for all rows. For a spatial predicate, the constant side
becomes a `PreparedGeometry`, so the cost of the GEOS index is paid one time per call.

The format has its own unit tests (`gtest_column_binary_wire.cpp`), separate from the WASM
execution code.

---

## 3. Spatial predicate join

`SpatialRTreeJoin` is a new `IJoin` implementation. It puts an R-tree index on the right side of
a spatial join. An R-tree is a tree of bounding boxes that finds the boxes that overlap a given
box. Without it, ClickHouse compares every left row with every right row (O(N×M)).

### Build phase

For each right-side block, `addBlockToJoin` reads the geometry (WKB) from the right-side
geometry column and computes its bounding box. For a distance predicate (`st_dwithin`), it
expands the box by the distance argument. The entries of each block stay in their own chunk, so
build threads do not copy the full right side under a lock. The right-side blocks stay in
memory for the output.

When all blocks are in, the join builds one sub-tree per CPU core, in parallel. Each sub-tree
is a static, packed R-tree. Its entries are sorted along a Hilbert curve (a path that keeps
nearby points close together) with one radix sort. The nodes are stored in flat arrays. If all
right-side entries are points, a leaf stores only the coordinates and the row position, not a
full box.

### Probe phase

ClickHouse calls `joinBlock` one time for each left-side block, on several pipeline threads at
the same time. For each left row, the join searches the sub-trees for right-side boxes that
intersect the expanded left box. The result is a set of candidates.

The join groups the candidates by the side that has fewer distinct geometries. That side goes
to WASM as a `ColumnConst`, so the WASM predicate builds a GEOS `PreparedGeometry` one time per
group. The output contains only the candidates that pass the exact spatial predicate.

The join adds matched rows directly to the output. It does not first build the full candidate
set and then filter it. At SF10 (60 million trips, distance 0.0045°), one call can have
millions of candidates. The old approach allocated gigabytes of temporary data and corrupted
the malloc heap when several probe threads ran together.

Large work items run on a shared thread pool:

- A left row with millions of candidates is evaluated in batches in parallel.
- When most probe threads are finished, a remaining probe recruits idle threads for its
  groups. Thus one slow block does not run alone at the end of the join.

The join copies candidate geometries and output columns one column at a time, and prefetches
the next right-side rows. The right-side rows are in random order, so this removes many cache
misses.

### Query rewriting

Spatial joins are often written as comma joins with a `WHERE` clause
(`FROM trip, building WHERE st_dwithin(...)`). A new analyzer pass, `SpatialPredicateJoinPass`,
finds this pattern and rewrites it as `JOIN ... ON`. The planner then sends the join to
`SpatialRTreeJoin`, not to a hash join or a cross join. The `spatial_expand_arg` metadata tells
the join which argument holds the distance for the box expansion.

### LEFT JOIN

During the probe, the join records which left rows have no match. After the probe, it adds
these rows to the output with NULL right-side columns, as `LEFT JOIN` requires.

---

## 4. Spatial pruning

Three storage layers can skip data before they read rows. They use the bounding box of the
spatial predicate.

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

Iceberg manifest files can record the bounds of each data file, with the `covering.bbox` naming
convention. The fork changes two paths:

- Write path. When ClickHouse writes an Iceberg data file, it records the bounding box of the
  geometry column in the manifest entry.
- Read path. `ManifestFilesPruning` compares the recorded box with the box of the spatial
  predicate. It removes the manifest entries, and their data files, that cannot intersect. It
  does this before it opens any data file.

### MergeTree skip index

`spatial_bbox` is a new index type for a geometry column (stored as WKB) in a MergeTree table.
When ClickHouse builds the index, it records the bounding box of all rows in each granule. At
query time, it skips the granules whose box does not intersect the geometry of the spatial
filter. Thus the storage engine reads fewer rows.
([PR #104437](https://github.com/ClickHouse/ClickHouse/pull/104437), "Add spatial_bbox skip index
for MergeTree geometry columns", open)

---

## 5. WASM function chain fusion

`WasmChainFusionPass` is a new query plan pass. It finds WASM scalar UDF calls where the output
of one call is the input of the next. It merges such a chain into one crossing of the boundary
between ClickHouse and WASM.

Without fusion, ClickHouse serializes the output of the first function into a column (WKB
bytes for geometry), and the next function parses it again. For geometry pipelines such as
`st_length(st_makeline(a, b))`, this doubles the WKB work and allocates a temporary column that
is used only one time.

A WASM module declares which chains it supports. It exports `can_chain_execute(names, n)`,
which returns 1 if the module can run the named sequence in one call, without returning the
intermediate results. The pass finds linear chains in the expression graph where each
intermediate result is a WASM output that is used only one time. It replaces each chain with
one `WasmChain` function node that has the name of the fused export. It then registers that
export through the chain ABI.

Benchmark queries Q5 (`st_area(st_convexhull(...))`) and Q7 (`st_length(st_makeline(...))`) use
fused chains.
