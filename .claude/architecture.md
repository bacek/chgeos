# chgeos Architecture

## Wire formats

All functions use `ABI BUFFERED_V1` plus a `SETTINGS serialization_format = '...'` that selects the frame layout. **ColumnBinary is the default protocol** — canonical (no-suffix) functions and `_cb` functions are the same protocol: same WASM dispatch (`columnar_impl_wrapper`), same frame, same `is_spatial_predicate` / `spatial_expand_arg` behavior. The only difference is the SQL name.

### ColumnBinary (default)
- **Serialization:** `serialization_format = 'ColumnBinary'` (FormatFactory, CH fork — input/output formats in `src/Formats/ColumnarV1Wire.h`)
- **WASM side:** `src/columnar.hpp` — `BufHeader` (8B: `num_rows` u32 + `num_cols` u32) + `ColDescriptor[num_cols]` (40B each: `type` u64, `null_offset` u64, `offsets_offset` u64, `data_offset` u64, `data_size` u64) + data blocks. All offset arrays are u64; no null terminators on the wire.
- **ColType (u64 low byte):** 0=BYTES, 1–4=FIXED8/16/32/64, 5=COMPLEX (Array/Tuple, recursive), 6=VARIANT, 7=FIXEDN, 8=LOWCARD. Modifier bits: `COL_IS_NULLABLE 0x20`, `COL_IS_CONST 0x80`.
- **Constants:** `COL_IS_CONST` columns are sent **once**, not N times — a 169 KB filter polygon costs 169 KB regardless of row count. The wrapper detects this and builds a `PreparedGeometry` once (STR-tree).
- **Geometry arguments** arrive as `COL_BYTES` EWKB (the `COL_VARIANT` path also decodes, but chgeos emits plain EWKB strings).
- **CH side:** O(1) size precomputation (`buildColDescriptor` computes exact sizes → preallocate → fill) and in-place decode from `IColumn::assumeMutable()` pointers — zero-copy where the column type matches.
- **Exports:** bare name (canonical) and `name_cb`.
- `st_knn` is hand-written (export `st_knn` in `columnar.hpp`); `st_knn_cb` is a SQL alias to it.

### Buffers
- **Serialization:** `serialization_format = 'Buffers'`
- **WASM side:** `src/buffers.hpp` — native CH layout (`num_cols` u64, `num_rows` u64, per-column: `is_null` u8, `is_const` u8, CH type u8, `null_map`, `data`); type id matches `DataTypeEnum`
- **Exports:** `name_buffers`

### RowBinary
- **Serialization:** `serialization_format = 'RowBinary'`
- **WASM side:** `src/rowbinary.hpp` — per-type read/write (WKB `String` via `readBinaryBuffer`, numeric as raw LE); null = `UInt8(0)` per row before the row data
- **Exports:** `name_rb` (also `st_intersects_extent_rb`, the extent predicate)

### MsgPack
- **Serialization:** `serialization_format` unset (MsgPack is the `BUFFERED_V1` default)
- **WASM side:** `src/msgpack.hpp` — row-at-a-time via `read_row_from_buffer`
- **Exports:** `name_mp`; still the canonical path for the handful of functions without a ColumnBinary variant (a few I/O helpers) and for the CH native type converters

## Source layout

```
src/
  main.cpp              — all UDF registrations (macros only, no logic)
  columnar.hpp          — ColumnBinary wire format, ColView, columnar_impl_wrapper, st_knn
  col_binary.hpp        — CH_UDF_CB* macros (thin shim; _cb exports)
  rowbinary.hpp         — RowBinary wire format, rowbinary_impl_wrapper
  buffers.hpp           — Buffers (Native CH) wire format
  msgpack.hpp           — MsgPack wire format, impl_wrapper
  mem.hpp / mem.cpp     — raw_buffer, clickhouse_create_buffer, runtime stubs
  chain.hpp             — CHain engine (clickhouse_can_chain_execute / chain_execute)
  col_prep_op.hpp       — ColPrepOp / ColPrepDistOp / ColPrepPointOp / ColWkbScalarOp
  clickhouse_types.hpp  — generated; scripts/gen_clickhouse_types.py
  ch_column_binary_string.hpp — in-place ColumnString decode helpers
  functions/
    predicates.hpp      — st_*_impl functions + prepared/point callbacks
    overlay.hpp         — st_union_agg_impl, st_area_impl, etc.
    accessors.hpp, constructors.hpp, io.hpp, processing.hpp, transforms.hpp
    knn.hpp, flat.hpp, ch_to_wkb.hpp, bench.hpp
  geom/
    wkb.hpp / wkb.cpp   — read_wkb, write_ewkb, read_wkt, write_wkt
    wkb_envelope.hpp    — BBox, wkb_bbox(), BboxOp
    wkb_point.hpp, wkb_distance.hpp, wkb_cursor.hpp
    chgeom.hpp, filters.hpp, flat_batch.hpp, subdivide.hpp
```

## columnar_impl_wrapper

`src/columnar.hpp` defines a single template:

```cpp
template <typename Ret, typename... Args>
raw_buffer* columnar_impl_wrapper(
    raw_buffer* ptr, uint32_t,
    Ret (*impl)(Args...),
    BboxOp         bbox_op      = nullptr,  // fast bbox short-circuit before WKB parse
    bool           early_ret    = false,    // bbox miss → true (for st_disjoint)
    ColPrepOp      prep_a       = nullptr,  // PreparedGeometry when col(0) is const
    ColPrepOp      prep_b       = nullptr,  // PreparedGeometry when col(1) is const
    ColPrepDistOp  prep_a_dist  = nullptr,  // dist variant for st_dwithin, col(0) const
    ColPrepDistOp  prep_b_dist  = nullptr,
    ColPrepPointOp prep_a_point = nullptr,  // A-const polygon, B varies as raw WKB points
    ColPrepPointOp prep_b_point = nullptr,
    ColWkbScalarOp wkb_scalar   = nullptr)  // 1-arg accessor read straight from WKB
```

The wrapper handles: parsing `BufHeader` + `ColDescriptor[num_cols]`, extracting per-column data (respecting `COL_IS_CONST`), bbox short-circuit, constant-geometry → `PreparedGeometry` path, raw-WKB point fast path, calling `_impl` per row, and writing the result column.

Return type dispatch (via `if constexpr`): `bool`→COL_FIXED8, `double`→COL_FIXED64, `int32_t`→COL_FIXED32, `unique_ptr<Geometry>`→COL_NULL_BYTES, `string`→COL_BYTES.

Adding a new function:
1. Implement `name_impl` in the appropriate `functions/` header
2. Add the registration macro call in `main.cpp`
3. Add `CREATE OR REPLACE FUNCTION name` (ColumnBinary) + suffixed variants in `clickhouse/create.sql`

## PreparedGeometry optimization

When a geometry column is `COL_IS_CONST` (constant across all rows), the wrapper:
1. Parses the WKB once
2. Builds a `PreparedGeometry` (STR-tree spatial index) once
3. Calls the prep callback for each row instead of re-parsing

**ColPrepOp** `bool (*)(const PreparedGeometry*, const Geometry*)` — 2-arg predicates.
**ColPrepDistOp** `bool (*)(const PreparedGeometry*, const Geometry*, double)` — st_dwithin.
**ColPrepPointOp** `bool (*)(geos::Location)` — raw-WKB point fast path (no per-row GEOS object).
**ColWkbScalarOp** `std::optional<double> (*)(std::span<const uint8_t>)` — 1-arg accessors (st_x, st_y, …) read directly from WKB bytes.

Predicates are callbacks defined in `predicates.hpp`, e.g.:

| Predicate | prep_a (col 0 const) | prep_b (col 1 const) |
|---|---|---|
| st_contains | `pa->contains(b)` | `pb->within(a)` |
| st_within | `pa->within(b)` | `pb->contains(a)` |
| st_covers | `pa->covers(b)` | `pb->coveredBy(a)` |
| st_intersects | `pa->intersects(b)` | `pb->intersects(a)` |
| st_disjoint | `pa->disjoint(b)` | `pb->disjoint(a)` |
| st_equals | `pa->getGeometry().equals(b)` | `pb->getGeometry().equals(a)` |
| st_dwithin (dist) | `pa->isWithinDistance(b, d)` | `pb->isWithinDistance(a, d)` |

## Registration macros

| Macro | Export name | Wire format |
|-------|------------|-------------|
| `CH_UDF_COL(name)` / `CH_UDF_COL_BBOX2` / `CH_UDF_COL_BBOX2_POINT` / `CH_UDF_COL_PRED3` / `CH_UDF_COL_WKB1` / `CH_UDF_COL_COMPLEX` / `CH_UDF_COL_POINT` | `name` (canonical) | ColumnBinary |
| `CH_UDF_CB*` (same family, in `col_binary.hpp`) | `name_cb` | ColumnBinary |
| `CH_UDF_BUFFERED(name)` | `name_buffers` | Buffers |
| `CH_UDF_ROWBINARY(name)` | `name_rb` | RowBinary |
| `CH_UDF_PACKED_*` (in `msgpack.hpp`) | `name_mp` | MsgPack |

The `CH_UDF_COL*` and `CH_UDF_CB*` macros are identical except for the export name — both dispatch to `columnar_impl_wrapper`.

`create.sql` registers every function: canonical ColumnBinary name (predicates carry `is_spatial_predicate = 1`, st_dwithin also `spatial_expand_arg = 2`), `_cb`, `_buffers`, `_rb` where available, `_mp` alias, and a few bare `_mp`/`_rb` functions.

## WASM export naming

- `name` — ColumnBinary, canonical (default for all functions)
- `name_cb` — ColumnBinary, explicit suffix (benchmark suite)
- `name_buffers` — Buffers
- `name_rb` — RowBinary
- `name_mp` — MsgPack
- `st_intersects_extent_rb` — RowBinary extent predicate (`is_spatial_predicate = 1`, `spatial_expand_arg = 1`)
- `clickhouse_chain_execute` / `clickhouse_can_chain_execute` — CHain engine (`chain.hpp`)

## Spatial pruning

The CH fork (26.8.1) supports two spatial pruning paths for the spatial-predicate join engine, which picks an implementation based on available indexes (see `CH_CHANGES.md`):

- **R-tree join (preferred):** the `SpatialRTreeJoin` engine uses an R-tree index built from the `is_spatial_predicate = 1` function + `spatial_expand_arg` for O(log N) spatial lookups.
- **Storage-layer pruning (fallback):** when no R-tree index is available, the fork prunes row groups: GeoParquet (per-column bbox metadata), Iceberg (manifest partition bounds), MergeTree (spatial skip index).

The UDF still runs on surviving rows for exact evaluation — pruning only eliminates row groups that provably cannot match.

## Key files

| File | Role |
|------|------|
| `src/clickhouse_types.hpp` | generated — do not edit; `python3 scripts/gen_clickhouse_types.py ../ClickHouse` |
| `src/geom/chgeom.hpp` | Geography = Tuple(Int32 srid, Geometry), namespace `ch::geography` |
| `src/columnar.hpp` | ColumnBinary wire format (parsing, writing), `columnar_impl_wrapper` (all types, prepared, bbox), `st_knn` |
| `src/col_binary.hpp` | `CH_UDF_CB*` macros — `_cb` exports (shim over columnar.hpp) |
| `src/rowbinary.hpp` | RowBinary wire format (per-type read/write) |
| `src/buffers.hpp` | Buffers (Native CH) wire format |
| `src/msgpack.hpp` | MsgPack wire format (CH native type converters, a few legacy functions) |
| `src/chain.hpp` | CHain engine — `clickhouse_can_chain_execute` + `clickhouse_chain_execute` |
| `src/main.cpp` | WASM exports (module init, `st_*` registration for all formats) |
| `src/geos_wrapper.hpp` | GEOS C++ API wrapper (parse, write, ops) |
| `src/wkt_parser.hpp` | WKT → GEOS parser |
| `clickhouse/create.sql` | All `CREATE FUNCTION` definitions (ColumnBinary canonical + `_cb` + `_buffers` + `_rb` + `_mp`) |
| `clickhouse/config-test.xml` | Server config (WASM enabled, spatial index settings) |
| `clickhouse/test_e2e.py` | End-to-end test via clickhouse-local |

## Runtime stubs

Native (`clickhouse_throw`, `clickhouse_random`, `clickhouse_log`) in `tests/test_functions.cpp`.
WASM-only (`__cxa_*`, `getentropy`, `__assert_fail`) in `src/mem.cpp` under `#ifdef __wasi__`.
CH WasmTime provides WASI preview1 stubs via `define_wasi()` + `set_wasi()` in `WasmTimeRuntime.cpp`.
The `chgeos` executable is only built when cross-compiling (native build skips it; native build runs the gtest suite instead — see `tests/`).
