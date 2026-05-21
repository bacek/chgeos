# chgeos Architecture

## Three wire formats (overview)

**MsgPack** (`src/msgpack.hpp`, `src/mem.hpp`):
- One call per row; ClickHouse serializes each row as a msgpack sequence
- `impl_wrapper(buf, n, fn_impl)` — unpacks args row by row
- Registered via `CH_UDF_FUNC` macro

**RowBinary** (`src/rowbinary.hpp`):
- One call per batch; generic `rowbinary_impl_wrapper` deduces types from `_impl`
- Registered via `CH_UDF_RB_ONLY` / `CH_UDF_RB_BBOX2` macros

**COLUMNAR_V1** (`src/columnar.hpp`):
- One call for all N rows; ClickHouse sends columns (not rows)
- Constant columns (`COL_IS_CONST` flag) send one value broadcast to all rows
- `columnar_impl_wrapper(buf, n, fn_impl, ...)` — single generic template
- Registered via `CH_UDF_COL` / `CH_UDF_COL_BBOX2` / `CH_UDF_COL_PRED3` macros

## Source layout

```
src/
  main.cpp              — all UDF registrations (macros only, no logic)
  columnar.hpp          — COLUMNAR_V1 wire format, ColView, columnar_impl_wrapper
  rowbinary.hpp         — RowBinary wire format, rowbinary_impl_wrapper
  msgpack.hpp           — MsgPack wire format, impl_wrapper
  mem.hpp / mem.cpp     — raw_buffer, clickhouse_create_buffer, etc.
  col_prep_op.hpp       — ColPrepOp and ColPrepDistOp type aliases
  functions.hpp         — includes all function headers
  functions/
    predicates.hpp      — st_*_impl functions + ColPrepOp/ColPrepDistOp callbacks
    overlay.hpp         — st_union_agg_impl, st_area_impl, etc.
    accessors.hpp, constructors.hpp, io.hpp, processing.hpp, transforms.hpp
  geom/
    wkb.hpp / wkb.cpp   — read_wkb, write_ewkb, read_wkt, write_wkt
    wkb_envelope.hpp    — BBox, wkb_bbox(), BboxOp
```

## columnar_impl_wrapper

```cpp
template <typename Ret, typename... Args>
raw_buffer* columnar_impl_wrapper(
    raw_buffer* ptr, uint32_t,
    Ret (*impl)(Args...),
    BboxOp        bbox_op     = nullptr,   // fast bbox short-circuit
    bool          early_ret   = false,     // bbox miss → true (for st_disjoint)
    ColPrepOp     prep_a      = nullptr,   // PreparedGeometry when col(0) is const
    ColPrepOp     prep_b      = nullptr,   // PreparedGeometry when col(1) is const
    ColPrepDistOp prep_a_dist = nullptr,   // dist variant for st_dwithin, col(0) const
    ColPrepDistOp prep_b_dist = nullptr);  // dist variant for st_dwithin, col(1) const
```

Return type dispatch (via `if constexpr`): `bool`→COL_FIXED8, `double`→COL_FIXED64,
`int32_t`→COL_FIXED32, `unique_ptr<Geometry>`→COL_NULL_BYTES, `string`→COL_BYTES.

## PreparedGeometry optimization

When a geometry column is `COL_IS_CONST` (constant across all rows), the wrapper:
1. Parses the WKB once
2. Builds a `PreparedGeometry` (STR-tree spatial index) once
3. Calls the prep callback for each row instead of re-parsing

**ColPrepOp** `bool (*)(const PreparedGeometry*, const Geometry*)` — for 2-arg predicates.
**ColPrepDistOp** `bool (*)(const PreparedGeometry*, const Geometry*, double)` — for st_dwithin.

Callbacks are defined as `constexpr` non-capturing lambdas in `predicates.hpp`:

| Predicate | prep_a (col 0 const) | prep_b (col 1 const) |
|---|---|---|
| st_contains | `pa->contains(b)` | `pb->within(a)` |
| st_within | `pa->within(b)` | `pb->contains(a)` |
| st_covers | `pa->covers(b)` | `pb->coveredBy(a)` |
| st_coveredby | `pa->coveredBy(b)` | `pb->covers(a)` |
| st_intersects | `pa->intersects(b)` | `pb->intersects(a)` |
| st_disjoint | `pa->disjoint(b)` | `pb->disjoint(a)` |
| st_overlaps | `pa->overlaps(b)` | `pb->overlaps(a)` |
| st_crosses | `pa->crosses(b)` | `pb->crosses(a)` |
| st_touches | `pa->touches(b)` | `pb->touches(a)` |
| st_containsproperly | `pa->containsProperly(b)` | `nullptr` |
| st_equals | `pa->getGeometry().equals(b)` | `pb->getGeometry().equals(a)` |
| st_dwithin (dist) | `pa->isWithinDistance(b, d)` | `pb->isWithinDistance(a, d)` |

## Registration macros (main.cpp)

```cpp
// MsgPack / RowBinary (exported as name_mp)
CH_UDF_FUNC(name)                           // MsgPack
CH_UDF_RB_ONLY(name)                        // RowBinary
CH_UDF_RB_BBOX2(name, bbox_op, early_ret)   // RowBinary + bbox shortcut

// COLUMNAR_V1 (exported as name_col)
CH_UDF_COL(name)                            // Generic — all types deduced from name_impl
CH_UDF_COL_BBOX2(name, bbox_op, early_ret)  // + bbox + PreparedGeometry (2-arg predicates)
CH_UDF_COL_PRED3(name)                      // + PreparedGeometry dist (3-arg: geom,geom,double)
```

Adding a new function:
1. Implement `name_impl` in the appropriate `functions/` header
2. Add the macro call in `main.cpp`
3. Add `CREATE OR REPLACE FUNCTION name_mp` / `name_col` in `clickhouse/create.sql`
4. Add the canonical alias `CREATE OR REPLACE FUNCTION name AS (...) -> name_col(...)` (or `_mp`)

## ColumnBinary wire format (`src/col_binary.hpp`)

Fourth wire format, newer than COLUMNAR_V1. Symmetric layout: input and output use the same frame.

**Input (CH → WASM):**
```
num_columns (u32LE) | num_rows (u32LE)
per column:
  flags (u8, bit0=IS_CONST) | type_tags_size (u32LE) | type_tags (N bytes) | data_size (u64LE) | data
```

**Output (WASM → CH, always single column):**
```
num_columns (u32LE) | num_rows (u32LE) | flags (u8) | type_tags_size (u32LE) | type_tags | data_size (u64LE) | data
```

**Type tags** — recursive stream for auto-coercion (narrow-to-wide integer promotion):
```
Int8=0x01 UInt8=0x02 Int16=0x03 UInt16=0x04 Int32=0x05 UInt32=0x06
Int64=0x07 UInt64=0x08 Float32=0x09 Float64=0x0A String=0x0B
Nullable=0x0C  Array=0x0D  Tuple=0x0E
```
`type_tags_size == 0` → no tags (backward compatible).

**Data layout by type:**
- Fixed-width scalars (bool/int/float): packed tightly, `sizeof(T)` bytes per row
- String/geometry output: `ColCBBytesWriter` — u32[N+1] offset table prepended, then raw bytes
- String/geometry input: same u32 offset table layout (new), or legacy varint-prefixed blobs
- Array(T): `[N u64 per-row counts][u32[total_M+1] cumul offsets][element data]`

**Key types:**
- `ColBinaryInfo` — parsed column metadata (is_const, type_tags, data pointer)
- `ColBinaryBuf` — result of `parse_col_binary()`: num_rows, vector of ColBinaryInfo
- `ColBinaryReader` — sequential cursor over one column; handles const/non-const, u32-offset/varint, auto-coercion via `read_from_tag<T>()`
- `ColCBBytesWriter` — output helper for variable-length columns; manages u32 offset table in-place

**`col_binary_impl_wrapper`** — mirrors `columnar_impl_wrapper` with same PreparedGeometry + bbox optimizations, plus a `ColPrepPointOp` fast path for point-in-polygon via `IndexedPointInAreaLocator`:

```cpp
template <typename Ret, typename... Args>
raw_buffer* col_binary_impl_wrapper(
    const char* func_name,
    raw_buffer* ptr, uint32_t num_rows,
    Ret (*impl)(Args...),
    BboxOp         bbox_op      = nullptr,
    bool           early_ret    = false,
    ColPrepOp      prep_a       = nullptr,
    ColPrepOp      prep_b       = nullptr,
    ColPrepDistOp  prep_a_dist  = nullptr,
    ColPrepDistOp  prep_b_dist  = nullptr,
    ColPrepPointOp prep_a_point = nullptr,   // IndexedPointInAreaLocator (A-const, point input)
    ColPrepPointOp prep_b_point = nullptr);  // IndexedPointInAreaLocator (B-const, point input)
```

**ColPrepPointOp** `bool (*)(IndexedPointInAreaLocator*, double x, double y)` — fires when a const polygon receives WKB point input; avoids full GEOS predicate.

**Registration macros (main.cpp):**
```cpp
CH_UDF_CB(name)                              // Generic — all types deduced from name_impl
CH_UDF_CB_BBOX2(name, bbox_op, early_ret)    // + bbox + PreparedGeometry (2-arg predicates)
CH_UDF_CB_BBOX2_POINT(name, bbox_op, early_ret) // + IPIAL point-in-polygon fast path
CH_UDF_CB_PRED3(name)                        // + PreparedGeometry dist (3-arg: geom,geom,double)
```

Functions are exported as `name_cb`. The `st_knn_cb` function is hand-written (not macro-generated) due to its `Array(Tuple(UInt64, Float64))` return type.
