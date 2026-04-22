# chgeos — CLAUDE.md

PostGIS-compatible spatial UDF library for ClickHouse, compiled to WASM.
Uses GEOS 3.12+ for geometry operations. C++23.

## Build system

```bash
# Native build (tests only — can't link WASM binary with native clang)
cmake -B build -G Ninja          # first time only
ninja -C build                   # build + link chgeos_tests
ctest --output-on-failure -C build  # run all 261 tests

# WASM build (Emscripten, pre-configured in build_wasm/)
ninja -C build_wasm              # produces build_wasm/bin/chgeos.wasm
```

ClickHouse binary: `../ClickHouse/build/programs/clickhouse`
(sibling directory, built from source)

Inspect WASM exports: `wasm-tools dump build_wasm/bin/chgeos.wasm | grep <name>`

LSP shows many false-positive errors for GEOS/CH headers — ignore them. The real compiler is always the source of truth.

## Running ClickHouse server

Already running as a background process:
```
../ClickHouse/build/programs/clickhouse server \
  --config-file=clickhouse/config-test.xml \
  2>/tmp/ch-server.log &
```

After starting, wait for it to finish loading (WASM module takes ~40s):
```bash
for i in $(seq 1 60); do
  ../ClickHouse/build/programs/clickhouse client --port 19000 --query "SELECT 1" 2>/dev/null && echo "ready" && break
  sleep 1
done
```

Logs: `tail -f /tmp/ch-server.log`

Connect: `../ClickHouse/build/programs/clickhouse client --port 19000`

## Reloading chgeos.wasm after a build

Only needed when the **WASM binary changes** (`ninja -C build_wasm`). For pure ClickHouse C++ changes, just restart the CH server — no WASM reload required.

Use `scripts/reload.sh` (does all steps below automatically).

Must drop all functions first — ClickHouse won't delete a module in use:

```bash
CH="../ClickHouse/build/programs/clickhouse"
# 1. Drop all registered functions
grep -oE "^CREATE OR REPLACE FUNCTION [a-z0-9_]+" clickhouse/create.sql \
  | sed 's/CREATE OR REPLACE FUNCTION /DROP FUNCTION IF EXISTS /' \
  | sed 's/$/ ;/' \
  | $CH client --port 19000 --multiquery
# 2. Delete module
$CH client --port 19000 --query "DELETE FROM system.webassembly_modules WHERE name='chgeos'"
# 3. Copy WASM to user_files and insert
cp build_wasm/bin/chgeos.wasm tmp/data/user_files/chgeos.wasm
$CH client --port 19000 --query \
  "INSERT INTO system.webassembly_modules (name, code) VALUES ('chgeos', file('chgeos.wasm'))"
# 4. Recreate all functions
$CH client --port 19000 --multiquery < clickhouse/create.sql
```

Note: `system.webassembly_functions` does not exist. Use `system.functions WHERE origin != 'System'` to inspect registered UDFs.

## Benchmarks

SF1 data: `/Users/bacek/src/spatial-bench/sf1/` (6M trip rows)

```bash
BENCH_RUNS=5 ./scripts/bench_sf1.sh ../ClickHouse/build/programs/clickhouse \
  /Users/bacek/src/spatial-bench/sf1 Q1
```

Optional third argument filters to a single query (Q1, Q3, etc.).

For manual ad-hoc queries, run the CH client from the data directory so that
`file('trip.parquet')` resolves correctly:

```bash
CH=~/src/ClickHouse/build/programs/clickhouse
cd /Users/bacek/src/spatial-bench/sf1   # or sf10
$CH client --port 19000 --query "SELECT count() FROM file('trip.parquet', Parquet)"
```

Reference numbers after PreparedGeometry optimization (April 2026, 6M rows):
| Variant | Q1 avg | Q3 avg |
|---------|--------|--------|
| msgpack | 293ms  | 355ms  |
| col     | 113ms  | 101ms  |

## Architecture

### Three wire formats

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

### Function naming

Each function can have up to three SQL names registered in `clickhouse/create.sql`:

| Suffix | ABI | Example | Registered as |
|--------|-----|---------|---------------|
| `_mp` | MsgPack or RowBinary | `st_contains_mp` | direct WASM binding |
| `_col` | COLUMNAR_V1 | `st_contains_col` | direct WASM binding |
| *(none)* | SQL alias | `st_contains` | `AS (a, b) -> st_contains_col(a, b)` |

Canonical (unsuffixed) aliases point to `_col` when available, `_mp` otherwise.
Users call canonical names; suffixed names are available for explicit path selection.

### Source layout

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

### columnar_impl_wrapper

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

### PreparedGeometry optimization

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

### Registration macros (main.cpp)

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

## Tests

Native tests only (261 total). Test files in `tests/`:
- `test_columnar.cpp` — COLUMNAR_V1 path: PreparedGeometry A/B-const + dist, null handling
- `test_rowbinary.cpp` — RowBinary wire format
- `test_predicates.cpp` — `_impl` functions directly
- `test_mem.cpp` — `raw_buffer`, msgpack roundtrip, `impl_wrapper`
- `test_unpack.cpp` — `unpack_arg`, `impl_wrapper` exception path
- `test_bbox_wrapper.cpp` — `with_bbox` shortcut
- `test_overlay.cpp` — st_union_agg, st_area, etc.
- others — constructors, accessors, io, transforms, processing

`tests/helpers.hpp` provides: `wkt2wkb()`, `geom()`, `wkb()`, `geom2wkt()`, `WasmPanicException`.

`tests/test_columnar.cpp` has `make_columnar()` / `bytes_col()` / `fixed64_col()` helpers
for building COLUMNAR_V1 buffers in tests — reuse for new columnar tests.

## Key pitfalls

- **`system.webassembly_functions` doesn't exist** — use `system.functions WHERE origin != 'System'`
- **WASM can't link natively** — linker errors in `build/` for the WASM target are expected; only `chgeos_tests` links
- **`vector<bool>` specialization** — don't iterate `const auto& v : vec<bool>`, use `T v : vec<T>` (bit_reference issue on Apple Clang)
- **LSP errors** — GEOS headers not in LSP include path; all GEOS-related errors in LSP are false positives
- **st_dwithin bbox check**: the const-col path uses `bbox_a.intersects(wkb_bbox(span_b).expanded(dist))`
- **`early_ret=true` for st_disjoint** — bbox miss means bboxes don't intersect → geometries are disjoint → result is `true`
- **`prep_b_st_containsproperly = nullptr`** — GEOS PreparedGeometry has no B-const acceleration for containsProperly
