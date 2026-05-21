# chgeos Tests

Native tests only (280 total). Test files in `tests/`:
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
