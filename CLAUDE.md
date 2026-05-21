# chgeos — CLAUDE.md

PostGIS-compatible spatial UDF library for ClickHouse, compiled to WASM.
Uses GEOS 3.12+ for geometry operations. C++23.

## Build system

Build dirs: `build_native/` (tests) and `build_wasm/` (WASM binary). See memory for commands.

ClickHouse binary: `../ClickHouse/build/programs/clickhouse`
(sibling directory, built from source)

Inspect WASM exports: `wasm-tools dump build_wasm/chgeos.wasm | grep <name>`

LSP shows many false-positive errors for GEOS/CH headers — ignore them. The real compiler is always the source of truth.

## Reference docs

- [Architecture](.claude/architecture.md) — wire formats, source layout, columnar_impl_wrapper, PreparedGeometry, macros
- [Tests](.claude/tests.md) — test files, helpers, how to write new tests
- [Server & benchmarks](.claude/server.md) — CH server, WASM reload, bench_sf.py setup
- [Pitfalls](.claude/pitfalls.md) — common gotchas
