# chgeos Server & Benchmarks

## Running ClickHouse server

To restart the server (after rebuilding CH or when needed):
```bash
scripts/restart_ch.sh
```

This script handles everything: kills any running instance, starts a fresh server, and waits until it is ready. CH takes ~30s to start; always wait before connecting.

Logs: `tail -f /tmp/ch-server.log`

Connect: `../ClickHouse/build/programs/clickhouse client --port 19000`

## Reloading chgeos.wasm after a build

Only needed when the **WASM binary changes** (`ninja -C build_wasm`). For pure ClickHouse C++ changes, just restart the CH server — no WASM reload required.

Use `scripts/reload.sh` (does all steps below automatically).

## Benchmarks

Data: `../spatial-bench/sf1/` (6M rows) and `../spatial-bench/sf10/` (60M rows).

Two data modes — run setup scripts once each:
```bash
./scripts/link_bench_data.sh   # parquet mode: symlinks sf1/sf10 into user_files/
./scripts/import_sf.sh ../ClickHouse/build/programs/clickhouse sf1   # native mode
./scripts/import_sf.sh ../ClickHouse/build/programs/clickhouse sf10  # native mode
```

Use `scripts/bench_sf.py`. See memory for invocation flags and rules.
