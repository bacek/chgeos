#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$SCRIPT_DIR/.."
CH="${CH:-$REPO/../ClickHouse/build/programs/clickhouse}"
WASM="$REPO/build_wasm/bin/chgeos.wasm"
USER_FILES="$REPO/tmp/data/user_files"

echo "==> Drop functions"
grep -oE "^CREATE OR REPLACE FUNCTION [a-z0-9_]+" "$REPO/clickhouse/create.sql" \
    | sed 's/CREATE OR REPLACE FUNCTION /DROP FUNCTION IF EXISTS /' \
    | sed 's/$/ ;/' \
    | "$CH" client --port 19000 --multiquery

echo "==> Delete module"
"$CH" client --port 19000 --query "DELETE FROM system.webassembly_modules WHERE name='chgeos'"

echo "==> Copy WASM"
cp "$WASM" "$USER_FILES/chgeos.wasm"

echo "==> Insert module"
"$CH" client --port 19000 --query "INSERT INTO system.webassembly_modules (name, code) VALUES ('chgeos', file('chgeos.wasm'))"

echo "==> Create functions"
"$CH" client --port 19000 --multiquery < "$REPO/clickhouse/create.sql"

echo "==> Done"
