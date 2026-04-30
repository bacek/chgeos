#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$SCRIPT_DIR/.."
CH="${CH:-$REPO/../ClickHouse/build/programs/clickhouse}"
CONFIG="$REPO/clickhouse/config-test.xml"

pkill -f "clickhouse server" 2>/dev/null || true
sleep 1

"$CH" server --config-file="$CONFIG" 2>/tmp/ch-server.log &

echo -n "Waiting for server"
for i in $(seq 1 60); do
    if "$CH" client --port 19000 --query "SELECT 1" 2>/dev/null; then
        echo "Ready after ${i}s"
        exit 0
    fi
    echo -n "."
    sleep 1
done

echo "ERROR: server did not become ready within 60s"
exit 1
