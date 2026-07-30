#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$SCRIPT_DIR/.."
CH="${CH:-$REPO/../ClickHouse/build/programs/clickhouse}"
CONFIG="$REPO/clickhouse/config-test.xml"

pkill -f "clickhouse server" 2>/dev/null || true

# Wait for the old server to actually exit. SIGTERM shutdown can take several
# seconds; starting too early makes the new instance fail to lock tmp/data/status
# ("Another server instance in same directory is already running") and die.
for _ in $(seq 1 60); do
    pgrep -f "clickhouse server" >/dev/null 2>&1 || break
    sleep 1
done
if pgrep -f "clickhouse server" >/dev/null 2>&1; then
    echo "ERROR: old server still running after 60s, refusing to start a second instance" >&2
    exit 1
fi

DATA_DIR="$REPO/tmp/data"
USER_FILES="$DATA_DIR/user_files"
mkdir -p "$USER_FILES"

# Write paths into config so it works cross-platform (no hardcoded /home/bacek)
TMP_CONFIG="$REPO/tmp/config-test-generated.xml"
sed -e "s|__DATA_DIR__|${DATA_DIR}|g" \
    -e "s|__USER_FILES_PATH__|${USER_FILES}|g" \
    < "$CONFIG" > "$TMP_CONFIG"

nohup "$CH" server --config-file="$TMP_CONFIG" 2>/tmp/ch-server.log &

echo -n "Waiting for server"
for i in $(seq 1 120); do
    if "$CH" client --port 19000 --query "SELECT 1" 2>/dev/null; then
        echo "Ready after ${i}s"
        exit 0
    fi
    echo -n "."
    sleep 1
done

echo "ERROR: server did not become ready within 60s"
exit 1
