#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(realpath "$SCRIPT_DIR/..")"
CH="${CH:-$REPO/../ClickHouse/build/programs/clickhouse}"
CONFIG="$REPO/clickhouse/config-test.xml"

# List running server pids. Matching on the full command line alone is unsafe:
# `pkill -f "clickhouse server"` also matches any shell, editor or grep whose
# arguments happen to contain that phrase, including the caller of this script.
# Require the executable name to actually be clickhouse.
server_pids() {
    local pid
    for pid in $(pgrep -f "clickhouse server" 2>/dev/null); do
        [[ "$(cat "/proc/$pid/comm" 2>/dev/null)" == "clickhouse" ]] && echo "$pid"
    done
}

# shellcheck disable=SC2046  # word splitting is intended: kill takes a pid list
[[ -n "$(server_pids)" ]] && kill $(server_pids) 2>/dev/null || true

# Wait for the old server to actually exit. SIGTERM shutdown can take several
# seconds; starting too early makes the new instance fail to lock tmp/data/status
# ("Another server instance in same directory is already running") and die.
for _ in $(seq 1 60); do
    [[ -z "$(server_pids)" ]] && break
    sleep 1
done
if [[ -n "$(server_pids)" ]]; then
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

# Detach every standard fd. Leaving stdout inherited keeps the caller's pipe
# open for as long as the server lives, so a caller that reads this script's
# output (a CI step, a tool wrapper) hangs after the script itself has exited.
nohup "$CH" server --config-file="$TMP_CONFIG" </dev/null >/tmp/ch-server.log 2>&1 &

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
