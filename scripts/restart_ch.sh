#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(realpath "$SCRIPT_DIR/..")"
CH="${CH:-$REPO/../ClickHouse/build/programs/clickhouse}"
CONFIG="$REPO/clickhouse/config-test.xml"
DATA_DIR="$REPO/tmp/data"
USER_FILES="$DATA_DIR/user_files"
TMP_CONFIG="$REPO/tmp/config-test-generated.xml"

# List running server pids. Matching on the full command line alone is unsafe:
# `pkill -f "clickhouse server"` also matches any shell, editor or grep whose
# arguments happen to contain that phrase, including the caller of this script.
# Require the process to actually be a server: argv[1] is "server" and one
# argument is exactly our --config-file. A shell or grep mentioning the phrase
# has its own argv[1] (e.g. "-c") and is not matched.
#
# Do not match on the executable name. A/B runs start servers from copied
# binaries with arbitrary names (.scratch/ch_pre, /tmp/clickhouse-9d8edb43e21);
# comm is also truncated to 15 characters. Any name-based filter silently skips
# such a server, it survives, the new one dies on the status-file lock, and the
# readiness probe then talks to the old process — i.e. every subsequent
# measurement uses the wrong binary.
#
# Match on this config path, not on "server --config-file" in general. Servers for
# other checkouts run from the same build tree with the same executable name, on
# their own ports and data directory; killing those interrupts whatever they are
# running (a long benchmark query dies as QUERY_WAS_CANCELLED) and, when they
# outlive the wait below, blocks this script from starting at all. Only the server
# holding this data directory can conflict with the one started here.
server_pids() {
    local pid
    local -a argv
    for pid in $(pgrep -f -- "--config-file=$TMP_CONFIG" 2>/dev/null); do
        mapfile -d '' -t argv < "/proc/$pid/cmdline" 2>/dev/null || continue
        [[ "${argv[1]:-}" == server ]] || continue
        for arg in "${argv[@]:2}"; do
            [[ "$arg" == "--config-file=$TMP_CONFIG" ]] && { echo "$pid"; break; }
        done
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

mkdir -p "$USER_FILES"

# Write paths into config so it works cross-platform (no hardcoded /home/bacek)
sed -e "s|__DATA_DIR__|${DATA_DIR}|g" \
    -e "s|__USER_FILES_PATH__|${USER_FILES}|g" \
    < "$CONFIG" > "$TMP_CONFIG"

# Detach every standard fd. Leaving stdout inherited keeps the caller's pipe
# open for as long as the server lives, so a caller that reads this script's
# output (a CI step, a tool wrapper) hangs after the script itself has exited.
nohup "$CH" server --config-file="$TMP_CONFIG" </dev/null >/tmp/ch-server.log 2>&1 &
NEW_PID=$!

echo -n "Waiting for server"
for i in $(seq 1 120); do
    # A successful SELECT 1 only proves *someone* is on port 19000. Check that
    # the server we started is the one still alive, so a failed start can never
    # be reported as ready against a leftover process running another binary.
    if ! kill -0 "$NEW_PID" 2>/dev/null; then
        echo
        echo "ERROR: server exited during startup; last lines of /tmp/ch-server.log:" >&2
        tail -20 /tmp/ch-server.log >&2
        exit 1
    fi
    if "$CH" client --port 19000 --query "SELECT 1" 2>/dev/null; then
        # A server that fails to bind can stay alive without listening, while
        # a leftover process keeps answering. Require our pid, or the server
        # child its watchdog forks, to own the port.
        owner="$(ss -ltnpH 'sport = :19000' 2>/dev/null | grep -o 'pid=[0-9]*' | head -1 | cut -d= -f2)"
        owner_parent="$(awk '{print $4}' "/proc/$owner/stat" 2>/dev/null)"
        if [[ "$owner" != "$NEW_PID" && "$owner_parent" != "$NEW_PID" ]]; then
            echo
            echo "ERROR: port 19000 is held by another process, not the new server (pid $NEW_PID):" >&2
            ss -ltnpH 'sport = :19000' >&2
            kill "$NEW_PID" 2>/dev/null
            exit 1
        fi
        echo "Ready after ${i}s (pid $NEW_PID)"
        exit 0
    fi
    echo -n "."
    sleep 1
done

echo "ERROR: server did not become ready within 60s"
exit 1
