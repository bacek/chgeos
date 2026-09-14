#!/usr/bin/env bash
# Regenerate tests/wire_fixtures/*.bin from the ClickHouse ColumnBinaryWire host
# writer, then tests/wire_fixtures.gen.hpp with the bytes embedded.
#
# The fixtures MUST come from the real host serializer (buildColDescriptor +
# writeColData), never reconstructed by hand: run
#
#   COLUMNAR_WIRE_DUMP_DIR=$PWD/tests/wire_fixtures \
#     ../ClickHouse/build/src/unit_tests_dbms --gtest_filter='ColumnBinaryWire*'
#
# from this repo root, then embed with:
#
#   ./tests/regenerate_wire_fixtures.sh --embed-only
set -euo pipefail
cd "$(dirname "$0")"

if [[ "${1:-}" != "--embed-only" ]]; then
  : > /dev/null
  echo "run the unit_tests_dbms command above first, then re-run with --embed-only"
fi

OUT=wire_fixtures.gen.hpp
{
  echo "// Byte-exact ColumnBinaryWire frames produced by the ClickHouse host writer"
  echo "// (src/Formats/ColumnBinaryWire.h buildColDescriptor + writeColData)."
  echo "// Regenerate: tests/regenerate_wire_fixtures.sh (see header comment)."
  echo "#pragma once"
  echo "#include <cstddef>"
  echo "#include <cstdint>"
  echo
  echo "namespace wire_fixture {"
  echo
  for f in wire_fixtures/*.bin; do
    name=$(basename "$f" .bin | tr 'a-z' 'A-Z')
    printf 'inline const uint8_t %s[] = {' "$name"
    xxd -i < "$f"
    printf '};\n'
    printf 'inline const size_t %s_len = %s;\n' "$name" "$(stat -c%s "$f")"
    echo
  done
  echo "}  // namespace wire_fixture"
} > "$OUT"
echo "wrote $OUT"
