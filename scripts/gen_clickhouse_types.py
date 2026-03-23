#!/usr/bin/env python3
"""
Generate src/clickhouse_types.hpp from a ClickHouse source checkout.

Usage:
    python3 scripts/gen_clickhouse_types.py /path/to/ClickHouse

    Example (from repo root):
        python3 scripts/gen_clickhouse_types.py ../ClickHouse

How it works:
  1. Reads src/Core/TypeId.h and extracts the TypeIndex enum verbatim,
     preserving exact numeric values so they match ClickHouse's ABI.

  2. Reads src/DataTypes/DataTypeCustomGeo.cpp and reconstructs:
       - Which type display-names (Point, LineString, …) belong to Geometry.
       - DataTypeVariant's sort order: the constructor inserts types into a
         std::map keyed by display name, which means members come out in
         lexicographic order.  The position in that sorted list is the
         global discriminant stored on the wire.

  3. Writes src/clickhouse_types.hpp containing:
       - CHTypeIndex enum — copy of TypeIndex for use without CH headers.
       - geo_discr:: constants — one per Geometry variant member.

  Wire format reminder (MsgPack BUFFERED_V1):
       Variant NULL   →  msgpack nil
       Variant value  →  fixarray[2]{ uint8_t global_discr, <value> }
       Tuple(T…)      →  fixarray[N]{ T0, T1, … }
       Array(T)       →  msgpack array{ T, … }
       Nullable(T)    →  nil | T

Regenerate whenever upgrading the ClickHouse target version.
"""

import re
import subprocess
import sys
from pathlib import Path


def parse_type_index(src: str) -> list[tuple[str, int]]:
    """Extract (name, value) pairs from the TypeIndex enum in TypeId.h."""
    m = re.search(r'enum class TypeIndex\s*:\s*uint8_t\s*\{([^}]+)\}', src, re.DOTALL)
    if not m:
        raise RuntimeError("Could not find TypeIndex enum in TypeId.h")

    entries: list[tuple[str, int]] = []
    current = 0
    for raw in m.group(1).splitlines():
        # strip comments and trailing commas
        line = raw.split('//')[0].strip().rstrip(',').strip()
        if not line:
            continue
        if '=' in line:
            name, val = line.split('=', 1)
            current = int(val.strip(), 0)
            entries.append((name.strip(), current))
        else:
            entries.append((line.strip(), current))
        current += 1

    return entries


def parse_geo_variant(src: str) -> list[str]:
    """
    Return the Geometry variant member names sorted as DataTypeVariant does.

    DataTypeVariant::DataTypeVariant() inserts into std::map<String,DataTypePtr>
    keyed by type->getName().  For custom-named types (Point, LineString, …)
    getName() returns the custom display name, not the underlying type string.
    The map iteration order is lexicographic, which becomes the discriminant order.

    Strategy:
      a) Find all  `auto <var> = …get(DataType<Name>Name().getName())`  lines
         to build a var → display-name map.
      b) Find the DataTypeVariant(std::vector{ v1, v2, … }) call.
      c) Map var names → display names, sort lexicographically.
    """
    # Map local variable name → ClickHouse display type name
    # e.g. "point_type" → "Point" from:
    #   auto point_type = DataTypeFactory::instance().get(DataTypePointName().getName());
    var_to_name: dict[str, str] = {}
    for m in re.finditer(
        r'auto\s+(\w+)\s*=.*?DataType(\w+)Name\(\)\.getName\(\)', src
    ):
        var_to_name[m.group(1)] = m.group(2)

    if not var_to_name:
        raise RuntimeError("Could not find DataType<X>Name() variable assignments in DataTypeCustomGeo.cpp")

    # Find: std::make_shared<DataTypeVariant>(std::vector{v1, v2, …})
    m = re.search(r'make_shared<DataTypeVariant>\(std::vector\{([^}]+)\}\)', src)
    if not m:
        raise RuntimeError("Could not find DataTypeVariant construction in DataTypeCustomGeo.cpp")

    var_names = [v.strip() for v in m.group(1).split(',')]
    type_names: list[str] = []
    for v in var_names:
        if v not in var_to_name:
            raise RuntimeError(f"Unknown variable '{v}' in Geometry Variant — update the parser")
        type_names.append(var_to_name[v])

    # Replicate DataTypeVariant's std::map sort (lexicographic by display name)
    type_names.sort()
    return type_names


def git_short_hash(path: Path) -> str:
    r = subprocess.run(
        ['git', 'rev-parse', '--short', 'HEAD'],
        cwd=path, capture_output=True, text=True
    )
    return r.stdout.strip() if r.returncode == 0 else 'unknown'


def generate(type_index: list[tuple[str, int]], geo_members: list[str], commit: str) -> str:
    lines: list[str] = []
    a = lines.append

    a("// AUTO-GENERATED — do not edit manually.")
    a(f"// Generated from ClickHouse @ {commit}")
    a("// Regenerate: python3 scripts/gen_clickhouse_types.py /path/to/ClickHouse")
    a("")
    a("#pragma once")
    a("#include <cstdint>")
    a("")
    a("namespace clickhouse {")
    a("")
    a("// -----------------------------------------------------------------------")
    a("// TypeIndex: mirrors ClickHouse src/Core/TypeId.h.")
    a("// Used to interpret type tags in MsgPack BUFFERED_V1 UDF wire format.")
    a("// -----------------------------------------------------------------------")
    a("enum class TypeIndex : uint8_t {")
    for name, val in type_index:
        a(f"    {name} = {val},")
    a("};")
    a("")
    a("// -----------------------------------------------------------------------")
    a("// Geometry variant discriminants.")
    a("//")
    a("// Geometry = Variant(" + ", ".join(geo_members) + ")")
    a("//")
    a("// DataTypeVariant sorts members by display name (lexicographic std::map).")
    a("// The sort position is the global discriminant stored in the wire format.")
    a("//")
    a("// Wire format:")
    a("//   NULL value  →  msgpack nil")
    a("//   Non-null    →  fixarray[2]{ uint8_t discr, <encoded value> }")
    a("// -----------------------------------------------------------------------")
    a("namespace geo_discr {")
    a("    constexpr uint8_t NULL_DISCRIMINANT = 255;")
    for i, name in enumerate(geo_members):
        a(f"    constexpr uint8_t {name} = {i};")
    a("} // namespace geo_discr")
    a("")
    a("} // namespace clickhouse")
    a("")

    return "\n".join(lines)


def main() -> None:
    if len(sys.argv) != 2:
        print(__doc__, file=sys.stderr)
        sys.exit(1)

    ch_dir = Path(sys.argv[1]).resolve()
    typeid_h   = ch_dir / "src/Core/TypeId.h"
    geo_cpp    = ch_dir / "src/DataTypes/DataTypeCustomGeo.cpp"

    for p in (typeid_h, geo_cpp):
        if not p.exists():
            sys.exit(f"Error: {p} not found — is {ch_dir} a ClickHouse checkout?")

    commit      = git_short_hash(ch_dir)
    type_index  = parse_type_index(typeid_h.read_text())
    geo_members = parse_geo_variant(geo_cpp.read_text())
    header      = generate(type_index, geo_members, commit)

    out = Path(__file__).parent.parent / "src" / "clickhouse_types.hpp"
    out.write_text(header)
    print(f"Written {out}  ({len(type_index)} TypeIndex entries, "
          f"{len(geo_members)} Geometry discriminants, CH @ {commit})")


if __name__ == "__main__":
    main()
