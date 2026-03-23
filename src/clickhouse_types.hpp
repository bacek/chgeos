// AUTO-GENERATED — do not edit manually.
// Generated from ClickHouse @ 62a023c96d9
// Regenerate: python3 scripts/gen_clickhouse_types.py /path/to/ClickHouse

#pragma once
#include <cstdint>

namespace clickhouse {

// -----------------------------------------------------------------------
// TypeIndex: mirrors ClickHouse src/Core/TypeId.h.
// Used to interpret type tags in MsgPack BUFFERED_V1 UDF wire format.
// -----------------------------------------------------------------------
enum class TypeIndex : uint8_t {
    Nothing = 0,
    UInt8 = 1,
    UInt16 = 2,
    UInt32 = 3,
    UInt64 = 4,
    UInt128 = 5,
    UInt256 = 6,
    Int8 = 7,
    Int16 = 8,
    Int32 = 9,
    Int64 = 10,
    Int128 = 11,
    Int256 = 12,
    BFloat16 = 13,
    Float32 = 14,
    Float64 = 15,
    Date = 16,
    Date32 = 17,
    DateTime = 18,
    DateTime64 = 19,
    Time = 20,
    Time64 = 21,
    String = 22,
    FixedString = 23,
    Enum8 = 24,
    Enum16 = 25,
    Decimal32 = 26,
    Decimal64 = 27,
    Decimal128 = 28,
    Decimal256 = 29,
    UUID = 30,
    Array = 31,
    Tuple = 32,
    QBit = 33,
    Set = 34,
    Interval = 35,
    Nullable = 36,
    Function = 37,
    AggregateFunction = 38,
    LowCardinality = 39,
    Map = 40,
    Object = 41,
    IPv4 = 42,
    IPv6 = 43,
    JSONPaths = 44,
    Variant = 45,
    Dynamic = 46,
};

// -----------------------------------------------------------------------
// Geometry variant discriminants.
//
// Geometry = Variant(LineString, MultiLineString, MultiPolygon, Point, Polygon, Ring)
//
// DataTypeVariant sorts members by display name (lexicographic std::map).
// The sort position is the global discriminant stored in the wire format.
//
// Wire format:
//   NULL value  →  msgpack nil
//   Non-null    →  fixarray[2]{ uint8_t discr, <encoded value> }
// -----------------------------------------------------------------------
namespace geo_discr {
    constexpr uint8_t NULL_DISCRIMINANT = 255;
    constexpr uint8_t LineString = 0;
    constexpr uint8_t MultiLineString = 1;
    constexpr uint8_t MultiPolygon = 2;
    constexpr uint8_t Point = 3;
    constexpr uint8_t Polygon = 4;
    constexpr uint8_t Ring = 5;
} // namespace geo_discr

} // namespace clickhouse
