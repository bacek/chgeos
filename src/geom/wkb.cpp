#include "wkb.hpp"

#include <bit>
#include <cstdlib>
#include <cstring>
#include <ostream>
#include <string>

#include <geos/geom/GeometryFactory.h>
#include <geos/geom/LineString.h>
#include <geos/geom/Point.h>
#include <geos/io/GeoJSONReader.h>
#include <geos/io/WKBReader.h>
#include <geos/io/WKBWriter.h>
#include <geos/io/WKTReader.h>
#include <geos/io/WKTWriter.h>

#include "../clickhouse.hpp"

namespace ch {

// ── EWKB constants (PostGIS extended WKB) ────────────────────────────────────
// Upper bits of the 32-bit type word carry dimension / SRID flags.
// Base geometry type is in the lower 28 bits (0x0FFFFFFF).
static constexpr uint32_t EWKB_Z_FLAG    = 0x80000000;
static constexpr uint32_t EWKB_M_FLAG    = 0x40000000;
static constexpr uint32_t EWKB_SRID_FLAG = 0x20000000;

// Read a 32-bit unsigned integer from p in the indicated byte order,
// returning it in native byte order.
static uint32_t read_u32(const uint8_t* p, bool little_endian) {
    uint32_t v;
    std::memcpy(&v, p, 4);
    if constexpr (std::endian::native == std::endian::little) {
        if (!little_endian) v = std::byteswap(v);
    } else {
        if (little_endian) v = std::byteswap(v);
    }
    return v;
}

// Write a native-order uint32 back to wire bytes in the indicated byte order.
static void write_u32(uint8_t* p, uint32_t v, bool little_endian) {
    if constexpr (std::endian::native == std::endian::little) {
        if (!little_endian) v = std::byteswap(v);
    } else {
        if (little_endian) v = std::byteswap(v);
    }
    std::memcpy(p, &v, 4);
}

// Parse EWKB (PostGIS extended WKB).
//
// EWKB wire layout (per PostGIS liblwgeom/lwin_wkb.c):
//   Byte 0      : endian flag (0x00 = big-endian, 0x01 = little-endian)
//   Bytes 1-4   : type word (uint32); flags in high bits:
//                   0x80000000  WKBZOFFSET  – has Z dimension
//                   0x40000000  WKBMOFFSET  – has M dimension
//                   0x20000000  WKBSRIDFLAG – SRID field follows type word
//                   0x0FFFFFFF  (mask)      – base geometry type (1–17)
//   Bytes 5-8   : SRID (int32, same byte order) — present only when WKBSRIDFLAG set
//   Bytes 9+    : geometry payload (coordinates, ring counts, etc.)
//
// When the SRID flag is set we strip the 4-byte SRID from the buffer,
// clear the flag in the type word, and pass the cleaned WKB to GEOS
// WKBReader (which handles standard WKB and ISO-style 3D WKB natively).
// The SRID is then re-applied via setSRID().
std::unique_ptr<Geometry> read_wkb(std::span<const uint8_t> input) {
    if (input.size() < 5)
        throw std::runtime_error("read_wkb: input too short");

    const uint8_t* p = input.data();
    bool little_endian = (p[0] == 0x01);

    uint32_t type_word = read_u32(p + 1, little_endian);
    bool has_srid = (type_word & EWKB_SRID_FLAG) != 0;

    int32_t srid = 0;
    std::vector<uint8_t> clean;

    if (has_srid) {
        if (input.size() < 9)
            throw std::runtime_error("read_wkb: EWKB SRID flag set but buffer too short");

        // Extract SRID (bytes 5–8).
        srid = static_cast<int32_t>(read_u32(p + 5, little_endian));

        // Rebuild WKB: clear SRID flag in type word and drop the 4-byte SRID field.
        clean.resize(input.size() - 4);
        clean[0] = p[0];                                                 // endian byte
        write_u32(clean.data() + 1, type_word & ~EWKB_SRID_FLAG, little_endian);
        std::memcpy(clean.data() + 5, p + 9, input.size() - 9);        // geometry payload

        p = clean.data();
        input = std::span(p, clean.size());
    }

    GeometryFactory::Ptr factory = GeometryFactory::create();
    WKBReader reader(*factory);
    auto geom = std::unique_ptr<Geometry>(reader.read(p, input.size()));
    if (!geom)
        throw std::runtime_error("read_wkb: WKBReader returned null");
    if (srid != 0)
        geom->setSRID(srid);

    return geom;
}

// Parse EWKT (PostGIS extended WKT).
//
// EWKT format (per PostGIS liblwgeom/lwin_wkt_lex.l + lwin_wkt_parse.y):
//   "SRID=<int>;<WKT>"   — SRID prefix is case-sensitive uppercase
//   "<WKT>"              — plain WKT, SRID stays 0
//
// The lexer matches regex SRID=-?[0-9]+ and the grammar requires a
// mandatory semicolon between the SRID token and the geometry body.
// SRID is extracted with strtol() and applied via setSRID().
std::unique_ptr<Geometry> read_wkt(std::span<const uint8_t> input) {
    const char* data = reinterpret_cast<const char*>(input.data());
    std::size_t size = input.size();
    int32_t srid = 0;

    if (size > 5 && std::strncmp(data, "SRID=", 5) == 0) {
        char* end;
        long val = std::strtol(data + 5, &end, 10);
        if (end != data + 5 && *end == ';') {
            if (val < INT32_MIN || val > INT32_MAX)
                throw std::runtime_error("read_wkt: SRID out of int32 range");
            srid = static_cast<int32_t>(val);
            std::size_t consumed = static_cast<std::size_t>(end + 1 - data);
            data += consumed;
            size -= consumed;
        }
        // Malformed prefix (no digits or no semicolon): treat as plain WKT.
    }

    GeometryFactory::Ptr factory = GeometryFactory::create();
    WKTReader reader(*factory);
    auto geom = std::unique_ptr<Geometry>(reader.read(std::string(data, size)));
    if (!geom)
        throw std::runtime_error("read_wkt: WKTReader returned null");
    if (srid != 0)
        geom->setSRID(srid);

    return geom;
}

std::unique_ptr<Geometry> read_geojson(std::span<const uint8_t> input) {
    GeoJSONReader reader;
    return reader.read(std::string(reinterpret_cast<const char*>(input.data()), input.size()));
}

raw_buffer write_ewkb(const std::unique_ptr<Geometry>& geom) {
    int32_t srid = geom->getSRID();

    WKBWriter writer;
    raw_buffer tmp(0);
    raw_write_buf buf{tmp};
    std::ostream wkb{&buf};
    writer.write(*geom, wkb);

    // SRID 0 means "no SRID assigned" (GEOS default); -1 is the GEOS internal
    // sentinel for "unknown / not set".  Neither should appear in wire EWKB —
    // omitting the SRID flag produces standard WKB that PostGIS and ClickHouse
    // both accept as SRID-less geometry.
    if (srid == 0 || srid == -1)
        return tmp;

    // Inject SRID into the WKB header to produce PostGIS EWKB.
    // Layout: [endian(1)] [type|SRID_FLAG(4)] [srid(4)] [payload...]
    // raw_buffer has no insert(), so we build a fresh buffer of the right size.
    bool little_endian = (tmp[0] == 0x01);
    uint32_t type_word = read_u32(tmp.data() + 1, little_endian);
    type_word |= EWKB_SRID_FLAG;

    raw_buffer r(0);
    r.reserve(tmp.size() + 4);
    r.push_back(tmp[0]);                                       // endian byte
    uint8_t type_bytes[4];
    write_u32(type_bytes, type_word, little_endian);
    r.append(type_bytes, 4);
    uint8_t srid_bytes[4];
    write_u32(srid_bytes, static_cast<uint32_t>(srid), little_endian);
    r.append(srid_bytes, 4);
    r.append(tmp.data() + 5, tmp.size() - 5);                 // geometry payload
    return r;
}

std::string write_wkt(const std::unique_ptr<Geometry>& geometry, bool ewkt) {
  WKTWriter writer;
  writer.setTrim(true);
  std::string wkt = writer.write(*geometry);
  std::string res;
  if (ewkt) {
    int srid = geometry->getSRID();
    if (srid != 0) {
      res.reserve(wkt.size() + 16);
      res = "SRID=" + std::to_string(srid) + ";" + wkt;
    } else {
      res = std::move(wkt);
    }
  } else {
    res = std::move(wkt);
  }
  return res;
}

} // namespace ch
