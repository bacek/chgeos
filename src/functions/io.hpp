#pragma once

#include <span>
#include <string>
#include <vector>

#include <geos/version.h>
#include "../geom/wkb.hpp"

namespace ch {

inline std::string geos_version_impl() { return GEOS_VERSION; }

inline std::string st_geomfromgeojson_impl(std::string_view geojson) {
    auto geom = read_geojson(std::span<const uint8_t>(
        reinterpret_cast<const uint8_t*>(geojson.data()), geojson.size()));
    if (!geom)
        throw std::runtime_error("st_geomfromgeojson: failed to parse GeoJSON");
    auto wkb = write_ewkb(geom);
    return std::string(reinterpret_cast<const char*>(wkb.data()), wkb.size());
}

inline std::string st_astext_impl(const std::unique_ptr<Geometry>& geometry) {
  return write_wkt(geometry, false);
}

// Returns EWKT: "SRID=<n>;<WKT>" when SRID is set, plain WKT otherwise.
inline std::string st_asewkt_impl(const std::unique_ptr<Geometry>& geometry) {
  return write_wkt(geometry, true);
}

} // namespace ch
