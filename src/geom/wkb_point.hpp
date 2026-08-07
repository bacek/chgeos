// Read the coordinate out of a WKB/EWKB POINT without a GEOS parse.
//
// Sits alongside wkb_envelope.hpp (bbox extraction) and wkb_distance.hpp (point
// distance) on top of the shared ByteCursor: same rule, no GEOS allocation.

#pragma once

#include <optional>
#include <span>

#include "wkb_cursor.hpp"

namespace ch {

struct XY {
    double x, y;
};

// Returns nullopt for anything that is not a POINT, so callers can fall back to
// the general path.  X and Y are always the first two ordinates regardless of
// dimensionality, so Z/M points read the same way — the trailing ordinates are
// simply not consumed.
//
// Throws on truncated input, matching read_wkb.
inline std::optional<XY> wkb_read_point(std::span<const uint8_t> wkb) {
    detail::ByteCursor c(wkb);
    if (detail::read_geom_header(c).base != 1) return std::nullopt;
    double x = c.read_double();
    double y = c.read_double();
    return XY{x, y};
}

} // namespace ch
