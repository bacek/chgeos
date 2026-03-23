#pragma once

// Convert ClickHouse native geometry types → internal EWKB representation.
// These are the bridge functions between CH's Tuple/Array geo types and
// the EWKB String used by all other chgeos functions.

#include "../geom/chgeom.hpp"
#include "../geom/wkb.hpp"

namespace ch {

inline Vector st_geomfromchpoint_impl(ChPoint p) {
    return write_ewkb(chpoint_to_geos(p));
}

inline Vector st_geomfromchlinestring_impl(ChLineString ls) {
    return write_ewkb(chlinestring_to_geos(ls));
}

inline Vector st_geomfromchpolygon_impl(ChPolygon poly) {
    return write_ewkb(chpolygon_to_geos(poly));
}

inline Vector st_geomfromchmultipolygon_impl(ChMultiPolygon mp) {
    return write_ewkb(chmultipolygon_to_geos(mp));
}

} // namespace ch
