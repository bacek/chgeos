#pragma once

// Conversion from ClickHouse native geometry types to GEOS geometry.
//
// ClickHouse's built-in geo types are just type aliases over standard
// Tuple/Array columns.  Over the MsgPack wire (BUFFERED_V1 ABI) they
// arrive as nested msgpack arrays that map directly to nested std::vectors:
//
//   Point           → std::vector<double>          e.g. {x, y}
//   Ring/LineString → std::vector<ChPoint>          i.e. vector of {x, y}
//   Polygon         → std::vector<ChLineString>     [0] = outer ring
//   MultiPolygon    → std::vector<ChPolygon>
//
// Using plain std::vectors means msgpack23's built-in CollectionLike
// unpacking handles every nesting level — no private-API calls needed.

#include <memory>
#include <stdexcept>
#include <vector>

#include <geos/geom/CoordinateSequence.h>
#include <geos/geom/GeometryFactory.h>
#include <geos/geom/LinearRing.h>
#include <geos/geom/LineString.h>
#include <geos/geom/MultiPolygon.h>
#include <geos/geom/Point.h>
#include <geos/geom/Polygon.h>

#include "../io.hpp"

namespace ch {

using namespace geos::geom;

// ── CH geometry C++ types ────────────────────────────────────────────────────

using ChPoint         = std::vector<double>;     // {x, y}
using ChLineString    = std::vector<ChPoint>;
using ChPolygon       = std::vector<ChLineString>; // [0]=outer, rest=holes
using ChMultiPolygon  = std::vector<ChPolygon>;

// ── Helpers ──────────────────────────────────────────────────────────────────

inline std::unique_ptr<CoordinateSequence> to_coord_seq(const ChLineString& ring) {
    auto cs = std::make_unique<CoordinateSequence>(ring.size());
    for (std::size_t i = 0; i < ring.size(); ++i) {
        if (ring[i].size() < 2)
            throw std::runtime_error("ChPoint must have at least 2 coordinates");
        cs->setAt(Coordinate(ring[i][0], ring[i][1]), i);
    }
    return cs;
}

// ── Conversion to GEOS ───────────────────────────────────────────────────────

inline std::unique_ptr<Geometry> chpoint_to_geos(const ChPoint& p) {
    if (p.size() < 2)
        throw std::runtime_error("ChPoint must have at least 2 coordinates");
    GeometryFactory::Ptr f = GeometryFactory::create();
    return f->createPoint(Coordinate(p[0], p[1]));
}

inline std::unique_ptr<Geometry> chlinestring_to_geos(const ChLineString& ls) {
    GeometryFactory::Ptr f = GeometryFactory::create();
    return f->createLineString(to_coord_seq(ls));
}

inline std::unique_ptr<Geometry> chpolygon_to_geos(const ChPolygon& poly) {
    if (poly.empty()) {
        GeometryFactory::Ptr f = GeometryFactory::create();
        return f->createPolygon();
    }
    GeometryFactory::Ptr f = GeometryFactory::create();
    auto outer = f->createLinearRing(to_coord_seq(poly[0]));
    std::vector<std::unique_ptr<LinearRing>> holes;
    for (std::size_t i = 1; i < poly.size(); ++i)
        holes.push_back(f->createLinearRing(to_coord_seq(poly[i])));
    return f->createPolygon(std::move(outer), std::move(holes));
}

inline std::unique_ptr<Geometry> chmultipolygon_to_geos(const ChMultiPolygon& mp) {
    GeometryFactory::Ptr f = GeometryFactory::create();
    std::vector<std::unique_ptr<Geometry>> polys;
    polys.reserve(mp.size());
    for (const auto& poly : mp)
        polys.push_back(chpolygon_to_geos(poly));
    return f->createMultiPolygon(std::move(polys));
}

} // namespace ch
