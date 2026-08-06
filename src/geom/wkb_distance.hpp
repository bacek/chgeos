// Exact distance from a point to a WKB geometry, computed straight from the
// coordinate array — no GEOS parse, no allocation.
//
// This is the refinement step of the kNN index (functions/knn.hpp): candidate
// selection works on bbox centroids, which is cheap but approximate, so the
// few surviving candidates get an exact distance here. For the small rings
// typical of building footprints (4–8 vertices) that is a handful of
// point-to-segment evaluations.
//
// Semantics match GEOS Geometry::distance() for a point argument: 0 when the
// point lies inside a polygon (or on its boundary), otherwise the distance to
// the nearest boundary segment. A point inside a hole measures to the hole's
// ring.

#pragma once

#include <algorithm>
#include <cmath>
#include <limits>
#include <span>
#include <vector>

#include "wkb_cursor.hpp"

namespace ch {

namespace detail {

// Squared distance from (px,py) to the segment (x1,y1)-(x2,y2).
[[gnu::always_inline]] inline double point_segment_dist2(
    double px, double py, double x1, double y1, double x2, double y2)
{
    double dx = x2 - x1, dy = y2 - y1;
    double len2 = dx * dx + dy * dy;
    // Degenerate segment: fall back to the endpoint.
    double t = (len2 > 0.0) ? ((px - x1) * dx + (py - y1) * dy) / len2 : 0.0;
    t = std::clamp(t, 0.0, 1.0);
    double ex = x1 + t * dx - px;
    double ey = y1 + t * dy - py;
    return ex * ex + ey * ey;
}

struct RingScan {
    double dist2 = std::numeric_limits<double>::infinity();
    bool crossings_odd = false; // ray-casting parity; meaningful for closed rings

    [[gnu::always_inline]] void add_segment(double px, double py,
                                            double x1, double y1,
                                            double x2, double y2)
    {
        dist2 = std::min(dist2, point_segment_dist2(px, py, x1, y1, x2, y2));

        // Cast a ray in +x: count edges straddling py that cross to the right.
        if ((y1 > py) != (y2 > py)) {
            double x_at_py = x1 + (py - y1) * (x2 - x1) / (y2 - y1);
            if (px < x_at_py) crossings_odd = !crossings_odd;
        }
    }
};

// Scan a ring already flattened into an interleaved xy array. This is the hot
// path for the kNN index, which caches candidate coordinates at build time so
// per-row refinement is arithmetic with no WKB walk.
inline RingScan scan_ring_coords(const double * xy, uint32_t num_points,
                                 double px, double py)
{
    RingScan out;
    if (num_points == 0) return out;
    if (num_points == 1) {
        double dx = px - xy[0], dy = py - xy[1];
        out.dist2 = dx * dx + dy * dy;
        return out;
    }
    for (uint32_t i = 1; i < num_points; ++i)
        out.add_segment(px, py, xy[2*i - 2], xy[2*i - 1], xy[2*i], xy[2*i + 1]);
    return out;
}

// Stream one coordinate sequence, accumulating the minimum squared distance to
// its segments and the ray-crossing parity for point-in-ring.
inline RingScan scan_coord_seq(ByteCursor & c, uint32_t num_points, uint32_t dims,
                               double px, double py)
{
    RingScan out;
    if (num_points == 0) return out;

    const size_t extra = (dims - 2) * sizeof(double);

    double x0 = c.read_double(), y0 = c.read_double();
    c.skip(extra);
    if (num_points == 1) {
        double dx = px - x0, dy = py - y0;
        out.dist2 = dx * dx + dy * dy;
        return out;
    }

    double xp = x0, yp = y0;
    for (uint32_t i = 1; i < num_points; ++i) {
        double x = c.read_double(), y = c.read_double();
        c.skip(extra);
        out.add_segment(px, py, xp, yp, x, y);
        xp = x; yp = y;
    }
    return out;
}

// Squared distance from (px,py) to the next geometry in the cursor.
inline double geometry_dist2(ByteCursor & c, double px, double py)
{
    GeomHeader h = read_geom_header(c);

    switch (h.base) {
        case 1: { // Point
            double x = c.read_double(), y = c.read_double();
            c.skip((h.dims - 2) * sizeof(double));
            double dx = px - x, dy = py - y;
            return dx * dx + dy * dy;
        }
        case 2: { // LineString
            uint32_t n = c.read_u32();
            return scan_coord_seq(c, n, h.dims, px, py).dist2;
        }
        case 3: { // Polygon
            uint32_t nrings = c.read_u32();
            if (nrings == 0) return std::numeric_limits<double>::infinity();

            RingScan ext = scan_coord_seq(c, c.read_u32(), h.dims, px, py);
            double best = ext.dist2;
            bool in_hole = false;

            for (uint32_t r = 1; r < nrings; ++r) {
                RingScan hole = scan_coord_seq(c, c.read_u32(), h.dims, px, py);
                best = std::min(best, hole.dist2);
                in_hole |= hole.crossings_odd;
            }

            // Inside the shell and not in a hole → the point is in the polygon.
            // Otherwise the nearest polygon point is on one of the rings: from
            // inside a hole any exit crosses that hole's ring first, so the
            // minimum over all rings is the right answer in both cases.
            if (ext.crossings_odd && !in_hole) return 0.0;
            return best;
        }
        case 4:  // MultiPoint
        case 5:  // MultiLineString
        case 6:  // MultiPolygon
        case 7: { // GeometryCollection
            uint32_t n = c.read_u32();
            double best = std::numeric_limits<double>::infinity();
            // No early exit on a zero distance: the cursor is shared with any
            // enclosing collection, so every child must be walked to the end.
            for (uint32_t i = 0; i < n; ++i)
                best = std::min(best, geometry_dist2(c, px, py));
            return best;
        }
        default:
            throw std::runtime_error("WKB: unknown geometry type");
    }
}

} // namespace detail

enum class RingKind : uint8_t;
struct RingRef;

namespace detail {

// Copy num_points coordinate pairs out of the cursor, dropping Z/M.
inline uint32_t append_coords(ByteCursor & c, uint32_t num_points, uint32_t dims,
                              std::vector<double> & coords)
{
    uint32_t offset = static_cast<uint32_t>(coords.size() / 2);
    const size_t extra = (dims - 2) * sizeof(double);
    // No reserve() here: reserving the exact required size on every ring forces
    // a reallocation per ring, turning a whole-column flatten into O(N²) copies.
    // push_back's geometric growth is what we want.
    for (uint32_t i = 0; i < num_points; ++i) {
        coords.push_back(c.read_double());
        coords.push_back(c.read_double());
        c.skip(extra);
    }
    return offset;
}

void flatten_geometry(ByteCursor & c, std::vector<double> & coords,
                      std::vector<RingRef> & rings);

} // namespace detail

// Distance from the point (px,py) to a WKB/EWKB geometry.
// Returns infinity for an empty geometry.
inline double wkb_point_distance(std::span<const uint8_t> wkb, double px, double py)
{
    detail::ByteCursor c(wkb);
    return std::sqrt(detail::geometry_dist2(c, px, py));
}

// ── Flattened ring cache ──────────────────────────────────────────────────────
// Decoding WKB per distance call is the dominant cost when the same candidate
// geometries are measured against millions of query points. Flattening their
// coordinates once turns each later distance into plain arithmetic.

enum class RingKind : uint8_t {
    Shell, // polygon exterior ring: an odd crossing count means "inside"
    Hole,  // polygon interior ring, belonging to the preceding Shell
    Open,  // point or linestring: no interior, so no crossing test
};

struct RingRef {
    uint32_t offset;      // index of the first coordinate pair in the xy array
    uint32_t num_points;
    RingKind kind;
};

// Append one geometry's rings to (coords, rings). Coordinates are interleaved
// x,y pairs; Z/M are dropped. A polygon contributes a Shell followed by its
// Holes, so a scan can group them by walking the list in order.
inline void flatten_wkb_rings(std::span<const uint8_t> wkb,
                              std::vector<double> & coords,
                              std::vector<RingRef> & rings)
{
    detail::ByteCursor c(wkb);
    detail::flatten_geometry(c, coords, rings);
}

namespace detail {

inline void flatten_geometry(ByteCursor & c, std::vector<double> & coords,
                             std::vector<RingRef> & rings)
{
    GeomHeader h = read_geom_header(c);

    switch (h.base) {
        case 1: { // Point
            uint32_t off = append_coords(c, 1, h.dims, coords);
            rings.push_back({off, 1, RingKind::Open});
            break;
        }
        case 2: { // LineString
            uint32_t n = c.read_u32();
            uint32_t off = append_coords(c, n, h.dims, coords);
            rings.push_back({off, n, RingKind::Open});
            break;
        }
        case 3: { // Polygon
            uint32_t nrings = c.read_u32();
            for (uint32_t r = 0; r < nrings; ++r) {
                uint32_t n = c.read_u32();
                uint32_t off = append_coords(c, n, h.dims, coords);
                rings.push_back({off, n, r == 0 ? RingKind::Shell : RingKind::Hole});
            }
            break;
        }
        case 4:  // MultiPoint
        case 5:  // MultiLineString
        case 6:  // MultiPolygon
        case 7: { // GeometryCollection
            uint32_t n = c.read_u32();
            for (uint32_t i = 0; i < n; ++i)
                flatten_geometry(c, coords, rings);
            break;
        }
        default:
            throw std::runtime_error("WKB: unknown geometry type");
    }
}

} // namespace detail

// Distance from a point to a geometry whose rings were flattened by
// flatten_wkb_rings(), given as the half-open ring range [first, last).
// Mirrors the polygon semantics of wkb_point_distance().
inline double flat_rings_point_distance(const std::vector<double> & coords,
                                        const std::vector<RingRef> & rings,
                                        uint32_t first, uint32_t last,
                                        double px, double py)
{
    double best2 = std::numeric_limits<double>::infinity();

    for (uint32_t i = first; i < last; )
    {
        const RingRef & r = rings[i];
        detail::RingScan scan = detail::scan_ring_coords(
            coords.data() + 2 * static_cast<size_t>(r.offset), r.num_points, px, py);
        best2 = std::min(best2, scan.dist2);
        ++i;

        if (r.kind == RingKind::Open) continue;

        // A shell owns the holes that follow it.
        bool in_hole = false;
        for (; i < last && rings[i].kind == RingKind::Hole; ++i)
        {
            detail::RingScan hole = detail::scan_ring_coords(
                coords.data() + 2 * static_cast<size_t>(rings[i].offset),
                rings[i].num_points, px, py);
            best2 = std::min(best2, hole.dist2);
            in_hole |= hole.crossings_odd;
        }

        if (scan.crossings_odd && !in_hole) return 0.0;
    }

    return std::sqrt(best2);
}

} // namespace ch
