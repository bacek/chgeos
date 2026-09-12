// Convex hull on raw coordinates, bit-identical to GEOS 3.12
// geos::algorithm::ConvexHull.
//
// GEOS's hull operates on a vector of coordinate pointers throughout; geometry
// objects appear only as the input source (a coordinate filter over the input
// geometry) and the output shell (a Point/LineString/Polygon built around the
// ring).  This file runs the same algorithm — unique extraction, octolateral
// reduce, focal pre-sort, radial comparator, Graham scan, ring cleaning,
// degenerate collapse — directly on coordinates held in the caller's pool, and
// appends the resulting ring to a FlatBatch.  Orientation tests are GEOS's own
// Orientation::index calls, so results are bit-identical by construction, not
// approximate.
//
// The GEOS-side originals live in third-party/geos/src/algorithm/ConvexHull.cpp.
// Any GEOS upgrade that changes ConvexHull must change this port in lockstep;
// the hull bit-exactness tests in tests/test_chain.cpp are that gate.
//
// Ring shapes mirror the geometry GEOS would have built:
//   0 unique coords → one empty ring     (GEOS: empty geometry,  area/length 0)
//   1 unique coord  → one 1-vertex ring  (GEOS: POINT)
//   2 unique coords → one 2-vertex ring  (GEOS: LINESTRING)
//   otherwise       → cleaned ring, closed (first == last), CCW (GEOS polygon),
//                     or a 2-vertex ring when the ring collapses to a line

#pragma once

#include <algorithm>
#include <cstddef>
#include <set>
#include <vector>

#include <geos/algorithm/Orientation.h>
#include <geos/algorithm/PointLocation.h>
#include <geos/geom/Coordinate.h>

#include "flat_batch.hpp"

namespace ch {
namespace flat_hull {

using geos::geom::Coordinate;
using CoordinateP = const Coordinate*;

// Scratch reused across rows so the per-row cost is not dominated by allocator
// traffic.  Callers keep one instance per block.
struct Buffers {
    std::vector<Coordinate>          pool;     // stable coordinate storage for one row
    std::vector<CoordinateP>         pts;      // unique pointers, extraction order
    std::vector<CoordinateP>         scan;     // graham scan output
    std::vector<CoordinateP>         cleaned;  // cleanRing output
};

// ConvexHull.cpp TUNING_REDUCE_SIZE.
inline constexpr std::size_t TUNING_REDUCE_SIZE = 50;

// ── Unique extraction ─────────────────────────────────────────────────────────
// UniqueCoordinateArrayFilter: first-occurrence order, equality by (x, y).
// NaN coordinates are the caller's problem (declined before extraction).
inline void extract_unique(std::vector<Coordinate>& pool,
                           std::vector<CoordinateP>& pts) {
    pts.clear();
    pts.reserve(pool.size());
    for (std::size_t i = 0; i < pool.size(); ++i) {
        bool dup = false;
        for (CoordinateP q : pts) {
            if (q->x == pool[i].x && q->y == pool[i].y) { dup = true; break; }
        }
        if (!dup) pts.push_back(&pool[i]);
    }
}

// ── reduce() ──────────────────────────────────────────────────────────────────
inline void computeInnerOctolateralPts(const std::vector<CoordinateP>& inputPts,
                                       std::vector<CoordinateP>& pts) {
    pts.assign(8, inputPts[0]);
    for (std::size_t i = 1, n = inputPts.size(); i < n; ++i) {
        if (inputPts[i]->x < pts[0]->x)
            pts[0] = inputPts[i];
        if (inputPts[i]->x - inputPts[i]->y < pts[1]->x - pts[1]->y)
            pts[1] = inputPts[i];
        if (inputPts[i]->y > pts[2]->y)
            pts[2] = inputPts[i];
        if (inputPts[i]->x + inputPts[i]->y > pts[3]->x + pts[3]->y)
            pts[3] = inputPts[i];
        if (inputPts[i]->x > pts[4]->x)
            pts[4] = inputPts[i];
        if (inputPts[i]->x - inputPts[i]->y > pts[5]->x - pts[5]->y)
            pts[5] = inputPts[i];
        if (inputPts[i]->y < pts[6]->y)
            pts[6] = inputPts[i];
        if (inputPts[i]->x + inputPts[i]->y < pts[7]->x + pts[7]->y)
            pts[7] = inputPts[i];
    }
}

inline bool computeInnerOctolateralRing(const std::vector<CoordinateP>& inputPts,
                                        std::vector<CoordinateP>& dest) {
    computeInnerOctolateralPts(inputPts, dest);
    dest.erase(std::unique(dest.begin(), dest.end()), dest.end());
    if (dest.size() < 3)
        return false;
    dest.push_back(dest[0]);
    return true;
}

inline void padArray3(std::vector<CoordinateP>& pts) {
    for (std::size_t i = pts.size(); i < 3; ++i)
        pts.push_back(pts[0]);
}

// Reduces pts in place via the interior octolateral polygon; on failure leaves
// pts untouched, exactly like GEOS's early return.
inline void reduce(std::vector<CoordinateP>& pts, std::vector<CoordinateP>& ring) {
    if (!computeInnerOctolateralRing(pts, ring))
        return;

    std::set<CoordinateP, geos::geom::CoordinateLessThan> reducedSet;
    reducedSet.insert(ring.begin(), ring.end());

    for (CoordinateP p : pts)
        if (!geos::algorithm::PointLocation::isInRing(*p, ring))
            reducedSet.insert(p);

    pts.assign(reducedSet.begin(), reducedSet.end());
    if (pts.size() < 3)
        padArray3(pts);
}

// ── preSort ───────────────────────────────────────────────────────────────────
inline void preSort(std::vector<CoordinateP>& pts) {
    for (std::size_t i = 1, n = pts.size(); i < n; ++i) {
        const Coordinate* p0 = pts[0];
        const Coordinate* pi = pts[i];
        if ((pi->y < p0->y) || ((pi->y == p0->y) && (pi->x < p0->x))) {
            const Coordinate* t = p0;
            pts[0] = pi;
            pts[i] = t;
        }
    }

    // RadiallyLessThen, verbatim from ConvexHull.cpp.
    const Coordinate* origin = pts[0];
    std::sort(pts.begin(), pts.end(), [origin](const Coordinate* p1, const Coordinate* p2) {
        int orient = geos::algorithm::Orientation::index(*origin, *p1, *p2);
        if (orient == geos::algorithm::Orientation::COUNTERCLOCKWISE) return false; // polarCompare != -1
        if (orient == geos::algorithm::Orientation::CLOCKWISE)        return true;
        if (p1->y > p2->y) return false;
        if (p1->y < p2->y) return true;
        if (p1->x > p2->x) return false;
        if (p1->x < p2->x) return true;
        return false; // p1 == p2
    });
}

// ── grahamScan ────────────────────────────────────────────────────────────────
inline void grahamScan(const std::vector<CoordinateP>& c, std::vector<CoordinateP>& ps) {
    ps.clear();
    ps.push_back(c[0]);
    ps.push_back(c[1]);
    ps.push_back(c[2]);

    for (std::size_t i = 3, n = c.size(); i < n; ++i) {
        const Coordinate* p = ps.back();
        ps.pop_back();
        while (!ps.empty() &&
               geos::algorithm::Orientation::index(*ps.back(), *p, *c[i]) > 0) {
            p = ps.back();
            ps.pop_back();
        }
        ps.push_back(p);
        ps.push_back(c[i]);
    }
    ps.push_back(c[0]);
}

// ── cleanRing ─────────────────────────────────────────────────────────────────
inline bool isBetween(const Coordinate& c1, const Coordinate& c2, const Coordinate& c3) {
    if (geos::algorithm::Orientation::index(c1, c2, c3) != 0)
        return false;
    if (c1.x != c3.x) {
        if (c1.x <= c2.x && c2.x <= c3.x) return true;
        if (c3.x <= c2.x && c2.x <= c1.x) return true;
    }
    if (c1.y != c3.y) {
        if (c1.y <= c2.y && c2.y <= c3.y) return true;
        if (c3.y <= c2.y && c2.y <= c1.y) return true;
    }
    return false;
}

inline void cleanRing(const std::vector<CoordinateP>& original,
                      std::vector<CoordinateP>& cleaned) {
    cleaned.clear();
    std::size_t npts = original.size();
    const Coordinate* last = original[npts - 1];

    const Coordinate* prev = nullptr;
    for (std::size_t i = 0; i < npts - 1; ++i) {
        const Coordinate* curr = original[i];
        const Coordinate* next = original[i + 1];
        if (curr->equals2D(*next))
            continue;
        if (prev != nullptr && isBetween(*prev, *curr, *next))
            continue;
        cleaned.push_back(curr);
        prev = curr;
    }
    cleaned.push_back(last);
}

// ── Top level ─────────────────────────────────────────────────────────────────
// Appends the hull ring for the row's coordinates (already NaN-free, held in
// buf.pool) to fb as exactly one ring.
inline void append_hull_ring(Buffers& buf, FlatBatch& fb) {
    extract_unique(buf.pool, buf.pts);

    fb.begin_ring();
    auto emit = [&](const std::vector<CoordinateP>& ring) {
        for (const Coordinate* p : ring) fb.push_vertex(p->x, p->y);
    };

    if (buf.pts.size() <= 2) {
        emit(buf.pts);  // 0, 1 or 2 vertices — mirrors empty / point / line hulls
        return;
    }

    if (buf.pts.size() > TUNING_REDUCE_SIZE)
        reduce(buf.pts, buf.scan);  // scan used as the octolateral ring buffer

    preSort(buf.pts);
    grahamScan(buf.pts, buf.scan);
    cleanRing(buf.scan, buf.cleaned);

    if (buf.cleaned.size() == 3) {
        // lineOrPolygon collapses a 3-point cleaned ring to a 2-vertex LINESTRING
        fb.push_vertex(buf.cleaned[0]->x, buf.cleaned[0]->y);
        fb.push_vertex(buf.cleaned[1]->x, buf.cleaned[1]->y);
        return;
    }
    emit(buf.cleaned);
}

} // namespace flat_hull
} // namespace ch
