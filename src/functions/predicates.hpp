#pragma once

#include <span>
#include <string>

#include <geos/geom/Geometry.h>
#include <geos/geom/IntersectionMatrix.h>
#include <geos/geom/prep/PreparedGeometryFactory.h>

#include "../geom/wkb.hpp"
#include "../geom/wkb_envelope.hpp"

namespace ch {

using Geometry = geos::geom::Geometry;
using Span     = std::span<const uint8_t>;

// ── Bbox operations ────────────────────────────────────────────────────────────

using BboxOp = bool (*)(const BBox &, const BBox &);
inline bool bbox_op_intersects(const BBox & a, const BBox & b) { return a.intersects(b); }
inline bool bbox_op_contains  (const BBox & a, const BBox & b) { return a.contains(b); }
inline bool bbox_op_rcontains (const BBox & a, const BBox & b) { return b.contains(a); }

// ── Bbox wrapper ───────────────────────────────────────────────────────────────
// Receives a bbox operation and an impl function (taking unique_ptr<Geometry>).
// Applies the bbox shortcut on raw spans; only deserialises to Geometry when the
// bbox check passes.  Returns early_ret on bbox mismatch.
template <typename Impl>
inline auto with_bbox(Span a, Span b, BboxOp bbox_op, bool early_ret, Impl impl) {
    if (!bbox_op(wkb_bbox(a), wkb_bbox(b))) return early_ret;
    return impl(read_wkb(a), read_wkb(b));
}

// ── Pure-bbox predicate (no GEOS) ─────────────────────────────────────────────

inline bool st_intersects_extent_impl(Span a, Span b) {
    return wkb_bbox(a).intersects(wkb_bbox(b));
}

// ── Pure GEOS impl functions (same as before fast bbox commit d9f6b41) ─────────
// These receive fully-parsed GEOS geometries; the bbox shortcut is applied at
// the UDF registration layer via with_bbox.

inline bool st_intersects_impl(std::unique_ptr<Geometry> a, std::unique_ptr<Geometry> b) {
    return a->intersects(b.get());
}

inline bool st_contains_impl(std::unique_ptr<Geometry> a, std::unique_ptr<Geometry> b) {
    return a->contains(b.get());
}

inline bool st_within_impl(std::unique_ptr<Geometry> a, std::unique_ptr<Geometry> b) {
    return a->within(b.get());
}

inline bool st_covers_impl(std::unique_ptr<Geometry> a, std::unique_ptr<Geometry> b) {
    return a->covers(b.get());
}

inline bool st_coveredby_impl(std::unique_ptr<Geometry> a, std::unique_ptr<Geometry> b) {
    return a->coveredBy(b.get());
}

inline bool st_overlaps_impl(std::unique_ptr<Geometry> a, std::unique_ptr<Geometry> b) {
    return a->overlaps(b.get());
}

inline bool st_crosses_impl(std::unique_ptr<Geometry> a, std::unique_ptr<Geometry> b) {
    return a->crosses(b.get());
}

inline bool st_touches_impl(std::unique_ptr<Geometry> a, std::unique_ptr<Geometry> b) {
    return a->touches(b.get());
}

inline bool st_containsproperly_impl(std::unique_ptr<Geometry> a, std::unique_ptr<Geometry> b) {
    auto prep = geos::geom::prep::PreparedGeometryFactory::prepare(a.get());
    return prep->containsProperly(b.get());
}

inline double st_distance_impl(std::unique_ptr<Geometry> a, std::unique_ptr<Geometry> b) {
    return a->distance(b.get());
}

inline bool st_dwithin_impl(Span a, Span b, double distance) {
    if (!wkb_bbox(a).intersects(wkb_bbox(b).expanded(distance))) return false;
    return read_wkb(a)->isWithinDistance(read_wkb(b).get(), distance);
}

inline bool st_disjoint_impl(std::unique_ptr<Geometry> a, std::unique_ptr<Geometry> b) {
    return a->disjoint(b.get());
}

inline bool st_equals_impl(std::unique_ptr<Geometry> a, std::unique_ptr<Geometry> b) {
    return a->equals(b.get());
}

inline std::string st_relate_impl(std::unique_ptr<Geometry> a, std::unique_ptr<Geometry> b) {
    return a->relate(b.get())->toString();
}

inline bool st_relate_pattern_impl(std::unique_ptr<Geometry> a, std::unique_ptr<Geometry> b, std::string_view pattern) {
    return a->relate(*b, std::string(pattern));
}

} // namespace ch
