#pragma once

#include <span>
#include <string>

#include <geos/geom/Geometry.h>
#include <geos/geom/IntersectionMatrix.h>
#include <geos/geom/prep/PreparedGeometryFactory.h>

#include "../col_prep_op.hpp"
#include "../geom/wkb.hpp"
#include "../geom/wkb_envelope.hpp"

namespace ch {

using Geometry              = geos::geom::Geometry;
using Span                  = std::span<const uint8_t>;
using PreparedGeometry      = geos::geom::prep::PreparedGeometry;
using PreparedGeometryFactory = geos::geom::prep::PreparedGeometryFactory;

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

// ── Per-column PreparedGeometry callbacks ─────────────────────────────────────
// prep_a_*: col(0) is const — prep is built from A, other is the variable B.
// prep_b_*: col(1) is const — prep is built from B, other is the variable A.
// All must be non-capturing (+[]) to convert to ColPrepOp (raw function pointer).

constexpr ColPrepOp prep_a_st_contains =
    +[](const PreparedGeometry* pa, const Geometry* b) { return pa->contains(b); };
constexpr ColPrepOp prep_b_st_contains =
    +[](const PreparedGeometry* pb, const Geometry* a) { return pb->within(a); };

constexpr ColPrepOp prep_a_st_within =
    +[](const PreparedGeometry* pa, const Geometry* b) { return pa->within(b); };
constexpr ColPrepOp prep_b_st_within =
    +[](const PreparedGeometry* pb, const Geometry* a) { return pb->contains(a); };

constexpr ColPrepOp prep_a_st_covers =
    +[](const PreparedGeometry* pa, const Geometry* b) { return pa->covers(b); };
constexpr ColPrepOp prep_b_st_covers =
    +[](const PreparedGeometry* pb, const Geometry* a) { return pb->coveredBy(a); };

constexpr ColPrepOp prep_a_st_coveredby =
    +[](const PreparedGeometry* pa, const Geometry* b) { return pa->coveredBy(b); };
constexpr ColPrepOp prep_b_st_coveredby =
    +[](const PreparedGeometry* pb, const Geometry* a) { return pb->covers(a); };

constexpr ColPrepOp prep_a_st_intersects =
    +[](const PreparedGeometry* pa, const Geometry* b) { return pa->intersects(b); };
constexpr ColPrepOp prep_b_st_intersects =
    +[](const PreparedGeometry* pb, const Geometry* a) { return pb->intersects(a); };

constexpr ColPrepOp prep_a_st_disjoint =
    +[](const PreparedGeometry* pa, const Geometry* b) { return pa->disjoint(b); };
constexpr ColPrepOp prep_b_st_disjoint =
    +[](const PreparedGeometry* pb, const Geometry* a) { return pb->disjoint(a); };

constexpr ColPrepOp prep_a_st_overlaps =
    +[](const PreparedGeometry* pa, const Geometry* b) { return pa->overlaps(b); };
constexpr ColPrepOp prep_b_st_overlaps =
    +[](const PreparedGeometry* pb, const Geometry* a) { return pb->overlaps(a); };

constexpr ColPrepOp prep_a_st_crosses =
    +[](const PreparedGeometry* pa, const Geometry* b) { return pa->crosses(b); };
constexpr ColPrepOp prep_b_st_crosses =
    +[](const PreparedGeometry* pb, const Geometry* a) { return pb->crosses(a); };

constexpr ColPrepOp prep_a_st_touches =
    +[](const PreparedGeometry* pa, const Geometry* b) { return pa->touches(b); };
constexpr ColPrepOp prep_b_st_touches =
    +[](const PreparedGeometry* pb, const Geometry* a) { return pb->touches(a); };

// containsProperly: PreparedGeometry only accelerates the A-const direction.
constexpr ColPrepOp prep_a_st_containsproperly =
    +[](const PreparedGeometry* pa, const Geometry* b) { return pa->containsProperly(b); };
constexpr ColPrepOp prep_b_st_containsproperly = nullptr;  // no PrepGeom accel for B-const

// equals: PreparedGeometry has no equals(); fall through to underlying Geometry.
// Still saves WKB re-parse for the const side.
constexpr ColPrepOp prep_a_st_equals =
    +[](const PreparedGeometry* pa, const Geometry* b) { return pa->getGeometry().equals(b); };
constexpr ColPrepOp prep_b_st_equals =
    +[](const PreparedGeometry* pb, const Geometry* a) { return pb->getGeometry().equals(a); };

} // namespace ch
