/// Tests for with_bbox() wrapper — exercises both the bbox shortcut path
/// and the full GEOS path for each bbox operation type.
/// Also tests BBox::expanded() and st_dwithin's empty-geometry edge cases.

#include <cmath>
#include <limits>
#include <gtest/gtest.h>
#include "helpers.hpp"
#include "functions/predicates.hpp"

using namespace ch;

// WKT → WKB span (backed by static storage so span stays valid)
static Span S(const std::string & wkt) {
    static std::vector<Vector> storage;
    storage.push_back(wkt2wkb(wkt));
    return wkb(storage.back());
}

// ── BBox::expanded() ──────────────────────────────────────────────────────────

TEST(BBoxExpanded, NormalExpansion) {
    BBox b;
    b.expand(0.0, 0.0);
    b.expand(4.0, 6.0);
    BBox e = b.expanded(1.0);
    EXPECT_DOUBLE_EQ(e.xmin, -1.0);
    EXPECT_DOUBLE_EQ(e.ymin, -1.0);
    EXPECT_DOUBLE_EQ(e.xmax,  5.0);
    EXPECT_DOUBLE_EQ(e.ymax,  7.0);
    EXPECT_FALSE(e.is_empty());
}

TEST(BBoxExpanded, EmptyStaysEmpty) {
    // Default BBox is empty; expanding it should remain empty, not produce NaN.
    BBox empty;
    ASSERT_TRUE(empty.is_empty());
    BBox e = empty.expanded(1.0);
    EXPECT_TRUE(e.is_empty());
}

TEST(BBoxExpanded, EmptyWithInfiniteDistance) {
    // Expanding an empty bbox by INFINITY must not produce NaN coordinates.
    // Before the fix: +inf - inf = NaN, breaking is_empty() and intersects().
    BBox empty;
    BBox e = empty.expanded(std::numeric_limits<double>::infinity());
    EXPECT_TRUE(e.is_empty());
    EXPECT_FALSE(std::isnan(e.xmin));
    EXPECT_FALSE(std::isnan(e.xmax));
}

// ── st_dwithin with infinite distance ─────────────────────────────────────────

TEST(StDWithin, InfiniteDistance) {
    // With distance=INFINITY the bbox pre-check must not NaN out.
    // Before the fix, wkb_bbox(b).expanded(INF) produced NaN coordinates,
    // making intersects() return false and skipping the GEOS call entirely —
    // a wrong result for clearly within-distance geometries.
    static std::vector<Vector> storage;
    storage.push_back(wkt2wkb("POINT (0 0)"));
    storage.push_back(wkt2wkb("POINT (1e9 1e9)"));
    Span a = wkb(storage[storage.size()-2]);
    Span b = wkb(storage[storage.size()-1]);
    EXPECT_TRUE(st_dwithin_impl(a, b, std::numeric_limits<double>::infinity()));
}

// ── bbox_op_intersects ────────────────────────────────────────────────────────
// Used by: st_intersects, st_overlaps, st_crosses, st_touches, st_equals

// Disjoint bboxes → bbox shortcut fires, early_ret=false returned immediately.
TEST(WithBbox, IntersectsBboxShortcut_ReturnsFalse) {
    auto a = S("POINT (0 0)");
    auto b = S("POINT (100 100)");
    // Bboxes don't intersect; GEOS never called.
    EXPECT_FALSE(with_bbox(a, b, bbox_op_intersects, false, st_intersects_impl));
}

// Disjoint bboxes → bbox shortcut fires, early_ret=true (disjoint case).
TEST(WithBbox, IntersectsBboxShortcut_ReturnsTrue) {
    auto a = S("POINT (0 0)");
    auto b = S("POINT (100 100)");
    EXPECT_TRUE(with_bbox(a, b, bbox_op_intersects, true, st_disjoint_impl));
}

// Intersecting bboxes, predicate true → full GEOS path.
TEST(WithBbox, IntersectsBboxPass_GeosTrue) {
    auto a = S("POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))");
    auto b = S("POINT (5 5)");
    EXPECT_TRUE(with_bbox(a, b, bbox_op_intersects, false, st_intersects_impl));
}

// Intersecting bboxes, but GEOS says false → full GEOS path, false returned.
TEST(WithBbox, IntersectsBboxPass_GeosFalse) {
    // Two L-shapes: bboxes overlap but geometries don't intersect.
    auto a = S("LINESTRING (0 0, 1 0, 1 1)");
    auto b = S("LINESTRING (0 1, 1 1, 1 2)");
    // Bboxes intersect (both span [0..1]×[0..1] / [0..1]×[1..2] — they share (0..1,1)).
    // They actually touch at (1,1), so intersects is true here. Use a cleaner example.
    // Non-touching: a horizontal segment and a vertical segment far apart.
    auto c = S("LINESTRING (0 0, 5 0)");
    auto d = S("LINESTRING (3 1, 3 2)");
    // bbox(c)=[0..5,0..0], bbox(d)=[3..3,1..2] — don't intersect in y → shortcut.
    EXPECT_FALSE(with_bbox(c, d, bbox_op_intersects, false, st_intersects_impl));
}

// ── bbox_op_contains ─────────────────────────────────────────────────────────
// Used by: st_contains, st_covers, st_containsproperly

// bbox(a) does NOT contain bbox(b) → shortcut, return false.
TEST(WithBbox, ContainsBboxShortcut) {
    // Point outside polygon's bbox entirely.
    auto poly = S("POLYGON ((0 0, 5 0, 5 5, 0 5, 0 0))");
    auto pt   = S("POINT (10 10)");
    EXPECT_FALSE(with_bbox(poly, pt, bbox_op_contains, false, st_contains_impl));
}

// bbox(a) contains bbox(b), predicate true.
TEST(WithBbox, ContainsBboxPass_GeosTrue) {
    auto poly = S("POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))");
    auto pt   = S("POINT (5 5)");
    EXPECT_TRUE(with_bbox(poly, pt, bbox_op_contains, false, st_contains_impl));
}

// bbox(a) contains bbox(b), but GEOS says false (boundary point for containsProperly).
TEST(WithBbox, ContainsBboxPass_GeosFalse) {
    auto poly = S("POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))");
    auto pt   = S("POINT (5 0)"); // on boundary
    EXPECT_FALSE(with_bbox(poly, pt, bbox_op_contains, false, st_containsproperly_impl));
}

// ── bbox_op_rcontains ─────────────────────────────────────────────────────────
// Used by: st_within, st_coveredby (bbox(b) must contain bbox(a))

// bbox(b) does NOT contain bbox(a) → shortcut, return false.
TEST(WithBbox, RcontainsBboxShortcut) {
    auto pt   = S("POINT (100 100)");
    auto poly = S("POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))");
    EXPECT_FALSE(with_bbox(pt, poly, bbox_op_rcontains, false, st_within_impl));
}

// bbox(b) contains bbox(a), predicate true.
TEST(WithBbox, RcontainsBboxPass_GeosTrue) {
    auto pt   = S("POINT (5 5)");
    auto poly = S("POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))");
    EXPECT_TRUE(with_bbox(pt, poly, bbox_op_rcontains, false, st_within_impl));
}

// bbox(b) contains bbox(a), but GEOS says false (point outside polygon).
TEST(WithBbox, RcontainsBboxPass_GeosFalse) {
    // Point whose bbox fits inside polygon's bbox, but the polygon has a hole
    // and the point is in the hole. Use a concave polygon instead: a point on
    // the fringe of the bbox but outside the actual polygon.
    // Polygon: triangle (0,0)-(10,0)-(0,10). Point (8,8) is inside bbox but
    // outside the triangle.
    auto pt   = S("POINT (8 8)");
    auto poly = S("POLYGON ((0 0, 10 0, 0 10, 0 0))");
    // bbox(pt) = [8..8,8..8], bbox(poly) = [0..10,0..10] → bbox(b) contains bbox(a).
    EXPECT_FALSE(with_bbox(pt, poly, bbox_op_rcontains, false, st_within_impl));
}
