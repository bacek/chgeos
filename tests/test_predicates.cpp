#include <gtest/gtest.h>
#include "helpers.hpp"

using namespace ch;

// WKT → GEOS Geometry (for _impl functions that take unique_ptr<Geometry>)
static auto W(const std::string & wkt) { return geom(wkt); }

// WKT → WKB span (for _impl functions that still take raw bytes: st_intersects_extent, st_dwithin)
static auto S(const std::string & wkt) {
  static std::vector<ch::Vector> storage;
  storage.push_back(wkt2wkb(wkt));
  return wkb(storage.back());
}

// ── ST_Distance ───────────────────────────────────────────────────────────────

TEST(StDistance, Pythagorean345) {
  EXPECT_DOUBLE_EQ(st_distance_impl(W("POINT (0 0)"), W("POINT (3 4)")), 5.0);
}

TEST(StDistance, Zero) {
  EXPECT_DOUBLE_EQ(st_distance_impl(W("POINT (5 5)"), W("POINT (5 5)")), 0.0);
}

TEST(StDistance, PointToLine) {
  // Point (0 1) to LINESTRING (0 0, 10 0) — perpendicular distance = 1
  EXPECT_DOUBLE_EQ(st_distance_impl(W("POINT (0 1)"), W("LINESTRING (0 0, 10 0)")), 1.0);
}

// ── ST_Contains ───────────────────────────────────────────────────────────────

TEST(StContains, PointInsidePolygon) {
  EXPECT_TRUE(st_contains_impl(W("POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))"), W("POINT (5 5)")));
}

TEST(StContains, PointOutsidePolygon) {
  EXPECT_FALSE(st_contains_impl(W("POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))"), W("POINT (-1 5)")));
}

// ── ST_Intersects ─────────────────────────────────────────────────────────────

TEST(StIntersects, PointInsidePolygon) {
  EXPECT_TRUE(st_intersects_impl(W("POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))"), W("POINT (5 5)")));
}

TEST(StIntersects, DisjointGeometries) {
  EXPECT_FALSE(st_intersects_impl(W("POINT (0 0)"), W("POINT (5 5)")));
}

TEST(StIntersects, CrossingLines) {
  EXPECT_TRUE(st_intersects_impl(W("LINESTRING (0 0, 10 10)"), W("LINESTRING (0 10, 10 0)")));
}

// ── ST_IntersectsExtent ───────────────────────────────────────────────────────

TEST(StIntersectsExtent, OverlappingPolygons) {
  EXPECT_TRUE(st_intersects_extent_impl(
    S("POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0))"),
    S("POLYGON ((1 1, 3 1, 3 3, 1 3, 1 1))")));
}

TEST(StIntersectsExtent, DisjointPolygons) {
  EXPECT_FALSE(st_intersects_extent_impl(
    S("POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))"),
    S("POLYGON ((2 2, 3 2, 3 3, 2 3, 2 2))")));
}

TEST(StIntersectsExtent, EnvelopeIntersectsButGeomDoesNot) {
  // Two L-shapes whose envelopes overlap but geometries don't.
  EXPECT_TRUE(st_intersects_extent_impl(
    S("LINESTRING (0 0, 1 0, 1 1)"),
    S("LINESTRING (0 1, 1 1, 1 2)")));
}

// ── ST_Touches ────────────────────────────────────────────────────────────────

TEST(StTouches, SharedEndpoint) {
  // Two lines sharing exactly one endpoint — touches, not intersects interior
  EXPECT_TRUE(st_touches_impl(W("LINESTRING (-1 0, 0 0)"), W("LINESTRING (0 0, 1 1)")));
}

TEST(StTouches, OverlappingLinesDontTouch) {
  // Overlapping lines share interior — intersects but does NOT touch
  EXPECT_FALSE(st_touches_impl(W("LINESTRING (0 0, 2 0)"), W("LINESTRING (1 0, 3 0)")));
}

// ── ST_DWithin ────────────────────────────────────────────────────────────────

TEST(StDWithin, ExactDistance) {
  // distance is exactly 5 — within 5 → true, within 4.9 → false
  EXPECT_TRUE( st_dwithin_impl(S("POINT (0 0)"), S("POINT (3 4)"), 5.0));
  EXPECT_FALSE(st_dwithin_impl(S("POINT (0 0)"), S("POINT (3 4)"), 4.9));
}

TEST(StDWithin, Coincident) {
  EXPECT_TRUE(st_dwithin_impl(S("POINT (1 1)"), S("POINT (1 1)"), 0.0));
}

// ── ST_Within ─────────────────────────────────────────────────────────────────

TEST(StWithin, PointInsidePolygon) {
  EXPECT_TRUE(st_within_impl(W("POINT (5 5)"), W("POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))")));
}

TEST(StWithin, PointOutside) {
  EXPECT_FALSE(st_within_impl(W("POINT (15 5)"), W("POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))")));
}

TEST(StWithin, ContainsIsNotWithin) {
  // contains and within are inverse — polygon is NOT within point
  EXPECT_FALSE(st_within_impl(W("POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))"), W("POINT (5 5)")));
}

// ── ST_Crosses ────────────────────────────────────────────────────────────────

TEST(StCrosses, CrossingLines) {
  EXPECT_TRUE(st_crosses_impl(W("LINESTRING (0 5, 10 5)"), W("LINESTRING (5 0, 5 10)")));
}

TEST(StCrosses, ParallelLines) {
  EXPECT_FALSE(st_crosses_impl(W("LINESTRING (0 0, 10 0)"), W("LINESTRING (0 1, 10 1)")));
}

// ── ST_Overlaps ───────────────────────────────────────────────────────────────

TEST(StOverlaps, PartiallyOverlappingPolygons) {
  EXPECT_TRUE(st_overlaps_impl(
    W("POLYGON ((0 0, 6 0, 6 6, 0 6, 0 0))"),
    W("POLYGON ((3 3, 9 3, 9 9, 3 9, 3 3))")));
}

TEST(StOverlaps, ContainedDoesNotOverlap) {
  EXPECT_FALSE(st_overlaps_impl(
    W("POLYGON ((1 1, 2 1, 2 2, 1 2, 1 1))"),
    W("POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))")));
}

// ── ST_Disjoint ───────────────────────────────────────────────────────────────

TEST(StDisjoint, SeparatePoints) {
  EXPECT_TRUE(st_disjoint_impl(W("POINT (0 0)"), W("POINT (5 5)")));
}

TEST(StDisjoint, TouchingLinesAreNotDisjoint) {
  // Sharing an endpoint → NOT disjoint
  EXPECT_FALSE(st_disjoint_impl(W("LINESTRING (0 0, 1 0)"), W("LINESTRING (1 0, 2 0)")));
}

// ── ST_Equals ─────────────────────────────────────────────────────────────────

TEST(StEquals, SamePoint) {
  EXPECT_TRUE(st_equals_impl(W("POINT (3 4)"), W("POINT (3 4)")));
}

TEST(StEquals, DifferentPoints) {
  EXPECT_FALSE(st_equals_impl(W("POINT (3 4)"), W("POINT (3 5)")));
}

TEST(StEquals, RingOrderDoesNotMatter) {
  // Same polygon, vertices in different order — geometrically equal
  EXPECT_TRUE(st_equals_impl(
    W("POLYGON ((0 0, 4 0, 4 4, 0 4, 0 0))"),
    W("POLYGON ((0 0, 0 4, 4 4, 4 0, 0 0))")));
}

// ── ST_Covers ─────────────────────────────────────────────────────────────────

TEST(StCovers, PolygonCoversInteriorPoint) {
  EXPECT_TRUE(st_covers_impl(W("POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))"), W("POINT (5 5)")));
}

TEST(StCovers, PolygonCoversBoundaryPoint) {
  EXPECT_TRUE(st_covers_impl(W("POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))"), W("POINT (5 0)")));
}

TEST(StCovers, DisjointNotCovered) {
  EXPECT_FALSE(st_covers_impl(W("POLYGON ((0 0, 5 0, 5 5, 0 5, 0 0))"), W("POINT (10 10)")));
}

// ── ST_CoveredBy ──────────────────────────────────────────────────────────────

TEST(StCoveredBy, PointCoveredByPolygon) {
  EXPECT_TRUE(st_coveredby_impl(W("POINT (5 5)"), W("POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))")));
}

TEST(StCoveredBy, BoundaryPointCoveredBy) {
  EXPECT_TRUE(st_coveredby_impl(W("POINT (0 5)"), W("POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))")));
}

// ── ST_ContainsProperly ───────────────────────────────────────────────────────

TEST(StContainsProperly, InteriorPoint) {
  EXPECT_TRUE(st_containsproperly_impl(
    W("POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))"),
    W("POINT (5 5)")));
}

TEST(StContainsProperly, BoundaryPointNotProper) {
  // boundary point is NOT properly contained
  EXPECT_FALSE(st_containsproperly_impl(
    W("POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))"),
    W("POINT (5 0)")));
}

// ── ST_Relate ─────────────────────────────────────────────────────────────────

TEST(StRelate, PointInsidePolygon) {
  auto matrix = st_relate_impl(
    geom("POINT (5 5)"),
    geom("POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))"));
  EXPECT_EQ(matrix.size(), 9u);
}

TEST(StRelate, DisjointPoints) {
  auto matrix = st_relate_impl(geom("POINT (0 0)"), geom("POINT (5 5)"));
  EXPECT_EQ(matrix, "FF0FFF0F2");
}

// ── ST_Relate (pattern) ───────────────────────────────────────────────────────

TEST(StRelatePattern, DisjointPointsFF) {
  std::string pat = "FF0FFF0F2";
  EXPECT_TRUE(st_relate_pattern_impl(
    geom("POINT (0 0)"), geom("POINT (5 5)"),
    pat));
}

TEST(StRelatePattern, WildcardPattern) {
  std::string pat = "0FFFFF212";
  EXPECT_TRUE(st_relate_pattern_impl(
    geom("POINT (5 5)"),
    geom("POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))"),
    pat));
}
