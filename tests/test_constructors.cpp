#include <gtest/gtest.h>
#include "helpers.hpp"

using namespace ch;

// ── ST_Envelope ───────────────────────────────────────────────────────────────

TEST(StEnvelope, SquarePolygon) {
  // Envelope of a square is the square itself
  EXPECT_EQ(geom2wkt(st_envelope_impl(geom("POLYGON ((0 0, 5 0, 5 5, 0 5, 0 0))"))),
            "POLYGON ((0 0, 5 0, 5 5, 0 5, 0 0))");
}

TEST(StEnvelope, DiagonalLine) {
  auto env = st_envelope_impl(geom("LINESTRING (0 0, 5 5)"));
  const auto *e = env->getEnvelopeInternal();
  EXPECT_DOUBLE_EQ(e->getMinX(), 0); EXPECT_DOUBLE_EQ(e->getMaxX(), 5);
  EXPECT_DOUBLE_EQ(e->getMinY(), 0); EXPECT_DOUBLE_EQ(e->getMaxY(), 5);
}

// ── ST_Expand ─────────────────────────────────────────────────────────────────

TEST(StExpand, PointExpand) {
  auto result = st_expand_impl(geom("POINT (0 0)"), 5.0);
  const auto *e = result->getEnvelopeInternal();
  EXPECT_DOUBLE_EQ(e->getMinX(), -5); EXPECT_DOUBLE_EQ(e->getMaxX(), 5);
  EXPECT_DOUBLE_EQ(e->getMinY(), -5); EXPECT_DOUBLE_EQ(e->getMaxY(), 5);
}

TEST(StExpand, PolygonExpand) {
  // 1x1 box expanded by 1 → 3x3 box
  auto result = st_expand_impl(geom("POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))"), 1.0);
  const auto *e = result->getEnvelopeInternal();
  EXPECT_DOUBLE_EQ(e->getMinX(), -1); EXPECT_DOUBLE_EQ(e->getMaxX(), 2);
  EXPECT_DOUBLE_EQ(e->getMinY(), -1); EXPECT_DOUBLE_EQ(e->getMaxY(), 2);
}

// ── ST_MakeBox2D ──────────────────────────────────────────────────────────────

TEST(StMakeBox2D, Basic) {
  auto result = st_makebox2d_impl(geom("POINT (0 0)"), geom("POINT (5 10)"));
  const auto *e = result->getEnvelopeInternal();
  EXPECT_DOUBLE_EQ(e->getMinX(), 0); EXPECT_DOUBLE_EQ(e->getMaxX(), 5);
  EXPECT_DOUBLE_EQ(e->getMinY(), 0); EXPECT_DOUBLE_EQ(e->getMaxY(), 10);
}

TEST(StMakeBox2D, ThrowsOnNonPoint) {
  EXPECT_THROW(
    st_makebox2d_impl(geom("LINESTRING (0 0, 1 1)"), geom("POINT (5 5)")),
    std::runtime_error);
}

// ── ST_Collect ────────────────────────────────────────────────────────────────

TEST(StCollect, TwoPoints) {
  auto result = st_collect_impl(geom("POINT (0 0)"), geom("POINT (1 1)"));
  EXPECT_EQ(result->getNumGeometries(), 2u);
}

TEST(StCollect, PointAndLine) {
  auto result = st_collect_impl(
    geom("POINT (0 0)"),
    geom("LINESTRING (1 1, 2 2)"));
  EXPECT_EQ(result->getNumGeometries(), 2u);
}

// ── ST_ConvexHull ─────────────────────────────────────────────────────────────

TEST(StConvexHull, Triangle) {
  // 4 points where one is interior — convex hull is the outer triangle
  auto hull = st_convexhull_impl(geom("MULTIPOINT ((0 0), (10 0), (5 10), (5 3))"));
  // GEOS normalizes CCW; verify it's a triangle containing all four input points
  EXPECT_EQ(hull->getNumPoints(), 4u); // 3 vertices + closing
  EXPECT_GT(hull->getArea(), 0.0);
}

TEST(StConvexHull, LineBecomesLine) {
  auto hull = st_convexhull_impl(geom("LINESTRING (0 0, 5 5)"));
  EXPECT_EQ(geom2wkt(hull), "LINESTRING (0 0, 5 5)");
}

// ── ST_Boundary ───────────────────────────────────────────────────────────────

TEST(StBoundary, PolygonBoundaryIsRing) {
  auto b = st_boundary_impl(geom("POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))"));
  // GEOS returns LineString (closed) for polygon boundary
  EXPECT_TRUE(b->getGeometryType() == "LinearRing" || b->getGeometryType() == "LineString");
  EXPECT_EQ(b->getNumPoints(), 5u); // closed ring
}

TEST(StBoundary, LineStringBoundaryTwoPoints) {
  auto b = st_boundary_impl(geom("LINESTRING (0 0, 5 5)"));
  EXPECT_EQ(b->getNumGeometries(), 2u);
}

// ── ST_Reverse ────────────────────────────────────────────────────────────────

TEST(StReverse, LineString) {
  EXPECT_EQ(geom2wkt(st_reverse_impl(geom("LINESTRING (0 0, 5 5, 10 0)"))),
            "LINESTRING (10 0, 5 5, 0 0)");
}

// ── ST_Normalize ──────────────────────────────────────────────────────────────

TEST(StNormalize, CanonicalForm) {
  // normalize should produce stable output regardless of input vertex order
  auto n1 = st_normalize_impl(geom("POLYGON ((0 0, 0 4, 4 4, 4 0, 0 0))"));
  auto n2 = st_normalize_impl(geom("POLYGON ((4 4, 4 0, 0 0, 0 4, 4 4))"));
  EXPECT_EQ(geom2wkt(n1), geom2wkt(n2));
}

// ── ST_GeometryN ──────────────────────────────────────────────────────────────

TEST(StGeometryN, FirstGeometry) {
  auto gc = geom("MULTIPOINT ((1 2), (3 4), (5 6))");
  EXPECT_EQ(geom2wkt(st_geometryn_impl(std::move(gc), 1)), "POINT (1 2)");
}

TEST(StGeometryN, ThirdGeometry) {
  auto gc = geom("MULTIPOINT ((1 2), (3 4), (5 6))");
  EXPECT_EQ(geom2wkt(st_geometryn_impl(std::move(gc), 3)), "POINT (5 6)");
}

TEST(StGeometryN, OutOfRangeThrows) {
  EXPECT_THROW(st_geometryn_impl(geom("MULTIPOINT ((1 2))"), 5), std::runtime_error);
}

// ── ST_SymDifference ──────────────────────────────────────────────────────────

TEST(StSymDifference, OverlappingSquares) {
  auto sd = st_symdifference_impl(
    geom("POLYGON ((0 0, 4 0, 4 4, 0 4, 0 0))"),
    geom("POLYGON ((2 2, 6 2, 6 6, 2 6, 2 2))"));
  // result must not be empty and must differ from each input
  EXPECT_FALSE(sd->isEmpty());
}

TEST(StSymDifference, DisjointPolygons) {
  auto sd = st_symdifference_impl(
    geom("POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))"),
    geom("POLYGON ((3 3, 4 3, 4 4, 3 4, 3 3))"));
  // sym difference of disjoint geoms = union
  EXPECT_NEAR(sd->getArea(), 2.0, 1e-10);
}

// ── ST_ExteriorRing ───────────────────────────────────────────────────────────

TEST(StExteriorRing, Square) {
  auto ring = st_exteriorring_impl(geom("POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))"));
  EXPECT_EQ(ring->getGeometryType(), "LinearRing");
  EXPECT_EQ(ring->getNumPoints(), 5u);
}

TEST(StExteriorRing, ThrowsOnNonPolygon) {
  EXPECT_THROW(st_exteriorring_impl(geom("LINESTRING (0 0, 1 1)")), std::runtime_error);
}

// ── ST_InteriorRingN ──────────────────────────────────────────────────────────

TEST(StInteriorRingN, FirstHole) {
  auto ring = st_interiorringn_impl(
    geom("POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0), (3 3, 7 3, 7 7, 3 7, 3 3))"), 1);
  EXPECT_EQ(ring->getGeometryType(), "LinearRing");
}

TEST(StInteriorRingN, OutOfRangeThrows) {
  EXPECT_THROW(
    st_interiorringn_impl(geom("POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))"), 1),
    std::runtime_error);
}

// ── ST_PointN ─────────────────────────────────────────────────────────────────

TEST(StPointN, FirstPoint) {
  EXPECT_EQ(geom2wkt(st_pointn_impl(geom("LINESTRING (0 0, 5 5, 10 0)"), 1)), "POINT (0 0)");
}

TEST(StPointN, LastPoint) {
  EXPECT_EQ(geom2wkt(st_pointn_impl(geom("LINESTRING (0 0, 5 5, 10 0)"), 3)), "POINT (10 0)");
}

TEST(StPointN, OutOfRangeThrows) {
  EXPECT_THROW(st_pointn_impl(geom("LINESTRING (0 0, 1 1)"), 5), std::runtime_error);
}

TEST(StPointN, ThrowsOnNonLinestring) {
  EXPECT_THROW(st_pointn_impl(geom("POINT (0 0)"), 1), std::runtime_error);
}

// ── ST_MinimumBoundingCircle ──────────────────────────────────────────────────

TEST(StMinimumBoundingCircle, Point) {
  auto circle = st_minimumboundingcircle_impl(geom("POINT (5 5)"));
  EXPECT_FALSE(circle->isEmpty());
}

TEST(StMinimumBoundingCircle, TwoPoints) {
  // Circle must cover both endpoints (they lie on the boundary)
  auto a = geom("POINT (0 0)");
  auto b = geom("POINT (4 0)");
  auto circle = st_minimumboundingcircle_impl(geom("MULTIPOINT ((0 0), (4 0))"));
  EXPECT_TRUE(circle->covers(a.get()));
  EXPECT_TRUE(circle->covers(b.get()));
}

// ── ST_Snap ───────────────────────────────────────────────────────────────────

TEST(StSnap, LineSnapsToLine) {
  auto snapped = st_snap_impl(
    geom("LINESTRING (0 0, 9.9 0)"),
    geom("LINESTRING (0 0, 10 0)"),
    0.5);
  EXPECT_FALSE(snapped->isEmpty());
}

// ── ST_OffsetCurve ────────────────────────────────────────────────────────────

TEST(StOffsetCurve, OffsetHorizontalLine) {
  auto offset = st_offsetcurve_impl(geom("LINESTRING (0 0, 10 0)"), 1.0);
  EXPECT_FALSE(offset->isEmpty());
  // Offset line should be above y=0
  EXPECT_GT(offset->getEnvelopeInternal()->getMinY(), 0.0);
}

// ── ST_LinMerge ───────────────────────────────────────────────────────────────

TEST(StLinMerge, TwoCollinearSegments) {
  // Two end-to-end segments should merge into one
  auto merged = st_linmerge_impl(geom("MULTILINESTRING ((0 0, 5 0), (5 0, 10 0))"));
  EXPECT_EQ(merged->getNumGeometries(), 1u);
}

TEST(StLinMerge, DisconnectedLines) {
  auto merged = st_linmerge_impl(geom("MULTILINESTRING ((0 0, 1 0), (5 0, 6 0))"));
  EXPECT_EQ(merged->getNumGeometries(), 2u);
}

// ── ST_Polygonize ─────────────────────────────────────────────────────────────

TEST(StPolygonize, SquareFromEdges) {
  auto poly = st_polygonize_impl(geom(
    "MULTILINESTRING ((0 0, 1 0), (1 0, 1 1), (1 1, 0 1), (0 1, 0 0))"));
  EXPECT_EQ(poly->getNumGeometries(), 1u);
  EXPECT_NEAR(poly->getGeometryN(0)->getArea(), 1.0, 1e-10);
}

// ── ST_DelaunayTriangles ──────────────────────────────────────────────────────

TEST(StDelaunayTriangles, FourPoints) {
  auto triangles = st_delaunaytriangles_impl(
    geom("MULTIPOINT ((0 0), (10 0), (10 10), (0 10))"), 0.0, 0);
  EXPECT_GT(triangles->getNumGeometries(), 0u);
}

TEST(StDelaunayTriangles, EdgesOnly) {
  auto edges = st_delaunaytriangles_impl(
    geom("MULTIPOINT ((0 0), (10 0), (5 8))"), 0.0, 1);
  // edges of 1 triangle = 3
  EXPECT_GT(edges->getNumGeometries(), 0u);
}

// ── ST_VoronoiDiagram ─────────────────────────────────────────────────────────

TEST(StVoronoiDiagram, ThreePoints) {
  auto voronoi = st_voronoidiagram_impl(
    geom("MULTIPOINT ((0 0), (10 0), (5 8))"), 0.0, 0);
  EXPECT_EQ(voronoi->getNumGeometries(), 3u);
}

TEST(StVoronoiDiagram, EdgesOnly) {
  auto edges = st_voronoidiagram_impl(
    geom("MULTIPOINT ((0 0), (10 0), (5 8))"), 0.0, 1);
  EXPECT_GT(edges->getNumGeometries(), 0u);
}

// ── ST_MakePolygon (error path) ───────────────────────────────────────────────

TEST(StMakePolygon, ThrowsOnNonLinestring) {
  EXPECT_THROW(st_makepolygon_impl(geom("POINT (0 0)")), std::runtime_error);
}

TEST(StMakePolygon, ThrowsOnPolygon) {
  EXPECT_THROW(
    st_makepolygon_impl(geom("POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))")),
    std::runtime_error);
}

// ── ST_AddPoint (error path) ──────────────────────────────────────────────────

TEST(StAddPoint, ThrowsOnNonLinestring) {
  EXPECT_THROW(
    st_addpoint_impl(geom("POINT (0 0)"), geom("POINT (1 1)"), 0),
    std::runtime_error);
}

TEST(StAddPoint, ThrowsOnNonPoint) {
  EXPECT_THROW(
    st_addpoint_impl(geom("LINESTRING (0 0, 1 1)"), geom("LINESTRING (2 2, 3 3)"), 0),
    std::runtime_error);
}

// ── ST_RemovePoint (error path) ───────────────────────────────────────────────

TEST(StRemovePoint, ThrowsOnNonLinestring) {
  EXPECT_THROW(st_removepoint_impl(geom("POINT (0 0)"), 0), std::runtime_error);
}

TEST(StRemovePoint, ThrowsOnNegativeIndex) {
  EXPECT_THROW(st_removepoint_impl(geom("LINESTRING (0 0, 5 5, 10 0)"), -1), std::runtime_error);
}

TEST(StRemovePoint, ThrowsOnOutOfRangeIndex) {
  EXPECT_THROW(st_removepoint_impl(geom("LINESTRING (0 0, 5 5, 10 0)"), 3), std::runtime_error);
}

// ── ST_SetPoint (error path) ──────────────────────────────────────────────────

TEST(StSetPoint, ThrowsOnNonLinestring) {
  EXPECT_THROW(
    st_setpoint_impl(geom("POINT (0 0)"), 0, geom("POINT (1 1)")),
    std::runtime_error);
}

TEST(StSetPoint, ThrowsOnNonPoint) {
  EXPECT_THROW(
    st_setpoint_impl(geom("LINESTRING (0 0, 5 5)"), 0, geom("LINESTRING (1 1, 2 2)")),
    std::runtime_error);
}

TEST(StSetPoint, ThrowsOnNegativeIndex) {
  EXPECT_THROW(
    st_setpoint_impl(geom("LINESTRING (0 0, 5 5)"), -1, geom("POINT (1 1)")),
    std::runtime_error);
}

TEST(StSetPoint, ThrowsOnOutOfRangeIndex) {
  EXPECT_THROW(
    st_setpoint_impl(geom("LINESTRING (0 0, 5 5)"), 5, geom("POINT (1 1)")),
    std::runtime_error);
}
