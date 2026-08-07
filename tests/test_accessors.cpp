#include <gtest/gtest.h>

#include <cmath>
#include <cstring>
#include <initializer_list>
#include <optional>

#include "helpers.hpp"

using namespace ch;

// ── ST_X / ST_Y ───────────────────────────────────────────────────────────────

TEST(StX, BasicCoordinates) {
  EXPECT_DOUBLE_EQ(st_x_impl(geom("POINT (1 2)")),  1.0);
  EXPECT_DOUBLE_EQ(st_x_impl(geom("POINT (10 20)")), 10.0);
}

TEST(StX, NegativeCoordinates) {
  EXPECT_DOUBLE_EQ(st_x_impl(geom("POINT (-3 7)")), -3.0);
}

TEST(StX, ThrowsOnNonPoint) {
  EXPECT_THROW(st_x_impl(geom("LINESTRING (0 0, 1 1)")), std::runtime_error);
}

TEST(StY, BasicCoordinates) {
  EXPECT_DOUBLE_EQ(st_y_impl(geom("POINT (1 2)")),   2.0);
  EXPECT_DOUBLE_EQ(st_y_impl(geom("POINT (10 20)")), 20.0);
}

TEST(StY, NegativeCoordinates) {
  EXPECT_DOUBLE_EQ(st_y_impl(geom("POINT (-3 -7)")), -7.0);
}

TEST(StY, ThrowsOnNonPoint) {
  EXPECT_THROW(st_y_impl(geom("POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))")), std::runtime_error);
}

// ── ST_X / ST_Y WKB fast path ────────────────────────────────────────────────
//
// st_x_wkb / st_y_wkb skip the GEOS parse.  The bar is not "close enough" but
// the same bits as st_x_impl / st_y_impl for every buffer they claim, and a
// decline for every buffer they do not — so each case below is asserted against
// the GEOS path rather than against a literal.

namespace {

void put_u32(Vector & v, uint32_t x) {
  uint8_t b[4];
  std::memcpy(b, &x, 4);
  v.insert(v.end(), b, b + 4);
}

void put_f64(Vector & v, double x) {
  uint8_t b[8];
  std::memcpy(b, &x, 8);
  v.insert(v.end(), b, b + 8);
}

// Little-endian WKB POINT with an explicit type word, so the test can spell out
// EWKB flag bits and ISO type ranges that write_ewkb would never emit.
Vector point_wkb(uint32_t type, std::optional<uint32_t> srid,
                 std::initializer_list<double> ordinates) {
  Vector v;
  v.push_back(0x01);
  put_u32(v, type);
  if (srid) put_u32(v, *srid);
  for (double d : ordinates) put_f64(v, d);
  return v;
}

// Same bits, not merely the same value: this is what distinguishes a real
// fast path from one that quietly rounds.
::testing::AssertionResult SameBits(double a, double b) {
  if (std::memcmp(&a, &b, sizeof(double)) == 0) return ::testing::AssertionSuccess();
  return ::testing::AssertionFailure() << a << " and " << b << " differ in bits";
}

// Both accessors must agree with GEOS on the same buffer, so every case runs
// through one helper rather than being written twice.
void ExpectFastPathMatchesGeos(const Vector & wkb_bytes) {
  std::optional<double> fx = st_x_wkb(wkb(wkb_bytes));
  std::optional<double> fy = st_y_wkb(wkb(wkb_bytes));
  ASSERT_TRUE(fx.has_value());
  ASSERT_TRUE(fy.has_value());
  EXPECT_TRUE(SameBits(*fx, st_x_impl(read_wkb(wkb(wkb_bytes)))));
  EXPECT_TRUE(SameBits(*fy, st_y_impl(read_wkb(wkb(wkb_bytes)))));
}

// A declined buffer must still behave exactly as it does today, which for every
// non-point and every empty point means the GEOS path throwing.
void ExpectDeclinedAndGeosThrows(const Vector & wkb_bytes) {
  EXPECT_FALSE(st_x_wkb(wkb(wkb_bytes)).has_value());
  EXPECT_FALSE(st_y_wkb(wkb(wkb_bytes)).has_value());
  EXPECT_ANY_THROW(st_x_impl(read_wkb(wkb(wkb_bytes))));
  EXPECT_ANY_THROW(st_y_impl(read_wkb(wkb(wkb_bytes))));
}

} // namespace

TEST(StXYWkb, Point2D) {
  ExpectFastPathMatchesGeos(wkt2wkb("POINT (1 2)"));
  ExpectFastPathMatchesGeos(point_wkb(1, std::nullopt, {3.0, 4.0}));
}

TEST(StXYWkb, NegativeAndFractional) {
  ExpectFastPathMatchesGeos(wkt2wkb("POINT (-3.5 7.25)"));
  ExpectFastPathMatchesGeos(point_wkb(1, std::nullopt, {-0.1, -1e-300}));
  ExpectFastPathMatchesGeos(point_wkb(1, std::nullopt, {1e308, -1e308}));
}

TEST(StXYWkb, BigEndianPoint) {
  // The byte-order flag is per geometry, so the fast path must honour it rather
  // than assume the host order.
  Vector be;
  be.push_back(0x00);                       // big-endian flag
  for (uint8_t b : {0u, 0u, 0u, 1u}) be.push_back(b);  // type 1 (Point), BE
  for (double d : {1.0, 2.0}) {
    uint8_t le[8];
    std::memcpy(le, &d, 8);
    for (int i = 7; i >= 0; --i) be.push_back(le[i]);
  }
  ExpectFastPathMatchesGeos(be);
}

TEST(StXYWkb, EwkbWithSrid) {
  ExpectFastPathMatchesGeos(point_wkb(0x20000001u, 4326u, {12.5, -7.5}));
}

TEST(StXYWkb, PointZ) {
  ExpectFastPathMatchesGeos(point_wkb(1001u, std::nullopt, {1.0, 2.0, 3.0}));
  ExpectFastPathMatchesGeos(point_wkb(0x80000001u, std::nullopt, {1.0, 2.0, 3.0}));
  ExpectFastPathMatchesGeos(point_wkb(0xA0000001u, 4326u, {1.0, 2.0, 3.0}));
}

TEST(StXYWkb, PointM) {
  ExpectFastPathMatchesGeos(point_wkb(2001u, std::nullopt, {1.0, 2.0, 9.0}));
  ExpectFastPathMatchesGeos(point_wkb(0x40000001u, std::nullopt, {1.0, 2.0, 9.0}));
}

TEST(StXYWkb, PointZM) {
  ExpectFastPathMatchesGeos(point_wkb(3001u, std::nullopt, {1.0, 2.0, 3.0, 9.0}));
  ExpectFastPathMatchesGeos(point_wkb(0xC0000001u, std::nullopt, {1.0, 2.0, 3.0, 9.0}));
}

TEST(StXYWkb, EmptyPointIsDeclined) {
  // GEOS encodes POINT EMPTY as POINT(NaN NaN) and Point::getX() throws on it,
  // so returning the NaN would be a behaviour change, not an optimization.
  ExpectDeclinedAndGeosThrows(wkt2wkb("POINT EMPTY"));
  ExpectDeclinedAndGeosThrows(point_wkb(1, std::nullopt,
      {std::numeric_limits<double>::quiet_NaN(),
       std::numeric_limits<double>::quiet_NaN()}));
}

TEST(StXYWkb, SingleNaNOrdinateIsClaimed) {
  // WKBReader only calls it empty when both ordinates are NaN; with one finite
  // ordinate the point is real and getX()/getY() return the NaN.
  double nan = std::numeric_limits<double>::quiet_NaN();
  ExpectFastPathMatchesGeos(point_wkb(1, std::nullopt, {nan, 2.0}));
  ExpectFastPathMatchesGeos(point_wkb(1, std::nullopt, {1.0, nan}));
}

TEST(StXYWkb, NonPointsAreDeclined) {
  ExpectDeclinedAndGeosThrows(wkt2wkb("LINESTRING (0 0, 1 1)"));
  ExpectDeclinedAndGeosThrows(wkt2wkb("POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))"));
  ExpectDeclinedAndGeosThrows(wkt2wkb("MULTIPOINT ((1 2))"));
  ExpectDeclinedAndGeosThrows(wkt2wkb("GEOMETRYCOLLECTION EMPTY"));
  ExpectDeclinedAndGeosThrows(wkt2wkb("LINESTRING EMPTY"));
}

TEST(StXYWkb, TruncatedBufferIsDeclined) {
  // The fast path must not report the error itself — read_wkb's message is what
  // the user sees today, and the wrapper only reaches it by falling back.
  Vector short_point = point_wkb(1, std::nullopt, {1.0, 2.0});
  short_point.resize(short_point.size() - 4);
  EXPECT_FALSE(st_x_wkb(wkb(short_point)).has_value());
  EXPECT_FALSE(st_y_wkb(wkb(short_point)).has_value());

  EXPECT_FALSE(st_x_wkb(wkb(Vector{})).has_value());
  EXPECT_FALSE(st_y_wkb(wkb(Vector{})).has_value());
}

// ── ST_Centroid ───────────────────────────────────────────────────────────────

TEST(StCentroid, Square) {
  EXPECT_EQ(geom2wkt(st_centroid_impl(geom("POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))"))), "POINT (5 5)");
}

TEST(StCentroid, Multipoint) {
  // (0,0) (2,0) (2,2) (0,2) → centroid (1,1)
  EXPECT_EQ(geom2wkt(st_centroid_impl(geom("MULTIPOINT ((0 0), (2 0), (2 2), (0 2))"))), "POINT (1 1)");
}

TEST(StCentroid, Line) {
  EXPECT_EQ(geom2wkt(st_centroid_impl(geom("LINESTRING (0 0, 2 0)"))), "POINT (1 0)");
}

// ── ST_Area ───────────────────────────────────────────────────────────────────

TEST(StArea, UnitSquare) {
  EXPECT_DOUBLE_EQ(st_area_impl(geom("POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))")), 1.0);
}

TEST(StArea, TenByTen) {
  EXPECT_DOUBLE_EQ(st_area_impl(geom("POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))")), 100.0);
}

TEST(StArea, LineStringIsZero) {
  EXPECT_DOUBLE_EQ(st_area_impl(geom("LINESTRING (0 0, 1 1)")), 0.0);
}

// ── ST_NPoints ────────────────────────────────────────────────────────────────

TEST(StNPoints, Linestring3) {
  EXPECT_EQ(st_npoints_impl(geom("LINESTRING (0 0, 1 1, 2 2)")), 3);
}

TEST(StNPoints, PolygonRingClosed) {
  // closed ring — GEOS counts the closing vertex
  EXPECT_EQ(st_npoints_impl(geom("POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))")), 5);
}

// ── ST_SRID ───────────────────────────────────────────────────────────────────

TEST(StSrid, DefaultIsZero) {
  // WKB (not EWKB) carries no SRID — defaults to 0
  EXPECT_EQ(st_srid_impl(geom("POINT (0 0)")), 0);
}

// ── ST_StartPoint / ST_EndPoint ───────────────────────────────────────────────

TEST(StStartPoint, Basic) {
  EXPECT_EQ(geom2wkt(st_startpoint_impl(geom("LINESTRING (0 0, 5 5, 10 0)"))), "POINT (0 0)");
}

TEST(StEndPoint, Basic) {
  EXPECT_EQ(geom2wkt(st_endpoint_impl(geom("LINESTRING (0 0, 5 5, 10 0)"))), "POINT (10 0)");
}

TEST(StStartPoint, ThrowsOnNonLine) {
  EXPECT_THROW(st_startpoint_impl(geom("POINT (0 0)")), std::runtime_error);
}

TEST(StEndPoint, ThrowsOnNonLine) {
  EXPECT_THROW(st_endpoint_impl(geom("POINT (0 0)")), std::runtime_error);
}

// ── ST_Length ─────────────────────────────────────────────────────────────────

TEST(StLength, HorizontalLine) {
  EXPECT_DOUBLE_EQ(st_length_impl(geom("LINESTRING (0 0, 5 0)")), 5.0);
}

TEST(StLength, PolygonPerimeterAsLength) {
  // GEOS getLength() on a polygon returns perimeter
  EXPECT_DOUBLE_EQ(st_length_impl(geom("POLYGON ((0 0, 4 0, 4 3, 0 3, 0 0))")), 14.0);
}

// ── ST_IsValid ────────────────────────────────────────────────────────────────

TEST(StIsValid, ValidPolygon) {
  EXPECT_TRUE(st_isvalid_impl(geom("POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))")));
}

TEST(StIsValid, SelfIntersectingPolygon) {
  EXPECT_FALSE(st_isvalid_impl(geom("POLYGON ((0 0, 10 10, 10 0, 0 10, 0 0))")));
}

// ── ST_IsEmpty ────────────────────────────────────────────────────────────────

TEST(StIsEmpty, NonEmptyPoint) {
  EXPECT_FALSE(st_isempty_impl(geom("POINT (0 0)")));
}

TEST(StIsEmpty, EmptyPolygon) {
  EXPECT_TRUE(st_isempty_impl(geom("POLYGON EMPTY")));
}

// ── ST_IsSimple ───────────────────────────────────────────────────────────────

TEST(StIsSimple, SimpleLine) {
  EXPECT_TRUE(st_issimple_impl(geom("LINESTRING (0 0, 5 5, 10 0)")));
}

TEST(StIsSimple, SelfIntersectingLine) {
  EXPECT_FALSE(st_issimple_impl(geom("LINESTRING (0 0, 10 10, 10 0, 0 10)")));
}

// ── ST_IsRing ─────────────────────────────────────────────────────────────────

TEST(StIsRing, ClosedLine) {
  EXPECT_TRUE(st_isring_impl(geom("LINESTRING (0 0, 1 0, 1 1, 0 0)")));
}

TEST(StIsRing, OpenLine) {
  EXPECT_FALSE(st_isring_impl(geom("LINESTRING (0 0, 5 5)")));
}

TEST(StIsRing, PolygonIsNotRing) {
  // st_isring only works on LineStrings
  EXPECT_FALSE(st_isring_impl(geom("POLYGON ((0 0, 1 0, 1 1, 0 0))")));
}

// ── ST_GeometryType ───────────────────────────────────────────────────────────

TEST(StGeometryType, Point) {
  EXPECT_EQ(st_geometrytype_impl(geom("POINT (0 0)")), "ST_Point");
}

TEST(StGeometryType, Polygon) {
  EXPECT_EQ(st_geometrytype_impl(geom("POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))")), "ST_Polygon");
}

TEST(StGeometryType, LineString) {
  EXPECT_EQ(st_geometrytype_impl(geom("LINESTRING (0 0, 1 1)")), "ST_LineString");
}

// ── ST_NumGeometries ──────────────────────────────────────────────────────────

TEST(StNumGeometries, SinglePoint) {
  EXPECT_EQ(st_numgeometries_impl(geom("POINT (0 0)")), 1);
}

TEST(StNumGeometries, MultiPoint) {
  EXPECT_EQ(st_numgeometries_impl(geom("MULTIPOINT ((0 0), (1 1), (2 2))")), 3);
}

// ── ST_Dimension ──────────────────────────────────────────────────────────────

TEST(StDimension, Point) {
  EXPECT_EQ(st_dimension_impl(geom("POINT (0 0)")), 0);
}

TEST(StDimension, LineString) {
  EXPECT_EQ(st_dimension_impl(geom("LINESTRING (0 0, 1 1)")), 1);
}

TEST(StDimension, Polygon) {
  EXPECT_EQ(st_dimension_impl(geom("POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))")), 2);
}

// ── ST_InteriorPoint ──────────────────────────────────────────────────────────

TEST(StInteriorPoint, Polygon) {
  auto pt = st_interiorpoint_impl(geom("POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))"));
  // interior point must lie within the polygon
  EXPECT_TRUE(pt->within(geom("POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))").get()));
}

// ── ST_IsValidReason ──────────────────────────────────────────────────────────

TEST(StIsValidReason, ValidGeometry) {
  EXPECT_EQ(st_isvalidreason_impl(geom("POINT (0 0)")), "Valid Geometry");
}

TEST(StIsValidReason, SelfIntersecting) {
  auto reason = st_isvalidreason_impl(geom("POLYGON ((0 0, 10 10, 10 0, 0 10, 0 0))"));
  EXPECT_NE(reason, "Valid Geometry");
  EXPECT_FALSE(reason.empty());
}

// ── ST_NumInteriorRings ───────────────────────────────────────────────────────

TEST(StNumInteriorRings, NoHole) {
  EXPECT_EQ(st_numinteriorrings_impl(geom("POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))")), 0);
}

TEST(StNumInteriorRings, OneHole) {
  EXPECT_EQ(st_numinteriorrings_impl(
    geom("POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0), (3 3, 7 3, 7 7, 3 7, 3 3))")), 1);
}

TEST(StNumInteriorRings, ThrowsOnNonPolygon) {
  EXPECT_THROW(st_numinteriorrings_impl(geom("LINESTRING (0 0, 1 1)")), std::runtime_error);
}

// ── ST_HausdorffDistance ──────────────────────────────────────────────────────

TEST(StHausdorffDistance, ParallelLines) {
  // Two horizontal lines separated by 1 unit — hausdorff distance = 1
  EXPECT_DOUBLE_EQ(
    st_hausdorffdistance_impl(geom("LINESTRING (0 0, 10 0)"), geom("LINESTRING (0 1, 10 1)")),
    1.0);
}

TEST(StHausdorffDistance, SameLine) {
  EXPECT_DOUBLE_EQ(
    st_hausdorffdistance_impl(geom("LINESTRING (0 0, 5 0)"), geom("LINESTRING (0 0, 5 0)")),
    0.0);
}

// ── ST_HausdorffDistance (densify) ────────────────────────────────────────────

TEST(StHausdorffDistanceDensify, ParallelLines) {
  double d = st_hausdorffdistance_densify_impl(
    geom("LINESTRING (0 0, 10 0)"), geom("LINESTRING (0 1, 10 1)"), 0.5);
  EXPECT_NEAR(d, 1.0, 0.01);
}

// ── ST_FrechetDistance ────────────────────────────────────────────────────────

TEST(StFrechetDistance, SameLines) {
  EXPECT_NEAR(
    st_frechetdistance_impl(geom("LINESTRING (0 0, 5 0)"), geom("LINESTRING (0 0, 5 0)")),
    0.0, 1e-10);
}

TEST(StFrechetDistance, OffsetLines) {
  double d = st_frechetdistance_impl(
    geom("LINESTRING (0 0, 10 0)"), geom("LINESTRING (0 1, 10 1)"));
  EXPECT_GT(d, 0.0);
}

// ── ST_FrechetDistance (densify) ─────────────────────────────────────────────

TEST(StFrechetDistanceDensify, SameLines) {
  EXPECT_NEAR(
    st_frechetdistance_densify_impl(geom("LINESTRING (0 0, 5 0)"), geom("LINESTRING (0 0, 5 0)"), 0.5),
    0.0, 1e-10);
}

// ── ST_Z ──────────────────────────────────────────────────────────────────────

TEST(StZ, Point3D) {
  EXPECT_DOUBLE_EQ(st_z_impl(geom("POINT Z (1 2 3)")), 3.0);
}

TEST(StZ, ThrowsOnNonPoint) {
  EXPECT_THROW(st_z_impl(geom("LINESTRING (0 0, 1 1)")), std::runtime_error);
}

TEST(StZ, ThrowsOnEmptyPoint) {
  EXPECT_THROW(st_z_impl(geom("POINT EMPTY")), std::runtime_error);
}

// ── ST_NRings ─────────────────────────────────────────────────────────────────

TEST(StNRings, PolygonNoHole) {
  EXPECT_EQ(st_nrings_impl(geom("POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))")), 1);
}

TEST(StNRings, PolygonWithHole) {
  EXPECT_EQ(st_nrings_impl(
    geom("POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0), (3 3, 7 3, 7 7, 3 7, 3 3))")), 2);
}

TEST(StNRings, ThrowsOnNonPolygon) {
  EXPECT_THROW(st_nrings_impl(geom("LINESTRING (0 0, 1 1)")), std::runtime_error);
}

// ── ST_Perimeter ──────────────────────────────────────────────────────────────

TEST(StPerimeter, Rectangle) {
  EXPECT_DOUBLE_EQ(st_perimeter_impl(geom("POLYGON ((0 0, 4 0, 4 3, 0 3, 0 0))")), 14.0);
}

// ── ST_NumPoints ──────────────────────────────────────────────────────────────

TEST(StNumPoints, ThreePointLine) {
  EXPECT_EQ(st_numpoints_impl(geom("LINESTRING (0 0, 5 5, 10 0)")), 3);
}

TEST(StNumPoints, ThrowsOnNonLine) {
  EXPECT_THROW(st_numpoints_impl(geom("POINT (0 0)")), std::runtime_error);
}
