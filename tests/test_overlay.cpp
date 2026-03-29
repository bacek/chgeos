#include <gtest/gtest.h>
#include "helpers.hpp"
#include "functions/processing.hpp"
#include "clickhouse.hpp"

using namespace ch;

// ── ST_UnionAgg ───────────────────────────────────────────────────────────────

static std::vector<std::unique_ptr<ch::Geometry>> geom_vec(std::initializer_list<std::string> wkts) {
  std::vector<std::unique_ptr<ch::Geometry>> v;
  for (auto& s : wkts) v.push_back(geom(s));
  return v;
}

TEST(StUnionAgg, DisjointPolygons) {
  // Two non-overlapping unit squares → union area = 2
  EXPECT_NEAR(st_area_impl(st_union_agg_impl(geom_vec({
    "POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))",
    "POLYGON ((5 5, 6 5, 6 6, 5 6, 5 5))",
  }))), 2.0, 1e-10);
}

TEST(StUnionAgg, OverlappingPolygons) {
  // Two 2x2 squares sharing a 1x1 corner → union area = 4+4-1 = 7
  EXPECT_NEAR(st_area_impl(st_union_agg_impl(geom_vec({
    "POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0))",
    "POLYGON ((1 1, 3 1, 3 3, 1 3, 1 1))",
  }))), 7.0, 1e-10);
}

TEST(StUnionAgg, ThreePolygons) {
  // Two overlapping 4x4 squares + one disjoint 2x2 → area = 28 + 4 = 32
  EXPECT_NEAR(st_area_impl(st_union_agg_impl(geom_vec({
    "POLYGON ((0 0, 4 0, 4 4, 0 4, 0 0))",
    "POLYGON ((2 2, 6 2, 6 6, 2 6, 2 2))",
    "POLYGON ((10 10, 12 10, 12 12, 10 12, 10 10))",
  }))), 32.0, 1e-10);
}

TEST(StUnionAgg, RealWorldDisjointPolygons) {
  // Two real-world disjoint polygons — union must be a 2-part multipolygon
  // whose area equals the sum of the individual areas.
  static const char *p1 =
    "POLYGON ((150.971113403 -32.374786143, 150.97111442 -32.374792924,"
    " 150.97111408 -32.37479391, 150.971112313 -32.374796858,"
    " 150.971104169 -32.374809353, 150.971103043 -32.374810454,"
    " 150.971097106 -32.374821054, 150.971104617 -32.374839666,"
    " 150.971104247 -32.374840413, 150.971088656 -32.374852033,"
    " 150.971087796 -32.374852471, 150.97107618 -32.374856662,"
    " 150.971074871 -32.37485657, 150.971072474 -32.374856469,"
    " 150.971061467 -32.374855917, 150.971059169 -32.374856343,"
    " 150.971053506 -32.374855535, 150.971042525 -32.374855778,"
    " 150.971037291 -32.374857069, 150.971036368 -32.374857382,"
    " 150.971028756 -32.374857382, 150.971027851 -32.374857458,"
    " 150.971014498 -32.37486049, 150.971001124 -32.374859733,"
    " 150.970994357 -32.374858181, 150.970974509 -32.374854693,"
    " 150.97096937 -32.374856381, 150.970967716 -32.37485667,"
    " 150.970966776 -32.374855934, 150.970947311 -32.374851923,"
    " 150.970941506 -32.374850115, 150.970927651 -32.374844729,"
    " 150.970921625 -32.374843228, 150.97091801 -32.374840998,"
    " 150.97091507 -32.374839417, 150.970910566 -32.374836224,"
    " 150.97090899 -32.374833899, 150.970907799 -32.374828562,"
    " 150.970899418 -32.374819315, 150.970884506 -32.374806543,"
    " 150.970884491 -32.374804527, 150.97088439 -32.374803022,"
    " 150.970891876 -32.374795874, 150.970899562 -32.374793844,"
    " 150.97090273 -32.374792402, 150.970909213 -32.374792416,"
    " 150.970914315 -32.374791867, 150.970919917 -32.374796917,"
    " 150.970935263 -32.374810873, 150.9709379 -32.374811727,"
    " 150.970938734 -32.374812462, 150.970943367 -32.374816776,"
    " 150.970945406 -32.374818266, 150.970946545 -32.374819632,"
    " 150.97097233 -32.374826171, 150.970988693 -32.374821564,"
    " 150.97099621 -32.374812846, 150.971000543 -32.37479542,"
    " 150.971002311 -32.374792391, 150.971004821 -32.374784215,"
    " 150.971013393 -32.374781557, 150.971017857 -32.374781539,"
    " 150.971020598 -32.374782079, 150.971035305 -32.374780737,"
    " 150.97103986 -32.374778608, 150.971041545 -32.374778914,"
    " 150.971042913 -32.374787444, 150.971042796 -32.374787938,"
    " 150.971041982 -32.374791036, 150.971077215 -32.374807571,"
    " 150.971084304 -32.374799474, 150.971100279 -32.374781332,"
    " 150.971100274 -32.374779168, 150.971101669 -32.374778568,"
    " 150.97110807 -32.374779609, 150.971074966 -32.374809936,"
    " 150.971041631 -32.374820522, 150.971113403 -32.374786143))";
  static const char *p2 =
    "POLYGON ((150.974629148 -32.374294171, 150.974633535 -32.374302575,"
    " 150.974634039 -32.374304071, 150.974633463 -32.374310826,"
    " 150.974633313 -32.374312767, 150.974631296 -32.374312685,"
    " 150.974630446 -32.374312671, 150.974623909 -32.374309031,"
    " 150.974623633 -32.374307578, 150.974622399 -32.37430181,"
    " 150.974625633 -32.374294973, 150.974629148 -32.374294171))";

  // p1 is topologically invalid — fix both inputs before unioning.
  auto valid1 = st_makevalid_impl(geom(p1));
  auto valid2 = st_makevalid_impl(geom(p2));
  double area1 = st_area_impl(std::move(st_makevalid_impl(geom(p1))));
  double area2 = st_area_impl(std::move(st_makevalid_impl(geom(p2))));

  std::vector<std::unique_ptr<ch::Geometry>> inputs;
  inputs.push_back(std::move(valid1));
  inputs.push_back(std::move(valid2));

  // The two geometries are geographically disjoint — union area = sum of areas.
  auto result = st_union_agg_impl(std::move(inputs));
  EXPECT_NEAR(st_area_impl(std::move(result)), area1 + area2, 1e-14);
}

// Bow-tie (figure-8) polygon: ring self-intersects at its midpoint.
// GEOS throws TopologyException during Union() on such invalid input.
// impl_wrapper catches std::exception and calls ch::panic(e.what()), which
// calls clickhouse_throw. In native tests clickhouse_throw re-throws as
// std::runtime_error, propagating the error message to the caller.
TEST(StUnionAgg, SelfIntersectingPolygonsPropagateViaPanic) {
  // Mirror what impl_wrapper does so the full panic path is exercised.
  auto do_union = [] {
    try {
      st_union_agg_impl(geom_vec({
        "POLYGON ((0 0, 2 2, 2 0, 0 2, 0 0))",  // bow-tie: self-intersects at (1,1)
        "POLYGON ((3 0, 5 2, 5 0, 3 2, 3 0))",  // bow-tie: self-intersects at (4,1)
      }));
    } catch (const std::exception & e) {
      ch::panic(e.what());
    }
  };

  EXPECT_THROW(do_union(), WasmPanicException);
}

TEST(StUnionAgg, SingleGeometry) {
  EXPECT_NEAR(st_area_impl(st_union_agg_impl(geom_vec({
    "POLYGON ((0 0, 3 0, 3 3, 0 3, 0 0))",
  }))), 9.0, 1e-10);
}

// ── ST_Difference ─────────────────────────────────────────────────────────────

TEST(StDifference, PolygonMinus) {
  // 4x4 minus right 2x4 half → 2x4 left half, area = 8
  auto result = st_difference_impl(
    geom("POLYGON ((0 0, 4 0, 4 4, 0 4, 0 0))"),
    geom("POLYGON ((2 0, 4 0, 4 4, 2 4, 2 0))"));
  EXPECT_NEAR(st_area_impl(std::move(result)), 8.0, 1e-10);
}

TEST(StDifference, NonOverlapping) {
  // Non-overlapping: A - B = A
  auto result = st_difference_impl(
    geom("POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))"),
    geom("POLYGON ((5 5, 6 5, 6 6, 5 6, 5 5))"));
  EXPECT_NEAR(st_area_impl(std::move(result)), 1.0, 1e-10);
}

// ── ST_Intersection ───────────────────────────────────────────────────────────

TEST(StIntersection, CrossingLines) {
  EXPECT_EQ(geom2wkt(st_intersection_impl(
    geom("LINESTRING (0 0, 10 10)"),
    geom("LINESTRING (0 10, 10 0)"))), "POINT (5 5)");
}

TEST(StIntersection, OverlappingPolygons) {
  // Two 2x2 squares overlapping in a 1x1 area
  auto result = st_intersection_impl(
    geom("POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0))"),
    geom("POLYGON ((1 1, 3 1, 3 3, 1 3, 1 1))"));
  EXPECT_NEAR(st_area_impl(std::move(result)), 1.0, 1e-10);
}

// ── ST_Union ──────────────────────────────────────────────────────────────────

TEST(StUnion, TwoPoints) {
  auto result = st_union_impl(geom("POINT (0 0)"), geom("POINT (1 1)"));
  EXPECT_EQ(result->getNumGeometries(), 2u);
}

TEST(StUnion, OverlappingSquares) {
  // Two 4x4 squares sharing a 2x2 corner → union area = 16+16-4 = 28
  auto result = st_union_impl(
    geom("POLYGON ((0 0, 4 0, 4 4, 0 4, 0 0))"),
    geom("POLYGON ((2 2, 6 2, 6 6, 2 6, 2 2))"));
  EXPECT_NEAR(st_area_impl(std::move(result)), 28.0, 1e-10);
}

// ── ST_UnaryUnion ─────────────────────────────────────────────────────────────

TEST(StUnaryUnion, MultiPolygonMerges) {
  // Two overlapping 4x4 squares as multipolygon → merged, area < 32
  auto result = st_unaryunion_impl(geom(
    "MULTIPOLYGON (((0 0, 4 0, 4 4, 0 4, 0 0)), ((2 2, 6 2, 6 6, 2 6, 2 2)))"));
  EXPECT_NEAR(st_area_impl(std::move(result)), 28.0, 1e-10);
}

// ── ST_ClusterIntersecting ────────────────────────────────────────────────────

TEST(StClusterIntersecting, DisjointPoints) {
  // Two far-apart points → two clusters
  auto result = st_clusterintersecting_impl(
    geom("GEOMETRYCOLLECTION (POINT (0 0), POINT (100 100))"));
  EXPECT_EQ(result->getNumGeometries(), 2u);
}

TEST(StClusterIntersecting, OverlappingPolygons) {
  // Two overlapping polygons → one cluster
  auto result = st_clusterintersecting_impl(geom(
    "GEOMETRYCOLLECTION ("
    "POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0)),"
    "POLYGON ((1 1, 3 1, 3 3, 1 3, 1 1)))"));
  EXPECT_EQ(result->getNumGeometries(), 1u);
}

TEST(StClusterIntersecting, TransitiveChain) {
  // A-B touch, B-C touch, but A-C don't → all three in one cluster
  auto result = st_clusterintersecting_impl(geom(
    "GEOMETRYCOLLECTION ("
    "POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0)),"
    "POLYGON ((1 1, 3 1, 3 3, 1 3, 1 1)),"
    "POLYGON ((2 2, 4 2, 4 4, 2 4, 2 2)))"));
  EXPECT_EQ(result->getNumGeometries(), 1u);
}

// ── ST_CollectAgg ─────────────────────────────────────────────────────────────

TEST(StCollectAgg, TwoPoints) {
  auto result = st_collect_agg_impl(geom_vec({"POINT (0 0)", "POINT (1 1)"}));
  EXPECT_EQ(result->getNumGeometries(), 2u);
}

TEST(StCollectAgg, DoesNotDissolve) {
  // Overlapping polygons must NOT be dissolved — collect ≠ union
  auto result = st_collect_agg_impl(geom_vec({
    "POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0))",
    "POLYGON ((1 1, 3 1, 3 3, 1 3, 1 1))",
  }));
  EXPECT_EQ(result->getNumGeometries(), 2u);
  // Raw sum of areas exceeds the actual merged area, confirming no dissolve
  EXPECT_NEAR(st_area_impl(std::move(result)), 4.0 + 4.0, 1e-10);
}

TEST(StCollectAgg, SingleGeometry) {
  auto result = st_collect_agg_impl(geom_vec({"POINT (3 7)"}));
  EXPECT_EQ(result->getNumGeometries(), 1u);
}

TEST(StCollectAgg, MixedTypes) {
  auto result = st_collect_agg_impl(geom_vec({
    "POINT (0 0)",
    "LINESTRING (0 0, 1 1)",
    "POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))",
  }));
  EXPECT_EQ(result->getNumGeometries(), 3u);
}

// ── ST_ExtentAgg ──────────────────────────────────────────────────────────────

TEST(StExtentAgg, TwoPoints) {
  // Points at (0,0) and (3,4) → envelope is a 3×4 rectangle, area = 12
  auto result = st_extent_agg_impl(geom_vec({"POINT (0 0)", "POINT (3 4)"}));
  EXPECT_NEAR(st_area_impl(std::move(result)), 12.0, 1e-10);
}

TEST(StExtentAgg, TwoPolygons) {
  // First polygon covers [0,2]×[0,2]; second [3,5]×[0,2]
  // Bounding envelope spans [0,5]×[0,2], area = 10
  auto result = st_extent_agg_impl(geom_vec({
    "POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0))",
    "POLYGON ((3 0, 5 0, 5 2, 3 2, 3 0))",
  }));
  EXPECT_NEAR(st_area_impl(std::move(result)), 10.0, 1e-10);
}

TEST(StExtentAgg, SingleGeometry) {
  // Envelope of a 2×3 rectangle is itself
  auto result = st_extent_agg_impl(geom_vec({
    "POLYGON ((0 0, 2 0, 2 3, 0 3, 0 0))",
  }));
  EXPECT_NEAR(st_area_impl(std::move(result)), 6.0, 1e-10);
}

TEST(StExtentAgg, EnvelopeCoversAllInputs) {
  // Every input geometry must be covered by the resulting envelope
  // (covers includes boundary; contains would exclude it)
  auto env = st_extent_agg_impl(geom_vec({
    "POINT (1 1)", "POINT (5 5)", "POINT (3 8)",
  }));
  EXPECT_TRUE(env->covers(geom("POINT (1 1)").get()));
  EXPECT_TRUE(env->covers(geom("POINT (5 5)").get()));
  EXPECT_TRUE(env->covers(geom("POINT (3 8)").get()));
}

// ── ST_MakeLineAgg ────────────────────────────────────────────────────────────

TEST(StMakeLineAgg, ThreePoints) {
  auto result = st_makeline_agg_impl(geom_vec({
    "POINT (0 0)", "POINT (1 0)", "POINT (1 1)",
  }));
  EXPECT_EQ(result->getNumPoints(), 3u);
  EXPECT_NEAR(st_length_impl(std::move(result)), 2.0, 1e-10);
}

TEST(StMakeLineAgg, TwoPoints) {
  auto result = st_makeline_agg_impl(geom_vec({
    "POINT (0 0)", "POINT (3 4)",
  }));
  EXPECT_EQ(result->getNumPoints(), 2u);
  EXPECT_NEAR(st_length_impl(std::move(result)), 5.0, 1e-10);
}

TEST(StMakeLineAgg, FromLinestrings) {
  // Two linestrings: each has 2 points → result has 4 points
  auto result = st_makeline_agg_impl(geom_vec({
    "LINESTRING (0 0, 1 0)",
    "LINESTRING (1 0, 2 0)",
  }));
  EXPECT_EQ(result->getNumPoints(), 4u);
}

// ── ST_ConvexHullAgg ──────────────────────────────────────────────────────────

TEST(StConvexHullAgg, TrianglePoints) {
  // Three corners of a right triangle → convex hull is that triangle, area = 0.5
  auto result = st_convexhull_agg_impl(geom_vec({
    "POINT (0 0)", "POINT (1 0)", "POINT (0 1)",
  }));
  EXPECT_NEAR(st_area_impl(std::move(result)), 0.5, 1e-10);
}

TEST(StConvexHullAgg, InteriorPointIgnored) {
  // Square corners + one interior point → hull is the square, area = 1
  auto result = st_convexhull_agg_impl(geom_vec({
    "POINT (0 0)", "POINT (1 0)", "POINT (1 1)", "POINT (0 1)",
    "POINT (0.5 0.5)",  // interior — must not affect the hull
  }));
  EXPECT_NEAR(st_area_impl(std::move(result)), 1.0, 1e-10);
}

TEST(StConvexHullAgg, TwoPolygons) {
  // Two 1×1 unit squares separated by a gap → hull is a 3×1 rectangle, area = 3
  auto result = st_convexhull_agg_impl(geom_vec({
    "POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))",
    "POLYGON ((2 0, 3 0, 3 1, 2 1, 2 0))",
  }));
  EXPECT_NEAR(st_area_impl(std::move(result)), 3.0, 1e-10);
}
