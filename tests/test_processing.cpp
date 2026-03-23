#include <gtest/gtest.h>
#include <cmath>
#include "helpers.hpp"

using namespace ch;

// ── ST_Buffer ─────────────────────────────────────────────────────────────────

TEST(StBuffer, PointAreaApproxPi) {
  // Default quad_segs=8 → 32-gon approximation of unit circle
  EXPECT_NEAR(st_area_impl(st_buffer_impl(geom("POINT (0 0)"), 1.0)), M_PI, 0.05);
}

TEST(StBuffer, PositiveRadiusIncreasesArea) {
  EXPECT_GT(st_area_impl(st_buffer_impl(geom("POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))"), 1.0)), 1.0);
}

TEST(StBuffer, ZeroRadiusPreservesArea) {
  EXPECT_NEAR(st_area_impl(st_buffer_impl(geom("POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))"), 0.0)), 1.0, 1e-10);
}

// ── ST_Buffer (with params) ───────────────────────────────────────────────────

TEST(StBufferParams, FlatEndcapFewerPoints) {
  auto round_npts = st_npoints_impl(st_buffer_impl(geom("LINESTRING (0 0, 10 0)"), 1.0));
  auto flat_npts  = st_npoints_impl(st_buffer_params_impl(geom("LINESTRING (0 0, 10 0)"), 1.0, params("endcap=flat")));
  // Flat cap has fewer vertices than round cap
  EXPECT_LT(flat_npts, round_npts);
}

TEST(StBufferParams, QuadSegsControlsResolution) {
  auto coarse = st_buffer_params_impl(geom("POINT (0 0)"), 1.0, params("quad_segs=2"));
  auto fine   = st_buffer_params_impl(geom("POINT (0 0)"), 1.0, params("quad_segs=16"));
  int32_t coarse_npts = coarse->getNumPoints();
  int32_t fine_npts   = fine->getNumPoints();
  double  coarse_area = coarse->getArea();
  double  fine_area   = fine->getArea();
  EXPECT_LT(coarse_npts, fine_npts);
  // Finer approximation is closer to π
  EXPECT_GT(fine_area, coarse_area);
}

// ── ST_Simplify ───────────────────────────────────────────────────────────────

TEST(StSimplify, CollinearPointsRemoved) {
  // All mid-points are collinear — high tolerance removes them, leaving 2 endpoints
  int32_t npts = st_npoints_impl(st_simplify_impl(
    geom("LINESTRING (0 0, 1 0, 2 0, 3 0, 10 0)"), 1.0));
  EXPECT_LE(npts, 4);
  EXPECT_GE(npts, 2);
}

TEST(StSimplify, ZeroTolerancePreservesNonCollinear) {
  // Zigzag: no 3 consecutive points are collinear, so none can be removed
  const std::string wkt = "LINESTRING (0 0, 1 1, 2 0, 3 1, 4 0)";
  int32_t npts_before = st_npoints_impl(geom(wkt));
  EXPECT_EQ(st_npoints_impl(st_simplify_impl(geom(wkt), 0.0)), npts_before);
}

// ── ST_MakeValid ──────────────────────────────────────────────────────────────

TEST(StMakeValid, BowTieBecomesValid) {
  // Self-intersecting bowtie polygon
  auto result = st_makevalid_impl(geom("POLYGON ((0 0, 10 10, 10 0, 0 10, 0 0))"));
  EXPECT_TRUE(result->isValid());
}

TEST(StMakeValid, ValidInputUnchanged) {
  auto result = st_makevalid_impl(geom("POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))"));
  EXPECT_TRUE(result->isValid());
  EXPECT_NEAR(st_area_impl(st_makevalid_impl(geom("POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))"))), 1.0, 1e-10);
}

// ── ST_Segmentize ─────────────────────────────────────────────────────────────

TEST(StSegmentize, AddsPoints) {
  const std::string wkt = "LINESTRING (0 0, 10 0)";
  int32_t npts_before = st_npoints_impl(geom(wkt));
  EXPECT_GT(st_npoints_impl(st_segmentize_impl(geom(wkt), 3.0)), npts_before);
}

TEST(StSegmentize, MaxSegmentRespected) {
  // Segment length 3 on a 10-unit line → at least ceil(10/3)+1 = 5 points
  EXPECT_GE(st_npoints_impl(st_segmentize_impl(geom("LINESTRING (0 0, 10 0)"), 3.0)), 5);
}

// ── ST_Subdivide ──────────────────────────────────────────────────────────────

TEST(StSubdivide, SplitsLargePolygon) {
  // 21-point polygon (> 5 point minimum clamp) with max_vertices=8 → must split
  auto result = st_subdivide_impl(
    geom("POLYGON ((0 0, 2 0, 4 0, 6 0, 8 0, 10 0,"
         " 10 2, 10 4, 10 6, 10 8, 10 10,"
         " 8 10, 6 10, 4 10, 2 10, 0 10,"
         " 0 8, 0 6, 0 4, 0 2, 0 0))"), 8);
  EXPECT_GT(result->getNumGeometries(), 1u);
}

TEST(StSubdivide, AreaPreserved) {
  auto coll = st_subdivide_impl(geom("POLYGON ((0 0, 100 0, 100 100, 0 100, 0 0))"), 4);
  double total = 0;
  for (std::size_t i = 0; i < coll->getNumGeometries(); ++i)
    total += coll->getGeometryN(i)->getArea();
  EXPECT_NEAR(total, 10000.0, 1e-6);
}
