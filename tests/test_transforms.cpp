#include <gtest/gtest.h>
#include "helpers.hpp"

using namespace ch;

// ── ST_Translate ──────────────────────────────────────────────────────────────

TEST(StTranslate, Point) {
  EXPECT_EQ(geom2wkt(st_translate_impl(geom("POINT (0 0)"), 5, 12)), "POINT (5 12)");
}

TEST(StTranslate, PreservesArea) {
  auto after = st_translate_impl(geom("POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))"), 100, 200);
  EXPECT_EQ(geom2wkt(after), "POLYGON ((100 200, 101 200, 101 201, 100 201, 100 200))");
  EXPECT_NEAR(st_area_impl(std::move(after)), 1.0, 1e-10);
}

// ── ST_Scale ──────────────────────────────────────────────────────────────────

TEST(StScale, Point) {
  EXPECT_EQ(geom2wkt(st_scale_impl(geom("POINT (1 1)"), 5, 5)), "POINT (5 5)");
}

TEST(StScale, PolygonArea) {
  // Scale 1x1 unit square by (3,4) → area = 3*4 = 12
  EXPECT_NEAR(st_area_impl(st_scale_impl(geom("POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))"), 3, 4)), 12.0, 1e-10);
}

TEST(StSetSrid, Point) {
  EXPECT_EQ(geom2wkt(st_setsrid_impl(geom("POINT (1 1)"), 4236)), "SRID=4236;POINT (1 1)");
}

