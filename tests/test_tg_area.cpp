#include <gtest/gtest.h>
#include <cstring>
#include <span>
#include <vector>

#include "helpers.hpp"
#include "tg_area.hpp"
#include "tg_geom.hpp"

using namespace ch;

// ── TgGeom: generic RAII handle ──────────────────────────────────────────────

TEST(TgGeom, ParsesAllGeometryTypes) {
    EXPECT_TRUE(TgGeom::from_wkb(wkb(wkt2wkb("POINT (0 0)"))).ok());
    EXPECT_TRUE(TgGeom::from_wkb(wkb(wkt2wkb("LINESTRING (0 0, 1 1)"))).ok());
    EXPECT_TRUE(TgGeom::from_wkb(wkb(wkt2wkb("POLYGON ((0 0, 1 0, 1 1, 0 0))"))).ok());
    EXPECT_TRUE(TgGeom::from_wkb(wkb(wkt2wkb("MULTIPOLYGON (((0 0, 1 0, 1 1, 0 0)))"))).ok());
    EXPECT_TRUE(TgGeom::from_wkb(wkb(wkt2wkb("GEOMETRYCOLLECTION (POINT (0 0))"))).ok());
}

TEST(TgGeom, ReportsType) {
    EXPECT_EQ(TgGeom::from_wkb(wkb(wkt2wkb("POINT (0 0)"))).type(), TG_POINT);
    EXPECT_EQ(TgGeom::from_wkb(wkb(wkt2wkb("LINESTRING (0 0, 1 1)"))).type(), TG_LINESTRING);
    EXPECT_EQ(TgGeom::from_wkb(wkb(wkt2wkb("POLYGON ((0 0, 1 0, 1 1, 0 0))"))).type(), TG_POLYGON);
    EXPECT_EQ(TgGeom::from_wkb(wkb(wkt2wkb("MULTIPOLYGON (((0 0, 1 0, 1 1, 0 0)))"))).type(), TG_MULTIPOLYGON);
}

TEST(TgGeom, ReportsRect) {
    auto g = TgGeom::from_wkb(wkb(wkt2wkb("POLYGON ((1 2, 10 2, 10 9, 1 9, 1 2))")));
    ASSERT_TRUE(g.ok());
    auto r = g.rect();
    EXPECT_DOUBLE_EQ(r.min.x, 1.0);
    EXPECT_DOUBLE_EQ(r.min.y, 2.0);
    EXPECT_DOUBLE_EQ(r.max.x, 10.0);
    EXPECT_DOUBLE_EQ(r.max.y, 9.0);
}

TEST(TgGeom, RejectsGarbage) {
    ch::Vector bad = {1, 2, 3};
    auto g = TgGeom::from_wkb(std::span<const uint8_t>(bad));
    EXPECT_FALSE(g.ok());
    EXPECT_EQ(static_cast<int>(g.type()), 0);
    EXPECT_EQ(g.get(), nullptr);
}

// Patch a plain-WKB byte string into PostGIS EWKB: set the SRID flag bit in
// the type word (offset 1, little-endian) and insert the 4-byte SRID at 5.
static ch::Vector with_srid(const ch::Vector& wkb, uint32_t srid) {
    ch::Vector v = wkb;
    uint32_t t;
    std::memcpy(&t, v.data() + 1, 4);
    t |= 0x20000000u;
    std::memcpy(v.data() + 1, &t, 4);
    v.insert(v.begin() + 5, 4, 0);
    std::memcpy(v.data() + 5, &srid, 4);
    return v;
}

// ── Parsing ──────────────────────────────────────────────────────────────────

TEST(TgArea, ParsesPolygon) {
    auto w = wkt2wkb("POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))");
    EXPECT_TRUE(TgArea::from_wkb(wkb(w)).ok());
}

TEST(TgArea, ParsesMultipolygon) {
    auto w = wkt2wkb("MULTIPOLYGON (((0 0, 1 0, 1 1, 0 0)), ((5 5, 6 5, 6 6, 5 5, 5 5)))");
    EXPECT_TRUE(TgArea::from_wkb(wkb(w)).ok());
}

TEST(TgArea, RejectsNonAreaGeometries_GeomLevel) {
    // The generic handle parses these fine — the area view must still refuse.
    auto pt = TgGeom::from_wkb(wkb(wkt2wkb("POINT (0 0)")));
    ASSERT_TRUE(pt.ok());
    ch::Vector p = wkt2wkb("POINT (0 0)");
    EXPECT_FALSE(TgArea::from_wkb(wkb(p)).ok());
}

TEST(TgArea, ParsesEwkbWithSrid) {
    auto w = with_srid(wkt2wkb("POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))"), 4326);
    EXPECT_TRUE(TgArea::from_wkb(wkb(w)).ok());
}

TEST(TgArea, RejectsNonAreaGeometries) {
    EXPECT_FALSE(TgArea::from_wkb(wkb(wkt2wkb("POINT (0 0)"))).ok());
    EXPECT_FALSE(TgArea::from_wkb(wkb(wkt2wkb("LINESTRING (0 0, 1 1)"))).ok());
    EXPECT_FALSE(TgArea::from_wkb(wkb(wkt2wkb("MULTIPOINT ((0 0), (1 1))"))).ok());
}

TEST(TgArea, RejectsGarbageAndEmpty) {
    ch::Vector garbage = {0x01, 0x02, 0x03};
    EXPECT_FALSE(TgArea::from_wkb(std::span<const uint8_t>(garbage)).ok());
    EXPECT_FALSE(TgArea::from_wkb(std::span<const uint8_t>{}).ok());
}

// ── Point location semantics (hand literals) ─────────────────────────────────
// kSquare10: (2,2) interior; (0,0) vertex = boundary; (5,0) edge midpoint =
// boundary; (12,12) exterior.
namespace {
TgArea square10() {
    return TgArea::from_wkb(wkb(wkt2wkb("POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))")));
}
}  // namespace

TEST(TgArea, InteriorMode) {
    auto a = square10();
    ASSERT_TRUE(a.ok());
    EXPECT_TRUE(a.point_in(2, 2, TgPointMode::kInterior));
    EXPECT_FALSE(a.point_in(0, 0, TgPointMode::kInterior));   // vertex
    EXPECT_FALSE(a.point_in(5, 0, TgPointMode::kInterior));   // edge midpoint
    EXPECT_FALSE(a.point_in(12, 12, TgPointMode::kInterior));
}

TEST(TgArea, CoversMode) {
    auto a = square10();
    ASSERT_TRUE(a.ok());
    EXPECT_TRUE(a.point_in(2, 2, TgPointMode::kCovers));
    EXPECT_TRUE(a.point_in(0, 0, TgPointMode::kCovers));      // vertex
    EXPECT_TRUE(a.point_in(5, 0, TgPointMode::kCovers));      // edge midpoint
    EXPECT_FALSE(a.point_in(12, 12, TgPointMode::kCovers));
}

TEST(TgArea, ExteriorMode) {
    auto a = square10();
    ASSERT_TRUE(a.ok());
    EXPECT_FALSE(a.point_in(2, 2, TgPointMode::kExterior));
    EXPECT_FALSE(a.point_in(0, 0, TgPointMode::kExterior));   // vertex
    EXPECT_FALSE(a.point_in(5, 0, TgPointMode::kExterior));   // edge midpoint
    EXPECT_TRUE(a.point_in(12, 12, TgPointMode::kExterior));
}
