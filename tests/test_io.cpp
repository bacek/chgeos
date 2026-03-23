#include <gtest/gtest.h>
#include <cstring>
#include <geos/io/WKTWriter.h>
#include "helpers.hpp"

using namespace ch;

// ── helpers local to I/O tests ────────────────────────────────────────────────

static std::unique_ptr<Geometry> from_ewkt(const std::string &ewkt) {
  Vector v(ewkt.begin(), ewkt.end());
  return read_wkt(v);
}

// Build a minimal EWKB little-endian Point(x, y) with SRID embedded.
// Layout: 0x01 | type(0x20000001 LE) | srid(LE) | x(LE double) | y(LE double)
static Vector make_ewkb_point(int32_t srid, double x, double y) {
  Vector v;
  v.push_back(0x01);  // little-endian

  uint32_t type = 0x00000001u | 0x20000000u;  // Point | EWKB_SRID_FLAG
  for (int i = 0; i < 4; ++i) v.push_back((type >> (i * 8)) & 0xff);

  uint32_t srid_u = static_cast<uint32_t>(srid);
  for (int i = 0; i < 4; ++i) v.push_back((srid_u >> (i * 8)) & 0xff);

  uint64_t xbits, ybits;
  std::memcpy(&xbits, &x, 8);
  std::memcpy(&ybits, &y, 8);
  for (int i = 0; i < 8; ++i) v.push_back((xbits >> (i * 8)) & 0xff);
  for (int i = 0; i < 8; ++i) v.push_back((ybits >> (i * 8)) & 0xff);
  return v;
}

// ── read_wkt / EWKT ───────────────────────────────────────────────────────────

TEST(ReadWkt, PlainWktNoSrid) {
  auto g = from_ewkt("POINT (1 2)");
  EXPECT_EQ(g->getSRID(), 0);
  geos::io::WKTWriter w;
  w.setTrim(true);
  EXPECT_EQ(w.write(*g), "POINT (1 2)");
}

TEST(ReadWkt, EwktPointSrid) {
  auto g = from_ewkt("SRID=4326;POINT (1.5 2.5)");
  EXPECT_EQ(g->getSRID(), 4326);
  geos::io::WKTWriter w;
  w.setTrim(true);
  EXPECT_EQ(w.write(*g), "POINT (1.5 2.5)");
}

TEST(ReadWkt, EwktPolygonSrid) {
  auto g = from_ewkt("SRID=32632;POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))");
  EXPECT_EQ(g->getSRID(), 32632);
  EXPECT_NEAR(g->getArea(), 1.0, 1e-10);
}

TEST(ReadWkt, EwktNegativeSrid) {
  // Negative SRIDs are allowed by the PostGIS lexer (SRID=-?[0-9]+).
  auto g = from_ewkt("SRID=-1;POINT (0 0)");
  EXPECT_EQ(g->getSRID(), -1);
}

TEST(ReadWkt, EwktCaseSensitive) {
  // Lowercase "srid=" is NOT recognised — treated as plain WKT and fails to parse.
  Vector v{'s','r','i','d','=','4','2',';','P','O','I','N','T',' ','(','0',' ','0',')'};
  EXPECT_THROW(read_wkt(v), std::exception);
}

TEST(ReadWkt, EwktRoundTrip) {
  // st_asewkt o read_wkt is identity.
  auto g = from_ewkt("SRID=4326;LINESTRING (0 0, 1 1)");
  auto res = st_asewkt_impl(std::move(g));
  EXPECT_EQ(std::string(res.begin(), res.end()), "SRID=4326;LINESTRING (0 0, 1 1)");
}

// ── ST_AsEWKT ─────────────────────────────────────────────────────────────────

TEST(StAsEwkt, NoSrid) {
  auto res = st_asewkt_impl(geom("POINT (1 2)"));
  EXPECT_EQ(std::string(res.begin(), res.end()), "POINT (1 2)");
}

TEST(StAsEwkt, WithSrid) {
  auto g = geom("POINT (1 2)");
  g->setSRID(4326);
  auto res = st_asewkt_impl(std::move(g));
  EXPECT_EQ(std::string(res.begin(), res.end()), "SRID=4326;POINT (1 2)");
}

TEST(StAsEwkt, PolygonWithSrid) {
  auto g = geom("POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))");
  g->setSRID(32632);
  auto res = st_asewkt_impl(std::move(g));
  EXPECT_EQ(std::string(res.begin(), res.end()),
            "SRID=32632;POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))");
}

// ── ST_GeomFromWKB ────────────────────────────────────────────────────────────

TEST(StGeomFromWkb, RoundTrip) {
  EXPECT_EQ(wkb2wkt(st_geomfromwkb_impl(wkt2wkb("POINT (1 2)"))), "POINT (1 2)");
  EXPECT_EQ(wkb2wkt(st_geomfromwkb_impl(wkt2wkb("LINESTRING (0 0, 1 1, 2 2)"))),
            "LINESTRING (0 0, 1 1, 2 2)");
}

TEST(StGeomFromWkb, EwkbSridExtracted) {
  auto ewkb = make_ewkb_point(4326, 1.5, 2.5);
  auto g = read_wkb(ewkb);
  EXPECT_EQ(g->getSRID(), 4326);

  geos::io::WKTWriter w;
  w.setTrim(true);
  EXPECT_EQ(w.write(*g), "POINT (1.5 2.5)");
}

TEST(StGeomFromWkb, EwkbNoSridUnchanged) {
  // Standard WKB (no SRID flag) — SRID stays 0.
  auto wkb = wkt2wkb("POINT (3 4)");
  auto g = read_wkb(wkb);
  EXPECT_EQ(g->getSRID(), 0);
  geos::io::WKTWriter w;
  w.setTrim(true);
  EXPECT_EQ(w.write(*g), "POINT (3 4)");
}

TEST(StGeomFromWkb, EwkbSridPolygon) {
  // Round-trip a polygon through WKB (standard), then inject SRID via EWKB.
  auto wkb = wkt2wkb("POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))");
  // Manually set SRID flag in the type word and prepend SRID field.
  Vector ewkb;
  ewkb.push_back(wkb[0]);  // endian byte
  uint32_t type;
  std::memcpy(&type, wkb.data() + 1, 4);
  type |= 0x20000000u;
  uint32_t srid32 = 32632u;
  for (int i = 0; i < 4; ++i) ewkb.push_back((type >> (i * 8)) & 0xff);
  for (int i = 0; i < 4; ++i) ewkb.push_back((srid32 >> (i * 8)) & 0xff);
  ewkb.insert(ewkb.end(), wkb.begin() + 5, wkb.end());

  auto g = read_wkb(ewkb);
  EXPECT_EQ(g->getSRID(), 32632);
  EXPECT_NEAR(g->getArea(), 1.0, 1e-10);
}

// ── write_ewkb ────────────────────────────────────────────────────────────────

TEST(WriteEwkb, NoSridPlainWkb) {
  // SRID == 0 → no SRID flag, plain WKB output.
  auto g = read_wkt(Vector({'P','O','I','N','T',' ','(','1',' ','2',')'}));
  auto out = write_ewkb(std::move(g));
  uint32_t type;
  std::memcpy(&type, out.data() + 1, 4);
  EXPECT_EQ(type & 0x20000000u, 0u);
}

TEST(WriteEwkb, WithSridInjectsFlag) {
  // SRID == 4326 → SRID flag set and SRID field present at bytes 5-8.
  auto g = read_wkb(make_ewkb_point(4326, 7.0, 8.0));
  ASSERT_EQ(g->getSRID(), 4326);
  auto out = write_ewkb(std::move(g));
  uint32_t type;
  std::memcpy(&type, out.data() + 1, 4);
  EXPECT_NE(type & 0x20000000u, 0u);
  uint32_t srid_out;
  std::memcpy(&srid_out, out.data() + 5, 4);
  EXPECT_EQ(srid_out, 4326u);
}

TEST(WriteEwkb, RoundTrip) {
  // Write EWKB then read it back: geometry and SRID must survive.
  auto g_in = read_wkb(make_ewkb_point(32632, 3.0, 4.0));
  auto ewkb = write_ewkb(g_in->clone());
  auto g_out = read_wkb(ewkb);
  EXPECT_EQ(g_out->getSRID(), 32632);
  geos::io::WKTWriter w; w.setTrim(true);
  EXPECT_EQ(w.write(*g_out), "POINT (3 4)");
}
