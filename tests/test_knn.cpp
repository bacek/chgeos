#include <gtest/gtest.h>

#include <random>

#include "helpers.hpp"

#include "functions/knn.hpp"
#include "geom/wkb_distance.hpp"

namespace {

// Round-trip the coordinates at full precision: std::to_string would round to
// six decimals, so the WKB point and the doubles under test would disagree.
std::string point_wkt(double px, double py) {
    char buf[128];
    std::snprintf(buf, sizeof(buf), "POINT(%.17g %.17g)", px, py);
    return buf;
}

// GEOS is the reference: the raw-WKB walk must agree with Geometry::distance().
double geos_distance(const std::string & wkt, double px, double py) {
    auto g = geom(wkt);
    auto p = geom(point_wkt(px, py));
    return g->distance(p.get());
}

void expect_matches_geos(const std::string & wkt, double px, double py) {
    ch::Vector w = wkt2wkb(wkt);
    EXPECT_NEAR(ch::wkb_point_distance(wkb(w), px, py), geos_distance(wkt, px, py), 1e-12)
        << wkt << " from (" << px << ", " << py << ")";
}

TEST(WkbPointDistance, Point) {
    expect_matches_geos("POINT(3 4)", 0, 0);
    expect_matches_geos("POINT(3 4)", 3, 4);
}

TEST(WkbPointDistance, LineString) {
    // Perpendicular foot inside a segment, and past both endpoints.
    expect_matches_geos("LINESTRING(0 0, 10 0)", 5, 3);
    expect_matches_geos("LINESTRING(0 0, 10 0)", -4, 0);
    expect_matches_geos("LINESTRING(0 0, 10 0, 10 10)", 13, 6);
}

TEST(WkbPointDistance, PolygonOutside) {
    expect_matches_geos("POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))", 15, 5);
    expect_matches_geos("POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))", -3, -4);
}

TEST(WkbPointDistance, PolygonInsideIsZero) {
    ch::Vector w = wkt2wkb("POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))");
    EXPECT_DOUBLE_EQ(ch::wkb_point_distance(wkb(w), 5, 5), 0.0);
    // On the boundary is also zero.
    EXPECT_DOUBLE_EQ(ch::wkb_point_distance(wkb(w), 0, 5), 0.0);
}

TEST(WkbPointDistance, PolygonWithHole) {
    const char * wkt = "POLYGON((0 0, 10 0, 10 10, 0 10, 0 0),"
                       "(4 4, 6 4, 6 6, 4 6, 4 4))";
    // Inside the hole: distance is to the hole's ring, not the shell.
    expect_matches_geos(wkt, 5, 5);
    // Between hole and shell: inside the polygon.
    expect_matches_geos(wkt, 2, 2);
    // Outside the shell entirely.
    expect_matches_geos(wkt, 12, 5);
}

TEST(WkbPointDistance, MultiPolygonTakesNearest) {
    expect_matches_geos("MULTIPOLYGON(((0 0, 1 0, 1 1, 0 1, 0 0)),"
                        "((20 20, 21 20, 21 21, 20 21, 20 20)))", 19, 20.5);
}

TEST(WkbPointDistance, EmptyGeometryIsInfinite) {
    ch::Vector w = wkt2wkb("POLYGON EMPTY");
    EXPECT_TRUE(std::isinf(ch::wkb_point_distance(wkb(w), 0, 0)));
}

// The index refines against flattened coordinates rather than walking the WKB,
// so the two must agree everywhere — including holes and multi-geometries.
TEST(FlatRingsDistance, MatchesWkbWalk) {
    const char * geoms[] = {
        "POINT(3 4)",
        "LINESTRING(0 0, 10 0, 10 10)",
        "POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))",
        "POLYGON((0 0, 10 0, 10 10, 0 10, 0 0),(4 4, 6 4, 6 6, 4 6, 4 4))",
        "MULTIPOLYGON(((0 0, 1 0, 1 1, 0 1, 0 0)),((20 20, 21 20, 21 21, 20 21, 20 20)))",
        "GEOMETRYCOLLECTION(POINT(0 0), LINESTRING(5 5, 7 7))",
    };
    const double probes[][2] = {{5, 5}, {2, 2}, {12, 5}, {-3, -4}, {19, 20.5}, {0, 0}, {6, 7}};

    for (const char * wkt : geoms) {
        ch::Vector w = wkt2wkb(wkt);
        std::vector<double> coords;
        std::vector<ch::RingRef> rings;
        ch::flatten_wkb_rings(wkb(w), coords, rings);

        for (const auto & p : probes) {
            double flat = ch::flat_rings_point_distance(
                coords, rings, 0, static_cast<uint32_t>(rings.size()), p[0], p[1]);
            EXPECT_NEAR(flat, ch::wkb_point_distance(wkb(w), p[0], p[1]), 1e-12)
                << wkt << " from (" << p[0] << ", " << p[1] << ")";
        }
    }
}

// Reference kNN: exact distance to every candidate, ordered like the index.
std::vector<std::pair<uint64_t, double>>
brute_force(const std::vector<ch::Vector> & cands, double px, double py, uint32_t k) {
    std::vector<std::pair<uint64_t, double>> all;
    for (uint64_t i = 0; i < cands.size(); ++i)
        all.emplace_back(i, ch::wkb_point_distance(wkb(cands[i]), px, py));
    std::sort(all.begin(), all.end(), ch::knn_result_less);
    all.resize(std::min<size_t>(k, all.size()));
    return all;
}

std::vector<std::span<const uint8_t>> spans(const std::vector<ch::Vector> & v) {
    std::vector<std::span<const uint8_t>> out;
    for (const auto & e : v) out.push_back(wkb(e));
    return out;
}

// Boxes whose sizes vary a lot, so centroid distance and true distance disagree
// enough that a centroid-only index would pick the wrong neighbours.
std::vector<ch::Vector> random_boxes(size_t n, uint32_t seed) {
    std::mt19937 rng(seed);
    std::uniform_real_distribution<double> pos(-100.0, 100.0);
    std::uniform_real_distribution<double> size(0.1, 12.0);

    std::vector<ch::Vector> out;
    for (size_t i = 0; i < n; ++i) {
        double x = pos(rng), y = pos(rng), w = size(rng), h = size(rng);
        char buf[256];
        std::snprintf(buf, sizeof(buf),
                      "POLYGON((%f %f, %f %f, %f %f, %f %f, %f %f))",
                      x, y, x + w, y, x + w, y + h, x, y + h, x, y);
        out.push_back(wkt2wkb(buf));
    }
    return out;
}

TEST(CentroidKNNIndex, MatchesBruteForceOnVariedBoxes) {
    auto cands = random_boxes(500, 1234);
    auto cand_spans = spans(cands);
    ch::CentroidKNNIndex index(cand_spans);

    std::mt19937 rng(99);
    std::uniform_real_distribution<double> pos(-110.0, 110.0);

    for (int trial = 0; trial < 200; ++trial) {
        double px = pos(rng), py = pos(rng);
        ch::Vector q = wkt2wkb(point_wkt(px, py));

        auto got = index.query(wkb(q), 5);
        auto want = brute_force(cands, px, py, 5);

        ASSERT_EQ(got.size(), want.size());
        for (size_t i = 0; i < want.size(); ++i) {
            EXPECT_EQ(got[i].first, want[i].first) << "trial " << trial << " rank " << i;
            EXPECT_NEAR(got[i].second, want[i].second, 1e-12);
        }
    }
}

TEST(CentroidKNNIndex, ReportsTrueDistanceNotCentroidDistance) {
    // One big box: its centroid is far from the query, its edge is close.
    std::vector<ch::Vector> cands{
        wkt2wkb("POLYGON((1 -50, 100 -50, 100 50, 1 50, 1 -50))"),
        wkt2wkb("POLYGON((5 0, 6 0, 6 1, 5 1, 5 0))"),
    };
    auto cand_spans = spans(cands);
    ch::CentroidKNNIndex index(cand_spans);

    ch::Vector q = wkt2wkb("POINT(0 0)");
    auto got = index.query(wkb(q), 2);

    ASSERT_EQ(got.size(), 2u);
    // The big box wins: 1.0 away, versus 5.0 for the small one. Its centroid
    // sits at x≈50, so a centroid-only index would rank it second.
    EXPECT_EQ(got[0].first, 0u);
    EXPECT_NEAR(got[0].second, 1.0, 1e-12);
    EXPECT_EQ(got[1].first, 1u);
    EXPECT_NEAR(got[1].second, 5.0, 1e-12);
}

TEST(CentroidKNNIndex, NonPointQueryUsesGeos) {
    std::vector<ch::Vector> cands{
        wkt2wkb("POLYGON((10 0, 11 0, 11 1, 10 1, 10 0))"),
        wkt2wkb("POLYGON((0 10, 1 10, 1 11, 0 11, 0 10))"),
    };
    auto cand_spans = spans(cands);
    ch::CentroidKNNIndex index(cand_spans);

    // A horizontal segment: nearer the first box than the second.
    ch::Vector q = wkt2wkb("LINESTRING(0 0, 5 0)");
    auto got = index.query(wkb(q), 2);

    ASSERT_EQ(got.size(), 2u);
    EXPECT_EQ(got[0].first, 0u);
    EXPECT_NEAR(got[0].second, 5.0, 1e-12);
    EXPECT_EQ(got[1].first, 1u);
}

TEST(CentroidKNNIndex, KLargerThanCandidateSet) {
    auto cands = random_boxes(3, 7);
    auto cand_spans = spans(cands);
    ch::CentroidKNNIndex index(cand_spans);

    ch::Vector q = wkt2wkb("POINT(0 0)");
    EXPECT_EQ(index.query(wkb(q), 10).size(), 3u);
}

TEST(CentroidKNNIndex, EmptyCandidatesAreSkipped) {
    std::vector<ch::Vector> cands{
        wkt2wkb("POLYGON EMPTY"),
        wkt2wkb("POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))"),
    };
    auto cand_spans = spans(cands);
    ch::CentroidKNNIndex index(cand_spans);

    ch::Vector q = wkt2wkb("POINT(-1 0)");
    auto got = index.query(wkb(q), 5);
    ASSERT_EQ(got.size(), 1u);
    EXPECT_EQ(got[0].first, 1u);
    EXPECT_NEAR(got[0].second, 1.0, 1e-12);
}

} // namespace
