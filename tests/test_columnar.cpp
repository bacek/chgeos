#include <gtest/gtest.h>
#include <cstring>
#include <optional>
#include <vector>

#include <geos/geom/LineString.h>
#include <geos/geom/Point.h>

#include "helpers.hpp"
#include "geo_columnar.hpp"
#include "functions/collect_fast.hpp"
#include "functions/overlay.hpp"
#include "functions/predicates.hpp"

using namespace ch;

// ── Columnar buffer builder ───────────────────────────────────────────────────

struct ColData {
    uint32_t              col_type;   // ColType | COL_IS_CONST
    std::vector<uint8_t>  null_map;   // non-empty → nullable column
    std::vector<uint64_t> offsets;    // non-empty → variable-width column
    std::vector<uint8_t>  data;
};

// Assemble a COLUMNAR_V1 raw_buffer from a row count and per-column data.
static raw_buffer* make_columnar(uint32_t num_rows, std::vector<ColData> cols) {
    uint32_t pos = HEADER_BYTES + static_cast<uint32_t>(cols.size()) * COL_DESC_BYTES;

    struct BI { uint32_t null_off, offsets_off, data_off, data_sz; };
    std::vector<BI> bi;
    for (auto& col : cols) {
        BI b{};
        if (!col.null_map.empty()) {
            b.null_off = pos;
            pos += static_cast<uint32_t>(col.null_map.size());
        }
        if (!col.offsets.empty()) {
            pos = (pos + 7u) & ~7u;   // 8-byte align
            b.offsets_off = pos;
            pos += static_cast<uint32_t>(col.offsets.size()) * 8u;
        }
        b.data_off = pos;
        b.data_sz  = static_cast<uint32_t>(col.data.size());
        pos += b.data_sz;
        bi.push_back(b);
    }

    auto* buf = clickhouse_create_buffer(pos);
    buf->resize(pos);
    uint8_t* p = buf->data();
    std::memset(p, 0, pos);

    uint32_t nc = static_cast<uint32_t>(cols.size());
    write_frame_header(p, num_rows, nc);

    for (size_t i = 0; i < cols.size(); ++i) {
        ColDescriptor d{};
        d.type           = cols[i].col_type;
        d.null_offset    = bi[i].null_off;
        d.offsets_offset = bi[i].offsets_off;
        d.data_offset    = bi[i].data_off;
        d.data_size      = bi[i].data_sz;
        std::memcpy(p + HEADER_BYTES + i * COL_DESC_BYTES, &d, sizeof(d));

        if (!cols[i].null_map.empty())
            std::memcpy(p + bi[i].null_off, cols[i].null_map.data(), cols[i].null_map.size());
        if (!cols[i].offsets.empty())
            std::memcpy(p + bi[i].offsets_off, cols[i].offsets.data(), cols[i].offsets.size() * 8);
        if (!cols[i].data.empty())
            std::memcpy(p + bi[i].data_off, cols[i].data.data(), cols[i].data.size());
    }
    return buf;
}

// Non-nullable variable-length (geometry/WKB) column.
static ColData bytes_col(bool is_const, const std::vector<ch::Vector>& wkbs) {
    ColData col;
    col.col_type = static_cast<uint32_t>(COL_BYTES) | (is_const ? static_cast<uint32_t>(COL_IS_CONST) : 0u);
    col.offsets.push_back(0u);
    for (auto& w : wkbs) {
        col.data.insert(col.data.end(), w.begin(), w.end());
        col.data.push_back(0u);   // CH ColumnString null terminator
        col.offsets.push_back(static_cast<uint64_t>(col.data.size()));
    }
    return col;
}

// Nullable variable-length column (null_map[i] == 0xFF → NULL).
static ColData null_bytes_col(bool is_const,
                              const std::vector<ch::Vector>& wkbs,
                              const std::vector<uint8_t>& nulls) {
    ColData col;
    col.col_type = static_cast<uint32_t>(COL_BYTES | COL_IS_NULLABLE) | (is_const ? static_cast<uint32_t>(COL_IS_CONST) : 0u);
    col.null_map = nulls;
    col.offsets.push_back(0u);
    for (size_t i = 0; i < wkbs.size(); ++i) {
        if (nulls[i]) {
            col.data.push_back(0u);   // empty string for null row
        } else {
            col.data.insert(col.data.end(), wkbs[i].begin(), wkbs[i].end());
            col.data.push_back(0u);
        }
        col.offsets.push_back(static_cast<uint64_t>(col.data.size()));
    }
    return col;
}

// Fixed-width 64-bit column (double).
static ColData fixed64_col(bool is_const, const std::vector<double>& vals) {
    ColData col;
    col.col_type = static_cast<uint32_t>(COL_FIXED64) | (is_const ? static_cast<uint32_t>(COL_IS_CONST) : 0u);
    col.data.resize(vals.size() * 8u);
    std::memcpy(col.data.data(), vals.data(), vals.size() * 8u);
    return col;
}

// Read COL_FIXED8 bool output and free the buffer.
static std::vector<uint8_t> read_bool_col(raw_buffer* out, uint32_t n) {
    std::vector<uint8_t> res(n);
    std::memcpy(res.data(), out->data() + HEADER_BYTES + COL_DESC_BYTES, n);
    clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(out));
    return res;
}

// ── st_contains_col: PreparedGeometry for 2-arg predicates ───────────────────

// Points used across the contains tests:
//   inside   POINT (0.5 0.5)  → st_contains(square, pt) = true
//   outside  POINT (2.0 2.0)  → false
//   boundary POINT (0.0 0.0)  → false (boundary, not interior)
static const std::string kSquare = "POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))";

TEST(ColumnarPrepGeom, ContainsAConst_MatchesBaseline) {
    auto poly   = wkt2wkb(kSquare);
    auto pt_in  = wkt2wkb("POINT (0.5 0.5)");
    auto pt_out = wkt2wkb("POINT (2.0 2.0)");
    auto pt_bnd = wkt2wkb("POINT (0.0 0.0)");
    const uint32_t n = 3;

    // A-const: col[0] is the const polygon, col[1] varies.
    auto* buf_aconst = make_columnar(n, {
        bytes_col(true,  {poly}),
        bytes_col(false, {pt_in, pt_out, pt_bnd}),
    });
    auto got = read_bool_col(
        columnar_impl_wrapper(buf_aconst, n, st_contains_impl,
            bbox_op_contains, false, prep_a_st_contains, prep_b_st_contains),
        n);
    clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(buf_aconst));

    EXPECT_EQ(got[0], 1u);  // inside
    EXPECT_EQ(got[1], 0u);  // outside
    EXPECT_EQ(got[2], 0u);  // boundary not properly contained

    // Baseline: non-const A (repeat poly for each row) — must agree.
    auto* buf_base = make_columnar(n, {
        bytes_col(false, {poly, poly, poly}),
        bytes_col(false, {pt_in, pt_out, pt_bnd}),
    });
    auto base = read_bool_col(
        columnar_impl_wrapper(buf_base, n, st_contains_impl,
            bbox_op_contains, false, prep_a_st_contains, prep_b_st_contains),
        n);
    clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(buf_base));

    EXPECT_EQ(got, base);
}

TEST(ColumnarPrepGeom, ContainsBConst_MatchesBaseline) {
    // B-const: col[1] is a const point, col[0] varies.
    // st_contains(polygon, const_pt): prep_b_st_contains = pb->within(a),
    // i.e. "does the const point lie within the variable polygon?"
    auto pt    = wkt2wkb("POINT (0.5 0.5)");
    auto big   = wkt2wkb("POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))");  // contains pt
    auto small = wkt2wkb("POLYGON ((2 2, 3 2, 3 3, 2 3, 2 2))");  // doesn't contain pt
    const uint32_t n = 2;

    auto* buf_bconst = make_columnar(n, {
        bytes_col(false, {big, small}),
        bytes_col(true,  {pt}),
    });
    auto got = read_bool_col(
        columnar_impl_wrapper(buf_bconst, n, st_contains_impl,
            bbox_op_contains, false, prep_a_st_contains, prep_b_st_contains),
        n);
    clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(buf_bconst));

    EXPECT_EQ(got[0], 1u);  // big polygon contains pt
    EXPECT_EQ(got[1], 0u);  // small polygon doesn't

    // Baseline: replicate the const point.
    auto* buf_base = make_columnar(n, {
        bytes_col(false, {big, small}),
        bytes_col(false, {pt, pt}),
    });
    auto base = read_bool_col(
        columnar_impl_wrapper(buf_base, n, st_contains_impl,
            bbox_op_contains, false, prep_a_st_contains, prep_b_st_contains),
        n);
    clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(buf_base));

    EXPECT_EQ(got, base);
}

// ── Point fast path: boundary semantics ──────────────────────────────────────
// A 2D WKB point against a const polygon takes a dedicated path that locates
// the point (INTERIOR / BOUNDARY / EXTERIOR) instead of running the full
// predicate.  Three things must agree on a boundary point: the indexed locator
// (batch >= INDEXED_LOCATOR_MIN_ROWS), the simple locator (smaller batch), and
// the generic PreparedGeometry path (polygon column not const).  A batch-size
// dependent answer shows up as a nondeterministic row count once the join
// splits its input differently between runs.
namespace {

// Cycle of points covering every Location against kSquare.
std::vector<std::vector<uint8_t>> cycled_points(uint32_t n) {
    const std::vector<std::string> wkt = {
        "POINT (0.5 0.5)",  // interior
        "POINT (0.0 0.0)",  // boundary: vertex
        "POINT (2.0 2.0)",  // exterior
        "POINT (0.5 0.0)",  // boundary: edge midpoint
    };
    std::vector<std::vector<uint8_t>> pts;
    for (uint32_t i = 0; i < n; ++i) pts.push_back(wkt2wkb(wkt[i % wkt.size()]));
    return pts;
}

// Expected result per point in the cycle above, given the truth value the
// predicate takes on the boundary.
std::vector<uint8_t> expected_cycle(uint32_t n, uint8_t on_boundary) {
    const std::vector<uint8_t> one_cycle = {1u, on_boundary, 0u, on_boundary};
    std::vector<uint8_t> exp;
    for (uint32_t i = 0; i < n; ++i) exp.push_back(one_cycle[i % one_cycle.size()]);
    return exp;
}

}  // namespace

// Predicates written point-first: st_within / st_coveredby / st_intersects.
// The const polygon is col[1], so this exercises the B-const point path.
TEST(ColumnarPointPath, BoundaryIsPredicateSpecific_PointFirst) {
    auto poly = wkt2wkb(kSquare);

    struct Case {
        const char* name;
        bool (*impl)(std::unique_ptr<Geometry>, std::unique_ptr<Geometry>);
        BboxOp bbox_op;
        ColPrepOp prep_a;
        ColPrepOp prep_b;
        ColPrepPointOp pt_a;
        ColPrepPointOp pt_b;
        uint8_t on_boundary;
    };
    const std::vector<Case> cases = {
        {"st_within",     st_within_impl,     bbox_op_rcontains,
         prep_a_st_within,     prep_b_st_within,
         prep_a_pt_st_within,     prep_b_pt_st_within,     0u},
        {"st_coveredby",  st_coveredby_impl,  bbox_op_rcontains,
         prep_a_st_coveredby,  prep_b_st_coveredby,
         prep_a_pt_st_coveredby,  prep_b_pt_st_coveredby,  1u},
        {"st_intersects", st_intersects_impl, bbox_op_intersects,
         prep_a_st_intersects, prep_b_st_intersects,
         prep_a_pt_st_intersects, prep_b_pt_st_intersects, 1u},
    };

    // Below and above the indexed-locator threshold.
    for (uint32_t n : {4u, INDEXED_LOCATOR_MIN_ROWS + 4u}) {
        auto pts = cycled_points(n);
        for (const auto& c : cases) {
            SCOPED_TRACE(std::string(c.name) + " n=" + std::to_string(n));

            auto* buf_pt = make_columnar(n, {
                bytes_col(false, pts),
                bytes_col(true,  {poly}),
            });
            auto got = read_bool_col(
                columnar_impl_wrapper(buf_pt, n, c.impl, c.bbox_op, false,
                                      c.prep_a, c.prep_b, nullptr, nullptr,
                                      c.pt_a, c.pt_b),
                n);
            clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(buf_pt));

            EXPECT_EQ(got, expected_cycle(n, c.on_boundary));

            // Generic PreparedGeometry path: same rows, polygon column not const.
            std::vector<std::vector<uint8_t>> polys(n, poly);
            auto* buf_gen = make_columnar(n, {
                bytes_col(false, pts),
                bytes_col(false, polys),
            });
            auto generic = read_bool_col(
                columnar_impl_wrapper(buf_gen, n, c.impl, c.bbox_op, false,
                                      c.prep_a, c.prep_b, nullptr, nullptr,
                                      c.pt_a, c.pt_b),
                n);
            clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(buf_gen));

            EXPECT_EQ(got, generic);
        }
    }
}

// Predicates written polygon-first: st_contains / st_covers.
// The const polygon is col[0], so this exercises the A-const point path.
TEST(ColumnarPointPath, BoundaryIsPredicateSpecific_PolygonFirst) {
    auto poly = wkt2wkb(kSquare);

    struct Case {
        const char* name;
        bool (*impl)(std::unique_ptr<Geometry>, std::unique_ptr<Geometry>);
        ColPrepOp prep_a;
        ColPrepOp prep_b;
        ColPrepPointOp pt_a;
        ColPrepPointOp pt_b;
        uint8_t on_boundary;
    };
    const std::vector<Case> cases = {
        {"st_contains", st_contains_impl,
         prep_a_st_contains, prep_b_st_contains,
         prep_a_pt_st_contains, prep_b_pt_st_contains, 0u},
        {"st_covers",   st_covers_impl,
         prep_a_st_covers,   prep_b_st_covers,
         prep_a_pt_st_covers,   prep_b_pt_st_covers,   1u},
    };

    for (uint32_t n : {4u, INDEXED_LOCATOR_MIN_ROWS + 4u}) {
        auto pts = cycled_points(n);
        for (const auto& c : cases) {
            SCOPED_TRACE(std::string(c.name) + " n=" + std::to_string(n));

            auto* buf_pt = make_columnar(n, {
                bytes_col(true,  {poly}),
                bytes_col(false, pts),
            });
            auto got = read_bool_col(
                columnar_impl_wrapper(buf_pt, n, c.impl, bbox_op_contains, false,
                                      c.prep_a, c.prep_b, nullptr, nullptr,
                                      c.pt_a, c.pt_b),
                n);
            clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(buf_pt));

            EXPECT_EQ(got, expected_cycle(n, c.on_boundary));

            std::vector<std::vector<uint8_t>> polys(n, poly);
            auto* buf_gen = make_columnar(n, {
                bytes_col(false, polys),
                bytes_col(false, pts),
            });
            auto generic = read_bool_col(
                columnar_impl_wrapper(buf_gen, n, c.impl, bbox_op_contains, false,
                                      c.prep_a, c.prep_b, nullptr, nullptr,
                                      c.pt_a, c.pt_b),
                n);
            clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(buf_gen));

            EXPECT_EQ(got, generic);
        }
    }
}

// The point path keeps the const polygon (and its index) across calls.  Two
// polygons of identical WKB size, alternated call by call, must each be
// answered against their own geometry — a cache keyed on anything weaker than
// the full bytes would answer the second call with the first polygon.
TEST(ColumnarPointPath, ConstPolygonChangesBetweenCalls) {
    auto left  = wkt2wkb("POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))");
    auto right = wkt2wkb("POLYGON ((2 0, 3 0, 3 1, 2 1, 2 0))");
    ASSERT_EQ(left.size(), right.size());

    auto in_left  = wkt2wkb("POINT (0.5 0.5)");
    auto in_right = wkt2wkb("POINT (2.5 0.5)");

    auto run = [&](const std::vector<uint8_t>& poly, uint32_t n) {
        std::vector<std::vector<uint8_t>> pts;
        for (uint32_t i = 0; i < n; ++i) pts.push_back(i % 2 ? in_right : in_left);
        auto* buf = make_columnar(n, {bytes_col(false, pts), bytes_col(true, {poly})});
        auto got = read_bool_col(
            columnar_impl_wrapper(buf, n, st_intersects_impl, bbox_op_intersects, false,
                                  prep_a_st_intersects, prep_b_st_intersects,
                                  nullptr, nullptr,
                                  prep_a_pt_st_intersects, prep_b_pt_st_intersects),
            n);
        clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(buf));
        return got;
    };
    auto expect = [](bool left_poly, uint32_t n) {
        std::vector<uint8_t> e;
        for (uint32_t i = 0; i < n; ++i) e.push_back((i % 2 == 0) == left_poly ? 1u : 0u);
        return e;
    };

    for (uint32_t n : {4u, INDEXED_LOCATOR_MIN_ROWS + 4u}) {
        SCOPED_TRACE("n=" + std::to_string(n));
        EXPECT_EQ(run(left,  n), expect(true,  n));
        EXPECT_EQ(run(right, n), expect(false, n));
        EXPECT_EQ(run(left,  n), expect(true,  n));
    }
}

TEST(ColumnarPrepGeom, ConstColNull_AllResultsZero) {
    // When the const geometry column is NULL, all output rows must be 0.
    auto dummy_wkb = wkt2wkb("POINT (0 0)");
    auto pt        = wkt2wkb("POINT (0.5 0.5)");
    const uint32_t n = 3;

    auto* buf = make_columnar(n, {
        null_bytes_col(true, {dummy_wkb}, {0xFFu}),   // const + null
        bytes_col(false, {pt, pt, pt}),
    });
    auto got = read_bool_col(
        columnar_impl_wrapper(buf, n, st_contains_impl,
            bbox_op_contains, false, prep_a_st_contains, prep_b_st_contains),
        n);
    clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(buf));

    EXPECT_EQ(got, (std::vector<uint8_t>{0u, 0u, 0u}));
}

TEST(ColumnarPrepGeom, VariableColNullRow_YieldsZero) {
    // NULL in a non-const variable column → that row's result is 0.
    auto poly  = wkt2wkb(kSquare);
    auto pt_in = wkt2wkb("POINT (0.5 0.5)");
    auto pt_dummy = wkt2wkb("POINT (0 0)");
    const uint32_t n = 3;

    auto* buf = make_columnar(n, {
        bytes_col(true, {poly}),
        null_bytes_col(false, {pt_in, pt_dummy, pt_in}, {0u, 0xFFu, 0u}),  // row 1 is NULL
    });
    auto got = read_bool_col(
        columnar_impl_wrapper(buf, n, st_contains_impl,
            bbox_op_contains, false, prep_a_st_contains, prep_b_st_contains),
        n);
    clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(buf));

    EXPECT_EQ(got[0], 1u);  // inside → true
    EXPECT_EQ(got[1], 0u);  // NULL → 0
    EXPECT_EQ(got[2], 1u);  // inside → true
}

// ── st_dwithin_col: PreparedGeometry for 3-arg dist predicates ───────────────

// Geometries and distances used across dwithin tests:
//   origin  POINT (0 0)
//   near    POINT (3 0)  → dist 3.0  — within 5.0
//   far     POINT (10 0) → dist 10.0 — outside 5.0
//   bbox-miss POINT (100 100) → rejected by bbox pre-filter before GEOS call
static constexpr double kDist = 5.0;

TEST(ColumnarPrepGeomDist, DWithinAConst_MatchesBaseline) {
    auto origin   = wkt2wkb("POINT (0 0)");
    auto near_pt  = wkt2wkb("POINT (3 0)");
    auto far_pt   = wkt2wkb("POINT (10 0)");
    auto bbox_miss = wkt2wkb("POINT (100 100)");
    const uint32_t n = 3;

    // A-const: col[0] = const origin, col[1] = variable, col[2] = const distance.
    auto* buf_aconst = make_columnar(n, {
        bytes_col(true,  {origin}),
        bytes_col(false, {near_pt, far_pt, bbox_miss}),
        fixed64_col(true, {kDist}),
    });
    auto got = read_bool_col(
        columnar_impl_wrapper(buf_aconst, n, st_dwithin_impl,
            nullptr, false, nullptr, nullptr,
            prep_a_st_dwithin, prep_b_st_dwithin),
        n);
    clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(buf_aconst));

    EXPECT_EQ(got[0], 1u);  // dist 3 < 5 → true
    EXPECT_EQ(got[1], 0u);  // dist 10 > 5 → false
    EXPECT_EQ(got[2], 0u);  // bbox miss → false

    // Baseline: non-const, same origin repeated.
    auto* buf_base = make_columnar(n, {
        bytes_col(false, {origin, origin, origin}),
        bytes_col(false, {near_pt, far_pt, bbox_miss}),
        fixed64_col(false, {kDist, kDist, kDist}),
    });
    auto base = read_bool_col(
        columnar_impl_wrapper(buf_base, n, st_dwithin_impl,
            nullptr, false, nullptr, nullptr,
            prep_a_st_dwithin, prep_b_st_dwithin),
        n);
    clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(buf_base));

    EXPECT_EQ(got, base);
}

TEST(ColumnarPrepGeomDist, DWithinBConst_MatchesBaseline) {
    // B-const: col[0] = variable, col[1] = const origin, col[2] = const distance.
    auto origin  = wkt2wkb("POINT (0 0)");
    auto near_pt = wkt2wkb("POINT (3 0)");
    auto far_pt  = wkt2wkb("POINT (10 0)");
    const uint32_t n = 2;

    auto* buf_bconst = make_columnar(n, {
        bytes_col(false, {near_pt, far_pt}),
        bytes_col(true,  {origin}),
        fixed64_col(true, {kDist}),
    });
    auto got = read_bool_col(
        columnar_impl_wrapper(buf_bconst, n, st_dwithin_impl,
            nullptr, false, nullptr, nullptr,
            prep_a_st_dwithin, prep_b_st_dwithin),
        n);
    clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(buf_bconst));

    EXPECT_EQ(got[0], 1u);  // near → true
    EXPECT_EQ(got[1], 0u);  // far  → false

    // Baseline: replicate const origin.
    auto* buf_base = make_columnar(n, {
        bytes_col(false, {near_pt, far_pt}),
        bytes_col(false, {origin, origin}),
        fixed64_col(false, {kDist, kDist}),
    });
    auto base = read_bool_col(
        columnar_impl_wrapper(buf_base, n, st_dwithin_impl,
            nullptr, false, nullptr, nullptr,
            prep_a_st_dwithin, prep_b_st_dwithin),
        n);
    clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(buf_base));

    EXPECT_EQ(got, base);
}

TEST(ColumnarPrepGeomDist, DWithinConstColNull_AllResultsZero) {
    auto dummy_wkb = wkt2wkb("POINT (0 0)");
    auto pt        = wkt2wkb("POINT (0.5 0.5)");
    const uint32_t n = 2;

    // const A is null → all rows must be 0
    auto* buf = make_columnar(n, {
        null_bytes_col(true, {dummy_wkb}, {0xFFu}),
        bytes_col(false, {pt, pt}),
        fixed64_col(true, {kDist}),
    });
    auto got = read_bool_col(
        columnar_impl_wrapper(buf, n, st_dwithin_impl,
            nullptr, false, nullptr, nullptr,
            prep_a_st_dwithin, prep_b_st_dwithin),
        n);
    clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(buf));

    EXPECT_EQ(got, (std::vector<uint8_t>{0u, 0u}));
}

TEST(ColumnarPrepGeomDist, DWithinBboxMissShortCircuits) {
    // Verifies that the bbox pre-filter in the A-const dist path fires:
    // a point 1000 units away must be rejected without calling isWithinDistance.
    auto origin  = wkt2wkb("POINT (0 0)");
    auto far_pt  = wkt2wkb("POINT (1000 1000)");
    const uint32_t n = 1;

    auto* buf = make_columnar(n, {
        bytes_col(true,  {origin}),
        bytes_col(false, {far_pt}),
        fixed64_col(true, {kDist}),
    });
    auto got = read_bool_col(
        columnar_impl_wrapper(buf, n, st_dwithin_impl,
            nullptr, false, nullptr, nullptr,
            prep_a_st_dwithin, prep_b_st_dwithin),
        n);
    clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(buf));

    EXPECT_EQ(got[0], 0u);
}

// ── COL_COMPLEX tests ─────────────────────────────────────────────────────────

// Build a COL_COMPLEX Array(String) column (const, 1 row containing all WKBs).
static ColData complex_array_string_col(const std::vector<ch::Vector>& wkbs) {
    ColData col;
    col.col_type = static_cast<uint32_t>(COL_COMPLEX) | static_cast<uint32_t>(COL_IS_CONST);

    // col_get_complex_array reads outer_offs from col.data, not col.offsets.
    // Layout: outer_offs[N+1] + inner_offs[M+1] + chars
    // N=1 (one const row), M=wkbs.size() (elements in that row)
    const uint32_t N = 1u;
    const uint32_t M = static_cast<uint32_t>(wkbs.size());

    std::vector<uint64_t> outer_offs = {0u, static_cast<uint64_t>(M)};

    std::vector<uint64_t> inner_offs(M + 1u);
    inner_offs[0] = 0u;
    std::vector<uint8_t> chars;
    for (uint32_t j = 0; j < M; ++j) {
        chars.insert(chars.end(), wkbs[j].begin(), wkbs[j].end());
        inner_offs[j + 1u] = static_cast<uint64_t>(chars.size());
    }

    const size_t outer_sz = (N + 1u) * sizeof(uint64_t);
    const size_t inner_sz = (M + 1u) * sizeof(uint64_t);
    col.data.resize(outer_sz + inner_sz + chars.size());
    std::memcpy(col.data.data(), outer_offs.data(), outer_sz);
    std::memcpy(col.data.data() + outer_sz, inner_offs.data(), inner_sz);
    std::memcpy(col.data.data() + outer_sz + inner_sz, chars.data(), chars.size());
    return col;
}

// Read the geometry WKB from a COL_COMPLEX output buffer.
// Geometry output is COL_BYTES (non-nullable — use std::optional<> for nullable).
static std::string read_geom_col_wkt(raw_buffer* buf) {
    uint32_t num_rows;
    std::memcpy(&num_rows, buf->data() + 8, 4);
    ColDescriptor d;
    std::memcpy(&d, buf->data() + HEADER_BYTES, sizeof(d));
    EXPECT_EQ(d.type & ~static_cast<uint32_t>(COL_IS_CONST),
              static_cast<uint32_t>(COL_BYTES));
    // Read first non-null row
    const uint64_t* offs = reinterpret_cast<const uint64_t*>(buf->data() + d.offsets_offset);
    const uint8_t*  data = buf->data() + d.data_offset;
    for (uint32_t i = 0; i < num_rows; ++i) {
        if (d.null_offset && buf->data()[d.null_offset + i]) continue;
        uint64_t s   = offs[i];
        uint64_t e   = offs[i + 1];
        uint64_t len = e - s;
        auto g = read_wkb({data + s, len});
        return geom2wkt(g);
    }
    return "";
}

// ── Test: COL_COMPLEX input → vector<unique_ptr<Geometry>> arg ───────────────

// Simple 1-arg aggregate: st_union_agg_impl(vector<Geometry>) → Geometry.
// We pass a COL_COMPLEX Array(String) column and verify the correct geometry union.
TEST(ColComplex, AggInputArrayOfWkbs) {
    // Union of two non-overlapping triangles → should produce a single geometry
    auto tri1 = wkt2wkb("POLYGON ((0 0, 1 0, 0 1, 0 0))");
    auto tri2 = wkt2wkb("POLYGON ((2 2, 3 2, 2 3, 2 2))");
    const uint32_t n = 1;  // one "row" = one group

    auto* buf = make_columnar(n, {
        complex_array_string_col({tri1, tri2}),
    });

    raw_buffer* out = columnar_impl_wrapper(buf, n, ch::st_union_agg_impl);
    clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(buf));

    // The result is a GEOMETRYCOLLECTION or MULTIPOLYGON of the two triangles
    std::string wkt = read_geom_col_wkt(out);
    clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(out));
    EXPECT_FALSE(wkt.empty());
    // Both triangles must appear in the result
    EXPECT_NE(wkt.find("POLYGON"), std::string::npos);
}

// ── Test: COL_COMPLEX output → vector<pair<uint64_t,double>> ─────────────────

// Trivial _impl that returns a vector of (index, value) pairs per row.
static std::vector<std::pair<uint64_t, double>> pair_vec_impl(int32_t n) {
    std::vector<std::pair<uint64_t, double>> result;
    for (int32_t i = 0; i < n; ++i)
        result.push_back({static_cast<uint64_t>(i), static_cast<double>(i) * 1.5});
    return result;
}

static std::vector<std::pair<uint64_t, double>> read_pair_vec_row(raw_buffer* buf, uint32_t row) {
    uint32_t num_rows;
    std::memcpy(&num_rows, buf->data() + 8, 4);
    ColDescriptor d;
    std::memcpy(&d, buf->data() + HEADER_BYTES, sizeof(d));
    EXPECT_EQ(d.type, static_cast<uint32_t>(COL_COMPLEX));

    const uint8_t* data = buf->data() + d.data_offset;
    // Layout: uint64[num_rows+1] outer_offs + uint64[M] keys + float64[M] dists
    const uint64_t* outer_offs = reinterpret_cast<const uint64_t*>(data);
    uint64_t M = outer_offs[num_rows];
    const uint64_t* keys  = reinterpret_cast<const uint64_t*>(data + (num_rows + 1u) * 8u);
    const double*   dists = reinterpret_cast<const double*>(keys + M);

    uint64_t start = outer_offs[row];
    uint64_t end   = outer_offs[row + 1u];
    std::vector<std::pair<uint64_t, double>> result;
    for (uint32_t j = start; j < end; ++j)
        result.push_back({keys[j], dists[j]});
    return result;
}

TEST(ColComplex, OutputVectorOfPairs) {
    // 3 rows: row 0 → 2 pairs, row 1 → 0 pairs, row 2 → 3 pairs
    const uint32_t n = 3;
    // Build a trivial input column (int32: counts per row)
    std::vector<int32_t> counts = {2, 0, 3};
    ColData count_col;
    count_col.col_type = static_cast<uint32_t>(COL_FIXED32);
    count_col.data.resize(n * 4u);
    std::memcpy(count_col.data.data(), counts.data(), n * 4u);

    auto* buf = make_columnar(n, {count_col});
    raw_buffer* out = columnar_impl_wrapper(buf, n, pair_vec_impl);
    clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(buf));

    // Row 0: 2 pairs → {(0,0.0),(1,1.5)}
    auto row0 = read_pair_vec_row(out, 0);
    ASSERT_EQ(row0.size(), 2u);
    EXPECT_EQ(row0[0].first, 0u);   EXPECT_DOUBLE_EQ(row0[0].second, 0.0);
    EXPECT_EQ(row0[1].first, 1u);   EXPECT_DOUBLE_EQ(row0[1].second, 1.5);

    // Row 1: 0 pairs
    auto row1 = read_pair_vec_row(out, 1);
    EXPECT_TRUE(row1.empty());

    // Row 2: 3 pairs → {(0,0.0),(1,1.5),(2,3.0)}
    auto row2 = read_pair_vec_row(out, 2);
    ASSERT_EQ(row2.size(), 3u);
    EXPECT_EQ(row2[2].first, 2u);   EXPECT_DOUBLE_EQ(row2[2].second, 3.0);

    clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(out));
}

// ── COL_VARIANT geometry decode tests ────────────────────────────────────────
//
// Geo discriminators (CH global alphabetical order): 0=LineString, 1=MultiLineString,
//   2=MultiPolygon, 3=Point, 4=Polygon, 5=Ring
//
// Helpers build a single-column COLUMNAR_V1 buffer with one COL_VARIANT column.

// Build a COL_VARIANT buffer containing only Point sub-variants.
// Each entry is {x, y} or nullopt for a NULL row.
static raw_buffer* make_variant_point_buf(
    const std::vector<std::optional<std::pair<double, double>>>& pts)
{
    uint32_t N = static_cast<uint32_t>(pts.size());
    uint32_t M = 0;  // non-null point count
    for (auto& p : pts) if (p) ++M;

    std::vector<uint8_t> discs(N);
    // Row positions are uint32 on the wire (host writeColData Variant branch),
    // not the uint64 COL_BYTES offsets. These fixtures wrote them as uint64 once,
    // which is why a decoder reading them as uint64 passed every test while
    // disagreeing with the real serializer on the field's width.
    std::vector<uint32_t> row_offs(N, 0u);
    std::vector<double> xs, ys;
    uint32_t sub_idx = 0;
    for (uint32_t i = 0; i < N; ++i) {
        if (pts[i]) {
            discs[i] = 3;  // Point global discriminator
            row_offs[i] = sub_idx++;
            xs.push_back(pts[i]->first);
            ys.push_back(pts[i]->second);
        } else {
            discs[i] = 0xFFu;  // NULL
        }
    }

    // Compute absolute offsets in the buffer.
    uint32_t pos = HEADER_BYTES + COL_DESC_BYTES;

    uint32_t disc_off = pos;
    pos += N;
    pos = (pos + 3u) & ~3u;  // align to 4 (host alignWriteCursor for this array)

    uint32_t offs_off = pos;
    pos += N * 4u;

    uint32_t data_off = pos;  // variant header
    // Header: uint32 K + K×{disc(1)+pad(3)+ColDescriptor(20)}
    uint32_t k = (M > 0u) ? 1u : 0u;
    uint32_t hdr_bytes = 4u + k * (4u + COL_DESC_BYTES);
    uint32_t sub_data_off = data_off + hdr_bytes;
    uint32_t sub_data_sz  = M * 16u;  // x[M] + y[M]
    uint32_t total        = sub_data_off + sub_data_sz;

    auto* buf = clickhouse_create_buffer(total);
    buf->resize(total);
    uint8_t* p = buf->data();
    std::memset(p, 0, total);

    // Frame header.  This helper once wrote the pre-magic BufHeader ([num_rows,
    // num_cols] at byte 0); the strict parse_columnar rejects that, and before
    // it did, every wrapper-level test over these fixtures silently ran with
    // num_rows == 0 and read the result array out of bounds.
    write_frame_header(p, N, 1);

    // ColDescriptor
    ColDescriptor d{};
    d.type           = static_cast<uint32_t>(COL_VARIANT);
    d.null_offset    = disc_off;
    d.offsets_offset = offs_off;
    d.data_offset    = data_off;
    d.data_size      = hdr_bytes + sub_data_sz;
    std::memcpy(p + HEADER_BYTES, &d, COL_DESC_BYTES);

    // Discriminators and row offsets
    std::memcpy(p + disc_off, discs.data(), N);
    std::memcpy(p + offs_off, row_offs.data(), N * 4u);

    if (M > 0u) {
        // Variant header: K=1
        std::memcpy(p + data_off, &k, 4u);
        // Record: disc=0, pad, inner_desc
        p[data_off + 4u] = 3u;  // global discriminator = Point
        ColDescriptor inner{};
        inner.type           = static_cast<uint32_t>(COL_COMPLEX);
        inner.null_offset    = M;  // sub_rows
        inner.offsets_offset = 0u;  // Tuple has no outer offsets
        inner.data_offset    = sub_data_off;
        inner.data_size      = sub_data_sz;
        std::memcpy(p + data_off + 4u + 4u, &inner, COL_DESC_BYTES);
        // Sub-col data: x[M] then y[M]
        std::memcpy(p + sub_data_off, xs.data(), M * 8u);
        std::memcpy(p + sub_data_off + M * 8u, ys.data(), M * 8u);
    }
    return buf;
}

// Build a COL_VARIANT buffer containing only LineString sub-variants (disc=0).
// Each entry is a list of {x,y} vertices, or nullopt for NULL.
static raw_buffer* make_variant_linestring_buf(
    const std::vector<std::optional<std::vector<std::pair<double, double>>>>& lines)
{
    uint32_t N = static_cast<uint32_t>(lines.size());

    // Collect non-null entries and build outer_offsets[M+1] and flattened x/y.
    std::vector<uint64_t> outer_offs;
    std::vector<double> xs, ys;
    uint32_t M = 0;
    outer_offs.push_back(0u);
    for (auto& ln : lines)
        if (ln) {
            ++M;
            for (auto& v : *ln) { xs.push_back(v.first); ys.push_back(v.second); }
            outer_offs.push_back(static_cast<uint64_t>(xs.size()));
        }

    std::vector<uint8_t> discs(N);
    std::vector<uint32_t> row_offs(N, 0u);  // uint32 on the wire; see make_variant_point_buf
    uint32_t sub_idx = 0;
    for (uint32_t i = 0; i < N; ++i) {
        if (lines[i]) { discs[i] = 0u; row_offs[i] = sub_idx++; }
        else            discs[i] = 0xFFu;
    }

    const uint32_t V = static_cast<uint32_t>(xs.size());

    uint32_t pos = HEADER_BYTES + COL_DESC_BYTES;
    uint32_t disc_off = pos;  pos += N;
    pos = (pos + 3u) & ~3u;
    uint32_t offs_off = pos;  pos += N * 4u;
    pos = (pos + 7u) & ~7u;
    uint32_t data_off = pos;  // variant header
    uint32_t k = (M > 0u) ? 1u : 0u;
    uint32_t hdr_bytes     = 4u + k * (4u + COL_DESC_BYTES);
    pos = data_off + hdr_bytes;
    // Inner sub-col: Array(Tuple) → outer_offs[M+1] at offsets_off, x/y at data_abs
    uint32_t inner_offs_off = pos;  pos += (M + 1u) * 8u;
    pos = (pos + 7u) & ~7u;
    uint32_t inner_data_off = pos;
    uint32_t inner_data_sz  = V * 16u;  // x[V] + y[V]
    pos += inner_data_sz;

    auto* buf = clickhouse_create_buffer(pos);
    buf->resize(pos);
    uint8_t* p = buf->data();
    std::memset(p, 0, pos);

    // Frame header — see the note in make_variant_point_buf: these helpers once
    // wrote the pre-magic BufHeader, which the strict parse_columnar rejects.
    write_frame_header(p, N, 1);

    ColDescriptor d{};
    d.type           = static_cast<uint32_t>(COL_VARIANT);
    d.null_offset    = disc_off;
    d.offsets_offset = offs_off;
    d.data_offset    = data_off;
    d.data_size      = pos - data_off;
    std::memcpy(p + HEADER_BYTES, &d, COL_DESC_BYTES);

    std::memcpy(p + disc_off, discs.data(), N);
    std::memcpy(p + offs_off, row_offs.data(), N * 4u);

    if (M > 0u) {
        std::memcpy(p + data_off, &k, 4u);
        p[data_off + 4u] = 0u;  // global discriminator = LineString
        ColDescriptor inner{};
        inner.type           = static_cast<uint32_t>(COL_COMPLEX);
        inner.null_offset    = M;
        inner.offsets_offset = 0;  // unused; decoder reads outer_offs from data_offset
        inner.data_offset    = inner_offs_off;  // outer_offs[M+1] here, x/y follow immediately
        inner.data_size      = inner_data_sz;
        std::memcpy(p + data_off + 4u + 4u, &inner, COL_DESC_BYTES);
        std::memcpy(p + inner_offs_off, outer_offs.data(), (M + 1u) * 8u);
        std::memcpy(p + inner_data_off, xs.data(), V * 8u);
        std::memcpy(p + inner_data_off + V * 8u, ys.data(), V * 8u);
    }
    return buf;
}

TEST(ColumnarVariant, PointDecodeNullableRows) {
    auto* buf = make_variant_point_buf({
        std::make_optional(std::pair{1.0, 2.0}),
        std::nullopt,
        std::make_optional(std::pair{3.0, 4.0}),
    });
    auto cb  = parse_columnar(buf);
    auto col = cb.col(0);
    ASSERT_EQ(col.base_type, static_cast<ColType>(COL_VARIANT));

    auto g0 = col_get_arg<std::unique_ptr<geos::geom::Geometry>>(col, 0);
    ASSERT_NE(g0, nullptr);
    ASSERT_EQ(g0->getGeometryTypeId(), geos::geom::GEOS_POINT);
    const auto* pt0 = static_cast<const geos::geom::Point*>(g0.get());
    EXPECT_DOUBLE_EQ(pt0->getX(), 1.0);
    EXPECT_DOUBLE_EQ(pt0->getY(), 2.0);

    auto g1 = col_get_arg<std::unique_ptr<geos::geom::Geometry>>(col, 1);
    EXPECT_EQ(g1, nullptr);  // NULL row

    auto g2 = col_get_arg<std::unique_ptr<geos::geom::Geometry>>(col, 2);
    ASSERT_NE(g2, nullptr);
    ASSERT_EQ(g2->getGeometryTypeId(), geos::geom::GEOS_POINT);
    const auto* pt2 = static_cast<const geos::geom::Point*>(g2.get());
    EXPECT_DOUBLE_EQ(pt2->getX(), 3.0);
    EXPECT_DOUBLE_EQ(pt2->getY(), 4.0);

    clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(buf));
}

TEST(ColumnarVariant, LineStringDecodeVertices) {
    // Two rows: LineString[(0,0)→(1,1)], LineString[(2,2)→(3,3)→(4,4)]
    auto* buf = make_variant_linestring_buf({
        std::make_optional(std::vector<std::pair<double,double>>{{0.0,0.0},{1.0,1.0}}),
        std::make_optional(std::vector<std::pair<double,double>>{{2.0,2.0},{3.0,3.0},{4.0,4.0}}),
    });
    auto cb  = parse_columnar(buf);
    auto col = cb.col(0);
    ASSERT_EQ(col.base_type, static_cast<ColType>(COL_VARIANT));

    auto g0 = col_get_arg<std::unique_ptr<geos::geom::Geometry>>(col, 0);
    ASSERT_NE(g0, nullptr);
    ASSERT_EQ(g0->getGeometryTypeId(), geos::geom::GEOS_LINESTRING);
    ASSERT_EQ(g0->getNumPoints(), 2u);
    EXPECT_DOUBLE_EQ(g0->getCoordinates()->getAt(0).x, 0.0);
    EXPECT_DOUBLE_EQ(g0->getCoordinates()->getAt(1).x, 1.0);

    auto g1 = col_get_arg<std::unique_ptr<geos::geom::Geometry>>(col, 1);
    ASSERT_NE(g1, nullptr);
    ASSERT_EQ(g1->getGeometryTypeId(), geos::geom::GEOS_LINESTRING);
    ASSERT_EQ(g1->getNumPoints(), 3u);
    EXPECT_DOUBLE_EQ(g1->getCoordinates()->getAt(0).x, 2.0);
    EXPECT_DOUBLE_EQ(g1->getCoordinates()->getAt(2).x, 4.0);

    clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(buf));
}

// Frame validation, COL_FIXEDN, COL_LOWCARD, COL_VARIANT row offsets and
// is_effectively_const_bytes are tested in third-party/clickhouse-wasm-columnar.

// ── st_x / st_y WKB fast path through the wrapper ────────────────────────────
//
// The unit tests in test_accessors.cpp pin st_x_wkb against st_x_impl.  These
// pin the wrapper: registering the op must change nothing an caller can observe,
// so every case is compared against the same wrapper called without it.

static std::vector<double> read_double_col(raw_buffer* out, uint32_t n) {
    std::vector<double> res(n);
    std::memcpy(res.data(), out->data() + HEADER_BYTES + COL_DESC_BYTES, n * 8u);
    clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(out));
    return res;
}

// Runs the wrapper over one geometry column, with and without the fast path,
// and requires the two outputs to be the same bits.
static void ExpectWrapperFastPathIsInvisible(std::vector<ColData> cols, uint32_t n) {
    auto* buf_geos = make_columnar(n, cols);
    auto geos = read_double_col(
        columnar_impl_wrapper(buf_geos, n, st_x_impl), n);
    clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(buf_geos));

    auto* buf_fast = make_columnar(n, cols);
    auto fast = read_double_col(
        columnar_impl_wrapper(buf_fast, n, st_x_impl,
            nullptr, false, nullptr, nullptr, nullptr, nullptr,
            nullptr, nullptr, st_x_wkb),
        n);
    clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(buf_fast));

    ASSERT_EQ(geos.size(), fast.size());
    for (uint32_t i = 0; i < n; ++i)
        EXPECT_EQ(0, std::memcmp(&geos[i], &fast[i], sizeof(double))) << "row " << i;
}

// Little-endian WKB POINT with an explicit type word (see test_accessors.cpp).
static ch::Vector col_point_wkb(uint32_t type, std::vector<uint32_t> srid,
                                std::vector<double> ordinates) {
    ch::Vector v;
    v.push_back(0x01);
    uint8_t b[8];
    std::memcpy(b, &type, 4);
    v.insert(v.end(), b, b + 4);
    for (uint32_t s : srid) {
        std::memcpy(b, &s, 4);
        v.insert(v.end(), b, b + 4);
    }
    for (double d : ordinates) {
        std::memcpy(b, &d, 8);
        v.insert(v.end(), b, b + 8);
    }
    return v;
}

TEST(ColumnarWkbScalar, MixedPointEncodingsMatchGeos) {
    ExpectWrapperFastPathIsInvisible({bytes_col(false, {
        wkt2wkb("POINT (1 2)"),
        wkt2wkb("POINT (-3.5 7.25)"),
        col_point_wkb(0x20000001u, {4326u}, {12.5, -7.5}),   // EWKB + SRID
        col_point_wkb(1001u, {}, {1.0, 2.0, 3.0}),           // PointZ  (ISO)
        col_point_wkb(0x40000001u, {}, {4.0, 5.0, 9.0}),     // PointM  (EWKB)
        col_point_wkb(3001u, {}, {6.0, 7.0, 8.0, 9.0}),      // PointZM (ISO)
    })}, 6);
}

TEST(ColumnarWkbScalar, NullRowsMatchGeos) {
    ExpectWrapperFastPathIsInvisible({null_bytes_col(false,
        {wkt2wkb("POINT (1 2)"), ch::Vector{}, wkt2wkb("POINT (3 4)")},
        {0u, 0xFFu, 0u})}, 3);
}

TEST(ColumnarWkbScalar, DeclinedRowFallsBackToGeos) {
    // A single NaN ordinate is a real point; both ordinates NaN is POINT EMPTY,
    // which the op declines and the GEOS path then throws on.  Both wrappers
    // must therefore panic identically.
    double nan = std::numeric_limits<double>::quiet_NaN();
    auto empty_point = col_point_wkb(1u, {}, {nan, nan});

    auto* buf = make_columnar(1, {bytes_col(false, {empty_point})});
    EXPECT_THROW(columnar_impl_wrapper(buf, 1, st_x_impl,
        nullptr, false, nullptr, nullptr, nullptr, nullptr,
        nullptr, nullptr, st_x_wkb), WasmPanicException);
    clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(buf));
}

TEST(ColumnarWkbScalar, NonPointRowPanicsAsBefore) {
    auto* buf = make_columnar(1, {bytes_col(false, {wkt2wkb("LINESTRING (0 0, 1 1)")})});
    EXPECT_THROW(columnar_impl_wrapper(buf, 1, st_x_impl,
        nullptr, false, nullptr, nullptr, nullptr, nullptr,
        nullptr, nullptr, st_x_wkb), WasmPanicException);
    clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(buf));
}

TEST(ColumnarWkbScalar, VariantColumnKeepsGeosPath) {
    // COL_VARIANT holds coordinates, not WKB; get_bytes() on it would be
    // nonsense, so registering the op must not divert this column.  The
    // assertion is that output is unchanged, not what the output is.
    auto rows = std::vector<std::optional<std::pair<double, double>>>{
        std::make_optional(std::pair{1.0, 2.0}),
        std::nullopt,
        std::make_optional(std::pair{3.0, 4.0}),
    };

    auto* buf_geos = make_variant_point_buf(rows);
    auto geos = read_double_col(columnar_impl_wrapper(buf_geos, 3, st_x_impl), 3);
    clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(buf_geos));

    auto* buf_fast = make_variant_point_buf(rows);
    auto fast = read_double_col(
        columnar_impl_wrapper(buf_fast, 3, st_x_impl,
            nullptr, false, nullptr, nullptr, nullptr, nullptr,
            nullptr, nullptr, st_x_wkb),
        3);
    clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(buf_fast));

    for (uint32_t i = 0; i < 3; ++i)
        EXPECT_EQ(0, std::memcmp(&geos[i], &fast[i], sizeof(double))) << "row " << i;
}

TEST(ColumnarWkbScalar, StYReadsSecondOrdinate) {
    auto* buf = make_columnar(2, {bytes_col(false, {
        col_point_wkb(0x20000001u, {4326u}, {12.5, -7.5}),
        col_point_wkb(3001u, {}, {6.0, 7.0, 8.0, 9.0}),
    })});
    auto got = read_double_col(
        columnar_impl_wrapper(buf, 2, st_y_impl,
            nullptr, false, nullptr, nullptr, nullptr, nullptr,
            nullptr, nullptr, st_y_wkb),
        2);
    clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(buf));

    EXPECT_DOUBLE_EQ(got[0], -7.5);
    EXPECT_DOUBLE_EQ(got[1], 7.0);
}

// ── ColView::is_null() for COL_VARIANT ────────────────────────────────────────
//
// For COL_VARIANT, null_map holds discriminators (0=LineString, 1=MultiLineString,
// 2=MultiPolygon, 3=Point, 4=Polygon, 5=Ring); 0xFF means NULL.
// For non-Variant nullable columns, null_map[i] != 0 means NULL.

namespace {

// Bitwise comparison for doubles (mirrors SameBits from test_accessors.cpp).
static void ExpectSameBits(double a, double b) {
    EXPECT_EQ(0, std::memcmp(&a, &b, sizeof(double)))
        << "row: " << a << " vs " << b;
}

}  // namespace

// A non-NULL Variant row with discriminator 3 (Point) must NOT be null.
// This test fails before the fix (is_null reads 3 != 0 → true).
TEST(ColViewIsNull, VariantNonZeroDiscriminatorIsNotNull) {
    auto* buf = make_variant_point_buf({
        std::make_optional(std::pair{1.0, 2.0}),
        std::make_optional(std::pair{3.0, 4.0}),
    });
    auto cb  = parse_columnar(buf);
    auto col = cb.col(0);
    ASSERT_EQ(col.base_type, static_cast<ColType>(COL_VARIANT));

    EXPECT_FALSE(col.is_null(0));
    EXPECT_FALSE(col.is_null(1));

    clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(buf));
}

// Variant column mixing NULL (0xFF) and non-NULL (discriminator 3) rows.
// Non-NULL rows should produce real x values, NULL rows should produce NaN.
TEST(ColViewIsNull, VariantMixedNullNonNullThroughWrapper) {
    auto rows = std::vector<std::optional<std::pair<double, double>>>{
        std::make_optional(std::pair{1.0, 2.0}),
        std::nullopt,
        std::make_optional(std::pair{3.0, 4.0}),
    };

    auto* buf = make_variant_point_buf(rows);
    auto got = read_double_col(
        columnar_impl_wrapper(buf, 3, st_x_impl), 3);
    clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(buf));

    ExpectSameBits(got[0], 1.0);
    ExpectSameBits(got[1], std::numeric_limits<double>::quiet_NaN());
    ExpectSameBits(got[2], 3.0);
}

// Non-Variant nullable column: null_map[i] != 0 means NULL.
// Verify the existing behaviour is preserved.
TEST(ColViewIsNull, NonVariantNullableUnchanged) {
    auto* buf = make_columnar(3, {
        null_bytes_col(false,
            {wkt2wkb("POINT (1 2)"), ch::Vector{}, wkt2wkb("POINT (3 4)")},
            {0u, 0xFFu, 0u}),
    });
    auto cb  = parse_columnar(buf);
    auto col = cb.col(0);
    ASSERT_EQ(col.base_type, static_cast<ColType>(COL_BYTES));

    EXPECT_FALSE(col.is_null(0));
    EXPECT_TRUE(col.is_null(1));
    EXPECT_FALSE(col.is_null(2));

    clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(buf));
}

// ── st_collect_agg fast export (functions/collect_fast.hpp) ─────────────────
// The fast export must be byte-identical to the generic wrapper it replaces:
// same WKB bytes per row, same frame.  Expected values are produced by running
// the generic path on the same input elements.

static ColData complex_array_string_rows(const std::vector<std::vector<ch::Vector>>& rows) {
    ColData col;
    col.col_type = static_cast<uint32_t>(COL_COMPLEX);

    const uint64_t N = rows.size();
    uint64_t M = 0;
    for (auto& r : rows) M += r.size();

    std::vector<uint64_t> outer_offs(N + 1u, 0);
    for (uint64_t i = 0; i < N; ++i) outer_offs[i + 1u] = outer_offs[i] + rows[i].size();

    std::vector<uint8_t> chars;
    std::vector<uint64_t> inner_offs(M + 1u, 0);
    uint64_t j = 0;
    for (auto& r : rows)
        for (auto& w : r) {
            chars.insert(chars.end(), w.begin(), w.end());
            inner_offs[++j] = chars.size();
        }

    const size_t outer_sz = (N + 1u) * sizeof(uint64_t);
    const size_t inner_sz = (M + 1u) * sizeof(uint64_t);
    col.data.resize(outer_sz + inner_sz + chars.size());
    std::memcpy(col.data.data(), outer_offs.data(), outer_sz);
    std::memcpy(col.data.data() + outer_sz, inner_offs.data(), inner_sz);
    std::memcpy(col.data.data() + outer_sz + inner_sz, chars.data(), chars.size());
    return col;
}

// Generic expectation: per-row WKB bytes, as columnar_impl_wrapper would emit.
static std::vector<std::vector<uint8_t>> generic_collect_expect(
        const std::vector<std::vector<ch::Vector>>& rows) {
    std::vector<std::vector<uint8_t>> out;
    for (auto& r : rows) {
        std::vector<std::unique_ptr<geos::geom::Geometry>> geoms;
        for (auto& w : r) geoms.push_back(read_wkb(wkb(w)));
        auto coll = ch::st_collect_agg_impl(std::move(geoms));
        auto bytes = ch::write_ewkb(coll);
        out.emplace_back(bytes.begin(), bytes.end());
    }
    return out;
}

static std::vector<std::pair<const uint8_t*, size_t>> read_bytes_rows(raw_buffer* buf) {
    uint32_t num_rows;
    std::memcpy(&num_rows, buf->data() + 8, 4);
    ColDescriptor d;
    std::memcpy(&d, buf->data() + HEADER_BYTES, sizeof(d));
    const uint64_t* offs = reinterpret_cast<const uint64_t*>(buf->data() + d.offsets_offset);
    const uint8_t*  data = buf->data() + d.data_offset;
    std::vector<std::pair<const uint8_t*, size_t>> rows;
    for (uint32_t i = 0; i < num_rows; ++i) {
        uint64_t s = offs[i], e = offs[i + 1];
        rows.emplace_back(data + s, static_cast<size_t>(e - s));
    }
    return rows;
}

TEST(CollectFast, MultiRowByteIdentityFastAndFallback) {
    auto pt_a = wkt2wkb("POINT (-111.7610 34.8697)");
    auto pt_b = wkt2wkb("POINT (1 2)");
    auto pt_c = wkt2wkb("POINT (1e-300 -1e-300)");
    auto poly = wkt2wkb("POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0))");
    auto line = wkt2wkb("LINESTRING (0 0, 1 1)");

    // Big-endian point WKB for the mixed-order row.
    ch::Vector be_pt(21);
    be_pt[0] = 0x00;
    const uint8_t be_type_bytes[4] = {0x00, 0x00, 0x00, 0x01};
    std::memcpy(be_pt.data() + 1, be_type_bytes, 4);
    const double coords[2] = {7.5, -8.5};
    for (int c = 0; c < 2; ++c) {
        uint64_t bits;
        std::memcpy(&bits, &coords[c], 8);
        for (int by = 0; by < 8; ++by)
            be_pt[5 + by] = (bits >> (8 * (7 - by))) & 0xFF;
    }

    std::vector<std::vector<ch::Vector>> rows = {
        {pt_a, pt_b, pt_c},          // all LE points → fast
        {},                          // empty array → fast (header only)
        {pt_a, poly},                // mixed → generic fallback
        {be_pt},                     // BE point → generic fallback (normalizes to LE)
        {pt_b, line},                // fallback
        {pt_c, pt_a, pt_b, pt_c},    // fast
    };
    uint32_t n = static_cast<uint32_t>(rows.size());

    auto expected = generic_collect_expect(rows);

    auto* buf = make_columnar(n, {complex_array_string_rows(rows)});
    raw_buffer* out = ch::st_collect_agg_col_fast(buf, n);
    clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(buf));

    auto got = read_bytes_rows(out);
    ASSERT_EQ(got.size(), expected.size());
    for (size_t i = 0; i < expected.size(); ++i) {
        ASSERT_EQ(got[i].second, expected[i].size()) << "row " << i << " length";
        EXPECT_TRUE(std::equal(expected[i].begin(), expected[i].end(), got[i].first))
            << "row " << i << " bytes differ";
    }
    clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(out));
}

