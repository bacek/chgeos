#include <gtest/gtest.h>
#include <cstring>
#include <vector>

#include "helpers.hpp"
#include "columnar.hpp"
#include "functions/predicates.hpp"

using namespace ch;

// ── Columnar buffer builder ───────────────────────────────────────────────────

struct ColData {
    uint32_t              col_type;   // ColType | COL_IS_CONST
    std::vector<uint8_t>  null_map;   // non-empty → nullable column
    std::vector<uint32_t> offsets;    // non-empty → variable-width column
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
            pos = (pos + 3u) & ~3u;   // 4-byte align
            b.offsets_off = pos;
            pos += static_cast<uint32_t>(col.offsets.size()) * 4u;
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

    std::memcpy(p, &num_rows, 4);
    uint32_t nc = static_cast<uint32_t>(cols.size());
    std::memcpy(p + 4, &nc, 4);

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
            std::memcpy(p + bi[i].offsets_off, cols[i].offsets.data(), cols[i].offsets.size() * 4);
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
        col.offsets.push_back(static_cast<uint32_t>(col.data.size()));
    }
    return col;
}

// Nullable variable-length column (null_map[i] != 0 → NULL).
static ColData null_bytes_col(bool is_const,
                              const std::vector<ch::Vector>& wkbs,
                              const std::vector<uint8_t>& nulls) {
    ColData col;
    col.col_type = static_cast<uint32_t>(COL_NULL_BYTES) | (is_const ? static_cast<uint32_t>(COL_IS_CONST) : 0u);
    col.null_map = nulls;
    col.offsets.push_back(0u);
    for (size_t i = 0; i < wkbs.size(); ++i) {
        if (nulls[i]) {
            col.data.push_back(0u);   // empty string for null row
        } else {
            col.data.insert(col.data.end(), wkbs[i].begin(), wkbs[i].end());
            col.data.push_back(0u);
        }
        col.offsets.push_back(static_cast<uint32_t>(col.data.size()));
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

TEST(ColumnarPrepGeom, ConstColNull_AllResultsZero) {
    // When the const geometry column is NULL, all output rows must be 0.
    auto dummy_wkb = wkt2wkb("POINT (0 0)");
    auto pt        = wkt2wkb("POINT (0.5 0.5)");
    const uint32_t n = 3;

    auto* buf = make_columnar(n, {
        null_bytes_col(true, {dummy_wkb}, {1u}),   // const + null
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
        null_bytes_col(false, {pt_in, pt_dummy, pt_in}, {0u, 1u, 0u}),  // row 1 is NULL
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
        null_bytes_col(true, {dummy_wkb}, {1u}),
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
