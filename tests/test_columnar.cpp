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

// ── COL_COMPLEX tests ─────────────────────────────────────────────────────────

// Build a COL_COMPLEX Array(String) column (const, 1 row containing all WKBs).
static ColData complex_array_string_col(const std::vector<ch::Vector>& wkbs) {
    ColData col;
    col.col_type = static_cast<uint32_t>(COL_COMPLEX) | static_cast<uint32_t>(COL_IS_CONST);

    // outer_offsets: uint32[2] = {0, M}
    uint32_t M = static_cast<uint32_t>(wkbs.size());
    col.offsets = {0u, M};

    // data = inner_offsets[M+1] + bytes (null-terminated)
    std::vector<uint32_t> inner_offs(M + 1u);
    inner_offs[0] = 0u;
    std::vector<uint8_t> chars;
    for (uint32_t j = 0; j < M; ++j) {
        chars.insert(chars.end(), wkbs[j].begin(), wkbs[j].end());
        chars.push_back(0u);
        inner_offs[j + 1u] = static_cast<uint32_t>(chars.size());
    }
    // Flatten inner_offs + chars into col.data
    col.data.resize((M + 1u) * sizeof(uint32_t) + chars.size());
    std::memcpy(col.data.data(), inner_offs.data(), (M + 1u) * sizeof(uint32_t));
    std::memcpy(col.data.data() + (M + 1u) * sizeof(uint32_t), chars.data(), chars.size());
    return col;
}

// Read the geometry WKB from a COL_COMPLEX output buffer.
// For a Geometry (nullable String) output, just reads the first (and only) non-null WKB.
static std::string read_geom_col_wkt(raw_buffer* buf) {
    // Output is COL_NULL_BYTES (existing Geometry path)
    uint32_t num_rows;
    std::memcpy(&num_rows, buf->data(), 4);
    ColDescriptor d;
    std::memcpy(&d, buf->data() + HEADER_BYTES, sizeof(d));
    EXPECT_EQ(d.type & ~static_cast<uint32_t>(COL_IS_CONST),
              static_cast<uint32_t>(COL_NULL_BYTES));
    // Read first non-null row
    const uint32_t* offs = reinterpret_cast<const uint32_t*>(buf->data() + d.offsets_offset);
    const uint8_t*  data = buf->data() + d.data_offset;
    for (uint32_t i = 0; i < num_rows; ++i) {
        if (d.null_offset && buf->data()[d.null_offset + i]) continue;
        uint32_t s   = offs[i];
        uint32_t e   = offs[i + 1];
        uint32_t len = (e > s + 1u) ? e - s - 1u : 0u;
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
    std::memcpy(&num_rows, buf->data(), 4);
    ColDescriptor d;
    std::memcpy(&d, buf->data() + HEADER_BYTES, sizeof(d));
    EXPECT_EQ(d.type, static_cast<uint32_t>(COL_COMPLEX));

    const uint8_t* data = buf->data() + d.data_offset;
    // Layout: uint32[num_rows+1] outer_offs + uint64[M] keys + float64[M] dists
    const uint32_t* outer_offs = reinterpret_cast<const uint32_t*>(data);
    uint32_t M = outer_offs[num_rows];
    const uint64_t* keys  = reinterpret_cast<const uint64_t*>(data + (num_rows + 1u) * 4u);
    const double*   dists = reinterpret_cast<const double*>(keys + M);

    uint32_t start = outer_offs[row];
    uint32_t end   = outer_offs[row + 1u];
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
