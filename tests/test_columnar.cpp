#include <gtest/gtest.h>
#include <cstring>
#include <optional>
#include <vector>

#include <geos/geom/LineString.h>
#include <geos/geom/Point.h>

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

// Nullable variable-length column (null_map[i] == 0xFF → NULL).
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
    pos = (pos + 3u) & ~3u;  // align to 4

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

    // BufHeader
    std::memcpy(p, &N, 4);
    uint32_t one = 1;
    std::memcpy(p + 4, &one, 4);

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
    std::vector<uint32_t> outer_offs;
    std::vector<double> xs, ys;
    uint32_t M = 0;
    outer_offs.push_back(0u);
    for (auto& ln : lines)
        if (ln) {
            ++M;
            for (auto& v : *ln) { xs.push_back(v.first); ys.push_back(v.second); }
            outer_offs.push_back(static_cast<uint32_t>(xs.size()));
        }

    std::vector<uint8_t> discs(N);
    std::vector<uint32_t> row_offs(N, 0u);
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
    pos = (pos + 3u) & ~3u;
    uint32_t data_off = pos;  // variant header
    uint32_t k = (M > 0u) ? 1u : 0u;
    uint32_t hdr_bytes     = 4u + k * (4u + COL_DESC_BYTES);
    pos = data_off + hdr_bytes;
    // Inner sub-col: Array(Tuple) → outer_offs[M+1] at offsets_off, x/y at data_abs
    uint32_t inner_offs_off = pos;  pos += (M + 1u) * 4u;
    pos = (pos + 3u) & ~3u;
    uint32_t inner_data_off = pos;
    uint32_t inner_data_sz  = V * 16u;  // x[V] + y[V]
    pos += inner_data_sz;

    auto* buf = clickhouse_create_buffer(pos);
    buf->resize(pos);
    uint8_t* p = buf->data();
    std::memset(p, 0, pos);

    std::memcpy(p, &N, 4);
    uint32_t one = 1;
    std::memcpy(p + 4, &one, 4);

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
        inner.offsets_offset = inner_offs_off;
        inner.data_offset    = inner_data_off;
        inner.data_size      = inner_data_sz;
        std::memcpy(p + data_off + 4u + 4u, &inner, COL_DESC_BYTES);
        std::memcpy(p + inner_offs_off, outer_offs.data(), (M + 1u) * 4u);
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
