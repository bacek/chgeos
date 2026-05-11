#include <gtest/gtest.h>
#include <cstring>
#include <vector>

#include <geos/geom/Point.h>

#include "helpers.hpp"
#include "buffers.hpp"
#include "functions/predicates.hpp"

using namespace ch;

// ── Varint helpers ────────────────────────────────────────────────────────────

TEST(BuffersVarint, WriteReadRoundTrip) {
    // Test a range of values that exercise 1-5 byte varints.
    std::vector<uint64_t> vals = {0u, 1u, 127u, 128u, 255u,
                                  256u, 16383u, 16384u,
                                  2097151u, 2097152u,
                                  268435455u, 268435456u};
    for (auto v : vals) {
        raw_buffer* buf = clickhouse_create_buffer(0);
        buf->clear();
        writeVarUInt(v, *buf);

        raw_buffer* out = clickhouse_create_buffer(0);
        out->clear();
        uint64_t decoded;
        EXPECT_TRUE(readVarUInt(buf->data(), buf->data() + buf->size(), decoded));
        out->resize(buf->size());
        std::memcpy(out->data(), buf->data(), buf->size());

        EXPECT_EQ(decoded, v);
        clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(buf));
        clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(out));
    }
}

TEST(BuffersVarint, LengthOfVarUInt) {
    EXPECT_EQ(getLengthOfVarUInt(0), 1);
    EXPECT_EQ(getLengthOfVarUInt(127), 1);
    EXPECT_EQ(getLengthOfVarUInt(128), 2);
    EXPECT_EQ(getLengthOfVarUInt(16383), 2);
    EXPECT_EQ(getLengthOfVarUInt(16384), 3);
    EXPECT_EQ(getLengthOfVarUInt(2097151), 3);
    EXPECT_EQ(getLengthOfVarUInt(2097152), 4);
    EXPECT_EQ(getLengthOfVarUInt(268435455), 4);
    EXPECT_EQ(getLengthOfVarUInt(268435456ULL), 5);
}

// ── BuffersBuf parsing ────────────────────────────────────────────────────────

// Build a Buffers-format raw_buffer from num_rows, num_cols, and per-column data.
static raw_buffer* make_buffers_buf(uint32_t num_rows, uint32_t num_cols,
                                     const std::vector<std::vector<uint8_t>>& col_data) {
     raw_buffer* buf = clickhouse_create_buffer(0);
     buf->clear();
     writeBinaryLE64(static_cast<uint64_t>(num_cols), *buf);
     writeBinaryLE64(static_cast<uint64_t>(num_rows), *buf);
     for (uint32_t i = 0; i < num_cols; ++i) {
         writeBinaryLE64(static_cast<uint64_t>(col_data[i].size()), *buf);
         for (size_t j = 0; j < col_data[i].size(); ++j)
             buf->push_back(col_data[i][j]);
     }
     return buf;
 }

// Helper to get bytes from a parsed BuffersBuf column (BuffersBuf has no get_bytes).
static std::span<const uint8_t> bb_get_col(const BuffersBuf& bb, uint32_t col, uint32_t row) {
    BuffersColView cv;
    cv.data = bb.cols[col].data;
    cv.buffer_size = bb.cols[col].buffer_size;
    cv.row_count = static_cast<uint32_t>(bb.cols[col].buffer_size);
    return cv.get_bytes(row);
}

// Helper to get WKB bytes from a BuffersBuf column (using read_wkb).
static std::unique_ptr<ch::Geometry> bb_get_geom(const BuffersBuf& bb, uint32_t col, uint32_t row) {
    auto bytes = bb_get_col(bb, col, row);
    return ch::read_wkb(bytes);
}

TEST(BuffersParse, SingleCol) {
    // num_rows=3, num_cols=1, col[0] = 3 bytes
    std::vector<uint8_t> data = {0x01, 0x02, 0x03};
    auto* buf = make_buffers_buf(3, 1, {data});

    auto bb = parse_buffers(buf);
    EXPECT_EQ(bb.num_rows, 3u);
    EXPECT_EQ(bb.num_cols, 1u);
    EXPECT_EQ(bb.cols.size(), 1u);
    EXPECT_EQ(bb.cols[0].buffer_size, 3u);
    EXPECT_EQ(bb.cols[0].data[0], 0x01);
    EXPECT_EQ(bb.cols[0].data[1], 0x02);
    EXPECT_EQ(bb.cols[0].data[2], 0x03);

    clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(buf));
}

TEST(BuffersParse, MultiCol) {
    // 2 rows, 3 columns
    std::vector<uint8_t> col0 = {0xAA, 0xBB};
    std::vector<uint8_t> col1 = {0x01, 0x02, 0x03};
    std::vector<uint8_t> col2 = {0xFF};
    auto* buf = make_buffers_buf(2, 3, {col0, col1, col2});

    auto bb = parse_buffers(buf);
    EXPECT_EQ(bb.num_rows, 2u);
    EXPECT_EQ(bb.num_cols, 3u);
    EXPECT_EQ(bb.cols.size(), 3u);
    EXPECT_EQ(bb.cols[0].buffer_size, 2u);
    EXPECT_EQ(bb.cols[1].buffer_size, 3u);
    EXPECT_EQ(bb.cols[2].buffer_size, 1u);

    clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(buf));
}

TEST(BuffersParse, LargeVarint) {
    // buffer_size = 0x40000 (3 bytes varint)
    std::vector<uint8_t> data(0x40000, 0xAB);
    auto* buf = make_buffers_buf(1, 1, {data});

    auto bb = parse_buffers(buf);
    EXPECT_EQ(bb.num_rows, 1u);
    EXPECT_EQ(bb.num_cols, 1u);
    EXPECT_EQ(bb.cols[0].buffer_size, 0x40000u);

    clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(buf));
}

// ── BuffersColView ────────────────────────────────────────────────────────────

// Build a column buffer containing N varint-length strings (each: varint(len)+bytes).
static std::vector<uint8_t> make_strings_col(const std::vector<std::string>& strs) {
    std::vector<uint8_t> col;
    for (auto& s : strs) {
        uint32_t len = static_cast<uint32_t>(s.size());
        raw_buffer* vb = clickhouse_create_buffer(0);
        vb->clear();
        writeVarUInt(len, *vb);
        for (uint32_t i = 0; i < len; ++i)
            vb->push_back(static_cast<uint8_t>(s[i]));
        col.insert(col.end(), vb->data(), vb->data() + vb->size());
        clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(vb));
    }
    return col;
}

TEST(BuffersColView, GetBytes) {
    auto col_data = make_strings_col({"hello", "world", "x"});
    BuffersColView cv;
    cv.data = col_data.data();
    cv.buffer_size = col_data.size();
    cv.row_count = 3;

    auto s0 = cv.get_bytes(0);
    EXPECT_EQ(s0.size(), 5u);
    EXPECT_EQ(std::string(s0.begin(), s0.end()), "hello");

    auto s1 = cv.get_bytes(1);
    EXPECT_EQ(s1.size(), 5u);
    EXPECT_EQ(std::string(s1.begin(), s1.end()), "world");

    auto s2 = cv.get_bytes(2);
    EXPECT_EQ(s2.size(), 1u);
    EXPECT_EQ(s2[0], 'x');
}

TEST(BuffersColView, GetBytesEmptyString) {
    auto col_data = make_strings_col({"", "a", ""});
    BuffersColView cv;
    cv.data = col_data.data();
    cv.buffer_size = col_data.size();
    cv.row_count = 3;

    auto s0 = cv.get_bytes(0);
    EXPECT_EQ(s0.size(), 0u);

    auto s2 = cv.get_bytes(2);
    EXPECT_EQ(s2.size(), 0u);
}

TEST(BuffersColView, IsEffectivelyConst_True) {
    auto col_data = make_strings_col({"same", "same", "same"});
    BuffersColView cv;
    cv.data = col_data.data();
    cv.buffer_size = col_data.size();
    cv.row_count = 3;
    EXPECT_TRUE(cv.is_effectively_const());
}

TEST(BuffersColView, IsEffectivelyConst_False) {
    auto col_data = make_strings_col({"same", "diff", "same"});
    BuffersColView cv;
    cv.data = col_data.data();
    cv.buffer_size = col_data.size();
    cv.row_count = 3;
    EXPECT_FALSE(cv.is_effectively_const());
}

TEST(BuffersColView, IsEffectivelyConst_SingleRow) {
    auto col_data = make_strings_col({"only"});
    BuffersColView cv;
    cv.data = col_data.data();
    cv.buffer_size = col_data.size();
    cv.row_count = 1;
    EXPECT_TRUE(cv.is_effectively_const());  // single row is always const
}

TEST(BuffersColView, IsEffectivelyConst_ZeroRows) {
    std::vector<uint8_t> col_data;
    BuffersColView cv;
    cv.data = col_data.data();
    cv.buffer_size = 0;
    cv.row_count = 0;
    EXPECT_TRUE(cv.is_effectively_const());  // 0 rows ≤ 1
}

// ── BuffersOut output collector ───────────────────────────────────────────────

TEST(BuffersOut, RoundTripBool) {
    const uint32_t n = 3;
    BuffersOut out_buf;
    out_buf.init(1);
    bool vals_bool[] = {true, false, true};
    for (auto v : vals_bool)
        out_buf.push_fixed(reinterpret_cast<const uint8_t*>(&v), sizeof(bool));

    auto* result = out_buf.finalize(n, 1);
    // Result layout: varint(num_rows) | varint(1 col) | varint(data_size) | data
    auto bb = parse_buffers(result);
    EXPECT_EQ(bb.num_rows, n);
    EXPECT_EQ(bb.num_cols, 1u);
    EXPECT_EQ(bb.cols[0].buffer_size, n);
    EXPECT_EQ(bb.cols[0].data[0], 1u);
    EXPECT_EQ(bb.cols[0].data[1], 0u);
    EXPECT_EQ(bb.cols[0].data[2], 1u);

    clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(result));
}

TEST(BuffersOut, RoundTripDouble) {
    const uint32_t n = 2;
    BuffersOut out_buf;
    out_buf.init(1);
    double vals[] = {1.5, 2.5};
    for (auto v : vals)
        out_buf.push_fixed(reinterpret_cast<const uint8_t*>(&v), sizeof(double));

    auto* result = out_buf.finalize(n, 1);
    auto bb = parse_buffers(result);
    EXPECT_EQ(bb.cols[0].buffer_size, 16u);  // 2 * sizeof(double)

    double r0, r1;
    std::memcpy(&r0, bb.cols[0].data, 8);
    std::memcpy(&r1, bb.cols[0].data + 8, 8);
    EXPECT_DOUBLE_EQ(r0, 1.5);
    EXPECT_DOUBLE_EQ(r1, 2.5);

    clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(result));
}

TEST(BuffersOut, RoundTripInt32) {
    const uint32_t n = 2;
    BuffersOut out_buf;
    out_buf.init(1);
    int32_t vals[] = {42, -7};
    for (auto v : vals)
        out_buf.push_fixed(reinterpret_cast<const uint8_t*>(&v), sizeof(int32_t));

    auto* result = out_buf.finalize(n, 1);
    auto bb = parse_buffers(result);
    EXPECT_EQ(bb.cols[0].buffer_size, 8u);

    int32_t r0, r1;
    std::memcpy(&r0, bb.cols[0].data, 4);
    std::memcpy(&r1, bb.cols[0].data + 4, 4);
    EXPECT_EQ(r0, 42);
    EXPECT_EQ(r1, -7);

    clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(result));
}

TEST(BuffersOut, RoundTripString) {
     const uint32_t n = 2;
     BuffersOut out_buf;
     out_buf.init(1);
     out_buf.push_string("hello");
     out_buf.push_string("world");

     auto* result = out_buf.finalize(n, 1);
     auto bb = parse_buffers(result);
     // First string: varint(5) + "hello" (Native format, no null terminator)
     auto s0 = std::string(bb.cols[0].data, bb.cols[0].data + bb.cols[0].buffer_size);
     EXPECT_EQ(s0[0], 5u);  // varint len
     EXPECT_EQ(s0[1], 'h');
     EXPECT_EQ(s0[2], 'e');
     EXPECT_EQ(s0[3], 'l');
     EXPECT_EQ(s0[4], 'l');
     EXPECT_EQ(s0[5], 'o');

     clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(result));
 }

 TEST(BuffersOut, RoundTripNullString) {
     const uint32_t n = 2;
     BuffersOut out_buf;
     out_buf.init(1);
     out_buf.push_string("hello");
     out_buf.push_native_string_null();

     auto* result = out_buf.finalize(n, 1);
     auto bb = parse_buffers(result);
     // Second string is null: varint(0) only (Native format)
     const uint8_t* p = bb.cols[0].data;
     const uint8_t* end = p + bb.cols[0].buffer_size;
     // Skip first string: varint(5) + "hello"
     uint64_t len;
     readVarUInt(p, end, len);
     p += getLengthOfVarUInt(static_cast<uint32_t>(len)) + static_cast<uint32_t>(len);
     // Read null string: varint(0)
     EXPECT_TRUE(readVarUInt(p, end, len));
     EXPECT_EQ(len, 0u);

     clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(result));
 }

TEST(BuffersOut, RoundTripGeometry) {
    const uint32_t n = 2;
    BuffersOut out_buf;
    out_buf.init(1);

    auto g1 = geom("POINT (0 0)");
    auto g2 = geom("POINT (1 1)");
    out_buf.push_geometry(std::move(g1));
    out_buf.push_geometry(std::move(g2));

    auto* result = out_buf.finalize(n, 1);
    auto bb = parse_buffers(result);
    // Each geometry is varint(len) + wkb bytes + null
    auto s0 = bb_get_col(bb, 0, 0);
    auto s1 = bb_get_col(bb, 0, 1);
    EXPECT_FALSE(s0.empty());
    EXPECT_FALSE(s1.empty());

    auto r0 = read_wkb(s0);
    auto r1 = read_wkb(s1);
    ASSERT_NE(r0, nullptr);
    ASSERT_NE(r1, nullptr);
    EXPECT_EQ(r0->getGeometryTypeId(), geos::geom::GEOS_POINT);
    EXPECT_EQ(r1->getGeometryTypeId(), geos::geom::GEOS_POINT);

    clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(result));
}

TEST(BuffersOut, RoundTripGeometryNull) {
    const uint32_t n = 2;
    BuffersOut out_buf;
    out_buf.init(1);
    out_buf.push_geometry(nullptr);
    out_buf.push_geometry(geom("POINT (0 0)"));

    auto* result = out_buf.finalize(n, 1);
    auto bb = parse_buffers(result);
    auto s0 = bb_get_col(bb, 0, 0);
    auto s1 = bb_get_col(bb, 0, 1);
    // Null geometry: get_bytes returns empty span (varint(0) + null byte stripped)
    EXPECT_TRUE(s0.empty());

    // Non-null: has WKB
    EXPECT_FALSE(s1.empty());

    clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(result));
}

TEST(BuffersOut, MultiCol) {
    const uint32_t n = 3;
    BuffersOut out_buf;
    out_buf.init(2);

    // Col 0: bools
    out_buf.set_col(0);
    bool bvals[] = {true, false, true};
    for (auto v : bvals)
        out_buf.push_fixed(reinterpret_cast<const uint8_t*>(&v), sizeof(bool));

    // Col 1: doubles
    out_buf.set_col(1);
    double vals[] = {1.0, 2.0, 3.0};
    for (auto v : vals)
        out_buf.push_fixed(reinterpret_cast<const uint8_t*>(&v), sizeof(double));

    auto* result = out_buf.finalize(n, 2);
    auto bb = parse_buffers(result);
    EXPECT_EQ(bb.num_rows, n);
    EXPECT_EQ(bb.num_cols, 2u);
    EXPECT_EQ(bb.cols[0].buffer_size, 3u);   // 3 bools
    EXPECT_EQ(bb.cols[1].buffer_size, 24u);  // 3 doubles

    clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(result));
}

// ── buffers_impl_wrapper: bool return with bbox + PreparedGeometry ────────────

// Build a Buffers-format input buffer for geometry predicates.
// Each geometry arg is a column of varint(len)+wkb strings (Native format).
static raw_buffer* make_buffers_geom_buf(uint32_t num_rows,
                                         const std::vector<ch::Vector>& col0_wkbs,
                                         const std::vector<ch::Vector>& col1_wkbs) {
    auto write_col = [&](const std::vector<ch::Vector>& wkbs, std::vector<uint8_t>& out) {
        for (uint32_t i = 0; i < num_rows; ++i) {
            const auto& wkb = wkbs.empty() ? wkbs[0] : wkbs[i % wkbs.size()];
            uint32_t len = static_cast<uint32_t>(wkb.size());
            raw_buffer* vb = clickhouse_create_buffer(0);
            vb->clear();
            writeVarUInt(len, *vb);
            vb->append(wkb.data(), len);
            out.insert(out.end(), vb->data(), vb->data() + vb->size());
            clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(vb));
        }
    };

    std::vector<uint8_t> col0_data, col1_data;
    write_col(col0_wkbs, col0_data);
    write_col(col1_wkbs, col1_data);
    return make_buffers_buf(num_rows, 2, {col0_data, col1_data});
}

// Read bool output from buffers_impl_wrapper result.
static std::vector<uint8_t> read_buffers_bool(raw_buffer* out, uint32_t n) {
    auto bb = parse_buffers(const_cast<const raw_buffer*>(out));
    std::vector<uint8_t> res(n);
    std::memcpy(res.data(), bb.cols[0].data, n);
    clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(out));
    return res;
}

// Geometries used across the buffers predicate tests:
//   inside   POINT (0.5 0.5)  → st_contains(square, pt) = true
//   outside  POINT (2.0 2.0)  → false
//   boundary POINT (0.0 0.0)  → false
static const std::string kSquare = "POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))";

TEST(BuffersImpl, BoolNoOpt_MatchesBaseline) {
    auto poly   = wkt2wkb(kSquare);
    auto pt_in  = wkt2wkb("POINT (0.5 0.5)");
    auto pt_out = wkt2wkb("POINT (2.0 2.0)");
    const uint32_t n = 2;

    // No const columns — no optimization paths fire.
    auto* buf = make_buffers_geom_buf(n, {poly, poly}, {pt_in, pt_out});
    auto got = read_buffers_bool(
        buffers_impl_wrapper(buf, n, st_contains_impl),
        n);

    EXPECT_EQ(got[0], 1u);
    EXPECT_EQ(got[1], 0u);
}

TEST(BuffersImpl, BoolAConst_MatchesBaseline) {
    auto poly   = wkt2wkb(kSquare);
    auto pt_in  = wkt2wkb("POINT (0.5 0.5)");
    auto pt_out = wkt2wkb("POINT (2.0 2.0)");
    auto pt_bnd = wkt2wkb("POINT (0.0 0.0)");
    const uint32_t n = 3;

    // A-const: col[0] has 1 WKB repeated for all rows (ClickHouse expands const cols).
    std::vector<ch::Vector> col0_aconst(n, poly);
    auto* buf_aconst = make_buffers_geom_buf(n, col0_aconst, {pt_in, pt_out, pt_bnd});
    auto got = read_buffers_bool(
        buffers_impl_wrapper(buf_aconst, n, st_contains_impl,
            bbox_op_contains, false, prep_a_st_contains, prep_b_st_contains),
        n);
    clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(buf_aconst));

    EXPECT_EQ(got[0], 1u);
    EXPECT_EQ(got[1], 0u);
    EXPECT_EQ(got[2], 0u);

    // Baseline: non-const A (repeat poly).
    auto* buf_base = make_buffers_geom_buf(n, {poly, poly, poly}, {pt_in, pt_out, pt_bnd});
    auto base = read_buffers_bool(
        buffers_impl_wrapper(buf_base, n, st_contains_impl,
            bbox_op_contains, false, prep_a_st_contains, prep_b_st_contains),
        n);
    clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(buf_base));

    EXPECT_EQ(got, base);
}

TEST(BuffersImpl, BoolBConst_MatchesBaseline) {
    auto pt    = wkt2wkb("POINT (0.5 0.5)");
    auto big   = wkt2wkb("POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))");
    auto small = wkt2wkb("POLYGON ((2 2, 3 2, 3 3, 2 3, 2 2))");
    const uint32_t n = 2;

    // B-const: col[1] has 1 WKB repeated for all rows (ClickHouse expands const cols).
    std::vector<ch::Vector> col1_bconst(n, pt);
    auto* buf_bconst = make_buffers_geom_buf(n, {big, small}, col1_bconst);
    auto got = read_buffers_bool(
        buffers_impl_wrapper(buf_bconst, n, st_contains_impl,
            bbox_op_contains, false, prep_a_st_contains, prep_b_st_contains),
        n);
    clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(buf_bconst));

    EXPECT_EQ(got[0], 1u);
    EXPECT_EQ(got[1], 0u);

    // Baseline: replicate const B.
    auto* buf_base = make_buffers_geom_buf(n, {big, small}, {pt, pt});
    auto base = read_buffers_bool(
        buffers_impl_wrapper(buf_base, n, st_contains_impl,
            bbox_op_contains, false, prep_a_st_contains, prep_b_st_contains),
        n);
    clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(buf_base));

    EXPECT_EQ(got, base);
}

TEST(BuffersImpl, BoolEarlyRet_Disjoint) {
    // early_ret=true: bbox miss → result=1 (true, they are disjoint).
    auto poly   = wkt2wkb(kSquare);
    auto far_pt = wkt2wkb("POINT (100 100)");
    const uint32_t n = 1;

    auto* buf = make_buffers_geom_buf(n, {poly}, {far_pt});
    auto got = read_buffers_bool(
        buffers_impl_wrapper(buf, n, st_disjoint_impl,
            nullptr, true),
        n);
    clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(buf));

    EXPECT_EQ(got[0], 1u);  // disjoint → true
}

TEST(BuffersImpl, BoolBboxShortCircuit) {
    // Bbox pre-filter rejects before GEOS call.
    auto poly   = wkt2wkb(kSquare);
    auto far_pt = wkt2wkb("POINT (100 100)");
    const uint32_t n = 1;

    auto* buf = make_buffers_geom_buf(n, {poly}, {far_pt});
    auto got = read_buffers_bool(
        buffers_impl_wrapper(buf, n, st_contains_impl,
            bbox_op_contains, false),
        n);
    clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(buf));

    EXPECT_EQ(got[0], 0u);  // bbox miss → false
}

TEST(BuffersImpl, BoolAConstBboxShortCircuit) {
    // A-const path with bbox pre-filter: far point rejected before GEOS call.
    auto poly   = wkt2wkb(kSquare);
    auto far_pt = wkt2wkb("POINT (100 100)");
    auto near_pt = wkt2wkb("POINT (0.5 0.5)");
    const uint32_t n = 2;

    auto* buf = make_buffers_geom_buf(n, {poly}, {near_pt, far_pt});
    auto got = read_buffers_bool(
        buffers_impl_wrapper(buf, n, st_contains_impl,
            bbox_op_contains, false, prep_a_st_contains, prep_b_st_contains),
        n);
    clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(buf));

    EXPECT_EQ(got[0], 1u);  // near → contained
    EXPECT_EQ(got[1], 0u);  // far → bbox rejected
}

TEST(BuffersImpl, BoolAConstDist_MatchesBaseline) {
    // st_dwithin with A-const PreparedGeometry.
    auto origin  = wkt2wkb("POINT (0 0)");
    auto near_pt = wkt2wkb("POINT (3 0)");
    auto far_pt  = wkt2wkb("POINT (10 0)");
    constexpr double kDist = 5.0;
    const uint32_t n = 2;

    // Build 3-col buffer: col0=geom A (const), col1=geom B (varies), col2=double dist.
    std::vector<uint8_t> col0_data;
    for (int i = 0; i < n; ++i) {
        auto wkb = origin;
        uint32_t len = static_cast<uint32_t>(wkb.size());
        raw_buffer* vb = clickhouse_create_buffer(0);
        vb->clear();
        writeVarUInt(len, *vb);
        vb->append(wkb.data(), len);
        col0_data.insert(col0_data.end(), vb->data(), vb->data() + vb->size());
        clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(vb));
    }

    std::vector<uint8_t> col1_data;
    for (auto& wkb : {near_pt, far_pt}) {
        uint32_t len = static_cast<uint32_t>(wkb.size());
        raw_buffer* vb = clickhouse_create_buffer(0);
        vb->clear();
        writeVarUInt(len, *vb);
        vb->append(wkb.data(), len);
        col1_data.insert(col1_data.end(), vb->data(), vb->data() + vb->size());
        clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(vb));
    }

    std::vector<uint8_t> col2_data;
    for (int i = 0; i < n; ++i) {
        col2_data.insert(col2_data.end(),
                         reinterpret_cast<const uint8_t*>(&kDist),
                         reinterpret_cast<const uint8_t*>(&kDist) + sizeof(double));
    }

    raw_buffer* buf = make_buffers_buf(n, 3, {col0_data, col1_data, col2_data});
    auto got = read_buffers_bool(
        buffers_impl_wrapper(buf, n, st_dwithin_impl,
            nullptr, false, nullptr, nullptr,
            prep_a_st_dwithin, prep_b_st_dwithin),
        n);
    clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(buf));

    EXPECT_EQ(got[0], 1u);  // dist 3 < 5
    EXPECT_EQ(got[1], 0u);  // dist 10 > 5

    // Baseline: non-const A.
    std::vector<uint8_t> col0_base;
    for (int i = 0; i < n; ++i) {
        auto wkb = origin;
        uint32_t len = static_cast<uint32_t>(wkb.size());
        raw_buffer* vb = clickhouse_create_buffer(0);
        vb->clear();
        writeVarUInt(len, *vb);
        vb->append(wkb.data(), len);
        col0_base.insert(col0_base.end(), vb->data(), vb->data() + vb->size());
        clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(vb));
    }
    raw_buffer* buf_base = make_buffers_buf(n, 3, {col0_base, col1_data, col2_data});
    auto base = read_buffers_bool(
        buffers_impl_wrapper(buf_base, n, st_dwithin_impl,
            nullptr, false, nullptr, nullptr,
            prep_a_st_dwithin, prep_b_st_dwithin),
        n);
    clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(buf_base));

    EXPECT_EQ(got, base);
}

// ── buffers_impl_wrapper: double return ───────────────────────────────────────

// Trivial _impl: returns the single double argument.
static double double_identity_impl(double x) { return x; }

TEST(BuffersImpl, DoubleReturn) {
    // Build 1-col buffer with 3 double values (raw bytes, no prefix).
    std::vector<uint8_t> col_data;
    double vals[] = {1.5, -2.5, 0.0};
    for (auto v : vals) {
        col_data.insert(col_data.end(),
                        reinterpret_cast<const uint8_t*>(&v),
                        reinterpret_cast<const uint8_t*>(&v) + sizeof(double));
    }

    auto* buf = make_buffers_buf(3, 1, {col_data});
    auto* result = buffers_impl_wrapper(buf, 3, double_identity_impl);

    auto bb = parse_buffers(result);
    EXPECT_EQ(bb.num_rows, 3u);
    EXPECT_EQ(bb.num_cols, 1u);
    EXPECT_EQ(bb.cols[0].buffer_size, 24u);  // 3 * 8

    double r0, r1, r2;
    std::memcpy(&r0, bb.cols[0].data, 8);
    std::memcpy(&r1, bb.cols[0].data + 8, 8);
    std::memcpy(&r2, bb.cols[0].data + 16, 8);
    EXPECT_DOUBLE_EQ(r0, 1.5);
    EXPECT_DOUBLE_EQ(r1, -2.5);
    EXPECT_DOUBLE_EQ(r2, 0.0);

    clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(result));
}

// ── buffers_impl_wrapper: geometry return ─────────────────────────────────────

// Trivial _impl: returns a copy of the input geometry.
static std::unique_ptr<geos::geom::Geometry> geom_identity_impl(
    std::unique_ptr<geos::geom::Geometry> g) {
    return g;
}

TEST(BuffersImpl, GeometryReturn) {
    auto wkb1 = wkt2wkb("POINT (1 2)");
    auto wkb2 = wkt2wkb("POINT (3 4)");

    std::vector<uint8_t> col_data;
    std::vector<ch::Vector> wkbs;
    wkbs.push_back(wkb1);
    wkbs.push_back(wkb2);
    for (auto& wkb : wkbs) {
        uint32_t len = static_cast<uint32_t>(wkb.size());
        raw_buffer* vb = clickhouse_create_buffer(0);
        vb->clear();
        writeVarUInt(len, *vb);
        vb->append(wkb.data(), len);
        col_data.insert(col_data.end(), vb->data(), vb->data() + vb->size());
        clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(vb));
    }

    auto* buf = make_buffers_buf(2, 1, {col_data});
    auto* result = buffers_impl_wrapper(buf, 2, geom_identity_impl);

    auto bb = parse_buffers(result);
    auto r0 = bb_get_col(bb, 0, 0);
    auto r1 = bb_get_col(bb, 0, 1);
    auto gr0 = read_wkb(r0);
    auto gr1 = read_wkb(r1);
    ASSERT_NE(gr0, nullptr);
    ASSERT_NE(gr1, nullptr);
    EXPECT_DOUBLE_EQ(static_cast<const geos::geom::Point*>(gr0.get())->getX(), 1.0);
    EXPECT_DOUBLE_EQ(static_cast<const geos::geom::Point*>(gr1.get())->getY(), 4.0);

    clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(result));
}

// ── buffers_impl_wrapper: string return ───────────────────────────────────────

// Trivial _impl: returns "ok" for every row.
static std::string string_ok_impl() { return "ok"; }

TEST(BuffersImpl, StringReturn) {
    // 0-arg function: no input columns, just num_rows.
    auto* buf = make_buffers_buf(3, 0, {});
    auto* result = buffers_impl_wrapper(buf, 3, string_ok_impl);

    auto bb = parse_buffers(result);
    auto s0 = bb_get_col(bb, 0, 0);
    auto s1 = bb_get_col(bb, 0, 1);
    auto s2 = bb_get_col(bb, 0, 2);
    EXPECT_EQ(std::string(s0.begin(), s0.end()), "ok");
    EXPECT_EQ(std::string(s1.begin(), s1.end()), "ok");
    EXPECT_EQ(std::string(s2.begin(), s2.end()), "ok");

    clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(result));
}

// ── buffers_impl_wrapper: int32 return ────────────────────────────────────────

// Trivial _impl: returns the int32 argument.
static int32_t int32_identity_impl(int32_t x) { return x; }

TEST(BuffersImpl, Int32Return) {
    std::vector<uint8_t> col_data;
    int32_t vals[] = {100, -200, 0};
    for (auto v : vals) {
        col_data.insert(col_data.end(),
                        reinterpret_cast<const uint8_t*>(&v),
                        reinterpret_cast<const uint8_t*>(&v) + sizeof(int32_t));
    }

    auto* buf = make_buffers_buf(3, 1, {col_data});
    auto* result = buffers_impl_wrapper(buf, 3, int32_identity_impl);

    auto bb = parse_buffers(result);
    EXPECT_EQ(bb.cols[0].buffer_size, 12u);

    int32_t r0, r1, r2;
    std::memcpy(&r0, bb.cols[0].data, 4);
    std::memcpy(&r1, bb.cols[0].data + 4, 4);
    std::memcpy(&r2, bb.cols[0].data + 8, 4);
    EXPECT_EQ(r0, 100);
    EXPECT_EQ(r1, -200);
    EXPECT_EQ(r2, 0);

    clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(result));
}

// ── buf_get_arg ───────────────────────────────────────────────────────────────

TEST(BuffersArg, GetStringView) {
    auto col_data = make_strings_col({"hello", "world"});
    BuffersColView cv;
    cv.data = col_data.data();
    cv.buffer_size = col_data.size();
    cv.row_count = 2;

    auto s0 = buf_get_arg<std::string_view>(cv, 0);
    EXPECT_EQ(std::string(s0), "hello");
    auto s1 = buf_get_arg<std::string_view>(cv, 1);
    EXPECT_EQ(std::string(s1), "world");
}

TEST(BuffersArg, GetDouble) {
    std::vector<uint8_t> data;
    double vals[] = {3.14, 2.71};
    data.insert(data.end(), reinterpret_cast<const uint8_t*>(vals),
                reinterpret_cast<const uint8_t*>(vals) + 2 * sizeof(double));
    BuffersColView cv;
    cv.data = data.data();
    cv.buffer_size = data.size();
    cv.row_count = 2;

    EXPECT_DOUBLE_EQ(buf_get_arg<double>(cv, 0), 3.14);
    EXPECT_DOUBLE_EQ(buf_get_arg<double>(cv, 1), 2.71);
}

TEST(BuffersArg, GetInt32) {
    std::vector<uint8_t> data;
    int32_t vals[] = {42, -1};
    data.insert(data.end(), reinterpret_cast<const uint8_t*>(vals),
                reinterpret_cast<const uint8_t*>(vals) + 2 * sizeof(int32_t));
    BuffersColView cv;
    cv.data = data.data();
    cv.buffer_size = data.size();
    cv.row_count = 2;

    EXPECT_EQ(buf_get_arg<int32_t>(cv, 0), 42);
    EXPECT_EQ(buf_get_arg<int32_t>(cv, 1), -1);
}

TEST(BuffersArg, GetGeometry) {
    auto g = geom("POINT (5 6)");
    auto wkb = write_ewkb(g);
    auto col_data = make_strings_col({std::string(wkb.begin(), wkb.end())});
    BuffersColView cv;
    cv.data = col_data.data();
    cv.buffer_size = col_data.size();
    cv.row_count = 1;

    auto result = buf_get_arg<std::unique_ptr<geos::geom::Geometry>>(cv, 0);
    ASSERT_NE(result, nullptr);
    EXPECT_EQ(result->getGeometryTypeId(), geos::geom::GEOS_POINT);
    EXPECT_DOUBLE_EQ(static_cast<const geos::geom::Point*>(result.get())->getX(), 5.0);
    EXPECT_DOUBLE_EQ(static_cast<const geos::geom::Point*>(result.get())->getY(), 6.0);
}

TEST(BuffersImpl, BConstTrace) {
    auto pt    = wkt2wkb("POINT (0.5 0.5)");
    auto big   = wkt2wkb("POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))");
    auto small = wkt2wkb("POLYGON ((2 2, 3 2, 3 3, 2 3, 2 2))");
    const uint32_t n = 2;

    std::cout << "big size=" << big.size() << " small size=" << small.size() << std::endl;
    std::cout << "big==small: " << (big == small) << std::endl;
    std::cout << "memcmp: " << std::memcmp(big.data(), small.data(), big.size()) << std::endl;
    
    std::vector<ch::Vector> col1_bconst(n, pt);
    auto* buf_bconst = make_buffers_geom_buf(n, {big, small}, col1_bconst);
    
    auto bb = parse_buffers(buf_bconst);
    BuffersColView col0, col1;
    col0.data = bb.cols[0].data;
    col0.buffer_size = bb.cols[0].buffer_size;
    col0.row_count = n;
    col1.data = bb.cols[1].data;
    col1.buffer_size = bb.cols[1].buffer_size;
    col1.row_count = n;
    
    std::cout << "col0 is_effectively_const=" << col0.is_effectively_const() << std::endl;
    std::cout << "col1 is_effectively_const=" << col1.is_effectively_const() << std::endl;
    std::cout << "col0 WKB0 size=" << col0.get_bytes(0).size()
              << " WKB1 size=" << col0.get_bytes(1).size() << std::endl;
    std::cout << "col1 WKB0 size=" << col1.get_bytes(0).size() << std::endl;
    
    BBox bbox_a0 = wkb_bbox(col0.get_bytes(0));
    BBox bbox_a1 = wkb_bbox(col0.get_bytes(1));
    BBox bbox_b  = wkb_bbox(col1.get_bytes(0));
    
    std::cout << "bbox_a0: (" << bbox_a0.xmin << "," << bbox_a0.ymin << ")-(" 
              << bbox_a0.xmax << "," << bbox_a0.ymax << ")" << std::endl;
    std::cout << "bbox_a1: (" << bbox_a1.xmin << "," << bbox_a1.ymin << ")-(" 
              << bbox_a1.xmax << "," << bbox_a1.ymax << ")" << std::endl;
    std::cout << "bbox_b:  (" << bbox_b.xmin << "," << bbox_b.ymin << ")-(" 
              << bbox_b.xmax << "," << bbox_b.ymax << ")" << std::endl;
    std::cout << "bbox_op_contains(a0, b) = " << bbox_op_contains(bbox_a0, bbox_b) << std::endl;
    std::cout << "bbox_op_contains(a1, b) = " << bbox_op_contains(bbox_a1, bbox_b) << std::endl;
    
    auto* result = buffers_impl_wrapper(buf_bconst, n, st_contains_impl,
        bbox_op_contains, false, prep_a_st_contains, prep_b_st_contains);
    
    auto bb_out = parse_buffers(result);
    std::cout << "got[0]=" << (int)bb_out.cols[0].data[0]
              << " got[1]=" << (int)bb_out.cols[0].data[1] << std::endl;
    
    clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(buf_bconst));
    clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(result));
}
