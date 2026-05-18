#include <gtest/gtest.h>
#include <cstring>
#include <vector>

#include <geos/geom/Point.h>

#include "helpers.hpp"
#include "col_binary.hpp"
#include "functions/predicates.hpp"

using namespace ch;

// ── Varint helpers ────────────────────────────────────────────────────────────

TEST(ColBinaryVarint, WriteReadRoundTrip) {
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

// ── Helper: build a ColumnBinary-format raw_buffer ────────────────────────────

// Build a ColumnBinary-format raw_buffer from num_rows, num_cols, and per-column data.
// Each column has: flags(u8), data_size(u64LE), data.
static raw_buffer* make_col_binary_buf(uint32_t num_rows, uint32_t num_cols,
                                        const std::vector<std::vector<uint8_t>>& col_data,
                                        uint8_t col_flags = 0) {
     raw_buffer* buf = clickhouse_create_buffer(0);
     buf->clear();
     writeBinaryLE32(num_cols, *buf);
     writeBinaryLE32(num_rows, *buf);
     for (uint32_t i = 0; i < num_cols; ++i) {
         buf->push_back(col_flags);
         uint64_t data_size = static_cast<uint64_t>(col_data[i].size());
         for (uint32_t j = 0; j < 8; ++j)
             buf->push_back(static_cast<uint8_t>(data_size >> (j * 8)));
         for (size_t j = 0; j < col_data[i].size(); ++j)
             buf->push_back(col_data[i][j]);
     }
     return buf;
 }

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

 // ── ColBinaryBuf parsing ──────────────────────────────────────────────────────

TEST(ColBinaryParse, SingleCol) {
    std::vector<uint8_t> data = {0x01, 0x02, 0x03};
    auto* buf = make_col_binary_buf(3, 1, {data});

    auto cb = parse_col_binary(buf);
    EXPECT_EQ(cb.num_rows, 3u);
    EXPECT_EQ(cb.num_cols, 1u);
    EXPECT_EQ(cb.cols.size(), 1u);
    EXPECT_FALSE(cb.cols[0].is_const);
    EXPECT_EQ(cb.cols[0].data_size, 3u);
    EXPECT_EQ(cb.cols[0].data[0], 0x01);
    EXPECT_EQ(cb.cols[0].data[1], 0x02);
    EXPECT_EQ(cb.cols[0].data[2], 0x03);

    clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(buf));
}

TEST(ColBinaryParse, MultiCol) {
    std::vector<uint8_t> col0 = {0xAA, 0xBB};
    std::vector<uint8_t> col1 = {0x01, 0x02, 0x03};
    std::vector<uint8_t> col2 = {0xFF};
    auto* buf = make_col_binary_buf(2, 3, {col0, col1, col2});

    auto cb = parse_col_binary(buf);
    EXPECT_EQ(cb.num_rows, 2u);
    EXPECT_EQ(cb.num_cols, 3u);
    EXPECT_EQ(cb.cols.size(), 3u);
    EXPECT_EQ(cb.cols[0].data_size, 2u);
    EXPECT_EQ(cb.cols[1].data_size, 3u);
    EXPECT_EQ(cb.cols[2].data_size, 1u);

    clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(buf));
}

TEST(ColBinaryParse, ConstFlag) {
    std::vector<uint8_t> data = {0x42};
    auto* buf = make_col_binary_buf(5, 1, {data}, 0x01); // IS_CONST flag

    auto cb = parse_col_binary(buf);
    EXPECT_EQ(cb.num_rows, 5u);
    EXPECT_EQ(cb.num_cols, 1u);
    EXPECT_TRUE(cb.cols[0].is_const);
    EXPECT_EQ(cb.cols[0].data_size, 1u);

    clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(buf));
}

TEST(ColBinaryParse, LargeDataSize) {
    // data_size = 0x10000000 (fits in u64)
    std::vector<uint8_t> data(0x100, 0xAB);
    auto* buf = make_col_binary_buf(1, 1, {data});

    auto cb = parse_col_binary(buf);
    EXPECT_EQ(cb.num_rows, 1u);
    EXPECT_EQ(cb.num_cols, 1u);
    EXPECT_EQ(cb.cols[0].data_size, 0x100ULL);

    clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(buf));
}

// ── ColBinaryReader ───────────────────────────────────────────────────────────

// Helper: get next bytes from a ColBinaryReader.
static std::span<const uint8_t> cb_next(ColBinaryReader& reader) {
    return reader.read_blob();
}

TEST(ColBinaryReader, GetStringView) {
    auto col_data = make_strings_col({"hello", "world"});
    ColBinaryInfo ci;
    ci.is_const = false;
    ci.data = col_data.data();
    ci.data_size = col_data.size();
    ColBinaryReader reader = ColBinaryReader::from_col(ci, 2);

    auto s0 = unpack_arg<std::string_view>(reader);
    EXPECT_EQ(std::string(s0), "hello");
    auto s1 = unpack_arg<std::string_view>(reader);
    EXPECT_EQ(std::string(s1), "world");
}

TEST(ColBinaryReader, GetDouble) {
    std::vector<uint8_t> data;
    double vals[] = {3.14, 2.71};
    data.insert(data.end(), reinterpret_cast<const uint8_t*>(vals),
                reinterpret_cast<const uint8_t*>(vals) + 2 * sizeof(double));
    ColBinaryInfo ci;
    ci.is_const = false;
    ci.data = data.data();
    ci.data_size = data.size();
    ColBinaryReader reader = ColBinaryReader::from_col(ci, 2);

    EXPECT_DOUBLE_EQ(unpack_arg<double>(reader), 3.14);
    EXPECT_DOUBLE_EQ(unpack_arg<double>(reader), 2.71);
}

TEST(ColBinaryReader, GetInt32) {
    std::vector<uint8_t> data;
    int32_t vals[] = {42, -1};
    data.insert(data.end(), reinterpret_cast<const uint8_t*>(vals),
                reinterpret_cast<const uint8_t*>(vals) + 2 * sizeof(int32_t));
    ColBinaryInfo ci;
    ci.is_const = false;
    ci.data = data.data();
    ci.data_size = data.size();
    ColBinaryReader reader = ColBinaryReader::from_col(ci, 2);

    EXPECT_EQ(unpack_arg<int32_t>(reader), 42);
    EXPECT_EQ(unpack_arg<int32_t>(reader), -1);
}

TEST(ColBinaryReader, GetGeometry) {
    auto g = geom("POINT (5 6)");
    auto wkb = write_ewkb(g);
    auto col_data = make_strings_col({std::string(wkb.begin(), wkb.end())});
    ColBinaryInfo ci;
    ci.is_const = false;
    ci.data = col_data.data();
    ci.data_size = col_data.size();
    ColBinaryReader reader = ColBinaryReader::from_col(ci, 1);

    auto result = unpack_arg<std::unique_ptr<geos::geom::Geometry>>(reader);
    ASSERT_NE(result, nullptr);
    EXPECT_EQ(result->getGeometryTypeId(), geos::geom::GEOS_POINT);
    EXPECT_DOUBLE_EQ(static_cast<const geos::geom::Point*>(result.get())->getX(), 5.0);
    EXPECT_DOUBLE_EQ(static_cast<const geos::geom::Point*>(result.get())->getY(), 6.0);
}

TEST(ColBinaryReader, ConstDouble) {
    std::vector<uint8_t> data;
    double val = 3.14;
    data.insert(data.end(), reinterpret_cast<const uint8_t*>(&val),
                reinterpret_cast<const uint8_t*>(&val) + sizeof(double));
    ColBinaryInfo ci;
    ci.is_const = true;
    ci.data = data.data();
    ci.data_size = data.size();
    ColBinaryReader reader = ColBinaryReader::from_col(ci, 100);

    // Should return the same value for every call
    for (int i = 0; i < 100; ++i) {
        EXPECT_DOUBLE_EQ(unpack_arg<double>(reader), 3.14);
    }
}

TEST(ColBinaryReader, ConstGeometry) {
    auto g = geom("POINT (1 2)");
    auto wkb = write_ewkb(g);
    std::vector<uint8_t> col_data;
    {
        // Const String columns use u32[2] header: {o0=0, o1=len} + bytes.
        uint32_t o0 = 0, o1 = static_cast<uint32_t>(wkb.size());
        for (int j = 0; j < 4; ++j) col_data.push_back(static_cast<uint8_t>(o0 >> (j * 8)));
        for (int j = 0; j < 4; ++j) col_data.push_back(static_cast<uint8_t>(o1 >> (j * 8)));
        col_data.insert(col_data.end(), wkb.begin(), wkb.end());
    }
    ColBinaryInfo ci;
    ci.is_const = true;
    ci.data = col_data.data();
    ci.data_size = col_data.size();
    ColBinaryReader reader = ColBinaryReader::from_col(ci, 50);

    for (int i = 0; i < 50; ++i) {
        auto result = unpack_arg<std::unique_ptr<geos::geom::Geometry>>(reader);
        ASSERT_NE(result, nullptr);
        EXPECT_DOUBLE_EQ(static_cast<const geos::geom::Point*>(result.get())->getX(), 1.0);
        EXPECT_DOUBLE_EQ(static_cast<const geos::geom::Point*>(result.get())->getY(), 2.0);
    }
}

// ── col_binary_impl_wrapper: bool return header format ────────────────────────

TEST(ColBinaryImpl, BoolHeaderOrder) {
    const uint32_t n = 5;
    auto poly = wkt2wkb("POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))");
    auto pt   = wkt2wkb("POINT (0.5 0.5)");

    // Build u32 offset format for each column
    auto write_geom_col = [&](const std::vector<ch::Vector>& wkbs, std::vector<uint8_t>& offsets, std::vector<uint8_t>& payload) {
        std::vector<uint32_t> off;
        off.push_back(0);
        for (uint32_t i = 0; i < n; ++i) {
            const auto& wkb = wkbs[i % wkbs.size()];
            payload.insert(payload.end(), wkb.data(), wkb.data() + wkb.size());
            off.push_back(static_cast<uint32_t>(payload.size()));
        }
        for (uint32_t o : off) {
            for (uint32_t j = 0; j < 4; ++j)
                offsets.push_back(static_cast<uint8_t>(o >> (j * 8)));
        }
    };

    std::vector<uint8_t> col0_offsets, col0_payload, col1_offsets, col1_payload;
    std::vector<ch::Vector> polys(n, poly), pts(n, pt);
    write_geom_col(polys, col0_offsets, col0_payload);
    write_geom_col(pts, col1_offsets, col1_payload);

    std::vector<uint8_t> col0_data, col1_data;
    col0_data.insert(col0_data.end(), col0_offsets.begin(), col0_offsets.end());
    col0_data.insert(col0_data.end(), col0_payload.begin(), col0_payload.end());
    col1_data.insert(col1_data.end(), col1_offsets.begin(), col1_offsets.end());
    col1_data.insert(col1_data.end(), col1_payload.begin(), col1_payload.end());

    auto* buf = make_col_binary_buf(n, 2, {col0_data, col1_data});
    auto* result = col_binary_impl_wrapper("st_contains_cb", buf, n, st_contains_impl,
        bbox_op_contains, false, prep_a_st_contains, prep_b_st_contains);
    clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(buf));

    // Parse the output header: num_cols(u32LE) | num_rows(u32LE) | flags(u8) | data_size(u64LE) | data
    const uint8_t* p = result->data();
    const uint8_t* end = result->data() + result->size();
    uint32_t v32;
    uint64_t v64;

    EXPECT_TRUE(readBinaryLE32(p, end, v32));
    EXPECT_EQ(v32, 1u);

    EXPECT_TRUE(readBinaryLE32(p, end, v32));
    EXPECT_EQ(v32, n);

    EXPECT_EQ(*p++, 0u); // flags

    EXPECT_TRUE(readBinaryLE64(p, end, v64));
    EXPECT_EQ(v64, static_cast<uint64_t>(n)); // data_size = n bool bytes

    clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(result));
}

// ── col_binary_impl_wrapper: bool return with bbox + PreparedGeometry ─────────

// Build a ColumnBinary-format input buffer for geometry predicates.
// Uses u32 offset format: u32[N+1] offsets at data_start, raw bytes follow.
static raw_buffer* make_col_binary_geom_buf(uint32_t num_rows,
                                               const std::vector<ch::Vector>& col0_wkbs,
                                               const std::vector<ch::Vector>& col1_wkbs,
                                               uint32_t const_col_idx = UINT32_MAX) {
    auto write_col = [&](const std::vector<ch::Vector>& wkbs, std::vector<uint8_t>& offsets, std::vector<uint8_t>& payload, bool is_const) {
        uint32_t rows = is_const ? 1 : num_rows;
        std::vector<uint32_t> off;
        off.reserve(rows + 1);
        off.push_back(0);
        for (uint32_t i = 0; i < rows; ++i) {
            const auto& wkb = wkbs.empty() ? wkbs[0] : wkbs[i % wkbs.size()];
            payload.insert(payload.end(), wkb.data(), wkb.data() + wkb.size());
            off.push_back(static_cast<uint32_t>(payload.size()));
        }
        for (uint32_t o : off) {
            for (uint32_t j = 0; j < 4; ++j)
                offsets.push_back(static_cast<uint8_t>(o >> (j * 8)));
        }
    };

    std::vector<uint8_t> col0_offsets, col0_payload, col1_offsets, col1_payload;
    write_col(col0_wkbs, col0_offsets, col0_payload, const_col_idx == 0);
    write_col(col1_wkbs, col1_offsets, col1_payload, const_col_idx == 1);

    // Combine offsets + payload for each column
    std::vector<uint8_t> col0_data, col1_data;
    col0_data.insert(col0_data.end(), col0_offsets.begin(), col0_offsets.end());
    col0_data.insert(col0_data.end(), col0_payload.begin(), col0_payload.end());
    col1_data.insert(col1_data.end(), col1_offsets.begin(), col1_offsets.end());
    col1_data.insert(col1_data.end(), col1_payload.begin(), col1_payload.end());

    uint8_t col0_flags = const_col_idx == 0 ? 0x01 : 0;
    uint8_t col1_flags = const_col_idx == 1 ? 0x01 : 0;

    raw_buffer* buf = clickhouse_create_buffer(0);
    buf->clear();
    writeBinaryLE32(2, *buf);
    writeBinaryLE32(num_rows, *buf);

    // Col0: flags + data_size(u64) + data
    buf->push_back(col0_flags);
    uint64_t ds0 = col0_data.size();
    for (uint32_t j = 0; j < 8; ++j) buf->push_back(static_cast<uint8_t>(ds0 >> (j * 8)));
    for (auto b : col0_data) buf->push_back(b);

    // Col1
    buf->push_back(col1_flags);
    uint64_t ds1 = col1_data.size();
    for (uint32_t j = 0; j < 8; ++j) buf->push_back(static_cast<uint8_t>(ds1 >> (j * 8)));
    for (auto b : col1_data) buf->push_back(b);

    return buf;
}

// Read bool output from col_binary_impl_wrapper result.
static std::vector<uint8_t> read_cb_bool(raw_buffer* out, uint32_t n) {
    auto cb = parse_col_binary(out);
    std::vector<uint8_t> res(n);
    std::memcpy(res.data(), cb.cols[0].data, n);
    clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(out));
    return res;
}

static const std::string kSquare = "POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))";

TEST(ColBinaryImpl, BoolNoOpt_MatchesBaseline) {
    auto poly   = wkt2wkb(kSquare);
    auto pt_in  = wkt2wkb("POINT (0.5 0.5)");
    auto pt_out = wkt2wkb("POINT (2.0 2.0)");
    const uint32_t n = 2;

    auto* buf = make_col_binary_geom_buf(n, {poly, poly}, {pt_in, pt_out});
    auto got = read_cb_bool(
        col_binary_impl_wrapper("st_contains_cb", buf, n, st_contains_impl),
        n);
    clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(buf));

    EXPECT_EQ(got[0], 1u);
    EXPECT_EQ(got[1], 0u);
}

TEST(ColBinaryImpl, BoolAConst_MatchesBaseline) {
    auto poly   = wkt2wkb(kSquare);
    auto pt_in  = wkt2wkb("POINT (0.5 0.5)");
    auto pt_out = wkt2wkb("POINT (2.0 2.0)");
    auto pt_bnd = wkt2wkb("POINT (0.0 0.0)");
    const uint32_t n = 3;

    std::vector<ch::Vector> col0_aconst(n, poly);
    auto* buf_aconst = make_col_binary_geom_buf(n, col0_aconst, {pt_in, pt_out, pt_bnd});
    auto got = read_cb_bool(
        col_binary_impl_wrapper("st_contains_aconst_cb", buf_aconst, n, st_contains_impl,
            bbox_op_contains, false, prep_a_st_contains, prep_b_st_contains),
        n);
    clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(buf_aconst));

    EXPECT_EQ(got[0], 1u);
    EXPECT_EQ(got[1], 0u);
    EXPECT_EQ(got[2], 0u);

    auto* buf_base = make_col_binary_geom_buf(n, {poly, poly, poly}, {pt_in, pt_out, pt_bnd});
    auto base = read_cb_bool(
        col_binary_impl_wrapper("st_contains_base_cb", buf_base, n, st_contains_impl,
            bbox_op_contains, false, prep_a_st_contains, prep_b_st_contains),
        n);
    clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(buf_base));

    EXPECT_EQ(got, base);
}

TEST(ColBinaryImpl, BoolBConst_MatchesBaseline) {
    auto pt    = wkt2wkb("POINT (0.5 0.5)");
    auto big   = wkt2wkb("POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))");
    auto small = wkt2wkb("POLYGON ((2 2, 3 2, 3 3, 2 3, 2 2))");
    const uint32_t n = 2;

    std::vector<ch::Vector> col1_bconst(n, pt);
    auto* buf_bconst = make_col_binary_geom_buf(n, {big, small}, col1_bconst);
    auto got = read_cb_bool(
        col_binary_impl_wrapper("st_contains_bconst_cb", buf_bconst, n, st_contains_impl,
            bbox_op_contains, false, prep_a_st_contains, prep_b_st_contains),
        n);
    clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(buf_bconst));

    EXPECT_EQ(got[0], 1u);
    EXPECT_EQ(got[1], 0u);

    auto* buf_base = make_col_binary_geom_buf(n, {big, small}, {pt, pt});
    auto base = read_cb_bool(
        col_binary_impl_wrapper("st_contains_base_cb", buf_base, n, st_contains_impl,
            bbox_op_contains, false, prep_a_st_contains, prep_b_st_contains),
        n);
    clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(buf_base));

    EXPECT_EQ(got, base);
}

TEST(ColBinaryImpl, BoolEarlyRet_Disjoint) {
    auto poly   = wkt2wkb(kSquare);
    auto far_pt = wkt2wkb("POINT (100 100)");
    const uint32_t n = 1;

    auto* buf = make_col_binary_geom_buf(n, {poly}, {far_pt});
    auto got = read_cb_bool(
        col_binary_impl_wrapper("st_disjoint_cb", buf, n, st_disjoint_impl,
            nullptr, true),
        n);
    clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(buf));

    EXPECT_EQ(got[0], 1u);
}

TEST(ColBinaryImpl, BoolBboxShortCircuit) {
    auto poly   = wkt2wkb(kSquare);
    auto far_pt = wkt2wkb("POINT (100 100)");
    const uint32_t n = 1;

    auto* buf = make_col_binary_geom_buf(n, {poly}, {far_pt});
    auto got = read_cb_bool(
        col_binary_impl_wrapper("st_contains_cb", buf, n, st_contains_impl,
            bbox_op_contains, false),
        n);
    clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(buf));

    EXPECT_EQ(got[0], 0u);
}

TEST(ColBinaryImpl, BoolAConstBboxShortCircuit) {
    auto poly   = wkt2wkb(kSquare);
    auto far_pt = wkt2wkb("POINT (100 100)");
    auto near_pt = wkt2wkb("POINT (0.5 0.5)");
    const uint32_t n = 2;

    auto* buf = make_col_binary_geom_buf(n, {poly}, {near_pt, far_pt});
    auto got = read_cb_bool(
        col_binary_impl_wrapper("st_contains_cb", buf, n, st_contains_impl,
            bbox_op_contains, false, prep_a_st_contains, prep_b_st_contains),
        n);
    clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(buf));

    EXPECT_EQ(got[0], 1u);
    EXPECT_EQ(got[1], 0u);
}

// ── col_binary_impl_wrapper: double return ────────────────────────────────────

static double double_identity_impl(double x) { return x; }

TEST(ColBinaryImpl, DoubleReturn) {
    std::vector<uint8_t> col_data;
    double vals[] = {1.5, -2.5, 0.0};
    for (auto v : vals) {
        col_data.insert(col_data.end(),
                        reinterpret_cast<const uint8_t*>(&v),
                        reinterpret_cast<const uint8_t*>(&v) + sizeof(double));
    }

    auto* buf = make_col_binary_buf(3, 1, {col_data}, 0);
    auto* result = col_binary_impl_wrapper("double_identity_cb", buf, 3, double_identity_impl);

    auto cb = parse_col_binary(result);
    EXPECT_EQ(cb.num_rows, 3u);
    EXPECT_EQ(cb.num_cols, 1u);
    EXPECT_EQ(cb.cols[0].data_size, 24u);

    double r0, r1, r2;
    std::memcpy(&r0, cb.cols[0].data, 8);
    std::memcpy(&r1, cb.cols[0].data + 8, 8);
    std::memcpy(&r2, cb.cols[0].data + 16, 8);
    EXPECT_DOUBLE_EQ(r0, 1.5);
    EXPECT_DOUBLE_EQ(r1, -2.5);
    EXPECT_DOUBLE_EQ(r2, 0.0);

    clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(result));
}

// ── col_binary_impl_wrapper: geometry return ──────────────────────────────────

static std::unique_ptr<geos::geom::Geometry> geom_identity_impl(
    std::unique_ptr<geos::geom::Geometry> g) {
    return g;
}

TEST(ColBinaryImpl, GeometryReturn) {
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

    auto* buf = make_col_binary_buf(2, 1, {col_data});
    auto* result = col_binary_impl_wrapper("geom_identity_cb", buf, 2, geom_identity_impl);

    auto cb = parse_col_binary(result);
    ColBinaryReader reader = ColBinaryReader::from_col(cb.cols[0], 2);
    auto r0 = reader.read_blob();
    auto r1 = reader.read_blob();
    auto gr0 = read_wkb(r0);
    auto gr1 = read_wkb(r1);
    ASSERT_NE(gr0, nullptr);
    ASSERT_NE(gr1, nullptr);
    EXPECT_DOUBLE_EQ(static_cast<const geos::geom::Point*>(gr0.get())->getX(), 1.0);
    EXPECT_DOUBLE_EQ(static_cast<const geos::geom::Point*>(gr1.get())->getY(), 4.0);

    clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(result));
}

// ── col_binary_impl_wrapper: string return ────────────────────────────────────

static std::string string_ok_impl() { return "ok"; }

TEST(ColBinaryImpl, StringReturn) {
    auto* buf = make_col_binary_buf(3, 0, {});
    auto* result = col_binary_impl_wrapper("string_ok_cb", buf, 3, string_ok_impl);

    auto cb = parse_col_binary(result);
    ColBinaryReader reader = ColBinaryReader::from_col(cb.cols[0], 3);
    auto s0 = reader.read_blob();
    auto s1 = reader.read_blob();
    auto s2 = reader.read_blob();
    EXPECT_EQ(std::string(s0.begin(), s0.end()), "ok");
    EXPECT_EQ(std::string(s1.begin(), s1.end()), "ok");
    EXPECT_EQ(std::string(s2.begin(), s2.end()), "ok");

    clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(result));
}

// ── col_binary_impl_wrapper: int32 return ─────────────────────────────────────

static int32_t int32_identity_impl(int32_t x) { return x; }

TEST(ColBinaryImpl, Int32Return) {
    std::vector<uint8_t> col_data;
    int32_t vals[] = {100, -200, 0};
    for (auto v : vals) {
        col_data.insert(col_data.end(),
                        reinterpret_cast<const uint8_t*>(&v),
                        reinterpret_cast<const uint8_t*>(&v) + sizeof(int32_t));
    }

    auto* buf = make_col_binary_buf(3, 1, {col_data}, 0);
    auto* result = col_binary_impl_wrapper("int32_identity_cb", buf, 3, int32_identity_impl);

    auto cb = parse_col_binary(result);
    EXPECT_EQ(cb.cols[0].data_size, 12u);

    int32_t r0, r1, r2;
    std::memcpy(&r0, cb.cols[0].data, 4);
    std::memcpy(&r1, cb.cols[0].data + 4, 4);
    std::memcpy(&r2, cb.cols[0].data + 8, 4);
    EXPECT_EQ(r0, 100);
    EXPECT_EQ(r1, -200);
    EXPECT_EQ(r2, 0);

    clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(result));
}

// ── col_binary_impl_wrapper: uint32 return ────────────────────────────────────

static uint32_t uint32_identity_impl(uint32_t x) { return x; }

TEST(ColBinaryImpl, Uint32Return) {
    std::vector<uint8_t> col_data;
    uint32_t vals[] = {100u, 200u, 0u};
    for (auto v : vals) {
        col_data.insert(col_data.end(),
                        reinterpret_cast<const uint8_t*>(&v),
                        reinterpret_cast<const uint8_t*>(&v) + sizeof(uint32_t));
    }

    auto* buf = make_col_binary_buf(3, 1, {col_data}, 0);
    auto* result = col_binary_impl_wrapper("uint32_identity_cb", buf, 3, uint32_identity_impl);

    auto cb = parse_col_binary(result);
    EXPECT_EQ(cb.cols[0].data_size, 12u);

    uint32_t r0, r1, r2;
    std::memcpy(&r0, cb.cols[0].data, 4);
    std::memcpy(&r1, cb.cols[0].data + 4, 4);
    std::memcpy(&r2, cb.cols[0].data + 8, 4);
    EXPECT_EQ(r0, 100u);
    EXPECT_EQ(r1, 200u);
    EXPECT_EQ(r2, 0u);

    clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(result));
}

// ── col_binary_impl_wrapper: st_dwithin ───────────────────────────────────────

// Build a 3-col ColumnBinary buffer: geom, geom, double
static raw_buffer* make_col_binary_geom3(uint32_t num_rows,
    const std::vector<ch::Vector>& col0_wkbs,
    const std::vector<ch::Vector>& col1_wkbs,
    const std::vector<double>& col2_vals,
    uint32_t const_col_idx = UINT32_MAX) {
    auto write_col = [&](const std::vector<ch::Vector>& wkbs, std::vector<uint8_t>& out, bool is_const) {
        uint32_t rows = is_const ? 1 : num_rows;
        for (uint32_t i = 0; i < rows; ++i) {
            const auto& wkb = wkbs.empty() ? wkbs[0] : wkbs[i % wkbs.size()];
            uint32_t len = static_cast<uint32_t>(wkb.size());
            if (is_const) {
                // CH native format for const String: u32[2] {0, len} + raw bytes
                uint32_t o0 = 0, o1 = len;
                for (int j = 0; j < 4; ++j) out.push_back(static_cast<uint8_t>(o0 >> (j * 8)));
                for (int j = 0; j < 4; ++j) out.push_back(static_cast<uint8_t>(o1 >> (j * 8)));
                out.insert(out.end(), wkb.data(), wkb.data() + wkb.size());
            } else {
                raw_buffer* vb = clickhouse_create_buffer(0);
                vb->clear();
                writeVarUInt(len, *vb);
                vb->append(wkb.data(), len);
                out.insert(out.end(), vb->data(), vb->data() + vb->size());
                clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(vb));
            }
        }
    };

    std::vector<uint8_t> col0_data, col1_data, col2_data;
    write_col(col0_wkbs, col0_data, const_col_idx == 0);
    write_col(col1_wkbs, col1_data, const_col_idx == 1);
    for (auto v : col2_vals) {
        col2_data.insert(col2_data.end(),
                         reinterpret_cast<const uint8_t*>(&v),
                         reinterpret_cast<const uint8_t*>(&v) + sizeof(double));
    }

    raw_buffer* buf = clickhouse_create_buffer(0);
    buf->clear();
    writeBinaryLE32(3, *buf);
    writeBinaryLE32(num_rows, *buf);

    // Col0: geom
    buf->push_back(const_col_idx == 0 ? 0x01 : 0);
    uint64_t ds0 = col0_data.size();
    for (uint32_t j = 0; j < 8; ++j) buf->push_back(static_cast<uint8_t>(ds0 >> (j * 8)));
    for (auto b : col0_data) buf->push_back(b);

    // Col1: geom
    buf->push_back(const_col_idx == 1 ? 0x01 : 0);
    uint64_t ds1 = col1_data.size();
    for (uint32_t j = 0; j < 8; ++j) buf->push_back(static_cast<uint8_t>(ds1 >> (j * 8)));
    for (auto b : col1_data) buf->push_back(b);

    // Col2: double
    buf->push_back(0);
    uint64_t ds2 = col2_data.size();
    for (uint32_t j = 0; j < 8; ++j) buf->push_back(static_cast<uint8_t>(ds2 >> (j * 8)));
    for (auto b : col2_data) buf->push_back(b);

    return buf;
}

TEST(ColBinaryImpl, BoolAConstDist_MatchesBaseline) {
    auto origin  = wkt2wkb("POINT (0 0)");
    auto near_pt = wkt2wkb("POINT (3 0)");
    auto far_pt  = wkt2wkb("POINT (10 0)");
    constexpr double kDist = 5.0;
    const uint32_t n = 2;

    std::vector<double> col2_vals(n, kDist);
    std::vector<ch::Vector> col0_aconst(n, origin);

    auto* buf = make_col_binary_geom3(n, col0_aconst, {near_pt, far_pt}, col2_vals, 0);
    auto got = read_cb_bool(
        col_binary_impl_wrapper("st_dwithin_cb", buf, n, st_dwithin_impl,
            nullptr, false, nullptr, nullptr,
            prep_a_st_dwithin, prep_b_st_dwithin),
        n);
    clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(buf));

    EXPECT_EQ(got[0], 1u);
    EXPECT_EQ(got[1], 0u);

    // Baseline: non-const A
    std::vector<ch::Vector> col0_base = {origin, origin};
    auto* buf_base = make_col_binary_geom3(n, col0_base, {near_pt, far_pt}, col2_vals);
    auto base = read_cb_bool(
        col_binary_impl_wrapper("st_dwithin_base_cb", buf_base, n, st_dwithin_impl,
            nullptr, false, nullptr, nullptr,
            prep_a_st_dwithin, prep_b_st_dwithin),
        n);
    clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(buf_base));

    EXPECT_EQ(got, base);
}

TEST(ColBinaryImpl, BoolBConstDist_MatchesBaseline) {
    auto near_pt = wkt2wkb("POINT (3 0)");
    auto far_pt  = wkt2wkb("POINT (10 0)");
    auto origin  = wkt2wkb("POINT (0 0)");
    constexpr double kDist = 5.0;
    const uint32_t n = 2;

    std::vector<double> col2_vals(n, kDist);
    std::vector<ch::Vector> col1_bconst(n, origin);

    auto* buf = make_col_binary_geom3(n, {near_pt, far_pt}, col1_bconst, col2_vals, 1);
    auto got = read_cb_bool(
        col_binary_impl_wrapper("st_dwithin_bconst_cb", buf, n, st_dwithin_impl,
            nullptr, false, nullptr, nullptr,
            prep_a_st_dwithin, prep_b_st_dwithin),
        n);
    clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(buf));

    EXPECT_EQ(got[0], 1u);
    EXPECT_EQ(got[1], 0u);

    // Baseline: non-const B
    std::vector<ch::Vector> col1_base = {origin, origin};
    auto* buf_base = make_col_binary_geom3(n, {near_pt, far_pt}, col1_base, col2_vals);
    auto base = read_cb_bool(
        col_binary_impl_wrapper("st_dwithin_base_cb", buf_base, n, st_dwithin_impl,
            nullptr, false, nullptr, nullptr,
            prep_a_st_dwithin, prep_b_st_dwithin),
        n);
    clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(buf_base));

    EXPECT_EQ(got, base);
}

// ── Array(String) with cumulative offsets ─────────────────────────────────────

// Helper: build a ColumnBinary buffer with a single Array(String) column.
// CH wire format: [N u64 per-row counts][u32[total_M+1] cumul str offsets][chars+nulls]
static raw_buffer* make_col_binary_array_col(uint32_t num_rows,
                                              const std::vector<std::vector<std::string>>& rows) {
    raw_buffer* buf = clickhouse_create_buffer(0);
    buf->clear();
    writeBinaryLE32(1, *buf);
    writeBinaryLE32(num_rows, *buf);

    // Build flat string list — CH ColumnBinaryOutputFormat does NOT write null terminators.
    std::string chars_data;
    std::vector<uint32_t> str_offsets = {0};
    for (uint32_t i = 0; i < num_rows; ++i) {
        for (auto& s : rows[i]) {
            chars_data += s;
            str_offsets.push_back(static_cast<uint32_t>(chars_data.size()));
        }
    }

    std::vector<uint8_t> col_data;
    // N u64LE per-row element counts.
    for (uint32_t i = 0; i < num_rows; ++i) {
        uint64_t c = rows[i].size();
        for (int j = 0; j < 8; ++j)
            col_data.push_back(static_cast<uint8_t>(c >> (j * 8)));
    }
    // u32[total_M+1] cumulative string offsets.
    for (uint32_t o : str_offsets) {
        for (int j = 0; j < 4; ++j)
            col_data.push_back(static_cast<uint8_t>(o >> (j * 8)));
    }
    // Chars (CH ColumnBinaryOutputFormat does not write null terminators).
    col_data.insert(col_data.end(), chars_data.begin(), chars_data.end());

    buf->push_back(0); // non-const, Array(String)
    uint64_t ds = col_data.size();
    for (uint32_t j = 0; j < 8; ++j)
        buf->push_back(static_cast<uint8_t>(ds >> (j * 8)));
    for (auto b : col_data) buf->push_back(b);

    return buf;
}

// Helper: build a const Array(String) column buffer.
// CH wire format (1 row): [u64 M][u32[M+1] cumul str offsets][chars+nulls]
static raw_buffer* make_col_binary_array_col_const(uint32_t num_rows,
                                                    const std::vector<std::string>& one_row) {
    raw_buffer* buf = clickhouse_create_buffer(0);
    buf->clear();
    writeBinaryLE32(1, *buf);
    writeBinaryLE32(num_rows, *buf);

    uint64_t M = one_row.size();
    std::string chars_data;
    std::vector<uint32_t> str_offsets = {0};
    for (auto& s : one_row) {
        chars_data += s;
        str_offsets.push_back(static_cast<uint32_t>(chars_data.size()));
    }

    std::vector<uint8_t> col_data;
    // u64 M
    for (int j = 0; j < 8; ++j)
        col_data.push_back(static_cast<uint8_t>(M >> (j * 8)));
    // u32[M+1] cumulative string offsets
    for (uint32_t o : str_offsets) {
        for (int j = 0; j < 4; ++j)
            col_data.push_back(static_cast<uint8_t>(o >> (j * 8)));
    }
    // chars (CH ColumnBinaryOutputFormat does not write null terminators)
    col_data.insert(col_data.end(), chars_data.begin(), chars_data.end());

    buf->push_back(0x01); // const, Array(String)
    uint64_t ds = static_cast<uint64_t>(col_data.size());
    for (uint32_t j = 0; j < 8; ++j)
        buf->push_back(static_cast<uint8_t>(ds >> (j * 8)));
    for (auto b : col_data) buf->push_back(b);

    return buf;
}

static uint32_t array_size_impl(std::vector<std::unique_ptr<geos::geom::Geometry>> geoms) {
    return static_cast<uint32_t>(geoms.size());
}

TEST(ColBinaryImpl, ArrayNonConst_CumulativeOffsets) {
    std::vector<std::vector<std::string>> rows = {
        {"POINT(1 2)", "POINT(3 4)"},
        {"POINT(5 6)"},
        {}
    };
    auto* buf = make_col_binary_array_col(3, rows);
    auto* result = col_binary_impl_wrapper("array_size_cb", buf, 3, array_size_impl);
    auto cb = parse_col_binary(result);
    uint32_t r0, r1, r2;
    std::memcpy(&r0, cb.cols[0].data, 4);
    std::memcpy(&r1, cb.cols[0].data + 4, 4);
    std::memcpy(&r2, cb.cols[0].data + 8, 4);

    EXPECT_EQ(r0, 2u);
    EXPECT_EQ(r1, 1u);
    EXPECT_EQ(r2, 0u);

    clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(result));
}

TEST(ColBinaryImpl, ArrayConst_CumulativeOffsets) {
    std::vector<std::string> one_row = {"POINT(1 2)", "POINT(3 4)", "POINT(5 6)"};
    const uint32_t n = 5;
    auto* buf = make_col_binary_array_col_const(n, one_row);

    auto* result = col_binary_impl_wrapper("array_size_const_cb", buf, n, array_size_impl);
    auto cb = parse_col_binary(result);
    for (uint32_t i = 0; i < n; ++i) {
        uint32_t r;
        std::memcpy(&r, cb.cols[0].data + i * 4, 4);
        EXPECT_EQ(r, 3u) << "row " << i;
    }

    clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(result));
}

TEST(ColBinaryImpl, ArrayDebug_DirectUnpack) {
    // Const Array(String) with 3 WKT strings.
    // CH format: [u64 M][u32[M+1] cumul str offsets][chars] (no null terminators)
    const std::vector<std::string> strs = {"POINT(0 0)", "POINT(1 1)", "POINT(2 2)"};
    auto* buf = make_col_binary_array_col_const(5, strs);

    auto cb = parse_col_binary(buf);
    ColBinaryReader reader = ColBinaryReader::from_col(cb.cols[0], 5);

    auto result = unpack_arg<std::vector<std::unique_ptr<geos::geom::Geometry>>>(reader);
    EXPECT_EQ(result.size(), 3u);
    for (auto& g : result) ASSERT_NE(g, nullptr);

    clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(buf));
}

// ── BConst_NativeFormat: const col with Native format ─────────────────────────

TEST(ColBinaryImpl, BConst_NativeFormat_Predicate) {
    auto big   = wkt2wkb("POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))");
    auto small = wkt2wkb("POLYGON ((2 2, 3 2, 3 3, 2 3, 2 2))");
    auto pt    = wkt2wkb("POINT (0.5 0.5)");
    const uint32_t n = 2;

    std::vector<uint8_t> col0_data;
    for (uint32_t i = 0; i < n; ++i) {
        const auto& wkb = (i == 0) ? big : small;
        uint32_t len = static_cast<uint32_t>(wkb.size());
        raw_buffer* vb = clickhouse_create_buffer(0);
        vb->clear();
        writeVarUInt(len, *vb);
        vb->append(wkb.data(), len);
        col0_data.insert(col0_data.end(), vb->data(), vb->data() + vb->size());
        clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(vb));
    }

    // const col1: CH native format u32[2] {0, len} + raw WKB bytes
    std::vector<uint8_t> col1_data;
    {
        uint32_t o0 = 0, o1 = static_cast<uint32_t>(pt.size());
        for (int j = 0; j < 4; ++j) col1_data.push_back(static_cast<uint8_t>(o0 >> (j * 8)));
        for (int j = 0; j < 4; ++j) col1_data.push_back(static_cast<uint8_t>(o1 >> (j * 8)));
        col1_data.insert(col1_data.end(), pt.begin(), pt.end());
    }

    raw_buffer* buf = clickhouse_create_buffer(0);
    buf->clear();
    writeBinaryLE32(2, *buf);
    writeBinaryLE32(n, *buf);

    // Col0: non-const
    buf->push_back(0);
    uint64_t ds0 = col0_data.size();
    for (uint32_t j = 0; j < 8; ++j) buf->push_back(static_cast<uint8_t>(ds0 >> (j * 8)));
    for (auto b : col0_data) buf->push_back(b);

    // Col1: const
    buf->push_back(0x01);
    uint64_t ds1 = col1_data.size();
    for (uint32_t j = 0; j < 8; ++j) buf->push_back(static_cast<uint8_t>(ds1 >> (j * 8)));
    for (auto b : col1_data) buf->push_back(b);

    auto got = read_cb_bool(
        col_binary_impl_wrapper("st_contains_bconst_native", buf, n, st_contains_impl,
            bbox_op_contains, false, prep_a_st_contains, prep_b_st_contains),
        n);

    EXPECT_EQ(got[0], 1u);
    EXPECT_EQ(got[1], 0u);
    clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(buf));
}

TEST(ColBinaryImpl, BConst_NativeFormat_Dwithin) {
    auto near_pt = wkt2wkb("POINT (3 0)");
    auto far_pt  = wkt2wkb("POINT (10 0)");
    auto origin  = wkt2wkb("POINT (0 0)");
    constexpr double kDist = 5.0;
    const uint32_t n = 2;

    std::vector<uint8_t> col0_data;
    for (uint32_t i = 0; i < n; ++i) {
        const auto& wkb = (i == 0) ? near_pt : far_pt;
        uint32_t len = static_cast<uint32_t>(wkb.size());
        raw_buffer* vb = clickhouse_create_buffer(0);
        vb->clear();
        writeVarUInt(len, *vb);
        vb->append(wkb.data(), len);
        col0_data.insert(col0_data.end(), vb->data(), vb->data() + vb->size());
        clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(vb));
    }

    std::vector<uint8_t> col1_data;
    {
        uint32_t o0 = 0, o1 = static_cast<uint32_t>(origin.size());
        for (int j = 0; j < 4; ++j) col1_data.push_back(static_cast<uint8_t>(o0 >> (j * 8)));
        for (int j = 0; j < 4; ++j) col1_data.push_back(static_cast<uint8_t>(o1 >> (j * 8)));
        col1_data.insert(col1_data.end(), origin.begin(), origin.end());
    }

    std::vector<uint8_t> col2_data;
    for (uint32_t i = 0; i < n; ++i)
        col2_data.insert(col2_data.end(),
                         reinterpret_cast<const uint8_t*>(&kDist),
                         reinterpret_cast<const uint8_t*>(&kDist) + sizeof(double));

    raw_buffer* buf = clickhouse_create_buffer(0);
    buf->clear();
    writeBinaryLE32(3, *buf);
    writeBinaryLE32(n, *buf);

    // Col0: non-const geom
    buf->push_back(0);
    uint64_t ds0 = col0_data.size();
    for (uint32_t j = 0; j < 8; ++j) buf->push_back(static_cast<uint8_t>(ds0 >> (j * 8)));
    for (auto b : col0_data) buf->push_back(b);

    // Col1: const geom
    buf->push_back(0x01);
    uint64_t ds1 = col1_data.size();
    for (uint32_t j = 0; j < 8; ++j) buf->push_back(static_cast<uint8_t>(ds1 >> (j * 8)));
    for (auto b : col1_data) buf->push_back(b);

    // Col2: non-const double
    buf->push_back(0);
    uint64_t ds2 = col2_data.size();
    for (uint32_t j = 0; j < 8; ++j) buf->push_back(static_cast<uint8_t>(ds2 >> (j * 8)));
    for (auto b : col2_data) buf->push_back(b);

    auto got = read_cb_bool(
        col_binary_impl_wrapper("st_dwithin_bconst_native", buf, n, st_dwithin_impl,
            nullptr, false, nullptr, nullptr,
            prep_a_st_dwithin, prep_b_st_dwithin),
        n);

    EXPECT_EQ(got[0], 1u);
    EXPECT_EQ(got[1], 0u);
    clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(buf));
}
