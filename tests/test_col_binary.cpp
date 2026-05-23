#include <gtest/gtest.h>
#include <cstring>
#include <vector>

#include <geos/geom/Point.h>

#include "helpers.hpp"
#include "col_binary.hpp"
#include "functions/predicates.hpp"

using namespace ch;

static const std::string kSquare = "POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))";

// ── COLUMNAR_V1 helpers ───────────────────────────────────────────────────────

static void write_le32(std::vector<uint8_t>& buf, uint32_t v) {
    for (int j = 0; j < 4; ++j) buf.push_back(static_cast<uint8_t>(v >> (j * 8)));
}
static void write_le32(raw_buffer& buf, uint32_t v) {
    for (int j = 0; j < 4; ++j) buf.push_back(static_cast<uint8_t>(v >> (j * 8)));
}

static void write_le64(std::vector<uint8_t>& buf, uint64_t v) {
    for (int j = 0; j < 8; ++j) buf.push_back(static_cast<uint8_t>(v >> (j * 8)));
}
static void write_le64(raw_buffer& buf, uint64_t v) {
    for (int j = 0; j < 8; ++j) buf.push_back(static_cast<uint8_t>(v >> (j * 8)));
}

// Build a COLUMNAR_V1 raw_buffer from num_rows, num_cols, and per-column data.
// Each column: ColDescriptor (type, null_offset=0, offsets_offset, data_offset, data_size) + data.
// For COL_BYTES: data = u32[N+1] offsets + raw bytes.
// For fixed types: data = raw bytes.
static raw_buffer* make_col_buf(uint32_t num_rows, uint32_t num_cols,
                                 const std::vector<std::vector<uint8_t>>& col_data,
                                 const std::vector<uint32_t>& col_offsets, // per-column offsets for string data
                                 const std::vector<uint32_t>& col_types) {
    raw_buffer* buf = clickhouse_create_buffer(0);
    buf->clear();

    // BufHeader
    write_le32(*buf, num_rows);
    write_le32(*buf, num_cols);

    // Build data area: interleaved offsets + data per column
    std::vector<uint8_t> data_area;
    std::vector<uint32_t> desc_offsets; // byte offset of each ColDescriptor in the final buffer
    desc_offsets.reserve(num_cols);

    uint32_t header_end = 8u + num_cols * 20u;
    uint32_t data_pos = header_end;

    std::vector<uint8_t> all_descs;
    all_descs.resize(num_cols * 20);

    for (uint32_t i = 0; i < num_cols; ++i) {
        uint32_t type = col_types[i];
        uint32_t off_off = 0, data_off = 0;

        if ((type & ~COL_IS_NULLABLE) == COL_BYTES || type == COL_COMPLEX) {
            off_off = data_pos;
            data_pos += (col_offsets[i] > 0 ? col_offsets[i] : (num_rows + 1)) * 4u;
            // Pad to align
            data_pos = (data_pos + 3u) & ~3u;
            data_off = data_pos;
            data_pos += static_cast<uint32_t>(col_data[i].size());
        } else {
            data_off = data_pos;
            data_pos += static_cast<uint32_t>(col_data[i].size());
        }

        uint32_t d_off = static_cast<uint32_t>(all_descs.size());
        write_le32(all_descs, type);
        write_le32(all_descs, 0u); // null_offset
        write_le32(all_descs, off_off);
        write_le32(all_descs, data_off);
        write_le32(all_descs, static_cast<uint32_t>(col_data[i].size()));
    }

    buf->append(all_descs.data(), static_cast<uint32_t>(all_descs.size()));

    // Data area: for each column, write offsets (if any) then data
    for (uint32_t i = 0; i < num_cols; ++i) {
        uint32_t type = col_types[i];
        if ((type & ~COL_IS_NULLABLE) == COL_BYTES || type == COL_COMPLEX) {
            uint32_t num_offs = col_offsets[i] > 0 ? col_offsets[i] : (num_rows + 1);
            for (uint32_t j = 0; j < num_offs; ++j)
                write_le32(*buf, 0); // placeholder, fixed below
            buf->append(col_data[i].data(), static_cast<uint32_t>(col_data[i].size()));
        } else {
            buf->append(col_data[i].data(), static_cast<uint32_t>(col_data[i].size()));
        }
    }

    // Patch offsets into the data area
    uint8_t* data_ptr = buf->data() + header_end;
    uint32_t pos = 0;
    for (uint32_t i = 0; i < num_cols; ++i) {
        uint32_t type = col_types[i];
        if ((type & ~COL_IS_NULLABLE) == COL_BYTES || type == COL_COMPLEX) {
            uint32_t num_offs = col_offsets[i] > 0 ? col_offsets[i] : (num_rows + 1);
            uint32_t* off_ptr = reinterpret_cast<uint32_t*>(data_ptr + pos);
            for (uint32_t j = 0; j < num_offs; ++j)
                off_ptr[j] = static_cast<uint32_t>(col_offsets[j] > 0 ? col_offsets[j] : 0);
            pos += num_offs * 4u;
            pos += static_cast<uint32_t>(col_data[i].size());
            pos = (pos + 3u) & ~3u;
        } else {
            pos += static_cast<uint32_t>(col_data[i].size());
        }
    }

    return buf;
}

// Simpler helper: build COLUMNAR_V1 buffer with a single fixed-width column.
static raw_buffer* make_col_fixed(uint32_t num_rows, uint8_t col_type,
                                   const std::vector<uint8_t>& data) {
    raw_buffer* buf = clickhouse_create_buffer(0);
    buf->clear();

    write_le32(*buf, num_rows);
    write_le32(*buf, 1u);

    // ColDescriptor
    write_le32(*buf, col_type);
    write_le32(*buf, 0u);     // null_offset
    write_le32(*buf, 0u);     // offsets_offset
    write_le32(*buf, 8u + 20u); // data_offset
    write_le32(*buf, static_cast<uint32_t>(data.size()));

    buf->append(data.data(), static_cast<uint32_t>(data.size()));
    return buf;
}

// Build a COLUMNAR_V1 buffer with a single COL_BYTES (string) column.
// col_data: raw string bytes; col_offsets: u32[N+1] start offsets.
static raw_buffer* make_col_string(uint32_t num_rows,
                                    const std::vector<uint8_t>& col_data,
                                    const std::vector<uint32_t>& col_offsets) {
    raw_buffer* buf = clickhouse_create_buffer(0);
    buf->clear();

    write_le32(*buf, num_rows);
    write_le32(*buf, 1u);

    uint32_t data_offset = 8u + 20u + (num_rows + 1u) * 4u;

    // ColDescriptor
    write_le32(*buf, COL_BYTES);
    write_le32(*buf, 0u);     // null_offset
    write_le32(*buf, 8u + 20u); // offsets_offset
    write_le32(*buf, data_offset); // data_offset
    write_le32(*buf, static_cast<uint32_t>(col_data.size()));

    // Offsets
    for (uint32_t o : col_offsets)
        write_le32(*buf, o);

    // Data
    buf->append(col_data.data(), static_cast<uint32_t>(col_data.size()));
    return buf;
}

// Build a 2-column COLUMNAR_V1 buffer for geometry predicates.
// Uses COL_BYTES for geometry columns.
static raw_buffer* make_col_geom_buf(uint32_t num_rows,
                                      const std::vector<ch::Vector>& col0_wkbs,
                                      const std::vector<ch::Vector>& col1_wkbs,
                                      uint32_t const_col_idx = UINT32_MAX) {
    raw_buffer* buf = clickhouse_create_buffer(0);
    buf->clear();

    write_le32(*buf, num_rows);
    write_le32(*buf, 2u);

    // Build data for each column — returns (offsets u32[], raw-wkb-bytes)
    auto build_col = [&](const std::vector<ch::Vector>& wkbs, bool is_const) {
        uint32_t rows = is_const ? 1u : num_rows;
        std::vector<uint32_t> offsets;
        offsets.reserve(rows + 1);
        offsets.push_back(0);
        for (uint32_t i = 0; i < rows; ++i) {
            const auto& wkb = wkbs[i % wkbs.size()];
            offsets.push_back(static_cast<uint32_t>(offsets.back() + wkb.size() + 1u)); // +1 null term
        }
        std::vector<uint8_t> data;
        for (uint32_t i = 0; i < rows; ++i) {
            const auto& wkb = wkbs[i % wkbs.size()];
            data.insert(data.end(), wkb.begin(), wkb.end());
            data.push_back(0); // null terminator
        }
        return std::make_pair(offsets, data);
    };

    auto [offs0, data0] = build_col(col0_wkbs, const_col_idx == 0);
    auto [offs1, data1] = build_col(col1_wkbs, const_col_idx == 1);

    uint32_t offsets0_off = 8u + 2u * 20u;  // header + 2 descriptors
    uint32_t offsets1_off = offsets0_off + static_cast<uint32_t>(offs0.size()) * 4u;
    uint32_t data0_off = offsets1_off + static_cast<uint32_t>(offs1.size()) * 4u;
    uint32_t data1_off = data0_off + static_cast<uint32_t>(data0.size());

    // ColDescriptor col0
    write_le32(*buf, COL_BYTES | (const_col_idx == 0 ? COL_IS_CONST : 0u));
    write_le32(*buf, 0u);
    write_le32(*buf, offsets0_off);
    write_le32(*buf, data0_off);
    write_le32(*buf, static_cast<uint32_t>(data0.size()));

    // ColDescriptor col1
    write_le32(*buf, COL_BYTES | (const_col_idx == 1 ? COL_IS_CONST : 0u));
    write_le32(*buf, 0u);
    write_le32(*buf, offsets1_off);
    write_le32(*buf, data1_off);
    write_le32(*buf, static_cast<uint32_t>(data1.size()));

    // Offsets + data
    for (uint32_t o : offs0) write_le32(*buf, o);
    for (uint32_t o : offs1) write_le32(*buf, o);
    buf->append(data0.data(), static_cast<uint32_t>(data0.size()));
    buf->append(data1.data(), static_cast<uint32_t>(data1.size()));

    return buf;
}

// Build a 3-column COLUMNAR_V1 buffer: geom, geom, double
static raw_buffer* make_col_geom3(uint32_t num_rows,
    const std::vector<ch::Vector>& col0_wkbs,
    const std::vector<ch::Vector>& col1_wkbs,
    const std::vector<double>& col2_vals,
    uint32_t const_col_idx = UINT32_MAX) {
    raw_buffer* buf = clickhouse_create_buffer(0);
    buf->clear();

    write_le32(*buf, num_rows);
    write_le32(*buf, 3u);

    // Build geom column
    auto build_geom_col = [&](const std::vector<ch::Vector>& wkbs, bool is_const) {
        uint32_t rows = is_const ? 1u : num_rows;
        std::vector<uint32_t> offsets;
        offsets.push_back(0);
        for (uint32_t i = 0; i < rows; ++i) {
            const auto& wkb = wkbs[i % wkbs.size()];
            offsets.push_back(static_cast<uint32_t>(offsets.back() + wkb.size() + 1u));
        }
        std::vector<uint8_t> data;
        for (uint32_t i = 0; i < rows; ++i) {
            const auto& wkb = wkbs[i % wkbs.size()];
            data.insert(data.end(), wkb.begin(), wkb.end());
            data.push_back(0);
        }
        return std::make_pair(offsets, data);
    };

    auto [offs0, data0] = build_geom_col(col0_wkbs, const_col_idx == 0);
    auto [offs1, data1] = build_geom_col(col1_wkbs, const_col_idx == 1);

    uint32_t offsets0_off = 8u + 60u;
    uint32_t offsets1_off = offsets0_off + static_cast<uint32_t>(offs0.size()) * 4u;
    uint32_t data0_off = offsets1_off + static_cast<uint32_t>(offs1.size()) * 4u;
    uint32_t data1_off = data0_off + static_cast<uint32_t>(data0.size());
    uint32_t data2_off = data1_off + static_cast<uint32_t>(data1.size());

    // ColDescriptor col0
    write_le32(*buf, COL_BYTES | (const_col_idx == 0 ? COL_IS_CONST : 0u));
    write_le32(*buf, 0u);
    write_le32(*buf, offsets0_off);
    write_le32(*buf, data0_off);
    write_le32(*buf, static_cast<uint32_t>(data0.size()));

    // ColDescriptor col1
    write_le32(*buf, COL_BYTES | (const_col_idx == 1 ? COL_IS_CONST : 0u));
    write_le32(*buf, 0u);
    write_le32(*buf, offsets1_off);
    write_le32(*buf, data1_off);
    write_le32(*buf, static_cast<uint32_t>(data1.size()));

    // ColDescriptor col2 (fixed64, no offsets)
    write_le32(*buf, COL_FIXED64);
    write_le32(*buf, 0u);
    write_le32(*buf, 0u);
    write_le32(*buf, data2_off);
    write_le32(*buf, static_cast<uint32_t>(col2_vals.size() * 8u));

    // Offsets + data
    for (uint32_t o : offs0) write_le32(*buf, o);
    for (uint32_t o : offs1) write_le32(*buf, o);
    buf->append(data0.data(), static_cast<uint32_t>(data0.size()));
    buf->append(data1.data(), static_cast<uint32_t>(data1.size()));
    for (auto v : col2_vals)
        buf->append(reinterpret_cast<const uint8_t*>(&v), 8u);

    return buf;
}

// Parse COLUMNAR_V1 output and read bool bytes.
static std::vector<uint8_t> read_col_bool(raw_buffer* out, uint32_t n) {
    auto cb = parse_columnar(out);
    auto col = cb.col(0);
    std::vector<uint8_t> res(n);
    std::memcpy(res.data(), col.data, n);
    clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(out));
    return res;
}

// ── parse_columnar: header parsing ────────────────────────────────────────────

TEST(ColumnarParse, SingleCol) {
    std::vector<uint8_t> data = {0x01, 0x02, 0x03};
    auto* buf = make_col_fixed(3, COL_FIXED8, data);

    auto cb = parse_columnar(buf);
    EXPECT_EQ(cb.num_rows, 3u);
    EXPECT_EQ(cb.num_cols, 1u);

    auto col = cb.col(0);
    EXPECT_EQ(col.base_type, COL_FIXED8);
    EXPECT_FALSE(col.is_const);
    EXPECT_EQ(col.row_count, 3u);
    EXPECT_EQ(col.data[0], 0x01);
    EXPECT_EQ(col.data[1], 0x02);
    EXPECT_EQ(col.data[2], 0x03);

    clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(buf));
}

TEST(ColumnarParse, MultiCol) {
    std::vector<uint8_t> col0 = {0xAA, 0xBB};
    std::vector<uint8_t> col1 = {0x01, 0x02, 0x03};
    std::vector<uint8_t> col2 = {0xFF};

    raw_buffer* buf = clickhouse_create_buffer(0);
    buf->clear();
    write_le32(*buf, 2u); // num_rows
    write_le32(*buf, 3u); // num_cols

    // ColDescriptor col0
    write_le32(*buf, COL_FIXED8);
    write_le32(*buf, 0u);
    write_le32(*buf, 0u);
    write_le32(*buf, 8u + 60u);
    write_le32(*buf, 2u);
    // ColDescriptor col1
    write_le32(*buf, COL_FIXED8);
    write_le32(*buf, 0u);
    write_le32(*buf, 0u);
    write_le32(*buf, 8u + 60u + 2u);
    write_le32(*buf, 3u);
    // ColDescriptor col2
    write_le32(*buf, COL_FIXED8);
    write_le32(*buf, 0u);
    write_le32(*buf, 0u);
    write_le32(*buf, 8u + 60u + 5u);
    write_le32(*buf, 1u);

    buf->push_back(0xAA); buf->push_back(0xBB);
    buf->push_back(0x01); buf->push_back(0x02); buf->push_back(0x03);
    buf->push_back(0xFF);

    auto cb = parse_columnar(buf);
    EXPECT_EQ(cb.num_rows, 2u);
    EXPECT_EQ(cb.num_cols, 3u);
    EXPECT_EQ(cb.descs[0].data_size, 2u);
    EXPECT_EQ(cb.descs[1].data_size, 3u);
    EXPECT_EQ(cb.descs[2].data_size, 1u);

    clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(buf));
}

// ── columnar_impl_wrapper: bool return ────────────────────────────────────────

TEST(ColumnarImpl, BoolNoOpt_MatchesBaseline) {
    auto poly   = wkt2wkb(kSquare);
    auto pt_in  = wkt2wkb("POINT (0.5 0.5)");
    auto pt_out = wkt2wkb("POINT (2.0 2.0)");
    const uint32_t n = 2;

    auto* buf = make_col_geom_buf(n, {poly, poly}, {pt_in, pt_out});
    auto got = read_col_bool(
        columnar_impl_wrapper(buf, n, st_contains_impl),
        n);
    clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(buf));

    EXPECT_EQ(got[0], 1u);
    EXPECT_EQ(got[1], 0u);
}

TEST(ColumnarImpl, BoolAConst_MatchesBaseline) {
    auto poly   = wkt2wkb(kSquare);
    auto pt_in  = wkt2wkb("POINT (0.5 0.5)");
    auto pt_out = wkt2wkb("POINT (2.0 2.0)");
    auto pt_bnd = wkt2wkb("POINT (0.0 0.0)");
    const uint32_t n = 3;

    std::vector<ch::Vector> col0_aconst(n, poly);
    auto* buf_aconst = make_col_geom_buf(n, col0_aconst, {pt_in, pt_out, pt_bnd});
    auto got = read_col_bool(
        columnar_impl_wrapper(buf_aconst, n, st_contains_impl,
            bbox_op_contains, false, prep_a_st_contains, prep_b_st_contains),
        n);
    clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(buf_aconst));

    EXPECT_EQ(got[0], 1u);
    EXPECT_EQ(got[1], 0u);
    EXPECT_EQ(got[2], 0u);

    auto* buf_base = make_col_geom_buf(n, {poly, poly, poly}, {pt_in, pt_out, pt_bnd});
    auto base = read_col_bool(
        columnar_impl_wrapper(buf_base, n, st_contains_impl,
            bbox_op_contains, false, prep_a_st_contains, prep_b_st_contains),
        n);
    clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(buf_base));

    EXPECT_EQ(got, base);
}

TEST(ColumnarImpl, BoolBConst_MatchesBaseline) {
    auto pt    = wkt2wkb("POINT (0.5 0.5)");
    auto big   = wkt2wkb("POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))");
    auto small = wkt2wkb("POLYGON ((2 2, 3 2, 3 3, 2 3, 2 2))");
    const uint32_t n = 2;

    std::vector<ch::Vector> col1_bconst(n, pt);
    auto* buf_bconst = make_col_geom_buf(n, {big, small}, col1_bconst);
    auto got = read_col_bool(
        columnar_impl_wrapper(buf_bconst, n, st_contains_impl,
            bbox_op_contains, false, prep_a_st_contains, prep_b_st_contains),
        n);
    clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(buf_bconst));

    EXPECT_EQ(got[0], 1u);
    EXPECT_EQ(got[1], 0u);

    auto* buf_base = make_col_geom_buf(n, {big, small}, {pt, pt});
    auto base = read_col_bool(
        columnar_impl_wrapper(buf_base, n, st_contains_impl,
            bbox_op_contains, false, prep_a_st_contains, prep_b_st_contains),
        n);
    clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(buf_base));

    EXPECT_EQ(got, base);
}

TEST(ColumnarImpl, BoolEarlyRet_Disjoint) {
    auto poly   = wkt2wkb(kSquare);
    auto far_pt = wkt2wkb("POINT (100 100)");
    const uint32_t n = 1;

    auto* buf = make_col_geom_buf(n, {poly}, {far_pt});
    auto got = read_col_bool(
        columnar_impl_wrapper(buf, n, st_disjoint_impl,
            nullptr, true),
        n);
    clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(buf));

    EXPECT_EQ(got[0], 1u);
}

TEST(ColumnarImpl, BoolBboxShortCircuit) {
    auto poly   = wkt2wkb(kSquare);
    auto far_pt = wkt2wkb("POINT (100 100)");
    const uint32_t n = 1;

    auto* buf = make_col_geom_buf(n, {poly}, {far_pt});
    auto got = read_col_bool(
        columnar_impl_wrapper(buf, n, st_contains_impl,
            bbox_op_contains, false),
        n);
    clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(buf));

    EXPECT_EQ(got[0], 0u);
}

TEST(ColumnarImpl, BoolAConstBboxShortCircuit) {
    auto poly   = wkt2wkb(kSquare);
    auto far_pt = wkt2wkb("POINT (100 100)");
    auto near_pt = wkt2wkb("POINT (0.5 0.5)");
    const uint32_t n = 2;

    auto* buf = make_col_geom_buf(n, {poly}, {near_pt, far_pt});
    auto got = read_col_bool(
        columnar_impl_wrapper(buf, n, st_contains_impl,
            bbox_op_contains, false, prep_a_st_contains, prep_b_st_contains),
        n);
    clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(buf));

    EXPECT_EQ(got[0], 1u);
    EXPECT_EQ(got[1], 0u);
}

// ── columnar_impl_wrapper: double return ──────────────────────────────────────

static double double_identity_impl(double x) { return x; }

TEST(ColumnarImpl, DoubleReturn) {
    std::vector<uint8_t> col_data;
    double vals[] = {1.5, -2.5, 0.0};
    for (auto v : vals)
        col_data.insert(col_data.end(),
                        reinterpret_cast<const uint8_t*>(&v),
                        reinterpret_cast<const uint8_t*>(&v) + sizeof(double));

    auto* buf = make_col_fixed(3, COL_FIXED64, col_data);
    auto* result = columnar_impl_wrapper(buf, 3, double_identity_impl);

    auto cb = parse_columnar(result);
    auto col = cb.col(0);
    EXPECT_EQ(col.row_count, 3u);

    double r0, r1, r2;
    std::memcpy(&r0, col.data, 8);
    std::memcpy(&r1, col.data + 8, 8);
    std::memcpy(&r2, col.data + 16, 8);
    EXPECT_DOUBLE_EQ(r0, 1.5);
    EXPECT_DOUBLE_EQ(r1, -2.5);
    EXPECT_DOUBLE_EQ(r2, 0.0);

    clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(result));
}

// ── columnar_impl_wrapper: geometry return ────────────────────────────────────

static std::unique_ptr<geos::geom::Geometry> geom_identity_impl(
    std::unique_ptr<geos::geom::Geometry> g) {
    return g;
}

TEST(ColumnarImpl, GeometryReturn) {
    auto wkb1 = wkt2wkb("POINT (1 2)");
    auto wkb2 = wkt2wkb("POINT (3 4)");

    std::vector<uint32_t> offsets = {0, static_cast<uint32_t>(wkb1.size() + 1),
                                      static_cast<uint32_t>(wkb1.size() + 1 + wkb2.size() + 1)};
    std::vector<uint8_t> data;
    data.insert(data.end(), wkb1.begin(), wkb1.end());
    data.push_back(0);
    data.insert(data.end(), wkb2.begin(), wkb2.end());
    data.push_back(0);

    auto* buf = make_col_string(2, data, offsets);
    auto* result = columnar_impl_wrapper(buf, 2, geom_identity_impl);

    auto cb = parse_columnar(result);
    auto col = cb.col(0);
    auto s0 = col.get_bytes(0);
    auto s1 = col.get_bytes(1);
    auto gr0 = read_wkb(s0);
    auto gr1 = read_wkb(s1);
    ASSERT_NE(gr0, nullptr);
    ASSERT_NE(gr1, nullptr);
    EXPECT_DOUBLE_EQ(static_cast<const geos::geom::Point*>(gr0.get())->getX(), 1.0);
    EXPECT_DOUBLE_EQ(static_cast<const geos::geom::Point*>(gr1.get())->getY(), 4.0);

    clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(result));
}

// ── columnar_impl_wrapper: string return ──────────────────────────────────────

static std::string string_ok_impl() { return "ok"; }

TEST(ColumnarImpl, StringReturn) {
    auto* buf = make_col_fixed(3, COL_FIXED8, {});
    auto* result = columnar_impl_wrapper(buf, 3, string_ok_impl);

    auto cb = parse_columnar(result);
    auto col = cb.col(0);
    auto s0 = col.get_bytes(0);
    auto s1 = col.get_bytes(1);
    auto s2 = col.get_bytes(2);
    EXPECT_EQ(std::string(s0.begin(), s0.end()), "ok");
    EXPECT_EQ(std::string(s1.begin(), s1.end()), "ok");
    EXPECT_EQ(std::string(s2.begin(), s2.end()), "ok");

    clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(result));
}

// ── columnar_impl_wrapper: int32 return ───────────────────────────────────────

static int32_t int32_identity_impl(int32_t x) { return x; }

TEST(ColumnarImpl, Int32Return) {
    std::vector<uint8_t> col_data;
    int32_t vals[] = {100, -200, 0};
    for (auto v : vals)
        col_data.insert(col_data.end(),
                        reinterpret_cast<const uint8_t*>(&v),
                        reinterpret_cast<const uint8_t*>(&v) + sizeof(int32_t));

    auto* buf = make_col_fixed(3, COL_FIXED32, col_data);
    auto* result = columnar_impl_wrapper(buf, 3, int32_identity_impl);

    auto cb = parse_columnar(result);
    auto col = cb.col(0);
    EXPECT_EQ(col.row_count, 3u);

    int32_t r0, r1, r2;
    std::memcpy(&r0, col.data, 4);
    std::memcpy(&r1, col.data + 4, 4);
    std::memcpy(&r2, col.data + 8, 4);
    EXPECT_EQ(r0, 100);
    EXPECT_EQ(r1, -200);
    EXPECT_EQ(r2, 0);

    clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(result));
}

// ── columnar_impl_wrapper: uint32 return ──────────────────────────────────────

static uint32_t uint32_identity_impl(uint32_t x) { return x; }

TEST(ColumnarImpl, Uint32Return) {
    std::vector<uint8_t> col_data;
    uint32_t vals[] = {100u, 200u, 0u};
    for (auto v : vals)
        col_data.insert(col_data.end(),
                        reinterpret_cast<const uint8_t*>(&v),
                        reinterpret_cast<const uint8_t*>(&v) + sizeof(uint32_t));

    auto* buf = make_col_fixed(3, COL_FIXED32, col_data);
    auto* result = columnar_impl_wrapper(buf, 3, uint32_identity_impl);

    auto cb = parse_columnar(result);
    auto col = cb.col(0);
    EXPECT_EQ(col.row_count, 3u);

    uint32_t r0, r1, r2;
    std::memcpy(&r0, col.data, 4);
    std::memcpy(&r1, col.data + 4, 4);
    std::memcpy(&r2, col.data + 8, 4);
    EXPECT_EQ(r0, 100u);
    EXPECT_EQ(r1, 200u);
    EXPECT_EQ(r2, 0u);

    clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(result));
}

// ── columnar_impl_wrapper: st_dwithin ─────────────────────────────────────────

TEST(ColumnarImpl, BoolAConstDist_MatchesBaseline) {
    auto origin  = wkt2wkb("POINT (0 0)");
    auto near_pt = wkt2wkb("POINT (3 0)");
    auto far_pt  = wkt2wkb("POINT (10 0)");
    constexpr double kDist = 5.0;
    const uint32_t n = 2;

    std::vector<double> col2_vals(n, kDist);
    std::vector<ch::Vector> col0_aconst(n, origin);

    auto* buf = make_col_geom3(n, col0_aconst, {near_pt, far_pt}, col2_vals, 0);
    auto got = read_col_bool(
        columnar_impl_wrapper(buf, n, st_dwithin_impl,
            nullptr, false, nullptr, nullptr,
            prep_a_st_dwithin, prep_b_st_dwithin),
        n);
    clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(buf));

    EXPECT_EQ(got[0], 1u);
    EXPECT_EQ(got[1], 0u);

    std::vector<ch::Vector> col0_base = {origin, origin};
    auto* buf_base = make_col_geom3(n, col0_base, {near_pt, far_pt}, col2_vals);
    auto base = read_col_bool(
        columnar_impl_wrapper(buf_base, n, st_dwithin_impl,
            nullptr, false, nullptr, nullptr,
            prep_a_st_dwithin, prep_b_st_dwithin),
        n);
    clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(buf_base));

    EXPECT_EQ(got, base);
}

TEST(ColumnarImpl, BoolBConstDist_MatchesBaseline) {
    auto near_pt = wkt2wkb("POINT (3 0)");
    auto far_pt  = wkt2wkb("POINT (10 0)");
    auto origin  = wkt2wkb("POINT (0 0)");
    constexpr double kDist = 5.0;
    const uint32_t n = 2;

    std::vector<double> col2_vals(n, kDist);
    std::vector<ch::Vector> col1_bconst(n, origin);

    auto* buf = make_col_geom3(n, {near_pt, far_pt}, col1_bconst, col2_vals, 1);
    auto got = read_col_bool(
        columnar_impl_wrapper(buf, n, st_dwithin_impl,
            nullptr, false, nullptr, nullptr,
            prep_a_st_dwithin, prep_b_st_dwithin),
        n);
    clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(buf));

    EXPECT_EQ(got[0], 1u);
    EXPECT_EQ(got[1], 0u);

    std::vector<ch::Vector> col1_base = {origin, origin};
    auto* buf_base = make_col_geom3(n, {near_pt, far_pt}, col1_base, col2_vals);
    auto base = read_col_bool(
        columnar_impl_wrapper(buf_base, n, st_dwithin_impl,
            nullptr, false, nullptr, nullptr,
            prep_a_st_dwithin, prep_b_st_dwithin),
        n);
    clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(buf_base));

    EXPECT_EQ(got, base);
}

// ── COL_BYTES string column ───────────────────────────────────────────────────

TEST(ColumnarImpl, StringCol_GetBytes) {
    // Build: offsets = {0, 6, 12}, data = "hello\0world\0"
    std::vector<uint32_t> offsets = {0u, 6u, 12u};
    std::vector<uint8_t> data;
    data.insert(data.end(), "hello", "hello" + 5);
    data.push_back(0);
    data.insert(data.end(), "world", "world" + 5);
    data.push_back(0);

    auto* buf = make_col_string(2, data, offsets);

    auto cb = parse_columnar(buf);
    auto col = cb.col(0);
    EXPECT_EQ(col.base_type, COL_BYTES);
    EXPECT_EQ(col.row_count, 2u);

    auto s0 = col.get_bytes(0);
    auto s1 = col.get_bytes(1);
    EXPECT_EQ(std::string(s0.begin(), s0.end()), "hello");
    EXPECT_EQ(std::string(s1.begin(), s1.end()), "world");

    clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(buf));
}

// ── COL_IS_CONST flag ─────────────────────────────────────────────────────────

TEST(ColumnarImpl, ConstFlag) {
    auto poly = wkt2wkb(kSquare);
    const uint32_t n = 5;

    // Build const geom column: 1 row of WKB with null terminator
    std::vector<uint32_t> offsets = {0u, static_cast<uint32_t>(poly.size() + 1u)};
    std::vector<uint8_t> data;
    for (uint32_t o : offsets) write_le32(data, o);
    data.insert(data.end(), poly.begin(), poly.end());
    data.push_back(0);

    raw_buffer* buf = clickhouse_create_buffer(0);
    buf->clear();
    write_le32(*buf, n);
    write_le32(*buf, 1u);

    uint32_t off_off = 8u + 20u;
    uint32_t data_off = off_off + 2u * 4u;

    // ColDescriptor with COL_IS_CONST
    write_le32(*buf, static_cast<uint32_t>(COL_BYTES | COL_IS_CONST));
    write_le32(*buf, 0u);
    write_le32(*buf, off_off);
    write_le32(*buf, data_off);
    write_le32(*buf, static_cast<uint32_t>(data.size()));

    for (uint32_t o : offsets) write_le32(*buf, o);
    buf->append(data.data(), static_cast<uint32_t>(data.size()));

    auto cb = parse_columnar(buf);
    auto col = cb.col(0);
    EXPECT_TRUE(col.is_const);
    EXPECT_EQ(col.row_count, 1u);
    EXPECT_EQ(col.get_bytes(0).size(), poly.size());
    EXPECT_EQ(col.get_bytes(4).size(), poly.size()); // same data for all rows

    clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(buf));
}
