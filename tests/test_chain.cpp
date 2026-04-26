#include <gtest/gtest.h>
#include <cstring>
#include <cmath>
#include <vector>
#include <string>

#include "helpers.hpp"
#include "chain.hpp"
#include "functions.hpp"

using namespace ch;

// ── One-time chain registration ───────────────────────────────────────────────
// main.cpp is not compiled into the test binary, so we register here.

static bool chain_registered = [] {
    CH_CHAIN_SOURCE(st_makeline);
    CH_CHAIN_XFORM(st_convexhull);
    CH_CHAIN_XFORM(st_envelope);
    CH_CHAIN_XFORM_D(st_buffer);
    CH_CHAIN_SINK(st_length);
    CH_CHAIN_SINK(st_area);
    CH_CHAIN_SINK(st_astext);
    CH_CHAIN_SINK(st_isempty);
    return true;
}();

// ── Chain descriptor builder ──────────────────────────────────────────────────
// Produces: [n_funcs: u32][cstr name_0]...[cstr name_n-1]

static raw_buffer* make_chain_descriptor(const std::vector<std::string>& names) {
    size_t sz = 4;  // n_funcs
    for (auto& n : names) sz += n.size() + 1;

    raw_buffer* out = clickhouse_create_buffer(static_cast<uint32_t>(sz));
    out->resize(sz);
    uint8_t* p = out->data();

    uint32_t n = static_cast<uint32_t>(names.size());
    std::memcpy(p, &n, 4);
    p += 4;
    for (auto& name : names) {
        std::memcpy(p, name.c_str(), name.size() + 1);
        p += name.size() + 1;
    }
    return out;
}

// Reuse make_columnar / bytes_col from test_columnar.cpp via a minimal local
// copy (only what we need).

struct ColData {
    uint32_t              col_type;
    std::vector<uint8_t>  null_map;
    std::vector<uint32_t> offsets;
    std::vector<uint8_t>  data;
};

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
            pos = (pos + 3u) & ~3u;
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
            std::memcpy(p + bi[i].offsets_off, cols[i].offsets.data(),
                        cols[i].offsets.size() * 4);
        if (!cols[i].data.empty())
            std::memcpy(p + bi[i].data_off, cols[i].data.data(), cols[i].data.size());
    }
    return buf;
}

static ColData fixed64_col(double value) {
    ColData col;
    col.col_type = static_cast<uint32_t>(COL_FIXED64) | static_cast<uint32_t>(COL_IS_CONST);
    col.data.resize(8);
    std::memcpy(col.data.data(), &value, 8);
    return col;
}

static ColData bytes_col(bool is_const, const std::vector<Vector>& wkbs) {
    ColData col;
    col.col_type = static_cast<uint32_t>(COL_BYTES)
                 | (is_const ? static_cast<uint32_t>(COL_IS_CONST) : 0u);
    col.offsets.push_back(0u);
    for (auto& w : wkbs) {
        col.data.insert(col.data.end(), w.begin(), w.end());
        col.data.push_back(0u);
        col.offsets.push_back(static_cast<uint32_t>(col.data.size()));
    }
    return col;
}

static ColData null_bytes_col(bool is_const,
                               const std::vector<Vector>& wkbs,
                               const std::vector<uint8_t>& nulls) {
    ColData col;
    col.col_type = static_cast<uint32_t>(COL_NULL_BYTES)
                 | (is_const ? static_cast<uint32_t>(COL_IS_CONST) : 0u);
    col.null_map = nulls;
    col.offsets.push_back(0u);
    for (size_t i = 0; i < wkbs.size(); ++i) {
        if (nulls[i]) {
            col.data.push_back(0u);
        } else {
            col.data.insert(col.data.end(), wkbs[i].begin(), wkbs[i].end());
            col.data.push_back(0u);
        }
        col.offsets.push_back(static_cast<uint32_t>(col.data.size()));
    }
    return col;
}

// Read a double output column.
static std::vector<double> read_f64_col(raw_buffer* out, uint32_t n) {
    std::vector<double> res(n);
    std::memcpy(res.data(), out->data() + HEADER_BYTES + COL_DESC_BYTES, n * 8u);
    clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(out));
    return res;
}

// Read a bool (uint8) output column.
static std::vector<uint8_t> read_bool_col(raw_buffer* out, uint32_t n) {
    std::vector<uint8_t> res(n);
    std::memcpy(res.data(), out->data() + HEADER_BYTES + COL_DESC_BYTES, n);
    clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(out));
    return res;
}

// Read the WKT string from a COL_BYTES output at row i.
static std::string read_bytes_row(raw_buffer* out, uint32_t i) {
    auto cb = parse_columnar(out);
    auto col = cb.col(0);
    auto sp  = col.get_bytes(i);
    std::string s(reinterpret_cast<const char*>(sp.data()), sp.size());
    clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(out));
    return s;
}

// ── validate_chain tests ──────────────────────────────────────────────────────

TEST(ChainValidate, ValidSourceSink) {
    EXPECT_TRUE(validate_chain({"st_makeline", "st_length"}));
}

TEST(ChainValidate, ValidSourceXformSink) {
    EXPECT_TRUE(validate_chain({"st_makeline", "st_convexhull", "st_area"}));
}

TEST(ChainValidate, TooShort) {
    EXPECT_FALSE(validate_chain({"st_makeline"}));
    EXPECT_FALSE(validate_chain({}));
}

TEST(ChainValidate, WrongRoleOrder_SinkFirst) {
    EXPECT_FALSE(validate_chain({"st_length", "st_makeline"}));
}

// XFORM with as_source can head a chain (enables short chains where the SOURCE
// is not chain-registered, e.g. st_collect_agg → st_convexhull → st_area).
TEST(ChainValidate, XformWithSource_HeadsChain) {
    EXPECT_TRUE(validate_chain({"st_convexhull", "st_length"}));
}

// XFORM without as_source (e.g. CH_CHAIN_XFORM_D) cannot head a chain.
TEST(ChainValidate, XformWithoutSource_CannotHead) {
    EXPECT_FALSE(validate_chain({"st_buffer", "st_length"}));
}

TEST(ChainValidate, UnknownFunction) {
    EXPECT_FALSE(validate_chain({"st_makeline", "st_nonexistent"}));
}

// ── chain_execute_impl: SOURCE → SINK ────────────────────────────────────────

TEST(ChainExecute, MakelineLength_3_4_5) {
    // POINT(0 0) → POINT(3 4): line length = 5.0 (Pythagorean triple).
    auto p00 = wkt2wkb("POINT (0 0)");
    auto p34 = wkt2wkb("POINT (3 4)");

    auto* col_buf  = make_columnar(1, {
        bytes_col(false, {p00}),
        bytes_col(false, {p34}),
    });
    auto* chain = make_chain_descriptor({"st_makeline", "st_length"});
    auto* out = chain_execute_impl(chain, col_buf, 1);
    clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(chain));
    clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(col_buf));

    auto vals = read_f64_col(out, 1);
    EXPECT_DOUBLE_EQ(vals[0], 5.0);
}

TEST(ChainExecute, MakelineLength_MultiRow) {
    // Row 0: (0,0)→(1,0) len=1. Row 1: (0,0)→(0,3) len=3.
    auto p00 = wkt2wkb("POINT (0 0)");
    auto p10 = wkt2wkb("POINT (1 0)");
    auto p03 = wkt2wkb("POINT (0 3)");

    auto* col_buf = make_columnar(2, {
        bytes_col(false, {p00, p00}),
        bytes_col(false, {p10, p03}),
    });
    auto* chain = make_chain_descriptor({"st_makeline", "st_length"});
    auto* out = chain_execute_impl(chain, col_buf, 2);
    clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(chain));
    clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(col_buf));

    auto vals = read_f64_col(out, 2);
    EXPECT_DOUBLE_EQ(vals[0], 1.0);
    EXPECT_DOUBLE_EQ(vals[1], 3.0);
}

TEST(ChainExecute, MakelineArea_IsZero) {
    // Linestring has no area.
    auto p00 = wkt2wkb("POINT (0 0)");
    auto p11 = wkt2wkb("POINT (1 1)");

    auto* col_buf = make_columnar(1, {
        bytes_col(false, {p00}),
        bytes_col(false, {p11}),
    });
    auto* chain = make_chain_descriptor({"st_makeline", "st_area"});
    auto* out = chain_execute_impl(chain, col_buf, 1);
    clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(chain));
    clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(col_buf));

    auto vals = read_f64_col(out, 1);
    EXPECT_DOUBLE_EQ(vals[0], 0.0);
}

TEST(ChainExecute, MakelineAstext) {
    auto p00 = wkt2wkb("POINT (0 0)");
    auto p10 = wkt2wkb("POINT (1 0)");

    auto* col_buf = make_columnar(1, {
        bytes_col(false, {p00}),
        bytes_col(false, {p10}),
    });
    auto* chain = make_chain_descriptor({"st_makeline", "st_astext"});
    auto* out = chain_execute_impl(chain, col_buf, 1);
    clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(chain));
    clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(col_buf));

    // Read from out manually before it's freed inside read_bytes_row.
    auto cb  = parse_columnar(out);
    auto col = cb.col(0);
    auto sp  = col.get_bytes(0);
    std::string s(reinterpret_cast<const char*>(sp.data()), sp.size());
    clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(out));

    EXPECT_EQ(s, "LINESTRING (0 0, 1 0)");
}

// ── chain_execute_impl: SOURCE → XFORM → SINK ────────────────────────────────

TEST(ChainExecute, MakelineConvexhullArea) {
    // Convex hull of a line is still a line → area = 0.
    auto p00 = wkt2wkb("POINT (0 0)");
    auto p11 = wkt2wkb("POINT (1 1)");

    auto* col_buf = make_columnar(1, {
        bytes_col(false, {p00}),
        bytes_col(false, {p11}),
    });
    auto* chain = make_chain_descriptor({"st_makeline", "st_convexhull", "st_area"});
    auto* out = chain_execute_impl(chain, col_buf, 1);
    clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(chain));
    clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(col_buf));

    auto vals = read_f64_col(out, 1);
    EXPECT_DOUBLE_EQ(vals[0], 0.0);
}

TEST(ChainExecute, MakelineEnvelopeIsEmpty) {
    // Envelope of a linestring is non-empty.
    auto p00 = wkt2wkb("POINT (0 0)");
    auto p11 = wkt2wkb("POINT (1 1)");

    auto* col_buf = make_columnar(1, {
        bytes_col(false, {p00}),
        bytes_col(false, {p11}),
    });
    auto* chain = make_chain_descriptor({"st_makeline", "st_envelope", "st_isempty"});
    auto* out = chain_execute_impl(chain, col_buf, 1);
    clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(chain));
    clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(col_buf));

    auto vals = read_bool_col(out, 1);
    EXPECT_EQ(vals[0], 0u);  // not empty
}

// ── Null propagation ──────────────────────────────────────────────────────────

TEST(ChainExecute, NullInput_ProducesNaN) {
    // Row 0: null point → chain should propagate null → NaN length.
    auto p00 = wkt2wkb("POINT (0 0)");
    auto p11 = wkt2wkb("POINT (1 1)");

    // col[0] has a null at row 0.
    auto* col_buf = make_columnar(2, {
        null_bytes_col(false, {p00, p00}, {0xFFu, 0}),  // row 0 null, row 1 ok
        bytes_col(false, {p11, p11}),
    });
    auto* chain = make_chain_descriptor({"st_makeline", "st_length"});
    auto* out = chain_execute_impl(chain, col_buf, 2);
    clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(chain));
    clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(col_buf));

    auto vals = read_f64_col(out, 2);
    EXPECT_TRUE(std::isnan(vals[0]));
    EXPECT_DOUBLE_EQ(vals[1], std::sqrt(2.0));
}

TEST(ChainExecute, BufferArea_ScalarXform) {
    // st_area(st_buffer(st_makeline(A, B), r)):
    // row_buf = [col_A, col_B, const_r].  The scalar goes in as COL_IS_CONST COL_FIXED64.
    auto p00 = wkt2wkb("POINT (0 0)");
    auto p10 = wkt2wkb("POINT (1 0)");

    auto* col_buf = make_columnar(1, {
        bytes_col(false, {p00}),   // col 0: geom A
        bytes_col(false, {p10}),   // col 1: geom B
        fixed64_col(0.5),          // col 2: buffer radius (COL_IS_CONST)
    });
    auto* chain = make_chain_descriptor({"st_makeline", "st_buffer", "st_area"});
    auto* out = chain_execute_impl(chain, col_buf, 1);
    clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(chain));
    clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(col_buf));

    auto vals = read_f64_col(out, 1);
    // Buffered linestring must have positive area.
    EXPECT_GT(vals[0], 0.0);
}
