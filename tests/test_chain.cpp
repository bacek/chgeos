#include <gtest/gtest.h>
#include <bit>
#include <cstring>
#include <cmath>
#include <vector>
#include <string>

#include "helpers.hpp"
#include "chain.hpp"
#include "functions.hpp"
#include "functions/flat.hpp"

using namespace ch;

// ── One-time chain registration ───────────────────────────────────────────────
// main.cpp is not compiled into the test binary, so we register here.

static bool chain_registered = [] {
    CH_CHAIN_SOURCE(st_makeline);
    CH_CHAIN_SOURCE(st_makebox2d);
    CH_CHAIN_XFORM(st_convexhull);
    CH_CHAIN_XFORM(st_envelope);
    CH_CHAIN_XFORM(st_reverse);
    CH_CHAIN_XFORM_D(st_buffer);
    CH_CHAIN_XFORM_DD(st_translate);
    CH_CHAIN_SINK(st_length);
    CH_CHAIN_SINK(st_area);
    CH_CHAIN_SINK(st_astext);
    CH_CHAIN_SINK(st_isempty);
    CH_CHAIN_SINK(st_npoints);

    // Same flat variants main.cpp registers, so the tests below exercise the
    // dispatch the server actually runs.
    CH_CHAIN_FLAT_SOURCE(st_makeline,  flat_source_st_makeline);
    CH_CHAIN_FLAT_SOURCE(st_makebox2d, flat_source_st_makebox2d);
    CH_CHAIN_FLAT_SOURCE(st_reverse,   flat_source_st_reverse);
    CH_CHAIN_FLAT_XFORM(st_reverse,    flat_xform_st_reverse);
    CH_CHAIN_FLAT_XFORM(st_translate,  flat_xform_st_translate);
    CH_CHAIN_FLAT_SINK(st_length,      flat_sink_st_length);
    CH_CHAIN_FLAT_SINK(st_npoints,     flat_sink_st_npoints);
    CH_CHAIN_FLAT_SINK(st_isempty,     flat_sink_st_isempty);
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
    std::vector<uint64_t> offsets;
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
            pos = (pos + 7u) & ~7u;
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
                        cols[i].offsets.size() * 8);
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
        col.offsets.push_back(static_cast<uint64_t>(col.data.size()));
    }
    return col;
}

static ColData null_bytes_col(bool is_const,
                               const std::vector<Vector>& wkbs,
                               const std::vector<uint8_t>& nulls) {
    ColData col;
    col.col_type = static_cast<uint32_t>(COL_BYTES | COL_IS_NULLABLE)
                 | (is_const ? static_cast<uint32_t>(COL_IS_CONST) : 0u);
    col.null_map = nulls;
    col.offsets.push_back(0u);
    for (size_t i = 0; i < wkbs.size(); ++i) {
        if (!nulls[i])
            col.data.insert(col.data.end(), wkbs[i].begin(), wkbs[i].end());
        col.offsets.push_back(static_cast<uint64_t>(col.data.size()));
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

// Read an int32 output column.
static std::vector<int32_t> read_i32_col(raw_buffer* out, uint32_t n) {
    std::vector<int32_t> res(n);
    std::memcpy(res.data(), out->data() + HEADER_BYTES + COL_DESC_BYTES, n * 4u);
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

// ── Flat currency ─────────────────────────────────────────────────────────────
// The flat path claims to be the same arithmetic as the GEOS path, not an
// approximation of it, so these compare bit patterns against the composition
// they replace rather than against literals.  A change to either side shows up.

static double generic_makeline_length(const Vector& a, const Vector& b) {
    return st_length_impl(st_makeline_impl(read_wkb(wkb(a)), read_wkb(wkb(b))));
}

// Runs the two-stage chain end to end, exactly as the server would.
static std::vector<double> run_makeline_length(const std::vector<Vector>& as,
                                               const std::vector<Vector>& bs) {
    uint32_t n = static_cast<uint32_t>(as.size());
    auto* col_buf = make_columnar(n, {bytes_col(false, as), bytes_col(false, bs)});
    auto* chain   = make_chain_descriptor({"st_makeline", "st_length"});
    auto* out     = chain_execute_impl(chain, col_buf, n);
    clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(chain));
    clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(col_buf));
    return read_f64_col(out, n);
}

static void expect_bit_equal(double got, double want, const std::string& what) {
    EXPECT_EQ(std::bit_cast<uint64_t>(got), std::bit_cast<uint64_t>(want)) << what;
}

TEST(ChainFlat, MakelineLength_MatchesGeosPath) {
    const std::vector<std::pair<std::string, std::string>> pairs = {
        {"POINT (0 0)",               "POINT (3 4)"},                 // 3-4-5
        {"POINT (0 0)",               "POINT (0 0)"},                 // zero length
        {"POINT (-111.7610 34.8697)", "POINT (-111.9060 35.0047)"},   // benchmark scale
        {"POINT (1e-9 1e-9)",         "POINT (-1e-9 -1e-9)"},         // tiny
        {"POINT (1e150 1e150)",       "POINT (-1e150 -1e150)"},       // squares to inf
        {"POINT (-5 -5)",             "POINT (5 5)"},                 // sign crossing
    };

    std::vector<Vector> as, bs;
    for (const auto& [wa, wb] : pairs) {
        as.push_back(wkt2wkb(wa));
        bs.push_back(wkt2wkb(wb));
    }

    auto got = run_makeline_length(as, bs);
    for (size_t i = 0; i < pairs.size(); ++i)
        expect_bit_equal(got[i], generic_makeline_length(as[i], bs[i]),
                         pairs[i].first + " -> " + pairs[i].second);
}

TEST(ChainFlat, MakelineLength_NonPointDeclinesWholeBlock) {
    // st_makeline over a LINESTRING has merge semantics the flat source does not
    // express, so it declines and the whole block runs on GEOS.  The point row in
    // the same block must come out identical either way.
    std::vector<Vector> as = {wkt2wkb("LINESTRING (0 0, 1 0)"), wkt2wkb("POINT (0 0)")};
    std::vector<Vector> bs = {wkt2wkb("POINT (1 1)"),           wkt2wkb("POINT (3 4)")};

    auto* probe = make_columnar(2, {bytes_col(false, as), bytes_col(false, bs)});
    EXPECT_FALSE(flat_source_st_makeline(parse_columnar(probe), 2).has_value())
        << "non-POINT input must decline";
    clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(probe));

    auto got = run_makeline_length(as, bs);
    expect_bit_equal(got[0], generic_makeline_length(as[0], bs[0]), "linestring row");
    expect_bit_equal(got[1], generic_makeline_length(as[1], bs[1]), "point row");
}

// Hand-built so the Z ordinate is guaranteed present, whatever output dimension
// the WKB writer happens to use.  Little-endian host assumed, as everywhere else
// in these tests.
static Vector wkb_point_z(double x, double y, double z) {
    Vector v(5);
    v[0] = 0x01;
    uint32_t type = 1001;  // ISO WKB PointZ
    std::memcpy(v.data() + 1, &type, 4);
    for (double d : {x, y, z}) {
        size_t off = v.size();
        v.resize(off + 8);
        std::memcpy(v.data() + off, &d, 8);
    }
    return v;
}

TEST(ChainFlat, MakelineLength_IgnoresZ) {
    // GEOS measures LineString length in 2D, so a Z ordinate must not reach the
    // answer on either path.
    std::vector<Vector> as = {wkb_point_z(0, 0, 100)};
    std::vector<Vector> bs = {wkb_point_z(3, 4, -100)};

    auto got = run_makeline_length(as, bs);
    expect_bit_equal(got[0], generic_makeline_length(as[0], bs[0]), "PointZ");
    EXPECT_DOUBLE_EQ(got[0], 5.0);
}

TEST(ChainFlat, MakelineLength_NullProducesNaN) {
    // chain_sink_run turns a null handle into NaN; the flat sink must too.
    auto p00 = wkt2wkb("POINT (0 0)");
    auto p34 = wkt2wkb("POINT (3 4)");

    auto* col_buf = make_columnar(2, {
        null_bytes_col(false, {p00, p00}, {0, 1}),
        bytes_col(false, {p34, p34}),
    });
    auto* chain = make_chain_descriptor({"st_makeline", "st_length"});
    auto* out   = chain_execute_impl(chain, col_buf, 2);
    clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(chain));
    clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(col_buf));

    auto vals = read_f64_col(out, 2);
    EXPECT_DOUBLE_EQ(vals[0], 5.0);
    EXPECT_TRUE(std::isnan(vals[1]));
}

TEST(ChainFlat, DispatchResolvesWireSuffixes) {
    auto p00 = wkt2wkb("POINT (0 0)");
    auto p34 = wkt2wkb("POINT (3 4)");

    auto* col_buf = make_columnar(1, {bytes_col(false, {p00}), bytes_col(false, {p34})});
    auto* chain = make_chain_descriptor({"st_makeline_col", "st_length_col"});
    auto* out = chain_execute_impl(chain, col_buf, 1);
    clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(chain));
    clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(col_buf));

    EXPECT_DOUBLE_EQ(read_f64_col(out, 1)[0], 5.0);
}

TEST(ChainFlat, ChainWithNonFlatStageStaysOnGeos) {
    // st_convexhull has no flat variant, so this chain must not attempt the flat
    // path at all — and must still produce the GEOS answer.
    EXPECT_FALSE(chain_all_flat({"st_makeline", "st_convexhull", "st_length"}));
    EXPECT_TRUE(chain_all_flat({"st_makeline", "st_length"}));
}

// ── Flat currency: generic geometry source and the Tier 1 stages ──────────────
// These chains all head on a plain geometry column, which flat_read_column feeds,
// so they cover the generic source as well as the stage under test.

// A spread of shapes wide enough that every branch of the WKB walk is hit.
static const std::vector<std::string>& tier1_geoms() {
    static const std::vector<std::string> geoms = {
        "POINT (1 2)",
        "POINT EMPTY",
        "LINESTRING (0 0, 3 4, 3 0)",
        "LINESTRING EMPTY",
        "POLYGON ((0 0, 4 0, 4 3, 0 3, 0 0))",
        "POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0), (2 2, 4 2, 4 4, 2 4, 2 2))",
        "MULTIPOINT ((0 0), (5 5))",
        "MULTILINESTRING ((0 0, 3 4), (1 1, 4 5))",
        "MULTIPOLYGON (((0 0, 2 0, 2 2, 0 2, 0 0)), ((5 5, 7 5, 7 7, 5 7, 5 5)))",
        "GEOMETRYCOLLECTION (POINT (0 0), LINESTRING (0 0, 3 4))",
        "GEOMETRYCOLLECTION EMPTY",
    };
    return geoms;
}

static std::vector<Vector> tier1_wkbs() {
    std::vector<Vector> v;
    for (const auto& g : tier1_geoms()) v.push_back(wkt2wkb(g));
    return v;
}

// Runs a chain over one geometry column, optionally with two double constants.
static raw_buffer* run_geom_chain(const std::vector<std::string>& names,
                                  const std::vector<Vector>& geoms,
                                  std::vector<ColData> scalars = {}) {
    uint32_t n = static_cast<uint32_t>(geoms.size());
    std::vector<ColData> cols = {bytes_col(false, geoms)};
    for (auto& s : scalars) cols.push_back(s);

    auto* col_buf = make_columnar(n, std::move(cols));
    auto* chain   = make_chain_descriptor(names);
    auto* out     = chain_execute_impl(chain, col_buf, n);
    clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(chain));
    clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(col_buf));
    return out;
}

TEST(ChainFlat, ReverseLength_MatchesGeosPath) {
    auto geoms = tier1_wkbs();
    ASSERT_TRUE(chain_all_flat({"st_reverse", "st_length"}));

    // Agreeing with GEOS proves nothing if the flat source quietly declined and
    // GEOS answered both sides, so pin down that these shapes are accepted.
    auto* probe = make_columnar(static_cast<uint32_t>(geoms.size()),
                                {bytes_col(false, geoms)});
    ASSERT_TRUE(flat_read_column(parse_columnar(probe).col(0),
                                 static_cast<uint32_t>(geoms.size())).has_value());
    clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(probe));

    auto got = read_f64_col(run_geom_chain({"st_reverse", "st_length"}, geoms),
                            static_cast<uint32_t>(geoms.size()));
    for (size_t i = 0; i < geoms.size(); ++i)
        expect_bit_equal(got[i],
                         st_length_impl(st_reverse_impl(read_wkb(wkb(geoms[i])))),
                         tier1_geoms()[i]);
}

TEST(ChainFlat, ReverseIsAnInvolutionOnCoordinates) {
    // Two reversals must restore the original coordinate order exactly, which a
    // length sink alone cannot show — length is symmetric under reversal.
    FlatBatch once, twice;
    for (FlatBatch* fb : {&once, &twice}) {
        fb->reserve_rows(1);
        fb->begin_row(true);
        ASSERT_TRUE(flat_append_wkb(*fb, wkb(wkt2wkb(
            "POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0), (2 2, 4 2, 4 4, 2 4, 2 2))"))));
        fb->finish();
    }
    std::vector<double> original = once.xy;

    flat_xform_st_reverse(once, {});
    EXPECT_NE(once.xy, original) << "reversal must actually move coordinates";

    flat_xform_st_reverse(twice, {});
    flat_xform_st_reverse(twice, {});
    EXPECT_EQ(twice.xy, original);
}

TEST(ChainFlat, TranslateLength_MatchesGeosPath) {
    auto geoms = tier1_wkbs();
    const double dx = -111.75, dy = 34.5;
    ASSERT_TRUE(chain_all_flat({"st_reverse", "st_translate", "st_length"}));

    auto got = read_f64_col(
        run_geom_chain({"st_reverse", "st_translate", "st_length"}, geoms,
                       {fixed64_col(dx), fixed64_col(dy)}),
        static_cast<uint32_t>(geoms.size()));

    for (size_t i = 0; i < geoms.size(); ++i)
        expect_bit_equal(got[i],
                         st_length_impl(st_translate_impl(
                             st_reverse_impl(read_wkb(wkb(geoms[i]))), dx, dy)),
                         tier1_geoms()[i]);
}

TEST(ChainFlat, NPoints_MatchesGeosPath) {
    auto geoms = tier1_wkbs();
    ASSERT_TRUE(chain_all_flat({"st_reverse", "st_npoints"}));

    auto got = read_i32_col(run_geom_chain({"st_reverse", "st_npoints"}, geoms),
                            static_cast<uint32_t>(geoms.size()));
    for (size_t i = 0; i < geoms.size(); ++i)
        EXPECT_EQ(got[i], st_npoints_impl(st_reverse_impl(read_wkb(wkb(geoms[i])))))
            << tier1_geoms()[i];
}

TEST(ChainFlat, IsEmpty_MatchesGeosPath) {
    auto geoms = tier1_wkbs();
    ASSERT_TRUE(chain_all_flat({"st_reverse", "st_isempty"}));

    auto got = read_bool_col(run_geom_chain({"st_reverse", "st_isempty"}, geoms),
                             static_cast<uint32_t>(geoms.size()));
    for (size_t i = 0; i < geoms.size(); ++i)
        EXPECT_EQ(got[i] != 0,
                  st_isempty_impl(st_reverse_impl(read_wkb(wkb(geoms[i])))))
            << tier1_geoms()[i];
}

TEST(ChainFlat, NullRowMatchesSinkConventions) {
    // chain_sink_run gives a null handle 0 for both an int and a bool sink; the
    // flat sinks must not report a null row as an empty geometry with 0 points.
    auto pt = wkt2wkb("POINT (1 2)");
    auto* col_buf = make_columnar(2, {null_bytes_col(false, {pt, pt}, {0, 1})});
    auto* chain   = make_chain_descriptor({"st_reverse", "st_isempty"});
    auto* out     = chain_execute_impl(chain, col_buf, 2);
    clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(chain));
    clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(col_buf));

    auto vals = read_bool_col(out, 2);
    EXPECT_EQ(vals[0], 0u);
    EXPECT_EQ(vals[1], 0u) << "null row must be 0, not treated as empty";
}

// Runs st_makebox2d(a, b) into a sink, exactly as the server would.
static raw_buffer* run_box_chain(const std::string& sink,
                                 const std::vector<Vector>& as,
                                 const std::vector<Vector>& bs) {
    uint32_t n = static_cast<uint32_t>(as.size());
    auto* col_buf = make_columnar(n, {bytes_col(false, as), bytes_col(false, bs)});
    auto* chain   = make_chain_descriptor({"st_makebox2d", sink});
    auto* out     = chain_execute_impl(chain, col_buf, n);
    clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(chain));
    clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(col_buf));
    return out;
}

TEST(ChainFlat, MakeBox2d_MatchesGeosPath) {
    // Corners in every relative order, since Envelope sorts them itself.
    const std::vector<std::pair<std::string, std::string>> pairs = {
        {"POINT (0 0)",  "POINT (4 3)"},
        {"POINT (4 3)",  "POINT (0 0)"},
        {"POINT (-5 3)", "POINT (5 -3)"},
        {"POINT (-111.7610 34.8697)", "POINT (-111.9060 35.0047)"},
    };
    std::vector<Vector> as, bs;
    for (const auto& [wa, wb] : pairs) {
        as.push_back(wkt2wkb(wa));
        bs.push_back(wkt2wkb(wb));
    }
    uint32_t n = static_cast<uint32_t>(pairs.size());

    auto len = read_f64_col(run_box_chain("st_length", as, bs), n);
    auto pts = read_i32_col(run_box_chain("st_npoints", as, bs), n);
    for (uint32_t i = 0; i < n; ++i) {
        auto geos = [&] {
            return st_makebox2d_impl(read_wkb(wkb(as[i])), read_wkb(wkb(bs[i])));
        };
        expect_bit_equal(len[i], st_length_impl(geos()), pairs[i].first);
        EXPECT_EQ(pts[i], st_npoints_impl(geos())) << pairs[i].first;
    }
}

TEST(ChainFlat, MakeBox2d_DegenerateAndNonPointDecline) {
    auto p00 = wkt2wkb("POINT (0 0)");
    auto p03 = wkt2wkb("POINT (0 3)");   // zero width  -> GEOS gives a LINESTRING
    auto p43 = wkt2wkb("POINT (4 3)");
    auto line = wkt2wkb("LINESTRING (0 0, 1 1)");

    auto declines = [](const std::vector<Vector>& as, const std::vector<Vector>& bs) {
        auto* probe = make_columnar(static_cast<uint32_t>(as.size()),
                                    {bytes_col(false, as), bytes_col(false, bs)});
        bool ok = flat_source_st_makebox2d(parse_columnar(probe),
                                           static_cast<uint32_t>(as.size())).has_value();
        clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(probe));
        return !ok;
    };

    EXPECT_TRUE(declines({p00}, {p03})) << "zero-width box is not a polygon";
    EXPECT_TRUE(declines({p00}, {p00})) << "zero-area box is not a polygon";
    EXPECT_TRUE(declines({line}, {p43})) << "non-POINT input";
    EXPECT_FALSE(declines({p00}, {p43}));

    // A declining row must not corrupt the block: the GEOS path still answers.
    std::vector<Vector> as = {p00, p00}, bs = {p03, p43};
    auto len = read_f64_col(run_box_chain("st_length", as, bs), 2);
    for (uint32_t i = 0; i < 2; ++i)
        expect_bit_equal(len[i],
                         st_length_impl(st_makebox2d_impl(read_wkb(wkb(as[i])),
                                                          read_wkb(wkb(bs[i])))),
                         "row " + std::to_string(i));
}

// ── FlatBatch: WKB walk ───────────────────────────────────────────────────────
// flat_append_wkb is what future stages will build on, so it is tested against
// GEOS directly rather than only through the one chain that uses it today.

static double flat_length_of(const std::string& wkt) {
    FlatBatch fb;
    fb.reserve_rows(1);
    fb.begin_row(true);
    EXPECT_TRUE(flat_append_wkb(fb, wkb(wkt2wkb(wkt)))) << wkt;
    fb.finish();
    return read_f64_col(flat_sink_st_length(fb, 1), 1)[0];
}

TEST(FlatBatch, LengthMatchesGeosAcrossGeometryTypes) {
    const std::vector<std::string> geoms = {
        "POINT (1 2)",
        "LINESTRING (0 0, 3 4, 3 0)",
        "POLYGON ((0 0, 4 0, 4 3, 0 3, 0 0))",
        "POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0), (2 2, 4 2, 4 4, 2 4, 2 2))",
        "MULTIPOINT ((0 0), (5 5))",
        "MULTILINESTRING ((0 0, 3 4), (1 1, 4 5))",
        "MULTIPOLYGON (((0 0, 2 0, 2 2, 0 2, 0 0)), ((5 5, 7 5, 7 7, 5 7, 5 5)))",
        "GEOMETRYCOLLECTION (POINT (0 0), LINESTRING (0 0, 3 4))",
    };

    for (const auto& g : geoms)
        expect_bit_equal(flat_length_of(g), st_length_impl(read_wkb(wkb(wkt2wkb(g)))), g);
}

TEST(FlatBatch, EmptyRowRangeForAbsentRow) {
    FlatBatch fb;
    fb.reserve_rows(2);
    fb.begin_row(false);
    fb.begin_row(true);
    fb.begin_ring();
    fb.push_vertex(0, 0);
    fb.push_vertex(3, 4);
    fb.finish();

    EXPECT_EQ(fb.ring_begin(0), fb.ring_end(0));
    EXPECT_EQ(fb.ring_end(1) - fb.ring_begin(1), 1u);
    EXPECT_EQ(fb.vertex_end(fb.ring_begin(1)) - fb.vertex_begin(fb.ring_begin(1)), 2u);
}
