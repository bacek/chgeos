#include <gtest/gtest.h>

#include "helpers.hpp"
#include "rowbinary.hpp"
#include "functions/predicates.hpp"

using namespace ch;

// ── Helpers ───────────────────────────────────────────────────────────────────

// Encode a VarUInt into a vector.
static void push_varuint(std::vector<uint8_t> & buf, uint64_t v) {
    while (v >= 0x80) {
        buf.push_back(static_cast<uint8_t>((v & 0x7F) | 0x80));
        v >>= 7;
    }
    buf.push_back(static_cast<uint8_t>(v));
}

// Append a WKT geometry as a RowBinary String field (varuint + raw WKB).
static void push_rb_geom(std::vector<uint8_t> & buf, const std::string & wkt) {
    auto wkb = wkt2wkb(wkt);
    push_varuint(buf, wkb.size());
    buf.insert(buf.end(), wkb.begin(), wkb.end());
}

// Build a RowBinary input raw_buffer for a 2-String-argument function.
// Each element of `rows` is a pair (wkt_a, wkt_b).
static raw_buffer * make_rb2(
    std::initializer_list<std::pair<std::string, std::string>> rows)
{
    std::vector<uint8_t> data;
    for (auto & [a, b] : rows) {
        push_rb_geom(data, a);
        push_rb_geom(data, b);
    }
    auto * buf = clickhouse_create_buffer(1);
    buf->resize(0);
    buf->append(data.data(), static_cast<uint32_t>(data.size()));
    return buf;
}

// ── rb_read_varuint ───────────────────────────────────────────────────────────

TEST(RbVarUInt, SingleByte) {
    std::vector<uint8_t> v = {42};
    const uint8_t * p = v.data();
    EXPECT_EQ(rb_read_varuint(p, v.data() + v.size()), 42u);
    EXPECT_EQ(p, v.data() + 1);
}

TEST(RbVarUInt, MultiByte) {
    // 300 = 0x12C → LEB128: 0xAC 0x02
    std::vector<uint8_t> v = {0xAC, 0x02};
    const uint8_t * p = v.data();
    EXPECT_EQ(rb_read_varuint(p, v.data() + v.size()), 300u);
    EXPECT_EQ(p, v.data() + 2);
}

TEST(RbVarUInt, Zero) {
    std::vector<uint8_t> v = {0x00};
    const uint8_t * p = v.data();
    EXPECT_EQ(rb_read_varuint(p, v.data() + v.size()), 0u);
}

TEST(RbVarUInt, Truncated) {
    std::vector<uint8_t> v = {0x80}; // continuation bit set, no more bytes
    const uint8_t * p = v.data();
    EXPECT_THROW(rb_read_varuint(p, v.data() + v.size()), std::runtime_error);
}

// ── rb_write_varuint ──────────────────────────────────────────────────────────

TEST(RbVarUInt, RoundTrip) {
    for (uint64_t v : {0ULL, 1ULL, 127ULL, 128ULL, 300ULL, 16383ULL, 16384ULL, 1000000ULL}) {
        auto * buf = clickhouse_create_buffer(16);
        buf->clear();
        rb_write_varuint(buf, v);
        const uint8_t * p = buf->begin();
        EXPECT_EQ(rb_read_varuint(p, buf->end()), v) << "roundtrip failed for " << v;
        clickhouse_destroy_buffer(reinterpret_cast<uint8_t *>(buf));
    }
}

// ── rb_unpack_arg (Span) ──────────────────────────────────────────────────────

TEST(RbUnpackArg, SpanZeroCopy) {
    std::vector<uint8_t> data = {0x03, 0xAA, 0xBB, 0xCC}; // len=3, then 3 bytes
    const uint8_t * p = data.data();
    std::span<const uint8_t> s;
    rb_unpack_arg(p, data.data() + data.size(), s);
    EXPECT_EQ(s.size(), 3u);
    EXPECT_EQ(s[0], 0xAA);
    EXPECT_EQ(s[1], 0xBB);
    EXPECT_EQ(s[2], 0xCC);
    EXPECT_EQ(p, data.data() + 4);
}

TEST(RbUnpackArg, SpanTruncated) {
    std::vector<uint8_t> data = {0x05, 0xAA}; // claims 5 bytes, only 1 available
    const uint8_t * p = data.data();
    std::span<const uint8_t> s;
    EXPECT_THROW(rb_unpack_arg(p, data.data() + data.size(), s), std::runtime_error);
}

// ── rb_pack_result (bool/UInt8) ───────────────────────────────────────────────

TEST(RbPackResult, BoolTrue) {
    auto * buf = clickhouse_create_buffer(4);
    buf->clear();
    rb_pack_result(buf, true);
    EXPECT_EQ(buf->size(), 1u);
    EXPECT_EQ((*buf)[0], 1u);
    clickhouse_destroy_buffer(reinterpret_cast<uint8_t *>(buf));
}

TEST(RbPackResult, BoolFalse) {
    auto * buf = clickhouse_create_buffer(4);
    buf->clear();
    rb_pack_result(buf, false);
    EXPECT_EQ(buf->size(), 1u);
    EXPECT_EQ((*buf)[0], 0u);
    clickhouse_destroy_buffer(reinterpret_cast<uint8_t *>(buf));
}

// ── rowbinary_impl_wrapper — st_intersects_extent ────────────────────────────

TEST(RowBinaryWrapper, IntersectingBoxes) {
    // Two overlapping unit squares — should return 1
    auto * in = make_rb2({
        {"POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0))",
         "POLYGON ((1 1, 3 1, 3 3, 1 3, 1 1))"},
    });
    auto * out = rowbinary_impl_wrapper(in, 1, st_intersects_extent_impl);
    ASSERT_EQ(out->size(), 1u);
    EXPECT_EQ((*out)[0], 1u);
    clickhouse_destroy_buffer(reinterpret_cast<uint8_t *>(in));
    clickhouse_destroy_buffer(reinterpret_cast<uint8_t *>(out));
}

TEST(RowBinaryWrapper, DisjointBoxes) {
    // Two far-apart squares — should return 0
    auto * in = make_rb2({
        {"POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))",
         "POLYGON ((5 5, 6 5, 6 6, 5 6, 5 5))"},
    });
    auto * out = rowbinary_impl_wrapper(in, 1, st_intersects_extent_impl);
    ASSERT_EQ(out->size(), 1u);
    EXPECT_EQ((*out)[0], 0u);
    clickhouse_destroy_buffer(reinterpret_cast<uint8_t *>(in));
    clickhouse_destroy_buffer(reinterpret_cast<uint8_t *>(out));
}

TEST(RowBinaryWrapper, MultipleRows) {
    // 3 rows: intersecting, disjoint, point inside polygon
    auto * in = make_rb2({
        {"POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0))",
         "POLYGON ((1 1, 3 1, 3 3, 1 3, 1 1))"},  // intersects → 1
        {"POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))",
         "POLYGON ((5 5, 6 5, 6 6, 5 6, 5 5))"},  // disjoint → 0
        {"POINT (0.5 0.5)",
         "POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))"},  // point inside box → 1
    });
    auto * out = rowbinary_impl_wrapper(in, 3, st_intersects_extent_impl);
    ASSERT_EQ(out->size(), 3u);
    EXPECT_EQ((*out)[0], 1u);
    EXPECT_EQ((*out)[1], 0u);
    EXPECT_EQ((*out)[2], 1u);
    clickhouse_destroy_buffer(reinterpret_cast<uint8_t *>(in));
    clickhouse_destroy_buffer(reinterpret_cast<uint8_t *>(out));
}

TEST(RowBinaryWrapper, TouchingEdge) {
    // Touching edges share a boundary — bboxes intersect → 1
    auto * in = make_rb2({
        {"POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))",
         "POLYGON ((1 0, 2 0, 2 1, 1 1, 1 0))"},
    });
    auto * out = rowbinary_impl_wrapper(in, 1, st_intersects_extent_impl);
    ASSERT_EQ(out->size(), 1u);
    EXPECT_EQ((*out)[0], 1u);
    clickhouse_destroy_buffer(reinterpret_cast<uint8_t *>(in));
    clickhouse_destroy_buffer(reinterpret_cast<uint8_t *>(out));
}

TEST(RowBinaryWrapper, SameResultAsMsgPack) {
    // Both wrappers must agree on every row.
    const std::vector<std::pair<std::string,std::string>> cases = {
        {"POLYGON ((0 0,2 0,2 2,0 2,0 0))", "POLYGON ((1 1,3 1,3 3,1 3,1 1))"},
        {"POLYGON ((0 0,1 0,1 1,0 1,0 0))", "POLYGON ((5 5,6 5,6 6,5 6,5 5))"},
        {"POINT (0.5 0.5)",                 "POLYGON ((0 0,1 0,1 1,0 1,0 0))"},
    };
    for (auto & [wkt_a, wkt_b] : cases) {
        auto span_a = wkt2wkb(wkt_a);
        auto span_b = wkt2wkb(wkt_b);
        bool msgpack_result = st_intersects_extent_impl(
            wkb(span_a), wkb(span_b));

        auto * in = make_rb2({{wkt_a, wkt_b}});
        auto * out = rowbinary_impl_wrapper(in, 1, st_intersects_extent_impl);
        bool rb_result = (*out)[0] != 0;
        EXPECT_EQ(rb_result, msgpack_result) << wkt_a << " vs " << wkt_b;
        clickhouse_destroy_buffer(reinterpret_cast<uint8_t *>(in));
        clickhouse_destroy_buffer(reinterpret_cast<uint8_t *>(out));
    }
}
