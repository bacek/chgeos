#include <gtest/gtest.h>
#include <span>
#include <vector>

#include <msgpack23/msgpack23.h>

#include "helpers.hpp"
#include "mem.hpp"
#include "udf.hpp"
#include "functions/overlay.hpp"

using namespace ch;

// ── raw_buffer_back_inserter / msgpack round-trip ─────────────────────────────
//
// These tests exercise the Packer→raw_buffer_back_inserter→Unpacker path that
// impl_wrapper uses at runtime.  In particular they catch the cursor-clobber
// bug where *it++ = val wrote through a per-copy position field, causing every
// subsequent write to land at offset 0.

// Pack N values of the same type into a raw_buffer, unpack them, compare.
template<typename T>
static std::vector<T> msgpack_roundtrip(const std::vector<T>& values) {
  auto *buf = clickhouse_create_buffer(64);
  {
    auto writer = raw_buffer_back_inserter(buf);
    msgpack23::Packer packer{writer};
    for (const auto& v : values) packer(v);
  }
  auto unpacker = msgpack23::Unpacker(
      std::span<const uint8_t>(buf->begin(), buf->end()));
  std::vector<T> out;
  for (std::size_t i = 0; i < values.size(); ++i) {
    T v{};
    unpacker(v);
    out.push_back(v);
  }
  clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(buf));
  return out;
}

TEST(RawBufferBackInserter, SingleBool) {
  EXPECT_EQ(msgpack_roundtrip<bool>({true}),  std::vector<bool>{true});
  EXPECT_EQ(msgpack_roundtrip<bool>({false}), std::vector<bool>{false});
}

TEST(RawBufferBackInserter, MultipleBools) {
  auto out = msgpack_roundtrip<bool>({true, false, true, true, false});
  EXPECT_EQ(out, (std::vector<bool>{true, false, true, true, false}));
}

TEST(RawBufferBackInserter, MultipleDoubles) {
  auto out = msgpack_roundtrip<double>({1.5, -2.25, 0.0, 1e10});
  ASSERT_EQ(out.size(), 4u);
  EXPECT_DOUBLE_EQ(out[0],  1.5);
  EXPECT_DOUBLE_EQ(out[1], -2.25);
  EXPECT_DOUBLE_EQ(out[2],  0.0);
  EXPECT_DOUBLE_EQ(out[3],  1e10);
}

TEST(RawBufferBackInserter, BlobRoundtrip) {
  Vector wkb = wkt2wkb("POINT (3 4)");
  auto out = msgpack_roundtrip<Vector>({wkb});
  ASSERT_EQ(out.size(), 1u);
  EXPECT_EQ(out[0], wkb);
}

TEST(RawBufferBackInserter, MixedTypes) {
  auto *buf = clickhouse_create_buffer(64);
  {
    auto writer = raw_buffer_back_inserter(buf);
    msgpack23::Packer packer{writer};
    packer(true);
    packer(42.0);
    packer(false);
    packer(std::int32_t{12345});
  }
  auto unpacker = msgpack23::Unpacker(
      std::span<const uint8_t>(buf->begin(), buf->end()));
  bool b1{}; double d{}; bool b2{}; std::int32_t i{};
  unpacker(b1, d, b2, i);
  EXPECT_EQ(b1, true);
  EXPECT_DOUBLE_EQ(d, 42.0);
  EXPECT_EQ(b2, false);
  EXPECT_EQ(i, 12345);
  clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(buf));
}

TEST(RawBufferBackInserter, ImplWrapperRoundtrip) {
  Vector wkb1 = wkt2wkb("POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))");  // area 1
  Vector wkb2 = wkt2wkb("POLYGON ((0 0, 2 0, 2 3, 0 3, 0 0))");  // area 6

  auto *in = clickhouse_create_buffer(256);
  {
    auto writer = raw_buffer_back_inserter(in);
    msgpack23::Packer packer{writer};
    packer(wkb1);
    packer(wkb2);
  }

  raw_buffer *out = impl_wrapper(in, 2, st_area_impl);

  auto unpacker = msgpack23::Unpacker(
      std::span<const uint8_t>(out->begin(), out->end()));
  double a1{}, a2{};
  unpacker(a1, a2);
  EXPECT_DOUBLE_EQ(a1, 1.0);
  EXPECT_DOUBLE_EQ(a2, 6.0);

  clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(in));
  clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(out));
}

// ── st_union_agg roundtrip ────────────────────────────────────────────────────

TEST(ImplWrapper, UnionAggRoundtrip) {
  // st_union_agg_impl takes vector<unique_ptr<Geometry>> — one row is a
  // msgpack array of WKB blobs.  Two disjoint unit squares → union area = 2.
  Vector wkb1 = wkt2wkb("POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))");
  Vector wkb2 = wkt2wkb("POLYGON ((5 5, 6 5, 6 6, 5 6, 5 5))");

  auto *in = clickhouse_create_buffer(256);
  {
    auto writer = raw_buffer_back_inserter(in);
    msgpack23::Packer packer{writer};
    packer(std::vector<Vector>{wkb1, wkb2});  // one row: array of two blobs
  }

  raw_buffer *out = impl_wrapper(in, 1, st_union_agg_impl);

  auto unpacker = msgpack23::Unpacker(
      std::span<const uint8_t>(out->begin(), out->end()));
  ch::Vector result_wkb;
  unpacker(result_wkb);
  auto result = read_wkb(std::span<const uint8_t>(result_wkb));
  EXPECT_NEAR(result->getArea(), 2.0, 1e-10);

  clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(in));
  clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(out));
}
