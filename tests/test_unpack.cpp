#include <gtest/gtest.h>
#include <span>
#include <vector>

#include <msgpack23/msgpack23.h>

#include "helpers.hpp"
#include "mem.hpp"
#include "udf.hpp"
#include "clickhouse.hpp"
#include "functions/overlay.hpp"

using namespace ch;

// ── unpack_arg: integral types ────────────────────────────────────────────────

TEST(UnpackArg, IntegralUint32) {
  auto *buf = clickhouse_create_buffer(64);
  {
    auto writer = raw_buffer_back_inserter(buf);
    msgpack23::Packer packer{writer};
    packer(uint32_t{42});
  }
  auto unpacker = msgpack23::Unpacker(std::span<const uint8_t>(buf->begin(), buf->end()));
  uint32_t v{};
  ch::unpack_arg(unpacker, v);
  EXPECT_EQ(v, 42u);
  clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(buf));
}

TEST(UnpackArg, IntegralNegativeInt32) {
  // -33 is below the negative fixint range (-32..-1), so msgpack23 encodes it
  // with an explicit int8 prefix (0xd0) rather than a raw fixint byte.
  // unpack_type(int64_t) handles the int8 case correctly via sign extension.
  auto *buf = clickhouse_create_buffer(64);
  {
    auto writer = raw_buffer_back_inserter(buf);
    msgpack23::Packer packer{writer};
    packer(int32_t{-33});
  }
  auto unpacker = msgpack23::Unpacker(std::span<const uint8_t>(buf->begin(), buf->end()));
  int32_t v{};
  ch::unpack_arg(unpacker, v);
  EXPECT_EQ(v, -33);
  clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(buf));
}

TEST(UnpackArg, IntegralOutOfRangeThrows) {
  // Pack a value that fits int32_t but not int8_t — must throw out_of_range
  // instead of silently truncating.
  auto *buf = clickhouse_create_buffer(64);
  {
    auto writer = raw_buffer_back_inserter(buf);
    msgpack23::Packer packer{writer};
    packer(int32_t{1000});
  }
  auto unpacker = msgpack23::Unpacker(std::span<const uint8_t>(buf->begin(), buf->end()));
  int8_t v{};
  EXPECT_THROW(ch::unpack_arg(unpacker, v), std::out_of_range);
  clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(buf));
}

TEST(UnpackArg, IntegralNarrowInRange) {
  // Pack a small positive value, unpack into int16_t — must succeed.
  auto *buf = clickhouse_create_buffer(64);
  {
    auto writer = raw_buffer_back_inserter(buf);
    msgpack23::Packer packer{writer};
    packer(int32_t{255});
  }
  auto unpacker = msgpack23::Unpacker(std::span<const uint8_t>(buf->begin(), buf->end()));
  int16_t v{};
  ch::unpack_arg(unpacker, v);
  EXPECT_EQ(v, 255);
  clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(buf));
}

TEST(UnpackArg, IntegralViaImplWrapper) {
  // End-to-end: geos_log_test_impl(uint32_t, string) goes through the
  // integral unpack_arg specialization for the level argument.
  const uint32_t level = static_cast<uint32_t>(ch::log_level::debug);
  const ch::Vector msg_bytes{'t', 'e', 's', 't'};

  auto *in = clickhouse_create_buffer(64);
  {
    auto writer = raw_buffer_back_inserter(in);
    msgpack23::Packer packer{writer};
    packer(level);
    packer(msg_bytes);
  }

  raw_buffer *out = impl_wrapper(in, 1, ch::geos_log_test_impl);

  auto unpacker = msgpack23::Unpacker(std::span<const uint8_t>(out->begin(), out->end()));
  uint32_t result{};
  unpacker(result);
  EXPECT_EQ(result, level);

  clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(in));
  clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(out));
}

// ── unpack_arg: String → std::string ─────────────────────────────────────────

TEST(UnpackArg, StringRoundtrip) {
  // Verify unpack_arg<std::string> via geos_log_test_impl roundtrip:
  // pack (level=6, msg as bin blob), call impl_wrapper, check returned level.
  const uint32_t level = static_cast<uint32_t>(ch::log_level::information);
  const std::string msg = "hello from test";
  const ch::Vector msg_bytes(msg.begin(), msg.end());

  auto *in = clickhouse_create_buffer(256);
  {
    auto writer = raw_buffer_back_inserter(in);
    msgpack23::Packer packer{writer};
    packer(level);
    packer(msg_bytes);
  }

  raw_buffer *out = impl_wrapper(in, 1, ch::geos_log_test_impl);

  auto unpacker = msgpack23::Unpacker(
      std::span<const uint8_t>(out->begin(), out->end()));
  uint32_t result{};
  unpacker(result);
  EXPECT_EQ(result, level);

  clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(in));
  clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(out));
}

// ── impl_wrapper: exception from impl propagates through ch::panic ────────────

TEST(ImplWrapper, InvalidGeometryThrowsViaChPanic) {
  // Bow-tie (figure-8) self-intersecting polygons. GEOS throws TopologyException
  // from coll->Union() inside st_union_agg_impl. impl_wrapper catches
  // std::exception and calls ch::panic(e.what()) → clickhouse_throw, which in
  // native tests re-throws as std::runtime_error.
  std::vector<ch::Vector> blobs = {
    wkt2wkb("POLYGON ((0 0, 2 2, 2 0, 0 2, 0 0))"),  // bow-tie, self-intersects at (1,1)
    wkt2wkb("POLYGON ((3 0, 5 2, 5 0, 3 2, 3 0))"),  // bow-tie, self-intersects at (4,1)
  };

  auto *in = clickhouse_create_buffer(512);
  {
    auto writer = raw_buffer_back_inserter(in);
    msgpack23::Packer packer{writer};
    packer(blobs);
  }

  EXPECT_THROW(impl_wrapper(in, 1, ch::st_union_agg_impl), WasmPanicException);

  clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(in));
}

// ── unpack_arg: Array(String) → vector<unique_ptr<Geometry>> ──────────────────

TEST(UnpackArg, GeometryVectorFromArray) {
  const std::string wkt_pt   = "POINT (3 4)";
  const std::string wkt_poly = "POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))";

  std::vector<ch::Vector> blobs = {wkt2wkb(wkt_pt), wkt2wkb(wkt_poly)};

  auto *buf = clickhouse_create_buffer(256);
  {
    auto writer = raw_buffer_back_inserter(buf);
    msgpack23::Packer packer{writer};
    packer(blobs);
  }

  auto unpacker = msgpack23::Unpacker(
      std::span<const uint8_t>(buf->begin(), buf->end()));
  std::vector<std::unique_ptr<ch::Geometry>> geoms;
  ch::unpack_arg(unpacker, geoms);

  ASSERT_EQ(geoms.size(), 2u);
  EXPECT_TRUE(geoms[0]->equals(geom(wkt_pt).get()));
  EXPECT_TRUE(geoms[1]->equals(geom(wkt_poly).get()));

  clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(buf));
}
