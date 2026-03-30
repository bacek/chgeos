#pragma once

#include <cstddef>
#include <cstdint>
#include <iterator>
#include <limits>
#include <memory>
#include <span>
#include <stdexcept>
#include <string_view>
#include <tuple>
#include <type_traits>

#include <msgpack23/msgpack23.h>
#include <vector>

#include "clickhouse.hpp"
#include "geom/chgeom.hpp"
#include "geom/wkb.hpp"
#include "mem.hpp"

namespace ch {

template <typename B, typename Iter>
void pack_result(msgpack23::Packer<B, Iter> &p,
                 std::unique_ptr<geos::geom::Geometry> res) {
  const auto buf = write_ewkb(res);
  p(std::span(buf.data(), buf.size()));
}

template <typename B, typename Iter>
void pack_result(msgpack23::Packer<B, Iter> &p, const raw_buffer &res) {
  p(std::span(res.data(), res.size()));
}

template <typename B, typename Iter>
void pack_result(msgpack23::Packer<B, Iter> &p, const std::string &res) {
  p(std::span(reinterpret_cast<const uint8_t *>(res.data()), res.size()));
}

template <typename B, typename Iter, typename T>
void pack_result(msgpack23::Packer<B, Iter> &p, const T &res) {
  p(res);
}

// Generic fallback: unpack T directly via the Unpacker.
template <typename B, typename T>
void unpack_arg(msgpack23::Unpacker<B> &u, T &arg) {
  u(arg);
}

// Specialization: unique_ptr<Geometry> arrives as a WKB bin blob.
// Read the raw bytes and decode via read_wkb so impl functions
// never touch MsgPack directly.
template <typename B>
void unpack_arg(msgpack23::Unpacker<B> &u,
                std::unique_ptr<geos::geom::Geometry> &arg) {
  std::span<const B> raw;
  u(raw);
  arg = read_wkb(raw);
}

// Specialization: integral arguments — unpack into the widest type for the
// signedness of T, then bounds-check before narrowing.
// bool is excluded: it has its own msgpack format and the generic fallback handles it.
template <typename B, std::integral T>
  requires (!std::is_same_v<T, bool>)
void unpack_arg(msgpack23::Unpacker<B> &u, T &arg) {
  using Wide = std::conditional_t<std::is_signed_v<T>, int64_t, uint64_t>;
  Wide v{};
  u(v);
  if (v < static_cast<Wide>(std::numeric_limits<T>::min()) ||
      v > static_cast<Wide>(std::numeric_limits<T>::max()))
    throw std::out_of_range("integer argument out of range for target type");
  arg = static_cast<T>(v);
}

// Specialization: String arrives as a msgpack bin blob — unpack as span,
// then construct std::string from the raw bytes.
template <typename B>
void unpack_arg(msgpack23::Unpacker<B> &u, std::string &arg) {
  std::span<const B> raw;
  u(raw);
  arg.assign(reinterpret_cast<const char *>(raw.data()), raw.size());
}

// Zero-copy variant: returns a string_view into the packed buffer.
// Valid only for the lifetime of the input buffer.
template <typename B>
void unpack_arg(msgpack23::Unpacker<B> &u, std::string_view &arg) {
  std::span<const B> raw;
  u(raw);
  arg = std::string_view(reinterpret_cast<const char *>(raw.data()), raw.size());
}

// Specialization: vector<unique_ptr<Geometry>> arrives as Array(String) —
// a msgpack array where every element is a WKB bin blob.
template <typename B>
void unpack_arg(msgpack23::Unpacker<B> &u,
                std::vector<std::unique_ptr<geos::geom::Geometry>> &arg) {
  std::vector<std::span<const uint8_t>> raw_vec;
  u(raw_vec);
  for (const auto span : raw_vec) {
    arg.emplace_back(read_wkb(span));
  }
}

template <typename Ret, typename... Args>
raw_buffer *impl_wrapper(raw_buffer *ptr, uint32_t num_rows,
                         Ret (*impl)(Args...)) {
  raw_buffer *buf = clickhouse_create_buffer(1024);
  try {
    auto raw = raw_buffer_back_inserter(buf);
    msgpack23::Packer packer{raw};

    std::span<uint8_t> span = std::span(ptr->begin(), ptr->end());
    auto unpacker = msgpack23::Unpacker(span);

    for (uint32_t row_num = 0; row_num < num_rows; ++row_num) {
      std::tuple<std::decay_t<Args>...> args{};
      std::apply([&unpacker](auto &...a) { (unpack_arg(unpacker, a), ...); },
          args);
      auto res = std::apply(impl, std::move(args));
      pack_result(packer, std::move(res));
    }

    return buf;
  } catch(const std::exception& e) {
    clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(buf));
    ch::panic(e.what());
    __builtin_unreachable();
  } catch(...) {
    clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(buf));
    ch::panic("Unknown exception in WASM UDF");
    __builtin_unreachable();
  }
}

} // namespace ch

#define CH_UDF_FUNC(name)                                                      \
  __attribute__((export_name(#name))) ch::raw_buffer *name(ch::raw_buffer *ptr, \
                                                      uint32_t num_rows) {     \
    return ch::impl_wrapper(ptr, num_rows, ch::name##_impl);                   \
  }

// RowBinary scalar function — exports under the plain name.
// Replace CH_UDF_FUNC with this for scalar functions; add
//   SETTINGS serialization_format = 'RowBinary'
// to the SQL DDL.
#define CH_UDF_RB_ONLY(name)                                                           \
    __attribute__((export_name(#name))) ch::raw_buffer *                               \
    name(ch::raw_buffer * ptr, uint32_t num_rows) {                                    \
        return ch::rowbinary_impl_wrapper(ptr, num_rows, ch::name##_impl);             \
    }

// RowBinary binary geometry predicate with bbox shortcut.
// Replace CH_UDF_BBOX2 with this; same SQL DDL changes as CH_UDF_RB_ONLY.
#define CH_UDF_RB_BBOX2(name, bbox_op, early_ret)                                          \
    __attribute__((export_name(#name))) ch::raw_buffer * name(                             \
        ch::raw_buffer * ptr, uint32_t num_rows) {                                         \
        return ch::rowbinary_impl_wrapper(ptr, num_rows,                                   \
            +[](std::span<const uint8_t> a, std::span<const uint8_t> b) -> bool {         \
                return ch::with_bbox(a, b, ch::bbox_op, early_ret,                        \
                    ch::name##_impl);                                                       \
            });                                                                             \
    }

// Binary geometry predicate with bbox shortcut.
// bbox_op : one of bbox_op_intersects / bbox_op_contains / bbox_op_rcontains
// early_ret: value returned when the bbox check fails (false for most, true for disjoint)
#define CH_UDF_BBOX2(name, bbox_op, early_ret)                                      \
    __attribute__((export_name(#name))) ch::raw_buffer * name(                      \
        ch::raw_buffer * ptr, uint32_t num_rows) {                                  \
        return ch::impl_wrapper(ptr, num_rows,                                      \
            +[](std::span<const uint8_t> a, std::span<const uint8_t> b) -> bool {  \
                return ch::with_bbox(a, b, ch::bbox_op, early_ret,                 \
                    ch::name##_impl);                                               \
            });                                                                     \
    }
