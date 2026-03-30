#pragma once

// RowBinary serialization support for BUFFERED_V1 WASM UDFs.
//
// ClickHouse RowBinary wire format (relevant types):
//   String  : VarUInt (LEB128) byte-count, then raw bytes
//   UInt8   : 1 byte
//   Float64 : 8 bytes, little-endian
//   Int32   : 4 bytes, little-endian (two's complement)
//
// Register functions using this ABI with:
//   ABI BUFFERED_V1
//   SETTINGS serialization_format = 'RowBinary'

#include <cstdint>
#include <cstring>
#include <memory>
#include <span>
#include <stdexcept>
#include <tuple>
#include <type_traits>

#include "clickhouse.hpp"
#include "geom/wkb.hpp"
#include "mem.hpp"

namespace ch {

// ── VarUInt / LEB128 ──────────────────────────────────────────────────────────

inline uint64_t rb_read_varuint(const uint8_t *& ptr, const uint8_t * end) {
    uint64_t result = 0;
    unsigned shift = 0;
    while (ptr < end) {
        uint8_t byte = *ptr++;
        result |= static_cast<uint64_t>(byte & 0x7F) << shift;
        if ((byte & 0x80) == 0) return result;
        shift += 7;
        if (shift >= 64) throw std::runtime_error("RowBinary: VarUInt overflow");
    }
    throw std::runtime_error("RowBinary: input truncated in VarUInt");
}

inline void rb_write_varuint(raw_buffer * buf, uint64_t v) {
    while (v >= 0x80) {
        buf->push_back(static_cast<uint8_t>((v & 0x7F) | 0x80));
        v >>= 7;
    }
    buf->push_back(static_cast<uint8_t>(v));
}

// ── rb_unpack_arg ─────────────────────────────────────────────────────────────
// Read one argument from the RowBinary input stream, advancing ptr.

// Span<const uint8_t>: zero-copy view into the input buffer (valid for the
// duration of the call — the input buffer is not freed until after the result
// is returned).
inline void rb_unpack_arg(const uint8_t *& ptr, const uint8_t * end,
                          std::span<const uint8_t> & arg) {
    uint64_t len = rb_read_varuint(ptr, end);
    if (static_cast<uint64_t>(end - ptr) < len)
        throw std::runtime_error("RowBinary: string body truncated");
    arg = std::span<const uint8_t>(ptr, static_cast<size_t>(len));
    ptr += len;
}

// string_view: zero-copy view into the input buffer.
// Valid only for the lifetime of the input buffer (same as udf.hpp variant).
inline void rb_unpack_arg(const uint8_t *& ptr, const uint8_t * end,
                          std::string_view & arg) {
    uint64_t len = rb_read_varuint(ptr, end);
    if (static_cast<uint64_t>(end - ptr) < len)
        throw std::runtime_error("RowBinary: string body truncated");
    arg = std::string_view(reinterpret_cast<const char *>(ptr), static_cast<size_t>(len));
    ptr += len;
}

// unique_ptr<Geometry>: read string field, parse as WKB.
inline void rb_unpack_arg(const uint8_t *& ptr, const uint8_t * end,
                          std::unique_ptr<geos::geom::Geometry> & arg) {
    std::span<const uint8_t> raw;
    rb_unpack_arg(ptr, end, raw);
    arg = read_wkb(raw);
}

// Fixed-width integers (LE).
template <std::integral T>
    requires (!std::is_same_v<T, bool>)
inline void rb_unpack_arg(const uint8_t *& ptr, const uint8_t * end, T & arg) {
    if (static_cast<uint64_t>(end - ptr) < sizeof(T))
        throw std::runtime_error("RowBinary: integer truncated");
    std::memcpy(&arg, ptr, sizeof(T));
    ptr += sizeof(T);
}

// Floating-point (LE, IEEE 754 — same layout as WASM/x86 host).
template <std::floating_point T>
inline void rb_unpack_arg(const uint8_t *& ptr, const uint8_t * end, T & arg) {
    if (static_cast<uint64_t>(end - ptr) < sizeof(T))
        throw std::runtime_error("RowBinary: float truncated");
    std::memcpy(&arg, ptr, sizeof(T));
    ptr += sizeof(T);
}

// ── rb_pack_result ────────────────────────────────────────────────────────────
// Append one result value to the RowBinary output buffer.

inline void rb_pack_result(raw_buffer * buf, bool v) {
    buf->push_back(v ? 1u : 0u);
}

template <std::integral T>
    requires (!std::is_same_v<T, bool>)
inline void rb_pack_result(raw_buffer * buf, T v) {
    uint8_t tmp[sizeof(T)];
    std::memcpy(tmp, &v, sizeof(T));
    buf->append(tmp, sizeof(T));
}

template <std::floating_point T>
inline void rb_pack_result(raw_buffer * buf, T v) {
    uint8_t tmp[sizeof(T)];
    std::memcpy(tmp, &v, sizeof(T));
    buf->append(tmp, sizeof(T));
}

inline void rb_pack_result(raw_buffer * buf, std::unique_ptr<geos::geom::Geometry> g) {
    auto bytes = write_ewkb(g);
    rb_write_varuint(buf, bytes.size());
    buf->append(bytes.data(), static_cast<uint32_t>(bytes.size()));
}

inline void rb_pack_result(raw_buffer * buf, const std::string & s) {
    rb_write_varuint(buf, s.size());
    buf->append(reinterpret_cast<const uint8_t *>(s.data()), static_cast<uint32_t>(s.size()));
}

inline void rb_pack_result(raw_buffer * buf, const raw_buffer & bytes) {
    rb_write_varuint(buf, bytes.size());
    buf->append(bytes.data(), bytes.size());
}

// ── rowbinary_impl_wrapper ────────────────────────────────────────────────────
// Mirrors impl_wrapper in udf.hpp but uses RowBinary (de)serialization.
// Register the function with SETTINGS serialization_format = 'RowBinary'.

template <typename Ret, typename... Args>
raw_buffer * rowbinary_impl_wrapper(raw_buffer * ptr, uint32_t num_rows,
                                    Ret (*impl)(Args...)) {
    raw_buffer * buf = clickhouse_create_buffer(num_rows ? num_rows : 1);
    buf->clear();  // reset logical size; capacity kept for efficiency
    try {
        const uint8_t * in     = ptr->begin();
        const uint8_t * in_end = ptr->end();

        for (uint32_t row_num = 0; row_num < num_rows; ++row_num) {
            std::tuple<std::decay_t<Args>...> args{};
            std::apply([&in, &in_end](auto &... a) {
                (rb_unpack_arg(in, in_end, a), ...);
            }, args);
            auto res = std::apply(impl, std::move(args));
            rb_pack_result(buf, std::move(res));
        }

        return buf;
    } catch (const std::exception & e) {
        clickhouse_destroy_buffer(reinterpret_cast<uint8_t *>(buf));
        ch::panic(e.what());
        __builtin_unreachable();
    } catch (...) {
        clickhouse_destroy_buffer(reinterpret_cast<uint8_t *>(buf));
        ch::panic("Unknown exception in RowBinary WASM UDF");
        __builtin_unreachable();
    }
}

} // namespace ch

// Register a WASM export that reads/writes RowBinary instead of MsgPack.
// The exported symbol is <name>_rb; register in SQL as:
//   CREATE OR REPLACE FUNCTION <name>_rb ... ABI BUFFERED_V1
//   SETTINGS serialization_format = 'RowBinary';
#define CH_UDF_RB_FUNC(name)                                                               \
    __attribute__((export_name(#name "_rb"))) ch::raw_buffer *                             \
    name##_rb(ch::raw_buffer * ptr, uint32_t num_rows) {                                   \
        return ch::rowbinary_impl_wrapper(ptr, num_rows, ch::name##_impl);                 \
    }
