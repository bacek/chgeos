#pragma once
// Buffers (Native CH) wire format for BUFFERED_V1 WASM UDFs.
// serialization_format = 'Buffers'
//
// Wire layout (input and output use identical structure):
//   num_cols : u64LE
//   num_rows : u64LE
//   for each column:
//     buffer_size : u64LE   (bytes of col_data that follow)
//     col_data    : Native CH column (NativeWriter format — see below)
//
// Native CH ColumnString (col_data for String columns):
//   per row: VarUInt(byte_length) + raw_bytes   (no null terminator, no offset array)
//
// Native CH fixed-width column (UInt8, Int32, Float64, …):
//   T[N]  — little-endian packed, no padding
//
// SQL registration:
//   ABI BUFFERED_V1
//   SETTINGS serialization_format = 'Buffers'
// Export symbol: <name>_buffers

#include <array>
#include <cstdint>
#include <cstring>
#include <memory>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <tuple>
#include <type_traits>
#include <vector>

#include "clickhouse.hpp"
#include "geom/wkb.hpp"
#include "mem.hpp"

namespace ch {

// ── I/O helpers ─────────────────────────────────────────────────────────────────

inline uint64_t buf_read_u64(const uint8_t*& ptr, const uint8_t* end) {
    if (static_cast<uint64_t>(end - ptr) < 8)
        throw std::runtime_error("Buffers: input truncated reading u64");
    uint64_t v;
    std::memcpy(&v, ptr, 8);
    ptr += 8;
    return v;
}

inline void buf_write_u64(raw_buffer* buf, uint64_t v) {
    buf->append(reinterpret_cast<const uint8_t*>(&v), 8);
}

// ClickHouse VarUInt: 7 bits per byte, MSB = continuation bit.
inline uint64_t buf_read_varuint(const uint8_t*& p) noexcept {
    uint64_t v = 0;
    unsigned shift = 0;
    while (true) {
        uint8_t b = *p++;
        v |= static_cast<uint64_t>(b & 0x7F) << shift;
        if (!(b & 0x80)) break;
        shift += 7;
    }
    return v;
}

inline void buf_write_varuint(raw_buffer* buf, uint64_t v) {
    while (v >= 0x80) {
        buf->push_back(static_cast<uint8_t>((v & 0x7F) | 0x80));
        v >>= 7;
    }
    buf->push_back(static_cast<uint8_t>(v));
}

// Returns the number of bytes that buf_write_varuint would emit for v.
inline uint64_t varuint_size(uint64_t v) noexcept {
    uint64_t n = 0;
    do { ++n; v >>= 7; } while (v);
    return n;
}

// ── Column cursors ──────────────────────────────────────────────────────────────

// Native CH ColumnString in Buffers format: per-row VarUInt(len) + raw_bytes.
// Pre-scanned during make() for O(1) random access.
struct BufStringCol {
    std::vector<std::span<const uint8_t>> spans;

    std::span<const uint8_t> at(uint64_t i) const noexcept {
        return spans[static_cast<size_t>(i)];
    }

    static BufStringCol make(const uint8_t* col_data, uint64_t num_rows) {
        BufStringCol col;
        col.spans.reserve(static_cast<size_t>(num_rows));
        const uint8_t* p = col_data;
        for (uint64_t i = 0; i < num_rows; ++i) {
            uint64_t len = buf_read_varuint(p);
            col.spans.push_back({p, static_cast<size_t>(len)});
            p += len;
        }
        return col;
    }
};

template <typename T>
struct BufFixedCol {
    const T* data;
    T at(uint64_t i) const noexcept { return data[i]; }
};

// ── Cursor type traits ──────────────────────────────────────────────────────────

template <typename T>
struct buf_cursor_for;

template <>
struct buf_cursor_for<std::span<const uint8_t>> {
    using type = BufStringCol;
    static BufStringCol make(const uint8_t* col_data, uint64_t num_rows) {
        return BufStringCol::make(col_data, num_rows);
    }
    static std::span<const uint8_t> get(const BufStringCol& c, uint64_t i) noexcept {
        return c.at(i);
    }
};

template <>
struct buf_cursor_for<std::string_view> {
    using type = BufStringCol;
    static BufStringCol make(const uint8_t* col_data, uint64_t num_rows) {
        return BufStringCol::make(col_data, num_rows);
    }
    static std::string_view get(const BufStringCol& c, uint64_t i) noexcept {
        auto sp = c.at(i);
        return {reinterpret_cast<const char*>(sp.data()), sp.size()};
    }
};

template <>
struct buf_cursor_for<std::unique_ptr<geos::geom::Geometry>> {
    using type = BufStringCol;
    static BufStringCol make(const uint8_t* col_data, uint64_t num_rows) {
        return BufStringCol::make(col_data, num_rows);
    }
    static std::unique_ptr<geos::geom::Geometry> get(const BufStringCol& c, uint64_t i) {
        return read_wkb(c.at(i));
    }
};

template <std::integral T>
    requires (!std::is_same_v<T, bool>)
struct buf_cursor_for<T> {
    using type = BufFixedCol<T>;
    static BufFixedCol<T> make(const uint8_t* col_data, uint64_t) noexcept {
        return {reinterpret_cast<const T*>(col_data)};
    }
    static T get(const BufFixedCol<T>& c, uint64_t i) noexcept { return c.at(i); }
};

template <std::floating_point T>
struct buf_cursor_for<T> {
    using type = BufFixedCol<T>;
    static BufFixedCol<T> make(const uint8_t* col_data, uint64_t) noexcept {
        return {reinterpret_cast<const T*>(col_data)};
    }
    static T get(const BufFixedCol<T>& c, uint64_t i) noexcept { return c.at(i); }
};

// ── Result serialization ─────────────────────────────────────────────────────────

inline std::vector<uint8_t> buf_serialize_str(std::unique_ptr<geos::geom::Geometry> g) {
    auto rb = write_ewkb(g);
    return {rb.begin(), rb.end()};
}
inline std::vector<uint8_t> buf_serialize_str(std::string s) {
    return {s.begin(), s.end()};
}

// ── Row processing + output writer ──────────────────────────────────────────────

template <typename Ret, typename... Args, std::size_t... I>
void buf_process(raw_buffer* buf, uint64_t num_rows,
                 Ret (*impl)(Args...),
                 const std::tuple<typename buf_cursor_for<std::decay_t<Args>>::type...>& cs,
                 std::index_sequence<I...>) {
    using RetD = std::decay_t<Ret>;

    if constexpr (std::is_arithmetic_v<RetD>) {
        constexpr uint64_t elem = std::is_same_v<RetD, bool> ? 1u : sizeof(RetD);
        buf_write_u64(buf, 1);
        buf_write_u64(buf, num_rows);
        buf_write_u64(buf, num_rows * elem);
        for (uint64_t row = 0; row < num_rows; ++row) {
            RetD res = impl(
                buf_cursor_for<std::decay_t<Args>>::get(std::get<I>(cs), row)...);
            if constexpr (std::is_same_v<RetD, bool>) {
                buf->push_back(res ? 1u : 0u);
            } else {
                buf->append(reinterpret_cast<const uint8_t*>(&res), sizeof(RetD));
            }
        }
    } else {
        // String/geometry output: collect all rows, then write ColumnString.
        std::vector<std::vector<uint8_t>> rows;
        rows.reserve(static_cast<size_t>(num_rows));
        for (uint64_t row = 0; row < num_rows; ++row) {
            rows.push_back(buf_serialize_str(
                impl(buf_cursor_for<std::decay_t<Args>>::get(std::get<I>(cs), row)...)));
        }
        // buffer_size = sum of (varuint_encoded_length + data_bytes) per row
        uint64_t total = 0;
        for (const auto& r : rows)
            total += varuint_size(r.size()) + r.size();
        buf_write_u64(buf, 1);
        buf_write_u64(buf, num_rows);
        buf_write_u64(buf, total);
        for (const auto& r : rows) {
            buf_write_varuint(buf, static_cast<uint64_t>(r.size()));
            buf->append(r.data(), static_cast<uint32_t>(r.size()));
        }
    }
}

// ── Main wrapper ─────────────────────────────────────────────────────────────────

template <typename Ret, typename... Args>
raw_buffer* buffers_impl_wrapper(raw_buffer* ptr, uint32_t num_rows_hint,
                                  Ret (*impl)(Args...)) {
    raw_buffer* buf = clickhouse_create_buffer(num_rows_hint ? num_rows_hint : 1);
    buf->clear();
    try {
        const uint8_t* in     = ptr->begin();
        const uint8_t* in_end = ptr->end();

        uint64_t num_cols = buf_read_u64(in, in_end);
        uint64_t num_rows = buf_read_u64(in, in_end);

        if (num_cols != sizeof...(Args))
            throw std::runtime_error("Buffers: column count mismatch");

        // Sequential scan — avoids any evaluation-order ambiguity when building cursors.
        std::array<const uint8_t*, sizeof...(Args)> col_data{};
        for (size_t c = 0; c < sizeof...(Args); ++c) {
            uint64_t bsz = buf_read_u64(in, in_end);
            if (static_cast<uint64_t>(in_end - in) < bsz)
                throw std::runtime_error("Buffers: column data truncated");
            col_data[c] = in;
            in += bsz;
        }

        // Build typed cursors; col_data[] is fully populated so order doesn't matter.
        auto cursors = [&]<std::size_t... J>(std::index_sequence<J...>) {
            return std::make_tuple(
                buf_cursor_for<std::decay_t<
                    std::tuple_element_t<J, std::tuple<Args...>>>>::make(col_data[J], num_rows)...
            );
        }(std::index_sequence_for<Args...>{});

        buf_process(buf, num_rows, impl, cursors, std::index_sequence_for<Args...>{});
        return buf;
    } catch (const std::exception& e) {
        clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(buf));
        ch::panic(e.what());
        __builtin_unreachable();
    } catch (...) {
        clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(buf));
        ch::panic("Unknown exception in Buffers WASM UDF");
        __builtin_unreachable();
    }
}

} // namespace ch

#define CH_UDF_BUFFERS(name)                                                               \
    __attribute__((export_name(#name "_buffers")))                                         \
    ch::raw_buffer * name##_buffers(ch::raw_buffer * ptr, uint32_t num_rows) {            \
        return ch::buffers_impl_wrapper(ptr, num_rows, ch::name##_impl);                  \
    }
