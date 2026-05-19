#pragma once

// ColumnBinary wire format for ClickHouse WASM UDFs.
//
// Input wire layout (CH → WASM):
//   num_columns (u32LE) | num_rows (u32LE)
//   For each column:
//     flags (u8, bit0=IS_CONST) | type_tags_size (u32LE) | type_tags (N bytes) | data_size (u64LE) | data
//   type_tags_size == 0 → no type tags (backward compatible).
//   Type tags: Nullable/Array/Tuple are containers (have children),
//   all others are leaves. Full recursive stream for complex nested types.
//
// Output wire layout (WASM → CH, always single column):
//   num_columns (u32LE) | num_rows (u32LE) | flags (u8) | type_tags_size (u32LE) | type_tags + data_size (u64LE) | data

#include <algorithm>
#include <array>
#include <cinttypes>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <format>
#include <span>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>
#include <memory>

#include "clickhouse.hpp"
#include "col_prep_op.hpp"
#include "columnar.hpp"
#include "functions/knn.hpp"
#include "geom/wkb.hpp"
#include "geom/wkb_envelope.hpp"
#include "mem.hpp"

#include <geos/geom/prep/PreparedGeometryFactory.h>

namespace ch {

// ── Type aliases ─────────────────────────────────────────────────────────────

using BboxOp = bool (*)(const BBox&, const BBox&);
using ColPrepOp = bool (*)(const geos::geom::prep::PreparedGeometry*, const geos::geom::Geometry*);
using ColPrepDistOp = bool (*)(const geos::geom::prep::PreparedGeometry*,
                                const geos::geom::Geometry*, double);
using ColPrepPointOp = bool (*)(geos::algorithm::locate::IndexedPointInAreaLocator*,
                                 double, double);

// ── Binary helpers ─────────────────────────────────────────────────────────────

static inline bool readBinaryLE32(const uint8_t*& p, const uint8_t* end, uint32_t& out) {
    if (p + 4 > end) return false;
    out = 0;
    for (int i = 0; i < 4; ++i)
        out |= static_cast<uint32_t>(p[i]) << (i * 8);
    p += 4;
    return true;
}

// ── Type tags ─────────────────────────────────────────────────────────────────
// ClickHouse native type tags for auto-coercion on the WASM side.
// Scalar tags: 0x01–0x0A, String=0x0B, Nullable=0x0C, Array=0x0D, Tuple=0x0E

enum class TypeTag : uint8_t {
    Int8    = 0x01,
    UInt8   = 0x02,
    Int16   = 0x03,
    UInt16  = 0x04,
    Int32   = 0x05,
    UInt32  = 0x06,
    Int64   = 0x07,
    UInt64  = 0x08,
    Float32 = 0x09,
    Float64 = 0x0A,
    String  = 0x0B,
    Nullable= 0x0C,
    Array   = 0x0D,
    Tuple   = 0x0E,
};

// Read a size-prefixed type tag stream from the wire.
// Reads type_tags_size (u32LE) bytes of type tags.
// Returns the vector of type tags for this column.
// type_tags_size == 0 → empty vector (backward compatible).
static inline std::vector<TypeTag> read_type_tags(const uint8_t*& p, const uint8_t* end, uint32_t& out_size) {
    out_size = 0;
    if (!readBinaryLE32(p, end, out_size)) return {};
    if (out_size == 0) return {};
    if (p + out_size > end) return {};
    std::vector<TypeTag> tags;
    tags.reserve(out_size);
    for (uint32_t i = 0; i < out_size; ++i)
        tags.push_back(static_cast<TypeTag>(p[i]));
    p += out_size;
    return tags;
}

// Write a recursive type tag stream to a buffer.
static inline void write_type_tags(raw_buffer& buf, const std::vector<TypeTag>& tags) {
    for (auto tag : tags)
        buf.push_back(static_cast<uint8_t>(tag));
}

// Check if the type tags represent a scalar type that can be widened.
static inline bool is_scalar_tag(TypeTag tag) {
    return tag >= TypeTag::Int8 && tag <= TypeTag::Float64;
}

// Check if a source tag can be widened to a target C++ type.
static inline bool can_widen(TypeTag src, std::type_identity<int8_t>)    { return src == TypeTag::Int8; }
static inline bool can_widen(TypeTag src, std::type_identity<int16_t>)   { return src == TypeTag::Int8 || src == TypeTag::Int16; }
static inline bool can_widen(TypeTag src, std::type_identity<int32_t>)   { return src >= TypeTag::Int8 && src <= TypeTag::Int32; }
static inline bool can_widen(TypeTag src, std::type_identity<int64_t>)   { return src >= TypeTag::Int8 && src <= TypeTag::Int64; }
static inline bool can_widen(TypeTag src, std::type_identity<uint8_t>)   { return src == TypeTag::UInt8; }
static inline bool can_widen(TypeTag src, std::type_identity<uint16_t>)  { return src == TypeTag::UInt8 || src == TypeTag::UInt16; }
static inline bool can_widen(TypeTag src, std::type_identity<uint32_t>)  { return src >= TypeTag::UInt8 && src <= TypeTag::UInt32; }
static inline bool can_widen(TypeTag src, std::type_identity<uint64_t>)  { return src >= TypeTag::UInt8 && src <= TypeTag::UInt64; }
static inline bool can_widen(TypeTag src, std::type_identity<float>)     { return src == TypeTag::Float32; }
static inline bool can_widen(TypeTag src, std::type_identity<double>)    { return src == TypeTag::Float32 || src == TypeTag::Float64; }

// Get the type tag for a return type.
template <typename T> struct ret_type_tag {};
template <> struct ret_type_tag<bool>       { static constexpr TypeTag value = TypeTag::Int8; };
template <> struct ret_type_tag<double>     { static constexpr TypeTag value = TypeTag::Float64; };
template <> struct ret_type_tag<float>      { static constexpr TypeTag value = TypeTag::Float32; };
template <> struct ret_type_tag<int32_t>    { static constexpr TypeTag value = TypeTag::Int32; };
template <> struct ret_type_tag<uint32_t>   { static constexpr TypeTag value = TypeTag::UInt32; };
template <> struct ret_type_tag<int64_t>    { static constexpr TypeTag value = TypeTag::Int64; };
template <> struct ret_type_tag<uint64_t>   { static constexpr TypeTag value = TypeTag::UInt64; };
template <> struct ret_type_tag<int16_t>    { static constexpr TypeTag value = TypeTag::Int16; };
template <> struct ret_type_tag<uint16_t>   { static constexpr TypeTag value = TypeTag::UInt16; };
template <> struct ret_type_tag<int8_t>     { static constexpr TypeTag value = TypeTag::Int8; };
template <> struct ret_type_tag<uint8_t>    { static constexpr TypeTag value = TypeTag::UInt8; };
template <> struct ret_type_tag<std::string_view> { static constexpr TypeTag value = TypeTag::String; };
template <> struct ret_type_tag<std::string>    { static constexpr TypeTag value = TypeTag::String; };
template <> struct ret_type_tag<std::span<const uint8_t>> { static constexpr TypeTag value = TypeTag::String; };
template <> struct ret_type_tag<std::unique_ptr<geos::geom::Geometry>> { static constexpr TypeTag value = TypeTag::String; };
template <> struct ret_type_tag<raw_buffer>   { static constexpr TypeTag value = TypeTag::String; };

// Get the byte size for a type tag.
static inline size_t tag_byte_size(TypeTag tag) {
    switch (tag) {
        case TypeTag::Int8:    case TypeTag::UInt8:   return 1;
        case TypeTag::Int16:   case TypeTag::UInt16:  return 2;
        case TypeTag::Int32:   case TypeTag::UInt32:  return 4;
        case TypeTag::Int64:   case TypeTag::UInt64:  return 8;
        case TypeTag::Float32: return 4;
        case TypeTag::Float64: return 8;
        default: return 0;
    }
}

// ── ColBinaryBuf ─────────────────────────────────────────────────────────────

struct ColBinaryInfo {
    bool            is_const;
    uint32_t        type_tags_size;  // number of type tag bytes (0 = none)
    std::vector<TypeTag> type_tags;  // recursive stream for auto-coercion
    uint64_t        data_size;
    const uint8_t*  data;

    // O(1) data_size for u32 offset format: (N+1)*4 + last_offset.
    // Callers: compute this before building the column data block.
    static inline uint64_t compute_data_size_u32_offsets(uint64_t num_rows, uint64_t total_bytes) {
        return (num_rows + 1u) * 4u + total_bytes;
    }
};

struct ColBinaryBuf {
    uint32_t num_rows;
    uint32_t num_cols;
    std::vector<ColBinaryInfo> cols;
};

// ── Varint helpers ────────────────────────────────────────────────────────────

static inline uint32_t getLengthOfVarUInt(uint32_t n) {
    if (n < 0x80u) return 1;
    if (n < 0x4000u) return 2;
    if (n < 0x200000u) return 3;
    if (n < 0x10000000u) return 4;
    return 5;
}

static inline bool readVarUInt(const uint8_t* p, const uint8_t* end, uint64_t& result) {
    result = 0;
    int shift = 0;
    const uint8_t* q = p;
    while (q < end) {
        uint8_t byte = *q++;
        result |= static_cast<uint64_t>(byte & 0x7F) << shift;
        if ((byte & 0x80) == 0) return true;
        shift += 7;
        if (shift >= 63) return false;
    }
    return false;
}

static inline void writeVarUInt(uint64_t n, raw_buffer& buf) {
    while (n >= 0x80) {
        buf.push_back(static_cast<uint8_t>((n & 0x7F) | 0x80));
        n >>= 7;
    }
    buf.push_back(static_cast<uint8_t>(n));
}

static inline void writeBinaryLE64(uint64_t n, raw_buffer& buf) {
    for (uint32_t i = 0; i < 8; ++i)
        buf.push_back(static_cast<uint8_t>(n >> (i * 8)));
}

static inline void writeBinaryLE32(uint32_t n, raw_buffer& buf) {
    for (uint32_t i = 0; i < 4; ++i)
        buf.push_back(static_cast<uint8_t>(n >> (i * 8)));
}

static inline bool readBinaryLE64(const uint8_t*& p, const uint8_t* end, uint64_t& out) {
    if (p + 8 > end) return false;
    out = 0;
    for (int i = 0; i < 8; ++i)
        out |= static_cast<uint64_t>(p[i]) << (i * 8);
    p += 8;
    return true;
}

// ── parse_col_binary: ColumnBinary format ─────────────────────────────────────

inline ColBinaryBuf parse_col_binary(const raw_buffer* buf) {
    ColBinaryBuf cb;
    const uint8_t* p = buf->data();
    const uint8_t* end = p + buf->size();
    uint32_t v32;
    uint64_t v64;

    if (!readBinaryLE32(p, end, v32)) ch::panic("parse_col_binary: truncated num_columns");
    cb.num_cols = v32;

    if (!readBinaryLE32(p, end, v32)) ch::panic("parse_col_binary: truncated num_rows");
    cb.num_rows = v32;

    for (uint32_t i = 0; i < cb.num_cols; ++i) {
        ColBinaryInfo ci;

        // flags (u8)
        if (p >= end) ch::panic("parse_col_binary: truncated flags");
        uint8_t flags = *p++;
        ci.is_const = (flags & 0x01) != 0;

        // type_tags_size (u32LE) + type_tags
        ci.type_tags = read_type_tags(p, end, ci.type_tags_size);

        // data_size (u64LE)
        if (!readBinaryLE64(p, end, v64)) ch::panic("parse_col_binary: truncated data_size");
        ci.data_size = v64;

        // data
        if (p + ci.data_size > end) ch::panic("parse_col_binary: data overflows buffer");
        ci.data = p;
        p += ci.data_size;

        cb.cols.push_back(ci);
    }
    return cb;
}

// ── ColBinaryReader: sequential access ────────────────────────────────────────
// For const columns: always returns the same single-row data.
// Auto-coercion: if type_tags is populated and the wire type is narrower than
// the C++ template parameter, the reader reads the narrower type and promotes.
// type_tags empty → no coercion (backward compatible with existing tests).

struct ColBinaryReader {
    bool           is_const;
    uint32_t       stored_rows;   // 1 if const, else num_rows
    const uint8_t* data_start;    // data section start (offsets for COL_BYTES, raw for fixed-width)
    const uint8_t* data_end;      // data_start + data_size
    mutable const uint8_t* p;     // cursor for sequential reads (COL_BYTES non-const)
    std::vector<TypeTag> type_tags;
    // Cached wire size from the last consumed type tag (for non-const multi-value reads).
    // Set when a tag is consumed; reused when no tag is available.
    mutable size_t cached_wire_size = 0;

    std::vector<TypeTag>::const_iterator tag_iter() const { return type_tags.begin(); }

    // Cached blob for const-mode COL_BYTES (parsed once, returned on every call).
    mutable std::span<const uint8_t> cached_blob{};
    mutable bool blob_cached = false;

    // Current row index for non-const COL_BYTES columns (used by array parsing).
    mutable uint32_t current_row = 0;

    // Cached offsets for non-const array columns (parsed once, reused per row).
    mutable std::vector<uint64_t> cached_offsets{};
    mutable bool offsets_cached = false;

    // u32 offsets for non-const COL_BYTES (new format: u32[N+1] + raw bytes).
    mutable const uint32_t* cached_u32_offsets = nullptr;
    mutable const uint8_t*  cached_u32_data = nullptr;
    mutable bool u32_offsets_cached = false;

    bool is_null(uint32_t) const { return false; }

    // Read a variable-length blob.
    // const: u32[2] offsets at start of data → span between offsets[0] and offsets[1].
    // non-const (new format): u32[N+1] offsets + raw bytes → lookup offsets[current_row].
    // non-const (legacy format): varint(len) + payload at cursor.
    std::span<const uint8_t> read_blob() noexcept {
        if (is_const) {
            if (!blob_cached) {
                blob_cached = true;
                cached_blob = parse_blob_from(data_start, data_end);
            }
            return cached_blob;
        }
        auto span = read_blob_from_cursor();
        ++current_row;
        return span;
    }

    // Read exactly n bytes (fixed-size types: double, int32, etc.).
    std::span<const uint8_t> read_fixed_bytes(size_t n) const noexcept {
        if (is_const) {
            if (n == 0) return {};
            return {data_start, n};
        }
        if (p + n > data_end) return {};
        const uint8_t* start = p;
        p += n;
        return {start, n};
    }

    // Read a fixed-size scalar from cursor (non-const, sequential).
    template <typename T>
    T read_fixed_seq() const noexcept {
        T v;
        if (is_const) {
            std::memcpy(&v, data_start, sizeof(T));
            return v;
        }
        if (p + sizeof(T) > data_end) return T{};
        std::memcpy(&v, p, sizeof(T));
        p += sizeof(T);
        return v;
    }

    // Read a fixed-size scalar, auto-promoting from narrower wire types.
    // e.g. Int8 wire → int32_t C++: reads 1 byte, sign-extends to 4 bytes.
    // For const columns: reads from data_start (single value).
    // For non-const: reads from cursor at wire_size, advances by wire_size.
    template <typename T>
    T read_fixed(uint32_t row) const noexcept {
        T v{};
        size_t elem_size = sizeof(T);
        const uint8_t* elem_ptr = data_start + (is_const ? 0 : row) * elem_size;
        std::memcpy(&v, elem_ptr, elem_size);
        return v;
    }

    // Read a fixed-size scalar, auto-promoting from narrower wire types.
    // wire_tag: the consumed type tag from the iterator.
    // For const columns: reads from data_start (single value).
    // For non-const: reads from cursor p, advances by wire_size (or cached_wire_size for default).
    template <typename T>
    T read_from_tag(TypeTag wire_tag) const noexcept {
        size_t wire_size = tag_byte_size(wire_tag);
        size_t cpp_size = sizeof(T);

        if (wire_size == 0 || wire_size == cpp_size) {
            // Same size or unknown tag — direct read from cursor.
            T v;
            const uint8_t* src = is_const ? data_start : p;
            if (src + cpp_size > data_end) return T{};
            std::memcpy(&v, src, cpp_size);
            if (!is_const) p += cpp_size;
            return v;
        }

        // wire_size < cpp_size: auto-widen (sign-extend for signed, zero-extend for unsigned).
        uint8_t buf[8] = {};
        const uint8_t* src = is_const ? data_start : p;
        if (src + wire_size > data_end) return T{};
        std::memcpy(buf, src, wire_size);
        if (std::is_signed_v<T>) {
            uint8_t sign = buf[wire_size - 1] & 0x80 ? 0xFF : 0x00;
            for (size_t i = wire_size; i < cpp_size; ++i)
                buf[i] = sign;
        }
        T v;
        std::memcpy(&v, buf, cpp_size);
        if (!is_const) p += wire_size;  // advance by wire size, not cpp size
        return v;
    }

    static ColBinaryReader from_col(const ColBinaryInfo& ci, uint32_t num_rows) {
        ColBinaryReader r;
        r.is_const         = ci.is_const;
        r.stored_rows      = r.is_const ? 1 : num_rows;
        r.data_start       = ci.data;
        r.data_end         = ci.data + ci.data_size;
        r.p                = r.data_start;
        r.blob_cached      = false;
        r.u32_offsets_cached = false;
        r.cached_u32_offsets = nullptr;
        r.cached_u32_data    = nullptr;
        r.current_row        = 0;
        r.type_tags        = ci.type_tags;
        r.cached_wire_size = 0;
        return r;
    }

    // Parse a u32-offset blob from the start of a const column's data.
    // Format: u32[2] {offset_0, offset_1} + raw bytes → span at data[offset_0..offset_1).
    static std::span<const uint8_t> parse_blob_from(const uint8_t* start, const uint8_t* end) {
        if (!start || start + 8 > end) return {};
        uint32_t o0, o1;
        std::memcpy(&o0, start, 4);
        std::memcpy(&o1, start + 4, 4);
        if (o0 > o1) return {};
        const uint8_t* data = start + 8;  // skip the 2-entry u32 offset table
        if (data + o1 > end) return {};
        return {data + o0, static_cast<size_t>(o1 - o0)};
    }

    // Read a blob using u32 offset lookup (new format) or varint (legacy).
    // New format: u32[N+1] offsets at data_start, raw bytes follow.
    // Legacy format: sequential varint(len) + payload.
    std::span<const uint8_t> read_blob_from_cursor() noexcept {
        if (!data_start || !data_end) return {};

        // Try u32 offset path first.
        const uint32_t* u32_offs = nullptr;
        const uint8_t*  u32_dat  = nullptr;

        if (u32_offsets_cached) {
            u32_offs = cached_u32_offsets;
            u32_dat  = cached_u32_data;
        } else {
            uint32_t n = stored_rows;
            if (n + 1u < 2u) n = 1u;
            u32_offs = reinterpret_cast<const uint32_t*>(data_start);
            u32_dat  = data_start + (n + 1u) * sizeof(uint32_t);

            // Validate: offsets[N] + remaining bytes must not exceed data_end.
            uint32_t last_off = u32_offs[n];
            if (u32_dat <= data_end && u32_offs[0] == 0 && u32_dat + last_off == data_end) {
                cached_u32_offsets = u32_offs;
                cached_u32_data = u32_dat;
                u32_offsets_cached = true;
            } else {
                // Legacy varint format — skip u32 path.
                u32_offs = nullptr;
            }
        }

        if (u32_offs) {
            uint32_t row = current_row;
            if (row + 1 > stored_rows) return {};
            uint32_t o0 = u32_offs[row];
            uint32_t o1 = u32_offs[row + 1];
            if (o0 > o1) return {};
            p = u32_dat + o0;
            return {p, static_cast<size_t>(o1 - o0)};
        }

        // Legacy varint path (for backward compatibility).
        if (!p) return {};
        const uint8_t* limit = data_end;
        uint64_t data_len = 0;
        if (!readVarUInt(p, limit, data_len)) return {};
        uint32_t prefix = getLengthOfVarUInt(static_cast<uint32_t>(data_len));
        if (p + prefix > limit) return {};
        if (p + prefix + data_len > limit) return {};
        const uint8_t* start = p + prefix;
        p += prefix + static_cast<uint32_t>(data_len);
        return {start, static_cast<size_t>(data_len)};
    }

    // Read a u64LE value from the cursor (for parsing Native array offsets).
    bool read_u64le(uint64_t& out) const noexcept {
        if (!p || p + 8 > data_end) return false;
        out = 0;
        for (int i = 0; i < 8; ++i)
            out |= static_cast<uint64_t>(p[i]) << (i * 8);
        p += 8;
        return true;
    }

    // Peek at the next blob without advancing the cursor.
    // Uses u32 offset lookup for new format, varint for legacy.
    std::span<const uint8_t> peek_blob() const noexcept {
        if (!data_start || !data_end) return {};

        if (is_const) {
            if (!blob_cached) {
                const_cast<ColBinaryReader*>(this)->blob_cached = true;
                const_cast<ColBinaryReader*>(this)->cached_blob = parse_blob_from(data_start, data_end);
            }
            return cached_blob;
        }

        // For non-const, use the u32 offset at current_row.
        if (!u32_offsets_cached) {
            uint32_t n = stored_rows;
            if (n + 1u < 2u) n = 1u;
            const uint32_t* offs = reinterpret_cast<const uint32_t*>(data_start);
            const uint8_t*  dat  = data_start + (n + 1u) * sizeof(uint32_t);
            uint32_t last_off = offs[n];
            if (dat + last_off <= data_end) {
                const_cast<ColBinaryReader*>(this)->cached_u32_offsets = offs;
                const_cast<ColBinaryReader*>(this)->cached_u32_data = dat;
                const_cast<ColBinaryReader*>(this)->u32_offsets_cached = true;
            }
        }

        if (u32_offsets_cached && cached_u32_offsets) {
            uint32_t row = current_row;
            if (row + 1 > stored_rows) return {};
            uint32_t o0 = cached_u32_offsets[row];
            uint32_t o1 = cached_u32_offsets[row + 1];
            if (o0 > o1) return {};
            return {cached_u32_data + o0, static_cast<size_t>(o1 - o0)};
        }

        // Legacy varint path.
        if (!p || p >= data_end) return {};
        const uint8_t* limit = data_end;
        uint64_t data_len = 0;
        if (!readVarUInt(p, limit, data_len)) return {};
        uint32_t prefix = getLengthOfVarUInt(static_cast<uint32_t>(data_len));
        if (p + prefix > limit) return {};
        if (p + prefix + data_len > limit) return {};
        return {p + prefix, static_cast<size_t>(data_len)};
    }
};

// ── pack_result: write a single result value into a raw_buffer ───────────────

static inline void pack_result(raw_buffer& buf, bool v) {
    uint8_t byte = v ? 1u : 0u;
    buf.push_back(byte);
}

static inline void pack_result(raw_buffer& buf, double v) {
    uint8_t bytes[8];
    std::memcpy(bytes, &v, 8);
    for (uint32_t i = 0; i < 8; ++i)
        buf.push_back(bytes[i]);
}

static inline void pack_result(raw_buffer& buf, int32_t v) {
    uint8_t bytes[4];
    std::memcpy(bytes, &v, 4);
    for (uint32_t i = 0; i < 4; ++i)
        buf.push_back(bytes[i]);
}

static inline void pack_result(raw_buffer& buf, uint32_t v) {
    uint8_t bytes[4];
    std::memcpy(bytes, &v, 4);
    for (uint32_t i = 0; i < 4; ++i)
        buf.push_back(bytes[i]);
}

static inline void pack_result(raw_buffer& buf, std::unique_ptr<geos::geom::Geometry> g) {
    if (!g) { writeVarUInt(0u, buf); return; }
    auto wkb = write_ewkb(g);
    writeVarUInt(static_cast<uint64_t>(wkb.size()), buf);
    for (size_t i = 0; i < wkb.size(); ++i)
        buf.push_back(wkb[i]);
}

static inline void pack_result(raw_buffer& buf, std::string_view s) {
    uint32_t len = static_cast<uint32_t>(s.size());
    writeVarUInt(static_cast<uint64_t>(len), buf);
    for (uint32_t i = 0; i < len; ++i)
        buf.push_back(static_cast<uint8_t>(s[i]));
}

static inline void pack_result(raw_buffer& buf, raw_buffer v) {
    writeVarUInt(static_cast<uint64_t>(v.size()), buf);
    for (size_t i = 0; i < v.size(); ++i)
        buf.push_back(v[i]);
}

// ── unpack_arg: deserialize one argument from ColBinaryReader ────────────────
// Type tags are consumed hierarchically: each specialization consumes the tag
// matching its C++ type (Array→vector, Nullable→optional, scalar→widen).
// Tags empty → no coercion, read native size.

template <typename T>
T unpack_arg(ColBinaryReader& r, std::vector<TypeTag>::const_iterator tags_it) {
    if constexpr (std::is_arithmetic_v<T>) {
        // Consume the next type tag if available; otherwise use cached wire size.
        TypeTag wire_tag = TypeTag::Int32;
        bool has_tag = (tags_it != r.type_tags.end());
        if (has_tag) {
            wire_tag = *tags_it++;
        } else if (r.cached_wire_size > 0) {
            // Reuse wire size from the previously consumed tag.
            wire_tag = static_cast<TypeTag>(r.cached_wire_size);
        } else {
            // No tag and no cached size — read native C++ size from cursor.
            return r.read_fixed_seq<T>();
        }
        if constexpr (std::is_same_v<T, double>)       return r.read_from_tag<double>(wire_tag);
        else if constexpr (std::is_same_v<T, float>)   return r.read_from_tag<float>(wire_tag);
        else if constexpr (std::is_same_v<T, int32_t>) return r.read_from_tag<int32_t>(wire_tag);
        else if constexpr (std::is_same_v<T, uint32_t>)return r.read_from_tag<uint32_t>(wire_tag);
        else if constexpr (std::is_same_v<T, int64_t>) return r.read_from_tag<int64_t>(wire_tag);
        else if constexpr (std::is_same_v<T, uint64_t>)return r.read_from_tag<uint64_t>(wire_tag);
        else if constexpr (std::is_same_v<T, int16_t>) return r.read_from_tag<int16_t>(wire_tag);
        else if constexpr (std::is_same_v<T, uint16_t>)return r.read_from_tag<uint16_t>(wire_tag);
        else if constexpr (std::is_same_v<T, int8_t>)  return r.read_from_tag<int8_t>(wire_tag);
        else if constexpr (std::is_same_v<T, uint8_t>) return r.read_from_tag<uint8_t>(wire_tag);
        else return r.read_fixed<T>(0);
        // Cache the wire size for subsequent reads (when no tag is available).
        if (has_tag) {
            const_cast<ColBinaryReader&>(r).cached_wire_size = tag_byte_size(wire_tag);
        }
    }
    // String: no type tags, read blob.
    else if constexpr (std::is_same_v<T, std::string_view>) {
        auto span = r.read_blob();
        return {reinterpret_cast<const char*>(span.data()), span.size()};
    }
    // Span: no type tags, read blob.
    else if constexpr (std::is_same_v<T, std::span<const uint8_t>>) {
        return r.read_blob();
    }
    // unique_ptr<Geometry>: no type tags, read blob as WKB/WKT.
    else if constexpr (std::is_same_v<T, std::unique_ptr<geos::geom::Geometry>>) {
        auto span = r.read_blob();
        if (span.empty()) return nullptr;
        if (ch::is_wkb(span)) return ch::read_wkb(span);
        return ch::read_wkt(span);
    }
    // Geometry*: no type tags, read blob as WKB/WKT (steals ownership).
    else if constexpr (std::is_same_v<T, geos::geom::Geometry*>) {
        auto span = r.read_blob();
        if (span.empty()) return nullptr;
        try { return ch::read_wkb(span).release(); }
        catch (...) { return ch::read_wkt(span).release(); }
    }
    // Geometry const*: delegates to non-const.
    else if constexpr (std::is_same_v<T, geos::geom::Geometry const*>) {
        return unpack_arg<geos::geom::Geometry*>(r, tags_it);
    }
    // std::optional<T>: consume Nullable tag, recurse into T.
    else if constexpr (std::is_same_v<T, std::optional<typename T::value_type>>) {
        if (tags_it != r.type_tags.end()) ++tags_it;  // consume Nullable tag
        return unpack_arg<typename T::value_type>(r, tags_it);
    }
    // std::vector<T>: consume Array tag, recurse into T.
    else if constexpr (std::is_same_v<T, std::vector<typename T::value_type>>) {
        return unpack_arg_vector<typename T::value_type>(r, tags_it);
    }
    else {
        static_assert(sizeof(T) == 0, "unpack_arg: unsupported type");
    }
}

template <>
inline std::vector<std::unique_ptr<geos::geom::Geometry>>
unpack_arg<std::vector<std::unique_ptr<geos::geom::Geometry>>>(ColBinaryReader& r, std::vector<TypeTag>::const_iterator tags_it) {
    if (tags_it != r.type_tags.end()) ++tags_it;  // consume Array tag
    std::vector<std::unique_ptr<Geometry>> result;

    if (!r.data_start || !r.data_end) return result;

    auto parse_geom = [](const uint8_t* p, size_t len) -> std::unique_ptr<Geometry> {
        if (len == 0) return nullptr;
        auto sp = std::span<const uint8_t>{p, len};
        if (ch::is_wkb(sp)) return ch::read_wkb(sp);
        return ch::read_wkt(sp);
    };

    if (r.is_const) {
        // [N u64 per-row counts][u32[M+1] cumul str offsets][chars]
        // (ColumnBinary: no null terminators in data)
        const uint8_t* p = r.data_start;
        const uint8_t* end = r.data_end;
        uint64_t M = 0;
        // Read all per-row counts to skip past them to u32 offsets.
        uint32_t n = r.stored_rows;
        if (p + n * 8 > end) return result;
        const uint8_t* off_p = p + n * 8;
        if (n > 0) {
            for (uint32_t i = 0; i < n; ++i) {
                uint64_t count = 0;
                std::memcpy(&count, p, 8); p += 8;
                M = count;
            }
        }
        if (M == 0 || off_p + (M + 1) * 4 > end) return result;
        const uint32_t* str_offs = reinterpret_cast<const uint32_t*>(off_p);
        const uint8_t* chars = off_p + (M + 1) * 4;
        result.reserve(static_cast<size_t>(M));
        for (uint64_t i = 0; i < M; ++i) {
            uint32_t o0 = str_offs[i], o1 = str_offs[i + 1];
            if (o0 > o1 || chars + o1 > end) { result.push_back(nullptr); continue; }
            result.push_back(parse_geom(chars + o0, static_cast<size_t>(o1 - o0)));
        }
    } else {
        // Non-const: [N u64 per-row counts][u32[total_M+1] offsets][chars].
        // Cache row-start table and string-offset pointer on first call.
        if (!r.offsets_cached) {
            const uint8_t* p = r.data_start;
            const uint8_t* end = r.data_end;
            r.cached_offsets.clear();
            r.cached_offsets.reserve(r.stored_rows + 1);
            r.cached_offsets.push_back(0);
            uint64_t total_M = 0;
            for (uint32_t i = 0; i < r.stored_rows && p + 8 <= end; ++i) {
                uint64_t count = 0;
                std::memcpy(&count, p, 8); p += 8;
                total_M += count;
                r.cached_offsets.push_back(total_M);
            }
            if (total_M > 0 && p + (total_M + 1) * 4 <= end) {
                r.cached_u32_offsets = reinterpret_cast<const uint32_t*>(p);
                r.cached_u32_data = p + (total_M + 1) * 4;
            }
            r.offsets_cached = true;
        }

        uint32_t row = r.current_row++;
        if (row + 1 >= static_cast<uint32_t>(r.cached_offsets.size())) return result;
        uint64_t elem_start = r.cached_offsets[row];
        uint64_t elem_end   = r.cached_offsets[row + 1];
        if (elem_start >= elem_end || !r.cached_u32_offsets || !r.cached_u32_data) return result;

        const uint8_t* chars    = r.cached_u32_data;
        const uint8_t* data_end = r.data_end;
        result.reserve(static_cast<size_t>(elem_end - elem_start));
        for (uint64_t i = elem_start; i < elem_end; ++i) {
            uint32_t o0 = r.cached_u32_offsets[i], o1 = r.cached_u32_offsets[i + 1];
            if (o0 > o1 || chars + o1 > data_end) { result.push_back(nullptr); continue; }
            result.push_back(parse_geom(chars + o0, static_cast<size_t>(o1 - o0)));
        }
    }
    return result;
}

// Generic std::vector<T> unpacking: consumes Array tag, recurses into T.
// Wire format: [N u64 per-row counts][u32[M+1] cumul offsets][T-serialized data]
template <typename T>
std::vector<T> unpack_arg_vector(ColBinaryReader& r, std::vector<TypeTag>::const_iterator tags_it) {
    if (tags_it != r.type_tags.end()) ++tags_it;  // consume Array tag
    std::vector<T> result;

    if (!r.data_start || !r.data_end) return result;

    if (r.is_const) {
        // [u64 M][u32[M+1] cumul offsets][data]
        const uint8_t* p = r.data_start;
        const uint8_t* end = r.data_end;
        uint64_t M = 0;
        if (p + 8 <= end) {
            std::memcpy(&M, p, 8); p += 8;
        }
        if (M == 0 || p + (M + 1) * 4 > end) return result;
        const uint32_t* offs = reinterpret_cast<const uint32_t*>(p);
        const uint8_t* data = p + (M + 1) * 4;
        result.reserve(static_cast<size_t>(M));
        for (uint64_t i = 0; i < M; ++i) {
            uint32_t o0 = offs[i], o1 = offs[i + 1];
            if (o0 > o1 || data + o1 > end) { result.push_back(T{}); continue; }
            ColBinaryInfo elem_ci;
            elem_ci.is_const = true;
            elem_ci.data = data + o0;
            elem_ci.data_size = o1 - o0;
            ColBinaryReader elem_r = ColBinaryReader::from_col(elem_ci, 1);
            result.push_back(unpack_arg<T>(elem_r, tags_it));
        }
    } else {
        // [N u64 per-row counts][u32[total_M+1] cumul offsets][data]
        const uint8_t* p = r.data_start;
        const uint8_t* end = r.data_end;
        std::vector<uint64_t> row_starts;
        row_starts.reserve(r.stored_rows + 1);
        row_starts.push_back(0);
        uint64_t total_M = 0;
        for (uint32_t i = 0; i < r.stored_rows && p + 8 <= end; ++i) {
            uint64_t count = 0;
            std::memcpy(&count, p, 8); p += 8;
            total_M += count;
            row_starts.push_back(total_M);
        }
        if (total_M == 0 || p + (total_M + 1) * 4 > end) return result;
        const uint32_t* offs = reinterpret_cast<const uint32_t*>(p);
        const uint8_t* data = p + (total_M + 1) * 4;
        for (uint32_t row = 0; row < r.stored_rows; ++row) {
            uint64_t elem_start = row < row_starts.size() ? row_starts[row] : 0;
            uint64_t elem_end = (row + 1) < row_starts.size() ? row_starts[row + 1] : 0;
            if (elem_start >= elem_end || elem_end > total_M) continue;
            for (uint64_t i = elem_start; i < elem_end; ++i) {
                uint32_t o0 = offs[i], o1 = offs[i + 1];
                if (o0 > o1 || data + o1 > end) { result.push_back(T{}); continue; }
                ColBinaryInfo elem_ci;
                elem_ci.is_const = true;
                elem_ci.data = data + o0;
                elem_ci.data_size = o1 - o0;
                ColBinaryReader elem_r = ColBinaryReader::from_col(elem_ci, 1);
                result.push_back(unpack_arg<T>(elem_r, tags_it));
            }
        }
    }
    return result;
}

// ── col_binary_impl_worker: return-type dispatch ─────────────────────────────

template <typename Ret, size_t N, typename Invoke>
struct col_binary_impl_worker {
    static void run(raw_buffer& out, uint32_t n, const char* func_name, Invoke& invoke,
                    std::array<ColBinaryReader, N>&,
                    BboxOp, bool,
                    ColPrepOp, ColPrepOp,
                    ColPrepDistOp, ColPrepDistOp,
                    ColPrepPointOp, ColPrepPointOp) {
        for (uint32_t i = 0; i < n; ++i)
            pack_result(out, invoke());
    }
};

template <size_t N, typename Invoke>
struct col_binary_impl_worker<bool, N, Invoke> {
    static void run(raw_buffer& out, uint32_t n, const char* func_name, Invoke& invoke,
                    std::array<ColBinaryReader, N>& readers,
                    BboxOp bbox_op, bool early_ret,
                    ColPrepOp prep_a, ColPrepOp prep_b,
                    ColPrepDistOp prep_a_dist, ColPrepDistOp prep_b_dist,
                    ColPrepPointOp prep_a_point, ColPrepPointOp prep_b_point)
    {
        using PGF = geos::geom::prep::PreparedGeometryFactory;

        // ── A-const path ────────────────────────────────────────────────────
        if (readers[0].is_const && prep_a && n > 0) {
            auto span_a = unpack_arg<std::span<const uint8_t>>(readers[0], readers[0].tag_iter());
            if (span_a.empty() && n > 1) {
                for (uint32_t i = 0; i < n; ++i)
                    pack_result(out, false);
            } else {
                BBox bbox_a = wkb_bbox(span_a);
                auto geom_a = read_wkb(span_a);
                auto pa = PGF::prepare(geom_a.get());

                if (prep_a_point) {
                    auto s1 = readers[1].peek_blob();
                    uint32_t pt_type = 0;
                    if (s1.size() == 21 && s1[0] == 0x01)
                        std::memcpy(&pt_type, s1.data() + 1, 4);
                    auto gtype = geom_a->getGeometryTypeId();
                    if (pt_type == 1u &&
                        (gtype == geos::geom::GEOS_POLYGON ||
                         gtype == geos::geom::GEOS_MULTIPOLYGON)) {
                        using IPIAL = geos::algorithm::locate::IndexedPointInAreaLocator;
                        IPIAL locator(*geom_a);
                        for (uint32_t i = 0; i < n; ++i) {
                            auto span_b = readers[1].read_blob();
                            double px, py;
                            std::memcpy(&px, span_b.data() + 5, 8);
                            std::memcpy(&py, span_b.data() + 13, 8);
                            if (bbox_op && !bbox_op(bbox_a, BBox{px, py, px, py})) {
                                pack_result(out, early_ret); continue;
                            }
                            pack_result(out, prep_a_point(&locator, px, py));
                        }
                    } else {
                        for (uint32_t i = 0; i < n; ++i) {
                            auto span_b = readers[1].read_blob();
                            if (bbox_op && !bbox_op(bbox_a, wkb_bbox(span_b))) {
                                pack_result(out, early_ret); continue;
                            }
                            pack_result(out, prep_a(pa.get(), read_wkb(span_b).get()));
                        }
                    }
                } else {
                    for (uint32_t i = 0; i < n; ++i) {
                        auto span_b = readers[1].read_blob();
                        if (bbox_op && !bbox_op(bbox_a, wkb_bbox(span_b))) {
                            pack_result(out, early_ret); continue;
                        }
                        pack_result(out, prep_a(pa.get(), read_wkb(span_b).get()));
                    }
                }
            }
            return;
        }

        // ── B-const path ────────────────────────────────────────────────────
        if (readers[1].is_const && prep_b && n > 0) {
            auto span_b = unpack_arg<std::span<const uint8_t>>(readers[1], readers[1].tag_iter());
            BBox bbox_b = wkb_bbox(span_b);
            auto geom_b = read_wkb(span_b);
            auto pb = PGF::prepare(geom_b.get());

            if (prep_b_point) {
                auto s0 = readers[0].peek_blob();
                uint32_t pt_type = 0;
                if (s0.size() == 21 && s0[0] == 0x01)
                    std::memcpy(&pt_type, s0.data() + 1, 4);
                auto gtype = geom_b->getGeometryTypeId();
                if (pt_type == 1u &&
                    (gtype == geos::geom::GEOS_POLYGON ||
                     gtype == geos::geom::GEOS_MULTIPOLYGON)) {
                    using IPIAL = geos::algorithm::locate::IndexedPointInAreaLocator;
                    IPIAL locator(*geom_b);
                    for (uint32_t i = 0; i < n; ++i) {
                        auto span_a = readers[0].read_blob();
                        double px, py;
                        std::memcpy(&px, span_a.data() + 5, 8);
                        std::memcpy(&py, span_a.data() + 13, 8);
                        if (bbox_op && !bbox_op(BBox{px, py, px, py}, bbox_b)) {
                            pack_result(out, early_ret); continue;
                        }
                        pack_result(out, prep_b_point(&locator, px, py));
                    }
                } else {
                    for (uint32_t i = 0; i < n; ++i) {
                        auto span_a = readers[0].read_blob();
                        if (bbox_op && !bbox_op(wkb_bbox(span_a), bbox_b)) {
                            pack_result(out, early_ret); continue;
                        }
                        pack_result(out, prep_b(pb.get(), read_wkb(span_a).get()));
                    }
                }
            } else {
                for (uint32_t i = 0; i < n; ++i) {
                    auto span_a = readers[0].read_blob();
                    if (bbox_op && !bbox_op(wkb_bbox(span_a), bbox_b)) {
                        pack_result(out, early_ret); continue;
                    }
                    auto geom_a = read_wkb(span_a);
                    pack_result(out, geom_a ? prep_b(pb.get(), geom_a.get()) : false);
                }
            }
            return;
        }

        // ── 3-arg dist path (st_dwithin) ────────────────────────────────────
        if constexpr (N >= 3) {
            if (readers[0].is_const && prep_a_dist && n > 0) {
                auto span_a = unpack_arg<std::span<const uint8_t>>(readers[0], readers[0].tag_iter());
                BBox bbox_a = wkb_bbox(span_a);
                auto geom_a = read_wkb(span_a);
                auto pa = PGF::prepare(geom_a.get());
                for (uint32_t i = 0; i < n; ++i) {
                    auto span_b = readers[1].read_blob();
                    double dist = unpack_arg<double>(readers[2], readers[2].tag_iter());
                    if (!bbox_a.intersects(wkb_bbox(span_b).expanded(dist))) {
                        pack_result(out, false); continue;
                    }
                    pack_result(out, prep_a_dist(pa.get(), read_wkb(span_b).get(), dist));
                }
                return;
            }
            if (readers[1].is_const && prep_b_dist && n > 0) {
                auto span_b = unpack_arg<std::span<const uint8_t>>(readers[1], readers[1].tag_iter());
                BBox bbox_b = wkb_bbox(span_b);
                auto geom_b = read_wkb(span_b);
                auto pb = PGF::prepare(geom_b.get());
                for (uint32_t i = 0; i < n; ++i) {
                    auto span_a = readers[0].read_blob();
                    double dist = unpack_arg<double>(readers[2], readers[2].tag_iter());
                    if (!wkb_bbox(span_a).intersects(bbox_b.expanded(dist))) {
                        pack_result(out, false); continue;
                    }
                    pack_result(out, prep_b_dist(pb.get(), read_wkb(span_a).get(), dist));
                }
                return;
            }
        }

        // ── Fallback ────────────────────────────────────────────────────────
        for (uint32_t i = 0; i < n; ++i)
            pack_result(out, invoke());
    }
};

// ── col_binary_impl_wrapper ──────────────────────────────────────────────────

template <typename Ret, typename... Args>
raw_buffer* col_binary_impl_wrapper(const char* func_name,
                                    raw_buffer* ptr,
                                    uint32_t num_rows,
                                    Ret (*impl)(Args...),
                                    BboxOp         bbox_op      = nullptr,
                                    bool           early_ret    = false,
                                    ColPrepOp      prep_a       = nullptr,
                                    ColPrepOp      prep_b       = nullptr,
                                    ColPrepDistOp  prep_a_dist  = nullptr,
                                    ColPrepDistOp  prep_b_dist  = nullptr,
                                    ColPrepPointOp prep_a_point = nullptr,
                                    ColPrepPointOp prep_b_point = nullptr)
{
    auto cb = parse_col_binary(ptr);
    uint32_t n = cb.num_rows;
    constexpr size_t nargs = sizeof...(Args);

    std::array<ColBinaryReader, nargs> readers;
    for (size_t j = 0; j < nargs; ++j) {
        if (j < cb.cols.size())
            readers[j] = ColBinaryReader::from_col(cb.cols[j], n);
    }

    auto invoke = [&readers, impl]() {
        return [&]<size_t... I>(std::index_sequence<I...>) {
            return impl(unpack_arg<std::decay_t<Args>>(readers[I], readers[I].tag_iter())...);
        }(std::make_index_sequence<nargs>{});
    };

    raw_buffer* out = nullptr;
    try {
        out = clickhouse_create_buffer(28);
        out->clear();

        // Output header: num_cols(u32LE) + num_rows(u32LE) + flags(u8) + type_tags_size(u32LE) + type_tag + data_size(u64LE)
        writeBinaryLE32(1u, *out);
        writeBinaryLE32(static_cast<uint32_t>(n), *out);
        out->push_back(0x00);  // flags: 0 (non-const output)
        out->push_back(1); out->push_back(0); out->push_back(0); out->push_back(0); // type_tags_size = 1
        out->push_back(static_cast<uint8_t>(ret_type_tag<std::decay_t<Ret>>::value));

        size_t data_size_pos = out->size();
        writeBinaryLE64(0u, *out);  // placeholder; patched after data is written

        col_binary_impl_worker<Ret, nargs, decltype(invoke)>::run(
            *out, n, func_name, invoke, readers,
            bbox_op, early_ret,
            prep_a, prep_b, prep_a_dist, prep_b_dist,
            prep_a_point, prep_b_point);

        uint64_t data_size = static_cast<uint64_t>(out->size() - data_size_pos - 8);
        for (int i = 0; i < 8; ++i)
            (*out)[data_size_pos + i] = static_cast<uint8_t>(data_size >> (i * 8));

        return out;

    } catch (const std::exception& e) {
        if (out) clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(out));
        panic(e.what());
    }
    __builtin_unreachable();
}

// ── st_knn_cb: k-nearest-neighbour (ColumnBinary) ─────────────────────────────
// Signature: st_knn_cb(query String, candidates Array(String), k UInt32)
//            → Array(Tuple(UInt64, Float64))
//
// Output ColumnBinary Array format (N rows):
//   N × u64LE cumulative end-offsets  (no leading zero — CH Array serialization)
//   all UInt64 indices  (columnar, Tuple field 0)
//   all Float64 distances  (columnar, Tuple field 1)
//
// Input Array(String) CH wire format:
//   const col:     [u64 M][u32[M+1] cumul str offsets][chars+null-terminators]
//   non-const col: [N u64 per-row counts][u32[total_M+1] cumul str offsets][chars]

__attribute__((export_name("st_knn_cb")))
raw_buffer* st_knn_cb(raw_buffer* ptr, uint32_t num_rows) {
    auto cb = parse_col_binary(ptr);
    auto& col_q = cb.cols[0];  // String (WKB)
    auto& col_c = cb.cols[1];  // Array(String)
    auto& col_k = cb.cols[2];  // UInt32

    uint32_t k = *reinterpret_cast<const uint32_t*>(col_k.data);

    raw_buffer* out = clickhouse_create_buffer(28);
    out->clear();
    writeBinaryLE32(1u, *out);
    writeBinaryLE32(num_rows, *out);
    out->push_back(0x00);  // flags
    // type_tags: Array(Tuple(UInt64, Float64)) → [Array, Tuple, UInt64, Float64]
    out->push_back(4); out->push_back(0); out->push_back(0); out->push_back(0);
    out->push_back(static_cast<uint8_t>(TypeTag::Array));
    out->push_back(static_cast<uint8_t>(TypeTag::Tuple));
    out->push_back(static_cast<uint8_t>(TypeTag::UInt64));
    out->push_back(static_cast<uint8_t>(TypeTag::Float64));
    size_t data_size_pos = out->size();
    writeBinaryLE64(0ull, *out);

    try {
        ColBinaryReader rq = ColBinaryReader::from_col(col_q, num_rows);

        std::vector<std::vector<std::pair<uint64_t, double>>> row_results(num_rows);

        if (k > 0 && num_rows > 0) {
            if (col_c.is_const) {
                // [N u64 per-row counts][u32[M+1] cumul str offsets][chars+null-terminators]
                const uint8_t* p = col_c.data;
                const uint8_t* cend = col_c.data + col_c.data_size;
                uint64_t M = 0;
                std::vector<std::span<const uint8_t>> cands;
                // Const column stores exactly 1 copy: [u64 count][u32[M+1] offsets][chars]
                if (p + 8 <= cend) {
                    std::memcpy(&M, p, 8); p += 8;
                    if (M > 0 && p + (M + 1) * 4 <= cend) {
                        const uint32_t* str_offs = reinterpret_cast<const uint32_t*>(p);
                        p += (M + 1) * 4;
                        const uint8_t* chars = p;
                        cands.reserve(static_cast<size_t>(M));
                        for (uint64_t i = 0; i < M; ++i) {
                            uint32_t o0 = str_offs[i], o1 = str_offs[i + 1];
                            if (o0 > o1 || chars + o1 > cend) break;
                            cands.push_back({chars + o0, static_cast<size_t>(o1 - o0)});
                        }
                    }
                }
                CentroidKNNIndex idx(cands);
                for (uint32_t i = 0; i < num_rows; ++i) {
                    auto span_q = rq.read_blob();
                    if (span_q.empty() || idx.empty()) continue;
                    BBox qb = wkb_bbox(span_q);
                    if (qb.is_empty()) continue;
                    double qx = (qb.xmin + qb.xmax) * 0.5;
                    double qy = (qb.ymin + qb.ymax) * 0.5;
                    row_results[i] = idx.query(qx, qy, k);
                }
            } else {
                // [N u64 per-row counts][u32[total_M+1] cumul str offsets][chars+null-terminators]
                const uint8_t* p = col_c.data;
                const uint8_t* cend = col_c.data + col_c.data_size;
                std::vector<uint64_t> row_starts;
                row_starts.reserve(num_rows + 1);
                row_starts.push_back(0);
                uint64_t total_M = 0;
                for (uint32_t i = 0; i < num_rows && p + 8 <= cend; ++i) {
                    uint64_t count = 0;
                    std::memcpy(&count, p, 8);
                    p += 8;
                    total_M += count;
                    row_starts.push_back(total_M);
                }
                std::vector<std::span<const uint8_t>> all_elems;
                if (total_M > 0 && p + (total_M + 1) * 4 <= cend) {
                    const uint32_t* str_offs = reinterpret_cast<const uint32_t*>(p);
                    p += (total_M + 1) * 4;
                    const uint8_t* chars = p;
                    all_elems.reserve(static_cast<size_t>(total_M));
                    for (uint64_t i = 0; i < total_M; ++i) {
                        uint32_t o0 = str_offs[i], o1 = str_offs[i + 1];
                        if (o0 > o1 || chars + o1 > cend) break;
                        all_elems.push_back({chars + o0, static_cast<size_t>(o1 - o0)});
                    }
                }
                for (uint32_t i = 0; i < num_rows; ++i) {
                    auto span_q = rq.read_blob();
                    if (span_q.empty()) continue;
                    uint64_t elem_start = i < row_starts.size() ? row_starts[i] : 0;
                    uint64_t elem_end = (i + 1) < row_starts.size() ? row_starts[i + 1] : 0;
                    if (elem_start >= elem_end || elem_end > all_elems.size()) continue;
                    std::vector<std::span<const uint8_t>> cands(
                        all_elems.begin() + static_cast<ptrdiff_t>(elem_start),
                        all_elems.begin() + static_cast<ptrdiff_t>(elem_end));
                    auto q = read_wkb(span_q);
                    if (!q) continue;
                    row_results[i] = st_knn_brute(q.get(), cands, k);
                }
            }
        }

        // N per-row element counts (CH Array input: readBinaryLittleEndian per-row, not cumulative).
        for (uint32_t i = 0; i < num_rows; ++i)
            writeBinaryLE64(static_cast<uint64_t>(row_results[i].size()), *out);
        // All UInt64 indices (Tuple field 0, columnar).
        for (uint32_t i = 0; i < num_rows; ++i)
            for (auto& [idx, dist] : row_results[i])
                writeBinaryLE64(idx, *out);
        // All Float64 distances (Tuple field 1, columnar).
        for (uint32_t i = 0; i < num_rows; ++i) {
            for (auto& [idx, dist] : row_results[i]) {
                uint64_t bits;
                std::memcpy(&bits, &dist, 8);
                writeBinaryLE64(bits, *out);
            }
        }
    } catch (const std::exception& e) {
        clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(out));
        panic(e.what());
    }

    uint64_t data_size = static_cast<uint64_t>(out->size()) - data_size_pos - 8;
    for (int i = 0; i < 8; ++i)
        (*out)[data_size_pos + i] = static_cast<uint8_t>(data_size >> (i * 8));

    return out;
}

} // namespace ch

// ── ColumnBinary wire format macros ───────────────────────────────────────────

#define CH_UDF_CB(name)                                                      \
    __attribute__((export_name(#name "_cb")))                                \
    ch::raw_buffer * name##_cb(ch::raw_buffer * ptr,                         \
                               uint32_t num_rows) {                          \
        return ch::col_binary_impl_wrapper(#name, ptr, num_rows, ch::name##_impl); \
    }

#define CH_UDF_CB_BBOX2(name, bbox_op, early_ret)                            \
    __attribute__((export_name(#name "_cb")))                                \
    ch::raw_buffer * name##_cb(ch::raw_buffer * ptr,                         \
                               uint32_t num_rows) {                          \
        return ch::col_binary_impl_wrapper(#name, ptr, num_rows, ch::name##_impl, \
            ch::bbox_op, early_ret, ch::prep_a_##name, ch::prep_b_##name);   \
    }

#define CH_UDF_CB_BBOX2_POINT(name, bbox_op, early_ret)                      \
    __attribute__((export_name(#name "_cb")))                                \
    ch::raw_buffer * name##_cb(ch::raw_buffer * ptr,                         \
                               uint32_t num_rows) {                          \
        return ch::col_binary_impl_wrapper(#name, ptr, num_rows, ch::name##_impl, \
            ch::bbox_op, early_ret, ch::prep_a_##name, ch::prep_b_##name,    \
            nullptr, nullptr,                                                \
            ch::prep_a_pt_##name, ch::prep_b_pt_##name);                     \
    }

#define CH_UDF_CB_PRED3(name)                                                \
    __attribute__((export_name(#name "_cb")))                                \
    ch::raw_buffer * name##_cb(ch::raw_buffer * ptr,                         \
                               uint32_t num_rows) {                          \
        return ch::col_binary_impl_wrapper(#name, ptr, num_rows, ch::name##_impl, \
            nullptr, false, nullptr, nullptr,                                \
            ch::prep_a_##name, ch::prep_b_##name);                           \
    }
