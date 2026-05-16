#pragma once

// ColumnBinary wire format for ClickHouse WASM UDFs.
//
// Input wire layout (CH → WASM):
//   num_columns (u32LE) | num_rows (u32LE)
//   For each column:
//     flags (u8, bit0=IS_CONST) | data_size (u64LE) | data
//   Type is deduced from the C++ function signature — no type tags on the wire.
//
// Output wire layout (WASM → CH, always single column):
//   num_columns (u32LE) | num_rows (u32LE) | flags (u8) | data_size (u64LE) | data

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

// ── ColBinaryBuf ─────────────────────────────────────────────────────────────

struct ColBinaryInfo {
    bool           is_const;
    uint64_t       data_size;
    const uint8_t* data;

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

static inline bool readBinaryLE32(const uint8_t*& p, const uint8_t* end, uint32_t& out) {
    if (p + 4 > end) return false;
    out = 0;
    for (int i = 0; i < 4; ++i)
        out |= static_cast<uint32_t>(p[i]) << (i * 8);
    p += 4;
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
// No nullable support — wire type always matches C++ signature.

struct ColBinaryReader {
    bool           is_const;
    uint32_t       stored_rows;   // 1 if const, else num_rows
    const uint8_t* data_start;    // data section start (offsets for COL_BYTES, raw for fixed-width)
    const uint8_t* data_end;      // data_start + data_size
    mutable const uint8_t* p;     // cursor for sequential reads (COL_BYTES non-const)

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

    template <typename T>
    T read_fixed(uint32_t row) const noexcept {
        T v{};
        size_t elem_size = sizeof(T);
        const uint8_t* elem_ptr = data_start + (is_const ? 0 : row) * elem_size;
        std::memcpy(&v, elem_ptr, elem_size);
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
            if (u32_dat + last_off <= data_end) {
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

    // Debug: print hex dump of a span (for WASM troubleshooting).
    void debug_dump(const char* label, const uint8_t* start, size_t len) const {
        char buf[256];
        int off = 0;
        off += snprintf(buf + off, sizeof(buf) - off, "DBG|%s: %zu bytes", label, len);
        if (off >= (int)sizeof(buf) - 1) return;
        for (size_t i = 0; i < len && i < 64; ++i) {
            off += snprintf(buf + off, sizeof(buf) - off, " %02x", start[i]);
            if (off >= (int)sizeof(buf) - 1) break;
        }
        if (len > 64) off += snprintf(buf + off, sizeof(buf) - off, " ...");
        ch::log(ch::log_level::debug, buf);
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

template <typename T>
T unpack_arg(ColBinaryReader&);

template <>
inline double unpack_arg<double>(ColBinaryReader& r) {
    auto span = r.read_fixed_bytes(sizeof(double));
    double v;
    std::memcpy(&v, span.data(), sizeof(v));
    return v;
}

template <>
inline int32_t unpack_arg<int32_t>(ColBinaryReader& r) {
    auto span = r.read_fixed_bytes(sizeof(int32_t));
    int32_t v;
    std::memcpy(&v, span.data(), sizeof(v));
    return v;
}

template <>
inline uint32_t unpack_arg<uint32_t>(ColBinaryReader& r) {
    auto span = r.read_fixed_bytes(sizeof(uint32_t));
    uint32_t v;
    std::memcpy(&v, span.data(), sizeof(v));
    return v;
}

template <>
inline std::string_view unpack_arg<std::string_view>(ColBinaryReader& r) {
    auto span = r.read_blob();
    return {reinterpret_cast<const char*>(span.data()), span.size()};
}

template <>
inline std::span<const uint8_t> unpack_arg<std::span<const uint8_t>>(ColBinaryReader& r) {
    return r.read_blob();
}

template <>
inline std::unique_ptr<geos::geom::Geometry> unpack_arg<std::unique_ptr<geos::geom::Geometry>>(ColBinaryReader& r) {
    auto span = r.read_blob();
    if (span.empty()) return nullptr;
    if (ch::is_wkb(span)) return ch::read_wkb(span);
    return ch::read_wkt(span);
}

template <>
inline geos::geom::Geometry* unpack_arg<geos::geom::Geometry*>(ColBinaryReader& r) {
    auto span = r.read_blob();
    if (span.empty()) return nullptr;
    try { return ch::read_wkb(span).release(); }
    catch (...) { return ch::read_wkt(span).release(); }
}

template <>
inline geos::geom::Geometry const* unpack_arg<geos::geom::Geometry const*>(ColBinaryReader& r) {
    return unpack_arg<geos::geom::Geometry*>(r);
}

template <>
inline std::vector<std::unique_ptr<geos::geom::Geometry>>
unpack_arg<std::vector<std::unique_ptr<geos::geom::Geometry>>>(ColBinaryReader& r) {
    std::vector<uint64_t> offsets;
    std::vector<std::unique_ptr<Geometry>> result;
    char errbuf[128];
    int off = 0;

    if (!r.data_start || !r.data_end) ch::panic("ARRAY: null data pointers");
    size_t data_len = r.data_end - r.data_start;
    off += snprintf(errbuf, sizeof(errbuf), "ARRAY: const=%d rows=%d data_len=%zu",
                    r.is_const, r.stored_rows, data_len);
    ch::log(ch::log_level::debug, errbuf);

    uint64_t offsets_buf[4096];
    std::unique_ptr<Geometry> result_buf[4096];
    uint32_t num_results = 0;
    uint32_t num_offsets = 0;

    auto add_geom = [&](std::unique_ptr<Geometry> g) {
        if (num_results < 4096) result_buf[num_results++] = std::move(g);
    };

    if (r.is_const) {
        if (!r.blob_cached) {
            r.cached_blob = r.parse_blob_from(r.data_start, r.data_end);
            r.blob_cached = true;
        }
        const uint8_t* p = r.cached_blob.data();
        const uint8_t* end = p + r.cached_blob.size();
        if (p + 16 > end) return {};
        uint64_t o0, o1;
        std::memcpy(&o0, p, 8); p += 8;
        std::memcpy(&o1, p, 8); p += 8;
        uint64_t M = o1 - o0;
        off += snprintf(errbuf + off, sizeof(errbuf) - off, " CONST: o0=%" PRIu64 " o1=%" PRIu64 " M=%" PRIu64, o0, o1, M);
        for (uint64_t i = 0; i < M && num_results < 4096; ++i) {
            uint64_t data_len = 0;
            int shift = 0;
            const uint8_t* ep = p;
            while (ep < end) {
                uint8_t byte = *ep++;
                data_len |= static_cast<uint64_t>(byte & 0x7F) << shift;
                if ((byte & 0x80) == 0) break;
                shift += 7;
            }
            if (ep + data_len > end) break;
            if (data_len == 0) {
                add_geom(nullptr);
            } else {
                if (ch::is_wkb({ep, static_cast<size_t>(data_len)}))
                    add_geom(ch::read_wkb({ep, static_cast<size_t>(data_len)}));
                else
                    add_geom(ch::read_wkt({ep, static_cast<size_t>(data_len)}));
            }
            p = ep + data_len;
        }
    } else {
        const uint8_t* end = r.data_end;
        if (!r.offsets_cached) {
            const uint8_t* p = r.data_start;
            uint32_t expected_offsets = r.stored_rows + 1;
            if (expected_offsets > 4096) expected_offsets = 4096;
            if (expected_offsets < 2) expected_offsets = 2;
            for (uint32_t i = 0; i < expected_offsets && p + 8 <= end; ++i) {
                uint64_t o = 0;
                std::memcpy(&o, p, 8);
                r.cached_offsets.push_back(o);
                p += 8;
            }
            r.p = p;
            r.offsets_cached = true;
        }
        if (r.cached_offsets.size() < 2) return {};
        uint32_t row = r.current_row;
        if (row + 1 >= static_cast<uint32_t>(r.cached_offsets.size())) {
            off += snprintf(errbuf + off, sizeof(errbuf) - off, " ERROR: row+1 >= num_offsets");
            ch::panic(errbuf);
        }
        uint64_t elem_count = r.cached_offsets[row + 1] - r.cached_offsets[row];
        off += snprintf(errbuf + off, sizeof(errbuf) - off, " elem_count=%" PRIu64, elem_count);

        const uint8_t* p = r.p;
        for (uint64_t i = 0; i < elem_count && num_results < 4096; ++i) {
            uint64_t data_len = 0;
            int shift = 0;
            while (p < end) {
                uint8_t byte = *p++;
                data_len |= static_cast<uint64_t>(byte & 0x7F) << shift;
                if ((byte & 0x80) == 0) break;
                shift += 7;
            }
            if (p + data_len > end) break;
            if (data_len == 0) {
                add_geom(nullptr);
            } else {
                if (ch::is_wkb({p, static_cast<size_t>(data_len)}))
                    add_geom(ch::read_wkb({p, static_cast<size_t>(data_len)}));
                else
                    add_geom(ch::read_wkt({p, static_cast<size_t>(data_len)}));
            }
            p += data_len;
        }
        r.p = p;
        ++r.current_row;
    }
    ch::log(ch::log_level::debug, errbuf);
    for (uint32_t i = 0; i < num_results; ++i)
        result.push_back(std::move(result_buf[i]));
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
            auto span_a = unpack_arg<std::span<const uint8_t>>(readers[0]);
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
            auto span_b = unpack_arg<std::span<const uint8_t>>(readers[1]);
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
                auto span_a = unpack_arg<std::span<const uint8_t>>(readers[0]);
                BBox bbox_a = wkb_bbox(span_a);
                auto geom_a = read_wkb(span_a);
                auto pa = PGF::prepare(geom_a.get());
                for (uint32_t i = 0; i < n; ++i) {
                    auto span_b = readers[1].read_blob();
                    double dist = unpack_arg<double>(readers[2]);
                    if (!bbox_a.intersects(wkb_bbox(span_b).expanded(dist))) {
                        pack_result(out, false); continue;
                    }
                    pack_result(out, prep_a_dist(pa.get(), read_wkb(span_b).get(), dist));
                }
                return;
            }
            if (readers[1].is_const && prep_b_dist && n > 0) {
                auto span_b = unpack_arg<std::span<const uint8_t>>(readers[1]);
                BBox bbox_b = wkb_bbox(span_b);
                auto geom_b = read_wkb(span_b);
                auto pb = PGF::prepare(geom_b.get());
                for (uint32_t i = 0; i < n; ++i) {
                    auto span_a = readers[0].read_blob();
                    double dist = unpack_arg<double>(readers[2]);
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
    char dbg[64];
    int o = snprintf(dbg, sizeof(dbg), "CB_WRAPPER: %s rows=%d", func_name, num_rows);
    ch::log(ch::log_level::debug, dbg);
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
            return impl(unpack_arg<std::decay_t<Args>>(readers[I])...);
        }(std::make_index_sequence<nargs>{});
    };

    raw_buffer* out = nullptr;
    try {
        out = clickhouse_create_buffer(28);
        out->clear();

        // Output header: num_cols(u32LE) + num_rows(u32LE) + flags(u8) + data_size(u64LE)
        writeBinaryLE32(1u, *out);
        writeBinaryLE32(static_cast<uint32_t>(n), *out);
        out->push_back(0x00);  // flags: 0 (non-const output)

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
// Output format: raw ColumnBinary Array data (NOT COL_COMPLEX):
//   [offset_0: u64LE][offset_1: u64LE]...[offset_n-1: u64LE]
//   [idx_0: u64LE][idx_1: u64LE]...[idx_{total-1}: u64LE]
//   [dist_0: u64LE][dist_1: u64LE]...[dist_{total-1}: u64LE]
//
// Offsets are cumulative element counts (matching readArrayColumnData).
// Tuple elements are written in columnar order: all UInt64 indices first,
// then all Float64 distances (matching deserializeBinaryBulk for Tuple).

__attribute__((export_name("st_knn_cb")))
raw_buffer* st_knn_cb(raw_buffer* ptr, uint32_t num_rows) {
    auto cb = parse_col_binary(ptr);
    auto& col_q = cb.cols[0];  // String (WKB)
    auto& col_c = cb.cols[1];  // Array(String) — COL_COMPLEX
    auto& col_k = cb.cols[2];  // UInt32

    uint32_t k = col_k.is_const
        ? *reinterpret_cast<const uint32_t*>(col_k.data)
        : *reinterpret_cast<const uint32_t*>(col_k.data);

    if (k == 0 || num_rows == 0) {
        raw_buffer* out = clickhouse_create_buffer(28);
        writeBinaryLE32(1u, *out);
        writeBinaryLE32(num_rows, *out);
        out->push_back(0x00);
        uint64_t zero = 0;
        writeBinaryLE64(zero, *out);
        return out;
    }

    raw_buffer* out = clickhouse_create_buffer(28);
    out->clear();
    writeBinaryLE32(1u, *out);
    writeBinaryLE32(num_rows, *out);
    out->push_back(0x00);
    size_t data_size_pos = out->size();
    uint64_t zero = 0;
    writeBinaryLE64(zero, *out);

    try {
        ColBinaryReader rq = ColBinaryReader::from_col(col_q, num_rows);

        // Collect results first — ColumnBinary Array format requires
        // offsets first, then all tuple elements in columnar order.
        std::vector<std::pair<uint64_t, double>> all_results;
        all_results.reserve(static_cast<size_t>(num_rows) * k);

        if (col_c.is_const) {
            const uint32_t* outer_offs = reinterpret_cast<const uint32_t*>(col_c.data);
            const uint32_t M = outer_offs[cb.num_rows];
            const uint32_t* inner_offs = reinterpret_cast<const uint32_t*>(col_c.data + (cb.num_rows + 1u) * 4u);
            const uint8_t* chars = col_c.data + (cb.num_rows + 1u) * 4u + (M + 1u) * 4u;

            std::vector<std::span<const uint8_t>> cands;
            cands.reserve(M);
            for (uint32_t i = 0; i < M; ++i) {
                uint32_t s = inner_offs[i], e = inner_offs[i + 1];
                uint32_t len = (e > s + 1u) ? e - s - 1u : 0u;
                cands.push_back({chars + s, len});
            }

            for (uint32_t i = 0; i < num_rows; ++i) {
                auto span_q = rq.read_blob();
                if (span_q.empty()) {
                    for (uint32_t j = 0; j < k; ++j)
                        all_results.emplace_back(0, 0.0);
                } else {
                    BBox pt = wkb_bbox(span_q);
                    if (pt.is_empty()) {
                        for (uint32_t j = 0; j < k; ++j)
                            all_results.emplace_back(0, 0.0);
                    } else {
                        auto result = st_knn_centroid(span_q, cands, k);
                        for (auto& r : result)
                            all_results.push_back(r);
                    }
                }
            }
        } else {
            const uint32_t* outer_offs = reinterpret_cast<const uint32_t*>(col_c.data);
            const uint32_t M = outer_offs[num_rows];
            const uint32_t* inner_offs = reinterpret_cast<const uint32_t*>(col_c.data + (num_rows + 1u) * 4u);
            const uint8_t* chars = col_c.data + (num_rows + 1u) * 4u + (M + 1u) * 4u;

            for (uint32_t i = 0; i < num_rows; ++i) {
                auto span_q = rq.read_blob();
                if (span_q.empty()) {
                    for (uint32_t j = 0; j < k; ++j)
                        all_results.emplace_back(0, 0.0);
                } else {
                    auto q = read_wkb(span_q);
                    uint32_t elem_start = outer_offs[i], elem_end = outer_offs[i + 1];
                    std::vector<std::span<const uint8_t>> cands;
                    cands.reserve(elem_end - elem_start);
                    for (uint32_t j = elem_start; j < elem_end; ++j) {
                        uint32_t s = inner_offs[j], e = inner_offs[j + 1];
                        uint32_t len = (e > s + 1u) ? e - s - 1u : 0u;
                        cands.push_back({chars + s, len});
                    }
                    auto result = st_knn_brute(q.get(), cands, k);
                    for (auto& r : result)
                        all_results.push_back(r);
                }
            }
        }

        // Write ColumnBinary Array format:
        // 1. Cumulative offsets (u64LE each)
        // 2. All UInt64 indices (u64LE each)
        // 3. All Float64 distances (u64LE each)
        uint64_t cumulative = 0;
        for (uint32_t i = 0; i < num_rows; ++i) {
            cumulative += k;
            writeBinaryLE64(cumulative, *out);
        }
        for (size_t i = 0; i < all_results.size(); ++i) {
            writeBinaryLE64(all_results[i].first, *out);
        }
        for (size_t i = 0; i < all_results.size(); ++i) {
            writeBinaryLE64(*reinterpret_cast<uint64_t*>(&all_results[i].second), *out);
        }
    } catch (const std::exception& e) {
        clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(out));
        panic(e.what());
    }

    uint64_t data_size = out->size() - data_size_pos - 8;
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
