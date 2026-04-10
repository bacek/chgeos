#pragma once

// COLUMNAR_V1 wire format for ClickHouse WASM UDFs — per-column persistent buffers.
//
// Two-buffer design: a tiny ephemeral call buffer + one persistent buffer per column.
// CH allocates per-column buffers once and reuses them when column data is unchanged.
// Constant geometry columns therefore cross the WASM boundary only on first use.
//
// ┌──────────────────────────────────────────────────────────────────────────┐
// │ Per-column buffer: ColBufHeader (32 bytes) followed by column data       │
// │   type           : u32  — ColType | COL_IS_CONST flag                   │
// │   num_rows       : u32  — stored rows (1 if COL_IS_CONST)               │
// │   null_offset    : u32  — byte offset to u8[num_rows]; 0 = no nulls     │
// │   offsets_offset : u32  — byte offset to u32[num_rows+1]; 0 = fixed     │
// │   data_offset    : u32  — byte offset to raw column data                │
// │   data_size      : u32  — bytes in the data block                       │
// │   version        : u64  — monotonic; same column data → same version    │
// │ Column data at offsets described above (same layout as before)           │
// ├──────────────────────────────────────────────────────────────────────────┤
// │ Call buffer (ephemeral, one per invocation)                              │
// │   num_rows       : u32                                                   │
// │   num_cols       : u32                                                   │
// │   col_handle[i]  : u64  — address of per-column raw_buffer (WASM ptr)   │
// └──────────────────────────────────────────────────────────────────────────┘
//
// Output buffer: unchanged format — BufHeader(8B) + ColDescriptor(20B) + data.
//
// String encoding (COL_BYTES / COL_NULL_BYTES): offsets[0..num_rows] start-based.
// Data stored with explicit null terminators; get_bytes() uses end-start-1.
//
// SQL: ABI COLUMNAR_V1

#include <algorithm>
#include <array>
#include <cstdint>
#include <cstring>
#include <limits>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>

#include <geos/geom/prep/PreparedGeometryFactory.h>

#include "clickhouse.hpp"
#include "col_prep_op.hpp"
#include "geom/wkb.hpp"
#include "geom/wkb_envelope.hpp"
#include "mem.hpp"

namespace ch {

// ── Type tags ────────────────────────────────────────────────────────────────

enum ColType : uint32_t {
    COL_BYTES       = 0,  // String:           offsets[row_count+1] + data
    COL_NULL_BYTES  = 1,  // Nullable(String): null_map + offsets + data
    COL_FIXED8      = 2,  // UInt8/Int8:       u8[row_count]
    COL_NULL_FIXED8 = 3,
    COL_FIXED32     = 4,  // UInt32/Int32/Float32
    COL_NULL_FIXED32= 5,
    COL_FIXED64     = 6,  // UInt64/Int64/Float64
    COL_NULL_FIXED64= 7,

    COL_IS_CONST    = 0x80u, // flag: 1 stored row, broadcast to num_rows
};

// ── Wire structs ──────────────────────────────────────────────────────────────

// Per-column persistent buffer header (24 bytes).
struct ColBufHeader {
    uint32_t type;            // ColType | COL_IS_CONST
    uint32_t num_rows;        // stored rows (1 if COL_IS_CONST)
    uint32_t null_offset;     // byte offset from buffer start; 0 = no nulls
    uint32_t offsets_offset;  // byte offset to u32[num_rows+1]; 0 = fixed-width
    uint32_t data_offset;     // byte offset to raw data
    uint32_t data_size;       // bytes of data
};
static_assert(sizeof(ColBufHeader) == 24);

// Output format descriptor (unchanged from original COLUMNAR_V1).
struct ColDescriptor {
    uint32_t type;
    uint32_t null_offset;
    uint32_t offsets_offset;
    uint32_t data_offset;
    uint32_t data_size;
};
static_assert(sizeof(ColDescriptor) == 20);

static constexpr uint32_t COL_BUF_HDR_BYTES = 24;  // sizeof ColBufHeader
static constexpr uint32_t CALL_HDR_BYTES    = 8;   // call buffer header: num_rows + num_cols
static constexpr uint32_t HEADER_BYTES      = 8;   // output BufHeader
static constexpr uint32_t COL_DESC_BYTES    = 20;  // output ColDescriptor

// ── Input column accessor ─────────────────────────────────────────────────────

struct ColView {
    ColType         base_type;
    bool            is_const;
    uint32_t        row_count;    // stored rows (1 if const)
    const uint8_t*  null_map;     // nullable: null_map[i]!=0 → NULL; nullptr = non-nullable
    const uint32_t* offsets;      // start-based; nullptr for fixed-width
    const uint8_t*  data;
    uintptr_t       col_handle;   // address of the per-column raw_buffer

    bool is_null(uint32_t row) const noexcept {
        if (!null_map) return false;
        return null_map[is_const ? 0u : row] != 0;
    }

    // For COL_BYTES/COL_NULL_BYTES — excludes the trailing null terminator.
    std::span<const uint8_t> get_bytes(uint32_t row) const noexcept {
        uint32_t idx   = is_const ? 0u : row;
        uint32_t start = offsets[idx];
        uint32_t end   = offsets[idx + 1];
        uint32_t len   = (end > start + 1) ? end - start - 1 : 0u;
        return {data + start, len};
    }

    template <typename T>
    T get_fixed(uint32_t row) const noexcept {
        uint32_t idx = is_const ? 0u : row;
        T v;
        std::memcpy(&v, data + idx * sizeof(T), sizeof(T));
        return v;
    }

    // True when every row in the batch carries identical bytes — covers the
    // cross-join pattern where CH repeats the same zone WKB N times without
    // setting COL_IS_CONST (which is only set for compile-time literals).
    //
    // Two-step check:
    //  1. O(1): all rows have equal size  →  offsets[N] == N * offsets[1]
    //  2. O(size): first and last rows have equal bytes  →  memcmp
    // For real geometry data the size check alone rejects nearly all mixes;
    // memcmp is a final guard.
    bool is_effectively_const_bytes() const noexcept {
        if (is_const) return true;
        if (!offsets || row_count < 2) return false;
        uint32_t elem_stride = offsets[1];           // bytes from row 0 start to row 1 start
        if (elem_stride == 0) return false;           // empty column
        if (offsets[row_count] != elem_stride * row_count) return false;  // unequal sizes
        // Compare first row bytes with last row bytes (excluding null terminator).
        uint32_t wkb_len = elem_stride > 0 ? elem_stride - 1 : 0;
        uint32_t last_start = offsets[row_count - 1];
        return std::memcmp(data, data + last_start, wkb_len) == 0;
    }
};

struct ColumnarBuf {
    uint32_t         num_rows;
    uint32_t         num_cols;
    const uint64_t*  handles;  // col_handle[i]: address of per-column raw_buffer

    ColView col(uint32_t i) const {
        // Dereference handle → per-column raw_buffer → ColBufHeader + data.
        // In WASM32, uintptr_t truncates u64 to 32 bits (the WASM linear-memory address).
        // In native test builds, all 64 bits hold the native pointer.
        uintptr_t handle = static_cast<uintptr_t>(handles[i]);
        const raw_buffer* cb = reinterpret_cast<const raw_buffer*>(handle);
        const uint8_t* base = cb->data();
        ColBufHeader hdr;
        std::memcpy(&hdr, base, sizeof(hdr));

        ColView v;
        v.is_const    = (hdr.type & COL_IS_CONST) != 0;
        v.base_type   = static_cast<ColType>(hdr.type & ~COL_IS_CONST);
        v.row_count   = hdr.num_rows;
        v.null_map    = hdr.null_offset    ? base + hdr.null_offset    : nullptr;
        v.offsets     = hdr.offsets_offset
                        ? reinterpret_cast<const uint32_t*>(base + hdr.offsets_offset)
                        : nullptr;
        v.data        = base + hdr.data_offset;
        v.col_handle  = handle;
        return v;
    }
};

inline ColumnarBuf parse_columnar(const raw_buffer* buf) {
    // Call buffer: [num_rows:u32 | num_cols:u32 | col_handle[0]:u64 | ...]
    const uint8_t* p = buf->data();
    ColumnarBuf cb;
    std::memcpy(&cb.num_rows, p,     4);
    std::memcpy(&cb.num_cols, p + 4, 4);
    cb.handles = reinterpret_cast<const uint64_t*>(p + CALL_HDR_BYTES);
    return cb;
}

// ── Output writers ────────────────────────────────────────────────────────────

// Write a fixed-width single-column output (e.g. UInt8 predicates, Float64 scalars).
// Caller fills out->data() + HEADER_BYTES + COL_DESC_BYTES with num_rows * sizeof(T).
template <typename T>
inline void col_write_fixed_header(raw_buffer* out, uint32_t num_rows, uint32_t col_type) {
    out->resize(HEADER_BYTES + COL_DESC_BYTES + num_rows * static_cast<uint32_t>(sizeof(T)));
    uint8_t* p = out->data();

    std::memcpy(p, &num_rows, 4);
    const uint32_t one = 1;
    std::memcpy(p + 4, &one, 4);

    ColDescriptor d{};
    d.type         = col_type;
    d.data_offset  = HEADER_BYTES + COL_DESC_BYTES;
    d.data_size    = num_rows * static_cast<uint32_t>(sizeof(T));
    std::memcpy(p + HEADER_BYTES, &d, sizeof(d));
}

// Streaming writer for a single variable-length (bytes) column output.
// Layout: [BufHeader][ColDescriptor][null_map: u8[N]][offsets: u32[N+1]][data...]
struct ColBytesWriter {
    raw_buffer* out;
    uint32_t    num_rows;
    uint32_t    rows_written = 0;
    uint32_t    null_base;
    uint32_t    offs_base;
    uint32_t    data_base;
    bool        nullable;

    explicit ColBytesWriter(raw_buffer* buf, uint32_t n, bool is_nullable = true)
        : out(buf), num_rows(n), nullable(is_nullable)
    {
        null_base = HEADER_BYTES + COL_DESC_BYTES;
        uint32_t after_null = null_base + (nullable ? n : 0u);
        offs_base = (after_null + 3u) & ~3u;   // align to 4
        data_base = offs_base + (n + 1u) * 4u;

        out->resize(data_base);
        uint8_t* p = out->data();

        std::memcpy(p, &n, 4);
        const uint32_t one = 1;
        std::memcpy(p + 4, &one, 4);

        ColDescriptor d{};
        d.type           = is_nullable ? COL_NULL_BYTES : COL_BYTES;
        d.null_offset    = is_nullable ? null_base : 0u;
        d.offsets_offset = offs_base;
        d.data_offset    = data_base;
        d.data_size      = 0;
        std::memcpy(p + HEADER_BYTES, &d, sizeof(d));

        const uint32_t zero = 0;
        std::memcpy(p + offs_base, &zero, 4);  // offsets[0] = 0
    }

    void push_null() {
        uint32_t i = rows_written++;
        // Read current end offset before any append that might realloc.
        uint32_t prev;
        std::memcpy(&prev, out->data() + offs_base + i * 4u, 4);
        // Append one null-terminator byte (empty string in CH ColumnString layout).
        out->push_back(0);
        // Re-fetch pointer after potential realloc.
        uint8_t* p = out->data();
        if (nullable) p[null_base + i] = 1;
        uint32_t next = prev + 1u;
        std::memcpy(p + offs_base + (i + 1u) * 4u, &next, 4);
    }

    void push_bytes(std::span<const uint8_t> bytes) {
        uint32_t i = rows_written++;
        uint32_t len = static_cast<uint32_t>(bytes.size());
        // Compute next offset before any realloc.
        uint32_t prev;
        std::memcpy(&prev, out->data() + offs_base + i * 4u, 4);
        uint32_t next = prev + len + 1u;   // +1 for null terminator
        // Append data + null terminator.
        out->append(bytes.data(), len);
        out->push_back(0);
        // Re-fetch pointer after potential realloc.
        uint8_t* p = out->data();
        if (nullable) p[null_base + i] = 0;
        std::memcpy(p + offs_base + (i + 1u) * 4u, &next, 4);
    }

    void push_geom(std::unique_ptr<geos::geom::Geometry> g) {
        if (!g) { push_null(); return; }
        auto wkb = write_ewkb(g);
        push_bytes({wkb.data(), wkb.size()});
    }

    void finish() {
        uint8_t* p = out->data();
        ColDescriptor d;
        std::memcpy(&d, p + HEADER_BYTES, sizeof(d));
        d.data_size = out->size() - data_base;
        std::memcpy(p + HEADER_BYTES, &d, sizeof(d));
    }
};

// ── Input column accessor by type ─────────────────────────────────────────────

template <typename T>
T col_get_arg(const ColView& col, uint32_t row) {
    if constexpr (std::is_same_v<T, std::span<const uint8_t>>) {
        return col.get_bytes(row);
    } else if constexpr (std::is_same_v<T, double>) {
        return col.get_fixed<double>(row);
    } else if constexpr (std::is_same_v<T, int32_t>) {
        return col.get_fixed<int32_t>(row);
    } else if constexpr (std::is_same_v<T, std::string_view>) {
        auto s = col.get_bytes(row);
        return {reinterpret_cast<const char*>(s.data()), s.size()};
    } else if constexpr (std::is_same_v<T, std::unique_ptr<geos::geom::Geometry>>) {
        return read_wkb(col.get_bytes(row));
    }
}

// ── PreparedGeometry cache ─────────────────────────────────────────────────────
// One-entry cache per const-column slot.  Keyed by col_handle (raw_buffer address).
// clickhouse_destroy_buffer() records freed addresses in a ring; valid_for() checks
// that ring so that if the allocator reuses an address for a different geometry the
// stale entry is invalidated rather than producing incorrect results.

struct ColPrepCache {
    uintptr_t col_handle = 0;
    BBox      bbox       = {};
    std::unique_ptr<geos::geom::Geometry>                     geom;
    std::unique_ptr<const geos::geom::prep::PreparedGeometry> prep;

    // A hit requires: same buffer address AND the buffer hasn't been destroyed
    // since we cached it.  clickhouse_destroy_buffer() records freed addresses
    // in a ring, so a reused address correctly triggers a rebuild.
    bool valid_for(const ColView& cv) const noexcept {
        return prep != nullptr &&
               col_handle == cv.col_handle &&
               !is_recently_destroyed(cv.col_handle);
    }

    void rebuild(const ColView& cv) {
        using PGF = geos::geom::prep::PreparedGeometryFactory;
        auto span  = cv.get_bytes(0);
        col_handle = cv.col_handle;
        bbox       = wkb_bbox(span);
        geom       = read_wkb(span);
        prep       = PGF::prepare(geom.get());
    }
};

// ── Generic columnar wrapper ───────────────────────────────────────────────────
// Mirrors rowbinary_impl_wrapper: takes a typed function pointer, deduces
// argument and return types, dispatches column reads and output format.
//
// Optional parameters (for binary geometry predicates only):
//   bbox_op  / early_ret — bbox short-circuit applied before WKB parsing
//   prep_a   — PreparedGeometry callback when col(0) is effectively const
//   prep_b   — PreparedGeometry callback when col(1) is effectively const
// Each const-column slot has a static one-entry ColPrepCache; the cache is
// invalidated when the buffer address changes or the old address was destroyed.

template <typename Ret, typename... Args>
raw_buffer* columnar_impl_wrapper(raw_buffer* ptr, uint32_t,
                                  Ret (*impl)(Args...),
                                  BboxOp        bbox_op      = nullptr,
                                  bool          early_ret    = false,
                                  ColPrepOp     prep_a       = nullptr,
                                  ColPrepOp     prep_b       = nullptr,
                                  ColPrepDistOp prep_a_dist  = nullptr,
                                  ColPrepDistOp prep_b_dist  = nullptr) {
    auto cb = parse_columnar(ptr);
    uint32_t n = cb.num_rows;
    constexpr size_t nargs = sizeof...(Args);

    std::array<ColView, nargs> cols;
    for (size_t j = 0; j < nargs; ++j) cols[j] = cb.col(static_cast<uint32_t>(j));

    // Call impl with args read from each column for a given row.
    auto invoke = [&](uint32_t row) {
        return [&]<size_t... I>(std::index_sequence<I...>) {
            return impl(col_get_arg<std::decay_t<Args>>(cols[I], row)...);
        }(std::make_index_sequence<nargs>{});
    };

    // Check whether any column is null for a given row.
    auto any_null = [&](uint32_t row) {
        bool null = false;
        for (size_t j = 0; j < nargs; ++j) null |= cols[j].is_null(row);
        return null;
    };

    raw_buffer* out = nullptr;
    try {
        // ── bool output (predicates) ──────────────────────────────────────────
        if constexpr (std::is_same_v<Ret, bool>) {
            out = clickhouse_create_buffer(HEADER_BYTES + COL_DESC_BYTES + n);
            col_write_fixed_header<uint8_t>(out, n, COL_FIXED8);
            uint8_t* res = out->data() + HEADER_BYTES + COL_DESC_BYTES;

            if constexpr (nargs >= 2) {
                // These statics are safe under parallel ClickHouse queries because
                // each WasmCompartment is an independent module instantiation with
                // its own linear memory.  Static C++ locals compiled to WASM live
                // inside that linear memory, so cache_a/cache_b are per-compartment,
                // not globally shared.  No locking needed.
                static ColPrepCache cache_a;
                static ColPrepCache cache_b;

                // A-const fast path: col(0) is constant (or effectively constant).
                // cache_a.valid_for() is O(1); is_effectively_const_bytes() is called
                // only on cache miss, amortising the O(size) check across batches.
                if (prep_a) {
                    bool eff_a = cache_a.valid_for(cols[0]) ||
                                 cols[0].is_effectively_const_bytes();
                    if (eff_a) {
                        if (cols[0].is_null(0)) { std::fill(res, res + n, 0u); return out; }
                        if (!cache_a.valid_for(cols[0])) cache_a.rebuild(cols[0]);
                        for (uint32_t i = 0; i < n; ++i) {
                            if (cols[1].is_null(i)) { res[i] = 0u; continue; }
                            auto span_b = cols[1].get_bytes(i);
                            if (bbox_op && !bbox_op(cache_a.bbox, wkb_bbox(span_b))) {
                                res[i] = early_ret ? 1u : 0u; continue;
                            }
                            res[i] = prep_a(cache_a.prep.get(), read_wkb(span_b).get()) ? 1u : 0u;
                        }
                        return out;
                    }
                }

                // B-const fast path: col(1) is constant (or effectively constant).
                if (prep_b) {
                    bool eff_b = cache_b.valid_for(cols[1]) ||
                                 cols[1].is_effectively_const_bytes();
                    if (eff_b) {
                        if (cols[1].is_null(0)) { std::fill(res, res + n, 0u); return out; }
                        if (!cache_b.valid_for(cols[1])) cache_b.rebuild(cols[1]);
                        for (uint32_t i = 0; i < n; ++i) {
                            if (cols[0].is_null(i)) { res[i] = 0u; continue; }
                            auto span_a = cols[0].get_bytes(i);
                            if (bbox_op && !bbox_op(wkb_bbox(span_a), cache_b.bbox)) {
                                res[i] = early_ret ? 1u : 0u; continue;
                            }
                            res[i] = prep_b(cache_b.prep.get(), read_wkb(span_a).get()) ? 1u : 0u;
                        }
                        return out;
                    }
                }
            }

            // 3-arg distance predicate: (geom, geom, double) with PreparedGeometry.
            // col(0)=geom_a, col(1)=geom_b, col(2)=distance.
            if constexpr (nargs >= 3) {
                // Same per-compartment isolation as cache_a/cache_b above.
                static ColPrepCache cache_a_dist;
                static ColPrepCache cache_b_dist;

                // A-const dist path
                if (prep_a_dist) {
                    bool eff_a = cache_a_dist.valid_for(cols[0]) ||
                                 cols[0].is_effectively_const_bytes();
                    if (eff_a) {
                        if (cols[0].is_null(0)) { std::fill(res, res + n, 0u); return out; }
                        if (!cache_a_dist.valid_for(cols[0])) cache_a_dist.rebuild(cols[0]);
                        for (uint32_t i = 0; i < n; ++i) {
                            if (cols[1].is_null(i)) { res[i] = 0u; continue; }
                            auto   span_b = cols[1].get_bytes(i);
                            double dist   = col_get_arg<double>(cols[2], i);
                            if (!cache_a_dist.bbox.intersects(wkb_bbox(span_b).expanded(dist))) {
                                res[i] = 0u; continue;
                            }
                            res[i] = prep_a_dist(cache_a_dist.prep.get(), read_wkb(span_b).get(), dist) ? 1u : 0u;
                        }
                        return out;
                    }
                }

                // B-const dist path
                if (prep_b_dist) {
                    bool eff_b = cache_b_dist.valid_for(cols[1]) ||
                                 cols[1].is_effectively_const_bytes();
                    if (eff_b) {
                        if (cols[1].is_null(0)) { std::fill(res, res + n, 0u); return out; }
                        if (!cache_b_dist.valid_for(cols[1])) cache_b_dist.rebuild(cols[1]);
                        for (uint32_t i = 0; i < n; ++i) {
                            if (cols[0].is_null(i)) { res[i] = 0u; continue; }
                            auto   span_a = cols[0].get_bytes(i);
                            double dist   = col_get_arg<double>(cols[2], i);
                            if (!wkb_bbox(span_a).intersects(cache_b_dist.bbox.expanded(dist))) {
                                res[i] = 0u; continue;
                            }
                            res[i] = prep_b_dist(cache_b_dist.prep.get(), read_wkb(span_a).get(), dist) ? 1u : 0u;
                        }
                        return out;
                    }
                }
            }

            // Baseline
            for (uint32_t i = 0; i < n; ++i) {
                if (any_null(i)) { res[i] = 0u; continue; }
                if constexpr (nargs >= 2) {
                    if (bbox_op && !bbox_op(wkb_bbox(cols[0].get_bytes(i)),
                                            wkb_bbox(cols[1].get_bytes(i)))) {
                        res[i] = early_ret ? 1u : 0u; continue;
                    }
                }
                res[i] = invoke(i) ? 1u : 0u;
            }
            return out;

        // ── double output ─────────────────────────────────────────────────────
        } else if constexpr (std::is_same_v<Ret, double>) {
            out = clickhouse_create_buffer(HEADER_BYTES + COL_DESC_BYTES + n * 8u);
            col_write_fixed_header<double>(out, n, COL_FIXED64);
            double* res = reinterpret_cast<double*>(out->data() + HEADER_BYTES + COL_DESC_BYTES);
            for (uint32_t i = 0; i < n; ++i)
                res[i] = any_null(i) ? std::numeric_limits<double>::quiet_NaN() : invoke(i);
            return out;

        // ── int32_t output ────────────────────────────────────────────────────
        } else if constexpr (std::is_same_v<Ret, int32_t>) {
            out = clickhouse_create_buffer(HEADER_BYTES + COL_DESC_BYTES + n * 4u);
            col_write_fixed_header<int32_t>(out, n, COL_FIXED32);
            int32_t* res = reinterpret_cast<int32_t*>(out->data() + HEADER_BYTES + COL_DESC_BYTES);
            for (uint32_t i = 0; i < n; ++i)
                res[i] = any_null(i) ? 0 : invoke(i);
            return out;

        // ── Geometry output (nullable) ────────────────────────────────────────
        } else if constexpr (std::is_same_v<Ret, std::unique_ptr<geos::geom::Geometry>>) {
            out = clickhouse_create_buffer(0);
            ColBytesWriter w(out, n);
            for (uint32_t i = 0; i < n; ++i) {
                if (any_null(i)) { w.push_null(); continue; }
                w.push_geom(invoke(i));
            }
            w.finish();
            return out;

        // ── string output (non-nullable — preserves current columnar_string* behaviour)
        } else if constexpr (std::is_same_v<Ret, std::string>) {
            out = clickhouse_create_buffer(0);
            ColBytesWriter w(out, n, /*nullable=*/false);
            for (uint32_t i = 0; i < n; ++i) {
                std::string s = invoke(i);
                w.push_bytes({reinterpret_cast<const uint8_t*>(s.data()), s.size()});
            }
            w.finish();
            return out;
        }

    } catch (const std::exception& e) {
        if (out) clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(out));
        ch::panic(e.what());
    }
    __builtin_unreachable();
}

} // namespace ch

// ── Registration macros ───────────────────────────────────────────────────────
// All macros route through columnar_impl_wrapper; return types and arg types
// are deduced from the _impl function pointer.

// 2-arg binary predicate with bbox shortcut + PreparedGeometry optimisation.
#define CH_UDF_COL_BBOX2(name, bbox_op, early_ret)                               \
    __attribute__((export_name(#name "_col")))                                   \
    ch::raw_buffer * name##_col(ch::raw_buffer * ptr, uint32_t num_rows) {       \
        return ch::columnar_impl_wrapper(ptr, num_rows, ch::name##_impl,         \
            ch::bbox_op, early_ret, ch::prep_a_##name, ch::prep_b_##name);       \
    }

// 3-arg predicate: (geom, geom, double) -> bool  with PreparedGeometry support.
#define CH_UDF_COL_PRED3(name)                                                   \
    __attribute__((export_name(#name "_col")))                                   \
    ch::raw_buffer * name##_col(ch::raw_buffer * ptr, uint32_t num_rows) {       \
        return ch::columnar_impl_wrapper(ptr, num_rows, ch::name##_impl,         \
            nullptr, false, nullptr, nullptr,                                     \
            ch::prep_a_##name, ch::prep_b_##name);                               \
    }

// Generic columnar wrapper — all arg/return types deduced from name##_impl.
#define CH_UDF_COL(name)                                                         \
    __attribute__((export_name(#name "_col")))                                   \
    ch::raw_buffer * name##_col(ch::raw_buffer * ptr, uint32_t num_rows) {       \
        return ch::columnar_impl_wrapper(ptr, num_rows, ch::name##_impl);        \
    }
