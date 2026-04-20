#pragma once

// COLUMNAR_V1 wire format for ClickHouse WASM UDFs.
//
// Replaces RowBinary with a columnar layout.  Key benefit: ColumnConst data
// (e.g. a constant 169 KB polygon) is passed ONCE regardless of num_rows.
//
// ┌──────────────────────────────────────────────────────────────────────────┐
// │ BufHeader (8 bytes)                                                      │
// │   num_rows : u32                                                         │
// │   num_cols : u32                                                         │
// ├──────────────────────────────────────────────────────────────────────────┤
// │ ColDescriptor[num_cols] (20 bytes each)                                  │
// │   type           : u32  — ColType | COL_IS_CONST flag                   │
// │   null_offset    : u32  — offset to u8[row_count] null map; 0=no nulls  │
// │   offsets_offset : u32  — offset to u32[row_count+1] start offsets;     │
// │                           0 for fixed-width columns                      │
// │   data_offset    : u32  — offset to raw column data                     │
// │   data_size      : u32  — total bytes in the data block                 │
// ├──────────────────────────────────────────────────────────────────────────┤
// │ Data blocks at offsets described above                                   │
// └──────────────────────────────────────────────────────────────────────────┘
//
// Offsets (COL_BYTES / COL_NULL_BYTES):
//   offsets[0..row_count] are start-based (offsets[0]=0).
//   Data is stored with explicit null terminators (CH 26.4+ ColumnString has none;
//   the CH-side serializer adds them so WASM get_bytes() can use end-start-1).
//   String i bytes: data[offsets[i] .. offsets[i+1]-2], len = offsets[i+1]-offsets[i]-1.
//
// SQL: ABI COLUMNAR_V1  (no serialization_format needed)

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
#include <geos/algorithm/locate/IndexedPointInAreaLocator.h>

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
    // COL_COMPLEX: generic Array(T) / Tuple(T...) — type-guided recursive format.
    // offsets_offset → uint32[N+1] outer offsets (for Array rows; 0 for Tuple/scalar).
    // data_offset    → recursive data block (layout determined by C++/CH declared type).
    // Recursive layout per type:
    //   scalar T:           T[N]  (packed, fixed width)
    //   String:             uint32[N+1] offsets + bytes (null-terminated per COL_BYTES)
    //   vector<T> (Array):  uint32[N+1] outer_offsets → M total, then recursive(M, T)
    //   pair/tuple (Tuple): recursive(N, T0) ++ recursive(N, T1) ++ ...  (columnar)
    COL_COMPLEX     = 8,

    COL_IS_CONST    = 0x80u, // flag: 1 stored row, broadcast to num_rows
    // COL_IS_REPEAT: column is cyclic with period R stored in offsets_offset.
    // Row i maps to stored_row[i % R].  For string columns the R+1 wire offsets
    // are embedded at the start of the data block (not at offsets_offset).
    COL_IS_REPEAT   = 0x40u,
};

// ── Type traits for complex (Array/Tuple) C++ types ─────────────────────────

template <typename T> struct is_vector_t      : std::false_type {};
template <typename T> struct is_vector_t<std::vector<T>> : std::true_type {};
template <typename T> inline constexpr bool is_vector_v = is_vector_t<T>::value;

template <typename T> struct is_pair_t        : std::false_type {};
template <typename A, typename B> struct is_pair_t<std::pair<A,B>> : std::true_type {};
template <typename T> inline constexpr bool is_pair_v = is_pair_t<T>::value;

template <typename T> struct is_tuple_t       : std::false_type {};
template <typename... Ts> struct is_tuple_t<std::tuple<Ts...>> : std::true_type {};
template <typename T> inline constexpr bool is_tuple_v = is_tuple_t<T>::value;

template <typename T> inline constexpr bool is_complex_v =
    is_vector_v<T> || is_pair_v<T> || is_tuple_v<T>;

// ── Wire structs ──────────────────────────────────────────────────────────────

struct ColDescriptor {
    uint32_t type;
    uint32_t null_offset;
    uint32_t offsets_offset;
    uint32_t data_offset;
    uint32_t data_size;
};
static_assert(sizeof(ColDescriptor) == 20);

static constexpr uint32_t HEADER_BYTES  = 8;   // sizeof BufHeader
static constexpr uint32_t COL_DESC_BYTES = 20;  // sizeof ColDescriptor

// ── Input column accessor ─────────────────────────────────────────────────────

struct ColView {
    ColType         base_type;
    bool            is_const;
    uint32_t        period;       // COL_IS_REPEAT period R; 0 = not cyclic
    uint32_t        row_count;    // stored rows (1 if const, R if repeat, N otherwise)
    const uint8_t*  null_map;     // nullable: null_map[i]!=0 → NULL; nullptr = non-nullable
    const uint32_t* offsets;      // start-based; nullptr for fixed-width
    const uint8_t*  data;

    // Map logical row to stored row index.
    uint32_t effective_row(uint32_t row) const noexcept {
        if (is_const) return 0u;
        if (period)   return row % period;
        return row;
    }

    bool is_null(uint32_t row) const noexcept {
        if (!null_map) return false;
        return null_map[effective_row(row)] != 0;
    }

    // For COL_BYTES/COL_NULL_BYTES — excludes the trailing null terminator.
    std::span<const uint8_t> get_bytes(uint32_t row) const noexcept {
        uint32_t idx   = effective_row(row);
        uint32_t start = offsets[idx];
        uint32_t end   = offsets[idx + 1];
        uint32_t len   = (end > start + 1) ? end - start - 1 : 0u;
        return {data + start, len};
    }

    template <typename T>
    T get_fixed(uint32_t row) const noexcept {
        uint32_t idx = effective_row(row);
        T v;
        std::memcpy(&v, data + idx * sizeof(T), sizeof(T));
        return v;
    }

    // True when every logical row carries the same bytes.
    // Covers: COL_IS_CONST, COL_IS_REPEAT with period==1, and the legacy
    // cross-join pattern where CH repeats the same WKB N times without a flag.
    bool is_effectively_const_bytes() const noexcept {
        if (is_const || period == 1u) return true;
        if (!offsets || row_count < 2) return false;
        uint32_t elem_stride = offsets[1];
        if (elem_stride == 0) return false;
        if (offsets[row_count] != elem_stride * row_count) return false;
        uint32_t wkb_len = elem_stride > 0 ? elem_stride - 1 : 0;
        uint32_t last_start = offsets[row_count - 1];
        return std::memcmp(data, data + last_start, wkb_len) == 0;
    }
};

struct ColumnarBuf {
    uint32_t              num_rows;
    uint32_t              num_cols;
    const ColDescriptor*  descs;
    const uint8_t*        base;

    ColView col(uint32_t i) const {
        ColDescriptor d;
        std::memcpy(&d, descs + i, sizeof(d));
        ColView v;
        bool has_repeat = (d.type & COL_IS_REPEAT) != 0;
        v.is_const  = (d.type & COL_IS_CONST) != 0;
        v.period    = has_repeat ? d.offsets_offset : 0u;
        v.base_type = static_cast<ColType>(d.type & ~(COL_IS_CONST | COL_IS_REPEAT));
        v.null_map  = d.null_offset ? base + d.null_offset : nullptr;

        if (has_repeat) {
            // offsets_offset carries the period R, not a byte offset.
            // For string columns, the R+1 wire offsets are embedded at data_offset.
            v.row_count = v.period;
            v.data      = base + d.data_offset;
            if (v.base_type == COL_BYTES || v.base_type == COL_NULL_BYTES) {
                v.offsets = reinterpret_cast<const uint32_t*>(v.data);
                v.data    = v.data + (v.period + 1u) * sizeof(uint32_t);
            } else {
                v.offsets = nullptr;
            }
        } else {
            v.row_count = v.is_const ? 1u : num_rows;
            v.offsets   = d.offsets_offset ? reinterpret_cast<const uint32_t*>(base + d.offsets_offset) : nullptr;
            v.data      = base + d.data_offset;
        }
        return v;
    }
};

inline ColumnarBuf parse_columnar(const raw_buffer* buf) {
    const uint8_t* p = buf->data();
    ColumnarBuf cb;
    cb.base = p;
    std::memcpy(&cb.num_rows, p,     4);
    std::memcpy(&cb.num_cols, p + 4, 4);
    cb.descs = reinterpret_cast<const ColDescriptor*>(p + HEADER_BYTES);
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

// ── COL_COMPLEX output writer ─────────────────────────────────────────────────
// write_complex_data<T>(out, n, get_val): appends N rows of type T to `out`.
// get_val(i) → T; may be called twice per element for pair/tuple fields.
// For vector<T>, pre-collects all rows before writing.

template <typename T, typename GetVal>
void write_complex_data(raw_buffer* out, uint32_t n, GetVal get_val) {
    if constexpr (std::is_arithmetic_v<T> && !std::is_same_v<T, bool>) {
        for (uint32_t i = 0; i < n; ++i) {
            T v = get_val(i);
            out->append(reinterpret_cast<const uint8_t*>(&v), sizeof(T));
        }
    } else if constexpr (std::is_same_v<T, std::string>) {
        // Two-pass: offsets[n+1] + bytes (null-terminated)
        std::vector<std::string> strs(n);
        for (uint32_t i = 0; i < n; ++i) strs[i] = get_val(i);
        std::vector<uint32_t> offs(n + 1u);
        offs[0] = 0u;
        for (uint32_t i = 0; i < n; ++i)
            offs[i + 1u] = offs[i] + static_cast<uint32_t>(strs[i].size()) + 1u;
        out->append(reinterpret_cast<const uint8_t*>(offs.data()), (n + 1u) * 4u);
        for (uint32_t i = 0; i < n; ++i) {
            out->append(reinterpret_cast<const uint8_t*>(strs[i].data()),
                        static_cast<uint32_t>(strs[i].size()));
            out->push_back(0u);
        }
    } else if constexpr (std::is_same_v<T, std::unique_ptr<geos::geom::Geometry>>) {
        // Two-pass: collect WKBs, then offsets + bytes (null-terminated)
        std::vector<std::vector<uint8_t>> wkbs(n);
        for (uint32_t i = 0; i < n; ++i) {
            auto g = get_val(i);
            if (g) wkbs[i] = write_ewkb(g);
        }
        std::vector<uint32_t> offs(n + 1u);
        offs[0] = 0u;
        for (uint32_t i = 0; i < n; ++i)
            offs[i + 1u] = offs[i] + static_cast<uint32_t>(wkbs[i].size()) + 1u;
        out->append(reinterpret_cast<const uint8_t*>(offs.data()), (n + 1u) * 4u);
        for (uint32_t i = 0; i < n; ++i) {
            out->append(wkbs[i].data(), static_cast<uint32_t>(wkbs[i].size()));
            out->push_back(0u);
        }
    } else if constexpr (is_vector_v<T>) {
        using ElemT = typename T::value_type;
        // Collect all rows, write outer offsets, flatten elements, recurse.
        std::vector<T> rows(n);
        for (uint32_t i = 0; i < n; ++i) rows[i] = get_val(i);
        std::vector<uint32_t> outer_offs(n + 1u);
        outer_offs[0] = 0u;
        for (uint32_t i = 0; i < n; ++i)
            outer_offs[i + 1u] = outer_offs[i] + static_cast<uint32_t>(rows[i].size());
        uint32_t M = outer_offs[n];
        out->append(reinterpret_cast<const uint8_t*>(outer_offs.data()), (n + 1u) * 4u);
        std::vector<ElemT> flat;
        flat.reserve(M);
        for (uint32_t i = 0; i < n; ++i)
            for (auto& elem : rows[i]) flat.push_back(std::move(elem));
        write_complex_data<ElemT>(out, M,
            [&](uint32_t j) -> const ElemT& { return flat[j]; });
    } else if constexpr (is_pair_v<T>) {
        using T1 = typename T::first_type;
        using T2 = typename T::second_type;
        write_complex_data<T1>(out, n, [&](uint32_t i) -> T1 { return get_val(i).first;  });
        write_complex_data<T2>(out, n, [&](uint32_t i) -> T2 { return get_val(i).second; });
    } else if constexpr (is_tuple_v<T>) {
        [&]<size_t... I>(std::index_sequence<I...>) {
            (write_complex_data<std::tuple_element_t<I, T>>(out, n,
                [&](uint32_t i) -> std::tuple_element_t<I, T> {
                    return std::get<I>(get_val(i));
                }), ...);
        }(std::make_index_sequence<std::tuple_size_v<T>>{});
    }
}

// Write a single-column COL_COMPLEX output buffer from n invocations of get_val.
template <typename Ret, typename GetVal>
raw_buffer* write_complex_col(uint32_t n, GetVal get_val) {
    raw_buffer* out = clickhouse_create_buffer(0);
    out->resize(HEADER_BYTES + COL_DESC_BYTES);
    uint8_t* p = out->data();
    std::memcpy(p,     &n,  4);
    const uint32_t one = 1u;
    std::memcpy(p + 4, &one, 4);
    ColDescriptor d{};
    d.type        = static_cast<uint32_t>(COL_COMPLEX);
    d.data_offset = HEADER_BYTES + COL_DESC_BYTES;
    std::memcpy(p + HEADER_BYTES, &d, sizeof(d));

    std::vector<Ret> vals(n);
    for (uint32_t i = 0; i < n; ++i) vals[i] = get_val(i);
    write_complex_data<Ret>(out, n,
        [&](uint32_t i) -> const Ret& { return vals[i]; });

    // Patch data_size (at byte offset 16 within ColDescriptor = HEADER_BYTES+16 in buf)
    uint32_t data_size = out->size() - (HEADER_BYTES + COL_DESC_BYTES);
    std::memcpy(out->data() + HEADER_BYTES + 16u, &data_size, 4u);
    return out;
}

// ── COL_COMPLEX array reader ──────────────────────────────────────────────────
// Reads one row of an Array(T) COL_COMPLEX column.
// Wire layout (see COL_COMPLEX comment in ColType):
//   col.offsets → uint32[row_count+1] outer offsets (cumulative element counts)
//   col.data    → element data:
//     Array(String/WKB): uint32[M_total+1] inner_offsets + bytes (null-terminated)
//     Array(arithmetic): ElemT[M_total] packed

template <typename ElemT>
std::vector<ElemT> col_get_complex_array(const ColView& col, uint32_t row) {
    uint32_t idx         = col.effective_row(row);
    uint32_t outer_start = col.offsets[idx];
    uint32_t outer_end   = col.offsets[idx + 1];
    uint32_t M_total     = col.offsets[col.row_count];  // row_count = 1/R/N per mode
    uint32_t count       = outer_end - outer_start;

    std::vector<ElemT> result;
    result.reserve(count);

    if constexpr (std::is_same_v<ElemT, std::span<const uint8_t>> ||
                  std::is_same_v<ElemT, std::unique_ptr<geos::geom::Geometry>>) {
        // Array(String / WKB): data = [uint32[M_total+1] inner_offs][bytes]
        const uint32_t* inner_offs = reinterpret_cast<const uint32_t*>(col.data);
        const uint8_t*  chars      = col.data + (M_total + 1u) * sizeof(uint32_t);
        for (uint32_t j = outer_start; j < outer_end; ++j) {
            uint32_t s   = inner_offs[j];
            uint32_t e   = inner_offs[j + 1];
            uint32_t len = (e > s + 1u) ? e - s - 1u : 0u;  // strip null term
            std::span<const uint8_t> sp{chars + s, len};
            if constexpr (std::is_same_v<ElemT, std::span<const uint8_t>>)
                result.push_back(sp);
            else
                result.push_back(read_wkb(sp));
        }
    } else if constexpr (std::is_arithmetic_v<ElemT>) {
        // Array(numeric): data = ElemT[M_total] packed
        const ElemT* data_ptr = reinterpret_cast<const ElemT*>(col.data);
        for (uint32_t j = outer_start; j < outer_end; ++j)
            result.push_back(data_ptr[j]);
    } else if constexpr (std::is_same_v<ElemT, std::string>) {
        const uint32_t* inner_offs = reinterpret_cast<const uint32_t*>(col.data);
        const uint8_t*  chars      = col.data + (M_total + 1u) * sizeof(uint32_t);
        for (uint32_t j = outer_start; j < outer_end; ++j) {
            uint32_t s   = inner_offs[j];
            uint32_t e   = inner_offs[j + 1];
            uint32_t len = (e > s + 1u) ? e - s - 1u : 0u;
            result.push_back(std::string(reinterpret_cast<const char*>(chars + s), len));
        }
    }
    return result;
}

// ── Input column accessor by type ─────────────────────────────────────────────

// Read a fixed-width column value as type T, widening from narrower stored types.
// CH passes integer literals as the smallest fitting type (e.g. UInt8 for `2`),
// but the _impl function may declare a wider type (e.g. int32_t).  We check the
// actual stored ColType and widen via static_cast.  For floating-point targets,
// bytes are bit-cast directly (no numeric cast across float/int boundary).
template <typename T>
T col_get_fixed_widened(const ColView& col, uint32_t row) noexcept {
    uint32_t idx = col.effective_row(row);
    switch (col.base_type) {
        case COL_FIXED8:
        case COL_NULL_FIXED8: {
            uint8_t v; std::memcpy(&v, col.data + idx, 1);
            return static_cast<T>(v);
        }
        case COL_FIXED32:
        case COL_NULL_FIXED32: {
            uint32_t v; std::memcpy(&v, col.data + idx * 4u, 4u);
            return static_cast<T>(v);
        }
        default: {
            T v; std::memcpy(&v, col.data + idx * sizeof(T), sizeof(T));
            return v;
        }
    }
}

template <typename T>
T col_get_arg(const ColView& col, uint32_t row) {
    if constexpr (is_vector_v<T>) {
        return col_get_complex_array<typename T::value_type>(col, row);
    } else if constexpr (std::is_same_v<T, std::span<const uint8_t>>) {
        return col.get_bytes(row);
    } else if constexpr (std::is_same_v<T, double>) {
        return col_get_fixed_widened<double>(col, row);
    } else if constexpr (std::is_same_v<T, int32_t>) {
        return col_get_fixed_widened<int32_t>(col, row);
    } else if constexpr (std::is_same_v<T, uint32_t>) {
        return col_get_fixed_widened<uint32_t>(col, row);
    } else if constexpr (std::is_same_v<T, std::string_view>) {
        auto s = col.get_bytes(row);
        return {reinterpret_cast<const char*>(s.data()), s.size()};
    } else if constexpr (std::is_same_v<T, std::unique_ptr<geos::geom::Geometry>>) {
        return read_wkb(col.get_bytes(row));
    }
}

// ── Generic columnar wrapper ───────────────────────────────────────────────────
// Mirrors rowbinary_impl_wrapper: takes a typed function pointer, deduces
// argument and return types, dispatches column reads and output format.
//
// Optional parameters (for binary geometry predicates only):
//   bbox_op  / early_ret — bbox short-circuit applied before WKB parsing
//   prep_a   — PreparedGeometry callback when col(0) is const
//   prep_b   — PreparedGeometry callback when col(1) is const

template <typename Ret, typename... Args>
raw_buffer* columnar_impl_wrapper(raw_buffer* ptr, uint32_t,
                                  Ret (*impl)(Args...),
                                  BboxOp         bbox_op      = nullptr,
                                  bool           early_ret    = false,
                                  ColPrepOp      prep_a       = nullptr,
                                  ColPrepOp      prep_b       = nullptr,
                                  ColPrepDistOp  prep_a_dist  = nullptr,
                                  ColPrepDistOp  prep_b_dist  = nullptr,
                                  ColPrepPointOp prep_a_point = nullptr,  // A-const polygon, B varies as points
                                  ColPrepPointOp prep_b_point = nullptr)  // B-const polygon, A varies as points
{
    using PGF = geos::geom::prep::PreparedGeometryFactory;

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
                // A-const fast path: prepare col(0) once, vary col(1)
                if (cols[0].is_effectively_const_bytes() && prep_a) {
                    if (cols[0].is_null(0)) { std::fill(res, res + n, 0u); return out; }
                    auto span_a = cols[0].get_bytes(0);
                    BBox  bbox_a = wkb_bbox(span_a);
                    auto  geom_a = read_wkb(span_a);

                    // Point fast path: col(1) contains 2D WKB points — no per-row GEOS alloc.
                    if (prep_a_point && n > 0 && !cols[1].is_null(0)) {
                        auto s1 = cols[1].get_bytes(0);
                        uint32_t pt_type = 0;
                        if (s1.size() == 21 && s1[0] == 0x01) memcpy(&pt_type, s1.data() + 1, 4);
                        auto gtype = geom_a->getGeometryTypeId();
                        if (pt_type == 1u && (gtype == geos::geom::GEOS_POLYGON
                                           || gtype == geos::geom::GEOS_MULTIPOLYGON)) {
                            using IPIAL = geos::algorithm::locate::IndexedPointInAreaLocator;
                            IPIAL locator(*geom_a);
                            for (uint32_t i = 0; i < n; ++i) {
                                if (cols[1].is_null(i)) { res[i] = 0u; continue; }
                                auto span_b = cols[1].get_bytes(i);
                                double px, py;
                                memcpy(&px, span_b.data() + 5, 8);
                                memcpy(&py, span_b.data() + 13, 8);
                                if (bbox_op && !bbox_op(bbox_a, BBox{px, py, px, py})) {
                                    res[i] = early_ret ? 1u : 0u; continue;
                                }
                                res[i] = prep_a_point(&locator, px, py) ? 1u : 0u;
                            }
                            return out;
                        }
                    }

                    auto  pa     = PGF::prepare(geom_a.get());
                    for (uint32_t i = 0; i < n; ++i) {
                        if (cols[1].is_null(i)) { res[i] = 0u; continue; }
                        auto span_b = cols[1].get_bytes(i);
                        if (bbox_op && !bbox_op(bbox_a, wkb_bbox(span_b))) {
                            res[i] = early_ret ? 1u : 0u; continue;
                        }
                        res[i] = prep_a(pa.get(), read_wkb(span_b).get()) ? 1u : 0u;
                    }
                    return out;
                }

                // B-const fast path: prepare col(1) once, vary col(0)
                if (cols[1].is_effectively_const_bytes() && prep_b) {
                    if (cols[1].is_null(0)) { std::fill(res, res + n, 0u); return out; }
                    auto span_b = cols[1].get_bytes(0);
                    BBox  bbox_b = wkb_bbox(span_b);
                    auto  geom_b = read_wkb(span_b);

                    // Point fast path: col(0) contains 2D WKB points — no per-row GEOS alloc.
                    if (prep_b_point && n > 0 && !cols[0].is_null(0)) {
                        auto s0 = cols[0].get_bytes(0);
                        uint32_t pt_type = 0;
                        if (s0.size() == 21 && s0[0] == 0x01) memcpy(&pt_type, s0.data() + 1, 4);
                        auto gtype = geom_b->getGeometryTypeId();
                        if (pt_type == 1u && (gtype == geos::geom::GEOS_POLYGON
                                           || gtype == geos::geom::GEOS_MULTIPOLYGON)) {
                            using IPIAL = geos::algorithm::locate::IndexedPointInAreaLocator;
                            IPIAL locator(*geom_b);
                            for (uint32_t i = 0; i < n; ++i) {
                                if (cols[0].is_null(i)) { res[i] = 0u; continue; }
                                auto span_a = cols[0].get_bytes(i);
                                double px, py;
                                memcpy(&px, span_a.data() + 5, 8);
                                memcpy(&py, span_a.data() + 13, 8);
                                if (bbox_op && !bbox_op(BBox{px, py, px, py}, bbox_b)) {
                                    res[i] = early_ret ? 1u : 0u; continue;
                                }
                                res[i] = prep_b_point(&locator, px, py) ? 1u : 0u;
                            }
                            return out;
                        }
                    }

                    auto  pb     = PGF::prepare(geom_b.get());
                    for (uint32_t i = 0; i < n; ++i) {
                        if (cols[0].is_null(i)) { res[i] = 0u; continue; }
                        auto span_a = cols[0].get_bytes(i);
                        if (bbox_op && !bbox_op(wkb_bbox(span_a), bbox_b)) {
                            res[i] = early_ret ? 1u : 0u; continue;
                        }
                        res[i] = prep_b(pb.get(), read_wkb(span_a).get()) ? 1u : 0u;
                    }
                    return out;
                }
            }

            // 3-arg distance predicate: (geom, geom, double) with PreparedGeometry.
            // col(0)=geom_a, col(1)=geom_b, col(2)=distance.
            if constexpr (nargs >= 3) {
                // A-const dist path
                if (cols[0].is_effectively_const_bytes() && prep_a_dist) {
                    if (cols[0].is_null(0)) { std::fill(res, res + n, 0u); return out; }
                    auto span_a = cols[0].get_bytes(0);
                    BBox  bbox_a = wkb_bbox(span_a);
                    auto  geom_a = read_wkb(span_a);
                    auto  pa     = PGF::prepare(geom_a.get());
                    for (uint32_t i = 0; i < n; ++i) {
                        if (cols[1].is_null(i)) { res[i] = 0u; continue; }
                        auto   span_b = cols[1].get_bytes(i);
                        double dist   = col_get_arg<double>(cols[2], i);
                        if (!bbox_a.intersects(wkb_bbox(span_b).expanded(dist))) {
                            res[i] = 0u; continue;
                        }
                        res[i] = prep_a_dist(pa.get(), read_wkb(span_b).get(), dist) ? 1u : 0u;
                    }
                    return out;
                }
                // B-const dist path
                if (cols[1].is_effectively_const_bytes() && prep_b_dist) {
                    if (cols[1].is_null(0)) { std::fill(res, res + n, 0u); return out; }
                    auto span_b = cols[1].get_bytes(0);
                    BBox  bbox_b = wkb_bbox(span_b);
                    auto  geom_b = read_wkb(span_b);
                    auto  pb     = PGF::prepare(geom_b.get());
                    for (uint32_t i = 0; i < n; ++i) {
                        if (cols[0].is_null(i)) { res[i] = 0u; continue; }
                        auto   span_a = cols[0].get_bytes(i);
                        double dist   = col_get_arg<double>(cols[2], i);
                        if (!wkb_bbox(span_a).intersects(bbox_b.expanded(dist))) {
                            res[i] = 0u; continue;
                        }
                        res[i] = prep_b_dist(pb.get(), read_wkb(span_a).get(), dist) ? 1u : 0u;
                    }
                    return out;
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

        // ── complex output: Array(T), Tuple(T...), pair, nested ──────────────
        } else if constexpr (is_complex_v<Ret>) {
            return write_complex_col<Ret>(n, [&](uint32_t i) -> Ret {
                return any_null(i) ? Ret{} : invoke(i);
            });
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
    __attribute__((export_name(#name)))                                          \
    ch::raw_buffer * name(ch::raw_buffer * ptr, uint32_t num_rows) {             \
        return ch::columnar_impl_wrapper(ptr, num_rows, ch::name##_impl,         \
            ch::bbox_op, early_ret, ch::prep_a_##name, ch::prep_b_##name);       \
    }

// Like CH_UDF_COL_BBOX2 but also registers ColPrepPointOp for 2D WKB point fast path.
// Requires prep_a_pt_##name and prep_b_pt_##name defined in predicates.hpp.
#define CH_UDF_COL_BBOX2_POINT(name, bbox_op, early_ret)                         \
    __attribute__((export_name(#name)))                                          \
    ch::raw_buffer * name(ch::raw_buffer * ptr, uint32_t num_rows) {             \
        return ch::columnar_impl_wrapper(ptr, num_rows, ch::name##_impl,         \
            ch::bbox_op, early_ret, ch::prep_a_##name, ch::prep_b_##name,        \
            nullptr, nullptr,                                                     \
            ch::prep_a_pt_##name, ch::prep_b_pt_##name);                         \
    }

// 3-arg predicate: (geom, geom, double) -> bool  with PreparedGeometry support.
#define CH_UDF_COL_PRED3(name)                                                   \
    __attribute__((export_name(#name)))                                          \
    ch::raw_buffer * name(ch::raw_buffer * ptr, uint32_t num_rows) {             \
        return ch::columnar_impl_wrapper(ptr, num_rows, ch::name##_impl,         \
            nullptr, false, nullptr, nullptr,                                     \
            ch::prep_a_##name, ch::prep_b_##name);                               \
    }

// Generic columnar wrapper — all arg/return types deduced from name##_impl.
#define CH_UDF_COL(name)                                                         \
    __attribute__((export_name(#name)))                                          \
    ch::raw_buffer * name(ch::raw_buffer * ptr, uint32_t num_rows) {             \
        return ch::columnar_impl_wrapper(ptr, num_rows, ch::name##_impl);        \
    }

// Canonical no-suffix alias for PRED3 functions that keep their _col export.
#define CH_UDF_CANONICAL(name)                                                   \
    __attribute__((export_name(#name)))                                          \
    ch::raw_buffer * name(ch::raw_buffer * ptr, uint32_t num_rows) {             \
        return name##_col(ptr, num_rows);                                        \
    }
