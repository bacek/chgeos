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

#include <cstdint>
#include <cstring>
#include <limits>
#include <span>
#include <stdexcept>

#include "clickhouse.hpp"
#include "geom/wkb.hpp"
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
    uint32_t        row_count;    // stored rows (1 if const)
    const uint8_t*  null_map;     // nullable: null_map[i]!=0 → NULL; nullptr = non-nullable
    const uint32_t* offsets;      // start-based; nullptr for fixed-width
    const uint8_t*  data;

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
        v.is_const  = (d.type & COL_IS_CONST) != 0;
        v.base_type = static_cast<ColType>(d.type & ~COL_IS_CONST);
        v.row_count = v.is_const ? 1u : num_rows;
        v.null_map  = d.null_offset    ? base + d.null_offset    : nullptr;
        v.offsets   = d.offsets_offset ? reinterpret_cast<const uint32_t*>(base + d.offsets_offset) : nullptr;
        v.data      = base + d.data_offset;
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

// ── Generic wrappers ──────────────────────────────────────────────────────────

// (Span, Span) -> bool
template <typename Impl>
raw_buffer* columnar_pred2(raw_buffer* ptr, uint32_t, Impl impl) {
    auto cb = parse_columnar(ptr);
    uint32_t n = cb.num_rows;
    auto ca = cb.col(0);
    auto cb2 = cb.col(1);

    raw_buffer* out = clickhouse_create_buffer(HEADER_BYTES + COL_DESC_BYTES + n);
    try {
        col_write_fixed_header<uint8_t>(out, n, COL_FIXED8);
        uint8_t* res = out->data() + HEADER_BYTES + COL_DESC_BYTES;
        for (uint32_t i = 0; i < n; ++i)
            res[i] = (!ca.is_null(i) && !cb2.is_null(i) && impl(ca.get_bytes(i), cb2.get_bytes(i))) ? 1u : 0u;
        return out;
    } catch (const std::exception& e) {
        clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(out));
        ch::panic(e.what());
    }
}

// (Span, Span, double) -> bool
template <typename Impl>
raw_buffer* columnar_pred3(raw_buffer* ptr, uint32_t, Impl impl) {
    auto cb = parse_columnar(ptr);
    uint32_t n = cb.num_rows;
    auto ca = cb.col(0);
    auto cb2 = cb.col(1);
    auto cc  = cb.col(2);

    raw_buffer* out = clickhouse_create_buffer(HEADER_BYTES + COL_DESC_BYTES + n);
    try {
        col_write_fixed_header<uint8_t>(out, n, COL_FIXED8);
        uint8_t* res = out->data() + HEADER_BYTES + COL_DESC_BYTES;
        for (uint32_t i = 0; i < n; ++i)
            res[i] = (!ca.is_null(i) && !cb2.is_null(i) &&
                      impl(ca.get_bytes(i), cb2.get_bytes(i), cc.get_fixed<double>(i))) ? 1u : 0u;
        return out;
    } catch (const std::exception& e) {
        clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(out));
        ch::panic(e.what());
    }
}

// (Span) -> double
template <typename Impl>
raw_buffer* columnar_scalar1_f64(raw_buffer* ptr, uint32_t, Impl impl) {
    auto cb = parse_columnar(ptr);
    uint32_t n = cb.num_rows;
    auto ca = cb.col(0);

    raw_buffer* out = clickhouse_create_buffer(HEADER_BYTES + COL_DESC_BYTES + n * 8u);
    try {
        col_write_fixed_header<double>(out, n, COL_FIXED64);
        double* res = reinterpret_cast<double*>(out->data() + HEADER_BYTES + COL_DESC_BYTES);
        for (uint32_t i = 0; i < n; ++i)
            res[i] = ca.is_null(i) ? std::numeric_limits<double>::quiet_NaN() : impl(ca.get_bytes(i));
        return out;
    } catch (const std::exception& e) {
        clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(out));
        ch::panic(e.what());
    }
}

// (Span, Span) -> double
template <typename Impl>
raw_buffer* columnar_scalar2_f64(raw_buffer* ptr, uint32_t, Impl impl) {
    auto cb = parse_columnar(ptr);
    uint32_t n = cb.num_rows;
    auto ca = cb.col(0);
    auto cb2 = cb.col(1);

    raw_buffer* out = clickhouse_create_buffer(HEADER_BYTES + COL_DESC_BYTES + n * 8u);
    try {
        col_write_fixed_header<double>(out, n, COL_FIXED64);
        double* res = reinterpret_cast<double*>(out->data() + HEADER_BYTES + COL_DESC_BYTES);
        for (uint32_t i = 0; i < n; ++i)
            res[i] = (ca.is_null(i) || cb2.is_null(i))
                ? std::numeric_limits<double>::quiet_NaN()
                : impl(ca.get_bytes(i), cb2.get_bytes(i));
        return out;
    } catch (const std::exception& e) {
        clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(out));
        ch::panic(e.what());
    }
}

// (Span, Span) -> WKB
template <typename Impl>
raw_buffer* columnar_geom2(raw_buffer* ptr, uint32_t, Impl impl) {
    auto cb = parse_columnar(ptr);
    uint32_t n = cb.num_rows;
    auto ca = cb.col(0);
    auto cb2 = cb.col(1);

    raw_buffer* out = clickhouse_create_buffer(0);
    try {
        ColBytesWriter w(out, n);
        for (uint32_t i = 0; i < n; ++i) {
            if (ca.is_null(i) || cb2.is_null(i)) { w.push_null(); continue; }
            w.push_geom(impl(read_wkb(ca.get_bytes(i)), read_wkb(cb2.get_bytes(i))));
        }
        w.finish();
        return out;
    } catch (const std::exception& e) {
        clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(out));
        ch::panic(e.what());
    }
}

// (Span) -> WKB
template <typename Impl>
raw_buffer* columnar_geom1(raw_buffer* ptr, uint32_t, Impl impl) {
    auto cb = parse_columnar(ptr);
    uint32_t n = cb.num_rows;
    auto ca = cb.col(0);

    raw_buffer* out = clickhouse_create_buffer(0);
    try {
        ColBytesWriter w(out, n);
        for (uint32_t i = 0; i < n; ++i) {
            if (ca.is_null(i)) { w.push_null(); continue; }
            w.push_geom(impl(read_wkb(ca.get_bytes(i))));
        }
        w.finish();
        return out;
    } catch (const std::exception& e) {
        clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(out));
        ch::panic(e.what());
    }
}

// (Span) -> bool
template <typename Impl>
raw_buffer* columnar_pred1(raw_buffer* ptr, uint32_t, Impl impl) {
    auto cb = parse_columnar(ptr);
    uint32_t n = cb.num_rows;
    auto ca = cb.col(0);

    raw_buffer* out = clickhouse_create_buffer(HEADER_BYTES + COL_DESC_BYTES + n);
    try {
        col_write_fixed_header<uint8_t>(out, n, COL_FIXED8);
        uint8_t* res = out->data() + HEADER_BYTES + COL_DESC_BYTES;
        for (uint32_t i = 0; i < n; ++i)
            res[i] = (!ca.is_null(i) && impl(ca.get_bytes(i))) ? 1u : 0u;
        return out;
    } catch (const std::exception& e) {
        clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(out));
        ch::panic(e.what());
    }
}

// (Span) -> int32_t
template <typename Impl>
raw_buffer* columnar_scalar1_i32(raw_buffer* ptr, uint32_t, Impl impl) {
    auto cb = parse_columnar(ptr);
    uint32_t n = cb.num_rows;
    auto ca = cb.col(0);

    raw_buffer* out = clickhouse_create_buffer(HEADER_BYTES + COL_DESC_BYTES + n * 4u);
    try {
        col_write_fixed_header<int32_t>(out, n, COL_FIXED32);
        int32_t* res = reinterpret_cast<int32_t*>(out->data() + HEADER_BYTES + COL_DESC_BYTES);
        for (uint32_t i = 0; i < n; ++i)
            res[i] = ca.is_null(i) ? 0 : impl(ca.get_bytes(i));
        return out;
    } catch (const std::exception& e) {
        clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(out));
        ch::panic(e.what());
    }
}

// (Span) -> std::string  (non-nullable output)
template <typename Impl>
raw_buffer* columnar_string1(raw_buffer* ptr, uint32_t, Impl impl) {
    auto cb = parse_columnar(ptr);
    uint32_t n = cb.num_rows;
    auto ca = cb.col(0);

    raw_buffer* out = clickhouse_create_buffer(0);
    try {
        ColBytesWriter w(out, n, /*nullable=*/false);
        for (uint32_t i = 0; i < n; ++i) {
            std::string s = impl(ca.get_bytes(i));
            w.push_bytes({reinterpret_cast<const uint8_t*>(s.data()), s.size()});
        }
        w.finish();
        return out;
    } catch (const std::exception& e) {
        clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(out));
        ch::panic(e.what());
    }
}

// (Span, Span) -> std::string  (non-nullable output)
template <typename Impl>
raw_buffer* columnar_string2(raw_buffer* ptr, uint32_t, Impl impl) {
    auto cb = parse_columnar(ptr);
    uint32_t n = cb.num_rows;
    auto ca = cb.col(0);
    auto cb2 = cb.col(1);

    raw_buffer* out = clickhouse_create_buffer(0);
    try {
        ColBytesWriter w(out, n, /*nullable=*/false);
        for (uint32_t i = 0; i < n; ++i) {
            std::string s = impl(ca.get_bytes(i), cb2.get_bytes(i));
            w.push_bytes({reinterpret_cast<const uint8_t*>(s.data()), s.size()});
        }
        w.finish();
        return out;
    } catch (const std::exception& e) {
        clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(out));
        ch::panic(e.what());
    }
}

} // namespace ch

// ── Registration macros ───────────────────────────────────────────────────────

// 2-arg binary predicate with bbox shortcut — exports <name>_col
#define CH_UDF_COL_BBOX2(name, bbox_op, early_ret)                               \
    __attribute__((export_name(#name "_col")))                                   \
    ch::raw_buffer * name##_col(ch::raw_buffer * ptr, uint32_t num_rows) {       \
        return ch::columnar_pred2(ptr, num_rows,                                 \
            +[](std::span<const uint8_t> a, std::span<const uint8_t> b) -> bool {\
                return ch::with_bbox(a, b, ch::bbox_op, early_ret,               \
                    ch::name##_impl);                                             \
            });                                                                   \
    }

// 3-arg predicate: (geom, geom, double) -> bool
#define CH_UDF_COL_PRED3(name)                                                   \
    __attribute__((export_name(#name "_col")))                                   \
    ch::raw_buffer * name##_col(ch::raw_buffer * ptr, uint32_t num_rows) {       \
        return ch::columnar_pred3(ptr, num_rows, ch::name##_impl);               \
    }

// 1-arg scalar(Span -> double) — for impls that take unique_ptr<Geometry>
#define CH_UDF_COL_SCALAR1_F64(name)                                             \
    __attribute__((export_name(#name "_col")))                                   \
    ch::raw_buffer * name##_col(ch::raw_buffer * ptr, uint32_t num_rows) {       \
        return ch::columnar_scalar1_f64(ptr, num_rows,                           \
            +[](std::span<const uint8_t> a) {                                    \
                return ch::name##_impl(ch::read_wkb(a));                         \
            });                                                                   \
    }

// 2-arg scalar(Span, Span -> double) — for impls that take unique_ptr<Geometry>
#define CH_UDF_COL_SCALAR2_F64(name)                                             \
    __attribute__((export_name(#name "_col")))                                   \
    ch::raw_buffer * name##_col(ch::raw_buffer * ptr, uint32_t num_rows) {       \
        return ch::columnar_scalar2_f64(ptr, num_rows,                           \
            +[](std::span<const uint8_t> a, std::span<const uint8_t> b) {       \
                return ch::name##_impl(ch::read_wkb(a), ch::read_wkb(b));       \
            });                                                                   \
    }

// 2-arg geom->geom
#define CH_UDF_COL_GEOM2(name)                                                   \
    __attribute__((export_name(#name "_col")))                                   \
    ch::raw_buffer * name##_col(ch::raw_buffer * ptr, uint32_t num_rows) {       \
        return ch::columnar_geom2(ptr, num_rows, ch::name##_impl);               \
    }

// 1-arg geom->geom
#define CH_UDF_COL_GEOM1(name)                                                   \
    __attribute__((export_name(#name "_col")))                                   \
    ch::raw_buffer * name##_col(ch::raw_buffer * ptr, uint32_t num_rows) {       \
        return ch::columnar_geom1(ptr, num_rows, ch::name##_impl);               \
    }

// 1-arg predicate (geom -> bool) → UInt8
#define CH_UDF_COL_PRED1(name)                                                   \
    __attribute__((export_name(#name "_col")))                                   \
    ch::raw_buffer * name##_col(ch::raw_buffer * ptr, uint32_t num_rows) {       \
        return ch::columnar_pred1(ptr, num_rows,                                 \
            +[](std::span<const uint8_t> a) -> bool {                            \
                return ch::name##_impl(ch::read_wkb(a));                         \
            });                                                                   \
    }

// 1-arg scalar (geom -> int32_t) → Int32
#define CH_UDF_COL_SCALAR1_I32(name)                                             \
    __attribute__((export_name(#name "_col")))                                   \
    ch::raw_buffer * name##_col(ch::raw_buffer * ptr, uint32_t num_rows) {       \
        return ch::columnar_scalar1_i32(ptr, num_rows,                           \
            +[](std::span<const uint8_t> a) {                                    \
                return ch::name##_impl(ch::read_wkb(a));                         \
            });                                                                   \
    }

// 1-arg string output (geom -> std::string) → String (non-nullable)
#define CH_UDF_COL_STRING1(name)                                                 \
    __attribute__((export_name(#name "_col")))                                   \
    ch::raw_buffer * name##_col(ch::raw_buffer * ptr, uint32_t num_rows) {       \
        return ch::columnar_string1(ptr, num_rows,                               \
            +[](std::span<const uint8_t> a) {                                    \
                return ch::name##_impl(ch::read_wkb(a));                         \
            });                                                                   \
    }

// 2-arg string output (geom, geom -> std::string) → String (non-nullable)
#define CH_UDF_COL_STRING2(name)                                                 \
    __attribute__((export_name(#name "_col")))                                   \
    ch::raw_buffer * name##_col(ch::raw_buffer * ptr, uint32_t num_rows) {       \
        return ch::columnar_string2(ptr, num_rows,                               \
            +[](std::span<const uint8_t> a, std::span<const uint8_t> b) {       \
                return ch::name##_impl(ch::read_wkb(a), ch::read_wkb(b));       \
            });                                                                   \
    }
