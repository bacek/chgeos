#pragma once

// COLUMNAR_V1 wire format for ClickHouse WASM UDFs.
//
// Replaces RowBinary with a columnar layout.  Key benefit: ColumnConst data
// (e.g. a constant 169 KB polygon) is passed ONCE regardless of num_rows.
//
// ┌──────────────────────────────────────────────────────────────────────────┐
// │ FrameHeader (16 bytes)                                                   │
// │   magic    : u32  — 'C','B','I','N'                                      │
// │   version  : u16  — 1                                                    │
// │   reserved : u16                                                         │
// │   num_rows : u32                                                         │
// │   num_cols : u32                                                         │
// ├──────────────────────────────────────────────────────────────────────────┤
// │ ColDescriptor[num_cols] (40 bytes each)                                  │
// │   type           : u64  — ColType | COL_IS_CONST flag                   │
// │   null_offset    : u64  — offset to u8[row_count] null map; 0=no nulls  │
// │   offsets_offset : u64  — offset to u64[row_count+1] start offsets;     │
// │                           0 for fixed-width columns                      │
// │   data_offset    : u64  — offset to raw column data                     │
// │   data_size      : u64  — total bytes in the data block                 │
// ├──────────────────────────────────────────────────────────────────────────┤
// │ Data blocks at offsets described above                                   │
// └──────────────────────────────────────────────────────────────────────────┘
//
// Offsets (COL_BYTES, nullable or not):
//   offsets[0..row_count] are start-based (offsets[0]=0).
//   No null terminators on the wire. ColumnString has no null terminators internally
//   (see ColumnString.h); the wire matches exactly.
//   String i bytes: data[offsets[i] .. offsets[i+1]-1], len = offsets[i+1]-offsets[i].
//
// SQL: ABI COLUMNAR_V1  (no serialization_format needed)

#include <algorithm>
#include <array>
#include <cstdint>
#include <cstring>
#include <limits>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>

#include <geos/geom/prep/PreparedGeometryFactory.h>
#include <geos/algorithm/locate/IndexedPointInAreaLocator.h>
#include <geos/algorithm/locate/SimplePointInAreaLocator.h>
#include <geos/geom/Coordinate.h>
#include <geos/geom/CoordinateSequence.h>
#include <geos/geom/GeometryFactory.h>
#include <geos/geom/LinearRing.h>
#include <geos/geom/LineString.h>
#include <geos/geom/MultiLineString.h>
#include <geos/geom/MultiPolygon.h>
#include <geos/geom/Polygon.h>

#include "clickhouse.hpp"
#include "col_prep_op.hpp"
#include "functions/knn.hpp"
#include "geom/wkb.hpp"
#include "geom/wkb_envelope.hpp"
#include "mem.hpp"

namespace ch {

// Minimum batch size at which building an IndexedPointInAreaLocator pays for
// itself against a plain edge scan.  Measured on real zone polygons across four
// orders of magnitude of size (77B–200KB WKB), the break-even sits in a narrow
// 9–24 row band; 16 lands on the correct side at both extremes.
inline constexpr uint32_t INDEXED_LOCATOR_MIN_ROWS = 16;

// ── Type tags ────────────────────────────────────────────────────────────────

enum ColType : uint32_t {
    COL_BYTES       = 0,  // String:            offsets[row_count+1] + data
    COL_FIXED8      = 1,  // UInt8/Int8:        u8[row_count]
    COL_FIXED16     = 2,  // UInt16/Int16:      u16[row_count]
    COL_FIXED32     = 3,  // UInt32/Int32/Float32
    COL_FIXED64     = 4,  // UInt64/Int64/Float64
    // COL_COMPLEX: generic Array(T) / Tuple(T...) — type-guided recursive format.
    // offsets_offset → uint64[N+1] outer offsets (for Array rows; 0 for Tuple/scalar).
    // data_offset    → recursive data block (layout determined by C++/CH declared type).
    // Recursive layout per type:
    //   scalar T:           T[N]  (packed, fixed width)
    //   String:             uint64[N+1] offsets + bytes (no null terminator, same as COL_BYTES)
    //   vector<T> (Array):  uint64[N+1] outer_offsets → M total, then recursive(M, T)
    //   pair/tuple (Tuple): recursive(N, T0) ++ recursive(N, T1) ++ ...  (columnar)
    COL_COMPLEX     = 5,
    COL_VARIANT     = 6,  // Variant(...): disc[N] + row_offs[N] + header{K, records} + sub-data
    // Fixed-width payload whose width is not 1/2/4/8: UUID, IPv6, Int128/UInt128,
    // Decimal128/256, FixedString(N) for N ∉ {1,2,4,8}.  No offsets array; the
    // element width is recorded only as data_size / row_count.  Widths 1/2/4/8 —
    // including FixedString(8) — are classed into COL_FIXED8/16/32/64 by the host,
    // so signedness and logical type never reach the wire: interpretation comes
    // from the declared C++ side, the tag only validates the width.
    COL_FIXEDN      = 7,
    // Top-level LowCardinality(T): offsets_offset → index[num_rows] of
    // index_elem_width ∈ {1,2,4,8} bytes; data_offset → uint32 dict_row_count,
    // uint8 index_elem_width, pad[3], an embedded ColDescriptor for the
    // dictionary, then the dictionary sub-column.  null_offset is always 0:
    // for LowCardinality(Nullable(T)) NULL rides on dictionary slot 0 and the
    // wire is byte-identical to a plain LC whose dictionary starts with the
    // default value, so this guest only ever decodes the non-nullable shape —
    // any COL_IS_NULLABLE bit on a COL_LOWCARD descriptor is rejected, and the
    // nullable form must be refused host-side before it reaches the wire.
    COL_LOWCARD     = 8,

    // Flags — OR'd onto base type.  Bits 5 and 7 as defined by the host; bits
    // outside the two flags and the known base tags are rejected by
    // ColumnarBuf::col rather than passed through.
    COL_IS_NULLABLE = 0x20u, // Nullable(T): null_offset carries u8[row_count] null map
    COL_IS_CONST    = 0x80u, // 1 stored row, broadcast to num_rows
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
    uint64_t type;
    uint64_t null_offset;
    uint64_t offsets_offset;
    uint64_t data_offset;
    uint64_t data_size;
};
static_assert(sizeof(ColDescriptor) == 40);

// Frame header, matching ClickHouse's ColumnBinaryWire.h:
// [4 B magic | 2 B version | 2 B reserved | 4 B num_rows | 4 B num_cols]
static constexpr uint32_t FRAME_MAGIC   = 0x4E494243u;  // 'C' | 'B'<<8 | 'I'<<16 | 'N'<<24
static constexpr uint16_t FRAME_VERSION = 1;
static constexpr uint32_t HEADER_BYTES  = 16;  // sizeof FrameHeader
static constexpr uint32_t COL_DESC_BYTES = 40;  // sizeof ColDescriptor

// Write the frame header at the start of an output buffer.
inline void write_frame_header(uint8_t* p, uint32_t num_rows, uint32_t num_cols) {
    const uint16_t reserved = 0;
    std::memcpy(p,      &FRAME_MAGIC,   4);
    std::memcpy(p + 4,  &FRAME_VERSION, 2);
    std::memcpy(p + 6,  &reserved,      2);
    std::memcpy(p + 8,  &num_rows,      4);
    std::memcpy(p + 12, &num_cols,      4);
}

// ── Input column accessor ─────────────────────────────────────────────────────

struct ColView {
    ColType         base_type;
    bool            is_const;
    uint32_t        row_count;    // stored rows (1 if const, N otherwise)
    const uint8_t*  null_map;     // nullable: null_map[i]!=0 → NULL;
                                  // COL_VARIANT: discriminators (0xFF = NULL)
    const uint64_t* offsets;      // start-based; nullptr for fixed-width
    const uint8_t*  data;
    uint64_t        data_size;    // descriptor's data blob size
    uint32_t        fixed_width;  // COL_FIXED8/16/32/64: tag width;
                                  // COL_FIXEDN: data_size / row_count (0 if row_count == 0)
    const uint8_t*  base;         // buffer base — needed for absolute offset navigation

    // COL_LOWCARD only: the shared index array and the dictionary sub-column's
    // descriptor (absolute offsets into the same frame).  Populated by
    // ColumnarBuf::col once the header and extents are validated.
    const uint8_t*  lc_index;      // index[row_count], lc_index_width bytes each
    uint8_t         lc_index_width;
    uint32_t        lc_dict_rows;
    ColDescriptor   lc_dict;

    // Map logical row to stored row index.
    uint32_t effective_row(uint32_t row) const noexcept {
        if (is_const) return 0u;
        return row;
    }

    bool is_null(uint32_t row) const noexcept {
        if (!null_map) return false;
        const uint8_t v = null_map[effective_row(row)];
        if (base_type == COL_VARIANT) return v == 0xFFu;
        return v != 0u;
    }

    // For COL_BYTES — exact byte span, no null terminator on wire.
    std::span<const uint8_t> get_bytes(uint32_t row) const noexcept {
        uint32_t idx   = effective_row(row);
        uint64_t start = offsets[idx];
        uint64_t end   = offsets[idx + 1];
        uint64_t len   = end - start;
        return {data + start, static_cast<size_t>(len)};
    }

    // Fixed-width row bytes (COL_FIXED8/16/32/64 via fixed_width, and
    // COL_FIXEDN).  The tag never says what the bytes mean — width only.
    std::span<const uint8_t> get_fixed_bytes(uint32_t row) const {
        if (fixed_width == 0u)
            panic("columnar: fixed-width read of a column with no width");
        uint32_t idx = effective_row(row);
        uint64_t off = uint64_t(idx) * fixed_width;
        if (off + fixed_width > data_size)
            panic("columnar: fixed-width row extends past the column data");
        return {data + off, fixed_width};
    }

    // COL_LOWCARD: the shared index value for a row, read at its wire width.
    uint64_t lc_index_at(uint32_t row) const {
        uint32_t idx = effective_row(row);
        const uint8_t* p = lc_index + uint64_t(idx) * lc_index_width;
        uint64_t v = 0;
        std::memcpy(&v, p, lc_index_width);
        return v;
    }

    // COL_LOWCARD with a COL_BYTES dictionary: the row's value bytes.
    std::span<const uint8_t> lc_get_bytes(uint32_t row) const {
        uint64_t e = lc_index_at(row);
        if (e >= lc_dict_rows)
            panic("columnar: LowCardinality index exceeds dictionary");
        const uint64_t* dict_offs =
            reinterpret_cast<const uint64_t*>(base + lc_dict.offsets_offset);
        uint64_t start = dict_offs[e];
        uint64_t end   = dict_offs[e + 1];
        if (end > lc_dict.data_size)
            panic("columnar: LowCardinality dictionary offsets exceed data");
        return {base + lc_dict.data_offset + start, static_cast<size_t>(end - start)};
    }

    template <typename T>
    T get_fixed(uint32_t row) const noexcept {
        uint32_t idx = effective_row(row);
        T v;
        std::memcpy(&v, data + idx * sizeof(T), sizeof(T));
        return v;
    }

    // True when every logical row carries the same bytes.
    // Covers: COL_IS_CONST, and the legacy cross-join pattern where CH
    // repeats the same WKB N times (uniform offsets + identical elements).
    //
    // Every row must be compared. Checking only the first and last is not
    // enough: callers use this to build one geometry from row 0 and reuse it
    // for the whole batch, so a differing middle row would be silently
    // evaluated against the wrong geometry. Uniform stride is common by
    // itself — every 2D WKB point is 21 bytes — so it proves nothing.
    // The loop exits at row 1 for a genuinely varying column, which is the
    // usual case.
    bool is_effectively_const_bytes() const noexcept {
        if (is_const) return true;
        if (!offsets || row_count < 2) return false;
        uint64_t elem_stride = offsets[1];
        if (elem_stride == 0) return false;
        if (offsets[row_count] != elem_stride * row_count) return false;
        for (uint32_t i = 1; i < row_count; ++i)
            if (std::memcmp(data, data + offsets[i], elem_stride) != 0)
                return false;
        return true;
    }
};

struct ColumnarBuf {
    uint32_t              num_rows;
    uint32_t              num_cols;
    uint64_t              total_bytes;
    const ColDescriptor*  descs;
    const uint8_t*        base;

    // Resolve one column descriptor into a ColView, refusing frames the guest
    // cannot interpret: an unknown base tag (the host's COL_FIXEDN/COL_LOWCARD
    // or plain garbage), stray bits outside the two flags, or a descriptor
    // whose null map / offsets array / data blob is not fully inside the frame.
    // Masking flags and hoping the C++ accessor picks a compatible layout is
    // exactly how a String column over a dictionary index array used to read
    // out-of-frame bytes without any error — see the ColumnBinaryWire.h
    // read-side branches, which validate every untrusted offset the same way.
    ColView col(uint32_t i) const {
        ColDescriptor d;
        std::memcpy(&d, descs + i, sizeof(d));

        // Stray bits above the flags (anything but base-tag low bits, 0x20, 0x80).
        if (d.type & ~(uint64_t)(COL_IS_CONST | COL_IS_NULLABLE | 0x0Fu))
            panic("columnar: descriptor type word has bits outside the known flags");
        const uint64_t base_tag = d.type & ~(uint64_t)(COL_IS_CONST | COL_IS_NULLABLE);
        const bool is_nullable = (d.type & COL_IS_NULLABLE) != 0;
        switch (base_tag) {
            case COL_BYTES:
            case COL_FIXED8:
            case COL_FIXED16:
            case COL_FIXED32:
            case COL_FIXED64:
            case COL_COMPLEX:
            case COL_VARIANT:
            case COL_FIXEDN:
                break;
            case COL_LOWCARD:
                // null_offset is always 0 for the writer's LC columns and the
                // dictionary's nullability never reaches the wire; a set bit is
                // a shape this guest would silently misread.
                if (is_nullable)
                    panic("columnar: LowCardinality(Nullable) is not supported");
                break;
            default:
                panic("columnar: unsupported column tag in descriptor");
        }

        ColView v{};
        v.is_const  = (d.type & COL_IS_CONST) != 0;
        v.base_type = static_cast<ColType>(base_tag);
        v.row_count = v.is_const ? 1u : num_rows;

        // Every extent below is checked against the frame size before any
        // pointer is formed from an untrusted offset.  0 keeps its "absent"
        // meaning for null/offset maps.
        // Extent checks mirror the host reader: an offset equal to the frame
        // size is only valid with a zero-length extent (a zero-row column's
        // empty blob legitimately ends the frame); the remaining-space form
        // keeps a huge untrusted offset from wrapping past the comparison.
        if (d.null_offset &&
            (d.null_offset > total_bytes || v.row_count > total_bytes - d.null_offset))
            panic("columnar: null map extends past end of frame");
        if (d.data_offset > total_bytes ||
            d.data_size > total_bytes - d.data_offset)
            panic("columnar: column data range extends past end of frame");

        // Fixed-width tags: record the element width and require the data blob
        // to be exactly row_count elements (the host reader enforces the same
        // equality; a short blob otherwise reads neighbouring columns as data).
        switch (base_tag) {
            case COL_FIXED8:  v.fixed_width = 1u; break;
            case COL_FIXED16: v.fixed_width = 2u; break;
            case COL_FIXED32: v.fixed_width = 4u; break;
            case COL_FIXED64: v.fixed_width = 8u; break;
            case COL_FIXEDN:
                // Width lives only in data_size/row_count.  A zero-row blob
                // has no width to recover; a non-divisible blob is corruption.
                if (v.row_count == 0u) {
                    if (d.data_size != 0u)
                        panic("columnar: COL_FIXEDN data_size must be 0 for an empty column");
                    v.fixed_width = 0u;
                } else {
                    if (d.data_size % v.row_count != 0u)
                        panic("columnar: COL_FIXEDN data_size is not a multiple of row count");
                    v.fixed_width = uint32_t(d.data_size / v.row_count);
                    if (v.fixed_width == 0u)
                        panic("columnar: COL_FIXEDN element width is zero");
                }
                break;
            default:
                break;
        }
        if (v.fixed_width != 0u &&
            d.data_size != uint64_t(v.row_count) * v.fixed_width)
            panic("columnar: fixed-width data_size does not match row count");

        if (d.offsets_offset) {
            // COL_BYTES reads offsets[idx + 1], so it needs row_count + 1
            // uint64 entries; COL_LOWCARD's array holds one entry per row at
            // the dictionary's index width (parsed below); other tagged users
            // (COL_COMPLEX arrays keep offsets inside the data blob, COL_VARIANT
            // reads one entry per row) need row_count at guest strides.
            uint64_t bytes;
            if (base_tag == COL_BYTES)
                bytes = (v.row_count + 1u) * sizeof(uint64_t);
            else if (base_tag == COL_LOWCARD)
                bytes = uint64_t(v.row_count) * lc_header_index_width(d);
            else
                bytes = v.row_count * sizeof(uint64_t);
            if (d.offsets_offset > total_bytes ||
                bytes > total_bytes - d.offsets_offset)
                panic("columnar: offsets array extends past end of frame");
        }

        v.null_map  = d.null_offset ? base + d.null_offset : nullptr;
        v.offsets   = d.offsets_offset ? reinterpret_cast<const uint64_t*>(base + d.offsets_offset) : nullptr;
        v.data      = base + d.data_offset;
        v.data_size = d.data_size;
        v.base      = base;

        if (base_tag == COL_LOWCARD)
            load_lowcard(v, d);
        return v;
    }

private:
    // index_elem_width from the COL_LOWCARD header at data_offset, validated to
    // be 1/2/4/8.  The header prefix is bounds-checked here because the generic
    // extent checks of col() consult the width before anything else reads it.
    uint8_t lc_header_index_width(const ColDescriptor& d) const {
        constexpr uint64_t prefix = 4u + 1u;
        if (d.data_size < prefix ||
            d.data_offset > total_bytes ||
            prefix > total_bytes - d.data_offset)
            panic("columnar: LowCardinality header truncated");
        uint8_t w;
        std::memcpy(&w, base + d.data_offset + 4u, 1);
        if (w != 1u && w != 2u && w != 4u && w != 8u)
            panic("columnar: LowCardinality index element width is not 1/2/4/8");
        return w;
    }

    // Validate and attach the dictionary of a COL_LOWCARD view.  The embedded
    // dict_desc is untrusted bytes, so every pointer formed from it is bounded
    // against the frame the same way the top-level descriptor's pointers are.
    void load_lowcard(ColView& v, const ColDescriptor& d) const {
        constexpr uint64_t header_bytes = 4u + 4u + COL_DESC_BYTES;
        if (d.data_size < header_bytes ||
            d.data_offset > total_bytes - header_bytes)
            panic("columnar: LowCardinality header truncated");
        const uint8_t w = lc_header_index_width(d);
        // The index array is mandatory for COL_LOWCARD — 0 is never a real
        // offset (same rejection as the host's readColumnFromDesc).
        if (d.offsets_offset == 0)
            panic("columnar: LowCardinality descriptor has no index array");

        uint32_t dict_rows;
        std::memcpy(&dict_rows, base + d.data_offset, 4);
        ColDescriptor dict{};
        std::memcpy(&dict, base + d.data_offset + 8u, COL_DESC_BYTES);

        // The writer never sets those bits on the dictionary descriptor, and a
        // const or nullable dictionary would break navigation (the host reader
        // rejects the const shape for the same reason).
        if (dict.type & COL_IS_CONST)
            panic("columnar: LowCardinality dictionary must not set COL_IS_CONST");
        if (dict.type & COL_IS_NULLABLE)
            panic("columnar: LowCardinality dictionary must not set COL_IS_NULLABLE");
        // Only string-shaped dictionaries are decoded: that is the sole shape
        // any consumer of a LowCardinality argument asks for.
        if ((dict.type & ~uint64_t(COL_IS_CONST | COL_IS_NULLABLE)) != COL_BYTES)
            panic("columnar: LowCardinality dictionary is not COL_BYTES");
        // ColumnUnique reserves the leading default slot, so a real dictionary
        // is never empty.
        if (dict_rows < 1u)
            panic("columnar: LowCardinality dictionary is empty");
        if (dict.offsets_offset > total_bytes ||
            (uint64_t(dict_rows) + 1u) * sizeof(uint64_t) > total_bytes - dict.offsets_offset)
            panic("columnar: LowCardinality dictionary offsets exceed frame");
        if (dict.data_offset > total_bytes ||
            dict.data_size > total_bytes - dict.data_offset)
            panic("columnar: LowCardinality dictionary data exceeds frame");

        // offsets_offset is the shared index array.
        v.lc_index        = base + d.offsets_offset;
        v.lc_index_width  = w;
        v.lc_dict_rows    = dict_rows;
        v.lc_dict         = dict;
    }
};

inline ColumnarBuf parse_columnar(const raw_buffer* buf) {
    const uint8_t* p = buf->data();
    const uint64_t total = buf->size();
    if (total < HEADER_BYTES)
        panic("columnar: frame shorter than header");

    // The host's readFrameHeader validates magic, version, and the reserved
    // field for the same reason: without it a layout change or a foreign byte
    // range is parsed as an arbitrary frame and produces arbitrary answers.
    uint32_t magic;
    uint16_t version, reserved;
    std::memcpy(&magic, p, 4);
    std::memcpy(&version, p + 4, 2);
    std::memcpy(&reserved, p + 6, 2);
    if (magic != FRAME_MAGIC)
        panic("columnar: bad frame magic");
    if (version != FRAME_VERSION)
        panic("columnar: unsupported frame version");
    if (reserved != 0)
        panic("columnar: reserved frame header field is non-zero");

    ColumnarBuf cb;
    cb.base = p;
    cb.total_bytes = total;
    std::memcpy(&cb.num_rows, p + 8,  4);
    std::memcpy(&cb.num_cols, p + 12, 4);
    if (cb.num_cols > (total - HEADER_BYTES) / COL_DESC_BYTES)
        panic("columnar: descriptor table extends past end of frame");
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

    write_frame_header(p, num_rows, 1);

    ColDescriptor d{};
    d.type         = col_type;
    d.data_offset  = HEADER_BYTES + COL_DESC_BYTES;
    d.data_size    = num_rows * static_cast<uint32_t>(sizeof(T));
    std::memcpy(p + HEADER_BYTES, &d, sizeof(d));
}

// Streaming writer for a single variable-length (bytes) column output.
// Layout: [BufHeader][ColDescriptor][null_map: u8[N]][offsets: u64[N+1]][data...]
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
        offs_base = (after_null + 7u) & ~7u;   // align to 8
        data_base = offs_base + (n + 1u) * 8u;

        out->resize(data_base);
        uint8_t* p = out->data();

        write_frame_header(p, n, 1);

        ColDescriptor d{};
        d.type           = COL_BYTES | (is_nullable ? COL_IS_NULLABLE : 0u);
        d.null_offset    = is_nullable ? null_base : 0u;
        d.offsets_offset = offs_base;
        d.data_offset    = data_base;
        d.data_size      = 0;
        std::memcpy(p + HEADER_BYTES, &d, sizeof(d));

        const uint64_t zero = 0;
        std::memcpy(p + offs_base, &zero, 8);  // offsets[0] = 0
    }

    void push_null() {
        uint32_t i = rows_written++;
        uint64_t prev;
        std::memcpy(&prev, out->data() + offs_base + i * 8u, 8);
        uint8_t* p = out->data();
        if (nullable) p[null_base + i] = 1;
        std::memcpy(p + offs_base + (i + 1u) * 8u, &prev, 8);
    }

    void push_bytes(std::span<const uint8_t> bytes) {
        uint32_t i = rows_written++;
        uint64_t len = static_cast<uint64_t>(bytes.size());
        uint64_t prev;
        std::memcpy(&prev, out->data() + offs_base + i * 8u, 8);
        uint64_t next = prev + len;
        out->append(bytes.data(), static_cast<uint32_t>(len));
        uint8_t* p = out->data();
        if (nullable) p[null_base + i] = 0;
        std::memcpy(p + offs_base + (i + 1u) * 8u, &next, 8);
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
        // Two-pass: offsets[n+1] + bytes (no null terminators)
        std::vector<std::string> strs(n);
        for (uint32_t i = 0; i < n; ++i) strs[i] = get_val(i);
        std::vector<uint64_t> offs(n + 1u);
        offs[0] = 0u;
        for (uint32_t i = 0; i < n; ++i)
            offs[i + 1u] = offs[i] + static_cast<uint64_t>(strs[i].size());
        out->append(reinterpret_cast<const uint8_t*>(offs.data()), (n + 1u) * 8u);
        for (uint32_t i = 0; i < n; ++i)
            out->append(reinterpret_cast<const uint8_t*>(strs[i].data()),
                        static_cast<uint32_t>(strs[i].size()));
    } else if constexpr (std::is_same_v<T, std::unique_ptr<geos::geom::Geometry>>) {
        // Two-pass: collect WKBs, then offsets + bytes (no null terminator)
        std::vector<std::vector<uint8_t>> wkbs(n);
        for (uint32_t i = 0; i < n; ++i) {
            auto g = get_val(i);
            if (g) wkbs[i] = write_ewkb(g);
        }
        std::vector<uint64_t> offs(n + 1u);
        offs[0] = 0u;
        for (uint32_t i = 0; i < n; ++i)
            offs[i + 1u] = offs[i] + static_cast<uint64_t>(wkbs[i].size());
        out->append(reinterpret_cast<const uint8_t*>(offs.data()), (n + 1u) * 8u);
        for (uint32_t i = 0; i < n; ++i)
            out->append(wkbs[i].data(), static_cast<uint32_t>(wkbs[i].size()));
    } else if constexpr (is_vector_v<T>) {
        using ElemT = typename T::value_type;
        // Collect all rows, write outer offsets, flatten elements, recurse.
        std::vector<T> rows(n);
        for (uint32_t i = 0; i < n; ++i) rows[i] = get_val(i);
        std::vector<uint64_t> outer_offs(n + 1u);
        outer_offs[0] = 0u;
        for (uint32_t i = 0; i < n; ++i)
            outer_offs[i + 1u] = outer_offs[i] + static_cast<uint64_t>(rows[i].size());
        uint32_t M = static_cast<uint32_t>(outer_offs[n]);
        out->append(reinterpret_cast<const uint8_t*>(outer_offs.data()), (n + 1u) * 8u);
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
    write_frame_header(p, n, 1);
    ColDescriptor d{};
    d.type = static_cast<uint32_t>(COL_COMPLEX);
    if constexpr (is_vector_v<Ret>) {
        // Array: outer uint64[n+1] offsets at data_offset, nested data follows immediately.
        d.offsets_offset = 0;
        d.data_offset    = HEADER_BYTES + COL_DESC_BYTES;
    } else {
        // Tuple/pair/scalar: no outer offsets, data starts immediately.
        d.offsets_offset = 0u;
        d.data_offset    = HEADER_BYTES + COL_DESC_BYTES;
    }
    std::memcpy(p + HEADER_BYTES, &d, sizeof(d));

    std::vector<Ret> vals(n);
    for (uint32_t i = 0; i < n; ++i) vals[i] = get_val(i);
    write_complex_data<Ret>(out, n,
        [&](uint32_t i) -> const Ret& { return vals[i]; });

    // Patch data_size (at byte offset 32 within ColDescriptor = HEADER_BYTES+32 in buf)
    uint64_t data_size = static_cast<uint64_t>(out->size() - (HEADER_BYTES + COL_DESC_BYTES));
    std::memcpy(out->data() + HEADER_BYTES + 32u, &data_size, 8u);
    return out;
}

// ── COL_COMPLEX array reader ──────────────────────────────────────────────────
// Reads one row of an Array(T) COL_COMPLEX column.
// Wire layout (see COL_COMPLEX comment in ColType):
//   col.offsets → uint64[row_count+1] outer offsets (cumulative element counts)
//   col.data    → element data:
//     Array(String/WKB): uint64[M_total+1] inner_offsets + bytes (null-terminated)
//     Array(arithmetic): ElemT[M_total] packed

template <typename ElemT>
std::vector<ElemT> col_get_complex_array(const ColView& col, uint32_t row) {
    uint32_t idx                = col.effective_row(row);
    const uint64_t* outer_offs  = reinterpret_cast<const uint64_t*>(col.data);
    uint64_t outer_start        = outer_offs[idx];
    uint64_t outer_end          = outer_offs[idx + 1];
    uint64_t M_total            = outer_offs[col.row_count];
    uint64_t count              = outer_end - outer_start;
    const uint8_t* inner_data   = col.data + (col.row_count + 1u) * sizeof(uint64_t);

    std::vector<ElemT> result;
    result.reserve(count);

    if constexpr (std::is_same_v<ElemT, std::span<const uint8_t>> ||
                  std::is_same_v<ElemT, std::unique_ptr<geos::geom::Geometry>>) {
        // Array(String / WKB): inner_data = [uint64[M_total+1] inner_offs][bytes]
        const uint64_t* inner_offs = reinterpret_cast<const uint64_t*>(inner_data);
        const uint8_t*  chars      = inner_data + (M_total + 1u) * sizeof(uint64_t);
        for (uint64_t j = outer_start; j < outer_end; ++j) {
            uint64_t s   = inner_offs[j];
            uint64_t e   = inner_offs[j + 1];
            uint64_t len = e - s;
            std::span<const uint8_t> sp{chars + s, static_cast<size_t>(len)};
            if constexpr (std::is_same_v<ElemT, std::span<const uint8_t>>)
                result.push_back(sp);
            else
                result.push_back(read_wkb(sp));
        }
    } else if constexpr (std::is_arithmetic_v<ElemT>) {
        // Array(numeric): inner_data = ElemT[M_total] packed
        const ElemT* data_ptr = reinterpret_cast<const ElemT*>(inner_data);
        for (uint32_t j = outer_start; j < outer_end; ++j)
            result.push_back(data_ptr[j]);
    } else if constexpr (std::is_same_v<ElemT, std::string>) {
        const uint64_t* inner_offs = reinterpret_cast<const uint64_t*>(inner_data);
        const uint8_t*  chars      = inner_data + (M_total + 1u) * sizeof(uint64_t);
        for (uint64_t j = outer_start; j < outer_end; ++j) {
            uint64_t s   = inner_offs[j];
            uint64_t e   = inner_offs[j + 1];
            uint64_t len = e - s;
            result.push_back(std::string(reinterpret_cast<const char*>(chars + s), len));
        }
    }
    return result;
}

// ── COL_VARIANT geometry decoder ─────────────────────────────────────────────
//
// Geo-discriminator constants (CH DataTypeCustomGeo.cpp alphabetical sort order):
//   0=LineString, 1=MultiLineString, 2=MultiPolygon, 3=Point, 4=Polygon, 5=Ring
//
// For each sub-column:
//   inner.null_offset = M (sub_row_count, stored by CH serializer)
//   inner.offsets_offset = 0 (unused; Array outer offsets are at inner.data_offset)
//   inner.data_offset    = absolute position of element data
//
// Sub-column layouts (COL_COMPLEX recursive):
//   Point            : Tuple → x[M] || y[M]
//   LineString, Ring : Array(Tuple) → outer_offs[M+1] + x[V] || y[V]
//   Polygon          : Array(Array(Tuple)) → ring_offs[M+1] + vert_offs[R+1] + x[V] || y[V]
//   MultiPolygon     : Array(Array(Array(Tuple))) → poly_offs[M+1] + ring_offs[P+1] + vert_offs[R+1] + x[V] || y[V]
//   MultiLineString  : Array(Array(Tuple)) — same wire layout as Polygon

// Build a CoordinateSequence from x[V]||y[V] arrays, vertex range [vs, ve).
inline std::unique_ptr<geos::geom::CoordinateSequence>
make_cs(const double* x, const double* y, uint32_t vs, uint32_t ve) {
    auto cs = std::make_unique<geos::geom::CoordinateSequence>();
    cs->reserve(ve - vs);
    for (uint32_t j = vs; j < ve; ++j)
        cs->add(geos::geom::CoordinateXY{x[j], y[j]});
    return cs;
}

inline std::unique_ptr<geos::geom::Geometry>
col_get_variant_geom(const ColView& col, uint32_t row) {
    using namespace geos::geom;
    const GeometryFactory* factory = GeometryFactory::getDefaultInstance();

    uint32_t eff = col.effective_row(row);

    // null_map holds the discriminators array for COL_VARIANT (0xFF = NULL).
    if (!col.null_map) return nullptr;
    const uint8_t disc = col.null_map[eff];
    if (disc == 0xFFu) return nullptr;

    // Row offset within the sub-column for this row.
    const uint64_t off = col.offsets[eff];

    // Parse variant header at col.data: uint32 K + K×{disc(1)+pad(3)+ColDescriptor(20)}.
    const uint8_t* hdr = col.data;
    uint32_t k;
    std::memcpy(&k, hdr, 4);

    // Find the record matching this discriminator.
    ColDescriptor inner{};
    bool found = false;
    const uint8_t* rp = hdr + 4u;
    for (uint32_t ri = 0; ri < k; ++ri, rp += 4u + COL_DESC_BYTES) {
        if (*rp == disc) {
            std::memcpy(&inner, rp + 4u, COL_DESC_BYTES);
            found = true;
            break;
        }
    }
    if (!found) return nullptr;

    const uint32_t M = static_cast<uint32_t>(inner.null_offset);  // sub_row_count stored by CH serializer

    // CH global discriminator order is alphabetical by type name:
    // 0=LineString, 1=MultiLineString, 2=MultiPolygon, 3=Point, 4=Polygon, 5=Ring
    switch (disc) {
        case 3: {  // Point: Tuple(Float64, Float64) → x[M] || y[M]
            const auto* x = reinterpret_cast<const double*>(col.base + inner.data_offset);
            const auto* y = x + M;
            return factory->createPoint(Coordinate{x[off], y[off]});
        }
        case 0:    // LineString: Array(Tuple(Float64, Float64))
        case 5: {  // Ring: same wire format, different geometry type
            const auto* lo   = reinterpret_cast<const uint64_t*>(col.base + inner.data_offset);
            const uint32_t V = static_cast<uint32_t>(lo[M]);
            const auto* x    = reinterpret_cast<const double*>(col.base + inner.data_offset + (M + 1u) * 8u);
            const auto* y    = x + V;
            auto cs = make_cs(x, y, lo[off], lo[off + 1]);
            if (disc == 5)
                return factory->createLinearRing(std::move(cs));
            return factory->createLineString(std::move(cs));
        }
        case 4: {  // Polygon: Array(Array(Tuple)) — first ring=exterior, rest=holes
            const auto* ring_lo  = reinterpret_cast<const uint64_t*>(col.base + inner.data_offset);
            const uint32_t R     = static_cast<uint32_t>(ring_lo[M]);
            const auto* vert_lo  = reinterpret_cast<const uint64_t*>(col.base + inner.data_offset + (M + 1u) * 8u);
            const uint32_t V     = static_cast<uint32_t>(vert_lo[R]);
            const auto* x        = reinterpret_cast<const double*>(col.base + inner.data_offset + (M + 1u) * 8u + (R + 1u) * 8u);
            const auto* y        = x + V;
            const uint32_t rs    = ring_lo[off];
            const uint32_t re    = ring_lo[off + 1];
            auto exterior = factory->createLinearRing(make_cs(x, y, vert_lo[rs], vert_lo[rs + 1]));
            std::vector<std::unique_ptr<LinearRing>> holes;
            holes.reserve(re - rs - 1u);
            for (uint32_t r = rs + 1u; r < re; ++r)
                holes.push_back(factory->createLinearRing(make_cs(x, y, vert_lo[r], vert_lo[r + 1])));
            return factory->createPolygon(std::move(exterior), std::move(holes));
        }
        case 2: {  // MultiPolygon: Array(Array(Array(Tuple)))
            const auto* poly_lo  = reinterpret_cast<const uint64_t*>(col.base + inner.data_offset);
            const uint32_t P     = static_cast<uint32_t>(poly_lo[M]);
            const auto* ring_lo  = reinterpret_cast<const uint64_t*>(col.base + inner.data_offset + (M + 1u) * 8u);
            const uint32_t R     = static_cast<uint32_t>(ring_lo[P]);
            const auto* vert_lo  = reinterpret_cast<const uint64_t*>(col.base + inner.data_offset + (M + 1u) * 8u + (P + 1u) * 8u);
            const uint32_t V     = static_cast<uint32_t>(vert_lo[R]);
            const auto* x        = reinterpret_cast<const double*>(col.base + inner.data_offset + (M + 1u) * 8u + (P + 1u) * 8u + (R + 1u) * 8u);
            const auto* y        = x + V;
            const uint32_t ps    = poly_lo[off];
            const uint32_t pe    = poly_lo[off + 1];
            std::vector<std::unique_ptr<Polygon>> polys;
            polys.reserve(pe - ps);
            for (uint32_t p = ps; p < pe; ++p) {
                const uint32_t rs = ring_lo[p];
                const uint32_t re = ring_lo[p + 1];
                auto ext = factory->createLinearRing(make_cs(x, y, vert_lo[rs], vert_lo[rs + 1]));
                std::vector<std::unique_ptr<LinearRing>> holes;
                holes.reserve(re - rs - 1u);
                for (uint32_t r = rs + 1u; r < re; ++r)
                    holes.push_back(factory->createLinearRing(make_cs(x, y, vert_lo[r], vert_lo[r + 1])));
                polys.push_back(factory->createPolygon(std::move(ext), std::move(holes)));
            }
            return factory->createMultiPolygon(std::move(polys));
        }
        case 1: {  // MultiLineString: Array(Array(Tuple)) — same wire layout as Polygon
            const auto* line_lo  = reinterpret_cast<const uint64_t*>(col.base + inner.data_offset);
            const uint32_t R     = static_cast<uint32_t>(line_lo[M]);
            const auto* vert_lo  = reinterpret_cast<const uint64_t*>(col.base + inner.data_offset + (M + 1u) * 8u);
            const uint32_t V     = static_cast<uint32_t>(vert_lo[R]);
            const auto* x        = reinterpret_cast<const double*>(col.base + inner.data_offset + (M + 1u) * 8u + (R + 1u) * 8u);
            const auto* y        = x + V;
            const uint32_t ls_s  = line_lo[off];
            const uint32_t ls_e  = line_lo[off + 1];
            std::vector<std::unique_ptr<LineString>> lines;
            lines.reserve(ls_e - ls_s);
            for (uint32_t r = ls_s; r < ls_e; ++r)
                lines.push_back(factory->createLineString(make_cs(x, y, vert_lo[r], vert_lo[r + 1])));
            return factory->createMultiLineString(std::move(lines));
        }
        default:
            return nullptr;
    }
}

// ── Input column accessor by type ─────────────────────────────────────────────

// Read a fixed-width column value as type T, widening from narrower stored types.
// CH passes integer literals as the smallest fitting type (e.g. UInt8 for `2`),
// but the _impl function may declare a wider type (e.g. int32_t).  We check the
// actual stored ColType and widen via static_cast.  For floating-point targets,
// bytes are bit-cast directly (no numeric cast across float/int boundary).
template <typename T>
T col_get_fixed_widened(const ColView& col, uint32_t row) {
    uint32_t idx = col.effective_row(row);
    switch (col.base_type) {
        case COL_FIXED8: {
            uint8_t v; std::memcpy(&v, col.data + idx, 1);
            return static_cast<T>(v);
        }
        case COL_FIXED16: {
            int16_t v; std::memcpy(&v, col.data + idx * 2u, 2u);
            return static_cast<T>(v);
        }
        case COL_FIXED32: {
            uint32_t v; std::memcpy(&v, col.data + idx * 4u, 4u);
            return static_cast<T>(v);
        }
        case COL_FIXED64: {
            // Read at the stored width first: memcpy'ing sizeof(T) from a
            // stride-8 array mis-reads every row once T is narrower than 8.
            uint64_t raw;
            std::memcpy(&raw, col.data + uint64_t(idx) * 8u, 8u);
            if constexpr (std::is_floating_point_v<T>) {
                T v; std::memcpy(&v, &raw, 8u);   // Float64: bit-cast
                return v;
            } else {
                return static_cast<T>(raw);        // numeric widening/truncation
            }
        }
        // COL_FIXEDN: any width; take the interpretation from T and let the
        // wire width veto a mismatch (it never carries signedness or type).
        case COL_FIXEDN: {
            if (col.fixed_width != uint32_t(sizeof(T)))
                panic("columnar: COL_FIXEDN width does not match the requested type");
            T v; std::memcpy(&v, col.data + uint64_t(idx) * sizeof(T), sizeof(T));
            return v;
        }
        default: {
            // Any non fixed-width tag (bytes/complex/variant/lowcard) is a
            // hard error — the old default: memcpy'd sizeof(T) bytes from
            // whatever `data` pointed at.
            panic("columnar: col_get_fixed_widened called on non fixed-width column");
        }
    }
}

// Span-shaped argument (the FixedString/bytes shape chgeos declares):
// dispatch by tag, validating the width rather than trusting it.  Interpretation
// comes from the C++ side; the tag never carries logical type.
inline std::span<const uint8_t> col_get_span_arg(const ColView& col, uint32_t row) {
    switch (col.base_type) {
        case COL_BYTES:
            return col.get_bytes(row);
        case COL_FIXED8:
        case COL_FIXED16:
        case COL_FIXED32:
        case COL_FIXED64:
        case COL_FIXEDN:
            return col.get_fixed_bytes(row);
        case COL_LOWCARD:
            return col.lc_get_bytes(row);
        default:
            panic("columnar: span argument against an unsupported column tag");
    }
}

template <typename T>
T col_get_arg(const ColView& col, uint32_t row) {
    if constexpr (is_vector_v<T>) {
        return col_get_complex_array<typename T::value_type>(col, row);
    } else if constexpr (std::is_same_v<T, std::span<const uint8_t>>) {
        return col_get_span_arg(col, row);
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
        if (col.base_type == COL_VARIANT)
            return col_get_variant_geom(col, row);
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
                                  ColPrepPointOp prep_b_point = nullptr,  // B-const polygon, A varies as points
                                  ColWkbScalarOp wkb_scalar   = nullptr)  // 1-arg accessor read straight from WKB
{
    using PGF = geos::geom::prep::PreparedGeometryFactory;

    raw_buffer* out = nullptr;
    try {
    // Parsing lives inside the try: a malformed frame must trap through
    // panic() like every other guest-side failure, not unwind through the
    // WASM boundary.
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
        // ── bool output (predicates) ──────────────────────────────────────────
        if constexpr (std::is_same_v<Ret, bool>) {
            out = clickhouse_create_buffer(HEADER_BYTES + COL_DESC_BYTES + n);
            col_write_fixed_header<uint8_t>(out, n, COL_FIXED8);
            uint8_t* res = out->data() + HEADER_BYTES + COL_DESC_BYTES;

            // COL_VARIANT columns can't be read via get_bytes(); all fast paths that
            // call get_bytes() or wkb_bbox() must be skipped when any arg is a Variant.
            bool has_variant = false;
            for (size_t j = 0; j < nargs; ++j)
                if (cols[j].base_type == COL_VARIANT) { has_variant = true; break; }

            // The point fast paths below read coordinates at fixed WKB offsets, so
            // every row of the varying column must be a plain 2D point. Sampling
            // one row is not enough: an SRID-carrying or 3D point anywhere in the
            // batch would be decoded as garbage, and which row comes first depends
            // on how the caller chunked its input.
            auto all_2d_points = [](const auto & col, uint32_t rows) {
                for (uint32_t i = 0; i < rows; ++i) {
                    if (col.is_null(i)) continue;
                    auto s = col.get_bytes(i);
                    if (s.size() != 21 || s[0] != 0x01) return false;
                    uint32_t t = 0;
                    memcpy(&t, s.data() + 1, 4);
                    if (t != 1u) return false;
                }
                return true;
            };

            if constexpr (nargs >= 2) {
                // A-const fast path: prepare col(0) once, vary col(1)
                if (!has_variant && cols[0].is_effectively_const_bytes() && prep_a) {
                    if (cols[0].is_null(0)) { std::fill(res, res + n, 0u); return out; }
                    auto span_a = cols[0].get_bytes(0);
                    BBox  bbox_a = wkb_bbox(span_a);
                    auto  geom_a = read_wkb(span_a);

                    // Point fast path: col(1) contains 2D WKB points — no per-row GEOS alloc.
                    if (prep_a_point && n > 0) {
                        auto gtype = geom_a->getGeometryTypeId();
                        if ((gtype == geos::geom::GEOS_POLYGON
                          || gtype == geos::geom::GEOS_MULTIPOLYGON)
                            && all_2d_points(cols[1], n)) {
                            using IPIAL = geos::algorithm::locate::IndexedPointInAreaLocator;
                            // Building the segment index costs O(edges); below the
                            // break-even batch size a direct edge scan is cheaper.
                            std::optional<IPIAL> locator;
                            if (n >= INDEXED_LOCATOR_MIN_ROWS) locator.emplace(*geom_a);
                            for (uint32_t i = 0; i < n; ++i) {
                                if (cols[1].is_null(i)) { res[i] = 0u; continue; }
                                auto span_b = cols[1].get_bytes(i);
                                double px, py;
                                memcpy(&px, span_b.data() + 5, 8);
                                memcpy(&py, span_b.data() + 13, 8);
                                if (bbox_op && !bbox_op(bbox_a, BBox{px, py, px, py})) {
                                    res[i] = early_ret ? 1u : 0u; continue;
                                }
                                geos::geom::CoordinateXY c{px, py};
                                auto loc = locator
                                    ? locator->locate(&c)
                                    : geos::algorithm::locate::SimplePointInAreaLocator::locate(
                                          c, geom_a.get());
                                res[i] = prep_a_point(loc) ? 1u : 0u;
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
                if (!has_variant && cols[1].is_effectively_const_bytes() && prep_b) {
                    if (cols[1].is_null(0)) { std::fill(res, res + n, 0u); return out; }
                    auto span_b = cols[1].get_bytes(0);
                    BBox  bbox_b = wkb_bbox(span_b);
                    auto  geom_b = read_wkb(span_b);

                    // Point fast path: col(0) contains 2D WKB points — no per-row GEOS alloc.
                    if (prep_b_point && n > 0) {
                        auto gtype = geom_b->getGeometryTypeId();
                        if ((gtype == geos::geom::GEOS_POLYGON
                          || gtype == geos::geom::GEOS_MULTIPOLYGON)
                            && all_2d_points(cols[0], n)) {
                            using IPIAL = geos::algorithm::locate::IndexedPointInAreaLocator;
                            // Building the segment index costs O(edges); below the
                            // break-even batch size a direct edge scan is cheaper.
                            std::optional<IPIAL> locator;
                            if (n >= INDEXED_LOCATOR_MIN_ROWS) locator.emplace(*geom_b);
                            for (uint32_t i = 0; i < n; ++i) {
                                if (cols[0].is_null(i)) { res[i] = 0u; continue; }
                                auto span_a = cols[0].get_bytes(i);
                                double px, py;
                                memcpy(&px, span_a.data() + 5, 8);
                                memcpy(&py, span_a.data() + 13, 8);
                                if (bbox_op && !bbox_op(BBox{px, py, px, py}, bbox_b)) {
                                    res[i] = early_ret ? 1u : 0u; continue;
                                }
                                geos::geom::CoordinateXY c{px, py};
                                auto loc = locator
                                    ? locator->locate(&c)
                                    : geos::algorithm::locate::SimplePointInAreaLocator::locate(
                                          c, geom_b.get());
                                res[i] = prep_b_point(loc) ? 1u : 0u;
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
                if (!has_variant && cols[0].is_effectively_const_bytes() && prep_a_dist) {
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
                if (!has_variant && cols[1].is_effectively_const_bytes() && prep_b_dist) {
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
                    if (bbox_op && !has_variant &&
                        !bbox_op(wkb_bbox(cols[0].get_bytes(i)),
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

            // WKB fast path: the answer is a fixed-offset read out of the row's
            // bytes, so no GEOS geometry is built.  A COL_VARIANT column holds
            // no WKB at all and keeps the GEOS path, as does any single row the
            // op declines — the op only ever claims cases it is sure of.
            if constexpr (nargs == 1) {
                if (wkb_scalar && cols[0].base_type != COL_VARIANT) {
                    for (uint32_t i = 0; i < n; ++i) {
                        if (cols[0].is_null(i)) {
                            res[i] = std::numeric_limits<double>::quiet_NaN();
                            continue;
                        }
                        std::optional<double> v = wkb_scalar(cols[0].get_bytes(i));
                        res[i] = v ? *v : invoke(i);
                    }
                    return out;
                }
            }

            for (uint32_t i = 0; i < n; ++i) {
                res[i] = any_null(i) ? std::numeric_limits<double>::quiet_NaN() : invoke(i);
            }
            return out;

        // ── int32_t output ────────────────────────────────────────────────────
        } else if constexpr (std::is_same_v<Ret, int32_t>) {
            out = clickhouse_create_buffer(HEADER_BYTES + COL_DESC_BYTES + n * 4u);
            col_write_fixed_header<int32_t>(out, n, COL_FIXED32);
            int32_t* res = reinterpret_cast<int32_t*>(out->data() + HEADER_BYTES + COL_DESC_BYTES);
            for (uint32_t i = 0; i < n; ++i) {
                res[i] = any_null(i) ? 0 : invoke(i);
            }
            return out;

        // ── uint32_t output ───────────────────────────────────────────────────
        } else if constexpr (std::is_same_v<Ret, uint32_t>) {
            out = clickhouse_create_buffer(HEADER_BYTES + COL_DESC_BYTES + n * 4u);
            col_write_fixed_header<uint32_t>(out, n, COL_FIXED32);
            uint32_t* res = reinterpret_cast<uint32_t*>(out->data() + HEADER_BYTES + COL_DESC_BYTES);
            for (uint32_t i = 0; i < n; ++i) {
                res[i] = any_null(i) ? 0u : invoke(i);
            }
            return out;

        // ── Geometry output (non-nullable — use std::optional<...> for nullable) ─
        } else if constexpr (std::is_same_v<Ret, std::unique_ptr<geos::geom::Geometry>>) {
            out = clickhouse_create_buffer(0);
            ColBytesWriter w(out, n, /*nullable=*/false);
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

// ── st_knn_col: k-nearest-neighbour (COLUMNAR_V1) ─────────────────────────────
// Signature: st_knn(query String, candidates Array(String), k UInt32)
//            → Array(Tuple(UInt64, Float64))

__attribute__((export_name("st_knn")))
inline ch::raw_buffer* st_knn_col(ch::raw_buffer* ptr, uint32_t)
{
    using KVPair   = std::pair<uint64_t, double>;
    using KNNResult = std::vector<KVPair>;

    ch::raw_buffer* out = nullptr;
    try {
    auto cb = ch::parse_columnar(ptr);
    uint32_t n     = cb.num_rows;
    ch::ColView col_q = cb.col(0);
    ch::ColView col_c = cb.col(1);
    ch::ColView col_k = cb.col(2);

    uint32_t k = ch::col_get_fixed_widened<uint32_t>(col_k, 0);

    if (k == 0 || n == 0)
        return ch::write_complex_col<KNNResult>(n, [](uint32_t) -> KNNResult { return {}; });
        if (col_c.is_const) {
            auto wkbs = ch::col_get_complex_array<std::span<const uint8_t>>(col_c, 0);
            ch::CentroidKNNIndex index(wkbs);
            return ch::write_complex_col<KNNResult>(n, [&](uint32_t row) -> KNNResult {
                if (col_q.is_null(row)) return {};
                return index.query(col_q.get_bytes(row), k);
            });
        } else {
            return ch::write_complex_col<KNNResult>(n, [&](uint32_t row) -> KNNResult {
                if (col_q.is_null(row)) return {};
                auto q   = ch::read_wkb(col_q.get_bytes(row));
                auto cands = ch::col_get_complex_array<std::span<const uint8_t>>(col_c, row);
                return ch::st_knn_brute(q.get(), cands, k);
            });
        }
    } catch (const std::exception& e) {
        if (out) clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(out));
        ch::panic(e.what());
    }
    __builtin_unreachable();
}

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

// 1-arg accessor returning double, with a ColWkbScalarOp fast path that reads
// the answer out of the WKB.  Requires name##_wkb defined alongside name##_impl.
#define CH_UDF_COL_WKB1(name)                                                    \
    __attribute__((export_name(#name)))                                          \
    ch::raw_buffer * name(ch::raw_buffer * ptr, uint32_t num_rows) {             \
        return ch::columnar_impl_wrapper(ptr, num_rows, ch::name##_impl,         \
            nullptr, false, nullptr, nullptr, nullptr, nullptr,                  \
            nullptr, nullptr, ch::name##_wkb);                                   \
    }

// Canonical no-suffix alias for PRED3 functions that keep their _col export.
#define CH_UDF_CANONICAL(name)                                                   \
    __attribute__((export_name(#name)))                                          \
    ch::raw_buffer * name(ch::raw_buffer * ptr, uint32_t num_rows) {             \
        return name##_col(ptr, num_rows);                                        \
    }
