#pragma once

// Buffers wire format (Native serialization) for ClickHouse WASM UDFs.
//
// Wire layout:
//   num_rows (varint) | num_cols (varint)
//   For each column:
//     buffer_size (varint) | column_data (Native format binary)
//
// Native serialization properties:
//   Fixed-width (UInt8-64, Int8-64, Float32/64): raw bytes, no prefix
//   String: varint(len) + bytes + null terminator

#include <algorithm>
#include <array>
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

#include "clickhouse.hpp"
#include "col_prep_op.hpp"
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

// ── BuffersBuf ───────────────────────────────────────────────────────────────

struct BuffersBuf {
    uint32_t num_rows;
    uint32_t num_cols;
    struct ColInfo {
        uint64_t buffer_size;
        const uint8_t* data;
    };
    std::vector<ColInfo> cols;
};

// ── BuffersColView ─────────────────────────────────────────────────────────

struct BuffersColView {
    const uint8_t* data;
    uint64_t buffer_size;
    uint32_t row_count;

    // Fixed-width: memcpy sizeof(T) per row.
    template <typename T>
    T get_fixed(uint32_t row) const noexcept {
        T v;
        std::memcpy(&v, data + row * sizeof(T), sizeof(T));
        return v;
    }

    // String: varint(len) + bytes + null terminator per row.
    std::span<const uint8_t> get_bytes(uint32_t row) const noexcept;

    // True when every row has identical bytes. Used to trigger PreparedGeometry.
    bool is_effectively_const() const noexcept;
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

// Write a varint to a raw_buffer — minimal implementation matching CH's IO/VarInt.h.
static inline void writeVarUInt(uint64_t n, raw_buffer& buf) {
    while (n >= 0x80) {
        buf.push_back(static_cast<uint8_t>((n & 0x7F) | 0x80));
        n >>= 7;
    }
    buf.push_back(static_cast<uint8_t>(n));
}

// Write an 8-byte little-endian value (matches ClickHouse Buffers format).
static inline void writeBinaryLE64(uint64_t n, raw_buffer& buf) {
    for (uint32_t i = 0; i < 8; ++i)
        buf.push_back(static_cast<uint8_t>(n >> (i * 8)));
}

// Read an 8-byte little-endian value from a buffer.
static inline bool readBinaryLE64(const uint8_t*& p, const uint8_t* end, uint64_t& out) {
    if (p + 8 > end) return false;
    out = 0;
    for (int i = 0; i < 8; ++i)
        out |= static_cast<uint64_t>(p[i]) << (i * 8);
    p += 8;
    return true;
}

// ── parse_buffers ───────────────────────────────────────────────────────────

inline BuffersBuf parse_buffers(const raw_buffer* buf) {
    BuffersBuf bb;
    const uint8_t* p = buf->data();
    const uint8_t* end = p + buf->size();
    uint64_t v;
    if (!readBinaryLE64(p, end, v)) return bb; bb.num_cols = static_cast<uint32_t>(v);
    if (!readBinaryLE64(p, end, v)) return bb; bb.num_rows = static_cast<uint32_t>(v);
    for (uint32_t i = 0; i < bb.num_cols; ++i) {
        BuffersBuf::ColInfo ci;
        if (!readBinaryLE64(p, end, v)) return bb; ci.buffer_size = static_cast<uint32_t>(v);
        ci.data = p;
        bb.cols.push_back(ci);
        p = ci.data + ci.buffer_size;
    }
    return bb;
}

// ── BuffersColView methods ─────────────────────────────────────────────────

inline std::span<const uint8_t> BuffersColView::get_bytes(uint32_t row) const noexcept {
    const uint8_t* p = data;
    const uint8_t* end = data + buffer_size;
    for (uint32_t i = 0; i < row; ++i) {
        uint64_t len;
        if (!readVarUInt(p, end, len)) return {data, 0};
        uint32_t varint_len = getLengthOfVarUInt(static_cast<uint32_t>(len));
        p += varint_len + static_cast<uint32_t>(len);
    }
    uint64_t len;
    if (!readVarUInt(p, end, len)) return {p, 0};
    uint32_t varint_len = getLengthOfVarUInt(static_cast<uint32_t>(len));
    return {p + varint_len, static_cast<uint32_t>(len)};
}

inline bool BuffersColView::is_effectively_const() const noexcept {
    if (row_count <= 1) return true;
    const uint8_t* p = data;
    const uint8_t* end = data + buffer_size;
    uint64_t first_len;
    if (!readVarUInt(p, end, first_len)) return false;
    uint32_t first_varint_len = getLengthOfVarUInt(static_cast<uint32_t>(first_len));
    uint32_t first_stride = first_varint_len + static_cast<uint32_t>(first_len);
    if (first_stride == 0 || first_len > 1000000) return false;
    const uint8_t* first_content = data + first_varint_len;
    p += first_stride;
    for (uint32_t i = 1; i < row_count; ++i) {
        uint64_t len;
        if (!readVarUInt(p, end, len)) return false;
        uint32_t varint_len = getLengthOfVarUInt(static_cast<uint32_t>(len));
        uint32_t stride = varint_len + static_cast<uint32_t>(len);
        if (stride != first_stride) return false;
        if (std::memcmp(p + varint_len, first_content, first_len) != 0) return false;
        p += stride;
    }
    return true;
}

// ── pack_result: write a single result value into a raw_buffer ──────────────

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

static inline void pack_result(raw_buffer& buf, std::unique_ptr<geos::geom::Geometry> g) {
    if (!g) {
        writeVarUInt(0u, buf);
        return;
    }
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

// ── arg getters for BuffersColView ──────────────────────────────────────────

template <typename T>
T buf_get_arg(const BuffersColView& col, uint32_t row) {
    if constexpr (std::is_same_v<T, std::span<const uint8_t>>) {
        return col.get_bytes(row);
    } else if constexpr (std::is_same_v<T, double>) {
        T v;
        std::memcpy(&v, col.data + row * sizeof(T), sizeof(T));
        return v;
    } else if constexpr (std::is_same_v<T, int32_t>) {
        T v;
        std::memcpy(&v, col.data + row * sizeof(T), sizeof(T));
        return v;
    } else if constexpr (std::is_same_v<T, uint32_t>) {
        T v;
        std::memcpy(&v, col.data + row * sizeof(T), sizeof(T));
        return v;
    } else if constexpr (std::is_same_v<T, std::string_view>) {
        auto s = col.get_bytes(row);
        return {reinterpret_cast<const char*>(s.data()), s.size()};
    } else if constexpr (std::is_same_v<T, std::unique_ptr<geos::geom::Geometry>>) {
        auto span = col.get_bytes(row);
        return read_wkb(span);
    } else {
        T v;
        std::memcpy(&v, col.data + row * sizeof(T), sizeof(T));
        return v;
    }
}

// ── buffers_impl_worker: return-type dispatch ────────────────────────────────
// Primary template: simple loop (double, int32_t, unique_ptr<Geometry>, string)
// bool specialization: PreparedGeometry optimizations for const-col paths.

template <typename Ret, size_t N, typename Invoke>
struct buffers_impl_worker {
    static void run(raw_buffer& out, uint32_t n, Invoke& invoke,
                    const std::array<BuffersColView, N>&,
                    BboxOp, bool,
                    ColPrepOp, ColPrepOp,
                    ColPrepDistOp, ColPrepDistOp,
                    ColPrepPointOp, ColPrepPointOp) {
        for (uint32_t i = 0; i < n; ++i)
            pack_result(out, invoke(i));
    }
};

template <size_t N, typename Invoke>
struct buffers_impl_worker<bool, N, Invoke> {
    static void run(raw_buffer& out, uint32_t n, Invoke& invoke,
                    const std::array<BuffersColView, N>& cols,
                    BboxOp bbox_op, bool early_ret,
                    ColPrepOp prep_a, ColPrepOp prep_b,
                    ColPrepDistOp prep_a_dist, ColPrepDistOp prep_b_dist,
                    ColPrepPointOp prep_a_point, ColPrepPointOp prep_b_point)
    {
        using PGF = geos::geom::prep::PreparedGeometryFactory;

        // ── A-const path ────────────────────────────────────────────────────
        if (cols[0].is_effectively_const() && prep_a && n > 0) {
            if (cols[0].get_bytes(0).empty() && n > 1) {
                for (uint32_t i = 0; i < n; ++i)
                    pack_result(out, false);
            } else {
                auto span_a = cols[0].get_bytes(0);
                BBox bbox_a = wkb_bbox(span_a);
                auto geom_a = read_wkb(span_a);
                auto pa = PGF::prepare(geom_a.get());

                if (prep_a_point) {
                    auto s1 = cols[1].get_bytes(0);
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
                            auto span_b = cols[1].get_bytes(i);
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
                            auto span_b = cols[1].get_bytes(i);
                            if (bbox_op && !bbox_op(bbox_a, wkb_bbox(span_b))) {
                                pack_result(out, early_ret); continue;
                            }
                            pack_result(out, prep_a(pa.get(), read_wkb(span_b).get()));
                        }
                    }
                } else {
                    for (uint32_t i = 0; i < n; ++i) {
                        auto span_b = cols[1].get_bytes(i);
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
        if (cols[1].is_effectively_const() && prep_b && n > 0) {
            auto span_b = cols[1].get_bytes(0);
            BBox bbox_b = wkb_bbox(span_b);
            auto geom_b = read_wkb(span_b);
            auto pb = PGF::prepare(geom_b.get());

            if (prep_b_point) {
                auto s0 = cols[0].get_bytes(0);
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
                        auto span_a = cols[0].get_bytes(i);
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
                        auto span_a = cols[0].get_bytes(i);
                        if (bbox_op && !bbox_op(wkb_bbox(span_a), bbox_b)) {
                            pack_result(out, early_ret); continue;
                        }
                        pack_result(out, prep_b(pb.get(), read_wkb(span_a).get()));
                    }
                }
            } else {
                for (uint32_t i = 0; i < n; ++i) {
                    auto span_a = cols[0].get_bytes(i);
                    if (bbox_op && !bbox_op(wkb_bbox(span_a), bbox_b)) {
                        pack_result(out, early_ret); continue;
                    }
                    pack_result(out, prep_b(pb.get(), read_wkb(span_a).get()));
                }
            }
            return;
        }

        // ── 3-arg dist path (st_dwithin) ────────────────────────────────────
        if constexpr (N >= 3) {
            if (cols[0].is_effectively_const() && prep_a_dist && n > 0) {
                auto span_a = cols[0].get_bytes(0);
                BBox bbox_a = wkb_bbox(span_a);
                auto geom_a = read_wkb(span_a);
                auto pa = PGF::prepare(geom_a.get());
                for (uint32_t i = 0; i < n; ++i) {
                    auto span_b = cols[1].get_bytes(i);
                    double dist = buf_get_arg<double>(cols[2], i);
                    if (!bbox_a.intersects(wkb_bbox(span_b).expanded(dist))) {
                        pack_result(out, false); continue;
                    }
                    pack_result(out, prep_a_dist(pa.get(), read_wkb(span_b).get(), dist));
                }
                return;
            }
            if (cols[1].is_effectively_const() && prep_b_dist && n > 0) {
                auto span_b = cols[1].get_bytes(0);
                BBox bbox_b = wkb_bbox(span_b);
                auto geom_b = read_wkb(span_b);
                auto pb = PGF::prepare(geom_b.get());
                for (uint32_t i = 0; i < n; ++i) {
                    auto span_a = cols[0].get_bytes(i);
                    double dist = buf_get_arg<double>(cols[2], i);
                    if (!wkb_bbox(span_a).intersects(bbox_b.expanded(dist))) {
                        pack_result(out, false); continue;
                    }
                    pack_result(out, prep_b_dist(pb.get(), read_wkb(span_a).get(), dist));
                }
                return;
            }
        }

        // ── Fallback ────────────────────────────────────────────────────────
        for (uint32_t i = 0; i < n; ++i) {
            if (bbox_op &&
                !bbox_op(wkb_bbox(cols[0].get_bytes(i)),
                         wkb_bbox(cols[1].get_bytes(i)))) {
                pack_result(out, early_ret); continue;
            }
            pack_result(out, invoke(i));
        }
    }
};

// ── buffers_impl_wrapper ────────────────────────────────────────────────────
// Single-column Buffers output: header (num_cols=1, num_rows, result_size=0)
// then results packed via pack_result(), then result_size updated in-place.

template <typename Ret, typename... Args>
raw_buffer* buffers_impl_wrapper(raw_buffer* ptr,
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
    auto bb = parse_buffers(ptr);
    uint32_t n = bb.num_rows;
    constexpr size_t nargs = sizeof...(Args);

    std::array<BuffersColView, nargs> cols;
    for (size_t j = 0; j < nargs; ++j) {
        BuffersColView cv;
        if (j < bb.cols.size()) {
            cv.data = bb.cols[j].data;
            cv.buffer_size = bb.cols[j].buffer_size;
        } else {
            cv.data = nullptr;
            cv.buffer_size = 0;
        }
        cv.row_count = n;
        cols[j] = cv;
    }

    auto invoke = [&](uint32_t row) {
        return [&]<size_t... I>(std::index_sequence<I...>) {
            return impl(buf_get_arg<std::decay_t<Args>>(cols[I], row)...);
        }(std::make_index_sequence<nargs>{});
    };

    raw_buffer* out = nullptr;
    try {
        out = clickhouse_create_buffer(24);
        out->clear();
        writeBinaryLE64(1u, *out);
        writeBinaryLE64(static_cast<uint64_t>(n), *out);
        writeBinaryLE64(0u, *out);  // placeholder, updated at end

        buffers_impl_worker<Ret, nargs, decltype(invoke)>::run(
            *out, n, invoke, cols,
            bbox_op, early_ret,
            prep_a, prep_b, prep_a_dist, prep_b_dist,
            prep_a_point, prep_b_point);

        size_t result_size = out->size() - 24;
        uint8_t* size_ptr = out->data() + 16;
        for (uint32_t i = 0; i < 8; ++i)
            *(size_ptr + i) = static_cast<uint8_t>(result_size >> (i * 8));

        return out;

    } catch (const std::exception& e) {
        if (out) clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(out));
        panic(e.what());
    }
    __builtin_unreachable();
}
} // namespace ch

// ── Buffers wire format macros ────────────────────────────────────────────────

#define CH_UDF_BUFFERS_BBOX2_POINT(name, bbox_op, early_ret)                     \
    __attribute__((export_name(#name "_buffers")))                               \
    ch::raw_buffer * name##_buffers(ch::raw_buffer * ptr,                        \
                                    uint32_t num_rows) {                         \
        return ch::buffers_impl_wrapper(ptr, num_rows, ch::name##_impl,          \
            ch::bbox_op, early_ret, ch::prep_a_##name, ch::prep_b_##name,        \
            nullptr, nullptr,                                                    \
            ch::prep_a_pt_##name, ch::prep_b_pt_##name);                         \
    }

#define CH_UDF_BUFFERS_BBOX2(name, bbox_op, early_ret)                           \
    __attribute__((export_name(#name "_buffers")))                               \
    ch::raw_buffer * name##_buffers(ch::raw_buffer * ptr,                        \
                                    uint32_t num_rows) {                         \
        return ch::buffers_impl_wrapper(ptr, num_rows, ch::name##_impl,          \
            ch::bbox_op, early_ret, ch::prep_a_##name, ch::prep_b_##name);       \
    }

#define CH_UDF_BUFFERS_PRED3(name)                                               \
    __attribute__((export_name(#name "_buffers")))                               \
    ch::raw_buffer * name##_buffers(ch::raw_buffer * ptr,                        \
                                    uint32_t num_rows) {                         \
        return ch::buffers_impl_wrapper(ptr, num_rows, ch::name##_impl,          \
            nullptr, false, nullptr, nullptr,                                    \
            ch::prep_a_##name, ch::prep_b_##name);                               \
    }

#define CH_UDF_BUFFERS(name)                                                     \
    __attribute__((export_name(#name "_buffers")))                               \
    ch::raw_buffer * name##_buffers(ch::raw_buffer * ptr,                        \
                                    uint32_t num_rows) {                         \
        return ch::buffers_impl_wrapper(ptr, num_rows, ch::name##_impl);         \
    }

