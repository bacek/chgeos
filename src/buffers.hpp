#pragma once

// Buffers wire format (Native serialization) for ClickHouse WASM UDFs.
//
// Wire layout:
//   num_rows (varint) | num_cols (varint)
//   For each column:
//     buffer_size (varint) | column_data (Native format binary)

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
        bool is_const = false;
    };
    std::vector<ColInfo> cols;
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
        if (!readBinaryLE64(p, end, v)) return bb;
        ci.is_const = (v & (1ULL << 63)) != 0;
        ci.buffer_size = static_cast<uint32_t>(v & 0x7FFFFFFF);
        ci.data = p;
        bb.cols.push_back(ci);
        p = ci.data + ci.buffer_size;
    }
    return bb;
}

// ── BuffersColReader: sequential access, O(1)/row ────────────────────────────
// For non-const columns: advances through varint-encoded data one row at a time.
// For const columns (is_const=true): always returns the same single-row data.

struct BuffersColReader {
    mutable const uint8_t* p = nullptr;
    const uint8_t* end = nullptr;
    mutable uint32_t remaining = 0;
    bool const_mode = false;
    const uint8_t* const_data = nullptr;
    uint32_t const_len = 0;

    // Read exactly n bytes (fixed-size types: double, int32, etc.).
    // In const mode always returns the same bytes.
    std::span<const uint8_t> read(size_t n) const noexcept {
        if (const_mode) {
            if (n > const_len) return {};
            return {const_data, n};
        }
        if (p + n > end) return {};
        const uint8_t* start = p;
        p += n;
        return {start, n};
    }

    // Read a variable-length blob (Native: varint(len) + payload).
    // In const mode strips the varint prefix; in non-const mode advances past it.
    std::span<const uint8_t> read_blob() noexcept {
        if (const_mode) {
            uint64_t data_len = 0;
            if (!readVarUInt(const_data, const_data + const_len, data_len)) return {};
            uint32_t prefix = getLengthOfVarUInt(static_cast<uint32_t>(data_len));
            if (static_cast<uint64_t>(prefix) + data_len > const_len) return {};
            return {const_data + prefix, static_cast<size_t>(data_len)};
        }
        uint64_t data_len = 0;
        if (!readVarUInt(p, end, data_len)) return {};
        p += getLengthOfVarUInt(static_cast<uint32_t>(data_len));
        if (p + data_len > end) return {};
        const uint8_t* start = p;
        p += data_len;
        return {start, static_cast<size_t>(data_len)};
    }

    static BuffersColReader from_col(const BuffersBuf::ColInfo& ci, uint32_t row_count) {
        BuffersColReader r;
        if (ci.is_const && ci.buffer_size > 0) {
            r.const_mode = true;
            r.const_data = ci.data;
            r.const_len  = static_cast<uint32_t>(ci.buffer_size);
        } else {
            r.p = ci.data;
            r.end = ci.data + ci.buffer_size;
            r.remaining = row_count;
        }
        return r;
    }

    bool done() const noexcept { return const_mode ? false : (remaining == 0); }
};

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

// ── unpack_arg: deserialize one argument from BuffersColReader ──────────────
// Each specialization knows how many bytes to read and how to interpret them.

template <typename T>
T unpack_arg(BuffersColReader&);

template <>
inline double unpack_arg<double>(BuffersColReader& r) {
    auto span = r.read(sizeof(double));
    double v;
    std::memcpy(&v, span.data(), sizeof(v));
    return v;
}

template <>
inline int32_t unpack_arg<int32_t>(BuffersColReader& r) {
    auto span = r.read(sizeof(int32_t));
    int32_t v;
    std::memcpy(&v, span.data(), sizeof(v));
    return v;
}

template <>
inline uint32_t unpack_arg<uint32_t>(BuffersColReader& r) {
    auto span = r.read(sizeof(uint32_t));
    uint32_t v;
    std::memcpy(&v, span.data(), sizeof(v));
    return v;
}

template <>
inline std::string_view unpack_arg<std::string_view>(BuffersColReader& r) {
    auto span = r.read_blob();
    return {reinterpret_cast<const char*>(span.data()), span.size()};
}

template <>
inline std::span<const uint8_t> unpack_arg<std::span<const uint8_t>>(BuffersColReader& r) {
    return r.read_blob();
}

template <>
inline std::unique_ptr<geos::geom::Geometry> unpack_arg<std::unique_ptr<geos::geom::Geometry>>(BuffersColReader& r) {
    auto span = r.read_blob();
    if (span.empty()) return nullptr;
    try { return ch::read_wkb(span); }
    catch (...) { return ch::read_wkt(span); }
}

template <>
inline geos::geom::Geometry* unpack_arg<geos::geom::Geometry*>(BuffersColReader& r) {
    auto span = r.read_blob();
    if (span.empty()) return nullptr;
    try { return ch::read_wkb(span).release(); }
    catch (...) { return ch::read_wkt(span).release(); }
}

template <>
inline geos::geom::Geometry const* unpack_arg<geos::geom::Geometry const*>(BuffersColReader& r) {
    return unpack_arg<geos::geom::Geometry*>(r);
}

// ── buffers_impl_worker: return-type dispatch ────────────────────────────────

template <typename Ret, size_t N, typename Invoke>
struct buffers_impl_worker {
    static void run(raw_buffer& out, uint32_t n, const char* func_name, Invoke& invoke,
                    std::array<BuffersColReader, N>&,
                    BboxOp, bool,
                    ColPrepOp, ColPrepOp,
                    ColPrepDistOp, ColPrepDistOp,
                    ColPrepPointOp, ColPrepPointOp) {
        log(std::format("{}: Primary template ({}), n={}", func_name, typeid(Ret).name(), n));
        for (uint32_t i = 0; i < n; ++i)
            pack_result(out, invoke());
    }
};

template <size_t N, typename Invoke>
struct buffers_impl_worker<bool, N, Invoke> {
    static void run(raw_buffer& out, uint32_t n, const char* func_name, Invoke& invoke,
                    std::array<BuffersColReader, N>& readers,
                    BboxOp bbox_op, bool early_ret,
                    ColPrepOp prep_a, ColPrepOp prep_b,
                    ColPrepDistOp prep_a_dist, ColPrepDistOp prep_b_dist,
                    ColPrepPointOp prep_a_point, ColPrepPointOp prep_b_point)
    {
        using PGF = geos::geom::prep::PreparedGeometryFactory;

        // ── A-const path ────────────────────────────────────────────────────
        if (readers[0].const_mode && prep_a && n > 0) {
            log(std::format("{}: A-const path, n={}", func_name, n));
            auto span_a = unpack_arg<std::span<const uint8_t>>(readers[0]);
            if (span_a.empty() && n > 1) {
                for (uint32_t i = 0; i < n; ++i)
                    pack_result(out, false);
            } else {
                BBox bbox_a = wkb_bbox(span_a);
                auto geom_a = read_wkb(span_a);
                auto pa = PGF::prepare(geom_a.get());

                if (prep_a_point) {
                    auto s1 = readers[1].read_blob();
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
        if (readers[1].const_mode && prep_b && n > 0) {
            log(std::format("{}: B-const path, n={}", func_name, n));
            auto span_b = unpack_arg<std::span<const uint8_t>>(readers[1]);
            BBox bbox_b = wkb_bbox(span_b);
            auto geom_b = read_wkb(span_b);
            auto pb = PGF::prepare(geom_b.get());

            if (prep_b_point) {
                auto s0 = readers[0].read_blob();
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
                    pack_result(out, prep_b(pb.get(), read_wkb(span_a).get()));
                }
            }
            return;
        }

        // ── 3-arg dist path (st_dwithin) ────────────────────────────────────
        if constexpr (N >= 3) {
            if (readers[0].const_mode && prep_a_dist && n > 0) {
                log(std::format("{}: A-const dist path, n={}", func_name, n));
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
            if (readers[1].const_mode && prep_b_dist && n > 0) {
                log(std::format("{}: B-const dist path, n={}", func_name, n));
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
        log(std::format("{}: Fallback path, n={}", func_name, n));
        for (uint32_t i = 0; i < n; ++i)
            pack_result(out, invoke());
    }
};

// ── buffers_impl_wrapper ────────────────────────────────────────────────────

template <typename Ret, typename... Args>
raw_buffer* buffers_impl_wrapper(const char* func_name,
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
    auto bb = parse_buffers(ptr);
    uint32_t n = bb.num_rows;
    constexpr size_t nargs = sizeof...(Args);

    log(std::format("{}: Executing {}/{}", func_name, bb.num_cols, bb.num_rows));

    std::array<BuffersColReader, nargs> readers;
    for (size_t j = 0; j < nargs; ++j) {
        if (j < bb.cols.size())
            readers[j] = BuffersColReader::from_col(bb.cols[j], n);
    }

    // Invoke lambda: dispatches to unpack_arg<T> for each argument
    auto invoke = [&readers, impl]() {
        return [&]<size_t... I>(std::index_sequence<I...>) {
            return impl(unpack_arg<std::decay_t<Args>>(readers[I])...);
        }(std::make_index_sequence<nargs>{});
    };

    raw_buffer* out = nullptr;
    try {
        out = clickhouse_create_buffer(24);
        out->clear();
        writeBinaryLE64(1u, *out);
        writeBinaryLE64(static_cast<uint64_t>(n), *out);
        writeBinaryLE64(0u, *out);

        buffers_impl_worker<Ret, nargs, decltype(invoke)>::run(
            *out, n, func_name, invoke, readers,
            bbox_op, early_ret,
            prep_a, prep_b, prep_a_dist, prep_b_dist,
            prep_a_point, prep_b_point);

        size_t result_size = out->size() - 24;
        uint8_t* size_ptr = out->data() + 16;
        for (uint32_t i = 0; i < 8; ++i)
            *(size_ptr + i) = static_cast<uint8_t>(result_size >> (i * 8));

        log(std::format("{}: Done, {} bytes output", func_name, result_size));
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
        return ch::buffers_impl_wrapper(#name, ptr, num_rows, ch::name##_impl,   \
            ch::bbox_op, early_ret, ch::prep_a_##name, ch::prep_b_##name,        \
            nullptr, nullptr,                                                    \
            ch::prep_a_pt_##name, ch::prep_b_pt_##name);                         \
    }

#define CH_UDF_BUFFERS_BBOX2(name, bbox_op, early_ret)                           \
    __attribute__((export_name(#name "_buffers")))                               \
    ch::raw_buffer * name##_buffers(ch::raw_buffer * ptr,                        \
                                    uint32_t num_rows) {                         \
        return ch::buffers_impl_wrapper(#name, ptr, num_rows, ch::name##_impl,   \
            ch::bbox_op, early_ret, ch::prep_a_##name, ch::prep_b_##name);       \
    }

#define CH_UDF_BUFFERS_PRED3(name)                                               \
    __attribute__((export_name(#name "_buffers")))                               \
    ch::raw_buffer * name##_buffers(ch::raw_buffer * ptr,                        \
                                    uint32_t num_rows) {                         \
        return ch::buffers_impl_wrapper(#name, ptr, num_rows, ch::name##_impl,   \
            nullptr, false, nullptr, nullptr,                                    \
            ch::prep_a_##name, ch::prep_b_##name);                               \
    }

#define CH_UDF_BUFFERS(name)                                                     \
    __attribute__((export_name(#name "_buffers")))                               \
    ch::raw_buffer * name##_buffers(ch::raw_buffer * ptr,                        \
                                    uint32_t num_rows) {                         \
        return ch::buffers_impl_wrapper(#name, ptr, num_rows, ch::name##_impl);  \
    }
