#pragma once

// GEOS layer over the COLUMNAR_V1 guest library (clickhouse_wasm/columnar.hpp):
// geometry encode/decode hooks, the COL_VARIANT geometry decoder, and the
// predicate/accessor wrapper with its const-argument and WKB fast paths.

#include <algorithm>
#include <array>
#include <cstdint>
#include <cstring>
#include <limits>
#include <optional>
#include <span>

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

#include <clickhouse_wasm/abi.hpp>
#include <clickhouse_wasm/columnar.hpp>

#include "col_prep_op.hpp"
#include "functions/knn.hpp"
#include "geom/wkb.hpp"
#include "geom/wkb_envelope.hpp"


namespace ch {

// Minimum batch size at which building an IndexedPointInAreaLocator pays for
// itself against a plain edge scan.  Measured on real zone polygons across four
// orders of magnitude of size (77B–200KB WKB), the break-even sits in a narrow
// 9–24 row band; 16 lands on the correct side at both extremes.
inline constexpr uint32_t INDEXED_LOCATOR_MIN_ROWS = 16;

// True when the WKB header names a plain 2D Polygon or MultiPolygon (either
// byte order).  Read from the header alone so the point fast path can commit
// to a polygon without parsing it; anything else — Z/M/SRID flags included —
// says no and goes the general way.
inline bool wkb_is_2d_area(std::span<const uint8_t> wkb) noexcept {
    if (wkb.size() < 5 || wkb[0] > 1) return false;
    uint32_t t = 0;
    std::memcpy(&t, wkb.data() + 1, 4);
    if (wkb[0] == 0) t = __builtin_bswap32(t);
    return t == 3u || t == 6u;
}

// The const polygon of the point-in-area fast path, parsed and indexed at most
// once per distinct WKB.  A constant argument arrives byte-identical in every
// batch of a query, so rebuilding it per call costs O(edges) each time for no
// answer change — on a 10k-vertex polygon that dwarfed the per-point work.
// One entry is enough: a const argument is the same across a query's batches,
// and a WASM instance runs one call at a time.
class ConstAreaLocator {
    using IPIAL = geos::algorithm::locate::IndexedPointInAreaLocator;

    std::vector<uint8_t>                  wkb_;
    std::unique_ptr<geos::geom::Geometry> geom_;
    std::unique_ptr<IPIAL>                index_;  // borrows *geom_, so dies first

public:
    // Point the cache at `wkb`, reparsing only if the bytes differ.  The
    // comparison is O(size), so call it once per batch, never per row.
    void bind(std::span<const uint8_t> wkb) {
        if (geom_ && wkb_.size() == wkb.size()
            && std::memcmp(wkb_.data(), wkb.data(), wkb.size()) == 0)
            return;
        index_.reset();
        geom_.reset();
        wkb_.clear();
        geom_ = read_wkb(wkb);
        wkb_.assign(wkb.begin(), wkb.end());
    }

    // Locate c against the bound polygon.  `batch_rows` keeps the old
    // break-even rule for building the index; once built it is reused.
    geos::geom::Location locate(const geos::geom::CoordinateXY & c, uint32_t batch_rows) {
        if (!index_ && batch_rows >= INDEXED_LOCATOR_MIN_ROWS)
            index_ = std::make_unique<IPIAL>(*geom_);
        return index_
            ? index_->locate(&c)
            : geos::algorithm::locate::SimplePointInAreaLocator::locate(c, geom_.get());
    }

    static ConstAreaLocator & instance() {
        static ConstAreaLocator cache;
        return cache;
    }
};

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

    // Row offset within the sub-column for this row — uint32 on the wire, not
    // the uint64 COL_BYTES offsets array; see ColView::variant_offset_at.
    const uint32_t off = col.variant_offset_at(row);

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

// ── Geometry as a column value ───────────────────────────────────────────────
// A geometry travels as WKB bytes; a null geometry is written as NULL / empty.

template <>
struct bytes_codec<std::unique_ptr<geos::geom::Geometry>> {
    static std::unique_ptr<geos::geom::Geometry> decode(std::span<const uint8_t> wkb) {
        return read_wkb(wkb);
    }
    static raw_buffer encode(const std::unique_ptr<geos::geom::Geometry>& g) {
        return write_ewkb(g);
    }
    static bool is_null(const std::unique_ptr<geos::geom::Geometry>& g) { return !g; }
};

// A geometry argument may also arrive as a native Geometry Variant column.
template <>
struct column_reader<std::unique_ptr<geos::geom::Geometry>> {
    static std::unique_ptr<geos::geom::Geometry> read(const ColView& col, uint32_t row) {
        if (col.base_type == COL_VARIANT)
            return col_get_variant_geom(col, row);
        return read_wkb(col.get_bytes(row));
    }
};

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
                                  ColWkbScalarOp wkb_scalar   = nullptr,  // 1-arg accessor read straight from WKB
                                  ColWkbPairOp   wkb_pair     = nullptr)  // 2-arg measure read straight from WKB
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

                    // Point fast path: col(1) contains 2D WKB points — no per-row GEOS alloc,
                    // and the polygon is only parsed once a point survives the bbox test.
                    if (prep_a_point && n > 0 && wkb_is_2d_area(span_a)
                        && all_2d_points(cols[1], n)) {
                        auto & area = ConstAreaLocator::instance();
                        bool bound = false;  // parse only once a point passes the bbox
                        for (uint32_t i = 0; i < n; ++i) {
                            if (cols[1].is_null(i)) { res[i] = 0u; continue; }
                            auto span_b = cols[1].get_bytes(i);
                            double px, py;
                            memcpy(&px, span_b.data() + 5, 8);
                            memcpy(&py, span_b.data() + 13, 8);
                            if (bbox_op && !bbox_op(bbox_a, BBox{px, py, px, py})) {
                                res[i] = early_ret ? 1u : 0u; continue;
                            }
                            if (!bound) { area.bind(span_a); bound = true; }
                            res[i] = prep_a_point(area.locate({px, py}, n)) ? 1u : 0u;
                        }
                        return out;
                    }

                    auto  geom_a = read_wkb(span_a);
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

                    // Point fast path: col(0) contains 2D WKB points — no per-row GEOS alloc,
                    // and the polygon is only parsed once a point survives the bbox test.
                    if (prep_b_point && n > 0 && wkb_is_2d_area(span_b)
                        && all_2d_points(cols[0], n)) {
                        auto & area = ConstAreaLocator::instance();
                        bool bound = false;  // parse only once a point passes the bbox
                        for (uint32_t i = 0; i < n; ++i) {
                            if (cols[0].is_null(i)) { res[i] = 0u; continue; }
                            auto span_a = cols[0].get_bytes(i);
                            double px, py;
                            memcpy(&px, span_a.data() + 5, 8);
                            memcpy(&py, span_a.data() + 13, 8);
                            if (bbox_op && !bbox_op(BBox{px, py, px, py}, bbox_b)) {
                                res[i] = early_ret ? 1u : 0u; continue;
                            }
                            if (!bound) { area.bind(span_b); bound = true; }
                            res[i] = prep_b_point(area.locate({px, py}, n)) ? 1u : 0u;
                        }
                        return out;
                    }

                    auto  geom_b = read_wkb(span_b);

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
            if constexpr (nargs == 2) {
                if (wkb_pair && cols[0].base_type != COL_VARIANT
                             && cols[1].base_type != COL_VARIANT) {
                    for (uint32_t i = 0; i < n; ++i) {
                        if (any_null(i)) {
                            res[i] = std::numeric_limits<double>::quiet_NaN();
                            continue;
                        }
                        std::optional<double> v = wkb_pair(cols[0].get_bytes(i), cols[1].get_bytes(i));
                        res[i] = v ? *v : invoke(i);
                    }
                    return out;
                }
            }

            for (uint32_t i = 0; i < n; ++i) {
                res[i] = any_null(i) ? std::numeric_limits<double>::quiet_NaN() : invoke(i);
            }
            return out;

        // ── everything else: the library's generic result writer ─────────────
        } else {
            return write_result_column<Ret>(n, any_null, invoke);
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
            const auto& index = ch::ConstKNNIndex::instance().bind(wkbs);
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

// 2-arg measure returning double, with a ColWkbPairOp fast path that reads the
// answer out of both WKBs.  Requires name##_wkb defined alongside name##_impl.
#define CH_UDF_COL_WKB2(name)                                                    \
    __attribute__((export_name(#name)))                                          \
    ch::raw_buffer * name(ch::raw_buffer * ptr, uint32_t num_rows) {             \
        return ch::columnar_impl_wrapper(ptr, num_rows, ch::name##_impl,         \
            nullptr, false, nullptr, nullptr, nullptr, nullptr,                  \
            nullptr, nullptr, nullptr, ch::name##_wkb);                          \
    }

// Canonical no-suffix alias for PRED3 functions that keep their _col export.
#define CH_UDF_CANONICAL(name)                                                   \
    __attribute__((export_name(#name)))                                          \
    ch::raw_buffer * name(ch::raw_buffer * ptr, uint32_t num_rows) {             \
        return name##_col(ptr, num_rows);                                        \
    }
