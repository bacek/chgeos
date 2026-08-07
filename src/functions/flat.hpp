// Flat-currency implementations of individual chain stages.
//
// Each one is an alternative implementation of a function already registered in
// chain.hpp, operating on FlatBatch instead of unique_ptr<Geometry>.  The chain
// runner picks them automatically when every stage of a chain has one; nothing
// here knows or cares which chains exist.
//
// The bar for adding a stage is bit-identical output, not approximate
// agreement: the flat path must produce the same double, including NaN and the
// same accumulation order, as the GEOS implementation it stands in for.  A
// stage that cannot guarantee that for some input declines it instead.

#pragma once

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <limits>
#include <optional>
#include <span>
#include <utility>

#include "../chain.hpp"
#include "../columnar.hpp"
#include "../geom/flat_batch.hpp"
#include "../geom/wkb_point.hpp"

namespace ch {

// Reads one geometry column into flat coordinates.  Shared by every stage that
// heads a chain over a plain geometry column, which is what lets a chain go flat
// without the source function being special-cased.
//
// Declines the block if any row holds a shape the flat model cannot carry.
inline std::optional<FlatBatch> flat_read_column(const ColView& col, uint32_t n) {
    FlatBatch fb;
    fb.reserve_rows(n);

    for (uint32_t i = 0; i < n; ++i) {
        if (col.is_null(i)) {
            fb.begin_row(false);
            continue;
        }
        fb.begin_row(true);
        if (!flat_append_wkb(fb, col.get_bytes(i))) return std::nullopt;
    }

    fb.finish();
    return fb;
}

// SOURCE st_makeline(a, b): two POINT columns become a two-vertex line per row.
//
// Declines the block if any non-null row is not a POINT — st_makeline over
// linestrings has merge semantics the flat model does not express.
inline std::optional<FlatBatch> flat_source_st_makeline(const ColumnarBuf& cb, uint32_t n) {
    ColView a = cb.col(0);
    ColView b = cb.col(1);

    FlatBatch fb;
    fb.reserve_rows(n);
    fb.xy.reserve(static_cast<size_t>(n) * 4);

    for (uint32_t i = 0; i < n; ++i) {
        // chain_source_run leaves a null row's handle empty; an absent row here
        // is the same thing, and the sinks turn both into the same value.
        if (a.is_null(i) || b.is_null(i)) {
            fb.begin_row(false);
            continue;
        }

        std::optional<XY> pa = wkb_read_point(a.get_bytes(i));
        std::optional<XY> pb = wkb_read_point(b.get_bytes(i));
        if (!pa || !pb) return std::nullopt;

        fb.begin_row(true);
        fb.begin_ring();
        fb.push_vertex(pa->x, pa->y);
        fb.push_vertex(pb->x, pb->y);
    }

    fb.finish();
    return fb;
}

// SINK st_length: sum of segment lengths over every ring of the row.
//
// Equivalence: GEOS Length::ofLine walks a sequence as `dx = x[i] - x[i-1]`,
// accumulating `sqrt(dx*dx + dy*dy)` in order; a polygon is the same sum over
// shell and holes, a collection the same sum over members, and a point is zero
// because a one-vertex run has no segments.  FlatBatch's ring list preserves
// exactly that traversal order, so the additions happen in the same sequence and
// the result is bit-identical rather than merely equal to within rounding.
inline raw_buffer* flat_sink_st_length(const FlatBatch& fb, uint32_t n) {
    raw_buffer* out = clickhouse_create_buffer(HEADER_BYTES + COL_DESC_BYTES + n * 8u);
    col_write_fixed_header<double>(out, n, COL_FIXED64);
    auto* res = reinterpret_cast<double*>(out->data() + HEADER_BYTES + COL_DESC_BYTES);

    for (uint32_t i = 0; i < n; ++i) {
        // chain_sink_run maps a null handle to NaN; match it.
        if (!fb.present[i]) {
            res[i] = std::numeric_limits<double>::quiet_NaN();
            continue;
        }

        double total = 0.0;
        for (uint32_t r = fb.ring_begin(i); r < fb.ring_end(i); ++r) {
            uint32_t v0 = fb.vertex_begin(r);
            uint32_t v1 = fb.vertex_end(r);
            for (uint32_t v = v0 + 1; v < v1; ++v) {
                double dx = fb.xy[2 * v]     - fb.xy[2 * (v - 1)];
                double dy = fb.xy[2 * v + 1] - fb.xy[2 * (v - 1) + 1];
                total += std::sqrt(dx * dx + dy * dy);
            }
        }
        res[i] = total;
    }
    return out;
}

// SOURCE st_makebox2d(low_left, up_right): the envelope of two points, as one
// closed five-vertex ring.
//
// Declines any block where GEOS would not return a polygon.  Envelope orders the
// two corners itself, and GeometryFactory::toGeometry collapses a zero-width or
// zero-height envelope to a LINESTRING, and a zero-area one to a POINT.  The
// flat model carries no type, so it cannot represent that collapse; a non-finite
// ordinate is declined for the same reason, since Envelope's min/max comparisons
// are meaningless against NaN.
inline std::optional<FlatBatch> flat_source_st_makebox2d(const ColumnarBuf& cb, uint32_t n) {
    ColView a = cb.col(0);
    ColView b = cb.col(1);

    FlatBatch fb;
    fb.reserve_rows(n);
    fb.xy.reserve(static_cast<size_t>(n) * 10);

    for (uint32_t i = 0; i < n; ++i) {
        if (a.is_null(i) || b.is_null(i)) {
            fb.begin_row(false);
            continue;
        }

        std::optional<XY> pa = wkb_read_point(a.get_bytes(i));
        std::optional<XY> pb = wkb_read_point(b.get_bytes(i));
        if (!pa || !pb) return std::nullopt;
        if (!std::isfinite(pa->x) || !std::isfinite(pa->y) ||
            !std::isfinite(pb->x) || !std::isfinite(pb->y)) return std::nullopt;

        double min_x = std::min(pa->x, pb->x), max_x = std::max(pa->x, pb->x);
        double min_y = std::min(pa->y, pb->y), max_y = std::max(pa->y, pb->y);
        if (min_x == max_x || min_y == max_y) return std::nullopt;

        fb.begin_row(true);
        fb.begin_ring();
        fb.push_vertex(min_x, min_y);
        fb.push_vertex(min_x, max_y);
        fb.push_vertex(max_x, max_y);
        fb.push_vertex(max_x, min_y);
        fb.push_vertex(min_x, min_y);
    }

    fb.finish();
    return fb;
}

// XFORM st_reverse: flip the vertex order within every ring.
//
// Equivalence: GEOS Geometry::reverse() reverses each component's coordinate
// sequence and leaves the order of rings and of collection members alone, which
// is exactly a per-ring reversal of the flat vertex runs.  No arithmetic is
// performed, so the coordinates come out as the same bits rather than as values
// that merely compare equal.
inline bool flat_xform_st_reverse(FlatBatch& fb, std::span<const ColView>) {
    for (uint32_t r = 0; r + 1 < fb.ring_start.size(); ++r) {
        uint32_t first = fb.vertex_begin(r);
        uint32_t last  = fb.vertex_end(r);
        if (last - first < 2) continue;
        for (uint32_t lo = first, hi = last - 1; lo < hi; ++lo, --hi) {
            std::swap(fb.xy[2 * lo],     fb.xy[2 * hi]);
            std::swap(fb.xy[2 * lo + 1], fb.xy[2 * hi + 1]);
        }
    }
    return true;
}

// SOURCE st_reverse: the chain machinery lets a zero-scalar XFORM head a chain,
// in which case it reads the column itself; mirror that for the flat path.
inline std::optional<FlatBatch> flat_source_st_reverse(const ColumnarBuf& cb, uint32_t n) {
    auto fb = flat_read_column(cb.col(0), n);
    if (fb) flat_xform_st_reverse(*fb, {});
    return fb;
}

// XFORM st_translate(dx, dy): shift every vertex.
//
// Equivalence: GEOS applies an AffineTransformation, which evaluates
// `m00*x + m01*y + m02` per ordinate with the translation matrix m00 = m11 = 1,
// m01 = m10 = 0, m02 = dx, m12 = dy.  The identity terms are spelled out rather
// than folded into `x + dx` because the two are not the same function: `0.0 * y`
// is NaN for infinite y, and the intervening `+ 0.0` turns a negative zero
// positive.  Same expression, same bits.  (The project builds without
// -ffast-math, so the compiler may not fold them either.)
inline bool flat_xform_st_translate(FlatBatch& fb, std::span<const ColView> scalars) {
    double dx = col_get_fixed_widened<double>(scalars[0], 0);
    double dy = col_get_fixed_widened<double>(scalars[1], 0);

    for (size_t v = 0; v + 1 < fb.xy.size(); v += 2) {
        double x = fb.xy[v];
        double y = fb.xy[v + 1];
        fb.xy[v]     = 1.0 * x + 0.0 * y + dx;
        fb.xy[v + 1] = 0.0 * x + 1.0 * y + dy;
    }
    return true;
}

// A row's rings are contiguous, so its whole vertex run is the span from the
// first ring's start to the last ring's end.
inline uint32_t flat_row_vertex_count(const FlatBatch& fb, uint32_t row) {
    uint32_t first_ring = fb.ring_begin(row);
    uint32_t last_ring  = fb.ring_end(row);
    if (last_ring == first_ring) return 0;
    return fb.vertex_end(last_ring - 1) - fb.vertex_begin(first_ring);
}

// SINK st_npoints: total vertex count over the whole row.
//
// Equivalence: GEOS getNumPoints() sums the coordinate count of every component
// and counts a closed ring's repeated endpoint each time it occurs.  The walker
// pushes WKB vertices verbatim, so the flat count is that same sum.
inline raw_buffer* flat_sink_st_npoints(const FlatBatch& fb, uint32_t n) {
    raw_buffer* out = clickhouse_create_buffer(HEADER_BYTES + COL_DESC_BYTES + n * 4u);
    col_write_fixed_header<int32_t>(out, n, COL_FIXED32);
    auto* res = reinterpret_cast<int32_t*>(out->data() + HEADER_BYTES + COL_DESC_BYTES);

    for (uint32_t i = 0; i < n; ++i)
        // chain_sink_run maps a null handle to 0; match it.
        res[i] = fb.present[i] ? static_cast<int32_t>(flat_row_vertex_count(fb, i)) : 0;

    return out;
}

// SINK st_isempty: true when the row holds no coordinates at all.
//
// Equivalence: every GEOS geometry that reports isEmpty() carries no
// coordinates, and every non-empty one carries at least one.  The empty point is
// the only case where that is not visible in the WKB structure, and
// flat_append_wkb already drops its NaN placeholder.
inline raw_buffer* flat_sink_st_isempty(const FlatBatch& fb, uint32_t n) {
    raw_buffer* out = clickhouse_create_buffer(HEADER_BYTES + COL_DESC_BYTES + n);
    col_write_fixed_header<uint8_t>(out, n, COL_FIXED8);
    uint8_t* res = out->data() + HEADER_BYTES + COL_DESC_BYTES;

    for (uint32_t i = 0; i < n; ++i)
        // chain_sink_run maps a null handle to 0 here rather than to true.
        res[i] = fb.present[i] && flat_row_vertex_count(fb, i) == 0 ? 1u : 0u;

    return out;
}

} // namespace ch
