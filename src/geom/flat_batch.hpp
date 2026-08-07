// Flat coordinate storage for a whole block of geometries.
//
// The chain machinery in chain.hpp normally passes unique_ptr<Geometry> between
// stages, which costs several heap allocations per row.  FlatBatch is the second
// currency: one contiguous coordinate array per block, with offsets marking ring
// and row boundaries.  A chain whose every stage can speak FlatBatch runs
// without GEOS ever being involved.
//
// Model: row → rings → vertices.  A ring is any contiguous vertex run that a
// geometry walk would traverse as a unit:
//
//   POINT             one ring, one vertex
//   LINESTRING        one ring
//   POLYGON           one ring per shell/hole
//   MULTI*            the rings of each member, concatenated
//
// The model deliberately does not record which ring is a shell and which a hole,
// nor where one polygon ends and the next begins.  That is enough for anything
// defined as a sum over vertex runs (length, perimeter, vertex count) and not
// enough for area or predicates — those need a richer batch, which is a
// deliberate later step rather than something stubbed out here.

#pragma once

#include <cmath>
#include <cstdint>
#include <span>
#include <vector>

#include "wkb_cursor.hpp"

namespace ch {

struct FlatBatch {
    uint32_t n = 0;                     // rows

    std::vector<double>   xy;           // x,y interleaved; 2 doubles per vertex
    std::vector<uint32_t> ring_start;   // vertex index per ring; size n_rings + 1
    std::vector<uint32_t> row_ring;     // first ring per row;   size n + 1
    std::vector<uint8_t>  present;      // size n; 0 marks a null row

    void reserve_rows(uint32_t rows) {
        n = rows;
        row_ring.reserve(rows + 1);
        present.reserve(rows);
        ring_start.reserve(rows + 1);
    }

    // Call once per row, before that row's vertices are appended.
    void begin_row(bool row_present) {
        row_ring.push_back(static_cast<uint32_t>(ring_start.size()));
        present.push_back(row_present ? 1u : 0u);
    }

    void begin_ring() {
        ring_start.push_back(static_cast<uint32_t>(xy.size() / 2));
    }

    void push_vertex(double x, double y) {
        xy.push_back(x);
        xy.push_back(y);
    }

    // Closes the offset arrays.  Both carry a trailing sentinel so every ring
    // and row can be read as a half-open range without a size special case.
    void finish() {
        ring_start.push_back(static_cast<uint32_t>(xy.size() / 2));
        row_ring.push_back(static_cast<uint32_t>(ring_start.size()) - 1);
    }

    uint32_t ring_begin(uint32_t row) const { return row_ring[row]; }
    uint32_t ring_end(uint32_t row)   const { return row_ring[row + 1]; }
    uint32_t vertex_begin(uint32_t ring) const { return ring_start[ring]; }
    uint32_t vertex_end(uint32_t ring)   const { return ring_start[ring + 1]; }
};

// Append one WKB geometry's rings to fb.  Returns false without appending
// anything further if the geometry is a shape the flat model cannot carry, so
// the caller can decline the block and let the generic GEOS path run.
//
// Throws on truncated input, matching read_wkb.
inline bool flat_append_wkb(FlatBatch& fb, std::span<const uint8_t> wkb) {
    detail::ByteCursor c(wkb);

    // Recursive walk: nested geometries carry their own byte-order and type
    // header, which read_geom_header consumes.
    auto append = [&fb](detail::ByteCursor& cur, auto&& self) -> bool {
        auto [base, dims] = detail::read_geom_header(cur);
        uint32_t extra = dims - 2;  // ordinates past X and Y, skipped per vertex

        auto read_run = [&](uint32_t num_points) {
            fb.begin_ring();
            for (uint32_t i = 0; i < num_points; ++i) {
                double x = cur.read_double();
                double y = cur.read_double();
                fb.push_vertex(x, y);
                if (extra) cur.skip_coords(1, extra);
            }
        };

        switch (base) {
            case 1: {  // POINT
                // GEOS writes an empty point as POINT(NaN NaN) and reads that
                // back as a geometry holding no coordinates.  Emitting the NaN
                // pair as a vertex would make vertex counts and emptiness
                // disagree with the GEOS path, so drop it and leave an empty
                // ring — which is what an empty LINESTRING already produces.
                double x = cur.read_double();
                double y = cur.read_double();
                if (extra) cur.skip_coords(1, extra);
                fb.begin_ring();
                if (!(std::isnan(x) && std::isnan(y))) fb.push_vertex(x, y);
                return true;
            }
            case 2:  // LINESTRING
                read_run(cur.read_u32());
                return true;
            case 3: { // POLYGON: shell then holes, each a ring
                uint32_t n_rings = cur.read_u32();
                for (uint32_t r = 0; r < n_rings; ++r) read_run(cur.read_u32());
                return true;
            }
            case 4:   // MULTIPOINT
            case 5:   // MULTILINESTRING
            case 6:   // MULTIPOLYGON
            case 7: { // GEOMETRYCOLLECTION
                uint32_t n_members = cur.read_u32();
                for (uint32_t m = 0; m < n_members; ++m)
                    if (!self(cur, self)) return false;
                return true;
            }
            default:
                return false;
        }
    };

    return append(c, append);
}

} // namespace ch
