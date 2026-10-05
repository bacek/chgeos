// COLUMNAR_V1 export for st_collect_agg with a no-geometry fast row.
//
// The generic wrapper decodes every array element through read_wkb into a GEOS
// object, wraps the row's vector in a GeometryCollection, and re-serializes it.
// For the overwhelmingly common case — the array holds nothing but 21-byte
// little-endian WKB points, which is what Parquet point columns are — the
// collection WKB is just a 9-byte header plus the input bytes concatenated
// verbatim, and the entire GEOS round trip is pure overhead.  This export
// checks each row for that shape and concatenates when it fits; any other row
// (non-point member, big-endian element, truncated input) falls back to the
// generic decode + st_collect_agg_impl + push_geom path, row by row, so output
// is byte-identical either way.

#pragma once

#include <cstring>
#include <memory>
#include <span>
#include <vector>

#include <geos/geom/Geometry.h>

#include "../geo_columnar.hpp"
#include "overlay.hpp"

namespace ch {

// Little-endian WKB Point: order byte 0x01, type 1 (u32 LE), two doubles.
inline constexpr uint32_t WKB_POINT_LE_SIZE = 21;

inline bool is_le_wkb_point(std::span<const uint8_t> b) {
    if (b.size() != WKB_POINT_LE_SIZE) return false;
    uint32_t type;
    std::memcpy(&type, b.data() + 1, 4);
    return b[0] == 0x01 && type == 1u;
}

// Fast row: header (order, type 7 = GeometryCollection, count) + members.
// Members are copied whole (each already carries its own LE point header).
inline void collect_wkb_header(uint32_t count, uint8_t* dst) {
    dst[0] = 0x01;
    uint32_t type = 7;
    std::memcpy(dst + 1, &type, 4);
    std::memcpy(dst + 5, &count, 4);
}

__attribute__((export_name("st_collect_agg")))
inline ch::raw_buffer* st_collect_agg_col_fast(ch::raw_buffer* ptr, uint32_t)
{
    auto cb = ch::parse_columnar(ptr);
    uint32_t n = cb.num_rows;
    ch::ColView col = cb.col(0);

    ch::raw_buffer* out = nullptr;
    try {
        out = clickhouse_create_buffer(0);
        ch::ColBytesWriter w(out, n, /*nullable=*/false);

        // Wire layout of Array(String) (see col_get_complex_array):
        //   outer offsets: uint64[row_count+1] cumulative element counts
        //   inner offsets: uint64[M_total+1] into one char area
        const uint64_t* outer_offs = reinterpret_cast<const uint64_t*>(col.data);
        const uint64_t M_total     = outer_offs[col.row_count];
        const uint8_t* inner_data  = col.data + (col.row_count + 1u) * sizeof(uint64_t);
        const uint64_t* inner_offs = reinterpret_cast<const uint64_t*>(inner_data);
        const uint8_t*  chars      = inner_data + (M_total + 1u) * sizeof(uint64_t);

        std::vector<uint8_t> scratch;   // fallback-free reuse for the byte copy
        for (uint32_t i = 0; i < n; ++i) {
            if (col.is_null(i)) { w.push_null(); continue; }

            uint32_t idx        = col.effective_row(i);
            uint64_t outer_start = outer_offs[idx];
            uint64_t outer_end   = outer_offs[idx + 1];
            uint64_t count       = outer_end - outer_start;

            bool points_only = true;
            size_t payload   = 0;
            for (uint64_t j = outer_start; j < outer_end; ++j) {
                uint64_t s = inner_offs[j], e = inner_offs[j + 1];
                std::span<const uint8_t> b{chars + s, static_cast<size_t>(e - s)};
                if (!is_le_wkb_point(b)) { points_only = false; break; }
                payload += b.size();
            }

            if (points_only) {
                scratch.clear();
                scratch.resize(9 + payload);
                collect_wkb_header(static_cast<uint32_t>(count), scratch.data());
                uint8_t* p = scratch.data() + 9;
                for (uint64_t j = outer_start; j < outer_end; ++j) {
                    uint64_t s = inner_offs[j], e = inner_offs[j + 1];
                    std::memcpy(p, chars + s, e - s);
                    p += e - s;
                }
                w.push_bytes({scratch.data(), scratch.size()});
                continue;
            }

            // Generic row: identical work to the wrapper it stands in for.
            std::vector<std::unique_ptr<geos::geom::Geometry>> geoms;
            geoms.reserve(count);
            for (uint64_t j = outer_start; j < outer_end; ++j) {
                uint64_t s = inner_offs[j], e = inner_offs[j + 1];
                geoms.push_back(read_wkb(std::span<const uint8_t>{chars + s,
                                                                  static_cast<size_t>(e - s)}));
            }
            w.push_value(st_collect_agg_impl(std::move(geoms)));
        }

        w.finish();
        return out;
    } catch (const std::exception& e) {
        if (out) clickhouse_destroy_buffer(reinterpret_cast<uint8_t*>(out));
        ch::panic(e.what());
    }
    __builtin_unreachable();
}

} // namespace ch
