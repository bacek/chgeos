#pragma once

#include <cmath>
#include <limits>
#include <span>

#include "wkb_cursor.hpp"

namespace ch {

struct BBox {
    double xmin =  std::numeric_limits<double>::infinity();
    double ymin =  std::numeric_limits<double>::infinity();
    double xmax = -std::numeric_limits<double>::infinity();
    double ymax = -std::numeric_limits<double>::infinity();

    [[gnu::always_inline]] bool is_empty() const { return xmin > xmax; }

    [[gnu::always_inline]] void expand(double x, double y) {
        if (std::isnan(x) || std::isnan(y)) return; // GEOS empty point
        if (x < xmin) xmin = x;
        if (x > xmax) xmax = x;
        if (y < ymin) ymin = y;
        if (y > ymax) ymax = y;
    }

    [[gnu::always_inline]] bool intersects(const BBox & o) const {
        return !is_empty() && !o.is_empty() &&
               xmax >= o.xmin && o.xmax >= xmin &&
               ymax >= o.ymin && o.ymax >= ymin;
    }

    [[gnu::always_inline]] bool contains(const BBox & o) const {
        return !is_empty() && !o.is_empty() &&
               xmin <= o.xmin && xmax >= o.xmax &&
               ymin <= o.ymin && ymax >= o.ymax;
    }

    [[gnu::always_inline]] BBox expanded(double d) const {
        if (is_empty()) return *this;
        return {xmin - d, ymin - d, xmax + d, ymax + d};
    }
};

namespace detail {

class WKBCursor {
    ByteCursor c_;

    [[gnu::always_inline]] void expand_coord_seq(BBox & env, uint32_t num_points, uint32_t coord_dims) {
        for (uint32_t i = 0; i < num_points; ++i) {
            double x = c_.read_double();
            double y = c_.read_double();
            env.expand(x, y);
            c_.skip((coord_dims - 2) * sizeof(double)); // skip Z, M if present
        }
    }

    void read_geometry(BBox & env) {
        GeomHeader h = read_geom_header(c_);

        switch (h.base) {
            case 1: { // Point
                double x = c_.read_double();
                double y = c_.read_double();
                env.expand(x, y);
                c_.skip((h.dims - 2) * sizeof(double));
                break;
            }
            case 2: { // LineString
                uint32_t n = c_.read_u32();
                expand_coord_seq(env, n, h.dims);
                break;
            }
            case 3: { // Polygon — only exterior ring extends bbox
                uint32_t nrings = c_.read_u32();
                if (nrings == 0) break;
                uint32_t n_ext = c_.read_u32();
                expand_coord_seq(env, n_ext, h.dims);
                for (uint32_t r = 1; r < nrings; ++r) {
                    uint32_t n = c_.read_u32();
                    c_.skip_coords(n, h.dims); // interior rings can't extend the bbox
                }
                break;
            }
            case 4:  // MultiPoint
            case 5:  // MultiLineString
            case 6:  // MultiPolygon
            case 7: { // GeometryCollection
                uint32_t n = c_.read_u32();
                for (uint32_t i = 0; i < n; ++i)
                    read_geometry(env);
                break;
            }
            default:
                throw std::runtime_error("WKB: unknown geometry type");
        }
    }

public:
    explicit WKBCursor(std::span<const uint8_t> data) : c_(data) {}

    BBox extract() {
        BBox env;
        read_geometry(env);
        return env;
    }
};

} // namespace detail

// Extract bounding box from WKB/EWKB bytes without GEOS allocation.
[[gnu::always_inline]] inline BBox wkb_bbox(std::span<const uint8_t> wkb) {
    return detail::WKBCursor(wkb).extract();
}

// True if the WKB encodes a single Point, so its bbox corner is the point
// itself — lets callers treat a bbox-derived coordinate as exact.
inline bool wkb_is_point(std::span<const uint8_t> wkb) {
    detail::ByteCursor c(wkb);
    return detail::read_geom_header(c).base == 1;
}

} // namespace ch
