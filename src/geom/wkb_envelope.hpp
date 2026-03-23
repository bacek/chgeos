#pragma once

#include <cmath>
#include <cstdint>
#include <limits>
#include <span>
#include <stdexcept>

namespace ch {

struct BBox {
    double xmin =  std::numeric_limits<double>::infinity();
    double ymin =  std::numeric_limits<double>::infinity();
    double xmax = -std::numeric_limits<double>::infinity();
    double ymax = -std::numeric_limits<double>::infinity();

    bool is_empty() const { return xmin > xmax; }

    void expand(double x, double y) {
        if (std::isnan(x) || std::isnan(y)) return; // GEOS empty point
        if (x < xmin) xmin = x;
        if (x > xmax) xmax = x;
        if (y < ymin) ymin = y;
        if (y > ymax) ymax = y;
    }

    bool intersects(const BBox & o) const {
        return !is_empty() && !o.is_empty() &&
               xmax >= o.xmin && o.xmax >= xmin &&
               ymax >= o.ymin && o.ymax >= ymin;
    }

    bool contains(const BBox & o) const {
        return !is_empty() && !o.is_empty() &&
               xmin <= o.xmin && xmax >= o.xmax &&
               ymin <= o.ymin && ymax >= o.ymax;
    }

    BBox expanded(double d) const {
        if (is_empty()) return *this;
        return {xmin - d, ymin - d, xmax + d, ymax + d};
    }
};

// Extract bounding box from WKB/EWKB bytes without GEOS allocation.
BBox wkb_bbox(std::span<const uint8_t> wkb);

} // namespace ch
