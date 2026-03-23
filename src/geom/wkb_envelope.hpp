#pragma once

#include <bit>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <limits>
#include <span>
#include <stdexcept>

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
    const uint8_t * pos_;
    const uint8_t * end_;
    bool le_ = true;

    [[gnu::always_inline]] void check(size_t n) const {
        if (pos_ + n > end_)
            throw std::runtime_error("WKB: truncated input");
    }

    [[gnu::always_inline]] void skip(size_t n) {
        check(n);
        pos_ += n;
    }

    [[gnu::always_inline]] uint32_t read_u32() {
        check(4);
        uint32_t v;
        std::memcpy(&v, pos_, 4);
        pos_ += 4;
        if (!le_) v = std::byteswap(v);
        return v;
    }

    [[gnu::always_inline]] double read_double() {
        check(8);
        uint64_t bits;
        std::memcpy(&bits, pos_, 8);
        pos_ += 8;
        if (!le_) bits = std::byteswap(bits);
        double v;
        std::memcpy(&v, &bits, 8);
        return v;
    }

    [[gnu::always_inline]] void expand_coord_seq(BBox & env, uint32_t num_points, uint32_t coord_dims) {
        for (uint32_t i = 0; i < num_points; ++i) {
            double x = read_double();
            double y = read_double();
            env.expand(x, y);
            skip((coord_dims - 2) * sizeof(double)); // skip Z, M if present
        }
    }

    // Skip a ring without expanding envelope (interior rings can't extend the bbox).
    [[gnu::always_inline]] void skip_coord_seq(uint32_t num_points, uint32_t coord_dims) {
        // Cast to size_t before multiplying to prevent uint32_t overflow on
        // adversarial WKB: e.g. num_points=0x20000000, coord_dims=4 →
        // 0x20000000 * 4 * 8 = 0x400000000, which wraps to 0 in uint32_t.
        skip(static_cast<size_t>(num_points) * coord_dims * sizeof(double));
    }

    void read_geometry(BBox & env) {
        check(1);
        le_ = (*pos_++ == 1);

        uint32_t type = read_u32();

        bool has_srid = (type & 0x20000000u) != 0;
        bool has_z    = (type & 0x80000000u) != 0;
        bool has_m    = (type & 0x40000000u) != 0;
        uint32_t base = type & 0x0FFFFFFFu;

        // ISO WKB Z/M type ranges
        if (base > 3000 && base <= 3007) { has_z = has_m = true; base -= 3000; }
        else if (base > 2000 && base <= 2007) { has_m = true; base -= 2000; }
        else if (base > 1000 && base <= 1007) { has_z = true; base -= 1000; }

        uint32_t dims = 2 + (has_z ? 1 : 0) + (has_m ? 1 : 0);

        if (has_srid) skip(4);

        switch (base) {
            case 1: { // Point
                double x = read_double();
                double y = read_double();
                env.expand(x, y);
                skip((dims - 2) * sizeof(double));
                break;
            }
            case 2: { // LineString
                uint32_t n = read_u32();
                expand_coord_seq(env, n, dims);
                break;
            }
            case 3: { // Polygon — only exterior ring extends bbox
                uint32_t nrings = read_u32();
                if (nrings == 0) break;
                uint32_t n_ext = read_u32();
                expand_coord_seq(env, n_ext, dims);
                for (uint32_t r = 1; r < nrings; ++r) {
                    uint32_t n = read_u32();
                    skip_coord_seq(n, dims);
                }
                break;
            }
            case 4:  // MultiPoint
            case 5:  // MultiLineString
            case 6:  // MultiPolygon
            case 7: { // GeometryCollection
                uint32_t n = read_u32();
                for (uint32_t i = 0; i < n; ++i)
                    read_geometry(env);
                break;
            }
            default:
                throw std::runtime_error("WKB: unknown geometry type");
        }
    }

public:
    explicit WKBCursor(std::span<const uint8_t> data)
        : pos_(data.data()), end_(data.data() + data.size()) {}

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

} // namespace ch
