// Low-level WKB/EWKB byte reader shared by the allocation-free geometry walks
// (wkb_envelope.hpp bbox extraction, wkb_distance.hpp point distance).
//
// Nothing here parses a geometry into GEOS: a walk reads the type header, then
// streams coordinates straight out of the buffer.

#pragma once

#include <bit>
#include <cstdint>
#include <cstring>
#include <span>
#include <stdexcept>

namespace ch::detail {

// Bounds-checked, endian-aware reader. Every WKB geometry (including nested
// ones inside a multi-geometry) carries its own byte-order flag, so the
// endianness is per-geometry state, updated by read_geom_header().
class ByteCursor {
    const uint8_t * pos_;
    const uint8_t * end_;
    bool le_ = true;

public:
    explicit ByteCursor(std::span<const uint8_t> data)
        : pos_(data.data()), end_(data.data() + data.size()) {}

    [[gnu::always_inline]] void check(size_t n) const {
        if (pos_ + n > end_)
            throw std::runtime_error("WKB: truncated input");
    }

    [[gnu::always_inline]] void skip(size_t n) {
        check(n);
        pos_ += n;
    }

    [[gnu::always_inline]] uint8_t read_u8() {
        check(1);
        return *pos_++;
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

    // Skip a run of coordinates without reading them.
    [[gnu::always_inline]] void skip_coords(uint32_t num_points, uint32_t dims) {
        // Cast to size_t before multiplying to prevent uint32_t overflow on
        // adversarial WKB: e.g. num_points=0x20000000, dims=4 →
        // 0x20000000 * 4 * 8 = 0x400000000, which wraps to 0 in uint32_t.
        skip(static_cast<size_t>(num_points) * dims * sizeof(double));
    }

    [[gnu::always_inline]] void set_little_endian(bool le) { le_ = le; }
};

// Geometry type header: the base OGC type (1=Point .. 7=GeometryCollection)
// and the coordinate dimensionality (2, 3 or 4).
struct GeomHeader {
    uint32_t base;
    uint32_t dims;
};

// Read the byte-order flag and type word, resolving both the EWKB high-bit
// flags and the ISO Z/M type ranges, and consume an EWKB SRID if present.
[[gnu::always_inline]] inline GeomHeader read_geom_header(ByteCursor & c) {
    c.set_little_endian(c.read_u8() == 1);

    uint32_t type = c.read_u32();

    bool has_srid = (type & 0x20000000u) != 0;
    bool has_z    = (type & 0x80000000u) != 0;
    bool has_m    = (type & 0x40000000u) != 0;
    uint32_t base = type & 0x0FFFFFFFu;

    // ISO WKB Z/M type ranges
    if (base > 3000 && base <= 3007) { has_z = has_m = true; base -= 3000; }
    else if (base > 2000 && base <= 2007) { has_m = true; base -= 2000; }
    else if (base > 1000 && base <= 1007) { has_z = true; base -= 1000; }

    if (has_srid) c.skip(4);

    return {base, 2u + (has_z ? 1u : 0u) + (has_m ? 1u : 0u)};
}

} // namespace ch::detail
