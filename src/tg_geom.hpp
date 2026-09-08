#pragma once

// Generic move-only RAII handle for tidwall/tg geometries (third-party/tg).
//
// tg is a self-contained C geometry library; `struct tg_geom` is an opaque
// type owned and freed by tg itself. TgGeom wraps one such object: parse it
// from WKB, inspect it, hand it to the raw tg C API, and it is freed
// automatically.
//
// WKB notes: the parser accepts PostGIS EWKB — SRID type-word flag (value
// read and discarded) and Z (0x80000000) / M (0x40000000) flags,
// plus the ISO dimension offsets — so 3D/4D input parses with the correct
// dimensions (tg_geom_dims / tg_geom_has_z / tg_geom_has_m). The geometry's
// spatial data is XY only: Z/M bytes are consumed from the stream (keeping
// it in sync) but ring/line point arrays store {x, y}, which is the same
// topology GEOS and PostGIS non-3D predicates operate on.

#include <cstdint>
#include <span>

extern "C" {
#include <tg.h>
}

namespace ch {

class TgGeom {
public:
    // Empty handle: ok() == false.
    TgGeom() = default;

    static TgGeom from_wkb(std::span<const uint8_t> wkb,
                           enum tg_index ix = TG_YSTRIPES) {
        TgGeom g;
        if (!wkb.empty()) {
            g.g_ = tg_parse_wkb_ix(wkb.data(), static_cast<int>(wkb.size()), ix);
            if (g.g_ && tg_geom_error(g.g_)) {
                tg_geom_free(g.g_);
                g.g_ = nullptr;
            }
        }
        return g;
    }

    // Parsed without error.
    bool ok() const { return g_ != nullptr; }

    enum tg_geom_type type() const {
        return g_ ? tg_geom_typeof(g_) : static_cast<enum tg_geom_type>(0);
    }

    // Bounding box (meaningful only when ok()).
    struct tg_rect rect() const {
        return g_ ? tg_geom_rect(g_) : (struct tg_rect){{0, 0}, {0, 0}};
    }

    // Raw tg API access for callers that need more than this handle exposes.
    const struct tg_geom *get() const { return g_; }

    TgGeom(const TgGeom &) = delete;
    TgGeom &operator=(const TgGeom &) = delete;
    TgGeom(TgGeom &&o) noexcept : g_(o.g_) { o.g_ = nullptr; }
    TgGeom &operator=(TgGeom &&o) noexcept {
        if (this != &o) {
            reset();
            g_ = o.g_;
            o.g_ = nullptr;
        }
        return *this;
    }
    ~TgGeom() { reset(); }

private:
    void reset() {
        if (g_) {
            tg_geom_free(g_);
            g_ = nullptr;
        }
    }
    struct tg_geom *g_ = nullptr;
};

} // namespace ch
