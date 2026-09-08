#pragma once

// Point-in-area fast path backed by tidwall/tg (third-party/tg).
//
// TgArea is an area-specific view over the generic TgGeom RAII handle: it
// only accepts POLYGON / MULTIPOLYGON and answers point-location tests with
// the same DE-9IM semantics the GEOS locator path produces:
//
//   kInterior  — point strictly inside the area   (boundary → false)
//   kCovers    — point inside or on the boundary  (boundary → true)
//   kExterior  — point strictly outside           (boundary → false)
//
// Holes are honoured (a point inside a hole is exterior; on a hole ring it is
// boundary). Per-row cost is a bbox rejection plus an indexed ray cast
// (ystripes); no GEOS object is allocated per row.
//
// EWKB Z/M areas are accepted: tg consumes the extra coordinate bytes and
// indexes only XY, which is exactly the topology GEOS's (and PostGIS's
// non-3D) predicates use, so results match the GEOS path bit for bit.
//
// Anything TgArea cannot claim — non-area types, malformed WKB — leaves
// ok() == false so the caller falls back to the GEOS path.

#include "tg_geom.hpp"

// Internal tg functions: TG_EXTERN in tg.c but not declared in tg.h.
// Per-ring point location; takes raw (x, y) so no per-row point geometry is
// allocated.
extern "C" {
bool tg_poly_contains_point(const struct tg_poly *poly, struct tg_point point);
bool tg_poly_covers_point(const struct tg_poly *poly, struct tg_point point);
}

namespace ch {

enum class TgPointMode { kInterior, kCovers, kExterior };

class TgArea {
public:
    static TgArea from_wkb(std::span<const uint8_t> wkb) {
        TgArea a;
        a.g_ = TgGeom::from_wkb(wkb, TG_YSTRIPES);
        if (!a.g_.ok()) return a;
        auto t = a.g_.type();
        a.area_ = (t == TG_POLYGON || t == TG_MULTIPOLYGON);
        return a;
    }

    bool ok() const { return area_ && g_.ok(); }

    bool point_in(double x, double y, TgPointMode mode) const {
        struct tg_point p{x, y};
        // Interior mode tests strict containment; covers/exterior use the
        // boundary-inclusive test so a boundary point is neither interior nor
        // exterior (matching GEOS Location / DE-9IM).
        const bool use_covers = mode != TgPointMode::kInterior;
        const auto *g = g_.get();
        bool any = false;
        if (tg_geom_typeof(g) == TG_POLYGON) {
            const auto *poly = tg_geom_poly(g);
            any = use_covers ? tg_poly_covers_point(poly, p)
                             : tg_poly_contains_point(poly, p);
        } else {  // TG_MULTIPOLYGON
            const int n = tg_geom_num_polys(g);
            for (int i = 0; i < n && !any; ++i) {
                const auto *poly = tg_geom_poly_at(g, i);
                any = poly != nullptr
                    && (use_covers ? tg_poly_covers_point(poly, p)
                                   : tg_poly_contains_point(poly, p));
            }
        }
        return mode == TgPointMode::kExterior ? !any : any;
    }

private:
    TgArea() = default;
    TgGeom g_;
    bool area_ = false;
};

} // namespace ch
