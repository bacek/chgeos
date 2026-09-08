// tg vs GEOS spike — native benchmark (M5 Pro), built with the project's
// CMake flags (Release = -O3).
//
// Build & run (project CMake target, native only):
//   cmake --build build_native --target tg_spike
//   ./build_native/tg_spike
//
// Regimes (mirroring chgeos columnar fast paths, src/columnar.hpp):
//   1. parse          — per-row WKB parse: tg (TG_NONE / TG_YSTRIPES) vs GEOS WKBReader
//   2. pt-const-poly  — const polygon + varying 2D point:
//                        GEOS IndexedPointInAreaLocator / SimplePointInAreaLocator
//                        vs tg ystripes polygon + tg_poly_contains_point
//   3. line-const-poly— const polygon + varying LINESTRING (st_intersects):
//                        GEOS wkb_bbox + per-row read_wkb + PreparedGeometry
//                        vs tg per-row TG_NONE parse + tg_geom_intersects
//                        (const side parsed once with TG_YSTRIPES)
//   4. both-varying   — two varying polygons (st_intersects):
//                        GEOS wkb_bbox + 2x read_wkb + intersects
//                        vs tg 2x parse + rect check + tg_geom_intersects
//
// All WKB is little-endian (matches chgeos wire format: the point fast path
// in columnar.hpp requires s[0] == 0x01). GEOS static reader + factory, same
// as chgeos read_wkb (src/geom/wkb.cpp:113).
//
// After the timed runs each regime cross-checks tg against GEOS on the same
// rows and prints the mismatch count (should be 0).

extern "C" {
#include <tg.h>
}
#include <geom/wkb_envelope.hpp>

#include <geos/version.h>
#include <geos/geom/Geometry.h>
#include <geos/geom/GeometryFactory.h>
#include <geos/geom/Coordinate.h>
#include <geos/geom/prep/PreparedGeometryFactory.h>
#include <geos/io/WKBReader.h>
#include <geos/algorithm/locate/IndexedPointInAreaLocator.h>
#include <geos/algorithm/locate/SimplePointInAreaLocator.h>

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstdio>
#include <cstring>
#include <numeric>
#include <cstdint>
#include <random>
#include <vector>

// Internal tg functions (TG_EXTERN in tg.c, not in tg.h) — same ones TgArea
// uses; avoid the per-row point-geometry allocation entirely.
extern "C" {
bool tg_poly_contains_point(const struct tg_poly *poly, struct tg_point point);
}

using namespace std::chrono;

// ── RNG ─────────────────────────────────────────────────────────────────────
struct Rng {
    uint64_t s = 0x9E3779B97F4A7C15ull;
    uint64_t next() { s ^= s << 13; s ^= s >> 7; s ^= s << 17; return s; }
    double unit() { return (next() >> 11) * (1.0 / 9007199254740992.0); }
    int range(int lo, int hi) { return lo + (int)(next() % (uint64_t)(hi - lo + 1)); }
};

// ── little-endian WKB writer ────────────────────────────────────────────────
static void u32le(std::vector<uint8_t> &v, uint32_t x) {
    v.push_back((uint8_t)x); v.push_back((uint8_t)(x >> 8));
    v.push_back((uint8_t)(x >> 16)); v.push_back((uint8_t)(x >> 24));
}
static void f64le(std::vector<uint8_t> &v, double x) {
    uint64_t b; std::memcpy(&b, &x, 8);
    for (int i = 0; i < 8; i++) v.push_back((uint8_t)(b >> (8 * i)));
}
static std::vector<uint8_t> wkb_point(double x, double y) {
    std::vector<uint8_t> v = {0x01}; u32le(v, 1); f64le(v, x); f64le(v, y);
    return v;
}
static std::vector<uint8_t> wkb_line(const std::vector<std::pair<double, double>> &pts) {
    std::vector<uint8_t> v = {0x01}; u32le(v, 2); u32le(v, (uint32_t)pts.size());
    for (auto &[x, y] : pts) { f64le(v, x); f64le(v, y); }
    return v;
}
static std::vector<uint8_t> wkb_poly(const std::vector<std::pair<double, double>> &ring) {
    std::vector<uint8_t> v = {0x01}; u32le(v, 3); u32le(v, 1); u32le(v, (uint32_t)ring.size());
    for (auto &[x, y] : ring) { f64le(v, x); f64le(v, y); }
    return v;
}

// Star-shaped polygon (always simple): radius = base * (1 + harmonics + jitter).
static std::vector<std::pair<double, double>> star_ring(Rng &rng, double cx, double cy,
                                                        double base, int n) {
    double p1 = rng.unit() * 6.283, p2 = rng.unit() * 6.283, p3 = rng.unit() * 6.283;
    std::vector<std::pair<double, double>> ring;
    ring.reserve(n + 1);
    for (int i = 0; i < n; i++) {
        double th = 6.2831853 * (double)i / n;
        double r = base * (1.0
                           + 0.22 * std::sin(2 * th + p1)
                           + 0.12 * std::sin(5 * th + p2)
                           + 0.06 * std::sin(9 * th + p3)
                           + 0.05 * (rng.unit() - 0.5));
        ring.push_back({cx + r * std::cos(th), cy + r * std::sin(th)});
    }
    ring.push_back(ring.front()); // close
    return ring;
}

// ── datasets ────────────────────────────────────────────────────────────────
static const int POLY_N   = 30000;   // varied polygons (parse + both-varying)
static const int LINE_N   = 30000;   // varied line strings
static const int PTS_N    = 1000000; // points for the const-poly regime
static const int PVARS_N  = 200000;  // both-varying pair count
static const int BENCH_PASSES = 5;

// ── bench harness ───────────────────────────────────────────────────────────
static uint64_t g_sink = 0;

template <class F>
static void bench(const char *name, int n, F &&f) {
    uint64_t wh = 0;
    f(n / 500, wh); // warmup (small subset)
    std::vector<double> t;
    t.reserve(BENCH_PASSES);
    for (int p = 0; p < BENCH_PASSES; p++) {
        uint64_t hits = 0;
        auto t0 = steady_clock::now();
        f(n, hits);
        auto t1 = steady_clock::now();
        asm volatile("" : "+r"(hits)); // compiler barrier: hits must be real
        g_sink += hits;
        t.push_back(duration<double>(t1 - t0).count() * 1e9 / n);
    }
    std::sort(t.begin(), t.end());
    std::printf("%-34s %12.1f ns/op\n", name, t[t.size() / 2]);
}

int main() {
    printf("tg vs GEOS spike — %s\n", __DATE__);
    printf("GEOS %s\n", GEOS_VERSION);

    Rng rng;
    auto *factory = geos::geom::GeometryFactory::getDefaultInstance();
    geos::io::WKBReader reader(*factory);

    // Varied polygons: centers in [0,60]^2, radius 3..9, 16..128 vertices.
    std::vector<std::vector<uint8_t>> polys;
    polys.reserve(POLY_N);
    double total_v = 0;
    for (int i = 0; i < POLY_N; i++) {
        auto ring = star_ring(rng, rng.unit() * 60, rng.unit() * 60,
                              3.0 + rng.unit() * 6.0, rng.range(16, 128));
        total_v += ring.size();
        polys.push_back(wkb_poly(ring));
    }
    printf("polygons: %d, avg %.1f vertices, %.1f MB total\n",
           POLY_N, total_v / POLY_N,
           std::accumulate(polys.begin(), polys.end(), 0.0,
                           [](double a, const auto &p) { return a + p.size(); }) / 1e6);

    // Const area polygon for the point regime (128-gon, base radius 5 at origin).
    auto const_ring = star_ring(rng, 0, 0, 5.0, 128);
    auto const_poly_wkb = wkb_poly(const_ring);
    auto geom_const = std::unique_ptr<geos::geom::Geometry>(
        reader.read(const_poly_wkb.data(), const_poly_wkb.size()));

    // Points: uniform in (-7,7)^2.
    std::vector<uint8_t> pts(PTS_N * 21);
    for (int i = 0; i < PTS_N; i++) {
        auto w = wkb_point((rng.unit() - 0.5) * 14, (rng.unit() - 0.5) * 14);
        std::memcpy(pts.data() + (size_t)i * 21, w.data(), 21);
    }
    auto read_pt = [&](int i, double &x, double &y) {
        const uint8_t *p = pts.data() + (size_t)i * 21;
        std::memcpy(&x, p + 5, 8);  // little-endian, same offsets as columnar.hpp
        std::memcpy(&y, p + 13, 8);
    };

    // Varying lines: random walks in [0,60]^2, 5..33 vertices.
    std::vector<std::vector<uint8_t>> lines;
    lines.reserve(LINE_N);
    for (int i = 0; i < LINE_N; i++) {
        double x = rng.unit() * 60, y = rng.unit() * 60, hd = rng.unit() * 6.283;
        std::vector<std::pair<double, double>> lw;
        int n = rng.range(5, 33);
        for (int j = 0; j < n; j++) {
            lw.push_back({x, y});
            double st = 1.0 + rng.unit() * 2.0;
            x += st * std::cos(hd); y += st * std::sin(hd);
            hd += (rng.unit() - 0.5) * 1.2;
        }
        lines.push_back(wkb_line(lw));
    }

    // Both-varying pairs (bijective pairing for diversity).
    std::vector<uint32_t> ppa(PVARS_N), ppb(PVARS_N);
    for (int i = 0; i < PVARS_N; i++) {
        ppa[i] = (uint32_t)i % POLY_N;
        ppb[i] = (uint32_t)((size_t)i * 6151 + 13) % POLY_N;
    }

    // ═══ 1. parse ══════════════════════════════════════════════════════════
    printf("\n[1] WKB parse, %d polygons (%.1f verts avg), 1M ops\n",
           POLY_N, total_v / POLY_N);
    bench("tg parse TG_NONE        ", 1000000, [&](int n, uint64_t &h) {
        for (int i = 0; i < n; i++) {
            const auto &w = polys[i % POLY_N];
            auto *g = tg_parse_wkb(w.data(), w.size());
            h += g != nullptr;
            tg_geom_free(g);
        }
    });
    bench("tg parse TG_YSTRIPES    ", 1000000, [&](int n, uint64_t &h) {
        for (int i = 0; i < n; i++) {
            const auto &w = polys[i % POLY_N];
            auto *g = tg_parse_wkb_ix(w.data(), w.size(), TG_YSTRIPES);
            h += g != nullptr;
            tg_geom_free(g);
        }
    });
    bench("GEOS WKBReader::read    ", 1000000, [&](int n, uint64_t &h) {
        for (int i = 0; i < n; i++) {
            const auto &w = polys[i % POLY_N];
            auto g = std::unique_ptr<geos::geom::Geometry>(
                reader.read(w.data(), w.size()));
            h += g != nullptr;
        }
    });

    // ═══ 2. const polygon + varying points ═════════════════════════════════
    printf("\n[2] const 128-gon + varying 2D point (contains), %d points, 1M ops\n",
           PTS_N);
    {
        geos::algorithm::locate::IndexedPointInAreaLocator ipial(*geom_const);
        bench("GEOS IPIAL locate         ", 1000000, [&](int n, uint64_t &h) {
            for (int i = 0; i < n; i++) {
                double x, y; read_pt(i, x, y);
                geos::geom::CoordinateXY c{x, y};
                h += ipial.locate(&c) == geos::geom::Location::INTERIOR;
            }
        });
    }
    bench("GEOS SimplePointInArea  ", 1000000, [&](int n, uint64_t &h) {
        for (int i = 0; i < n; i++) {
            double x, y; read_pt(i, x, y);
            geos::geom::CoordinateXY c{x, y};
            h += geos::algorithm::locate::SimplePointInAreaLocator::locate(
                     c, geom_const.get())
                 == geos::geom::Location::INTERIOR;
        }
    });
    {
        auto tg_const = tg_parse_wkb_ix(const_poly_wkb.data(), const_poly_wkb.size(),
                                        TG_YSTRIPES);
        const auto *tg_poly = tg_geom_poly(tg_const);
        bench("tg ystripes contains_pt   ", 1000000, [&](int n, uint64_t &h) {
            for (int i = 0; i < n; i++) {
                double x, y; read_pt(i, x, y);
                h += tg_poly_contains_point(tg_poly, (struct tg_point){x, y});
            }
        });
        tg_geom_free(tg_const);
    }

    // ═══ 3. const polygon + varying line (st_intersects) ═══════════════════
    printf("\n[3] const 128-gon at (30,30) + varying LINESTRING (intersects), 1M ops\n");
    // Re-center const poly: rebuild one at (30,30) so lines actually hit it.
    auto const_ring2 = star_ring(rng, 30, 30, 8.0, 128);
    auto const_poly2_wkb = wkb_poly(const_ring2);
    auto geom_c2 = std::unique_ptr<geos::geom::Geometry>(
        reader.read(const_poly2_wkb.data(), const_poly2_wkb.size()));
    auto bbox_c2 = ch::wkb_bbox({const_poly2_wkb.data(), const_poly2_wkb.size()});
    auto prep_c2 = geos::geom::prep::PreparedGeometryFactory::prepare(geom_c2.get());
    {
        auto tg_c2 = tg_parse_wkb_ix(const_poly2_wkb.data(), const_poly2_wkb.size(),
                                     TG_YSTRIPES);
        bench("GEOS bbox+parse+prepared  ", 1000000, [&](int n, uint64_t &h) {
            for (int i = 0; i < n; i++) {
                const auto &w = lines[i % LINE_N];
                auto bs = ch::wkb_bbox({w.data(), w.size()});
                if (!bbox_c2.intersects(bs)) continue;
                auto g = std::unique_ptr<geos::geom::Geometry>(
                    reader.read(w.data(), w.size()));
                h += prep_c2->intersects(g.get());
            }
        });
        bench("tg parse+intersects(OBB)  ", 1000000, [&](int n, uint64_t &h) {
            for (int i = 0; i < n; i++) {
                const auto &w = lines[i % LINE_N];
                auto *g = tg_parse_wkb(w.data(), w.size());
                if (tg_rect_intersects_rect(tg_geom_rect(tg_c2), tg_geom_rect(g)))
                    h += tg_geom_intersects(tg_c2, g);
                tg_geom_free(g);
            }
        });
        tg_geom_free(tg_c2);
    }

    // ═══ 4. both varying (st_intersects) ═══════════════════════════════════
    printf("\n[4] both-varying polygon pairs (intersects), %d pairs\n", PVARS_N);
    bench("GEOS bbox+2xparse+inters", PVARS_N, [&](int n, uint64_t &h) {
        for (int i = 0; i < n; i++) {
            const auto &wa = polys[ppa[i]], &wb = polys[ppb[i]];
            auto ea = ch::wkb_bbox({wa.data(), wa.size()});
            auto eb = ch::wkb_bbox({wb.data(), wb.size()});
            if (!ea.intersects(eb)) continue;
            auto ga = std::unique_ptr<geos::geom::Geometry>(
                reader.read(wa.data(), wa.size()));
            auto gb = std::unique_ptr<geos::geom::Geometry>(
                reader.read(wb.data(), wb.size()));
            h += ga->intersects(gb.get());
        }
    });
    bench("tg 2xparse+intersects   ", PVARS_N, [&](int n, uint64_t &h) {
        for (int i = 0; i < n; i++) {
            const auto &wa = polys[ppa[i]], &wb = polys[ppb[i]];
            auto *ga = tg_parse_wkb(wa.data(), wa.size());
            auto *gb = tg_parse_wkb(wb.data(), wb.size());
            if (tg_rect_intersects_rect(tg_geom_rect(ga), tg_geom_rect(gb)))
                h += tg_geom_intersects(ga, gb);
            tg_geom_free(ga);
            tg_geom_free(gb);
        }
    });

    std::printf("\nsink=%llu (anti-DCE)\n", (unsigned long long)g_sink);

    // ═══ correctness cross-check (200k rows each) ══════════════════════════
    printf("\n[correctness, 200k rows each]\n");
    // points: GEOS IPIAL interior vs tg contains
    {
        geos::algorithm::locate::IndexedPointInAreaLocator ipial(*geom_const);
        auto tg_const = tg_parse_wkb_ix(const_poly_wkb.data(), const_poly_wkb.size(),
                                        TG_YSTRIPES);
        const auto *tg_poly = tg_geom_poly(tg_const);
        uint64_t mm = 0;
        for (int i = 0; i < 200000; i++) {
            double x, y; read_pt(i, x, y);
            geos::geom::CoordinateXY c{x, y};
            bool geos_i = ipial.locate(&c) == geos::geom::Location::INTERIOR;
            bool tg_i = tg_poly_contains_point(tg_poly, (struct tg_point){x, y});
            mm += geos_i != tg_i;
        }
        std::printf("point interior: GEOS IPIAL vs tg contains — mismatches: %llu\n",
                    (unsigned long long)mm);
        tg_geom_free(tg_const);
    }
    // lines: GEOS prepared vs tg
    {
        auto tg_c2 = tg_parse_wkb_ix(const_poly2_wkb.data(), const_poly2_wkb.size(),
                                     TG_YSTRIPES);
        uint64_t mm = 0, tested = 0;
        for (int i = 0; i < 200000; i++) {
            const auto &w = lines[i % LINE_N];
            auto bs = ch::wkb_bbox({w.data(), w.size()});
            auto *tg_l = tg_parse_wkb(w.data(), w.size());
            bool tg_r = tg_rect_intersects_rect(tg_geom_rect(tg_c2), tg_geom_rect(tg_l))
                         && tg_geom_intersects(tg_c2, tg_l);
            bool geos_r = false;
            if (bbox_c2.intersects(bs)) {
                auto g = std::unique_ptr<geos::geom::Geometry>(
                    reader.read(w.data(), w.size()));
                geos_r = prep_c2->intersects(g.get());
            }
            tested++;
            mm += geos_r != tg_r;
            tg_geom_free(tg_l);
        }
        std::printf("line/poly intersects: GEOS prepared vs tg — mismatches: %llu / %llu\n",
                    (unsigned long long)mm, (unsigned long long)tested);
        tg_geom_free(tg_c2);
    }
    // both varying
    {
        uint64_t mm = 0;
        for (int i = 0; i < 200000; i++) {
            const auto &wa = polys[ppa[i]], &wb = polys[ppb[i]];
            auto ea = ch::wkb_bbox({wa.data(), wa.size()});
            auto eb = ch::wkb_bbox({wb.data(), wb.size()});
            bool geos_r = false;
            if (ea.intersects(eb)) {
                auto ga = std::unique_ptr<geos::geom::Geometry>(
                    reader.read(wa.data(), wa.size()));
                auto gb = std::unique_ptr<geos::geom::Geometry>(
                    reader.read(wb.data(), wb.size()));
                geos_r = ga->intersects(gb.get());
            }
            auto *ta = tg_parse_wkb(wa.data(), wa.size());
            auto *tb = tg_parse_wkb(wb.data(), wb.size());
            bool tg_r = tg_rect_intersects_rect(tg_geom_rect(ta), tg_geom_rect(tb))
                         && tg_geom_intersects(ta, tb);
            mm += geos_r != tg_r;
            tg_geom_free(ta);
            tg_geom_free(tb);
        }
        std::printf("poly/poly intersects: GEOS full vs tg — mismatches: %llu / 200000\n",
                    (unsigned long long)mm);
    }
    return 0;
}
