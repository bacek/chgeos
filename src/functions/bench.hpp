#pragma once

#include <span>
#include <string_view>

#include "../clickhouse.hpp"
#include "../geom/wkb.hpp"
#include "../geom/wkb_envelope.hpp"
#include "predicates.hpp"

namespace ch {

// ── Benchmarking helpers ───────────────────────────────────────────────────────
// bench_noop: measures MsgPack round-trip + WASM call overhead.
// Call as bench_noop(CAST(location AS String)) — input is raw EWKB bin blob.
inline uint8_t geos_bench_noop_impl([[maybe_unused]] std::span<const uint8_t>) {
    return 1;
}

// bench_noop_rb: same overhead via CH-native wkb() serialization path.
// Call as bench_noop_rb(wkb(location)) to isolate CH serialization cost.
inline uint8_t geos_bench_noop_rb_impl([[maybe_unused]] std::span<const uint8_t>) {
    return 1;
}

// bench_wkb_parse: measures EWKB → GEOS parse cost only.
inline uint8_t geos_bench_wkb_parse_impl(std::span<const uint8_t> a) {
    return read_wkb(a) != nullptr ? 1 : 0;
}

// bench_envelope: measures fast WKB bbox extraction (no GEOS allocation).
inline uint8_t geos_bench_envelope_impl(std::span<const uint8_t> a) {
    return !wkb_bbox(a).is_empty() ? 1 : 0;
}

// bench_intersects_nobbox: st_intersects with no bbox shortcut — raw GEOS cost.
inline bool geos_bench_intersects_nobbox_impl(std::span<const uint8_t> a,
                                               std::span<const uint8_t> b) {
    return st_intersects_impl(read_wkb(a), read_wkb(b));
}

// ── Debug / test helpers ───────────────────────────────────────────────────────
// These exercise the host log and exception ABI from SQL.

inline uint32_t geos_log_test_impl(uint32_t level, std::string_view msg) {
    log(static_cast<log_level>(level), msg);
    return level;
}

[[noreturn]] inline uint32_t geos_test_exception_impl(std::string_view msg) {
    panic(msg);
}

} // namespace ch
