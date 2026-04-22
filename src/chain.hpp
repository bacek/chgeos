#pragma once

// WASM function chaining — eliminates intermediate WKB serialization for
// composed calls like st_length(st_makeline(a, b)).
//
// Instead of two WASM calls with a WKB column materialized between them,
// chain_execute runs the whole composition in a single call.  unique_ptr<Geometry>
// flows between steps on the stack; no handle pool, no cross-call lifetimes.
//
// Opt-in at two levels:
//   1. Module: export clickhouse_can_chain_execute — CH skips all chain logic if absent.
//   2. Function: register via CH_CHAIN_SOURCE / CH_CHAIN_XFORM / CH_CHAIN_SINK.
//
// clickhouse_chain_execute receives two separate buffers:
//   chain_buf: [uint32_t n_funcs][cstr name_0]...[cstr name_n-1]
//   row_buf:   standard COLUMNAR_V1 buffer (identical to what name_col receives)
// clickhouse_can_chain_execute still uses chain_buf format only.

#include <cstdint>
#include <cstring>
#include <functional>
#include <limits>
#include <memory>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <vector>

#include "columnar.hpp"
#include "geom/wkb.hpp"
#include "mem.hpp"

namespace ch {

using GeomPtr = std::unique_ptr<geos::geom::Geometry>;

enum class ChainRole { SOURCE, XFORM, SINK };

struct ChainFn {
    ChainRole role;
    uint32_t  source_arity = 0;  // non-zero only for SOURCE

    // SOURCE: reads source_arity WKB geometry columns from cb → vector of GeomPtr.
    std::function<std::vector<GeomPtr>(const ColumnarBuf&, uint32_t n)> as_source;
    // XFORM: moves GeomPtr vector in, returns transformed GeomPtr vector.
    std::function<std::vector<GeomPtr>(std::vector<GeomPtr>)>           as_xform;
    // SINK: consumes GeomPtr vector, allocates and returns the result raw_buffer.
    std::function<raw_buffer*(std::vector<GeomPtr>, uint32_t n)>        as_sink;
};

inline std::unordered_map<std::string, ChainFn>& chain_registry() {
    static std::unordered_map<std::string, ChainFn> reg;
    return reg;
}

// ── Arity deduction ───────────────────────────────────────────────────────────

template <typename Ret, typename... Args>
constexpr size_t impl_arity(Ret (*)(Args...)) { return sizeof...(Args); }

// ── SOURCE helper ─────────────────────────────────────────────────────────────
// Reads N WKB geometry columns from the columnar buffer, calls Impl per row.
// Rows where any column is null produce a nullptr handle.

template <auto Impl, size_t N>
std::vector<GeomPtr> chain_source_run(const ColumnarBuf& cb, uint32_t n) {
    std::array<ColView, N> cols;
    for (size_t j = 0; j < N; ++j)
        cols[j] = cb.col(static_cast<uint32_t>(j));

    std::vector<GeomPtr> result(n);
    for (uint32_t i = 0; i < n; ++i) {
        bool any_null = false;
        for (size_t j = 0; j < N; ++j) any_null |= cols[j].is_null(i);
        if (any_null) continue;  // result[i] stays nullptr

        result[i] = [&]<size_t... I>(std::index_sequence<I...>) {
            return Impl(read_wkb(cols[I].get_bytes(i))...);
        }(std::make_index_sequence<N>{});
    }
    return result;
}

// ── XFORM helper ──────────────────────────────────────────────────────────────
// Applies Impl to each non-null handle; nullptr propagates through.

template <auto Impl>
std::vector<GeomPtr> chain_xform_run(std::vector<GeomPtr> handles) {
    for (auto& h : handles) {
        if (!h) continue;
        h = Impl(std::move(h));
    }
    return handles;
}

// ── SINK helpers ──────────────────────────────────────────────────────────────
// Return type of Impl drives output column format.  Null handles → neutral value.

template <auto Impl>
raw_buffer* chain_sink_run(std::vector<GeomPtr> handles, uint32_t n) {
    using Ret = std::decay_t<decltype(Impl(std::declval<GeomPtr>()))>;

    if constexpr (std::is_same_v<Ret, double>) {
        raw_buffer* out = clickhouse_create_buffer(
            HEADER_BYTES + COL_DESC_BYTES + n * 8u);
        col_write_fixed_header<double>(out, n, COL_FIXED64);
        auto* res = reinterpret_cast<double*>(
            out->data() + HEADER_BYTES + COL_DESC_BYTES);
        for (uint32_t i = 0; i < n; ++i)
            res[i] = handles[i] ? Impl(std::move(handles[i]))
                                : std::numeric_limits<double>::quiet_NaN();
        return out;

    } else if constexpr (std::is_same_v<Ret, bool>) {
        raw_buffer* out = clickhouse_create_buffer(
            HEADER_BYTES + COL_DESC_BYTES + n);
        col_write_fixed_header<uint8_t>(out, n, COL_FIXED8);
        uint8_t* res = out->data() + HEADER_BYTES + COL_DESC_BYTES;
        for (uint32_t i = 0; i < n; ++i)
            res[i] = handles[i] ? (Impl(std::move(handles[i])) ? 1u : 0u) : 0u;
        return out;

    } else if constexpr (std::is_same_v<Ret, int32_t>) {
        raw_buffer* out = clickhouse_create_buffer(
            HEADER_BYTES + COL_DESC_BYTES + n * 4u);
        col_write_fixed_header<int32_t>(out, n, COL_FIXED32);
        auto* res = reinterpret_cast<int32_t*>(
            out->data() + HEADER_BYTES + COL_DESC_BYTES);
        for (uint32_t i = 0; i < n; ++i)
            res[i] = handles[i] ? Impl(std::move(handles[i])) : 0;
        return out;

    } else if constexpr (std::is_same_v<Ret, std::string>) {
        raw_buffer* out = clickhouse_create_buffer(0);
        ColBytesWriter w(out, n, /*nullable=*/false);
        for (uint32_t i = 0; i < n; ++i) {
            if (!handles[i]) { w.push_null(); continue; }
            std::string s = Impl(std::move(handles[i]));
            w.push_bytes({reinterpret_cast<const uint8_t*>(s.data()), s.size()});
        }
        w.finish();
        return out;

    } else if constexpr (std::is_same_v<Ret, GeomPtr>) {
        raw_buffer* out = clickhouse_create_buffer(0);
        ColBytesWriter w(out, n);
        for (uint32_t i = 0; i < n; ++i) {
            if (!handles[i]) { w.push_null(); continue; }
            w.push_geom(Impl(std::move(handles[i])));
        }
        w.finish();
        return out;
    }
}

// ── Chain descriptor parsing ──────────────────────────────────────────────────

// Parse function names from chain_buf: [n_funcs: u32][cstr name_0]...[cstr name_n-1]
inline std::vector<std::string> parse_chain_names(const raw_buffer* chain_buf) {
    const uint8_t* p = chain_buf->data();
    uint32_t n_funcs;
    std::memcpy(&n_funcs, p, 4);
    p += 4;

    std::vector<std::string> names;
    names.reserve(n_funcs);
    for (uint32_t i = 0; i < n_funcs; ++i) {
        const char* s = reinterpret_cast<const char*>(p);
        size_t len = std::strlen(s);
        names.emplace_back(s, len);
        p += len + 1;
    }
    return names;
}


// ── Chain validation ──────────────────────────────────────────────────────────
// Checks: all names registered, role ordering is SOURCE → XFORM* → SINK.

inline bool validate_chain(const std::vector<std::string>& names) {
    if (names.size() < 2) return false;
    auto& reg = chain_registry();
    for (auto& name : names)
        if (!reg.count(name)) return false;
    if (reg.at(names.front()).role != ChainRole::SOURCE) return false;
    if (reg.at(names.back()).role  != ChainRole::SINK)   return false;
    for (size_t i = 1; i + 1 < names.size(); ++i)
        if (reg.at(names[i]).role != ChainRole::XFORM) return false;
    return true;
}

// ── Chain execution ───────────────────────────────────────────────────────────

inline raw_buffer* chain_execute_impl(raw_buffer* chain_buf, raw_buffer* row_buf, uint32_t /*n*/) {
    auto fn_names = parse_chain_names(chain_buf);
    auto cb       = parse_columnar(row_buf);
    uint32_t n    = cb.num_rows;
    auto& reg     = chain_registry();

    // SOURCE
    auto handles = reg.at(fn_names[0]).as_source(cb, n);

    // XFORMs
    for (size_t i = 1; i + 1 < fn_names.size(); ++i)
        handles = reg.at(fn_names[i]).as_xform(std::move(handles));

    // SINK
    return reg.at(fn_names.back()).as_sink(std::move(handles), n);
}

} // namespace ch

// ── Registration macros ───────────────────────────────────────────────────────
// Call these inside a static initializer in main.cpp to opt functions in.

#define CH_CHAIN_SOURCE(name)                                                   \
    ch::chain_registry()[#name] = ch::ChainFn{                                  \
        .role = ch::ChainRole::SOURCE,                                           \
        .source_arity = static_cast<uint32_t>(                                  \
            ch::impl_arity(ch::name##_impl)),                                   \
        .as_source = [](const ch::ColumnarBuf& cb, uint32_t n)                  \
                -> std::vector<ch::GeomPtr> {                                    \
            constexpr size_t _N = ch::impl_arity(ch::name##_impl);              \
            return ch::chain_source_run<ch::name##_impl, _N>(cb, n);            \
        }                                                                        \
    }

#define CH_CHAIN_XFORM(name)                                                    \
    ch::chain_registry()[#name] = ch::ChainFn{                                  \
        .role = ch::ChainRole::XFORM,                                            \
        .as_xform = [](std::vector<ch::GeomPtr> h)                              \
                -> std::vector<ch::GeomPtr> {                                    \
            return ch::chain_xform_run<ch::name##_impl>(std::move(h));          \
        }                                                                        \
    }

#define CH_CHAIN_SINK(name)                                                     \
    ch::chain_registry()[#name] = ch::ChainFn{                                  \
        .role = ch::ChainRole::SINK,                                             \
        .as_sink = [](std::vector<ch::GeomPtr> h, uint32_t n)                   \
                -> ch::raw_buffer* {                                             \
            return ch::chain_sink_run<ch::name##_impl>(std::move(h), n);        \
        }                                                                        \
    }
