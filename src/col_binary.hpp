#pragma once

// COLUMNAR_V1 exports for ClickHouse WASM UDFs.
// All macros dispatch to columnar_impl_wrapper (from columnar.hpp).

#include "columnar.hpp"

namespace ch {

// ── CH_UDF_CB* macros: COLUMNAR_V1 exports ───────────────────────────────────

#define CH_UDF_CB(name)                                                      \
    __attribute__((export_name(#name "_cb")))                                \
    ch::raw_buffer * name##_cb(ch::raw_buffer * ptr,                         \
                               uint32_t num_rows) {                          \
        return ch::columnar_impl_wrapper(ptr, num_rows, ch::name##_impl);    \
    }

#define CH_UDF_CB_BBOX2(name, bbox_op, early_ret)                            \
    __attribute__((export_name(#name "_cb")))                                \
    ch::raw_buffer * name##_cb(ch::raw_buffer * ptr,                         \
                               uint32_t num_rows) {                          \
        return ch::columnar_impl_wrapper(ptr, num_rows, ch::name##_impl,     \
            ch::bbox_op, early_ret, ch::prep_a_##name, ch::prep_b_##name);   \
    }

#define CH_UDF_CB_BBOX2_POINT(name, bbox_op, early_ret)                      \
    __attribute__((export_name(#name "_cb")))                                \
    ch::raw_buffer * name##_cb(ch::raw_buffer * ptr,                         \
                               uint32_t num_rows) {                          \
        return ch::columnar_impl_wrapper(ptr, num_rows, ch::name##_impl,     \
            ch::bbox_op, early_ret, ch::prep_a_##name, ch::prep_b_##name,    \
            nullptr, nullptr,                                                \
            ch::prep_a_pt_##name, ch::prep_b_pt_##name);                     \
    }

// 1-arg accessor returning double, with the same ColWkbScalarOp fast path as
// CH_UDF_COL_WKB1 — both macros share columnar_impl_wrapper, so the two exports
// cannot drift apart.
#define CH_UDF_CB_WKB1(name)                                                 \
    __attribute__((export_name(#name "_cb")))                                \
    ch::raw_buffer * name##_cb(ch::raw_buffer * ptr,                         \
                               uint32_t num_rows) {                          \
        return ch::columnar_impl_wrapper(ptr, num_rows, ch::name##_impl,     \
            nullptr, false, nullptr, nullptr, nullptr, nullptr,              \
            nullptr, nullptr, ch::name##_wkb);                               \
    }

#define CH_UDF_CB_PRED3(name)                                                \
    __attribute__((export_name(#name "_cb")))                                \
    ch::raw_buffer * name##_cb(ch::raw_buffer * ptr,                         \
                               uint32_t num_rows) {                          \
        return ch::columnar_impl_wrapper(ptr, num_rows, ch::name##_impl,     \
            nullptr, false, nullptr, nullptr,                                \
            ch::prep_a_##name, ch::prep_b_##name);                           \
    }

} // namespace ch
