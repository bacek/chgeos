#include "mem.hpp"
#include <cstdint>
#include <cstring>

// getentropy is unavailable in WASM/WASI. GEOS uses std::random_device (which
// calls getentropy) in snap rounding. Provide a simple stub — snap rounding
// only needs some entropy to avoid degenerate cases, not cryptographic quality.
#include "clickhouse.hpp"

#ifdef __EMSCRIPTEN__
#include <wasi/api.h>

// Stub out the last residual WASI import (fd_close called from libc dtors).
extern "C" __wasi_errno_t __wasi_fd_close(__wasi_fd_t) { return 0; }

// getentropy is called by std::random_device (used in GEOS snap rounding).
// Delegate to the host RNG so entropy quality matches the ClickHouse server.
extern "C" int getentropy(void *buffer, size_t length) {
    // getentropy(2) guarantees length <= 256; WASM32 size_t is 32-bit anyway.
    // Panic if somehow called with a larger request rather than silently truncating.
    if (length > UINT32_MAX) ch::panic("getentropy: length exceeds uint32_t");
    clickhouse_random(static_cast<uint8_t *>(buffer), static_cast<uint32_t>(length));
    return 0;
}

// Emscripten imports this from the JS host when an exception escapes uncaught.
// Since impl_wrapper catches everything, this should never be reached.
// Define it here so it stays in-module (not an env.* import) and routes through
// clickhouse_throw for safety.
extern "C" [[noreturn]] void __throw_exception_with_stack_trace(void *) {
    static constexpr char msg[] = "__throw_exception_with_stack_trace: uncaught C++ exception";
    clickhouse_throw(reinterpret_cast<const uint8_t *>(msg), sizeof(msg) - 1);
    __builtin_unreachable();
}

// Emscripten imports this from JS host when memory grows (ALLOW_MEMORY_GROWTH=1).
// ClickHouse doesn't provide it; stub it as a no-op to keep it in-module.
extern "C" void emscripten_notify_memory_growth(int) {}

// Override assert failure to propagate as a ClickHouse exception instead of abort().
extern "C" [[noreturn]] void __assert_fail(
    const char *assertion, const char *file, unsigned int line, const char *function)
{
    char buf[512];
    int n = __builtin_snprintf(buf, sizeof(buf), "assert(%s) failed at %s:%u in %s",
                               assertion, file, line, function);
    clickhouse_throw(reinterpret_cast<const uint8_t *>(buf),
                     static_cast<uint32_t>(n > 0 ? n : 0));
    __builtin_unreachable();
}
#endif // __EMSCRIPTEN__

extern "C" {
__attribute__((export_name("clickhouse_create_buffer")))
ch::raw_buffer *
clickhouse_create_buffer(uint32_t size) {
  return new (malloc(sizeof(ch::raw_buffer))) ch::raw_buffer(size);
}

__attribute__((export_name("clickhouse_destroy_buffer")))
void
clickhouse_destroy_buffer(uint8_t *buf) {
  ch::record_destroyed_handle(reinterpret_cast<uintptr_t>(buf));
  ch::raw_buffer *raw = reinterpret_cast<ch::raw_buffer *>(buf);
  raw->~raw_buffer();
  free(raw);
}

}
