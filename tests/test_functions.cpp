// Stubs for ClickHouse host imports — not available in native test builds.
// Must be compiled into the executable exactly once.
#include <cstdint>
#include <string>

#include "helpers.hpp"

extern "C" {
  void clickhouse_log(uint32_t, const uint8_t *, uint32_t) {}
  void clickhouse_throw(const uint8_t *msg, uint32_t len) {
    throw WasmPanicException(std::string(reinterpret_cast<const char *>(msg), len));
  }
  void clickhouse_random(uint8_t *buf, uint32_t len) {
    // Simple deterministic fill for native tests — entropy quality not needed.
    for (uint32_t i = 0; i < len; ++i) buf[i] = static_cast<uint8_t>(i ^ 0xa5u);
  }
}
