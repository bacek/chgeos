#pragma once

#include <cstdint>
#include <span>
#include <string_view>

extern "C" {
  void clickhouse_throw(const uint8_t* msg, uint32_t len);
  void clickhouse_log(uint32_t level, const uint8_t* msg, uint32_t len);
  // Fill buf[0..len) with cryptographically-seeded random bytes (host RNG).
  void clickhouse_random(uint8_t* buf, uint32_t len);
}

namespace ch {

// Mirror of Poco::Message::Priority (Poco/Message.h).
// fatal (1), critical (2), error (3) are intentionally omitted: the host clamps
// any level more severe than warning to warning to prevent WASM modules from
// triggering alerting systems or misrepresenting ClickHouse health.
enum class log_level : uint8_t {
  warning     = 4,
  notice      = 5,
  information = 6,
  debug       = 7,
  trace       = 8,
};

  inline void log(std::string_view str) {
    clickhouse_log(static_cast<uint32_t>(log_level::debug),
                   reinterpret_cast<const uint8_t*>(str.data()), str.size());
  }

  inline void log(log_level level, std::string_view str) {
    clickhouse_log(static_cast<uint32_t>(level),
                   reinterpret_cast<const uint8_t*>(str.data()), str.size());
  }

  [[noreturn]] inline void panic(std::string_view str) {
    log(str);
    clickhouse_throw(reinterpret_cast<const uint8_t*>(str.data()), str.size());
    __builtin_unreachable();
  };

}
