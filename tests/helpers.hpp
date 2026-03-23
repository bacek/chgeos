#pragma once
#include <cstring>
#include <memory>
#include <string>

#include "functions.hpp"

// Inline test helpers shared across all test_*.cpp files.

// Thrown by the native clickhouse_throw stub in test_functions.cpp.
// Using a distinct type lets tests assert that ch::panic() (and not some
// incidental std::runtime_error from elsewhere) is the source of the throw.
struct WasmPanicException : std::exception {
  explicit WasmPanicException(std::string msg_) : msg(std::move(msg_)) {}
  const char * what() const noexcept override { return msg.c_str(); }
  std::string msg;
};

inline ch::Vector wkt2wkb(const std::string &wkt) {
  return ch::write_ewkb(ch::read_wkt(ch::Vector(wkt.begin(), wkt.end())));
}

inline std::unique_ptr<ch::Geometry> geom(const std::string &wkt) {
  return ch::read_wkt(ch::Vector(wkt.begin(), wkt.end()));
}

inline std::string geom2wkt(const std::unique_ptr<ch::Geometry>& g) {
  return ch::write_wkt(g, true);
}

inline std::string wkb2wkt(const ch::Vector &wkb) {
  return geom2wkt(ch::read_wkb(wkb));
}

// WKB bytes as span — for predicate functions that now take raw bytes.
inline std::span<const uint8_t> wkb(const ch::Vector & v) {
  return {v.data(), v.size()};
}

inline ch::Vector params(const std::string &s) {
  return ch::Vector(s.begin(), s.end());
}
