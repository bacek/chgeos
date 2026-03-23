#pragma once

#include <cstdint>
#include <stdexcept>

#include "../geom/filters.hpp"
#include "../geom/wkb.hpp"

namespace ch {

inline std::unique_ptr<Geometry> st_translate_impl(std::unique_ptr<Geometry> geometry, double dx, double dy) {
  TranslateFilter f{dx, dy};
  geometry->apply_rw(f);
  geometry->geometryChanged();
  return geometry;
}

inline std::unique_ptr<Geometry> st_scale_impl(std::unique_ptr<Geometry> geometry, double xf, double yf) {
  ScaleFilter f{xf, yf};
  geometry->apply_rw(f);
  geometry->geometryChanged();
  return geometry;
}

inline std::unique_ptr<Geometry> st_transform_impl(std::unique_ptr<Geometry>, uint32_t) {
  throw std::runtime_error("st_transform: PROJ not linked");
}

inline std::unique_ptr<Geometry> st_transform_proj_impl(std::unique_ptr<Geometry>, std::span<const uint8_t>) {
  throw std::runtime_error("st_transform_proj: PROJ not linked");
}

// srid is UInt32 to match the SQL declaration in create.sql (ARGUMENTS srid UInt32).
inline std::unique_ptr<Geometry> st_setsrid_impl(std::unique_ptr<Geometry> geometry, uint32_t srid) {
  geometry->setSRID(srid);
  return geometry;
}

} // namespace ch
