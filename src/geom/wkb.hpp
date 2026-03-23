#pragma once

#include <cstdint>
#include <memory>
#include <span>

#include <geos/geom/Geometry.h>

#include "../io.hpp"

namespace ch {

using namespace geos::geom;
using namespace geos::io;

std::unique_ptr<Geometry> read_wkb(std::span<const uint8_t> input);
std::unique_ptr<Geometry> read_wkt(std::span<const uint8_t> input);
std::unique_ptr<Geometry> read_geojson(std::span<const uint8_t> input);
Vector write_ewkb(const std::unique_ptr<Geometry>& geometry);
std::string write_wkt(const std::unique_ptr<Geometry>& geometry, bool ewkt);

} // namespace ch
