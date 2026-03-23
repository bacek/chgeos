#pragma once

#include <sstream>
#include <string>
#include <vector>

#include <geos/geom/Envelope.h>
#include <geos/geom/util/Densifier.h>
#include <geos/operation/buffer/BufferOp.h>
#include <geos/operation/buffer/BufferParameters.h>
#include <geos/operation/valid/MakeValid.h>
#include <geos/simplify/TopologyPreservingSimplifier.h>

#include "../geom/subdivide.hpp"
#include "../geom/wkb.hpp"

namespace ch {

inline std::unique_ptr<Geometry> st_buffer_impl(std::unique_ptr<Geometry> geometry, double radius) {
  return geometry->buffer(radius);
}

inline std::unique_ptr<Geometry> st_buffer_params_impl(std::unique_ptr<Geometry> geometry, double radius, std::span<const uint8_t> params_vec) {
  using namespace geos::operation::buffer;
  BufferParameters params;
  std::istringstream iss(std::string(params_vec.begin(), params_vec.end()));
  std::string token;
  while (iss >> token) {
    auto eq = token.find('=');
    if (eq == std::string::npos) continue;
    const std::string key = token.substr(0, eq);
    const std::string val = token.substr(eq + 1);
    if (key == "endcap") {
      if      (val == "round")                 params.setEndCapStyle(BufferParameters::CAP_ROUND);
      else if (val == "flat" || val == "butt") params.setEndCapStyle(BufferParameters::CAP_FLAT);
      else if (val == "square")                params.setEndCapStyle(BufferParameters::CAP_SQUARE);
    } else if (key == "join") {
      if      (val == "round")                     params.setJoinStyle(BufferParameters::JOIN_ROUND);
      else if (val == "mitre" || val == "miter")   params.setJoinStyle(BufferParameters::JOIN_MITRE);
      else if (val == "bevel")                     params.setJoinStyle(BufferParameters::JOIN_BEVEL);
    } else if (key == "mitre_limit" || key == "miter_limit") {
      params.setMitreLimit(std::stod(val));
    } else if (key == "quad_segs") {
      params.setQuadrantSegments(std::stoi(val));
    } else if (key == "side") {
      params.setSingleSided(val == "left" || val == "right");
    }
  }
  return BufferOp::bufferOp(geometry.get(), radius, params);
}

inline std::unique_ptr<Geometry> st_segmentize_impl(std::unique_ptr<Geometry> geometry, double max_segment_length) {
  return geos::geom::util::Densifier::densify(geometry.get(), max_segment_length);
}

inline std::unique_ptr<Geometry> st_simplify_impl(std::unique_ptr<Geometry> geometry, double tolerance) {
  return geos::simplify::TopologyPreservingSimplifier::simplify(geometry.get(), tolerance);
}

inline std::unique_ptr<Geometry> st_subdivide_impl(std::unique_ptr<Geometry> geometry, int32_t max_vertices) {
  GeometryFactory::Ptr factory = GeometryFactory::create();
  std::vector<std::unique_ptr<Geometry>> results;
  subdivide_recursive(geometry.get(), std::max(max_vertices, 5), results, factory.get());
  return factory->createGeometryCollection(std::move(results));
}

inline std::unique_ptr<Geometry> st_makevalid_impl(std::unique_ptr<Geometry> geometry) {
  geos::operation::valid::MakeValid maker;
  return maker.build(geometry.get());
}

inline std::unique_ptr<Geometry> st_expand_impl(std::unique_ptr<Geometry> geometry, double units_to_expand) {
  Envelope env(*geometry->getEnvelopeInternal());
  env.expandBy(units_to_expand);
  GeometryFactory::Ptr factory = GeometryFactory::create();
  return factory->toGeometry(&env);
}

} // namespace ch
