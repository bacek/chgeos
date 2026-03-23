#pragma once

#include <memory>
#include <vector>

#include <geos/geom/Envelope.h>
#include <geos/geom/Geometry.h>
#include <geos/geom/GeometryFactory.h>

namespace ch {

using namespace geos::geom;

inline void subdivide_recursive(const Geometry *geom, int32_t max_vertices,
                                std::vector<std::unique_ptr<Geometry>> &results,
                                const GeometryFactory *factory, int depth = 0) {
  if (geom->isEmpty()) return;
  if (depth > 64 || static_cast<int32_t>(geom->getNumPoints()) <= max_vertices) {
    results.push_back(geom->clone());
    return;
  }
  const Envelope *env = geom->getEnvelopeInternal();
  Envelope half1, half2;
  if (env->getWidth() >= env->getHeight()) {
    double mid = (env->getMinX() + env->getMaxX()) / 2.0;
    half1 = Envelope(env->getMinX(), mid,  env->getMinY(), env->getMaxY());
    half2 = Envelope(mid, env->getMaxX(),  env->getMinY(), env->getMaxY());
  } else {
    double mid = (env->getMinY() + env->getMaxY()) / 2.0;
    half1 = Envelope(env->getMinX(), env->getMaxX(), env->getMinY(), mid);
    half2 = Envelope(env->getMinX(), env->getMaxX(), mid, env->getMaxY());
  }
  auto clip1 = factory->toGeometry(&half1);
  auto clip2 = factory->toGeometry(&half2);
  auto part1 = geom->intersection(clip1.get());
  auto part2 = geom->intersection(clip2.get());
  subdivide_recursive(part1.get(), max_vertices, results, factory, depth + 1);
  subdivide_recursive(part2.get(), max_vertices, results, factory, depth + 1);
}

} // namespace ch
