#pragma once

#include <vector>

#include <geos/geom/CoordinateSequence.h>
#include <geos/geom/Envelope.h>
#include <geos/geom/GeometryFactory.h>
#include <geos/operation/cluster/GeometryIntersectsClusterFinder.h>
#include <geos/operation/union/UnaryUnionOp.h>

#include "../geom/wkb.hpp"
#include "../clickhouse.hpp"

namespace ch {

inline std::unique_ptr<Geometry> st_union_impl(std::unique_ptr<Geometry> a, std::unique_ptr<Geometry> b) {
  return a->Union(b.get());
}

inline std::unique_ptr<Geometry> st_intersection_impl(std::unique_ptr<Geometry> a, std::unique_ptr<Geometry> b) {
  return a->intersection(b.get());
}

inline std::unique_ptr<Geometry> st_difference_impl(std::unique_ptr<Geometry> a, std::unique_ptr<Geometry> b) {
  return a->difference(b.get());
}

inline std::unique_ptr<Geometry> st_unaryunion_impl(std::unique_ptr<Geometry> geometry) {
  return geos::operation::geounion::UnaryUnionOp::Union(*geometry);
}

inline std::unique_ptr<Geometry> st_union_agg_impl(std::vector<std::unique_ptr<Geometry>> geoms) {
  GeometryFactory::Ptr factory = GeometryFactory::create();
  auto coll = factory->createGeometryCollection(std::move(geoms));
  return coll->Union();
}

inline std::unique_ptr<Geometry> st_collect_agg_impl(std::vector<std::unique_ptr<Geometry>> geoms) {
  GeometryFactory::Ptr factory = GeometryFactory::create();
  return factory->createGeometryCollection(std::move(geoms));
}

inline std::unique_ptr<Geometry> st_extent_agg_impl(std::vector<std::unique_ptr<Geometry>> geoms) {
  GeometryFactory::Ptr factory = GeometryFactory::create();
  geos::geom::Envelope env;
  for (const auto &g : geoms)
    env.expandToInclude(g->getEnvelopeInternal());
  return factory->toGeometry(&env);
}

inline std::unique_ptr<Geometry> st_makeline_agg_impl(std::vector<std::unique_ptr<Geometry>> geoms) {
  GeometryFactory::Ptr factory = GeometryFactory::create();
  auto seq = std::make_unique<geos::geom::CoordinateSequence>();
  for (const auto &g : geoms) {
    auto cs = g->getCoordinates();
    for (size_t i = 0; i < cs->size(); ++i) seq->add(cs->getAt(i));
  }
  return factory->createLineString(std::move(seq));
}

inline std::unique_ptr<Geometry> st_convexhull_agg_impl(std::vector<std::unique_ptr<Geometry>> geoms) {
  GeometryFactory::Ptr factory = GeometryFactory::create();
  auto coll = factory->createGeometryCollection(std::move(geoms));
  return coll->convexHull();
}

inline std::unique_ptr<Geometry> st_clusterintersecting_impl(std::unique_ptr<Geometry> geometry) {
  geos::operation::cluster::GeometryIntersectsClusterFinder finder;
  return finder.clusterToCollection(std::move(geometry));
}

} // namespace ch
