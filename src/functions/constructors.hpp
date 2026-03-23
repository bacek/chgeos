#pragma once

#include <stdexcept>
#include <vector>

#include <geos/algorithm/MinimumBoundingCircle.h>
#include <geos/geom/CoordinateSequence.h>
#include <geos/geom/GeometryFactory.h>
#include <geos/geom/Coordinate.h>
#include <geos/geom/Envelope.h>
#include <geos/geom/LinearRing.h>
#include <geos/geom/LineString.h>
#include <geos/geom/Point.h>
#include <geos/geom/Polygon.h>
#include <geos/noding/GeometryNoder.h>
#include <geos/operation/distance/DistanceOp.h>
#include <geos/operation/linemerge/LineMerger.h>
#include <geos/operation/overlay/snap/GeometrySnapper.h>
#include <geos/operation/polygonize/Polygonizer.h>
#include <geos/operation/buffer/OffsetCurve.h>
#include <geos/operation/sharedpaths/SharedPathsOp.h>
#include <geos/triangulate/DelaunayTriangulationBuilder.h>
#include <geos/triangulate/VoronoiDiagramBuilder.h>

#include "../geom/wkb.hpp"

namespace ch {

// I/O converters: these take raw text/bytes, not decoded geometry.
inline Vector st_geomfromtext_impl(std::span<const uint8_t> input) {
  return write_ewkb(read_wkt(input));
}

inline Vector st_geomfromwkb_impl(std::span<const uint8_t> input) {
  return write_ewkb(read_wkb(input));
}

inline std::unique_ptr<Geometry> st_extent_impl(std::unique_ptr<Geometry> geometry) {
  GeometryFactory::Ptr factory = GeometryFactory::create();
  return factory->toGeometry(geometry->getEnvelopeInternal());
}

inline std::unique_ptr<Geometry> st_envelope_impl(std::unique_ptr<Geometry> geometry) {
  return geometry->getEnvelope();
}

inline std::unique_ptr<Geometry> st_startpoint_impl(std::unique_ptr<Geometry> geometry) {
  const auto *ls = dynamic_cast<const LineString *>(geometry.get());
  if (!ls) throw std::runtime_error("st_startpoint: not a linestring");
  if (ls->isEmpty()) throw std::runtime_error("st_startpoint: empty linestring");
  return ls->getPointN(0)->clone();
}

inline std::unique_ptr<Geometry> st_endpoint_impl(std::unique_ptr<Geometry> geometry) {
  const auto *ls = dynamic_cast<const LineString *>(geometry.get());
  if (!ls) throw std::runtime_error("st_endpoint: not a linestring");
  if (ls->isEmpty()) throw std::runtime_error("st_endpoint: empty linestring");
  return ls->getPointN(ls->getNumPoints() - 1)->clone();
}

inline std::unique_ptr<Geometry> st_collect_impl(std::unique_ptr<Geometry> a, std::unique_ptr<Geometry> b) {
  GeometryFactory::Ptr factory = GeometryFactory::create();
  std::vector<std::unique_ptr<Geometry>> geoms;
  geoms.push_back(std::move(a));
  geoms.push_back(std::move(b));
  return factory->createGeometryCollection(std::move(geoms));
}

inline std::unique_ptr<Geometry> st_makebox2d_impl(std::unique_ptr<Geometry> low_left, std::unique_ptr<Geometry> up_right) {
  const auto *pll = dynamic_cast<const Point *>(low_left.get());
  const auto *pur = dynamic_cast<const Point *>(up_right.get());
  if (!pll || !pur) throw std::runtime_error("st_makebox2d: both arguments must be points");
  Envelope env(pll->getX(), pur->getX(), pll->getY(), pur->getY());
  GeometryFactory::Ptr factory = GeometryFactory::create();
  return factory->toGeometry(&env);
}

inline std::unique_ptr<Geometry> st_convexhull_impl(std::unique_ptr<Geometry> geometry) {
  return geometry->convexHull();
}

inline std::unique_ptr<Geometry> st_boundary_impl(std::unique_ptr<Geometry> geometry) {
  return geometry->getBoundary();
}

inline std::unique_ptr<Geometry> st_reverse_impl(std::unique_ptr<Geometry> geometry) {
  return geometry->reverse();
}

inline std::unique_ptr<Geometry> st_normalize_impl(std::unique_ptr<Geometry> geometry) {
  auto result = geometry->clone();
  result->normalize();
  return result;
}

inline std::unique_ptr<Geometry> st_geometryn_impl(std::unique_ptr<Geometry> geometry, int32_t n) {
  auto num = static_cast<int32_t>(geometry->getNumGeometries());
  if (n < 1 || n > num)
    throw std::runtime_error("st_geometryn: index out of range (1-based)");
  return geometry->getGeometryN(static_cast<size_t>(n - 1))->clone();
}

inline std::unique_ptr<Geometry> st_symdifference_impl(std::unique_ptr<Geometry> a, std::unique_ptr<Geometry> b) {
  return a->symDifference(b.get());
}

inline std::unique_ptr<Geometry> st_exteriorring_impl(std::unique_ptr<Geometry> geometry) {
  const auto *poly = dynamic_cast<const geos::geom::Polygon *>(geometry.get());
  if (!poly) throw std::runtime_error("st_exteriorring: not a polygon");
  return poly->getExteriorRing()->clone();
}

inline std::unique_ptr<Geometry> st_interiorringn_impl(std::unique_ptr<Geometry> geometry, int32_t n) {
  const auto *poly = dynamic_cast<const geos::geom::Polygon *>(geometry.get());
  if (!poly) throw std::runtime_error("st_interiorringn: not a polygon");
  auto num = static_cast<int32_t>(poly->getNumInteriorRing());
  if (n < 1 || n > num) throw std::runtime_error("st_interiorringn: index out of range (1-based)");
  return poly->getInteriorRingN(static_cast<size_t>(n - 1))->clone();
}

inline std::unique_ptr<Geometry> st_pointn_impl(std::unique_ptr<Geometry> geometry, int32_t n) {
  const auto *ls = dynamic_cast<const LineString *>(geometry.get());
  if (!ls) throw std::runtime_error("st_pointn: not a linestring");
  auto num = static_cast<int32_t>(ls->getNumPoints());
  if (n < 1 || n > num) throw std::runtime_error("st_pointn: index out of range (1-based)");
  return ls->getPointN(static_cast<size_t>(n - 1))->clone();
}

inline std::unique_ptr<Geometry> st_minimumboundingcircle_impl(std::unique_ptr<Geometry> geometry) {
  geos::algorithm::MinimumBoundingCircle mbc(geometry.get());
  return mbc.getCircle();
}

inline std::unique_ptr<Geometry> st_snap_impl(std::unique_ptr<Geometry> a, std::unique_ptr<Geometry> b, double tolerance) {
  geos::geom::GeomPtrPair result;
  geos::operation::overlay::snap::GeometrySnapper::snap(*a, *b, tolerance, result);
  return std::move(result.first);
}

inline std::unique_ptr<Geometry> st_offsetcurve_impl(std::unique_ptr<Geometry> geometry, double distance) {
  geos::operation::buffer::OffsetCurve oc(*geometry, distance);
  return oc.getCurve();
}

inline std::unique_ptr<Geometry> st_linmerge_impl(std::unique_ptr<Geometry> geometry) {
  geos::operation::linemerge::LineMerger merger;
  merger.add(geometry.get());
  auto lines = merger.getMergedLineStrings();
  GeometryFactory::Ptr factory = GeometryFactory::create();
  std::vector<std::unique_ptr<Geometry>> geoms;
  for (auto &l : lines) geoms.push_back(std::move(l));
  return factory->createGeometryCollection(std::move(geoms));
}

inline std::unique_ptr<Geometry> st_polygonize_impl(std::unique_ptr<Geometry> geometry) {
  geos::operation::polygonize::Polygonizer polygonizer;
  polygonizer.add(geometry.get());
  auto polys = polygonizer.getPolygons();
  GeometryFactory::Ptr factory = GeometryFactory::create();
  std::vector<std::unique_ptr<Geometry>> geoms;
  for (auto &p : polys) geoms.push_back(std::move(p));
  return factory->createGeometryCollection(std::move(geoms));
}

inline std::unique_ptr<Geometry> st_delaunaytriangles_impl(std::unique_ptr<Geometry> geometry, double tolerance, int32_t only_edges) {
  geos::triangulate::DelaunayTriangulationBuilder builder;
  builder.setTolerance(tolerance);
  builder.setSites(*geometry);
  GeometryFactory::Ptr factory = GeometryFactory::create();
  return only_edges ? builder.getEdges(*factory) : builder.getTriangles(*factory);
}

inline std::unique_ptr<Geometry> st_voronoidiagram_impl(std::unique_ptr<Geometry> geometry, double tolerance, int32_t only_edges) {
  geos::triangulate::VoronoiDiagramBuilder builder;
  builder.setTolerance(tolerance);
  builder.setSites(*geometry);
  GeometryFactory::Ptr factory = GeometryFactory::create();
  return only_edges ? builder.getDiagramEdges(*factory) : builder.getDiagram(*factory);
}

inline std::unique_ptr<Geometry> st_makepoint_impl(double x, double y) {
  GeometryFactory::Ptr factory = GeometryFactory::create();
  return factory->createPoint(geos::geom::Coordinate(x, y));
}

inline std::unique_ptr<Geometry> st_makepoint3d_impl(double x, double y, double z) {
  GeometryFactory::Ptr factory = GeometryFactory::create();
  return factory->createPoint(geos::geom::Coordinate(x, y, z));
}

inline std::unique_ptr<Geometry> st_makepolygon_impl(std::unique_ptr<Geometry> shell) {
  const auto *ls = dynamic_cast<const LineString *>(shell.get());
  if (!ls) throw std::runtime_error("st_makepolygon: shell must be a linestring");
  GeometryFactory::Ptr factory = GeometryFactory::create();
  auto ring = factory->createLinearRing(ls->getCoordinatesRO()->clone());
  return factory->createPolygon(std::move(ring));
}

inline std::unique_ptr<Geometry> st_makeline_impl(std::unique_ptr<Geometry> a, std::unique_ptr<Geometry> b) {
  GeometryFactory::Ptr factory = GeometryFactory::create();
  auto seq = std::make_unique<geos::geom::CoordinateSequence>();
  auto append = [&](const Geometry *g) {
    auto cs = g->getCoordinates();
    for (size_t i = 0; i < cs->size(); ++i) seq->add(cs->getAt(i));
  };
  append(a.get());
  append(b.get());
  return factory->createLineString(std::move(seq));
}

inline std::unique_ptr<Geometry> st_closestpoint_impl(std::unique_ptr<Geometry> a, std::unique_ptr<Geometry> b) {
  auto pts = geos::operation::distance::DistanceOp::nearestPoints(a.get(), b.get());
  GeometryFactory::Ptr factory = GeometryFactory::create();
  return factory->createPoint(pts->getAt(0));
}

inline std::unique_ptr<Geometry> st_shortestline_impl(std::unique_ptr<Geometry> a, std::unique_ptr<Geometry> b) {
  auto pts = geos::operation::distance::DistanceOp::nearestPoints(a.get(), b.get());
  GeometryFactory::Ptr factory = GeometryFactory::create();
  return factory->createLineString(std::move(pts));
}

inline std::unique_ptr<Geometry> st_sharedpaths_impl(std::unique_ptr<Geometry> a, std::unique_ptr<Geometry> b) {
  geos::operation::sharedpaths::SharedPathsOp::PathList same, opp;
  geos::operation::sharedpaths::SharedPathsOp::sharedPathsOp(*a, *b, same, opp);
  GeometryFactory::Ptr factory = GeometryFactory::create();
  auto to_geoms = [&](geos::operation::sharedpaths::SharedPathsOp::PathList & lst) {
    std::vector<std::unique_ptr<Geometry>> v;
    for (auto *l : lst) v.emplace_back(l);
    return factory->createGeometryCollection(std::move(v));
  };
  std::vector<std::unique_ptr<Geometry>> parts;
  parts.push_back(to_geoms(same));
  parts.push_back(to_geoms(opp));
  return factory->createGeometryCollection(std::move(parts));
}

inline std::unique_ptr<Geometry> st_node_impl(std::unique_ptr<Geometry> geometry) {
  return geos::noding::GeometryNoder::node(*geometry);
}

inline std::unique_ptr<Geometry> st_addpoint_impl(std::unique_ptr<Geometry> line, std::unique_ptr<Geometry> point, int32_t pos) {
  const auto *ls = dynamic_cast<const LineString *>(line.get());
  const auto *pt = dynamic_cast<const Point *>(point.get());
  if (!ls || !pt) throw std::runtime_error("st_addpoint: requires linestring and point");
  auto seq = ls->getCoordinatesRO()->clone();
  int32_t n = static_cast<int32_t>(seq->size());
  int32_t idx = (pos < 0 || pos > n) ? n : pos;
  seq->add(idx, pt->getCoordinate() ? *pt->getCoordinate() : geos::geom::Coordinate(), false);
  GeometryFactory::Ptr factory = GeometryFactory::create();
  return factory->createLineString(std::move(seq));
}

inline std::unique_ptr<Geometry> st_removepoint_impl(std::unique_ptr<Geometry> line, int32_t pos) {
  const auto *ls = dynamic_cast<const LineString *>(line.get());
  if (!ls) throw std::runtime_error("st_removepoint: not a linestring");
  const auto *src = ls->getCoordinatesRO();
  auto n = static_cast<int32_t>(src->size());
  if (pos < 0 || pos >= n) throw std::runtime_error("st_removepoint: index out of range");
  auto seq = std::make_unique<geos::geom::CoordinateSequence>();
  for (int32_t i = 0; i < n; ++i)
    if (i != pos) seq->add(src->getAt(i));
  GeometryFactory::Ptr factory = GeometryFactory::create();
  return factory->createLineString(std::move(seq));
}

inline std::unique_ptr<Geometry> st_setpoint_impl(std::unique_ptr<Geometry> line, int32_t pos, std::unique_ptr<Geometry> point) {
  const auto *ls = dynamic_cast<const LineString *>(line.get());
  const auto *pt = dynamic_cast<const Point *>(point.get());
  if (!ls || !pt) throw std::runtime_error("st_setpoint: requires linestring and point");
  auto seq = ls->getCoordinatesRO()->clone();
  auto n = static_cast<int32_t>(seq->size());
  if (pos < 0 || pos >= n) throw std::runtime_error("st_setpoint: index out of range");
  if (!pt->getCoordinate()) throw std::runtime_error("st_setpoint: empty point");
  seq->setAt(*pt->getCoordinate(), static_cast<size_t>(pos));
  GeometryFactory::Ptr factory = GeometryFactory::create();
  return factory->createLineString(std::move(seq));
}

} // namespace ch
