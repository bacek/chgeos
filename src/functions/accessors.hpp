#pragma once

#include <cstdint>
#include <limits>
#include <stdexcept>
#include <string>

#include <geos/algorithm/distance/DiscreteHausdorffDistance.h>
#include <geos/algorithm/distance/DiscreteFrechetDistance.h>
#include <geos/geom/LineString.h>
#include <geos/geom/Point.h>
#include <geos/geom/Polygon.h>
#include <geos/operation/valid/IsValidOp.h>

#include "../geom/wkb.hpp"

namespace ch {

inline double st_x_impl(std::unique_ptr<Geometry> geometry) {
  const auto *pt = dynamic_cast<const Point *>(geometry.get());
  if (!pt) throw std::runtime_error("st_x: not a point");
  return pt->getX();
}

inline double st_y_impl(std::unique_ptr<Geometry> geometry) {
  const auto *pt = dynamic_cast<const Point *>(geometry.get());
  if (!pt) throw std::runtime_error("st_y: not a point");
  return pt->getY();
}

inline std::unique_ptr<Geometry> st_centroid_impl(std::unique_ptr<Geometry> geometry) {
  return geometry->getCentroid();
}

inline double st_area_impl(std::unique_ptr<Geometry> geometry) {
  return geometry->getArea();
}

// ST_NPoints: total vertex count across any geometry type (PostGIS semantics).
// ST_NumPoints (below) is the PostGIS variant restricted to LineString only.
inline int32_t st_npoints_impl(std::unique_ptr<Geometry> geometry) {
  return static_cast<int32_t>(geometry->getNumPoints());
}

inline int32_t st_srid_impl(std::unique_ptr<Geometry> geometry) {
  return geometry->getSRID();
}

inline double st_length_impl(std::unique_ptr<Geometry> geometry) {
  return geometry->getLength();
}

inline bool st_isvalid_impl(std::unique_ptr<Geometry> geometry) {
  return geometry->isValid();
}

inline bool st_isempty_impl(std::unique_ptr<Geometry> geometry) {
  return geometry->isEmpty();
}

inline bool st_issimple_impl(std::unique_ptr<Geometry> geometry) {
  return geometry->isSimple();
}

inline bool st_isring_impl(std::unique_ptr<Geometry> geometry) {
  const auto *ls = dynamic_cast<const LineString *>(geometry.get());
  if (!ls) return false;
  return ls->isRing();
}

inline std::string st_geometrytype_impl(std::unique_ptr<Geometry> geometry) {
  return std::string("ST_") + geometry->getGeometryType();
}

inline int32_t st_numgeometries_impl(std::unique_ptr<Geometry> geometry) {
  return static_cast<int32_t>(geometry->getNumGeometries());
}

inline int32_t st_dimension_impl(std::unique_ptr<Geometry> geometry) {
  return static_cast<int32_t>(geometry->getDimension());
}

inline std::unique_ptr<Geometry> st_interiorpoint_impl(std::unique_ptr<Geometry> geometry) {
  return geometry->getInteriorPoint();
}

inline std::string st_isvalidreason_impl(std::unique_ptr<Geometry> geometry) {
  geos::operation::valid::IsValidOp op(geometry.get());
  const auto * err = op.getValidationError();
  return err ? err->getMessage() : std::string("Valid Geometry");
}

inline int32_t st_numinteriorrings_impl(std::unique_ptr<Geometry> geometry) {
  const auto *poly = dynamic_cast<const geos::geom::Polygon *>(geometry.get());
  if (!poly) throw std::runtime_error("st_numinteriorrings: not a polygon");
  return static_cast<int32_t>(poly->getNumInteriorRing());
}

inline double st_hausdorffdistance_impl(std::unique_ptr<Geometry> a, std::unique_ptr<Geometry> b) {
  return geos::algorithm::distance::DiscreteHausdorffDistance::distance(*a, *b);
}

inline double st_hausdorffdistance_densify_impl(std::unique_ptr<Geometry> a, std::unique_ptr<Geometry> b, double densify_frac) {
  return geos::algorithm::distance::DiscreteHausdorffDistance::distance(*a, *b, densify_frac);
}

inline double st_frechetdistance_impl(std::unique_ptr<Geometry> a, std::unique_ptr<Geometry> b) {
  return geos::algorithm::distance::DiscreteFrechetDistance::distance(*a, *b);
}

inline double st_frechetdistance_densify_impl(std::unique_ptr<Geometry> a, std::unique_ptr<Geometry> b, double densify_frac) {
  return geos::algorithm::distance::DiscreteFrechetDistance::distance(*a, *b, densify_frac);
}

inline double st_z_impl(std::unique_ptr<Geometry> geometry) {
  const auto *pt = dynamic_cast<const Point *>(geometry.get());
  if (!pt) throw std::runtime_error("st_z: not a point");
  if (pt->isEmpty()) throw std::runtime_error("st_z: empty point");
  // CoordinateSequence gives us the full Coordinate including Z
  const auto *seq = pt->getCoordinatesRO();
  if (!seq || seq->size() == 0) return std::numeric_limits<double>::quiet_NaN();
  return seq->getAt<geos::geom::Coordinate>(0).z;
}

inline int32_t st_nrings_impl(std::unique_ptr<Geometry> geometry) {
  const auto *poly = dynamic_cast<const geos::geom::Polygon *>(geometry.get());
  if (!poly) throw std::runtime_error("st_nrings: not a polygon");
  return static_cast<int32_t>(1 + poly->getNumInteriorRing());
}

// ST_Perimeter delegates to getLength() — same as st_length — because GEOS's
// getLength() already returns the perimeter for polygonal types (sum of all
// ring lengths). Keeping both names reduces cognitive load for PostGIS users
// who expect st_perimeter on polygons and st_length on linear geometries.
inline double st_perimeter_impl(std::unique_ptr<Geometry> geometry) {
  return geometry->getLength();
}

// ST_NumPoints: PostGIS restricts this to LineString only (throws otherwise).
// Use ST_NPoints for a version that works on any geometry type.
inline int32_t st_numpoints_impl(std::unique_ptr<Geometry> geometry) {
  const auto *ls = dynamic_cast<const LineString *>(geometry.get());
  if (!ls) throw std::runtime_error("st_numpoints: not a linestring");
  return static_cast<int32_t>(ls->getNumPoints());
}

} // namespace ch
