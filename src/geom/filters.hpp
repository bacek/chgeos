#pragma once

#include <geos/geom/CoordinateSequence.h>
#include <geos/geom/CoordinateSequenceFilter.h>

namespace ch {

using namespace geos::geom;

struct TranslateFilter : CoordinateSequenceFilter {
  double dx, dy;
  TranslateFilter(double dx, double dy) : dx(dx), dy(dy) {}
  void filter_rw(CoordinateSequence &seq, std::size_t i) override {
    seq[i].x += dx;
    seq[i].y += dy;
  }
  bool isDone() const override { return false; }
  bool isGeometryChanged() const override { return true; }
};

struct ScaleFilter : CoordinateSequenceFilter {
  double xf, yf;
  ScaleFilter(double xf, double yf) : xf(xf), yf(yf) {}
  void filter_rw(CoordinateSequence &seq, std::size_t i) override {
    seq[i].x *= xf;
    seq[i].y *= yf;
  }
  bool isDone() const override { return false; }
  bool isGeometryChanged() const override { return true; }
};

} // namespace ch
