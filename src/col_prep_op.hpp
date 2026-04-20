#pragma once

// Shared type for per-column PreparedGeometry callbacks.
// "I am the prepared geometry; other is the variable geometry for this row."

#include <geos/geom/Geometry.h>
#include <geos/geom/prep/PreparedGeometry.h>
#include <geos/algorithm/locate/IndexedPointInAreaLocator.h>

namespace ch {

using ColPrepOp = bool (*)(const geos::geom::prep::PreparedGeometry*,
                           const geos::geom::Geometry*);

// Like ColPrepOp but carries an extra double (distance) — used for st_dwithin.
using ColPrepDistOp = bool (*)(const geos::geom::prep::PreparedGeometry*,
                               const geos::geom::Geometry*, double);

// Fast path when the varying column contains only 2D WKB POINTs (21 bytes).
// The X,Y coordinates are extracted directly from raw WKB — no GEOS Geometry
// allocation per row.  The IndexedPointInAreaLocator is built once from the
// const polygon column and reused for every point in the batch.
using ColPrepPointOp = bool (*)(geos::algorithm::locate::IndexedPointInAreaLocator*,
                                double /*x*/, double /*y*/);

} // namespace ch
