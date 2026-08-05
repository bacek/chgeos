#pragma once

// Shared type for per-column PreparedGeometry callbacks.
// "I am the prepared geometry; other is the variable geometry for this row."

#include <geos/geom/Geometry.h>
#include <geos/geom/prep/PreparedGeometry.h>
#include <geos/geom/Location.h>

namespace ch {

using ColPrepOp = bool (*)(const geos::geom::prep::PreparedGeometry*,
                           const geos::geom::Geometry*);

// Like ColPrepOp but carries an extra double (distance) — used for st_dwithin.
using ColPrepDistOp = bool (*)(const geos::geom::prep::PreparedGeometry*,
                               const geos::geom::Geometry*, double);

// Fast path when the varying column contains only 2D WKB POINTs (21 bytes).
// The X,Y coordinates are extracted directly from raw WKB — no GEOS Geometry
// allocation per row, and the point is located against the const polygon with
// either an IndexedPointInAreaLocator or SimplePointInAreaLocator.
//
// This callback carries the only per-predicate part of that path: how the
// resulting Location maps to the predicate's truth value.  Keeping it here
// rather than in the caller is what makes BOUNDARY correct for covers /
// coveredby / intersects, which are true on the boundary while within /
// contains are false.
using ColPrepPointOp = bool (*)(geos::geom::Location);

} // namespace ch
