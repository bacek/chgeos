#pragma once

// Shared type for per-column PreparedGeometry callbacks.
// "I am the prepared geometry; other is the variable geometry for this row."

#include <geos/geom/Geometry.h>
#include <geos/geom/prep/PreparedGeometry.h>

namespace ch {

using ColPrepOp = bool (*)(const geos::geom::prep::PreparedGeometry*,
                           const geos::geom::Geometry*);

// Like ColPrepOp but carries an extra double (distance) — used for st_dwithin.
using ColPrepDistOp = bool (*)(const geos::geom::prep::PreparedGeometry*,
                               const geos::geom::Geometry*, double);

} // namespace ch
