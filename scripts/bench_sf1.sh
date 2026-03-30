#!/usr/bin/env bash
# SF1 geospatial benchmark suite for chgeos WASM UDFs.
#
# Usage:
#   ./scripts/bench_sf1.sh [path/to/clickhouse] [path/to/data/dir]
#
# The data directory must contain:
#   trip.parquet, zone.parquet, building.parquet, customer.parquet

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

CH="${1:-$(command -v clickhouse 2>/dev/null)}"
[[ -x "${CH}" ]] || { echo "ERROR: clickhouse binary not found; pass as first argument or put on PATH"; exit 1; }

DATA_DIR="${2:?ERROR: data directory required as second argument}"
[[ -d "${DATA_DIR}" ]] || { echo "ERROR: data directory '${DATA_DIR}' does not exist"; exit 1; }

PORT="${CH_PORT:-19000}"
FUEL="SETTINGS webassembly_udf_max_fuel=0"
RUNS="${BENCH_RUNS:-5}"

TRIP="${DATA_DIR}/trip.parquet"
ZONE="${DATA_DIR}/zone.parquet"
BUILDING="${DATA_DIR}/building.parquet"
CUSTOMER="${DATA_DIR}/customer.parquet"

run_once() {
    local query="$1"
    result=$( { time "${CH}" client --port "${PORT}" -q "${query}" 2>/dev/null; } 2>&1 )
    echo "${result}" | grep real | awk '{
        split($2, t, /m|s/); print int((t[1]*60 + t[2])*1000+0.5)
    }'
}

run() {
    local label="$1"; local query="$2"

    local times=() sum=0 min=999999 max=0
    for (( i=0; i<RUNS; i++ )); do
        local ms
        ms=$(run_once "${query}")
        times+=("${ms}")
        sum=$(( sum + ms ))
        (( ms < min )) && min=${ms}
        (( ms > max )) && max=${ms}
    done
    local avg=$(( sum / RUNS ))

    printf "| %-6s | %8s | %8s | %8s |\n" \
        "${label}" "${min}ms" "${avg}ms" "${max}ms"
}

echo ""
echo "Data dir: ${DATA_DIR}  (${RUNS} runs each)"
trip_count=$("${CH}" client --port "${PORT}" -q \
    "SELECT count() FROM file('${TRIP}', Parquet)" 2>/dev/null || echo '?')
echo "trip rows: ${trip_count}"
echo ""
printf "| %-6s | %8s | %8s | %8s |\n" "Query" "min" "avg" "max"
echo  "|--------|----------|----------|----------|"

# q1: trips within 50km of Sedona city center
run "Q1" \
"SELECT t_tripkey, st_x(t_pickuploc), st_y(t_pickuploc), t_pickuptime,
    st_distance(t_pickuploc, st_geomfromtext('POINT (-111.7610 34.8697)')) AS distance_to_center
 FROM file('${TRIP}', Parquet)
 WHERE st_dwithin(t_pickuploc, st_geomfromtext('POINT (-111.7610 34.8697)'), 0.45)
 ORDER BY distance_to_center ASC, t_tripkey ASC
 ${FUEL}"

# q2: count trips within Coconino County
run "Q2" \
"SELECT count() AS trip_count
 FROM file('${TRIP}', Parquet) t
 WHERE st_intersects(t.t_pickuploc,
     (SELECT z_boundary FROM file('${ZONE}', Parquet) WHERE z_name = 'Coconino County' LIMIT 1))
 ${FUEL}"

# q3: monthly stats within bounding box + buffer
run "Q3" \
"SELECT toStartOfMonth(t_pickuptime) AS pickup_month,
    count(t_tripkey) AS total_trips,
    avg(t_distance) AS avg_distance,
    avg(t_fare) AS avg_fare
 FROM file('${TRIP}', Parquet)
 WHERE st_dwithin(t_pickuploc,
     st_geomfromtext('POLYGON((-111.9060 34.7347,-111.6160 34.7347,-111.6160 35.0047,-111.9060 35.0047,-111.9060 34.7347))'),
     0.045)
 GROUP BY pickup_month
 ORDER BY pickup_month
 ${FUEL}"

# q4: zone distribution of top-1000 trips by tip
run "Q4" \
"SELECT z.z_zonekey, z.z_name, count() AS trip_count
 FROM file('${ZONE}', Parquet) z
 JOIN (SELECT t_pickuploc FROM file('${TRIP}', Parquet) ORDER BY t_tip DESC, t_tripkey ASC LIMIT 1000) top_trips
   ON st_within(top_trips.t_pickuploc, z.z_boundary)
 GROUP BY z.z_zonekey, z.z_name
 ORDER BY trip_count DESC, z.z_zonekey ASC
 ${FUEL}"

# q5: monthly convex hull area per repeat customer
run "Q5" \
"SELECT c.c_custkey, c.c_name AS customer_name,
    toStartOfMonth(t.t_pickuptime) AS pickup_month,
    st_area(st_convexhull(st_collect_agg(t.t_dropoffloc))) AS monthly_travel_hull_area,
    count() AS dropoff_count
 FROM file('${TRIP}', Parquet) t
 JOIN file('${CUSTOMER}', Parquet) c ON t.t_custkey = c.c_custkey
 GROUP BY c.c_custkey, c.c_name, pickup_month
 HAVING dropoff_count > 5
 ORDER BY dropoff_count DESC, c.c_custkey ASC
 ${FUEL}"

# q6: zone stats for trips in bbox-intersecting zones
run "Q6" \
"SELECT z.z_zonekey, z.z_name,
    count(t.t_tripkey) AS total_pickups,
    avg(t.t_totalamount) AS avg_amount,
    avg(t.t_dropofftime - t.t_pickuptime) AS avg_duration
 FROM file('${TRIP}', Parquet) t, file('${ZONE}', Parquet) z
 WHERE st_intersects(st_geomfromtext('POLYGON((-112.2110 34.4197,-111.3110 34.4197,-111.3110 35.3197,-112.2110 35.3197,-112.2110 34.4197))'), z.z_boundary)
   AND st_within(t.t_pickuploc, z.z_boundary)
 GROUP BY z.z_zonekey, z.z_name
 ORDER BY total_pickups DESC, z.z_zonekey ASC
 ${FUEL}"

# q7: detect route detours (reported vs geometric distance)
run "Q7" \
"WITH trip_lengths AS (
     SELECT t_tripkey,
         t_distance AS reported_distance_m,
         st_length(st_makeline(t_pickuploc, t_dropoffloc)) / 0.000009 AS line_distance_m
     FROM file('${TRIP}', Parquet)
 )
 SELECT t_tripkey, reported_distance_m, line_distance_m,
     reported_distance_m / nullIf(line_distance_m, 0) AS detour_ratio
 FROM trip_lengths
 ORDER BY detour_ratio DESC NULLS LAST, reported_distance_m DESC, t_tripkey ASC
 ${FUEL}"

# q8: nearby pickup count per building (~500m)
run "Q8" \
"SELECT b.b_buildingkey, b.b_name, count() AS nearby_pickup_count
 FROM file('${TRIP}', Parquet) t
 JOIN file('${BUILDING}', Parquet) b ON st_dwithin(t.t_pickuploc, b.b_boundary, 0.0045)
 GROUP BY b.b_buildingkey, b.b_name
 ORDER BY nearby_pickup_count DESC, b.b_buildingkey ASC
 ${FUEL}"

# q9: building conflation via IoU
run "Q9" \
"WITH b1 AS (SELECT b_buildingkey AS id, b_boundary AS geom FROM file('${BUILDING}', Parquet)),
      b2 AS (SELECT b_buildingkey AS id, b_boundary AS geom FROM file('${BUILDING}', Parquet)),
      pairs AS (
          SELECT b1.id AS building_1, b2.id AS building_2,
              st_area(b1.geom) AS area1, st_area(b2.geom) AS area2,
              st_area(st_intersection(b1.geom, b2.geom)) AS overlap_area
          FROM b1 JOIN b2 ON b1.id < b2.id AND st_intersects(b1.geom, b2.geom)
      )
 SELECT building_1, building_2, area1, area2, overlap_area,
     CASE WHEN overlap_area = 0 THEN 0.0
          WHEN (area1 + area2 - overlap_area) = 0 THEN 1.0
          ELSE overlap_area / (area1 + area2 - overlap_area) END AS iou
 FROM pairs
 ORDER BY iou DESC, building_1 ASC, building_2 ASC
 ${FUEL}"

# q10: zone stats for trips starting within each zone
run "Q10" \
"SELECT z.z_zonekey, z.z_name AS pickup_zone,
    avg(t.t_dropofftime - t.t_pickuptime) AS avg_duration,
    avg(t.t_distance) AS avg_distance,
    count(t.t_tripkey) AS num_trips
 FROM file('${ZONE}', Parquet) z
 LEFT JOIN file('${TRIP}', Parquet) t ON st_within(t.t_pickuploc, z.z_boundary)
 GROUP BY z.z_zonekey, z.z_name
 ORDER BY avg_duration DESC NULLS LAST, z.z_zonekey ASC
 ${FUEL}"

# q11: count cross-zone trips
run "Q11" \
"SELECT count() AS cross_zone_trip_count
 FROM file('${TRIP}', Parquet) t
 JOIN file('${ZONE}', Parquet) pickup_zone  ON st_within(t.t_pickuploc,   pickup_zone.z_boundary)
 JOIN file('${ZONE}', Parquet) dropoff_zone ON st_within(t.t_dropoffloc, dropoff_zone.z_boundary)
 WHERE pickup_zone.z_zonekey != dropoff_zone.z_zonekey
 ${FUEL}"

# q12: 5 nearest buildings per trip (dwithin pre-filter + window rank)
run "Q12" \
"WITH candidates AS (
     SELECT t.t_tripkey, t.t_pickuploc, b.b_buildingkey, b.b_name AS building_name,
         st_distance(t.t_pickuploc, b.b_boundary) AS distance_to_building,
         row_number() OVER (PARTITION BY t.t_tripkey ORDER BY st_distance(t.t_pickuploc, b.b_boundary)) AS rn
     FROM file('${TRIP}', Parquet) t CROSS JOIN file('${BUILDING}', Parquet) b
     WHERE st_dwithin(t.t_pickuploc, b.b_boundary, 0.05)
 )
 SELECT t_tripkey, t_pickuploc, b_buildingkey, building_name, distance_to_building
 FROM candidates WHERE rn <= 5
 ORDER BY distance_to_building, b_buildingkey
 ${FUEL}"
