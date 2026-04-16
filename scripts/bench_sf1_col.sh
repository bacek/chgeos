#!/usr/bin/env bash
# SF1 geospatial benchmark suite — COLUMNAR_V1 (_col) variant.
#
# Usage:
#   ./scripts/bench_sf1_col.sh [path/to/clickhouse] [path/to/data/dir] [QUERY]
#
# QUERY (optional): run only the named query, e.g. Q1, Q7
#
# The data directory must contain:
#   trip.parquet, zone.parquet, building.parquet, customer.parquet

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

CH="${1:-$(command -v clickhouse 2>/dev/null)}"
[[ -x "${CH}" ]] || { echo "ERROR: clickhouse binary not found; pass as first argument or put on PATH"; exit 1; }

DATA_DIR="${2:?ERROR: data directory required as second argument}"
[[ -d "${DATA_DIR}" ]] || { echo "ERROR: data directory '${DATA_DIR}' does not exist"; exit 1; }

QUERY_FILTER="${3:-}"

PORT="${CH_PORT:-19000}"
TIMEOUT="${BENCH_TIMEOUT:-120}"
FUEL="SETTINGS webassembly_udf_max_fuel=0, max_execution_time=${TIMEOUT}"
RUNS="${BENCH_RUNS:-5}"

# ClickHouse server restricts file() to user_files_path; symlink data files there.
USER_FILES="${CH_USER_FILES:-./tmp/data/user_files}"
for _f in trip zone building customer; do
    [[ -f "${DATA_DIR}/${_f}.parquet" ]] && ln -sf "${DATA_DIR}/${_f}.parquet" "${USER_FILES}/${_f}.parquet"
done

TRIP="file('trip.parquet', Parquet)"
ZONE="file('zone.parquet', Parquet)"
BUILDING="file('building.parquet', Parquet)"
CUSTOMER="file('customer.parquet', Parquet)"

run_once() {
    local query="$1"
    local tmpf; tmpf=$(mktemp)
    local rc=0
    "${CH}" client --port "${PORT}" --time -q "${query}" >/dev/null 2>"${tmpf}" || rc=$?
    local secs; secs=$(grep -E '^[0-9]+(\.[0-9]+)?$' "${tmpf}" | tail -1)
    rm -f "${tmpf}"
    [[ -z "${secs}" ]] && secs="${TIMEOUT}"
    local ms; ms=$(awk "BEGIN {printf \"%d\", ${secs} * 1000 + 0.5}")
    # Non-zero exit with time well below the limit = query error, not timeout.
    if [[ ${rc} -ne 0 && "${ms}" -lt $(( TIMEOUT * 1000 - 500 )) ]]; then
        echo "ERROR"
    else
        echo "${ms}"
    fi
}

run() {
    local label="$1"; local query="$2"
    [[ -n "${QUERY_FILTER}" && "${label}" != "${QUERY_FILTER}" ]] && return 0
    local times=() sum=0 min=999999 max=0 timed_out=0 errored=0 rows="?"
    for (( i=0; i<RUNS; i++ )); do
        local ms
        if (( i == 0 )); then
            local tmpf tmpf_err; tmpf=$(mktemp); tmpf_err=$(mktemp)
            local secs
            local rc=0
            "${CH}" client --port "${PORT}" --time -q "${query}" >"${tmpf}" 2>"${tmpf_err}" || rc=$?
            secs=$(grep -E '^[0-9]+(\.[0-9]+)?$' "${tmpf_err}" | tail -1)
            rm -f "${tmpf_err}"
            [[ -z "${secs}" ]] && secs="${TIMEOUT}"
            ms=$(awk "BEGIN {printf \"%d\", ${secs} * 1000 + 0.5}")
            if [[ ${rc} -ne 0 && "${ms}" -lt $(( TIMEOUT * 1000 - 500 )) ]]; then
                errored=1; rm -f "${tmpf}"; break
            fi
            if [[ "${ms}" -lt $(( TIMEOUT * 1000 - 500 )) ]]; then
                rows=$(wc -l < "${tmpf}" | tr -d ' ')
            fi
            rm -f "${tmpf}"
        else
            ms=$(run_once "${query}")
            if [[ "${ms}" == "ERROR" ]]; then errored=1; break; fi
        fi
        [[ "${ms}" -ge $(( TIMEOUT * 1000 - 500 )) ]] && { timed_out=1; break; }
        times+=("${ms}")
        sum=$(( sum + ms ))
        (( ms < min )) && min=${ms}
        (( ms > max )) && max=${ms}
    done

    if (( errored )); then
        printf "| %-6s | %8s | %8s | %8s | %10s |\n" "${label}" "ERROR" "ERROR" "ERROR" "${rows}"
        return
    fi
    if (( timed_out )); then
        printf "| %-6s | %8s | %8s | %8s | %10s |\n" "${label}" "TIMEOUT" "TIMEOUT" "TIMEOUT" "${rows}"
        return
    fi
    local avg=$(( sum / RUNS ))
    printf "| %-6s | %8s | %8s | %8s | %10s |\n" \
        "${label}" "${min}ms" "${avg}ms" "${max}ms" "${rows}"
}

echo ""
echo "Data dir: ${DATA_DIR}  (${RUNS} runs each)"
trip_count=$("${CH}" client --port "${PORT}" -q \
    "SELECT count() FROM ${TRIP}" 2>/dev/null || echo '?')
echo "trip rows: ${trip_count}"
echo ""
printf "| %-6s | %8s | %8s | %8s | %10s |\n" "Query" "min" "avg" "max" "rows"
echo  "|--------|----------|----------|----------|------------|"

# q1: trips within 50km of Sedona city center
run "Q1" \
"SELECT t_tripkey, st_x_col(t_pickuploc), st_y_col(t_pickuploc), t_pickuptime,
    st_distance_col(t_pickuploc, st_geomfromtext('POINT (-111.7610 34.8697)')) AS distance_to_center
 FROM ${TRIP}
 WHERE st_dwithin_col(t_pickuploc, st_geomfromtext('POINT (-111.7610 34.8697)'), 0.45)
 ORDER BY distance_to_center ASC, t_tripkey ASC
 ${FUEL}"

### # q2: count trips within Coconino County
### run "Q2" \
### "SELECT count() AS trip_count
###  FROM ${TRIP} t
###  WHERE st_intersects_col(t.t_pickuploc,
###      (SELECT z_boundary FROM ${ZONE} WHERE z_name = 'Coconino County' LIMIT 1))
###  ${FUEL}"

# q3: monthly stats within bounding box + buffer
run "Q3" \
"SELECT toStartOfMonth(t_pickuptime) AS pickup_month,
    count(t_tripkey) AS total_trips,
    avg(t_distance) AS avg_distance,
    avg(t_fare) AS avg_fare
 FROM ${TRIP}
 WHERE st_dwithin_col(t_pickuploc,
     st_geomfromtext('POLYGON((-111.9060 34.7347,-111.6160 34.7347,-111.6160 35.0047,-111.9060 35.0047,-111.9060 34.7347))'),
     0.045)
 GROUP BY pickup_month
 ORDER BY pickup_month
 ${FUEL}"

# q4: zone distribution of top-1000 trips by tip
run "Q4" \
"SELECT z.z_zonekey, z.z_name, count() AS trip_count
 FROM ${ZONE} z
 JOIN (SELECT t_pickuploc FROM ${TRIP} ORDER BY t_tip DESC, t_tripkey ASC LIMIT 1000) top_trips
   ON st_within_col(top_trips.t_pickuploc, z.z_boundary)
 GROUP BY z.z_zonekey, z.z_name
 ORDER BY trip_count DESC, z.z_zonekey ASC
 ${FUEL}"

# q5: monthly convex hull area per repeat customer
run "Q5" \
"SELECT c.c_custkey, c.c_name AS customer_name,
    toStartOfMonth(t.t_pickuptime) AS pickup_month,
    st_area_col(st_convexhull_col(st_collect_agg_mp(groupArray(t.t_dropoffloc)))) AS monthly_travel_hull_area,
    count() AS dropoff_count
 FROM ${TRIP} t
 JOIN ${CUSTOMER} c ON t.t_custkey = c.c_custkey
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
 FROM ${TRIP} t, ${ZONE} z
 WHERE st_intersects_col(st_geomfromtext('POLYGON((-112.2110 34.4197,-111.3110 34.4197,-111.3110 35.3197,-112.2110 35.3197,-112.2110 34.4197))'), z.z_boundary)
   AND st_within_col(t.t_pickuploc, z.z_boundary)
 GROUP BY z.z_zonekey, z.z_name
 ORDER BY total_pickups DESC, z.z_zonekey ASC
 ${FUEL}"

# q7: detect route detours (reported vs geometric distance)
run "Q7" \
"WITH trip_lengths AS (
     SELECT t_tripkey,
         t_distance AS reported_distance_m,
         st_length_col(st_makeline_col(t_pickuploc, t_dropoffloc)) / 0.000009 AS line_distance_m
     FROM ${TRIP}
 )
 SELECT t_tripkey, reported_distance_m, line_distance_m,
     reported_distance_m / nullIf(line_distance_m, 0) AS detour_ratio
 FROM trip_lengths
 ORDER BY detour_ratio DESC NULLS LAST, reported_distance_m DESC, t_tripkey ASC
 ${FUEL}"

# q8: nearby pickup count per building (~500m)
run "Q8" \
"SELECT b.b_buildingkey, b.b_name, count() AS nearby_pickup_count
 FROM ${TRIP} t
 JOIN ${BUILDING} b ON st_dwithin_col(t.t_pickuploc, b.b_boundary, 0.0045)
 GROUP BY b.b_buildingkey, b.b_name
 ORDER BY nearby_pickup_count DESC, b.b_buildingkey ASC
 ${FUEL}"

# q9: building conflation via IoU
run "Q9" \
"WITH b1 AS (SELECT b_buildingkey AS id, b_boundary AS geom FROM ${BUILDING}),
      b2 AS (SELECT b_buildingkey AS id, b_boundary AS geom FROM ${BUILDING}),
      pairs AS (
          SELECT b1.id AS building_1, b2.id AS building_2,
              st_area_col(b1.geom) AS area1, st_area_col(b2.geom) AS area2,
              st_area_col(st_intersection_col(b1.geom, b2.geom)) AS overlap_area
          FROM b1 JOIN b2 ON b1.id < b2.id AND st_intersects_col(b1.geom, b2.geom)
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
 FROM ${ZONE} z
 LEFT JOIN ${TRIP} t ON st_within_col(t.t_pickuploc, z.z_boundary)
 GROUP BY z.z_zonekey, z.z_name
 ORDER BY avg_duration DESC NULLS LAST, z.z_zonekey ASC
 ${FUEL}"

# q11: count cross-zone trips
run "Q11" \
"SELECT count() AS cross_zone_trip_count
 FROM ${TRIP} t
 JOIN ${ZONE} pickup_zone  ON st_within_col(t.t_pickuploc,   pickup_zone.z_boundary)
 JOIN ${ZONE} dropoff_zone ON st_within_col(t.t_dropoffloc, dropoff_zone.z_boundary)
 WHERE pickup_zone.z_zonekey != dropoff_zone.z_zonekey
 ${FUEL}"

# q12: 5 nearest buildings per trip (dwithin pre-filter + window rank)
run "Q12" \
"WITH candidates AS (
     SELECT t.t_tripkey, t.t_pickuploc, b.b_buildingkey, b.b_name AS building_name,
         st_distance_col(t.t_pickuploc, b.b_boundary) AS distance_to_building,
         row_number() OVER (PARTITION BY t.t_tripkey ORDER BY st_distance_col(t.t_pickuploc, b.b_boundary)) AS rn
     FROM ${TRIP} t CROSS JOIN ${BUILDING} b
     WHERE st_dwithin_col(t.t_pickuploc, b.b_boundary, 0.05)
 )
 SELECT t_tripkey, t_pickuploc, b_buildingkey, building_name, distance_to_building
 FROM candidates WHERE rn <= 5
 ORDER BY distance_to_building, b_buildingkey
 ${FUEL}"
