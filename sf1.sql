

-- q1: find trips starting within 50km of sedona city center, ordered by distance
select
   t.t_tripkey, st_x(t.t_pickuploc) as pickup_lon, st_y(t.t_pickuploc) as pickup_lat, t.t_pickuptime,
   st_distance(t.t_pickuploc, st_geomfromtext('point (-111.7610 34.8697)')) as distance_to_center
from 'trip.parquet' t
where st_dwithin(t.t_pickuploc, st_geomfromtext('point (-111.7610 34.8697)'), 0.45) -- 50km radius around sedona center
order by distance_to_center asc, t.t_tripkey asc


-- Q2: Count trips starting within Coconino County (Arizona) zone
select count(*) as trip_count_in_coconino_county
from 'trip.parquet' t
where st_intersects(t.t_pickuploc, (select z.z_boundary from 'zone.parquet' z where z.z_name = 'Coconino County' limit 1))


-- q3: monthly trip statistics within 15km radius of sedona city center (10km base + 5km buffer)
select
   date_trunc('month', t.t_pickuptime) as pickup_month, count(t.t_tripkey) as total_trips,
   avg(t.t_distance) as avg_distance, avg(t.t_dropofftime - t.t_pickuptime) as avg_duration,
   avg(t.t_fare) as avg_fare
from 'trip.parquet' t
where st_dwithin(
             t.t_pickuploc,
             st_geomfromtext('polygon((-111.9060 34.7347, -111.6160 34.7347, -111.6160 35.0047, -111.9060 35.0047, -111.9060 34.7347))'), -- 10km bounding box around sedona
             0.045 -- additional 5km buffer
     )
group by pickup_month
order by pickup_month


-- q4: zone distribution of top 1000 trips by tip amount
select z.z_zonekey, z.z_name, count(*) as trip_count
from
   'zone.parquet' z
       join (
       select t.t_pickuploc
       from 'trip.parquet' t
       order by t.t_tip desc, t.t_tripkey asc
           limit 1000 -- replace 1000 with x (how many top tips you want)
   ) top_trips on st_within(top_trips.t_pickuploc, z.z_boundary)
group by z.z_zonekey, z.z_name
order by trip_count desc, z.z_zonekey asc


-- q5: monthly travel patterns for repeat customers (convex hull of dropoff locations)
select
   c.c_custkey, c.c_name as customer_name,
   date_trunc('month', t.t_pickuptime) as pickup_month,
   st_area(st_convexhull(st_collect_agg(t.t_dropoffloc))) as monthly_travel_hull_area,
   count(*) as dropoff_count
from 'trip.parquet' t join 'customer.parquet' c on t.t_custkey = c.c_custkey
group by c.c_custkey, c.c_name, pickup_month
having dropoff_count > 5 -- only include repeat customers for meaningful hulls
order by dropoff_count desc, c.c_custkey asc


-- q6: zone statistics for trips intersecting a bounding box
select
   z.z_zonekey, z.z_name,
   count(t.t_tripkey) as total_pickups, avg(t.t_totalamount) as avg_distance,
   avg(t.t_dropofftime - t.t_pickuptime) as avg_duration
from 'trip.parquet' t, 'zone.parquet' z
where st_intersects(st_geomfromtext('polygon((-112.2110 34.4197, -111.3110 34.4197, -111.3110 35.3197, -112.2110 35.3197, -112.2110 34.4197))'), z.z_boundary)
 and st_within(t.t_pickuploc, z.z_boundary)
group by z.z_zonekey, z.z_name
order by total_pickups desc, z.z_zonekey asc


-- q7: detect potential route detours by comparing reported vs. geometric distances
with trip_lengths as (
   select
       t.t_tripkey,
       t.t_distance as reported_distance_m,
       st_length(
               st_makeline(
                       st_geomfromwkb(t.t_pickuploc),
                       st_geomfromwkb(t.t_dropoffloc)
               )
       ) / 0.000009 as line_distance_m -- 1 meter = 0.000009 degree
   from 'trip.parquet' t
)
select
   t.t_tripkey,
   t.reported_distance_m,
   t.line_distance_m,
   t.reported_distance_m / nullif(t.line_distance_m, 0) as detour_ratio
from 'trip_lengths.parquet' t
order by detour_ratio desc nulls last, reported_distance_m desc, t_tripkey asc


-- q8: count nearby pickups for each building within 500m radius
select b.b_buildingkey, b.b_name, count(*) as nearby_pickup_count
from 'trip.parquet' t join 'building.parquet' b on st_dwithin(t.t_pickuploc, b.b_boundary, 0.0045) -- ~500m
group by b.b_buildingkey, b.b_name
order by nearby_pickup_count desc, b.b_buildingkey asc


-- q9: building conflation (duplicate/overlap detection via iou), deterministic order
with b1 as (
   select b_buildingkey as id, st_geomfromwkb(b_boundary) as geom
   from 'building.parquet'
),
    b2 as (
        select b_buildingkey as id, st_geomfromwkb(b_boundary) as geom
        from 'building.parquet'
    ),
    pairs as (
        select
            b1.id as building_1,
            b2.id as building_2,
            st_area(b1.geom) as area1,
            st_area(b2.geom) as area2,
            st_area(st_intersection(b1.geom, b2.geom)) as overlap_area
        from b1
                 join b2
                      on b1.id < b2.id
                          and st_intersects(b1.geom, b2.geom)
    )
select
   building_1,
   building_2,
   area1,
   area2,
   overlap_area,
   case
       when overlap_area = 0 then 0.0
       when (area1 + area2 - overlap_area) = 0 then 1.0
       else overlap_area / (area1 + area2 - overlap_area)
       end as iou
from pairs
order by iou desc, building_1 asc, building_2 asc


-- q10: zone statistics for trips starting within each zone
select
   z.z_zonekey, z.z_name as pickup_zone, avg(t.t_dropofftime - t.t_pickuptime) as avg_duration,
   avg(t.t_distance) as avg_distance, count(t.t_tripkey) as num_trips
from 'zone.parquet' z left join 'trip.parquet' t on st_within(t.t_pickuploc, z.z_boundary)
group by z.z_zonekey, z.z_name
order by avg_duration desc nulls last, z.z_zonekey asc


-- q11: count trips that cross between different zones
select count(*) as cross_zone_trip_count
from
   'trip.parquet' t
       join 'zone.parquet' pickup_zone on st_within(t.t_pickuploc, pickup_zone.z_boundary)
       join 'zone.parquet' dropoff_zone on st_within(t.t_dropoffloc, dropoff_zone.z_boundary)
where pickup_zone.z_zonekey != dropoff_zone.z_zonekey


-- Q12 (ClickHouse): 5 nearest buildings per trip pickup location
WITH
    trips AS (
        SELECT
            t_tripkey,
            t_pickuploc,
            st_geomfromwkb(t_pickuploc) AS pickup_geom
        FROM file('trip.parquet', Parquet)
    ),
    buildings AS (
        SELECT
            b_buildingkey,
            b_name,
            st_geomfromwkb(b_boundary) AS boundary_geom
        FROM file('building.parquet', Parquet)
    ),
    ranked AS (
        SELECT
            t.t_tripkey,
            t.t_pickuploc,
            b.b_buildingkey,
            b.b_name                                          AS building_name,
            st_distance(t.pickup_geom, b.boundary_geom)       AS distance_to_building,
            row_number() OVER (
                PARTITION BY t.t_tripkey
                ORDER BY st_distance(t.pickup_geom, b.boundary_geom)
            )                                                  AS rn
        FROM trips t
        CROSS JOIN buildings b
    )
SELECT
    t_tripkey,
    t_pickuploc,
    b_buildingkey,
    building_name,
    distance_to_building
FROM ranked
WHERE rn <= 5
ORDER BY distance_to_building, b_buildingkey


● -- Q12 (ClickHouse): 5 nearest buildings per trip, pre-filtered by radius
  WITH
      trips AS (
          SELECT
              t_tripkey,
              t_pickuploc,
              st_geomfromwkb(t_pickuploc) AS pickup_geom
          FROM file('trip.parquet', Parquet)
      ),
      buildings AS (
          SELECT
              b_buildingkey,
              b_name,
              st_geomfromwkb(b_boundary) AS boundary_geom
          FROM file('building.parquet', Parquet)
      ),
      candidates AS (
          SELECT
              t.t_tripkey,
              t.t_pickuploc,
              b.b_buildingkey,
              b.b_name                                    AS building_name,
              st_distance(t.pickup_geom, b.boundary_geom) AS distance_to_building,
              row_number() OVER (
                  PARTITION BY t.t_tripkey
                  ORDER BY st_distance(t.pickup_geom, b.boundary_geom)
              )                                           AS rn
          FROM trips t
          CROSS JOIN buildings b
          WHERE st_dwithin(t.pickup_geom, b.boundary_geom, 1000.0)  -- tune radius
      )
  SELECT
      t_tripkey,
      t_pickuploc,
      b_buildingkey,
      building_name,
      distance_to_building
  FROM candidates
  WHERE rn <= 5
  ORDER BY distance_to_building, b_buildingkey
