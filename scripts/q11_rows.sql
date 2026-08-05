SELECT t.t_tripkey, pickup_zone.z_zonekey, dropoff_zone.z_zonekey
 FROM file('/data/bacek/src/chgeos/tmp/data/user_files/sf1/zone.parquet', Parquet) pickup_zone
 JOIN file('/data/bacek/src/chgeos/tmp/data/user_files/sf1/trip.parquet', Parquet) t            ON st_within(t.t_pickuploc,  pickup_zone.z_boundary)
 JOIN file('/data/bacek/src/chgeos/tmp/data/user_files/sf1/zone.parquet', Parquet) dropoff_zone ON st_within(t.t_dropoffloc, dropoff_zone.z_boundary)
 WHERE pickup_zone.z_zonekey != dropoff_zone.z_zonekey
 ORDER BY 1, 2, 3
 SETTINGS webassembly_udf_max_fuel=0, max_execution_time=600
