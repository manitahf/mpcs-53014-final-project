-- Before running below, run `hbase shell` and `create 'manita_trip_data_by_route', 'trips'`

drop table manita_trip_stats_by_route_and_weather;
create external table manita_trip_stats_by_route_and_weather (
    route_weather string,
    rideshare_trips bigint,
    rideshare_min_price bigint,
    rideshare_max_price bigint,
    rideshare_total_price bigint,
    rideshare_total_distance bigint,
    rideshare_total_duration bigint,
    taxi_trips bigint,
    taxi_min_price bigint,
    taxi_max_price bigint,
    taxi_total_price bigint,
    taxi_total_distance bigint,
    taxi_total_duration bigint)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ('hbase.columns.mapping' = ':key,rideshare:trips#b,
rideshare:min_price#b, rideshare:max_price#b, rideshare:total_price#b, rideshare:total_distance#b,
rideshare:total_duration#b, taxi:trips#b, taxi:min_price#b, taxi:max_price#b, taxi:total_price#b, taxi:total_distance#b, taxi:total_duration#b')
TBLPROPERTIES ('hbase.table.name' = 'manita_trip_stats_by_route_and_weather');


insert overwrite table manita_trip_stats_by_route_and_weather
  select concat(pickup_area, dropoff_area,weather_type,temperature_type),
    rideshare_trips,
    cast(rideshare_min_price*100 as bigint),
    cast(rideshare_max_price*100 as bigint),
    cast(rideshare_total_price*100 as bigint),
    cast(rideshare_total_distance*100 as bigint),
    rideshare_total_duration,
    taxi_trips,
    cast(taxi_min_price*100 as bigint),
    cast(taxi_max_price*100 as bigint),
    cast(taxi_total_price*100 as bigint),
    cast(taxi_total_distance*100 as bigint),
    taxi_total_duration
  from manita_trips_by_route_and_weather;