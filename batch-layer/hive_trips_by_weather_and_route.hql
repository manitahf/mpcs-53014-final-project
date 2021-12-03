
drop table if exists manita_trips_by_route_and_weather;
create table manita_trips_by_route_and_weather (
    pickup_area string,
    dropoff_area string,
    temperature_type tinyint,
    weather_type string,
    rideshare_trips bigint,
    rideshare_min_price double,
    rideshare_max_price double,
    rideshare_total_price double,
    rideshare_total_distance double,
    rideshare_total_duration bigint,
    taxi_trips bigint,
    taxi_min_price double,
    taxi_max_price double,
    taxi_total_price double,
    taxi_total_distance double,
    taxi_total_duration bigint)
    stored as orc;

insert overwrite table manita_trips_by_route_and_weather
  select r.pickup_area,
    r.dropoff_area,
    r.temperature_type,
    r.weather_type,
    rideshare_trips,
    rideshare_min_price,
    rideshare_max_price,
    rideshare_total_price,
    rideshare_total_distance,
    rideshare_total_duration,
    taxi_trips,
    taxi_min_price,
    taxi_max_price,
    taxi_total_price,
    taxi_total_distance,
    taxi_total_duration
  from
  (select pickup_area, dropoff_area, temperature_type, weather_type, sum(1) as rideshare_trips,
  min(trip_total) as rideshare_min_price, max(trip_total) as rideshare_max_price, sum(trip_total)
  as rideshare_total_price,
  sum(trip_miles) as rideshare_total_distance, sum(trip_seconds) as rideshare_total_duration
  from manita_trips_cas_weather where trip_type == 0 and trip_total > 0 group by pickup_area,
  dropoff_area, temperature_type, weather_type) r
  join
  (select pickup_area, dropoff_area, temperature_type, weather_type, sum(1) as taxi_trips,
  min(trip_total) as taxi_min_price, max(trip_total) as taxi_max_price, sum(trip_total)
  as taxi_total_price,
  sum(trip_miles) as taxi_total_distance, sum(trip_seconds) as taxi_total_duration
  from manita_trips_cas_weather where trip_type == 1  and trip_total > 0 and
  trip_total < 1000 group by pickup_area,
  dropoff_area, temperature_type, weather_type) t
  on r.pickup_area == t.pickup_area and r.dropoff_area == t.dropoff_area and
  r.temperature_type == t.temperature_type and r.weather_type == t.weather_type;


select pickup_area, dropoff_area, temperature_type, weather_type, rideshare_trips, taxi_trips,
rideshare_min_price, rideshare_max_price, taxi_min_price, taxi_max_price from manita_trips_by_route_and_weather where
pickup_area = "O’Hare" limit 20;
