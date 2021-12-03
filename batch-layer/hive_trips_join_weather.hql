add jar hdfs:///tmp/spertus/IngestWeather-1.0-SNAPSHOT.jar;

drop table if exists manita_trips_cas_weather;
create table manita_trips_cas_weather (
  trip_date date,
  pickup_area string,
  dropoff_area string,
  trip_total double,
  trip_miles double,
  trip_seconds int,
  trip_type tinyint,
  temperature_type tinyint,
  weather_type string)
  stored as orc;


insert overwrite table manita_trips_cas_weather
  select t.trip_date,
  pickup_area, dropoff_area, trip_total, trip_miles, t.trip_seconds, t.trip_type,
  case
    when w.meantemperature < 15 then 0
    when w.meantemperature < 39 then 1
    when w.meantemperature < 59 then 2
    when w.meantemperature < 79 then 3
    else 4
  end as tempature_type,
  case
    when w.hail then 'hail'
    when w.snow then 'snow'
    when w.thunder then 'thunder'
    when w.rain then 'rain'
    else 'clear'
  end as weather_type
  from manita_trips_and_cas t join manita_chi_weather w
  on year(t.trip_date) == w.year and month(t.trip_date) == w.month and
  day(trip_date) == w.day;

-- Test the above table
select trip_date, temperature_type,weather_type, trip_type from
manita_trips_cas_weather limit 10;
