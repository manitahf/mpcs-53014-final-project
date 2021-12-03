add jar hdfs:///tmp/spertus/IngestWeather-1.0-SNAPSHOT.jar;

-- Create table to hold 2021 weather data that is missing from WeatherSummary table
CREATE EXTERNAL TABLE IF NOT EXISTS ManitaWeatherSummary
  ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.thrift.ThriftDeserializer'
    WITH SERDEPROPERTIES (
      'serialization.class' = 'edu.uchicago.mpcs53013.weatherSummary.WeatherSummary',
      'serialization.format' =  'org.apache.thrift.protocol.TBinaryProtocol')
  STORED AS SEQUENCEFILE 
  LOCATION '/manita/inputs/chicago-weather';

-- Test our table
select * from ManitaWeatherSummary limit 5;

create table manita_chi_weather (
  year smallint,
  month tinyint,
  day tinyint,
  meantemperature double,
  rain boolean,
  snow boolean,
  hail boolean,
  thunder boolean)
  stored as orc;

insert into manita_chi_weather
    select year, month, day, meantemperature, rain, snow, hail, thunder from WeatherSummary where station == '725340';

insert into manita_chi_weather
    select year, month, day, meantemperature, rain, snow, hail, thunder from
    ManitaWeatherSummary;

-- Test our table
select * from manita_chi_weather limit 5;