-- This file will create an ORC table with crime data

-- First, map the CSV data we downloaded in Hive
drop table if exists manita_rideshare_trips_csv;
create external table manita_rideshare_trips_csv(
  trip_id string,
  trip_start_timestamp string,
  trip_end_timestamp string,
  trip_seconds int,
  trip_miles double,
  pickup_census_tract string,
  dropoff_census_tract string,
  pickup_community_area smallint,
  dropoff_community_area smallint,
  fare double,
  tip double,
  additional_charges double,
  trip_total double,
  shared_trip_authorized string,
  trips_pooled smallint,
  pickup_centroid_latitude double,
  pickup_centroid_longitude double,
  pickup_centroid_location string,
  dropoff_centroid_latitude double,
  dropoff_centroid_longitude double,
  dropoff_centroid_location string
)
  row format serde 'org.apache.hadoop.hive.serde2.OpenCSVSerde'

WITH SERDEPROPERTIES (
   "separatorChar" = ",",
   "quoteChar"     = "\""
)
STORED AS TEXTFILE
  location '/manita/inputs/rideshare/'
TBLPROPERTIES ("skip.header.line.count" = "1");

-- Run a test query to make sure the above worked correctly
select trip_id, trip_start_timestamp, pickup_community_area,
dropoff_community_area, shared_trip_authorized, trip_total from
manita_rideshare_trips_csv limit 20;

-- Create an ORC table for crime data (Note "stored as ORC" at the end)
drop table if exists manita_rideshare_trips;
create table manita_rideshare_trips(
  trip_id string,
  trip_start_timestamp string,
  trip_end_timestamp string,
  trip_seconds int,
  trip_miles double,
  pickup_census_tract string,
  dropoff_census_tract string,
  pickup_community_area smallint,
  dropoff_community_area smallint,
  fare double,
  tip double,
  additional_charges double,
  trip_total double,
  shared_trip_authorized string,
  trips_pooled smallint,
  pickup_centroid_latitude double,
  pickup_centroid_longitude double,
  pickup_centroid_location string,
  dropoff_centroid_latitude double,
  dropoff_centroid_longitude double,
  dropoff_centroid_location string)
  stored as orc;

-- Copy the CSV table to the ORC table
insert overwrite table manita_rideshare_trips select * from manita_rideshare_trips_csv
where pickup_community_area != '' and dropoff_community_area != ''
and trip_total != '' and shared_trip_authorized == 'false';

select trip_id, trip_start_timestamp, pickup_community_area,
dropoff_community_area, trip_total, shared_trip_authorized from manita_rideshare_trips limit 20;