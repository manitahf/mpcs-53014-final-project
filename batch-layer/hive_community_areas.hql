-- This file will create an ORC table with community area data

-- First, map the CSV data we downloaded in Hive
-- id,name,district
drop table if exists manita_chi_community_areas_csv;
create external table manita_chi_community_areas_csv(
  id smallint,
  name string,
  district string)
  row format serde 'org.apache.hadoop.hive.serde2.OpenCSVSerde'

WITH SERDEPROPERTIES (
   "separatorChar" = ",",
   "quoteChar"     = "\""
)
STORED AS TEXTFILE
  location '/manita/inputs-community_areas'
TBLPROPERTIES ("skip.header.line.count" = "1");



-- Run a test query to make sure the above worked correctly
select id,name,district from manita_chi_community_areas_csv limit 20;

-- Create an ORC table for crime data (Note "stored as ORC" at the end)
drop table if exists manita_chi_community_areas;
create table manita_chi_community_areas(
  id smallint,
  name string,
  district string)
  stored as orc;

-- Copy the CSV table to the ORC table
insert overwrite table manita_chi_community_areas select * from manita_chi_community_areas_csv;

select id,name,district from manita_chi_community_areas limit 20;