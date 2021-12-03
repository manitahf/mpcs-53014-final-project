mkdir community_areas
cd community_areas
vim community_areas.csv
-- add contents of community_areas.csv file

hdfs dfs -put community_areas.csv /manita/inputs-community_areas/community_areas.csv
