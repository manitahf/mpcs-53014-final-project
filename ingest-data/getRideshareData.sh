mkdir rideshare-trips
cd rideshare-trips
wget https://data.cityofchicago.org/api/views/m6dm-c72p/rows.csv?accessType=DOWNLOAD&api_foundry=true
mv rows.csv?accessType=DOWNLOAD rideshare-trips.csv

hdfs dfs -put rideshare-trips.csv /manita/inputs/rideshare/rideshare-trips.csv
