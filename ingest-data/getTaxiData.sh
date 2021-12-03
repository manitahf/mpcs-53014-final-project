mkdir taxi-trips
cd taxi-trips
wget https://data.cityofchicago.org/api/views/wrvz-psew/rows.csv?accessType=DOWNLOAD&api_foundry=true
mv rows.csv?accessType=DOWNLOAD taxi-trips.csv

hdfs dfs -put taxi-trips.csv /manita/inputs/taxi/taxi-trips.csv
