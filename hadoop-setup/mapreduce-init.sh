#!/bin/bash
sudo docker exec -it namenode bash
mkdir -p /opt/hadoop/resources
mkdir -p /user/data/location/
mkdir -p /user/data/weather/
exit
sudo docker cp /home/iitgcpuser/hadoop-demo/data-sources/processed_location_data.csv namenode:/user/data/location/
sudo docker cp /home/iitgcpuser/hadoop-demo/data-sources/processed_weather_data.csv namenode:/user/data/weather
cd /home/iitgcpuser/hadoop-demo/MapReduce
sudo chmod +X DistrictPerMonth.sh
sudo chmod +X TopPrecipitation.sh