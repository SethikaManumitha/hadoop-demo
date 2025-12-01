#!/bin/bash

# Create dirs inside namenode container
sudo docker exec namenode mkdir -p /opt/hadoop/resources
sudo docker exec namenode mkdir -p /user/data/location/
sudo docker exec namenode mkdir -p /user/data/weather/

# Copy files to container paths
sudo docker cp /home/iitgcpuser/hadoop-demo/data-sources/processed_location_data.csv \
    namenode:/user/data/location/

sudo docker cp /home/iitgcpuser/hadoop-demo/data-sources/processed_weather_data.csv \
    namenode:/user/data/weather/

# Fix permissions for your scripts
cd /home/iitgcpuser/hadoop-demo/MapReduce
sudo chmod +x DistrictPerMonth.sh
sudo chmod +x TopPrecipitation.sh
