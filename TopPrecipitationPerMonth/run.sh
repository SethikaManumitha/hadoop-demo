#!/bin/bash

# Exit immediately if any command fails
set -e

echo "Pulling latest code from Git..."
git pull origin main

echo "Building the project with Maven..."
mvn clean install

JAR_PATH="/home/iitgcpuser/hadoop-demo/TopPrecipitationPerMonth/target/TopPrecipitationPerMonth-1.0-SNAPSHOT.jar"
DOCKER_CONTAINER="namenode"
HDFS_INPUT="/data/processed_weather_data.csv"
HDFS_OUTPUT="/user/test/output/TopTen"
HADOOP_TARGET_PATH="/opt/hadoop/resources"

echo "Copying JAR to Hadoop container..."
sudo docker cp $JAR_PATH $DOCKER_CONTAINER:$HADOOP_TARGET_PATH

echo "Entering Hadoop container..."
sudo docker exec -it $DOCKER_CONTAINER bash -c "
    echo 'Removing previous HDFS output directory if it exists...'
    hdfs dfs -rm -r -skipTrash $HDFS_OUTPUT || true

    echo 'Running MapReduce job...'
    yarn jar $HADOOP_TARGET_PATH/TopPrecipitationPerMonth-1.0-SNAPSHOT.jar org.sethika.TopPrecipitation $HDFS_INPUT $HDFS_OUTPUT

    echo 'Displaying the result'
    hdfs dfs -cat $HDFS_OUTPUT/part-00000
"
