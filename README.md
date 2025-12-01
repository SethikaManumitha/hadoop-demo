# Hadoop Tutorials

This is a demo repository for Hadoop tutorials that I made for myself.Feel free to use it as you like.
---

## Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/SethikaManumitha/hadoop-demo.git
   ```

## Project structure

Below is a high-level view of the repository layout and important files:

```
hadoop-demo/
├─ README.md
├─ input.txt
├─ data-sources/
│  ├─ locationData.csv
│  ├─ processed_location_data.csv
│  ├─ processed_weather_data.csv
│  └─ weatherData.csv
├─ hadoop-setup/
│  ├─ docker-compose.yaml
│  ├─ hadoop.env
│  ├─ hadoop-hive.env
│  ├─ hive-site.xml
│  ├─ init-metastore.sql
│  ├─ namenode-entrypoint.sh
│  ├─ wait-for-it.sh
│  └─ hadoop_dir/
│     ├─ core-site.xml
│     └─ hdfs-site.xml
├─ hive-ql/
│  ├─ hive_commands.hql
│  └─ hive_commands.txt
├─ MapReduce/
│  ├─ pom.xml
│  ├─ DistrictPerMonth.sh
│  ├─ TopPrecipitation.sh
│  ├─ src/
│  │  └─ main/java/org/sethika/ (Java MapReduce sources)
│  └─ target/
└─ output/
   └─ Task11.csv
```

This layout highlights where to find Docker/Hadoop configuration, Hive queries, MapReduce source code and build artifacts, and sample data.
