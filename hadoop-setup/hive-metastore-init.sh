#!/bin/bash
sudo docker compose down
sudo docker volume rm hadoop-setup_metastore-db2
sudo docker compose up -d