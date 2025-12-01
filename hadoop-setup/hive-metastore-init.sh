#!/bin/bash
docker compose down
docker volume rm hadoop-setup_metastore-db2
docker compose up -d