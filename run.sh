#!/bin/bash

pip3 install -r requirements.txt &&

mkdir -p logs &&

# START TESTS
python -m tests.run_test &&

# CREATE POSTGRESQL IN CONTAINER
docker-compose up --build -d &&

# IMPORT SETTINGS
DB_SETTINGS=$(python -c "from config.config import DB_SETTINGS; print(DB_SETTINGS)") &&
DB_NAME=$(python -c "print($DB_SETTINGS['NAME'])") &&

docker exec -it my_postgres psql -U postgres -c "drop database if exists $DB_NAME" &&
docker exec -it my_postgres psql -U postgres -c "create database $DB_NAME" &&

python -m app.create_tables &&

python -m app.populate_tables &&

python -m app.process_tables