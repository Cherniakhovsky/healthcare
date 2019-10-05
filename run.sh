#!/bin/bash

# IMPORT SETTINGS
DB_SETTINGS=$(python -c "from config.config import DB_SETTINGS; print(DB_SETTINGS)")
DB_NAME=$(python -c "print($DB_SETTINGS['NAME'])")


# CREATE DB
#docker exec -it my_postgres psql -U postgres -c "create database $DB_NAME"

# CREATE TABLES
python create_tables.py




#echo $va
