#!/bin/bash

# This file generates creates the tables and their aligned couterparts in the mysql server
# Tables are named automatically. Number of brances created are

set -e

# Location of the TPCH-tbl DIRECTORY 
DATA_DIRECTORY="../data"

# Additional Data Sizes can be included here
DATA_SIZE=(0.1 1 3 4 5 10) 

echo "Started generating tables"

for i in "${DATA_SIZE[@]}"; 
do
    echo "Currently generating schema and aligned relationships ${i} GB of data..."
    python3 create_aligned_tables_mysql.py \
        --data_dir=$DATA_DIRECTORY/TPCH${i}GB \
        --force \
        --mysql_username=tpch \
        --mysql_password=tpch \
        --branches=1 
    echo "Done generating schema and aligned relationships ${i} GB of data..."
done;

