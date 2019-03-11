#!/bin/bash

# This file generates the TPC-H Dataset as .tbl files
# Files are saved in ../data/TPCH${DATA_SIZE}GB directiory 

set -e

# Location of the TPCH DIRECTORY 
DATA_DIRECTORY="../../../data"

# Location of the TPCH DBGEN Script 
TPCH_DIRECTORY="../data/2.17.3/dbgen"

# Additional Data Sizes can be included here
DATA_SIZE=(0.1 1 3 4 5 10) 

cd $TPCH_DIRECTORY

echo "Started generating tables"

for i in "${DATA_SIZE[@]}"; 
do
    echo "Currently generating ${i} GB of data..."
    ./dbgen -f -s $i -v
    mkdir -p $DATA_DIRECTORY/TPCH${i}GB
    mv ./*.tbl $DATA_DIRECTORY/TPCH${i}GB
    echo "Done generating ${i} GB of data..."
done;

