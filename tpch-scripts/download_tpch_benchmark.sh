#!/bin/bash
echo "Started Downloading the tpc-h data generator"

DOWNLOAD_PATH="../data"

# Create a the dataset
mkdir -p $DOWNLOAD_PATH

# Goto download path
cd $DOWNLOAD_PATH
# Download the zip file
wget -v https://www.dropbox.com/s/ngarfiaiatvsdlq/2.17.3.zip?dl=0 -O 2.17.3.zip

unzip 2.17.3.zip

# Change directory 
cd 2.17.3/dbgen

# Compile the C program
make

echo "All Done..."
