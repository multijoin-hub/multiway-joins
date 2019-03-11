## A Faster Multiway Joins in Databases(Relational)
We test the performance of a faster Multiway join algorithm with tpc-h benchmark database.

## Generate TPC-H Data

To generate tpc-h data, use the following scripts:

```bash
chmod +x ./tpch-scripts/download_tpch_benchmark.sh
chmod +x ./tpch-scripts/generate_tpch_data.sh
cd tpch-scripts
./download_tpch_benchmark.sh
./generate_tpch_data.sh
```

## Creating Schemas, Loading Tables and Creating Aligned Relations in a MYSQL-Server
First make sure you have the MYSQL server running in localhost. Create a new user named <code>tpch</code> with password <code>tpch</code>, which is used by the rest of the code. 
1. Login to the mysql console using root and password:
```bash
    mysql -u root -p
```

2. Use the following script to create new user:
```bash
    mysql> CREATE USER 'tpch'@'localhost' IDENTIFIED BY 'tpch';
    mysql> GRANT ALL PRIVILEGES ON * . * TO 'tpch'@'localhost';
```

3. Use the the following script to load all the tables in the database and Create Aligned Relations
```bash
python3 tpch-scripts/create_aligned_tables_mysql.py 
usage: 
            Create Aligned Relations For TPC-H Schema in Mysql Server
            By Default the data is assumed to be in ../data/TPCH* directories
            
       [-h] [--data_dir DATA_DIR] [--force] [--mysql_host MYSQL_HOST]
       [--mysql_username MYSQL_USERNAME] [--mysql_password MYSQL_PASSWORD]
       [--branches BRANCHES]

optional arguments:
  -h, --help            show this help message and exit
  --data_dir DATA_DIR, -d DATA_DIR
                        the location of the tpch-tables (*.tbl)
  --force, -f           Override the exisiting schemas
  --mysql_host MYSQL_HOST, -s MYSQL_HOST
  --mysql_username MYSQL_USERNAME, -u MYSQL_USERNAME
  --mysql_password MYSQL_PASSWORD, -p MYSQL_PASSWORD
  --branches BRANCHES, -b BRANCHES
                        Number of Branches To Create
```

