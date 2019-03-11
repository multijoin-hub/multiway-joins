#!/usr/bin/python3

"""
A Wrapper class around the mysql connector
That connects to the mysql server and creates
aligned relationships
"""

import argparse
import os
import sys
import mysql.connector
from mysql.connector import Error
import logging
import glob
from copy import copy


class AlignedRelationsCreator(object):

    def __init__(
                self,
                mysql_host="localhost",
                username="tpch",
                password="tpch",
                data_dir="../data/TPCH0.1GB",
                force=True,
                num_branches=1
                ):
        """
        Connect to the mysql server and create aligned relations
        For now exisiting databases and tables are dropped

        Arguments:
            -- mysql_host(str): Hostname for mysql server
            -- username(str): Username for mysql server
            -- password(str): Password for mysql server
            -- data_dir(str): Path to the .tbl files
            -- force(bool): Overwrite existing schemas

        Notes:
            -- TODO: Implement Force = False
        """
        self.data_dir = data_dir
        self.logger = logging.getLogger(self.__class__.__name__)
        c_handler = logging.StreamHandler()
        f_handler = logging.FileHandler('aligned_relations_creator.log')
        c_format = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        f_format = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        c_handler.setFormatter(c_format)
        f_handler.setFormatter(f_format)
        self.logger.setLevel(logging.DEBUG)
        # Add handlers to the logger
        self.logger.addHandler(c_handler)
        self.logger.addHandler(f_handler)
        self.schema_name = self.data_dir.split("/")[-1].replace(".", "_")
        if not force:
            force = True
        self.force = force
        try:
            self.conn = mysql.connector.connect(
                                                host=mysql_host,
                                                user=username,
                                                password=password,
                                                allow_local_infile=True
                                            )
            self.cursor = self.conn.cursor()

            if self.cursor:
                self.logger.info("Connected to the Mysql Server")
        except Error as e:
            self.logger.exception("Error Connecting to server")

    def _create_basic_relationship(self):
        """Create a basic TPC-H Schema and load tables from files"""
        self.logger.debug(
            "Following Schema will be created: {}".format(self.schema_name))
        queries = ""
        if self.force:
            queries += "DROP DATABASE IF EXISTS {}".format(self.schema_name)
            self.cursor.execute(queries)
            self.logger.info(
                "Database {} Exists. Dropping Now".format(self.schema_name))
        queries = "CREATE SCHEMA {0};".format(self.schema_name, self.schema_name)
        queries_use_db = "USE DATABASE {};".format(self.schema_name)
        try:
            self.cursor.execute(queries)
            self.logger.info(
                "Created following schema in server:{}".format(self.schema_name))
            # Change to current db
            self.conn.database = self.schema_name
            self.cursor = self.conn.cursor()
            self.logger.info("Database Changed to {}".format(self.schema_name))
        except Error as e:
            self.logger.exception("Error Creating Schema")
        for query in open('./create_tpch_schema.sql', 'r'):
            try:
                _ = self.cursor.execute(query)
            except:
                self.logger.exception("Error Creating tables")

        for alter_query in open('assign_pk_fk.sql', 'r'):
            # Add a Try Catch Block Later
            self.cursor.execute(alter_query)
            self.logger.debug('MySQL Query: {}'.format(alter_query))
        self.cursor.execute("SHOW TABLES;")
        table_names = self.cursor.fetchall()
        self.logger.info("Finished Creating Tables {}".format(table_names))
        return

    def _perform_infile_operations(self):
        """Perform infile operations to load the dataset"""
        self.conn.allow_local_infile = True  # Sanity
        self.cursor = self.conn.cursor()

        tbl_files = glob.glob(self.data_dir+"/*.tbl")
        tbl_names = [(tbl_file.split("/")[-1].replace(".tbl", "")).upper() for tbl_file in copy(tbl_files)]

        for tbl_file, tbl_name in zip(tbl_files, tbl_names):
            infile_query = "LOAD DATA LOCAL INFILE '<-->' INTO TABLE <<---->> FIELDS  TERMINATED BY '|';"
            infile_query = infile_query.replace("<-->", tbl_file)
            infile_query = infile_query.replace("<<---->>", tbl_name)
            try:
                self.cursor.execute(infile_query)
            except Error as e:
                self.logger.exception("Error Performing Infile Query")
            self.logger.debug("MySQL Query: {}".format(infile_query))
            self.logger.debug("Loaded File {} into table {}".format(tbl_file, tbl_name))
        # Commit the transaction
        self.conn.commit()
        self.logger.info("Finished loading all tables from {}".format(self.data_dir))

    def _create_aligned_relations(self, num_branches=1):
        """Create Relationships Defined in the paper
            TODO: Implement num_branches = 2
        """
        TABLE_NAMES = ['REGIONS_ALIGNED', 'NATION_ALIGNED', 'CUSTOMER_ALIGNED',
                        'SUPPLIER_ALIGNED', 'PARTSUPP_ALIGNED', 'ORDER_ALIGNED'
                        'LINEITEM_ALIGNED']
        SIDS = ['R_REGIONSID', 'N_REGIONSID', 'N_NATIONSID', ]
        TABLES = ['REGION', 'NATION', 'CUSTOMER', 'SUPPLIER', 'PARTSUPP', 'ORDER', 'LINITEM']

        hybrid = [(old, new) for old, new in zip(TABLE_NAMES, TABLES)]
        print(hybrid)
        

    def operate(self):
        """Create Aligned Relations"""
        self._create_basic_relationship()
        self._perform_infile_operations()
        # self._create_aligned_relations()


def parse_args():
    """Parse the Command Line Arguments and return them"""
    parser = argparse.ArgumentParser("""
            Create Aligned Relations For TPC-H Schema in Mysql Server
            By Default the data is assumed to be in ../data/TPCH* directories
            """)
    parser.add_argument("--data_dir", "-d", type=str,
                        help="the location of the tpch-tables (*.tbl)",
                        default="../data/TPCH0.1GB")
    parser.add_argument("--force", "-f", action="store_true",
                        default=False, help="Override the exisiting schemas")
    parser.add_argument("--mysql_host", "-s", type=str, default="localhost")
    parser.add_argument("--mysql_username", "-u", type=str, default="tpch")
    parser.add_argument("--mysql_password", "-p", type=str, default="tpch")
    parser.add_argument("--branches", "-b", type=int, default=1, help="Number of Branches To Create")
    program_args = parser.parse_args()
    return program_args


if __name__ == "__main__":
    args = parse_args()
    # print(args)
    al_rel_creator = AlignedRelationsCreator()
    # al_rel_creator.create_basic_relationship()
    al_rel_creator.operate()
