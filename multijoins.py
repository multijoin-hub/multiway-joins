import argparse
from defaults import params
import os
from utils import parse_yaml
def process_cmd_args():
    """
        Process the command line arguments and write 
        files accordingly.
    """
    parser = argparse.ArgumentParser("""
        Welcome to multiway join testbench. In
        this we will test multiway join alogrithm
        with on the TPC-H benchmark with multiple
        data, memory and swap sizes.
    """)
    parser.add_argument("config_file", help="The configuration file yaml")
    parser.add_argument("-t", "--threads", action="store_true", help="Try multithreaded queries") # Try multithreaded
    parser.add_argument("-v", "--verbose", 
                        action='count', 
                        help='Define the verbosity of the output ' +\
                            '(use -vvv to print output from SQL Queries)',
                        default=0)
    args = parser.parse_args()
    if args.verbose == 1:
        print("Verbosity level 1")
    if args.verbose == 2:
        print("Verbosity level 2")
    if args.verbose >= 3:
        print("Verbosity level 3")
    
    print("The configuration file is {}".format(args.config_file))
    return

def def_initial_config(config_file="./config.yaml"):
    """
    Process the config file and generate run configuration
    :param config_file: the path to the configuration file
    :return: None
    """
    initial_config = parse_yaml(os.path.abspath(config_file))
    for tables in initial_config['tables_metadata']:
        print(tables['name'])
    return


if __name__=="__main__":
    process_cmd_args()
    def_initial_config()
