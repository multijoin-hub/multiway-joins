"""
Set of utility methods and classes to facilitate the join algorithm
"""

import json
import logging
import os
import sys
from copy import copy
import pymysql
from pymysql.cursors import SSCursor


# Set up logging for this module
pre_join_utils_logger = logging.getLogger(__name__)
fmtr = logging.Formatter('[%(asctime)s:%(name)s:%(levelname)s] -> %(message)s')
if not os.path.exists("./logs"):  # Creates a logs directory
    os.makedirs('./logs', mode=0o765)
# set up file and stream handlers
fh = logging.FileHandler('./logs/pre_join_utils.log')
fh.setFormatter(fmtr)
fh.setLevel(logging.DEBUG)
sh = logging.StreamHandler(stream=sys.stdout)
sh.setFormatter(fmtr)
sh.setLevel(logging.INFO)
pre_join_utils_logger.addHandler(sh)
pre_join_utils_logger.addHandler(fh)
pre_join_utils_logger.setLevel(logging.DEBUG)


class TableNode():
    def __init__(self,
                 name,
                 parent=None,
                 child_list=None,
                 cursor=None,
                 sid_tuple=None):
        """Create a table Node"""
        self.name = name
        self.parent = parent
        self.child_list = child_list
        self.cursor = cursor
        self.sid_tuple = sid_tuple


class MySQLRelationTree():
    def __init__(self, config_file):
        """ Create a relation tree from config file """
        with open(config_file, "r") as fp:
            config = json.load(fp)
            self.server = config['server']
            self.tables_info = config['tables']
            if self.server == "MYSQL":
                self.conns = []
                for j in range(len(self.tables_info)):
                    connection = pymysql.connect(host=config['host'],
                                                 user=config['username'],
                                                 password=config['password'],
                                                 db=config['database'],
                                                 cursorclass=SSCursor)
                    self.conns.append(connection)

    def connect_and_return_root_node(self):
        """Connect and Return Root Node"""
        nodes_arr = self._connect_and_return_root_helper()
        return nodes_arr[0]

    def connect_and_return_node_arr(self):
        """Connect and Return root tree 
        nodes_arr = self._connect_and_return_root_helper()
        return nodes_arr

    def _connect_and_return_root_helper(self):
        result_curs = self._connect_and_return_cursors()
        nodes_arr = []
        # First Pass: Create Nodes
        for i in range(len(self.tables_info)):
            current_node = TableNode(self.tables_info[i]['name'])
            current_node.sid_tuple = self.tables_info[i]['sids']
            current_node.cursor = result_curs[i]
            nodes_arr.append(current_node)

        # Second Pass: Create Tree
        for j in range(len(self.tables_info)):
            if self.tables_info[j]['parent_idx'] > -1:
                nodes_arr[j].parent = nodes_arr[
                    self.tables_info[j]['parent_idx']]
                pre_join_utils_logger.info("{0}(currentNode) <- {1}(parent)"
                                           .format(nodes_arr[j].name,
                                                   nodes_arr[j].parent.name))

            nodes_arr[j].child_list = []
            for k in range(len(self.tables_info[j]['child_list_idx'])):
                if self.tables_info[j]['child_list_idx'][k] > -1:
                    nodes_arr[j].child_list.append(
                        nodes_arr[self.tables_info[j]['child_list_idx'][k]])
                    pre_join_utils_logger.info(
                                            "{0}(currentNode) -> {1}(child)"
                                            .format(nodes_arr[j].name,
                                                    nodes_arr[j].child_list[k].name))

            if len(nodes_arr[j].child_list) == 0:
                nodes_arr[j].child_list = None

        # The first element of the list is the root node
        return nodes_arr

    def _connect_and_return_cursors(self):
        """Helper method for connect and cursors"""
        statement = "SELECT * FROM {0} ORDER BY {1}"
        result_curs = []
        for j in range(len(self.tables_info)):
            stm = statement.format(self.tables_info[j]['name'],
                                   self.tables_info[j]['orderby'])
            try:
                cursor = self.conns[j].cursor()
                cursor.execute(stm)
                result_curs.append(cursor)
            except:
                pass
        return result_curs


def prune(root_node, depth=0):
    """Prune the tree to given depth"""
    if depth == 0:
        root_node.child_list = None
    child_list = root_node.child_list
    if child_list is not None:
        for child in child_list:
            prune(child, depth-1)
