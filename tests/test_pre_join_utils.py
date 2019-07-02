""" Test for pre_join_utils"""
import unittest
from join_scripts.pre_join_utils import MySQLRelationTree, TableNode, prune


class TestPreJoinUtils(unittest.TestCase):
    """Simple Test Cases"""
    def setUp(self):
        self.rtree = MySQLRelationTree("./join_scripts/join.config.json")

    def test_relation_tree_init(self):
        self.assertEqual(self.rtree.server, "MYSQL")

    def test_connect_and_return_tree(self):
        region_node = self.rtree.connect_and_return_root_node()
        nation_node = region_node.child_list[0]
        customer_node = nation_node.child_list[0]
        orders_node = customer_node.child_list[0]
        lineitem_node = orders_node.child_list[0]
        supplier_node = nation_node.child_list[1]
        partsupp_node = supplier_node.child_list[0]
        self.assertEqual(region_node.name, "REGION")
        self.assertEqual(region_node.child_list[0], nation_node)
        self.assertEqual(region_node.parent, None)
        self.assertEqual(region_node.sid_tuple, [-1, 1])
        self.assertEqual(nation_node.name, "NATION_ALIGNED")
        self.assertEqual(nation_node.child_list[0], customer_node)
        self.assertEqual(nation_node.child_list[1], supplier_node)
        self.assertEqual(nation_node.parent, region_node)
        self.assertEqual(nation_node.sid_tuple, [-1, 1])
        self.assertEqual(customer_node.name, "CUSTOMER_ALIGNED")
        self.assertEqual(customer_node.child_list[0], orders_node)
        self.assertEqual(customer_node.parent, nation_node)
        self.assertEqual(customer_node.sid_tuple, [-1, 1])
        self.assertEqual(orders_node.name, "ORDERS_ALIGNED")
        self.assertEqual(orders_node.child_list[0], lineitem_node)
        self.assertEqual(orders_node.parent, customer_node)
        self.assertEqual(orders_node.sid_tuple, [-1, 1])
        self.assertEqual(lineitem_node.name, "LINEITEM_ALIGNED")
        self.assertEqual(lineitem_node.child_list, None)
        self.assertEqual(lineitem_node.parent, orders_node)
        self.assertEqual(lineitem_node.sid_tuple, [-1, 1])
        self.assertEqual(supplier_node.name, "SUPPLIER_ALIGNED")
        self.assertEqual(supplier_node.child_list[0], partsupp_node)
        self.assertEqual(supplier_node.parent, nation_node)
        self.assertEqual(supplier_node.sid_tuple, [-1, 1])
        self.assertEqual(partsupp_node.name, "PARTSUPP_ALIGNED")
        self.assertEqual(partsupp_node.child_list, None)
        self.assertEqual(partsupp_node.parent, supplier_node)
        self.assertEqual(partsupp_node.sid_tuple, [-1, 1])

    def test_prune(self):
        root_node = self.rtree.connect_and_return_root_node()
        prune(root_node, 2)
        self.assertEqual(
            root_node.child_list[0].child_list[0].child_list, None)

if __name__ == "__main__":
    unittest.main()
