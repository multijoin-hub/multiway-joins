""" There are two methods in this join algorithm
"""
from collections import deque
from pre_join_utils import MySQLRelationTree, prune


def recursive_join_linear(root_node,
                          join_queue,
                          root_node_idx=0,
                          missed_tuple=None
                          ):
    join_queue.append(root_node.cursor.fetchone())
    if missed_tuple is not None:
        print(join_queue, missed_tuple)
    if root_node.parent is None:
        recursive_join_linear(root_node.child_list[0],
                              join_queue,
                              root_node_idx+1)
    elif root_node.child_list is not None:
        current_node = root_node
        while current_node.child_list is not None:
            if join_queue[root_node_idx-1][-1] != join_queue[root_node_idx][0]:
                join_queue[root_node_idx-1] = current_node.parent.cursor.fetchone()
            current_node = current_node.child_list[0]
        recursive_join_linear(root_node.child_list[0], join_queue,
                              root_node_idx+1)
    elif root_node.child_list is None:
        while join_queue[root_node_idx-1][-1] == join_queue[root_node_idx][0]:
            print(join_queue)
            join_queue[root_node_idx] = root_node.cursor.fetchone()
        missed_tuple = join_queue.pop()
        join_queue.pop()
        recursive_join_linear(root_node.parent, join_queue, root_node_idx-1, missed_tuple)


if __name__ == "__main__":
    rt = MySQLRelationTree('./join.config.json')
    root_node = rt.connect_and_return_root_node()
    join_queue = []
    prune(root_node, 4)
    try:
        recursive_join_linear(root_node, join_queue)
    except TypeError as e:
        print('Completed Linear Join')
    # rt.close_connection()ls

