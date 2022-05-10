from multiprocessing import parent_process
from re import X
from xmlrpc.client import boolean
from pyrsistent import T
from zmq import NULL


class BBTree:

    def __init__():
        return


class BBNode:
    ''' represents a bounding box volume around a single pixel in space-time '''

    int x_coord
    int y_coord
    int t_coord

    BBNode parent
    BBNode left_child
    BBNode right_child

    def __init__(self, x: int, y: int, t: int) -> BBNode:
        self.x_coord = x
        self.y_coord = y
        self.t_coord = t

    def primitive_collision(self, check_node: BBNode) -> boolean:
        dist_x = abs(self.x_coord - check_node.x_coord)
        dist_y = abs(self.y_coord - check_node.y_coord)
        dist_t = abs(self.t_coord - check_node.t_coord)
        if dist_x <= 5 and dist_y <= 5 and dist_t <= 15*60:
            return True
        return False

    def is_root(self):
        return self.parent is NULL

    def is_leaf(self):
