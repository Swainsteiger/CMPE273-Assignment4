import hashlib
import pickle
from hashlib import md5


class ConsistentHashing(object):
    def __init__(self, nodes=None, replicas=2):
        self.ring = dict()
        self.replicas = replicas
        self._sorted_keys = []
        if nodes:
            for node in nodes:
                self.add_node(node)

    def hash(self, key):
        key_bytes = pickle.dumps(key)
        return int(hashlib.md5(key_bytes).hexdigest(), 16)

    def add_node(self, node):
        for i in range(0, self.replicas):
            key = self.hash('%s:%s' % (node, i))
            self.ring[key] = node
            self._sorted_keys.append(key)
        self._sorted_keys.sort()

    def remove_node(self, node):
        for i in range(0, self.replicas):
            key = self.hash('%s:%s' % (node, i))
            del self.ring[key]
            self._sorted_keys.remove(key)

    # def replicate_iterator(self, node):
    #     return (self._hash("%s:%s" % (node, i))
    #             for i in xrange(self.replicas))

    def get_node(self, string_key):
        key = self.hash(string_key)
        nodes = self._sorted_keys
        for i in range(0, len(nodes)):
            node = nodes[i]
            if key <= node:
                return self.ring[node]
        return self.ring[nodes[0]]
