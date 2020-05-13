import pickle
import hashlib
import murmur3

from server_config import NODES

def find_node(object_bytes):
    high_score = -1
    winner = None
    for node in range(len(NODES)):
        score = murmur3.murmur3_32("%s-%s" % (str(node), str(object_bytes)))
        if score > high_score:
            (high_score, winner) = (score, node)
        elif score == high_score:
            (high_score, winner) = (score, max(str(node), str(winner)))
    node_object = pickle.dumps(winner)
    node_key = hash_code_hex(node_object)
    return node_key

def serialize(object):
    return pickle.dumps(object)


def deserialize(object_bytes):
    return pickle.loads(object_bytes)


def hash_code_hex(data_bytes):
    hash_code = hashlib.md5(data_bytes)
    return hash_code.hexdigest()


def serialize_PUT(object):
    object_bytes = pickle.dumps(object)
    hash_code = hash_code_hex(object_bytes)
    envelope_bytes = pickle.dumps({
        'operation': 'PUT',
        'id': hash_code,
        'payload': object
    })
    return envelope_bytes, hash_code

def serialize_GET(id):
    envelope_bytes = pickle.dumps({
        'operation': 'GET',
        'id': id
    })
    return envelope_bytes, id


def test():
    data_bytes, hash_code = serialize_PUT({ 'user': 'Foo' })
    print(f"Data Bytes={data_bytes}\nHash Code={hash_code}")

