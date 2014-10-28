import os
import marshal
import types
from threading import Thread

import common.locations as loc
from common.communication import TinyDataProtocol
from common.threads import ProtocolThread
from common.util import get_filepath


class FollowerServer(ProtocolThread, TinyDataProtocol):

    def __init__(self):
        ProtocolThread.__init__(self, self, 'localhost', loc.follower_listen_port, is_server=True)
        self.commands = {
            'store_chunk': self.handle_store_chunk,
            'remove_chunks': self.handle_remove_chunks,
            'get_chunk': self.handle_get_chunk,
            'map_reduce': self.handle_map_reduce,
        }

    def handle_store_chunk(self, sock, payload):
        file_id = payload[0]
        chunk_id = payload[1]
        chunk = payload[2]
        path = get_filepath(chunk_id)
        with open(path, 'w') as f:
            f.write(chunk)
            sock.queue_command(['store_chunk', 'success'])

    def handle_remove_chunks(self, sock, payload):
        file_id = payload[0]
        chunk_ids = payload[1:]
        paths = [get_filepath(chunk_id) for chunk_id in chunk_ids]
        for path in paths:
            os.remove(path)
        sock.queue_command(['remove_chunks', 'success'])

    def handle_get_chunk(self, sock, payload):
        file_id = payload[0]
        chunk_ids = payload[1:]

    def handle_map_reduce(self, sock, payload):
        file_id = payload[0]
        map_fn = types.FunctionType(marshal.loads(payload[1]), globals(), "some_func_name")
        reduce_fn = types.FunctionType(marshal.loads(payload[2]), globals(), "some_func_name")
        chunk_ids = payload[3:]

        reducer = Reducer(reduce_fn, sock)
        reducer.start()
        mapper = Mapper(map_fn, chunk_ids, reducer)
        mapper.start()

    def run(self):
        while True:
            self.select_iteration()


class Mapper(Thread):

    def __init__(self, map_fn, chunk_ids, reducer):
        self.map_fn = map_fn
        self.chunk_ids = chunk_ids
        self.reducer = reducer


class Reducer(Thread):

    def __init__(self, reduce_fn, sock):
        self.reduce_fn = reduce_fn
        self.sock