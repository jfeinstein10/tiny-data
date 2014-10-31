from collections import defaultdict
import os
import cPickle as pickle
from threading import Thread, Lock

import common.locations as loc
from common.threads import ProtocolThread
from common.util import get_filepath, deserialize_module


class FollowerServer(ProtocolThread):

    def __init__(self):
        ProtocolThread.__init__(self, loc.follower_ips[0], loc.follower_listen_port, is_server=True)
        self.commands = {
            'store_chunk': self.handle_store_chunk,
            'remove_chunk': self.handle_remove_chunk,
            'get_chunk': self.handle_get_chunk,
            'map_reduce': self.handle_map_reduce,
        }

    def handle_store_chunk(self, sock, payload):
        path = payload[0]
        chunk_id = payload[1]
        chunk = payload[2]
        path = get_filepath(chunk_id)
        with open(path, 'w') as f:
            f.write(chunk)
            sock.queue_command(['store_chunk', 'success'])

    def handle_remove_chunk(self, sock, payload):
        path = payload[0]
        chunk_id = payload[1]
        chunk_path = get_filepath(chunk_id)
        os.remove(chunk_path)
        sock.queue_command(['remove_chunk', 'success'])

    def handle_get_chunk(self, sock, payload):
        path = payload[0]
        chunk_ids = payload[1:]
        paths = [get_filepath(chunk_id) for chunk_id in chunk_ids]
        for path in paths:
            with open(path, 'r') as f:
                for line in f:
                    pass

    def handle_map_reduce(self, sock, payload):
        path = payload[0]
        job_contents = payload[1]
        chunk_ids = payload[2:]

        job = deserialize_module(job_contents)
        map_fn = job.map_fn
        reduce_fn = job.reduce_fn

        reducer = Reducer(reduce_fn, len(chunk_ids), sock)
        for chunk_id in chunk_ids:
            mapper = Mapper(map_fn, chunk_id, reducer)
            mapper.start()

    def run(self):
        while True:
            self.select_iteration()


class Mapper(Thread):

    def __init__(self, map_fn, chunk_id, reducer):
        self.map_fn = map_fn
        self.chunk_id = chunk_id
        self.reducer = reducer

    def run(self):
        results = defaultdict([])
        with open(get_filepath(self.chunk_id), 'r') as data:
            for line in data:
                pairs = self.map_fn(line)
                for key, value in pairs:
                    results[key].append(value)
        self.reducer.handle_mapping_results(results)


class Reducer(Thread):

    def __init__(self, reduce_fn, num_mappers, sock):
        self.reduce_fn = reduce_fn
        self.num_mappers = num_mappers
        self.num_results = 0
        self.lock = Lock()
        self.sock = sock
        self.results = defaultdict([])

    def handle_mapping_results(self, results):
        with self.lock:
            for key, values in results.iteritems():
                self.results[key] += values
            self.num_results += 1
            if self.num_results == self.num_mappers:
                self.start()

    def run(self):
        final_results = {}
        for key, values in self.results.iteritems():
            result = self.reduce_fn(key, values)
            final_results[key] = result
        self.sock.queue_command(['map_reduce', pickle.dumps(final_results)])
