from collections import defaultdict
import os
from uuid import uuid4
import cPickle as pickle
from threading import Thread

import common.locations as loc
from common.threads import ProtocolThread
from common.util import get_filepath, deserialize_module, ReturnStatus, get_tinydata_base, get_own_ip_address


own_ip_address = get_own_ip_address()


def get_next_free_chunk():
    while True:
        uuid = uuid4()
        if uuid not in os.listdir(get_tinydata_base()):
            return str(uuid)


class FollowerServer(ProtocolThread):

    def __init__(self):
        ProtocolThread.__init__(self, own_ip_address, loc.follower_port, is_server=True)
        master_sock = self.add_socket(loc.master_ip, loc.master_follower_port)
        master_sock.send(own_ip_address)
        self.remove_socket(master_sock)
        self.add_command('store_chunk', self.handle_store_chunk)
        self.add_command('remove_chunk', self.handle_remove_chunk)
        self.add_command('get_chunk', self.handle_get_chunk)
        self.add_command('map', self.handle_map)
        self.add_command('reduce',  self.handle_reduce)

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
        chunk_id = payload[1]
        path = get_filepath(chunk_id)
        with open(path, 'r') as f:
            contents = '/n'.join(f.readlines())
            sock.queue_command(['get_chunk', contents])

    def handle_map(self, sock, payload):
        # Get map payload info
        path = payload[0]
        chunk_id = payload[1]

        # Get map module if necessary
        map_fn = combine_fn = None
        if payload[2] != '0':
            map_mod = deserialize_module(payload[2])
            map_fn = map_mod.map_fn
        # Get combine module if necessary
        if payload[3] != '0':
            combine_mod = deserialize_module(payload[3])
            combine_fn = combine_mod.combine_fn

        mapper = Mapper(sock, chunk_id, map_fn, combine_fn)
        mapper.start()

    def handle_reduce(self, sock, payload):
        # Get reduce payload info
        path = payload[0]
        result_chunk_id = payload[2]
        map_chunk_ids = payload[3:]

        # Get reduce module if necessary
        reduce_fn = None
        if payload[1] != '0':
            reduce_mod = deserialize_module(payload[1])
            reduce_fn = reduce_mod.reduce_fn

        reducer = Reducer(sock, result_chunk_id, map_chunk_ids, reduce_fn)
        reducer.start()


class Mapper(Thread):

    def __init__(self, master_sock, chunk_id, map_fn, combine_fn):
        Thread.__init__(self)
        self.master_sock = master_sock
        self.chunk_id = chunk_id
        self.map_fn = map_fn
        self.combine_fn = combine_fn

    def run(self):
        if self.map_fn:
            # Perform map and collect results in dictionary
            counts = []
            result_dict = defaultdict(lambda: [])
            with open(get_filepath(self.chunk_id), 'r') as data:
                for line in data:
                    response = self.map_fn(line)
                    pairs = response
                    if isinstance(response, tuple):
                        pairs, new_counts = response
                        if len(counts) == 0:
                            counts = new_counts
                        else:
                            counts = map(lambda x: x[0]+x[1], counts, new_counts)
                    for key, value in pairs:
                        result_dict[key].append(value)
            # Put results into list
            # Use combine function if supplied
            result_list = []
            if self.combine_fn:
                for key in result_dict:
                    result_list.append((key, self.combine_fn(result_dict[key])))
            else:
                for key in result_dict:
                    for val in result_dict[key]:
                        result_list.append((key, val))
            # Pickle results into written file
            results_chunk_id = get_next_free_chunk()
            with open(get_filepath(results_chunk_id), 'w') as f:
                pickle.dump(result_list, f)
            # Send updates to master
            command = ['map_response', own_ip_address, str(ReturnStatus.SUCCESS), self.chunk_id, str(results_chunk_id), pickle.dumps(result_list)]
            for count in counts:
                command.append(str(count))
            self.master_sock.queue_command(command)


class Reducer(Thread):

    def __init__(self, master_sock, result_chunk_id, map_chunk_ids, reduce_fn):
        Thread.__init__(self)
        self.master_sock = master_sock
        self.result_chunk_id = result_chunk_id
        self.map_chunk_ids = map_chunk_ids
        self.reduce_fn = reduce_fn

    def run(self):
        if self.reduce_fn:
            # Collect key, values from map files into list
            keyvals = {}
            for chunk_id in self.map_chunk_ids:
                with open(get_filepath(chunk_id), 'r') as f:
                    map_results = pickle.load(f)
                    for key, value in map_results:
                        if key in keyvals:
                            keyvals[key].append(value)
                        else:
                            keyvals[key] = [value]
            # Perform reduce
            final_keyvals = {}
            for key, values in keyvals.iteritems():
                final_keyvals[key] = self.reduce_fn(key, values)
            # Write pickeled results to file
            with open(get_filepath(self.result_chunk_id), 'w') as f:
                pickle.dump(final_keyvals, f)
            # Send updates to master
            command = ['reduce_response', own_ip_address, str(ReturnStatus.SUCCESS)]
            self.master_sock.queue_command(command)