import sys
from collections import defaultdict
import os
import cPickle as pickle
from threading import Thread

import common.locations as loc
from common.threads import ProtocolThread
from common.util import get_filepath, deserialize_module, ReturnStatus


# Globals
map_fn = None
combine_fn = None
reduce_fn = None

chunk_ids_assigned = []
chunk_id_last_assigned = 0


class FollowerServer(ProtocolThread):

    def __init__(self, this_ip_addr, master_ip_addr):
        ProtocolThread.__init__(self, this_ip_addr, loc.follower_port, is_server=False)
        # Connect to master
        self.master_sock = self.add_socket((master_ip_addr, loc.master_follower_port))
        
        self.this_ip_addr = this_ip_addr

        self.commands = {
            'store_chunk': self.handle_store_chunk,
            'remove_chunk': self.handle_remove_chunk,
            'get_chunk': self.handle_get_chunk,
            'map': self.handle_map,
            'reduce':  self.handle_reduce
        }

    @staticmethod
    def get_next_free_chunk():
        global chunk_id_last_assigned
        all_tried = False
        cur_chunk_num = chunk_id_last_assigned
        while cur_chunk_num and not all_tried:
            if cur_chunk_num == -sys.maxint-1:
                cur_chunk_num = -1
            else:
                cur_chunk_num += 1
            if cur_chunk_num == chunk_id_last_assigned:
                all_tried = True
            elif cur_chunk_num not in chunk_ids_assigned:
                chunk_id_last_assigned = cur_chunk_num
                chunk_ids_assigned.append(cur_chunk_num)
                return cur_chunk_num

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

    def handle_map(self, sock, payload):
        global map_fn, combine_fn
        # Get map payload info
        path = payload[0]
        chunk_id = payload[1]
        
        # Get map module if necessary
        if payload[2] != '0':
            map_mod = deserialize_module(payload[2])
            map_fn = map_mod.map_fn
        # Get map module if necessary
        if payload[3] != '0':
            combine_mod = deserialize_module(payload[3])
            combine_fn = combine_mod.combine_fn
            
        mapper = Mapper(self, self.master_sock, chunk_id)
        mapper.start()
            
    def handle_reduce(self, sock, payload):
        global reduce_fn
        # Get reduce payload info
        path = payload[0]
        result_chunk_id = payload[2]
        map_chunk_ids = payload[3:]
        
        # Get reduce module if necessary
        if payload[1] != '0':
            reduce_mod = deserialize_module(payload[1])
            reduce_fn = reduce_mod.reduce_fn
            
        reducer = Reducer(self, sock, result_chunk_id, map_chunk_ids)
        reducer.start()

    def run(self):
        while True:
            self.select_iteration()


class Mapper(Thread):

    def __init__(self, follower, master_sock, chunk_id):
        self.chunk_id = chunk_id
        self.master_sock = master_sock
        self.follower = follower

    def run(self):
        global map_fn, combine_fn
        if map_fn:
            # Perform map and collect results in dictionary
            result_dict = defaultdict([])
            with open(get_filepath(self.chunk_id), 'r') as data:
                for line in data:
                    pairs, counts = self.map_fn(line)
                    for key, value in pairs:
                        result_dict[key].append(value)
            # Put results into list
            # Use combine function if supplied
            result_list = []
            if combine_fn:
                for key in result_dict:
                    result_list.append((key, combine_fn(result_dict[key])))
            else:
                for key in result_dict:
                    for val in result_dict[key]:
                        result_list.append((key, val))
            # Pickle results into written file
            results_chunk_id = FollowerServer.get_next_free_chunk()
            with open(get_filepath(results_chunk_id),'w') as f:
                pickle.dump(result_list, f)
            # Send updates to master
            command = ['map_response', self.follower.this_ip_addr, str(ReturnStatus.SUCCESS), self.chunk_id, str(results_chunk_id)]
            for count in counts:
                command.append(str(count))
            self.master_sock.queue_command(command)
                

class Reducer(Thread):

    def __init__(self, follower, master_sock, result_chunk_id, map_chunk_ids):
        self.master_sock = master_sock
        self.result_chunk_id = result_chunk_id
        self.map_chunk_ids = map_chunk_ids
        self.follower = follower

    def run(self):
        global reduce_fn
        if reduce_fn:
            # Collect key,values from map files into list
            keyvals = []
            for chunk_id in self.map_chunk_ids:
                with open(get_filepath(chunk_id), 'r') as f:
                    map_results = pickle.load(f)
                    for pair in map_results:
                        keyvals.append(pair)
            # Perform reduce
            final_keyvals = {}
            pairs, counts = reduce_fn(keyvals)
            for key, val in pairs:
                final_keyvals[key] = val
            # Write pickeled results to file
            with open(get_filepath(self.result_chunk_id), 'w') as f:
                pickle.dump(final_keyvals, f)
            # Send updates to master
            command = ['reduce_response', self.follower.this_ip_addr, str(ReturnStatus.SUCCESS)]
            for count in counts:
                command.append(str(count))
            self.master_sock.queue_command(command)