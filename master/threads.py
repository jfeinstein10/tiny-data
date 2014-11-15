import uuid
from collections import defaultdict
import cPickle as pickle

import common.locations as loc
from common.communication import TinyDataProtocolSocket
from common.threads import ProtocolThread
from common.util import ReturnStatus, deserialize_module
from master.file_system import FileSystem, REPLICA_TIMES


followers = []
fs = FileSystem()


class Follower(object):

    def __init__(self, ip_addr):
        self.ip_addr = ip_addr


class MasterServer(ProtocolThread):

    def __init__(self):
        ProtocolThread.__init__(self, 'localhost', loc.master_client_port, is_server=True)
        self.add_command('ls', self.handle_ls)
        self.add_command('rm', self.handle_rm)
        self.add_command('mkdir', self.handle_mkdir)
        self.add_command('cat', self.handle_cat)
        self.add_command('upload_chunk', self.handle_upload_chunk)
        self.add_command('map_reduce', self.handle_map_reduce_upload)

    def validate_exists(self, command, sock, path):
        if fs.exists(path):
            return fs._get_file(path)
        else:
            sock.queue_command([command, path + ' does not exist'])
            return None

    def validate_directory(self, command, sock, path):
        if fs.is_directory(path):
            return fs._get_file(path)
        elif fs.is_file(path):
            sock.queue_command([command, path + ' is not a directory'])
            return None
        else:
            sock.queue_command([command, path + ' does not exist'])
            return None

    def validate_file(self, command, sock, path):
        if fs.is_file(path):
            return fs._get_file(path)
        elif fs.is_directory(path):
            sock.queue_command([command, path + ' is not a file'])
            return None
        else:
            sock.queue_command([command, path + ' does not exist'])
            return None

    def handle_ls(self, sock, payload):
        path = payload[0]
        dir = self.validate_directory('ls', sock, path)
        if dir:
            contents = dir['children'].keys()
            sock.queue_command(['ls', ', '.join(contents)])

    def handle_rm(self, sock, payload):
        path = payload[0]
        file = self.validate_exists('rm', sock, path)
        if file:
            manipulator = ChunkManipulator(sock)
            self.remove_socket(sock, should_close=False)
            manipulator.send_remove(path)
            manipulator.start()

    def handle_mkdir(self, sock, payload):
        path = payload[0]
        successful = fs.create_directory(path)
        if successful:
            sock.queue_command(['mkdir', path + ' created successfully'])
        else:
            sock.queue_command(['mkdir', path + ' was not created'])

    def handle_cat(self, sock, payload):
        path = payload[0]
        file = self.validate_file('cat', sock, path)
        if file:
            manipulator = ChunkManipulator(sock)
            self.remove_socket(sock, should_close=False)
            manipulator.send_get(path)
            manipulator.start()

    def handle_upload_chunk(self, sock, payload):
        path = payload[0]
        chunk = payload[1]
        success = fs.is_file(path) or fs.create_file(path)
        if success:
            manipulator = ChunkManipulator(sock)
            # self.remove_socket(sock, should_close=False)
            manipulator.send_store_chunk(path, chunk)
            manipulator.start()
        else:
            sock.queue_command(['upload_chunk', 'unsuccessful'])

    def handle_map_reduce_upload(self, sock, payload):
        path = payload[0]
        path_results = payload[1]
        map_mod = payload[2]
        combine_mod = '0'
        if payload[3] != '0':
            combine_mod = payload[3]
        reduce_mod = payload[4]
        if self.validate_file('map_reduce', sock, path) and fs.create_file(path_results):
            mr_dispatcher = MapReduceDispatcher(sock, path, path_results, map_mod, combine_mod, reduce_mod)
            mr_dispatcher.start()
        else:
            sock.queue_command(['map_reduce_response', 'unsuccessful'])


class FollowerAcceptor(ProtocolThread):

    def __init__(self):
        ProtocolThread.__init__(self, 'localhost', loc.master_follower_port, is_server=True)

    def run(self):
        while len(self.socks) > 0:
            print followers
            ready_for_read, ready_for_write = self.select()
            # we can read
            for ready in ready_for_read:
                if self.accept_socket and ready == self.accept_socket:
                    sock, address = ready.accept()
                    follower_sock = TinyDataProtocolSocket(self, sock)
                    ip_address = follower_sock.recv(1024)
                    follower_sock.handle_close()
                    followers.append(Follower(ip_address))


class ChunkManipulator(ProtocolThread):

    def __init__(self, client_sock):
        ProtocolThread.__init__(self, is_server=False)
        self.client_sock = client_sock
        self.socks.append(self.client_sock)
        self.add_command('store_chunk', self.handle_store_chunk)
        self.add_command('remove_chunk', self.handle_remove_chunk)
        self.add_command('get_chunk', self.handle_get_chunk)

    def handle_store_chunk(self, sock, payload):
        self.client_sock.queue_command(['upload_chunk', 'chunk stored successfully'])

    def handle_remove_chunk(self, sock, payload):
        self.client_sock.queue_command(['remove_chunk', 'removed successfully'])

    def handle_get_chunk(self, sock, payload):
        self.client_sock.queue_command(['get_chunk', payload[0]])

    def send_store_chunk(self, path, chunk):
        chunk_id = str(uuid.uuid4())
        # Write replications to followers with least storage
        followers_to_write = get_followers_least_filled(REPLICA_TIMES)
        follower_locations = map(lambda f: f.ip_addr, followers_to_write)
        fs.add_chunk_to_file(path, chunk_id, follower_locations)
        for location in follower_locations:
            sock = self.add_socket(location, loc.follower_port)
            sock.queue_command(['store_chunk', path, chunk_id, chunk])

    def send_remove(self, path):
        for chunk_id, locations in fs.get_file_chunks(path).iteritems():
            for location in locations:
                sock = self.add_socket(location, loc.follower_port)
                sock.queue_command(['remove_chunk', path, chunk_id])
        fs.remove(path)

    def send_get(self, path):
        for chunk_id, locations in fs.get_file_chunks(path).iteritems():
            sock = self.add_socket(locations[0], loc.follower_port)
            sock.queue_command(['get_chunk', path, chunk_id])


class MapReduceDispatcher(ProtocolThread):

    def __init__(self, client_sock, path, path_results, map_mod, combine_mod, reduce_mod):
        ProtocolThread.__init__(self, is_server=False)
        self.client_sock = client_sock
        self.path = path
        self.path_results = path_results
        self.map_mod = map_mod
        self.combine_mod = combine_mod
        self.reduce_mod = reduce_mod
        self.reduce_fn = deserialize_module(self.reduce_mod).reduce_fn
        self.counts = []

        self.chunks = dict(fs.get_file_chunks(self.path))
        self.current_assignments = {follower.ip_addr: None for follower in followers}

        self.map_chunks_assigned = []
        self.map_chunks_completed = []
        self.map_result_chunks = {follower.ip_addr: [] for follower in followers}
        self.map_key_values = defaultdict(lambda: [])

        self.outstanding_reduce_followers = []
        self.followers_with_map = {}

        self.add_command('map_response', self.handle_map_response)
        self.add_command('reduce_response', self.handle_reduce_response)

    def handle_map_response(self, sock, payload):
        follower_ip_addr = payload[0]
        return_status = int(payload[1])
        if not (return_status == ReturnStatus.SUCCESS):
            self.client_sock.queue_command(['map_reduce_response', 'unsuccessful'])
        else:
            chunk_id = payload[2]
            result_chunk_id = payload[3]
            pairs = pickle.loads(payload[4])
            for key, value in pairs:
                self.map_key_values[key].append(value)
            # Update Counts
            new_counts = map(lambda x: float(x), payload[5:])
            if len(self.counts) == 0:
                self.counts = new_counts
            else:
                self.counts = map(lambda x: x[0] + x[1], zip(self.counts, new_counts))
            # Remove chunk from outstanding list
            self.map_chunks_assigned.remove(chunk_id)
            self.map_chunks_completed.append(chunk_id)
            self.map_result_chunks[follower_ip_addr].append(result_chunk_id)
            # Assign a new chunk to map
            self.assign_map(follower_ip_addr, sock)
            if len(self.map_chunks_assigned) == 0:
                self.perform_reduce()

    def handle_reduce_response(self, sock, payload):
        follower_ip_addr = payload[0]
        return_status = int(payload[1])
        if return_status == ReturnStatus.SUCCESS:
            self.outstanding_reduce_followers.pop(follower_ip_addr)
            self.client_sock.queue_command(['map_reduce_response', 'successful'])
        else:
            self.client_sock.queue_command(['map_reduce_response', 'unsuccessful'])

    def assign_map(self, follower_ip, sock=None):
        # Assign all pieces to mapped
        for chunk_id, follower_ips in self.chunks.iteritems():
            if chunk_id not in self.map_chunks_assigned \
                    and chunk_id not in self.map_chunks_completed \
                    and follower_ip in follower_ips:
                self.current_assignments[follower_ip] = chunk_id
                self.map_chunks_assigned.append(chunk_id)
                if not sock:
                    sock = self.add_socket(follower_ip, loc.follower_port)
                sock.queue_command(['map', self.path, chunk_id, self.map_mod, self.combine_mod])
                return
        if sock:
            self.remove_socket(sock, True)

    def perform_reduce(self):
        final_keyvals = {}
        for key, values in self.map_key_values.iteritems():
            final_keyvals[key] = self.reduce_fn(key, values)
        # self.client_sock.queue_command(['map_reduce', 'successful'])
        manipulator = ChunkManipulator(self.client_sock)
        manipulator.send_store_chunk(self.path_results, pickle.dumps(final_keyvals))
        manipulator.start()
        print final_keyvals

    def assign_reduce(self):
        # Assign reduces
        results_file = fs._get_file(self.path_results)
        for follower in self.followers_with_map.values():
            # Assign the file a chunk for this file
            results_chunk_id = str(uuid.uuid4())
            results_chunk = Chunk(results_chunk_id, follower)
            results_file['chunks'].append(results_chunk)
            follower.current_task = FollowerTask.REDUCE
            reduce_mod_out = '0'
            if self.reduce_mod and not follower.has_reduce_mod:
                reduce_mod_out = self.reduce_mod
                follower.has_reduce_mod = True
            command = ['reduce', reduce_mod_out, results_chunk_id]
            for map_chunk in follower.map_result_chunks:
                command.append(map_chunk)
            follower.sock.queue_command(command)
            self.outstanding_reduce_followers[follower.ip_addr] = follower

    def run(self):
        for follower in followers:
            self.assign_map(follower.ip_addr)
        while len(self.socks) > 0:
            self.select_iteration()


def compare_follower_storage(f1, f2):
    return f1.bytes_stored - f2.bytes_stored


def get_followers_least_filled(num_followers):
    sorted_list = sorted(followers, compare_follower_storage)
    return_num = min(num_followers, len(sorted_list))
    return sorted_list[:return_num]
