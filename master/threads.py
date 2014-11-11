from threading import Lock, Semaphore, Condition, Thread
import uuid

import common.locations as loc
from common.communication import TinyDataProtocolSocket
from common.threads import ProtocolThread
from common.util import ReturnStatus
from master.chunk import Chunk
from master.file_system import FileSystem, REPLICA_TIMES
from master.follower import *
from Queue import Queue


followers = {}
fs = FileSystem()


class MasterServer(ProtocolThread):

    def __init__(self):
        ProtocolThread.__init__(self, 'localhost', loc.master_client_port, is_server=True)
        self.commands = {
            'ls': self.handle_ls,
            'rm': self.handle_rm,
            'mkdir': self.handle_mkdir,
            'cat': self.handle_cat,
            'upload_chunk': self.handle_upload_chunk,
            'map_reduce': self.handle_map_reduce_upload,
        }

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
            self.remove_socket(sock, should_close=False)

    def handle_upload_chunk(self, sock, payload):
        path = payload[0]
        chunk = payload[1]
        success = fs.is_file(path) or fs.create_file(path)
        if success:
            manipulator = ChunkManipulator(sock)
            self.remove_socket(sock, should_close=False)
            manipulator.send_store_chunk(path, chunk)
        else:
            sock.queue_command(['upload_chunk', 'unsuccessful'])

    def handle_map_reduce_upload(self, sock, payload):
        path = payload[0]
        path_results = payload[1]
        map_mod = payload[2]
        combine_mod = None
        if payload[3] != '0':
            combine_mod = payload[3]
        reduce_mod = payload[4]
        if self.validate_file('map_reduce', sock, path):
            successful = fs.create_file(path_results)
            if successful:
                mr_dispatcher = MapReduceDispatcher(sock, path, path_results, map_mod, combine_mod, reduce_mod)
                mr_dispatcher.start()
            else:
                sock.queue_command(['map_reduce_response', 'unsuccessful'])


class FollowerAcceptor(ProtocolThread):
    
    def __init__(self):
        ProtocolThread.__init__(self, 'localhost', loc.master_follower_port, is_server=True)

    def run(self):
        while len(self.socks) > 0:
            ready_for_read, ready_for_write = self.select()
            # we can read
            for ready in ready_for_read:
                if self.accept_socket and ready == self.accept_socket:
                    sock, address = ready.accept()
                    follower_sock = TinyDataProtocolSocket(self, sock)
                    follower_sock.handle_close()
                    followers[address] = Follower(address)


class ChunkManipulator(ProtocolThread):

    def __init__(self, client_sock):
        ProtocolThread.__init__(self, is_server=False)
        self.client_sock = client_sock
        self.socks.append(self.client_sock)
        self.commands = {
            'store_chunk': self.handle_store_chunk,
            'remove_chunk': self.handle_remove_chunk
        }

    def handle_store_chunk(self, sock, payload):
        self.client_sock.queue_command(['upload_chunk', 'chunk stored successfully'])

    def handle_remove_chunk(self, sock, payload):
        self.client_sock.queue_command(['remove_chunk', 'removed successfully'])

    def send_store_chunk(self, path, chunk):
        chunk_id = str(uuid.uuid4())
        # Write replications to followers with least storage
        followers_to_write = fs.get_followers_least_filled(REPLICA_TIMES)
        follower_locations = map(lambda f: f.ip_addr, followers_to_write)
        fs.add_chunk_to_file(path, chunk_id, follower_locations)
        for follower in followers_to_write:
            follower.sock.queue_command(['store_chunk', path, chunk_id, chunk])

    def send_remove(self, path):
        for chunk_id, locations in fs.get_file_chunks(path).iteritems():
            for location in locations:
                sock = self.add_socket(location, loc.follower_port)
                sock.queue_command(['remove_chunk', path, chunk_id])
        fs.remove(path)


class MapReduceDispatcher(ProtocolThread):

    def __init__(self, client_sock, path, path_results, map_mod, combine_mod, reduce_mod):
        ProtocolThread.__init__(self, is_server=False)
        self.client_sock = client_sock
        self.path = path
        self.path_results = path_results
        self.map_mod = map_mod
        self.combine_mod = combine_mod
        self.reduce_mod = reduce_mod
        self.counts = []

        self.outstanding_chunks_to_map = []
        self.outstanding_chunks_to_map_lock = Lock()

        self.outstanding_reduce_followers = []
        self.outstanding_reduce_followers_lock = Lock()

        self.followers_with_map = {}
        self.commands = {
            'map_response':  self.handle_map_response,
            'reduce_response':  self.handle_reduce_response
        }

    def handle_map_response(self, sock, payload):
        follower_ip_addr = payload[0]
        return_status = int(payload[1])
        if not (return_status == ReturnStatus.SUCCESS):
            self.client_sock.queue_command(['map_reduce_response', 'unsuccessful'])
        else:
            mapped_chunk_id = payload[2]
            map_result_chunk_id = payload[3]
            # Update Counts
            if len(self.counts) == 0:
                for i in range(3, len(payload)):
                    self.counts.append(float(payload[i]))
            else:
                for i in range(3, len(payload)):
                    self.counts[i] += float(payload[i])
            # Update Follower information
            follower = followers[follower_ip_addr]
            follower.current_task = FollowerTask.NONE
            follower.map_result_chunks.append(Chunk(map_result_chunk_id, follower))
            # Remove chunk from outstanding list
            with self.outstanding_chunks_to_map_lock:
                for chunk in self.outstanding_chunks_to_map:
                    if chunk.chunk_id == mapped_chunk_id:
                        self.outstanding_chunks_to_map.remove(chunk)
                if len(self.outstanding_chunks_to_map) == 0:
                    self.assign_reduce()

    def handle_reduce_response(self, sock, payload):
        follower_ip_addr = payload[0]
        return_status = int(payload[1])
        if return_status == ReturnStatus.SUCCESS:
            with self.outstanding_reduce_followers_lock:
                self.outstanding_reduce_followers.pop(follower_ip_addr)
            self.client_sock.queue_command(['map_reduce_response', 'successful'])
        else:
            self.client_sock.queue_command(['map_reduce_response', 'unsuccessful'])

    def assign_map(self):
        # Get chunk ids that need to be mapped
        chunks_to_map_list = fs.get_file_chunks(self.path)
        chunks_to_map = Queue()
        for chunk in chunks_to_map_list:
            chunks_to_map.put(chunk)

        # Assign all pieces to mapped
        while not chunks_to_map.empty():
            cur_chunk = chunks_to_map.get()
            i = 0
            follower_assigned_map = False
            while i < len(cur_chunk.followers) and not follower_assigned_map:
                cur_follower = cur_chunk.followers[i]
                if cur_follower.current_task == FollowerTask.NONE:
                    cur_follower.current_task = FollowerTask.MAP
                    cur_follower.chunk_ids_mapped.append(cur_chunk.chunk_id)
                    map_mod_out = '0'
                    if self.map_mod and not cur_follower.has_map_mod:
                        map_mod_out = self.map_mod
                        cur_follower.has_map_mod = True
                    combine_mod_out = '0'
                    if self.combine_mod and not cur_follower.has_combine_mod:
                        combine_mod_out = self.combine_mod
                        cur_follower.has_combine_mod = True
                    with self.outstanding_chunks_to_map_cond:
                        self.outstanding_chunks_to_map.append(cur_chunk)
                    cur_follower.sock.queue_command(['map', '0', cur_chunk.chunk_id, map_mod_out, combine_mod_out])
                    if not self.followers_with_map.has_key(cur_follower.ip_addr):
                        self.followers_with_map[cur_follower.ip_addr] = cur_follower
                    follower_assigned_map = True
                    chunks_passed = 0
                i += 1
            if not follower_assigned_map:
                chunks_to_map.append(cur_chunk)
                chunks_passed += 1

            if chunks_passed == len(chunks_to_map_list):
                # Passed through entire list and no available followers match ones we need
                # Block until another becomes available
                with self.all_passed_cond:
                    self.all_passed = True
                    while self.all_passed:
                        self.all_passed_cond.wait()

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
            command = ['reduce', '0', reduce_mod_out, results_chunk_id]
            for map_chunk in follower.map_result_chunks:
                command.append(map_chunk)
            follower.sock.queue_command(command)
            with self.outstanding_reduce_followers_lock:
                self.outstanding_reduce_followers[follower.ip_addr] = follower

    def run(self):
        self.assign_map()
        while len(self.socks) > 0:
            self.select_iteration()
