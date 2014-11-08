from threading import Lock, Semaphore, Condition, Mutex
import uuid

import common.locations as loc
from common.threads import ProtocolThread
from master.file_system import FileSystem
from master.map_reduce_follower import *
from master.follower import *
import Queue



class MasterServer(ProtocolThread):

    def __init__(self, this_ip_addr):
        ProtocolThread.__init__(self, this_ip_addr, loc.master_client_port, is_server=True)
        self.this_ip_addr = this_ip_addr
        self.followers = {}
        self.clients = {}
        self.fs = FileSystem(self)
        self.follower_acceptor = FollowerAcceptor(self)
        self.follower_acceptor.start()
        self.assign_task_sema = None
        self.mr_dispatcher
        self.sock_with_map_request = None
        self.commands = {
            'ls': self.handle_ls,
            'rm': self.handle_rm,
            'mkdir': self.handle_mkdir,
            'cat': self.handle_cat,
            'upload_chunk': self.handle_upload_chunk,
            'map_reduce': self.handle_map_reduce,
            'map_response':  self.handle_map,
            'reduce_response':  self.handle_reduce
        }

    def validate_exists(self, command, sock, path):
        if self.fs.exists(path):
            return self.fs._get_file(path)
        else:
            sock.queue_command([command, path + ' does not exist'])
            return None

    def validate_directory(self, command, sock, path):
        if self.fs.is_directory(path):
            return self.fs._get_file(path)
        elif self.fs.is_file(path):
            sock.queue_command([command, path + ' is not a directory'])
            return None
        else:
            sock.queue_command([command, path + ' does not exist'])
            return None

    def validate_file(self, command, sock, path):
        if self.fs.is_file(path):
            return self.fs._get_file(path)
        elif self.fs.is_directory(path):
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
        successful = self.fs.create_directory(path)
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
        success = False
        if self.fs.exists(path) and self.fs._is_file(path):
            success = True
        else:
            success = self.fs.create_file(path)
        if success:
            manipulator = ChunkManipulator(sock)
            self.remove_socket(sock, should_close=False)
            manipulator.send_store_chunk(path, chunk)
        else:
            sock.queue_command(['upload_chunk', 'unsuccessful'])

    def handle_map_reduce(self, sock, payload):
        path = payload[0]
        path_results = payload[1]
        map_mod = payload[2]
        combine_mod = None
        if (payload[3]!='0'):  combine_mod = payload[3]
        reduce_mod = payload[4]
        # file = self.validate_file('map_reduce', sock, path) -- TODO:  Why are we validating this?
        successful = self.fs.create_file(path_results)
        if successful:
            self.sock_with_map_request = sock
            self.mr_dispatcher = MapReduceDispatcher(self, sock, path, path_results, map_mod, combine_mod, reduce_mod)
            self.mr_dispatcher.start()
        else:
            sock.queue_command(['map_reduce_response', 'unsuccessful'])

    def handle_map_response(self, sock, payload):
        follower_ip_addr = payload[0]
        return_status = int(payload[1])
        if (not (return_status == Return_Status.SUCCESS)):
            self.sock_with_map_request.queue_command(['map_reduce_response', 'unsuccessful'])
        else:
            mapped_chunk_id = payload[2]
            map_result_chunk_id = payload[3]
            # Update Counts
            if self.mr_dispatcher.counts == []:
                for i in range(3, len(payload)):
                    self.mr_dispatcher.counts.append(float(payload[i]))
            else:
                for i in range(3, len(payload)):
                    self.mr_dispatcher.counts[i] += float(payload[i])
            # Update Follower information
            follower = self.followers[follower_ip_addr]
            follower.current_task = Follower_Task.NONE
            follower.map_result_chunks.append(Chunk(map_result_chunk_id, follower))
            # Update Follower Task semaphore
            self.assign_task_sema.release()
            # Update pass all wait condition
            with self.mr_dispatcher.all_passed_cond:
                self.mr_dispatcher.all_passed = False
                self.mr_dispatcher.all_passed_cond.notify_all()
            # Remove chunk from outstanding list
            with self.mr_dispatcher.outstanding_chunks_to_map_cond:
                for chunk in self.mr_dispatcher.outstanding_chunks_to_map:
                    if chunk.chunk_id == mapped_chunk_id:
                        self.mr_dispatcher.outstanding_chunks_to_map.remove(chunk)
                if (len(self.mr_dispatcher.outstanding_chunks_to_map) == 0):
                    self.mr_dispatcher.oustanding_chunks_to_map_cond.notify_all()

    def handle_reduce_response(self, sock, payload):
        follower_ip_addr = payload[0]
        return_status = int(payload[1])
        if (return_status == ReturnStatus.SUCCESS):
            with self.mr_dispatcher.outstanding_reduce_followers_mutex:
                self.mr_dispatcher.outstanding_reduce_followers.pop(follower_ip_addr)
            self.sock_with_map_request.queue_command(['map_reduce_response', 'successful'])
        else:
            self.sock_with_map_request.queue_command(['map_reduce_response', 'unsuccessful'])



class FollowerAcceptor(ProtocolThread):
    
    def __init__(self, master_server):
        ProtocolThread.__init__(self, master_server.this_ip_addr, loc.master_follower_port, is_server=True)

    def run(self):
        while (len(self.socks) > 0):
            ready_for_read, ready_for_write = self.select()
            # we can read
            for ready in ready_for_read:
                if self.accept_socket and ready == self.accept_socket:
                    sock, address = ready.accept()
                    follower_sock = TinyDataProtocolSocket(self, sock)
                    master_server.socks_append(follower_sock)
                    master_server.followers[address] = Follower(sock, address)



class ChunkManipulator(Thread):

    def __init__(self, sock):
        self.sock = sock

    def handle_store_chunk(self, sock, payload):
        self.sock.queue_command(['upload_chunk', 'chunk stored successfully'])

    def handle_remove_chunk(self, sock, payload):
        self.sock.queue_command(['remove_chunk', 'removed successfully'])

    def send_store_chunk(self, path, chunk):
        chunk_id = str(uuid.uuid4())
        # Write replications to followers with least storage
        followers_to_write = self.fs.get_followers_least_filled(self.fs.REPLICA_TIMES)
        self.fs.add_chunk_to_file(path, chunk_id, followers_to_write)
        for follower in followers_to_write:
            follower.sock.queue_command(['store_chunk', path, chunk_id, chunk])

    def send_remove(self, path):
        for chunk_id, locations in self.fs.get_file_chunks(path).iteritems():
            for location in locations:
                sock = self.add_socket(location, loc.follower_listen_port)
                sock.queue_command(['remove_chunk', path, chunk_id])
        self.fs.remove(path)



class MapReduceDispatcher(Thread):

    def __init__(self, master_server, path, path_results, map_mod, combine_mod, reduce_mod):
        self.master_server = master_server
        self.path = path
        self.path_results = path_results
        self.map_mod = map_mod
        self.combine_mod = combine_mod
        self.reduce_mod = reduce_mod
        self.counts = []

        self.all_passed = True
        self.all_passed_lock = Lock()
        self.all_passed_cond = Condition(self.all_passed_lock)

        self.outstanding_chunks_to_map = []
        self.outstanding_chunks_to_map_lock = Lock()
        self.oustanding_chunks_to_map_cond = Condition(self.outstand_chunks_to_map_lock)

        self.outstanding_reduce_followers = []
        self.outstanding_reduce_followers_mutex = Mutex()

        self.followers_with_map = {}

    def run(self):
        master_server.assign_task_sema = Semaphore(len(master_server.followers))
        chunks_passed = 0

        # Get chunk ids that need to be mapped
        chunks_to_map_list = self.master_server.fs.get_file_chunks(path)
        chunks_to_map = Queue()
        for chunk in chunks_to_map_list:
            chunks_to_map.put(chunk)
        
        # Assign all pieces to mapped
        while(not chunks_to_map.empty()):
            master_server.assign_task_sema.aquire()
            cur_chunk = chunks_to_map.get()
            i = 0
            follower_assigned_map = False
            while (i < len(cur_chunk.followers) and not follower_assigned_map):
                cur_follower = cur_chunk.followers[i]
                if (cur_follower.current_task == Follower_Task.NONE):
                    cur_follower.current_task = Follower_Task.MAP
                    cur_follower.chunk_ids_mapped.append(cur_chunk.chunk_id)
                    map_mod_out = '0'
                    if (self.map_mod and not cur_follower.has_map_mod):
                        map_mod_out = self.map_mod
                        cur_follower.has_map_mod = True
                    combine_mod_out = '0'
                    if (self.combine_mod and not cur_follower.has_combine_mod):
                        combine_mod_out = self.combine_mod
                        cur_follower.has_combine_mod = True
                    with self.oustanding_chunks_to_map_cond:
                        self.outstanding_chunks_to_map.append(cur_chunk)
                    cur_follower.sock.queue_command(['map', '0', cur_chunk.chunk_id, map_mod_out, combine_mod_out])
                    if (not self.followers_with_map.has_key(cur_follower.ip_addr)):
                        self.followers_with_map[cur_follower.ip_addr] = cur_follower
                    follower_assigned_map = True
                    chunks_passed = 0
                i += 1
            if not follower_assigned_map:
                chunks_to_map.append(cur_chunk)
                chunks_passed += 1

            if (chunks_passed == len(chunks_to_map)):
                # Passed through entire list and no available followers match ones we need
                # Block until another becomes available
                with self.all_passed_cond:
                    self.all_passed = True
                    while (self.all_passed):
                        self.all_passed_cond.wait()

        # Wait for all outstanding map tasks to be complete
        with self.oustanding_chunks_to_map_cond:
            while (len(self.outstanding_chunks_to_map) > 0):
                self.full_pass_cond.wait()

        # Assign reduces
        results_file = master_server.fs._get_file(path_results)
        for follower in self.followers_with_map.values():
            # Assign the file a chunk for this file
            results_chunk_id = str(uuid.uuid4())
            results_chunk = Chunk(chunk_id, follower)
            results_file['chunks'].append(results_chunk)
            follower.current_task = Follower_Task.REDUCE
            reduce_mod_out = '0'
            if (self.reduce_mod and not follower.has_reduce_mod):
                reduce_mod_out = self.reduce_mod
                follower.has_reduce_mod = True
            command = ['reduce', '0', reduce_mod_out, results_chunk_id]
            for map_chunk in follower.map_result_chunks:
                command.append(map_chunk)
            follower.sock.queue_command(command)
            with self.outstanding_reduce_followers_mutex:
                self.outstanding_reduce_followers[follower.ip_addr] = follower
