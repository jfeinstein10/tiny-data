from threading import Lock
import uuid

import common.locations as loc
from common.threads import ProtocolThread
from master.file_system import FileSystem


class MasterServer(ProtocolThread):

    def __init__(self):
        ProtocolThread.__init__(self, 'localhost', loc.master_listen_port)
        self.fs = FileSystem()
        self.commands = {
            'ls': self.handle_ls,
            'rm': self.handle_rm,
            'mkdir': self.handle_mkdir,
            'cat': self.handle_cat,
            'upload_chunk': self.handle_upload_chunk,
            'map_reduce': self.handle_map_reduce,
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
            self.fs.remove(path)
            sock.queue_command(['rm', path + ' removed successfully'])

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
            pass

    def handle_upload_chunk(self, sock, payload):
        path = payload[0]
        chunk = payload[1]
        file = self.validate_file('upload_chunk', sock, path)
        if file:
            ChunkUploader(sock, path, chunk).run()

    def handle_map_reduce(self, sock, payload):
        path = payload[0]
        path_results = payload[1]
        map_fn = payload[2]
        reduce_fn = payload[3]
        file = self.validate_file('map_reduce', sock, path)
        results = self.validate_file('map_reduce', sock, path_results)
        if file and not results:
            self.fs.create_file(results)
            MapReduceDispatcher(sock, path, path_results, map_fn, reduce_fn)


class ChunkUploader(ProtocolThread):

    def __init__(self, sock, path, chunk):
        ProtocolThread.__init__(self, is_server=False)
        self.sock = sock
        self.path = path
        self.uuid = uuid.uuid4()
        self.chunk = chunk
        self.complete_lock = Lock()
        self.complete = False
        self.commands = {
            'store_chunk': self.handle_store_chunk
        }

    def handle_store_chunk(self):
        with self.complete_lock:
            self.complete = True
        self.sock.queue_command(['upload_chunk', self.path + ' chunk stored successfully'])

    def run(self):
        sock = self.add_socket(loc.follower_ips[0], loc.follower_listen_port)
        sock.queue_command(['store_chunk', self.path, self.uuid, self.chunk])
        while True:
            with self.complete_lock:
                if self.complete:
                    break
            self.select_iteration()


class MapReduceDispatcher(ProtocolThread):

    def __init__(self, sock, path, results_path, map_fn, reduce_fn):
        ProtocolThread.__init__(self, is_server=False)
        self.sock = sock
        self.path = path
        self.results_path = results_path
        self.map_fn = map_fn
        self.reduce_fn = reduce_fn

    def run(self):
        sock = self.add_socket(loc.follower_ips[0], loc.follower_listen_port)
        sock.queue_command(['map_reduce', self.map_fn, self.reduce_fn])
