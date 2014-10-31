from threading import Lock
import uuid

import common.locations as loc
from common.threads import ProtocolThread
from master.file_system import FileSystem


fs = FileSystem()


class MasterServer(ProtocolThread):

    def __init__(self):
        ProtocolThread.__init__(self, 'localhost', loc.master_listen_port, is_server=True)
        self.commands = {
            'ls': self.handle_ls,
            'rm': self.handle_rm,
            'mkdir': self.handle_mkdir,
            'cat': self.handle_cat,
            'upload_chunk': self.handle_upload_chunk,
            'map_reduce': self.handle_map_reduce,
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
        success = fs.create_file(path)
        if success:
            manipulator = ChunkManipulator(sock)
            self.remove_socket(sock, should_close=False)
            manipulator.send_store_chunk(path, chunk)
            manipulator.start()
        else:
            sock.queue_command(['upload_chunk', 'unsuccessful'])

    def handle_map_reduce(self, sock, payload):
        path = payload[0]
        path_results = payload[1]
        job_contents = payload[2]
        file = self.validate_file('map_reduce', sock, path)
        successful = fs.create_file(path_results)
        if file and successful:
            mr_dispatcher = MapReduceDispatcher(sock, path, path_results, job_contents)
            self.remove_socket(sock, should_close=False)
            mr_dispatcher.start()
        else:
            sock.queue_command(['map_reduce', 'unsuccessful'])


class ChunkManipulator(ProtocolThread):

    def __init__(self, sock):
        ProtocolThread.__init__(self, is_server=False)
        self.sock = sock
        self.socks.append(self.sock)
        self.commands = {
            'store_chunk': self.handle_store_chunk,
            'remove_chunk': self.handle_remove_chunk
        }

    def handle_store_chunk(self, sock, payload):
        self.sock.queue_command(['upload_chunk', 'chunk stored successfully'])

    def handle_remove_chunk(self, sock, payload):
        self.sock.queue_command(['remove_chunk', 'removed successfully'])

    def send_store_chunk(self, path, chunk):
        chunk_id = str(uuid.uuid4())
        fs.add_chunk_to_file(path, chunk_id, [loc.follower_ips[0]])
        # TODO make a smarter decision here
        sock = self.add_socket(loc.follower_ips[0], loc.follower_listen_port)
        sock.queue_command(['store_chunk', path, chunk_id, chunk])

    def send_remove(self, path):
        for chunk_id, locations in fs.get_file_chunks(path).iteritems():
            for location in locations:
                sock = self.add_socket(location, loc.follower_listen_port)
                sock.queue_command(['remove_chunk', path, chunk_id])
        fs.remove(path)


class MapReduceDispatcher(ProtocolThread):

    def __init__(self, sock, path, results_path, job_contents):
        ProtocolThread.__init__(self, is_server=False)
        self.sock = sock
        self.path = path
        self.results_path = results_path
        self.job_contents = job_contents
        self.commands = {
            'map_reduce': self.handle_results
        }

    def handle_results(self, sock, payload):
        pass

    def run(self):
        # TODO make a smarter decision here
        sock = self.add_socket(loc.follower_ips[0], loc.follower_listen_port)
        sock.queue_command(['map_reduce', self.path, self.job_contents, 0, 1, 2])
        while len(self.socks) > 0:
            self.select_iteration()
