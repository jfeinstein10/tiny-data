from common.communication import TinyDataProtocol
from master.file_system import FileSystem


class MasterFromClientProtocol(TinyDataProtocol):

    def __init__(self):
        TinyDataProtocol.__init__(self)
        self.fs = FileSystem()
        self.commands = {
            'ls': self.handle_ls,
            'rm': self.handle_rm,
            'mkdir': self.handle_mkdir,
            'cat': self.handle_cat,
            'file_upload': self.handle_file_upload,
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

    def handle_file_upload(self, sock, payload):
        path = payload[0]
        split_character = payload[1]
        split_frequency = payload[2]
        file = payload[3]
        pass

    def handle_map_reduce(self, sock, payload):
        path = payload[0]
        map_fn = payload[1]
        reduce_fn = payload[2]
        path_results = payload[3]
        pass
