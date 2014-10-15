
from common.communication import TinyDataProtocol


class ServerFromClientProtocol(TinyDataProtocol):

    def __init__(self):
        TinyDataProtocol.__init__(self)
        self.commands = {
            'ls': self.handle_ls,
            'mv': self.handle_mv,
            'rm': self.handle_rm,
            'mkdir': self.handle_mkdir,
            'cat': self.handle_cat,
            'file_upload': self.handle_file_upload,
            'map_reduce': self.handle_map_reduce,
        }

    def handle_ls(self, socket, payload):
        pass

    def handle_mv(self, socket, payload):
        pass

    def handle_rm(self, socket, payload):
        pass

    def handle_mkdir(self, socket, payload):
        pass

    def handle_cat(self, socket, payload):
        pass

    def handle_file_upload(self, socket, payload):
        pass

    def handle_map_reduce(self, socket, payload):
        pass
