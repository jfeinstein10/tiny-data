
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
        path = payload[0]
        pass

    def handle_mv(self, socket, payload):
        path_from = payload[0]
        path_to = payload[1]
        pass

    def handle_rm(self, socket, payload):
        path = payload[0]
        pass

    def handle_mkdir(self, socket, payload):
        path = payload[0]
        pass

    def handle_cat(self, socket, payload):
        path = payload[0]
        pass

    def handle_file_upload(self, socket, payload):
        path = payload[0]
        split_character = payload[1]
        num_before_split = payload[2]
        file = payload[3]
        pass

    def handle_map_reduce(self, socket, payload):
        path = payload[0]
        map_fn = payload[1]
        reduce_fn = payload[2]
        path_results = payload[3]
        pass
