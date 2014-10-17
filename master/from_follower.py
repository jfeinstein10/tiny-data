
from common.communication import TinyDataProtocol


class MasterFromFollowerProtocol(TinyDataProtocol):

    def __init__(self):
        TinyDataProtocol.__init__(self)
        self.commands = {
            'store_chunks': self.handle_store_chunk,
            'remove_chunks': self.handle_remove_chunk,
            'get_chunks': self.handle_get_chunk,
            'map_reduce': self.handle_map_reduce,
        }

    def handle_store_chunks(self, socket, payload):
        status = payload[0]
        pass

    def handle_remove_chunks(self, socket, payload):
        status = payload[0]
        pass

    def handle_get_chunks(self, socket, payload):
        status = payload[0]
        chunks = payload[1]
        pass

    def handle_map_reduce(self, socket, payload):
        status = payload[0]
        result = payload[1]
        pass