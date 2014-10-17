
from common.communication import TinyDataProtocol


class FollowerFromMasterProtocol(TinyDataProtocol):

    def __init__(self):
        TinyDataProtocol.__init__(self)
        self.commands = {
            'store_chunks': self.handle_store_chunk,
            'remove_chunks': self.handle_remove_chunk,
            'get_chunks': self.handle_get_chunk,
            'map_reduce': self.handle_map_reduce,
        }

    def handle_store_chunks(self, socket, payload):
        file_id = payload[0]
        chunk_ids = payload[1]
        pass

    def handle_remove_chunks(self, socket, payload):
        file_id = payload[0]
        chunk_ids = payload[1]
        pass

    def handle_get_chunks(self, socket, payload):
        file_id = payload[0]
        chunk_ids = payload[1]
        pass

    def handle_map_reduce(self, socket, payload):
        file_id = payload[0]
        map_fn = payload[1]
        reduce_fn = payload[2]
        pass