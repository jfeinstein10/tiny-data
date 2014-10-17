
from common.communication import TinyDataProtocol


class MasterFromFollowerProtocol(TinyDataProtocol):

    def __init__(self):
        TinyDataProtocol.__init__(self)
        self.commands = {
            'store_chunk': self.handle_store_chunk,
            'remove_chunks': self.handle_remove_chunks,
            'get_chunk': self.handle_get_chunk,
            'map_reduce': self.handle_map_reduce,
        }

    def handle_store_chunk(self, socket, payload):
        status = payload[0]
        chunk_id = payload[1]
        pass

    def handle_remove_chunks(self, socket, payload):
        status = payload[0]
        chunk_ids = payload[1:]
        pass

    def handle_get_chunk(self, socket, payload):
        status = payload[0]
        chunk_id = payload[1]
        chunk = payload[2]
        pass

    def handle_map_reduce(self, socket, payload):
        status = payload[0]
        result = payload[1]
        pass
