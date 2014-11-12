import zlib

from common.threads import ProtocolThread
import common.locations as loc
from common.util import serialize_module


class ClientThread(ProtocolThread):

    def __init__(self):
        ProtocolThread.__init__(self, is_server=False)
        self.sock = self.add_socket(loc.master_ip, loc.master_client_port)
        self.complete = False
        self.commands = {
            'ls': self.handle_result,
            'rm': self.handle_result,
            'mkdir': self.handle_result,
            'cat': self.handle_result,
            'upload_chunk': self.handle_result,
            'remove_chunk': self.handle_result,
            'map_reduce': self.handle_result,
        }

    def handle_result(self, sock, payload):
        print payload[0]
        self.remove_socket(sock)

    def send_simple(self, command, path):
        self.sock.queue_command([command, path])

    def send_map_reduce(self, path, results_path, map_path, reduce_path, combine_path=None):
        map_contents = serialize_module(map_path)
        reduce_contents = serialize_module(map_path)
        if combine_path:
            combine_contents = serialize_module(map_path)
        else:
            combine_contents = '0'
        self.sock.queue_command(['map_reduce', path, results_path, map_contents, combine_contents, reduce_contents])

    def send_upload(self, path, local_path, lines_per_chunk):
        print path, local_path, lines_per_chunk
        with open(local_path, 'r') as local_file:
            buff = ''
            count = 0
            for line in local_file:
                buff += line
                count += 1
                if count % lines_per_chunk == 0:
                    self.sock.queue_command(['upload_chunk', path, buff])
                    buff = ''
                    count = 0
            if buff:
                self.sock.queue_command(['upload_chunk', path, buff])
