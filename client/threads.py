import zlib

from common.threads import ProtocolThread
import common.locations as loc
from common.util import serialize_module


class ClientThread(ProtocolThread):

    def __init__(self):
        ProtocolThread.__init__(self, is_server=False)
        self.sock = self.add_socket(loc.master_ip, loc.master_listen_port)
        self.commands = {}

    def send_simple(self, command, path):
        self.sock.queue_command([command, path])

    def send_map_reduce(self, path, results_path, job_path):
        job_contents = serialize_module(job_path)
        self.sock.queue_command(['map_reduce', path, results_path, job_contents])

    def send_upload(self, path, local_path, split_on, split_freq):
        with open(local_path, 'r') as local_file:
            buff = ''
            count = 0
            for line in local_file:
                buff += line
                count += 1
                if count % split_freq == 0:
                    buff = zlib.compress(buff)
                    self.sock.queue_command(['upload_chunk', path, buff])
                    buff = ''
                    count = 0