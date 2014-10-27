import select
import socket
from threading import Thread

from common.communication import TinyDataSocket, TinyDataProtocolSocket


DEFAULT_TIMEOUT = 4


class ProtocolThread(Thread):

    def __init__(self, protocol, server='localhost', port=8000, is_server=True):
        Thread.__init__(self)
        self.protocol = protocol
        self.server = server
        self.port = port
        self.socks = []
        if is_server:
            self.accept_socket = TinyDataSocket(is_readable=True, is_writeable=False)
            self.accept_socket.listen(server, port)
            self.socks.append(self.accept_socket)

    def add_socket(self, server=None, port=None):
        server = server or self.server
        port = port or self.port
        sock = TinyDataProtocolSocket(self.protocol())
        sock.connect((server, port))
        self.socks.append(sock)
        return sock

    def select(self):
        socket_dict = {sock.get_socket(): sock for sock in self.socks}
        read_socks = [sock for sock, tdsock in socket_dict.iteritems() if tdsock.readable()]
        write_socks = [sock for sock, tdsock in socket_dict.iteritems() if tdsock.writeable()]
        ready_for_read, ready_for_write, _ = select.select(read_socks, write_socks, [], DEFAULT_TIMEOUT)
        ready_for_read = map(lambda s: socket_dict[s], ready_for_read)
        ready_for_write = map(lambda s: socket_dict[s], ready_for_write)
        return ready_for_read, ready_for_write

    def select_iteration(self):
        ready_for_read, ready_for_write = self.select()
        # we can read
        for ready in ready_for_read:
            if ready == self.accept_socket:
                sock, address = ready.accept()
                self.socks.append(TinyDataProtocolSocket(self.protocol(), sock))
            else:
                try:
                    if not ready.handle_read():
                        self.socks.remove(ready)
                except socket.error, e:
                    ready.handle_close()  # TODO for now...
                    self.socks.remove(ready)
        # we can write
        for ready in ready_for_write:
            try:
                ready.handle_write()
            except socket.error, e:
                ready.handle_close()  # TODO for now...
                self.socks.remove(ready)


    def run(self):
        while True:
            self.select_iteration()