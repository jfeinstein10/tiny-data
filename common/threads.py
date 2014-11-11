import select
import socket
from threading import Thread, Lock

from common.communication import TinyDataSocket, TinyDataProtocolSocket, TinyDataProtocol


DEFAULT_TIMEOUT = 4


class ProtocolThread(Thread, TinyDataProtocol):

    def __init__(self, server='localhost', port=8000, is_server=True):
        Thread.__init__(self)
        TinyDataProtocol.__init__(self)
        self.daemon = True
        self.server = server
        self.port = port
        self.socks = []
        self.socks_lock = Lock()
        if is_server:
            self.accept_socket = TinyDataSocket(is_readable=True, is_writeable=False)
            self.accept_socket.listen(server, port)
            self.socks_append(self.accept_socket)
        else:
            self.accept_socket = None

    def add_socket(self, server=None, port=None):
        server = server or self.server
        port = port or self.port
        sock = TinyDataProtocolSocket(self)
        sock.connect((server, port))
        self.socks_append(sock)
        return sock

    def remove_socket(self, sock, should_close=True):
        self.socks.remove(sock)
        if should_close:
            sock.handle_close()

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
            if self.accept_socket and ready == self.accept_socket:
                sock, address = ready.accept()
                self.socks_append(TinyDataProtocolSocket(self, sock))
            else:
                try:
                    if not ready.handle_read():
                        self.socks.remove(ready)
                except socket.error, e:
                    self.remove_socket(ready)
        # we can write
        for ready in ready_for_write:
            try:
                ready.handle_write()
            except socket.error, e:
                self.remove_socket(ready)

    def socks_append(self, sock):
        with self.socks_lock:
            self.socks.append(sock)

    def run(self):
        while len(self.socks) > 0:
            self.select_iteration()