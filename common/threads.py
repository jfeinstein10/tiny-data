import select
import socket
from threading import Thread

from common.communication import TinyDataSocket, TinyDataProtocolSocket


DEFAULT_TIMEOUT = 10


class ProtocolServer(Thread):

    def __init__(self, protocol, server='', port=0):
        Thread.__init__(self)
        self.protocol = protocol
        self.accept_socket = TinyDataSocket(is_readable=True, is_writeable=False)
        self.accept_socket.listen(server, port)

    def select(self, sockets):
        socket_dict = {sock.get_socket(): sock for sock in sockets}
        read_socks = [sock for sock, tdsock in socket_dict.iteritems() if tdsock.readable()]
        write_socks = [sock for sock, tdsock in socket_dict.iteritems() if tdsock.writeable()]
        ready_for_read, ready_for_write, _ = select.select(read_socks, write_socks, [], DEFAULT_TIMEOUT)
        ready_for_read = map(lambda s: socket_dict[s], ready_for_read)
        ready_for_write = map(lambda s: socket_dict[s], ready_for_write)
        return ready_for_read, ready_for_write

    def run(self):
        socks = [self.accept_socket]
        while True:
            ready_for_read, ready_for_write = self.select(socks)
            # we can read
            for ready in ready_for_read:
                if ready == self.accept_socket:
                    sock, address = ready.accept()
                    socks.append(TinyDataProtocolSocket(self.protocol(), sock))
                else:
                    try:
                        if not ready.handle_read():
                            socks.remove(ready)
                    except socket.error, e:
                        ready.handle_close() # TODO for now...
                        socks.remove(ready)
            # we can write
            for ready in ready_for_write:
                try:
                    ready.handle_write()
                except socket.error, e:
                    ready.handle_close() # TODO for now...
                    socks.remove(ready)