import select
import socket
from threading import Thread

from common.communication import TinyDataSocket, TinyDataProtocolSocket


class ProtocolServer(Thread):

    def __init__(self, protocol, server='', port=0):
        Thread.__init__(self)
        self.protocol = protocol
        self.accept_socket = TinyDataSocket(is_readable=True, is_writeable=False)
        self.accept_socket.listen(server, port)

    def run(self):
        socks = [self.accept_socket]
        while True:
            socket_dict = {sock.get_socket(): sock for sock in socks}
            read_socks = [sock for sock, tdsock in socket_dict.iteritems() if tdsock.readable()]
            write_socks = [sock for sock, tdsock in socket_dict.iteritems() if tdsock.writeable()]
            read_ready, write_ready, _ = select.select(read_socks, write_socks, [], 10)
            # we can read
            for ready in read_ready:
                ready = socket_dict[ready]
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
            for ready in write_ready:
                ready = socket_dict[ready]
                try:
                    ready.handle_write()
                except socket.error, e:
                    ready.handle_close() # TODO for now...
                    socks.remove(ready)