import os
import re
import select
import socket

from Queue import Queue


DEFAULT_PORT = 8000
MAX_LISTEN = 5
PROTOCOL_BUFFER_SIZE = 4096


class SocketHandler(object):

    def __init__(self, server='', port=DEFAULT_PORT):
        self.accepting = True
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.socket.setblocking(0)
        self.fd = self.socket.fileno()
        self.socket.bind((server, port))
        self.socket.listen(MAX_LISTEN)

    def accept(self):
        channel, address = self.socket.accept()
        self.accepting = False

    def readable(self):
        raise NotImplementedError('Subclasses must implement readable')

    def writable(self):
        raise NotImplementedError('Subclasses must implement writable')

    def read(self):
        raise NotImplementedError('Subclasses must implement read')

    def write(self):
        raise NotImplementedError('Subclasses must implement write')

    def select(self):
        while True:
            read_fds = [self.fd] if self.readable() else []
            write_fds = [self.fd] if self.writable() else []
            read_fds, write_fds, _ = select.select(read_fds, write_fds, [])
            # we can either read or accept a connection
            if self.fd in read_fds:
                if self.accepting:
                    self.accept()
                else:
                    self.read()
            # we can write
            if self.fd in write_fds:
                self.write()


class ChatHandler(SocketHandler):

    def __init__(self, server='', port=DEFAULT_PORT):
        SocketHandler.__init__(self, server, port)
        self.read_buffer = ''
        self.send_buffer = ''
        self.terminator = self.get_terminator()

    def get_terminator(self):
        raise NotImplementedError('Subclasses must implement get_terminator')

    def handle_command(self, command):
        raise NotImplementedError('Subclasses must implement handle_command')

    def readable(self):
        return True

    def writable(self):
        return True

    def read(self):
        # Receive data
        try:
            data = self.socket.recv(PROTOCOL_BUFFER_SIZE)
        except socket.error, why:
            return # TODO error checking everywhere...
        # Tack the data onto our buffer
        offset = len(self.read_buffer)
        self.read_buffer += data
        # Now look for a terminator
        if self.terminator in self.read_buffer:
            index = self.read_buffer.find(self.terminator, offset)
            command = self.read_buffer[:index]
            self.read_buffer = self.read_buffer[index+len(self.terminator):]
            handle_command(command)

    def write(self):
        sent = self.socket.send(self.send_buffer[:PROTOCOL_BUFFER_SIZE])
        self.send_buffer = self.send_buffer[sent:]

