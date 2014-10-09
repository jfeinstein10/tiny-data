import select
import socket
import os

DEFAULT_PORT = 8000

class CommunicationHandler(object):

    def __init__(self, server='', port=DEFAULT_PORT):
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.socket.setblocking(0)
        self.socket.bind((server, port))

    def readable(self):
        raise NotImplementedError('Subclasses must implement readable')

    def writable(self):
        raise NotImplementedError('Subclasses must implement writable')

    def handle_read(self):
        raise NotImplementedError('Subclasses must implement handle_read')

    def handle_write(self):
        raise NotImplementedError('Subclasses must implement handle_write')

    def select(self):
        pass