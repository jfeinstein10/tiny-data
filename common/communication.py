import socket
import ssl

from common import util

# SSL reference: http://carlo-hamalainen.net/blog/2013/1/24/python-ssl-socket-echo-test-with-self-signed-certificate

DEFAULT_PORT = 8000
MAX_LISTEN = 5
PROTOCOL_PACKET_SIZE = 4096
PROTOCOL_TERMINATOR = '\0\0'
PROTOCOL_DELIMITER = '\t'

class TinyDataSocket(object):

    def __init__(self, sock=None, is_readable=False, is_writeable=False, use_ssl=False):
        self.is_readable = is_readable
        self.is_writeable = is_writeable
        self.is_accepted = False
        if not sock:
            # Set up the socket
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket = sock
        if use_ssl:
            self.socket = ssl.wrap_socket(sock, keyfile=util.get_ssl_key(), certfile=util.get_ssl_cert(),
                                          cert_reqs=ssl.CERT_REQUIRED, ca_certs=util.get_ssl_cacerts(),
                                          do_handshake_on_connect=True)
        # self.socket.setblocking(0)
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    # Pass through methods
    def accept(self, *args):
        return self.socket.accept(*args)

    def connect(self, *args):
        return self.socket.connect(*args)

    def recv(self, *args):
        return self.socket.recv(*args)

    def send(self, *args):
        return self.socket.send(*args)

    def listen(self, server='', port=DEFAULT_PORT):
        self.socket.bind((server, port))
        return self.socket.listen(MAX_LISTEN)

    # TinyData specific
    def get_socket(self):
        return self.socket

    def readable(self):
        return self.is_readable

    def writeable(self):
        return self.is_writeable

    def handle_data(self, data):
        raise NotImplementedError()

    def handle_read(self):
        data = self.socket.recv(PROTOCOL_PACKET_SIZE)
        if not data:
            self.handle_close()
            return False
        else:
            self.handle_data(data)
            return True

    def handle_write(self):
        raise NotImplementedError()

    def handle_close(self):
        self.socket.close()


class TinyDataProtocolSocket(TinyDataSocket):

    def __init__(self, protocol, socket=None, use_ssl=False):
        TinyDataSocket.__init__(self, socket, True, True, use_ssl)
        self.protocol = protocol
        self.read_buffer = ''
        self.write_buffer = ''

    def handle_data(self, data):
        offset = len(self.read_buffer)
        terminator = self.protocol.get_terminator()
        self.read_buffer += data
        if terminator in self.read_buffer:
            index = self.read_buffer.find(terminator, offset)
            raw_command = self.read_buffer[:index]
            self.read_buffer = self.read_buffer[index+len(terminator):]
            self.protocol.handle_command(self, self.protocol.deserialize_command(raw_command))

    def writeable(self):
        return len(self.write_buffer) > 0

    def queue_command(self, command):
        self.protocol.validate_command(command)
        self.write_buffer += self.protocol.serialize_command(command)

    def handle_write(self):
        sent = self.send(self.write_buffer[:PROTOCOL_PACKET_SIZE])
        self.write_buffer = self.write_buffer[sent:]

    def send_command(self, command):
        self.protocol.validate_command(command)
        TinyDataSocket.send(self, self.protocol.serialize_command(command))


class TinyDataProtocol(object):
    """
    commands are in self.commands in the form {"command": fn(socket, payload), ...}
    protocol commands are of the form ["command", "...", "...", ...]
    """

    def __init__(self, terminator=PROTOCOL_TERMINATOR, delimiter=PROTOCOL_DELIMITER):
        self.commands = {}
        self.terminator = terminator
        self.delimiter = delimiter

    def get_terminator(self):
        return self.terminator

    def get_delimiter(self):
        return self.delimiter

    def handle_command(self, socket, command):
        if command[0] in self.commands:
            handle_fn = self.commands[command[0]]
            if handle_fn:
                handle_fn(socket, command[1:])

    def serialize_command(self, command):
        return self.delimiter.join(command) + self.terminator

    def deserialize_command(self, command):
        return command.split(self.delimiter)

    def validate_command(self, command):
        for piece in command:
            if self.terminator in piece or self.delimiter in piece:
                raise Exception('Command cannot contain protocol delimiter or terminator')
