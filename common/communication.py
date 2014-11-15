import socket
import ssl

from common import util

# SSL reference: http://carlo-hamalainen.net/blog/2013/1/24/python-ssl-socket-echo-test-with-self-signed-certificate

DEFAULT_PORT = 8000
MAX_LISTEN = 5
PROTOCOL_PACKET_SIZE = 8192
PROTOCOL_TERMINATOR = '\0\0'
PROTOCOL_DELIMITER = '\1\1'

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
        self.delimiter = self.protocol.get_delimiter()
        self.terminator = self.protocol.get_terminator()
        self.read_buffer = ''
        self.write_buffer = ''
        self.current_command = None
        self.streaming_started = False

    def read_and_update_buffer(self, length, offset):
        raw_response = self.read_buffer[:length]
        self.read_buffer = self.read_buffer[length + offset:]
        return raw_response

    def handle_data(self, data):
        self.read_buffer += data
        while True:
            # If we aren't already in a command, then try to get one
            if not self.current_command and self.delimiter in self.read_buffer:
                delimiter_index = self.read_buffer.find(self.delimiter)
                command = self.read_buffer[:delimiter_index]
                self.current_command = self.protocol.get_command(command)
                self.streaming_started = False
            # If we still don't have a command, then break
            if not self.current_command:
                break
            else:
                # See if the terminator is in the buffer
                terminator_index = -1
                if self.terminator in self.read_buffer:
                    terminator_index = self.read_buffer.find(self.terminator)
                if not self.current_command.streaming:
                    # Handle the not streaming case
                    if terminator_index >= 0:
                        raw_command = self.read_and_update_buffer(terminator_index, len(self.terminator))
                        self.current_command.end_callback(self, self.protocol.deserialize_command(raw_command)[1:])
                        self.current_command = None
                    else:
                        break
                else:
                    # Handle the streaming case
                    if not self.streaming_started:
                        delimiter_count = self.read_buffer.count(self.delimiter)
                        if delimiter_count >= self.current_command.num_args + 1:
                            index = util.find_nth(self.read_buffer, self.delimiter, self.current_command.num_args + 1)
                            raw_response = self.read_and_update_buffer(index, len(self.delimiter))
                            self.current_command.start_callback(self, self.protocol.deserialize_command(raw_response))
                            self.streaming_started = True
                    if self.streaming_started:
                        if terminator_index >= 0:
                            # Read up until the terminator and then we have a new command
                            raw_response = self.read_and_update_buffer(terminator_index, len(self.terminator))
                            self.current_command.streaming_callback(self, raw_response)
                            self.current_command.end_callback(self)
                            self.current_command = None
                        else:
                            # Dump the whole thing and then break
                            raw_response = self.read_and_update_buffer(len(self.read_buffer), 0)
                            self.current_command.streaming_callback(raw_response)
                            break
                    else:
                        break

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
        self._commands = {}
        self.terminator = terminator
        self.delimiter = delimiter

    def add_command(self, name, end_callback=None):
        self._commands[name] = ProtocolCommand(name, streaming=False, end_callback=end_callback)

    def add_streaming_command(self, name, num_args, start_callback, streaming_callback, end_callback):
        self._commands[name] = ProtocolCommand(name, num_args, True, start_callback, streaming_callback, end_callback)

    def get_terminator(self):
        return self.terminator

    def get_delimiter(self):
        return self.delimiter

    def get_command(self, command):
        if command in self._commands:
            return self._commands[command]
        else:
            return ProtocolCommand()

    def handle_command(self, socket, command):
        if command[0] in self._commands:
            handle_fn = self._commands[command[0]]
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


class ProtocolCommand(object):

    def __init__(self, name='', num_args=0, streaming=False, start_callback=None,
                 streaming_callback=None, end_callback=None):
        self.name = name
        self.num_args = num_args
        self.streaming = streaming
        self.start_callback = start_callback
        self.streaming_callback = streaming_callback
        self.end_callback = end_callback
