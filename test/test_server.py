import unittest

from common.communication import TinyDataProtocol
from common.threads import ProtocolThread


class TestProtocol(TinyDataProtocol):

    def __init__(self):
        TinyDataProtocol.__init__(self)
        self.commands = {
            'hello': self.handle_hello
        }

    def handle_hello(self, socket, payload):
        print payload


class TestServer(unittest.TestCase):

    def test_server(self):

        # Reactive server (just handles incoming connections)
        ProtocolThread(TestProtocol, 'localhost', 8000, is_server=True).run()

        # Proactive server, initiates a connection with another server
        thread = ProtocolThread(TestProtocol, 'localhost', 8000, is_server=False)
        socket = thread.add_socket()
        socket.queue_command(['hello', 'world'])
        socket.run()


if __name__ == '__main__':
    unittest.main()