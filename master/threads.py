from Queue import Queue
from threading import Thread

from common.threads import ProtocolThread
from master.from_client import MasterFromClientProtocol
from master.from_follower import MasterFromFollowerProtocol

class ClientThread(ProtocolThread):
    command_queue = Queue()

    def __init__(self):
        ProtocolThread.__init__(self, MasterFromClientProtocol, 'localhost', 8000)

    def run(self):
        while True:
            self.select_iteration()


class LogicThread(Thread):
    _instance = None

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super(LogicThread, cls).__new__(cls, *args, **kwargs)
        return cls._instance

    def __init__(self):
        self.command_queue = Queue()

    def run(self):
        pass

    def file_upload(self):
        pass

    def map_reduce(self):
        pass


class FollowerThread(ProtocolThread):
    command_queue = Queue()

    def __init_(self):
        ProtocolThread.__init__(self, MasterFromFollowerProtocol, 'localhost', 8001)

    def run(self):
        while True:
            self.select_iteration()