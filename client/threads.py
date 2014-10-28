
from common.threads import ProtocolThread


class ClientThread(ProtocolThread):

    def __init__(self):
        ProtocolThread.__init__(self, is_server=False)