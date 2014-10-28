
from common.communication import TinyDataProtocol
from common.threads import ProtocolThread


class ClientThread(ProtocolThread, TinyDataProtocol):

    def __init__(self):
        ProtocolThread.__init__(self, self, is_server=False)
        TinyDataProtocol.__init__(self)