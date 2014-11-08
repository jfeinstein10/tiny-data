class Client(object):
    
    def __init__(self, td_sock, ip_addr):
        self.sock = td_sock
        self.ip_addr = ip_addr