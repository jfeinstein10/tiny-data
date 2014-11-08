class Follower_Task:
    MAP = 0
    REDUCE = 1
    NONE = 2



class Follower_State:
    INACTIVE = 0
    ACTIVE = 1



class Follower:
    
    def __init__(self, sock, ip_addr):
        self.sock = sock
        self.ip_addr = ip_addr
        self.state = Follower_State.ACTIVE
        self.bytes_stored = 0
        self.current_task = Follower_Task.NONE
        self.has_map_mod = False
        self.has_combine_mod = False
        self.has_reduce_mod = False
        self.chunk_ids_mapped = []
        self.map_result_chunks = []