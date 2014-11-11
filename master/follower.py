

class FollowerTask(object):
    MAP = 0
    REDUCE = 1
    NONE = 2


class FollowerState(object):
    INACTIVE = 0
    ACTIVE = 1


class Follower(object):
    
    def __init__(self, ip_addr):
        self.ip_addr = ip_addr
        self.state = FollowerState.ACTIVE
        self.bytes_stored = 0
        self.current_task = FollowerTask.NONE
        self.has_map_mod = False
        self.has_combine_mod = False
        self.has_reduce_mod = False
        self.chunk_ids_mapped = []
        self.map_result_chunks = []