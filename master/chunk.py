class Chunk(object):

    def __init__(self, chunk_id):
        self.chunk_id = chunk_id
        self.followers = []

    def __init__(self, chunk_id, followers):
        self.chunk_id = chunk_id
        self.followers = followers