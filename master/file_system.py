from collections import defaultdict


REPLICA_TIMES = 2


# a dict-based file system
class FileSystem(object):
    _instance = None

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super(FileSystem, cls).__new__(cls, *args, **kwargs)
        return cls._instance

    def __init__(self):
        self.dict = self._new_dict()
        self.dict['is_directory'] = True
        self.dict['children'] = self._new_dict()

    def _new_dict(self):
        return defaultdict(lambda: None)

    def _get_file(self, path):
        steps = path.split('/')[1:]
        pwd = self.dict
        # TODO root is hardcoded
        if len(steps) == 1 and not steps[0]:
            return pwd
        for step in steps:
            if self._is_directory(pwd) and step in pwd['children']:
                pwd = pwd['children'][step]
            else:
                return None
        return pwd

    def _get_parent_path(self, path):
        return '/'.join(path.split('/')[:-1])

    def _get_filename(self, path):
        return path.split('/')[-1]

    def _get_parent(self, path):
        path = '/'.join(path.split('/')[:-1])
        return self._get_file(path)

    def _verify_path(self, path):
        return path[0] is '/' and path[-1] is not '/'

    def exists(self, path):
        return not (self._get_file(path) is None)

    def _is_file(self, file):
        return file and file['is_file']

    def is_file(self, path):
        return self._is_file(self._get_file(path))

    def _is_directory(self, file):
        return file and file['is_directory']

    def is_directory(self, path):
        return self._is_directory(self._get_file(path))

    def get_directory_contents(self, path):
        dir = self._get_file(path)
        if self._is_directory(dir):
            return dir['children'].keys()
        return []

    def get_file_chunks(self, path):
        file = self._get_file(path)
        if self._is_file(file):
            return file['chunks']
        return []

    def create_file(self, path):
        valid = self._verify_path(path)
        if not valid:
            return False
        parent = self._get_file(self._get_parent_path(path))
        if self._is_directory(parent):
            filename = self._get_filename(path)
            if parent['children'][filename]:
                return False
            else:
                file = self._new_dict()
                file['is_file'] = True
                file['chunks'] = {}
                parent['children'][filename] = file
                return True
        return False

    def create_directory(self, path):
        valid = self._verify_path(path)
        if not valid:
            return False
        parent = self._get_parent(path)
        if self._is_directory(parent):
            filename = self._get_filename(path)
            if parent['children'][filename]:
                return False
            else:
                dir = self._new_dict()
                dir['is_directory'] = True
                dir['children'] = self._new_dict()
                parent['children'][filename] = dir
                return True
        return False

    def add_chunk_to_file(self, path, chunk_id, followers):
        file = self._get_file(path)
        if self._is_file(file):
            file['chunks'][chunk_id] = followers
            return True
        return False

    def remove(self, path):
        parent = self._get_parent(path)
        if self._is_directory(parent):
            filename = self._get_filename(path)
            child = parent['children'][filename]
            del parent['children'][filename]
            return child
        return None
