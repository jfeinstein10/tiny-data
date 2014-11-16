import unittest

import master.__main__
import follower.__main__


class TestServer(unittest.TestCase):

    def test_server(self):
        master_threads = master.__main__.main()
        follower_threads = follower.__main__.main()
        for thread in master_threads:
            thread.join()
        for thread in follower_threads:
            thread.join()


if __name__ == '__main__':
    unittest.main()
