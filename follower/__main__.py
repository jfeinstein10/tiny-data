import os

from threads import FollowerServer
from common.util import get_tinydata_base


def main():
    base = get_tinydata_base()
    if not os.path.exists(base):
        os.mkdir(base)
    follower_server = FollowerServer()
    follower_server.start()
    follower_threads = [follower_server]
    for thread in follower_threads:
        thread.join()
    return follower_threads



if __name__ == '__main__':
    main()