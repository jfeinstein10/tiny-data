import os

from follower.threads import FollowerServer
from common.util import get_tinydata_base


def main():
    base = get_tinydata_base()
    if not os.path.exists(base):
        os.mkdir(base)
    follower_server = FollowerServer()
    follower_server.start()
    return [follower_server]


if __name__ == '__main__':
    follower_threads = main()
    for thread in follower_threads:
        thread.join()
