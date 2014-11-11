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
    main()