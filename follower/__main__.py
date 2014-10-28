import os

from follower.threads import FollowerServer
from common.util import get_tinydata_base


def main():
    base = get_tinydata_base()
    if not os.path.exists(base):
        os.mkdir(base)
    fServer = FollowerServer()
    fServer.start()
    fServer.join()


if __name__ == '__main__':
    main()