import argparse
import os

from follower.threads import FollowerServer
from common.util import get_tinydata_base


parser = argparse.ArgumentParser()
parser.add_argument('master_ip', help='The public IP address of the master machine')


def main(master_ip):
    base = get_tinydata_base()
    if not os.path.exists(base):
        os.mkdir(base)
    follower_server = FollowerServer(master_ip)
    follower_server.start()
    return [follower_server]


if __name__ == '__main__':
    args = parser.parse_args()
    follower_threads = main(args.master_ip)
    for thread in follower_threads:
        thread.join()
