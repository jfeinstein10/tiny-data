from master.threads import MasterServer, FollowerAcceptor


def main():
    follower_acceptor = FollowerAcceptor()
    follower_acceptor.start()
    master_server = MasterServer()
    master_server.start()
    return [master_server, follower_acceptor]


if __name__ == '__main__':
    main()